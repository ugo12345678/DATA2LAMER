from __future__ import annotations

import json
import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_RULES_PATH = (
    "data/regulations/generated_rules.json"
    if Path("data/regulations/generated_rules.json").exists()
    else "data/regulations/geospatial_rules_seed.json"
)
SEED_PATH = Path(os.environ.get("REG_GEOSPATIAL_RULES_FILE", DEFAULT_RULES_PATH))
CANDIDATES_PATH = Path(os.environ.get("REG_RULE_CANDIDATES_FILE", "data/regulations/generated_rule_candidates.json"))
SOURCE_DOCUMENTS_PATH = Path(
    os.environ.get("REG_SOURCE_DOCUMENTS_FILE", "data/regulations/source_documents_manifest.json")
)
SPOTS_TABLE = os.environ.get("REG_SPOTS_TABLE", "spots")
ZONES_TABLE = os.environ.get("REG_ZONES_TABLE", "zones")
ENABLE_ZONES = os.environ.get("REG_ENABLE_ZONES", "true").lower() == "true"
ENABLE_AUDIT_MODEL = os.environ.get("REG_ENABLE_AUDIT_MODEL", "true").lower() == "true"
ROW_FETCH_LIMIT = int(os.environ.get("REG_FETCH_LIMIT", "10000"))
CENTROID_DELTA_DEG = float(os.environ.get("REG_CENTROID_DELTA_DEG", "0.01"))
ALLOW_SPOTS_FALLBACK_FOR_ZONE_UNION = (
    os.environ.get("REG_ALLOW_SPOTS_FALLBACK_FOR_ZONE_UNION", "true").lower() == "true"
)
POSTGRES_INT_MAX = 2_147_483_647


@dataclass(frozen=True)
class BBox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_bbox(lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> BBox:
    return BBox(
        lat_min=min(lat_min, lat_max),
        lat_max=max(lat_min, lat_max),
        lon_min=min(lon_min, lon_max),
        lon_max=max(lon_min, lon_max),
    )


def bbox_overlap(a: BBox, b: BBox) -> bool:
    if a.lon_max < b.lon_min or a.lon_min > b.lon_max:
        return False
    if a.lat_max < b.lat_min or a.lat_min > b.lat_max:
        return False
    return True


def extract_bbox_from_numeric_columns(row: dict[str, Any]) -> BBox | None:
    candidates = [
        ("latitude_min", "latitude_max", "longitude_min", "longitude_max"),
        ("lat_min", "lat_max", "lon_min", "lon_max"),
        ("min_lat", "max_lat", "min_lon", "max_lon"),
    ]

    for keys in candidates:
        if all(k in row for k in keys):
            lat_min = as_float(row.get(keys[0]))
            lat_max = as_float(row.get(keys[1]))
            lon_min = as_float(row.get(keys[2]))
            lon_max = as_float(row.get(keys[3]))
            if None not in (lat_min, lat_max, lon_min, lon_max):
                return normalize_bbox(lat_min, lat_max, lon_min, lon_max)

    return None


def collect_geojson_lon_lat_pairs(node: Any, out: list[tuple[float, float]]) -> None:
    if isinstance(node, list):
        if len(node) >= 2 and isinstance(node[0], (int, float)) and isinstance(node[1], (int, float)):
            out.append((float(node[0]), float(node[1])))
            return
        for item in node:
            collect_geojson_lon_lat_pairs(item, out)


def extract_bbox_from_geojson_dict(payload: dict[str, Any]) -> BBox | None:
    bbox_values = payload.get("bbox")
    if isinstance(bbox_values, list) and len(bbox_values) >= 4:
        lon_min = as_float(bbox_values[0])
        lat_min = as_float(bbox_values[1])
        lon_max = as_float(bbox_values[2])
        lat_max = as_float(bbox_values[3])
        if None not in (lat_min, lat_max, lon_min, lon_max):
            return normalize_bbox(lat_min, lat_max, lon_min, lon_max)

    coordinates = payload.get("coordinates")
    if coordinates is None and isinstance(payload.get("geometry"), dict):
        coordinates = payload["geometry"].get("coordinates")

    if coordinates is not None:
        pairs: list[tuple[float, float]] = []
        collect_geojson_lon_lat_pairs(coordinates, pairs)
        if pairs:
            lons = [p[0] for p in pairs]
            lats = [p[1] for p in pairs]
            return normalize_bbox(min(lats), max(lats), min(lons), max(lons))

    features = payload.get("features")
    if isinstance(features, list):
        all_pairs: list[tuple[float, float]] = []
        for feature in features:
            if not isinstance(feature, dict):
                continue
            geometry = feature.get("geometry")
            if isinstance(geometry, dict):
                collect_geojson_lon_lat_pairs(geometry.get("coordinates"), all_pairs)
        if all_pairs:
            lons = [p[0] for p in all_pairs]
            lats = [p[1] for p in all_pairs]
            return normalize_bbox(min(lats), max(lats), min(lons), max(lons))

    return None


def extract_bbox_from_wkt(text: str) -> BBox | None:
    normalized = text.strip().upper()
    if not (normalized.startswith("POLYGON") or normalized.startswith("MULTIPOLYGON")):
        return None

    values = re.findall(r"-?\d+(?:\.\d+)?", text)
    if len(values) < 4:
        return None

    coords = [float(v) for v in values]
    if len(coords) % 2 != 0:
        return None

    lons = coords[0::2]
    lats = coords[1::2]
    return normalize_bbox(min(lats), max(lats), min(lons), max(lons))


def extract_bbox_from_geometry_columns(row: dict[str, Any]) -> BBox | None:
    for key in ("polygon", "bbox_json", "geometry", "geom", "geojson"):
        payload = row.get(key)
        if payload is None:
            continue

        if isinstance(payload, str):
            parsed = None
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                parsed = None

            if isinstance(parsed, dict):
                bbox = extract_bbox_from_geojson_dict(parsed)
                if bbox:
                    return bbox

            bbox = extract_bbox_from_wkt(payload)
            if bbox:
                return bbox

        if isinstance(payload, list):
            pairs: list[tuple[float, float]] = []
            collect_geojson_lon_lat_pairs(payload, pairs)
            if pairs:
                lons = [p[0] for p in pairs]
                lats = [p[1] for p in pairs]
                return normalize_bbox(min(lats), max(lats), min(lons), max(lons))

        if isinstance(payload, dict):
            bbox = extract_bbox_from_geojson_dict(payload)
            if bbox:
                return bbox

            lat_min = as_float(payload.get("lat_min") or payload.get("latitude_min"))
            lat_max = as_float(payload.get("lat_max") or payload.get("latitude_max"))
            lon_min = as_float(payload.get("lon_min") or payload.get("longitude_min"))
            lon_max = as_float(payload.get("lon_max") or payload.get("longitude_max"))
            if None not in (lat_min, lat_max, lon_min, lon_max):
                return normalize_bbox(lat_min, lat_max, lon_min, lon_max)

    return None


def extract_bbox_from_centroid(row: dict[str, Any], delta_deg: float = CENTROID_DELTA_DEG) -> BBox | None:
    center_candidates = [
        ("lat_center", "lon_center"),
        ("lat", "lon"),
        ("latitude", "longitude"),
    ]

    for lat_key, lon_key in center_candidates:
        if lat_key in row and lon_key in row:
            lat = as_float(row.get(lat_key))
            lon = as_float(row.get(lon_key))
            if lat is None or lon is None:
                continue
            return normalize_bbox(lat - delta_deg, lat + delta_deg, lon - delta_deg, lon + delta_deg)

    return None


def extract_entity_bbox(row: dict[str, Any]) -> BBox | None:
    return (
        extract_bbox_from_numeric_columns(row)
        or extract_bbox_from_geometry_columns(row)
        or extract_bbox_from_centroid(row)
    )


def compute_entities_envelope(items: list[dict[str, Any]]) -> BBox:
    if not items:
        raise ValueError("Impossible de calculer l'enveloppe: entites absentes.")

    lat_mins = [item["bbox"].lat_min for item in items]
    lat_maxs = [item["bbox"].lat_max for item in items]
    lon_mins = [item["bbox"].lon_min for item in items]
    lon_maxs = [item["bbox"].lon_max for item in items]

    return normalize_bbox(min(lat_mins), max(lat_maxs), min(lon_mins), max(lon_maxs))


def fetch_table_rows(client, table_name: str) -> list[dict[str, Any]]:
    response = client.table(table_name).select("*").limit(ROW_FETCH_LIMIT).execute()
    return response.data or []


def load_seed_rules() -> list[dict[str, Any]]:
    if not SEED_PATH.exists():
        raise FileNotFoundError(f"Fichier seed introuvable: {SEED_PATH}")

    payload = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Le seed geospatial doit etre une liste de regles.")
    return payload


def load_optional_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def to_spot_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        entity_id = row.get("id")
        if entity_id is None:
            continue

        bbox = extract_entity_bbox(row)
        if not bbox:
            continue

        items.append(
            {
                "id": str(entity_id),
                "name": row.get("name"),
                "zone_id": str(row.get("zone_id")) if row.get("zone_id") else None,
                "bbox": bbox,
            }
        )
    return items


def to_zone_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        entity_id = row.get("id")
        if entity_id is None:
            continue

        bbox = extract_entity_bbox(row)
        if not bbox:
            continue

        items.append(
            {
                "id": str(entity_id),
                "name": row.get("name"),
                "bbox": bbox,
            }
        )
    return items


def upsert_source_document(client, seed_rule: dict[str, Any], timestamp_iso: str) -> dict[str, Any]:
    source = seed_rule["source"]
    data = {
        "source_type": source["source_type"],
        "source_priority": source["source_priority"],
        "authority_name": source.get("authority_name"),
        "title": source["title"],
        "source_url": source["source_url"],
        "legal_reference": seed_rule.get("legal_reference"),
        "effective_date": source.get("effective_date"),
        "fetched_at": timestamp_iso,
        "checked_at": timestamp_iso,
        "needs_manual_review": bool(seed_rule.get("needs_manual_review", False)),
        "updated_at": timestamp_iso,
    }

    response = client.table("reg_documents_sources").upsert(data, on_conflict="source_url").execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Upsert source document n'a retourne aucune ligne.")
    return rows[0]


def primary_citation(seed_rule: dict[str, Any]) -> dict[str, Any]:
    citations = seed_rule.get("citations") or []
    if citations and isinstance(citations[0], dict):
        return citations[0]
    source = seed_rule.get("source") or {}
    return {
        "source_url": source.get("source_url"),
        "source_title": source.get("title"),
        "authority_name": source.get("authority_name"),
        "quote": seed_rule.get("description") or "",
        "locator": seed_rule.get("legal_reference") or seed_rule.get("rule_key"),
        "document_hash": f"legacy:{source.get('source_url') or seed_rule.get('rule_key')}",
        "confidence_score": seed_rule.get("confidence_score"),
    }


def safe_chunk_index(rule_key: str) -> int:
    digest = hashlib.sha1(rule_key.encode("utf-8")).hexdigest()[:8]
    raw_value = int(digest, 16)
    return (raw_value % POSTGRES_INT_MAX) + 1


def create_extraction_run(
    client,
    started_at: str,
    seed_rules: list[dict[str, Any]],
    source_documents: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> str:
    response = client.table("reg_extraction_runs").insert(
        {
            "status": "RUNNING",
            "started_at": started_at,
            "source_count": len({(rule.get("source") or {}).get("source_url") for rule in seed_rules}),
            "document_count": len(source_documents),
            "candidate_count": len(candidates) if candidates else len(seed_rules),
            "published_rule_count": len(seed_rules),
            "metadata": metadata,
        }
    ).execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Creation du run d'extraction impossible.")
    return str(rows[0]["id"])


def finalize_extraction_run(client, run_id: str, payload: dict[str, Any]) -> None:
    client.table("reg_extraction_runs").update(payload).eq("id", run_id).execute()


def upsert_audit_source_document(
    client,
    seed_rule: dict[str, Any],
    extraction_run_id: str | None,
    timestamp_iso: str,
) -> dict[str, Any]:
    source = seed_rule.get("source") or {}
    citation = primary_citation(seed_rule)
    source_url = str(citation.get("source_url") or source.get("source_url") or "")
    document_hash = str(citation.get("document_hash") or f"legacy:{source_url}")
    quote = str(citation.get("quote") or seed_rule.get("description") or "")

    data = {
        "extraction_run_id": extraction_run_id,
        "document_hash": document_hash,
        "source_url": source_url,
        "canonical_url": source_url,
        "source_type": source.get("source_type"),
        "authority_name": citation.get("authority_name") or source.get("authority_name"),
        "title": citation.get("source_title") or source.get("title"),
        "document_type": "pdf" if source_url.lower().endswith(".pdf") else "html",
        "content_length": len(quote),
        "fetched_at": timestamp_iso,
        "checked_at": timestamp_iso,
        "extraction_status": "ok",
        "metadata": {
            "rule_keys": [seed_rule.get("rule_key")],
            "source_priority": source.get("source_priority"),
        },
        "updated_at": timestamp_iso,
    }

    response = client.table("reg_source_documents").upsert(data, on_conflict="document_hash").execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Upsert document source auditable n'a retourne aucune ligne.")
    return rows[0]


def upsert_document_chunk(
    client,
    source_document_id: str,
    seed_rule: dict[str, Any],
    timestamp_iso: str,
) -> dict[str, Any]:
    citation = primary_citation(seed_rule)
    quote = str(citation.get("quote") or seed_rule.get("description") or "")
    rule_key = str(seed_rule.get("rule_key") or "")
    chunk_index = safe_chunk_index(rule_key)
    chunk_hash = f"{citation.get('document_hash')}:{rule_key}:{chunk_index}"
    data = {
        "source_document_id": source_document_id,
        "chunk_index": chunk_index,
        "chunk_hash": chunk_hash,
        "text_excerpt": quote,
        "token_estimate": max(1, len(quote.split())),
        "page_number": citation.get("page_number"),
        "locator": citation.get("locator"),
        "metadata": {
            "rule_key": seed_rule.get("rule_key"),
            "synced_at": timestamp_iso,
        },
    }
    response = client.table("reg_document_chunks").upsert(data, on_conflict="chunk_hash").execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Upsert chunk document n'a retourne aucune ligne.")
    return rows[0]


def upsert_rule_candidate(
    client,
    seed_rule: dict[str, Any],
    extraction_run_id: str | None,
    source_document_id: str | None,
    timestamp_iso: str,
) -> dict[str, Any]:
    candidate = seed_rule.get("candidate") or {}
    candidate_key = str(candidate.get("candidate_key") or seed_rule.get("rule_key"))
    data = {
        "extraction_run_id": extraction_run_id,
        "source_document_id": source_document_id,
        "rule_key": seed_rule["rule_key"],
        "candidate_key": candidate_key,
        "rule_type": seed_rule["rule_type"],
        "activity_type": seed_rule.get("activity_type"),
        "constraint_type": seed_rule.get("constraint_type"),
        "title": seed_rule["title"],
        "description": seed_rule["description"],
        "extracted_payload": candidate.get("extracted_payload") or {
            "metric_type": seed_rule.get("metric_type"),
            "metric_value": seed_rule.get("metric_value"),
            "metric_unit": seed_rule.get("metric_unit"),
            "species_common_name": seed_rule.get("species_common_name"),
            "zone": seed_rule.get("zone"),
        },
        "status": candidate.get("status") or "candidate",
        "confidence_score": seed_rule.get("confidence_score"),
        "needs_manual_review": bool(seed_rule.get("needs_manual_review", False)),
        "quality_flags": seed_rule.get("quality_flags") or [],
        "ai_audit": seed_rule.get("ai_audit") or [],
        "updated_at": timestamp_iso,
    }
    response = client.table("reg_rule_candidates").upsert(data, on_conflict="candidate_key").execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Upsert candidat reglementaire n'a retourne aucune ligne.")
    return rows[0]


def upsert_rule(client, seed_rule: dict[str, Any], source_document_id: str, timestamp_iso: str) -> dict[str, Any]:
    source = seed_rule["source"]
    metadata = {
        "notes": seed_rule.get("notes"),
        "quality_flags": seed_rule.get("quality_flags") or [],
        "ai_audit": seed_rule.get("ai_audit") or [],
        "citations": seed_rule.get("citations") or [],
        "candidate_key": (seed_rule.get("candidate") or {}).get("candidate_key"),
        "confidence_source": seed_rule.get("confidence_source"),
        "confidence_reason": seed_rule.get("confidence_reason"),
    }

    data = {
        "rule_key": seed_rule["rule_key"],
        "rule_type": seed_rule["rule_type"],
        "title": seed_rule["title"],
        "description": seed_rule["description"],
        "legal_reference": seed_rule.get("legal_reference"),
        "metric_type": seed_rule.get("metric_type"),
        "metric_value": seed_rule.get("metric_value"),
        "metric_unit": seed_rule.get("metric_unit"),
        "species_common_name": seed_rule.get("species_common_name"),
        "species_scientific_name": seed_rule.get("species_scientific_name"),
        "source_document_id": source_document_id,
        "source_priority": source["source_priority"],
        "effective_date": source.get("effective_date"),
        "fetched_at": timestamp_iso,
        "checked_at": timestamp_iso,
        "needs_manual_review": bool(seed_rule.get("needs_manual_review", False)),
        "is_geospatial": True,
        "status": seed_rule.get("status") or ("needs_review" if seed_rule.get("needs_manual_review") else "published"),
        "confidence_score": seed_rule.get("confidence_score"),
        "valid_from": seed_rule.get("valid_from") or source.get("effective_date"),
        "valid_to": seed_rule.get("valid_to"),
        "published_at": seed_rule.get("published_at"),
        "activity_type": seed_rule.get("activity_type"),
        "constraint_type": seed_rule.get("constraint_type"),
        "metadata": metadata,
        "updated_at": timestamp_iso,
    }

    response = client.table("reg_rules").upsert(data, on_conflict="rule_key").execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Upsert regle n'a retourne aucune ligne.")
    return rows[0]


def resolve_rule_zone_bbox(seed_rule: dict[str, Any], spots_envelope: BBox, zones_envelope: BBox | None) -> BBox:
    zone = seed_rule.get("zone") or {}
    strategy = (zone.get("strategy") or "SPOTS_ENVELOPE").upper()

    if strategy == "SPOTS_ENVELOPE":
        return spots_envelope

    if strategy in {"ZONES_ENVELOPE", "APP_ZONES_UNION"}:
        if not zones_envelope and ALLOW_SPOTS_FALLBACK_FOR_ZONE_UNION:
            return spots_envelope
        if not zones_envelope:
            raise ValueError("La strategie zone exige des zones app avec bbox exploitable.")
        return zones_envelope

    lat_min = as_float(zone.get("lat_min"))
    lat_max = as_float(zone.get("lat_max"))
    lon_min = as_float(zone.get("lon_min"))
    lon_max = as_float(zone.get("lon_max"))

    if None in (lat_min, lat_max, lon_min, lon_max):
        raise ValueError(f"Zone invalide pour rule_key={seed_rule.get('rule_key')}: bbox absente.")

    return normalize_bbox(lat_min, lat_max, lon_min, lon_max)


def upsert_rule_zone(client, seed_rule: dict[str, Any], rule_id: str, bbox: BBox, timestamp_iso: str) -> dict[str, Any]:
    zone = seed_rule["zone"]

    data = {
        "rule_id": rule_id,
        "zone_code": zone["zone_code"],
        "zone_name": zone["zone_name"],
        "lat_min": bbox.lat_min,
        "lat_max": bbox.lat_max,
        "lon_min": bbox.lon_min,
        "lon_max": bbox.lon_max,
        "checked_at": timestamp_iso,
        "fetched_at": timestamp_iso,
        "needs_manual_review": bool(seed_rule.get("needs_manual_review", False)),
        "jurisdiction_level": zone.get("jurisdiction_level") or zone.get("strategy"),
        "geometry_geojson": zone.get("geometry_geojson"),
        "geometry_source_url": zone.get("geometry_source_url"),
        "metadata": {
            "strategy": zone.get("strategy")
        },
        "updated_at": timestamp_iso,
    }

    response = client.table("reg_rule_zones").upsert(data, on_conflict="rule_id,zone_code").execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Upsert reg_rule_zones n'a retourne aucune ligne.")
    return rows[0]


def upsert_rule_citations(
    client,
    seed_rule: dict[str, Any],
    rule_id: str,
    candidate_id: str | None,
    source_document_id: str | None,
) -> int:
    citations = seed_rule.get("citations") or [primary_citation(seed_rule)]
    payload: list[dict[str, Any]] = []
    for citation in citations:
        if not isinstance(citation, dict):
            continue
        quote = str(citation.get("quote") or "").strip()
        source_url = str(citation.get("source_url") or "")
        if not quote or not source_url:
            continue
        payload.append(
            {
                "rule_id": rule_id,
                "candidate_id": candidate_id,
                "source_document_id": source_document_id,
                "source_url": source_url,
                "source_title": citation.get("source_title"),
                "authority_name": citation.get("authority_name"),
                "quote": quote,
                "page_number": citation.get("page_number"),
                "locator": citation.get("locator"),
                "document_hash": citation.get("document_hash"),
                "confidence_score": citation.get("confidence_score"),
            }
        )
    if not payload:
        return 0
    client.table("reg_rule_citations").delete().eq("rule_id", rule_id).execute()
    client.table("reg_rule_citations").insert(payload).execute()
    return len(payload)


def upsert_species(client, species: dict[str, Any], timestamp_iso: str) -> dict[str, Any]:
    canonical_name = str(species.get("canonical_name") or species.get("common_name") or "").strip()
    if not canonical_name:
        raise ValueError("Espece sans nom canonique.")
    data = {
        "canonical_name": canonical_name,
        "scientific_name": species.get("scientific_name"),
        "taxon_group": species.get("taxon_group"),
        "aliases": species.get("aliases") or [canonical_name],
        "external_ids": species.get("external_ids") or {},
        "updated_at": timestamp_iso,
    }
    response = client.table("reg_species").upsert(data, on_conflict="canonical_name").execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Upsert espece n'a retourne aucune ligne.")
    return rows[0]


def sync_rule_species(client, seed_rule: dict[str, Any], rule_id: str, timestamp_iso: str) -> int:
    count = 0
    for species in seed_rule.get("species") or []:
        if not isinstance(species, dict):
            continue
        species_row = upsert_species(client, species, timestamp_iso)
        client.table("reg_rule_species").upsert(
            {
                "rule_id": rule_id,
                "species_id": species_row["id"],
                "relation_type": "applies_to",
            },
            on_conflict="rule_id,species_id,relation_type",
        ).execute()
        count += 1
    return count


def sync_zone_assignments(
    client,
    zone_items: list[dict[str, Any]],
    rule_row: dict[str, Any],
    reg_zone_row: dict[str, Any],
    source_url: str,
    source_priority: int,
    timestamp_iso: str,
    needs_manual_review: bool,
) -> set[str]:
    rule_bbox = normalize_bbox(
        float(reg_zone_row["lat_min"]),
        float(reg_zone_row["lat_max"]),
        float(reg_zone_row["lon_min"]),
        float(reg_zone_row["lon_max"]),
    )

    matched_zone_ids: set[str] = set()
    payload: list[dict[str, Any]] = []

    for item in zone_items:
        if bbox_overlap(item["bbox"], rule_bbox):
            zone_id = item["id"]
            matched_zone_ids.add(zone_id)
            payload.append(
                {
                    "app_zone_id": zone_id,
                    "rule_id": rule_row["id"],
                    "reg_zone_id": reg_zone_row["id"],
                    "source_url": source_url,
                    "source_priority": source_priority,
                    "match_type": "bbox_overlap",
                    "assigned_at": timestamp_iso,
                    "checked_at": timestamp_iso,
                    "fetched_at": timestamp_iso,
                    "needs_manual_review": needs_manual_review,
                    "metadata": {
                        "zone_name": item.get("name")
                    },
                    "updated_at": timestamp_iso,
                }
            )

    if payload:
        client.table("reg_zone_assignments").upsert(payload, on_conflict="app_zone_id,rule_id").execute()

    existing = client.table("reg_zone_assignments").select("id,app_zone_id").eq("rule_id", rule_row["id"]).limit(ROW_FETCH_LIMIT).execute()
    to_delete = [r["id"] for r in (existing.data or []) if str(r.get("app_zone_id")) not in matched_zone_ids]
    if to_delete:
        client.table("reg_zone_assignments").delete().in_("id", to_delete).execute()

    return matched_zone_ids


def sync_spot_assignments(
    client,
    spot_items: list[dict[str, Any]],
    matched_zone_ids: set[str],
    rule_row: dict[str, Any],
    reg_zone_row: dict[str, Any],
    source_url: str,
    source_priority: int,
    timestamp_iso: str,
    needs_manual_review: bool,
) -> int:
    rule_bbox = normalize_bbox(
        float(reg_zone_row["lat_min"]),
        float(reg_zone_row["lat_max"]),
        float(reg_zone_row["lon_min"]),
        float(reg_zone_row["lon_max"]),
    )

    matched_spot_ids: list[str] = []
    payload: list[dict[str, Any]] = []

    for spot in spot_items:
        spot_id = spot["id"]
        zone_id = spot.get("zone_id")

        if zone_id and zone_id in matched_zone_ids:
            match_type = "zone_id_link"
        elif bbox_overlap(spot["bbox"], rule_bbox):
            match_type = "bbox_overlap"
        else:
            continue

        matched_spot_ids.append(spot_id)
        payload.append(
            {
                "spot_id": spot_id,
                "rule_id": rule_row["id"],
                "reg_zone_id": reg_zone_row["id"],
                "app_zone_id": zone_id,
                "source_url": source_url,
                "source_priority": source_priority,
                "match_type": match_type,
                "assigned_at": timestamp_iso,
                "checked_at": timestamp_iso,
                "fetched_at": timestamp_iso,
                "needs_manual_review": needs_manual_review,
                "metadata": {
                    "spot_name": spot.get("name")
                },
                "updated_at": timestamp_iso,
            }
        )

    if payload:
        client.table("reg_spot_assignments").upsert(payload, on_conflict="spot_id,rule_id").execute()

    existing = client.table("reg_spot_assignments").select("id,spot_id").eq("rule_id", rule_row["id"]).limit(ROW_FETCH_LIMIT).execute()
    to_delete = [r["id"] for r in (existing.data or []) if str(r.get("spot_id")) not in matched_spot_ids]
    if to_delete:
        client.table("reg_spot_assignments").delete().in_("id", to_delete).execute()

    return len(payload)


def create_run(client, started_at: str, metadata: dict[str, Any]) -> str:
    response = client.table("reg_assignment_runs").insert({"status": "RUNNING", "started_at": started_at, "metadata": metadata}).execute()
    rows = response.data or []
    if not rows:
        raise RuntimeError("Creation du run impossible.")
    return str(rows[0]["id"])


def finalize_run(client, run_id: str, payload: dict[str, Any]) -> None:
    client.table("reg_assignment_runs").update(payload).eq("id", run_id).execute()


def main() -> None:
    from pscripts.supabase_client import get_supabase

    client = get_supabase()
    started_at = now_utc_iso()

    metadata = {
        "spots_table": SPOTS_TABLE,
        "zones_table": ZONES_TABLE,
        "seed_file": str(SEED_PATH),
        "candidates_file": str(CANDIDATES_PATH),
        "source_documents_file": str(SOURCE_DOCUMENTS_PATH),
        "audit_model_enabled": ENABLE_AUDIT_MODEL,
    }

    run_id = create_run(client, started_at, metadata)
    extraction_run_id: str | None = None
    warning_count = 0

    try:
        seed_rules = load_seed_rules()
        rule_candidates = load_optional_json_list(CANDIDATES_PATH)
        source_documents = load_optional_json_list(SOURCE_DOCUMENTS_PATH)
        if ENABLE_AUDIT_MODEL:
            extraction_run_id = create_extraction_run(
                client,
                started_at,
                seed_rules,
                source_documents,
                rule_candidates,
                metadata,
            )

        spot_rows = fetch_table_rows(client, SPOTS_TABLE)
        spot_items = to_spot_items(spot_rows)
        if not spot_items:
            raise ValueError(f"Aucun spot exploitable dans '{SPOTS_TABLE}' (bbox introuvable).")

        zone_items: list[dict[str, Any]] = []
        if ENABLE_ZONES:
            try:
                zone_rows = fetch_table_rows(client, ZONES_TABLE)
                zone_items = to_zone_items(zone_rows)
                if zone_rows and not zone_items:
                    warning_count += 1
                    print(f"[WARN] Zones trouvees dans '{ZONES_TABLE}' mais polygon/bbox non exploitable.")
            except Exception as exc:
                warning_count += 1
                print(f"[WARN] Lecture zones ignoree ({ZONES_TABLE}): {exc}")

        spots_envelope = compute_entities_envelope(spot_items)
        zones_envelope = compute_entities_envelope(zone_items) if zone_items else None

        timestamp_iso = now_utc_iso()
        rules_count = 0
        spot_assignments_count = 0
        zone_assignments_count = 0
        citation_count = 0
        species_link_count = 0

        for seed_rule in seed_rules:
            audit_document_row: dict[str, Any] | None = None
            candidate_row: dict[str, Any] | None = None
            if ENABLE_AUDIT_MODEL:
                audit_document_row = upsert_audit_source_document(
                    client,
                    seed_rule,
                    extraction_run_id,
                    timestamp_iso,
                )
                upsert_document_chunk(client, str(audit_document_row["id"]), seed_rule, timestamp_iso)
                candidate_row = upsert_rule_candidate(
                    client,
                    seed_rule,
                    extraction_run_id,
                    str(audit_document_row["id"]),
                    timestamp_iso,
                )

            source_row = upsert_source_document(client, seed_rule, timestamp_iso)
            rule_row = upsert_rule(client, seed_rule, str(source_row["id"]), timestamp_iso)

            if ENABLE_AUDIT_MODEL:
                citation_count += upsert_rule_citations(
                    client,
                    seed_rule,
                    str(rule_row["id"]),
                    str(candidate_row["id"]) if candidate_row else None,
                    str(audit_document_row["id"]) if audit_document_row else None,
                )
                species_link_count += sync_rule_species(client, seed_rule, str(rule_row["id"]), timestamp_iso)

            rule_bbox = resolve_rule_zone_bbox(seed_rule, spots_envelope, zones_envelope)
            reg_zone_row = upsert_rule_zone(client, seed_rule, str(rule_row["id"]), rule_bbox, timestamp_iso)

            matched_zone_ids: set[str] = set()
            if zone_items:
                matched_zone_ids = sync_zone_assignments(
                    client,
                    zone_items,
                    rule_row,
                    reg_zone_row,
                    str(source_row["source_url"]),
                    int(source_row["source_priority"]),
                    timestamp_iso,
                    bool(seed_rule.get("needs_manual_review", False)),
                )

            assigned_spots = sync_spot_assignments(
                client,
                spot_items,
                matched_zone_ids,
                rule_row,
                reg_zone_row,
                str(source_row["source_url"]),
                int(source_row["source_priority"]),
                timestamp_iso,
                bool(seed_rule.get("needs_manual_review", False)),
            )

            rules_count += 1
            spot_assignments_count += assigned_spots
            zone_assignments_count += len(matched_zone_ids)

            print(
                f"[OK] rule={seed_rule['rule_key']} zones={len(matched_zone_ids)} spots={assigned_spots} reg_zone={reg_zone_row['zone_code']}"
            )

        finalize_run(
            client,
            run_id,
            {
                "status": "SUCCESS",
                "finished_at": now_utc_iso(),
                "spots_count": len(spot_items),
                "zones_count": len(zone_items),
                "rules_count": rules_count,
                "spot_assignments_count": spot_assignments_count,
                "zone_assignments_count": zone_assignments_count,
                "warning_count": warning_count,
                "metadata": {
                    **metadata,
                    "citation_count": citation_count,
                    "species_link_count": species_link_count,
                    "extraction_run_id": extraction_run_id,
                },
            },
        )
        if ENABLE_AUDIT_MODEL and extraction_run_id:
            finalize_extraction_run(
                client,
                extraction_run_id,
                {
                    "status": "SUCCESS",
                    "finished_at": now_utc_iso(),
                    "published_rule_count": rules_count,
                    "metadata": {
                        **metadata,
                        "citation_count": citation_count,
                        "species_link_count": species_link_count,
                    },
                },
            )

    except Exception as exc:
        if ENABLE_AUDIT_MODEL and extraction_run_id:
            finalize_extraction_run(
                client,
                extraction_run_id,
                {
                    "status": "FAILED",
                    "finished_at": now_utc_iso(),
                    "error_message": str(exc),
                },
            )
        finalize_run(
            client,
            run_id,
            {
                "status": "FAILED",
                "finished_at": now_utc_iso(),
                "warning_count": warning_count,
                "error_message": str(exc),
            },
        )
        raise


if __name__ == "__main__":
    main()
