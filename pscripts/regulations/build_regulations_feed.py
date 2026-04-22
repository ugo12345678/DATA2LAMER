from __future__ import annotations

import html as html_lib
import hashlib
import json
import os
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

import requests

SOURCE_CATALOG_PATH = Path(os.environ.get("REG_SOURCE_CATALOG_FILE", "data/regulations/source_endpoints.json"))
OUTPUT_RULES_PATH = Path(os.environ.get("REG_GENERATED_RULES_FILE", "data/regulations/generated_rules.json"))
STATIC_LEGIFRANCE_RULES_PATH = Path(
    os.environ.get("REG_STATIC_LEGIFRANCE_RULES_FILE", "data/regulations/static_legifrance_rules.json")
)
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REG_REQUEST_TIMEOUT_SECONDS", "25"))
MAX_LINKED_HTML_PER_SOURCE = int(os.environ.get("REG_MAX_LINKED_HTML_PER_SOURCE", "10"))
MAX_PDF_LINKS_PER_PAGE = int(os.environ.get("REG_MAX_PDF_LINKS_PER_PAGE", "12"))
ENABLE_PDF_OCR = os.environ.get("REG_ENABLE_PDF_OCR", "true").lower() == "true"
OCR_MIN_TEXT_CHARS = int(os.environ.get("REG_OCR_MIN_TEXT_CHARS", "900"))
OCR_MAX_PAGES = int(os.environ.get("REG_OCR_MAX_PAGES", "8"))
OCR_LANG = os.environ.get("REG_OCR_LANG", "fra+eng")

CRUSTACEAN_KEYWORDS = (
    "homard",
    "araignee",
    "tourteau",
    "langouste",
    "langoustine",
    "crabe",
    "etrille",
)
RELEVANT_SOURCE_KEYWORDS = (
    "peche",
    "loisir",
    "reglement",
    "capture",
    "taille",
    "quota",
    "bar",
    "lieu",
    "maquereau",
    "thon",
    "espadon",
    "merou",
    "corb",
    "coquillage",
    "crustace",
    "oursin",
    "sous-marine",
    "plongee",
    "calanques",
    "golfe",
)
SPECIES_ALIASES: dict[str, tuple[str, ...]] = {
    "bar": ("bar europeen", "bar"),
    "lieu jaune": ("lieu jaune",),
    "maquereau": ("maquereau",),
    "araignees": ("araignees de mer", "araignee de mer", "araignees", "araignee"),
    "pouces-pieds": ("pouces-pieds", "pouces pieds"),
    "coquilles saint-jacques": ("coquilles saint-jacques", "coquille saint-jacques"),
    "ormeaux": ("ormeaux", "ormeau"),
    "homard": ("homards", "homard"),
    "tourteau": ("tourteaux", "tourteau"),
    "thon rouge": ("thon rouge",),
    "dorade rose": ("dorade rose",),
    "dorade coryphene": ("dorade coryphene", "dorade coryphene"),
    "merou": ("merou", "merous"),
    "corb": ("corb",),
    "raie brunette": ("raie brunette",),
    "espadon": ("espadon",),
    "denti": ("denti",),
}
INVALID_SPECIES_NAMES = {
    "autres zones",
    "coquillages",
    "crustaces",
    "poissons",
    "attention",
    "cf",
    "reglementation",
    "especes sensibles",
}
QUOTA_SPECIES_STOPWORDS = {
    "captures",
    "capture",
    "limitee",
    "limitees",
    "limites",
    "quantite",
    "totale",
    "autorisee",
    "superieure",
    "inferieure",
    "pecheur",
    "navire",
    "jour",
    "specimens",
    "specimen",
    "unites",
    "kg",
    "danatifes",
}
DEFAULT_ZONE = {
    "zone_code": "APP_ZONES_UNION",
    "zone_name": "Union geospatiale des zones de l'application",
    "strategy": "APP_ZONES_UNION",
}
GEO_ZONE_PROFILES: dict[str, dict[str, Any]] = {
    "france": DEFAULT_ZONE,
    "namo": {
        "zone_code": "FACADE_NAMO",
        "zone_name": "Facade Nord Atlantique Manche Ouest",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 46.0,
        "lat_max": 51.4,
        "lon_min": -6.8,
        "lon_max": 0.2,
    },
    "memn": {
        "zone_code": "FACADE_MEMN",
        "zone_name": "Facade Manche Est Mer du Nord",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 48.2,
        "lat_max": 51.5,
        "lon_min": -2.0,
        "lon_max": 4.2,
    },
    "sud-atlantique": {
        "zone_code": "FACADE_SUD_ATLANTIQUE",
        "zone_name": "Facade Sud Atlantique",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 42.9,
        "lat_max": 47.4,
        "lon_min": -2.5,
        "lon_max": -0.8,
    },
    "mediterranee": {
        "zone_code": "FACADE_MEDITERRANEE",
        "zone_name": "Facade Mediterranee",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 41.0,
        "lat_max": 44.9,
        "lon_min": 2.2,
        "lon_max": 9.9,
    },
    "bretagne": {
        "zone_code": "REGION_BRETAGNE",
        "zone_name": "Bretagne",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 47.0,
        "lat_max": 49.2,
        "lon_min": -5.8,
        "lon_max": -1.0,
    },
    "pays-de-la-loire": {
        "zone_code": "REGION_PAYS_DE_LA_LOIRE",
        "zone_name": "Pays de la Loire",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 46.0,
        "lat_max": 47.9,
        "lon_min": -3.0,
        "lon_max": -0.7,
    },
    "golfe-du-lion": {
        "zone_code": "SECTEUR_GOLFE_DU_LION",
        "zone_name": "Golfe du Lion",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 42.2,
        "lat_max": 43.7,
        "lon_min": 2.5,
        "lon_max": 5.1,
    },
    "calanques": {
        "zone_code": "SECTEUR_CALANQUES",
        "zone_name": "Parc national des Calanques",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 43.0,
        "lat_max": 43.35,
        "lon_min": 5.25,
        "lon_max": 5.6,
    },
    "corse": {
        "zone_code": "SECTEUR_CORSE",
        "zone_name": "Corse",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 41.2,
        "lat_max": 43.2,
        "lon_min": 8.4,
        "lon_max": 9.7,
    },
    "north-48n": {
        "zone_code": "SECTEUR_NORD_48N",
        "zone_name": "Nord du parallele 48N",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 48.0,
        "lat_max": 51.5,
        "lon_min": -6.8,
        "lon_max": 4.2,
    },
    "south-48n": {
        "zone_code": "SECTEUR_SUD_48N",
        "zone_name": "Sud du parallele 48N",
        "strategy": "CUSTOM_BBOX",
        "lat_min": 41.0,
        "lat_max": 48.0,
        "lon_min": -6.8,
        "lon_max": 9.9,
    },
}


@dataclass
class SourceRecord:
    source_id: str
    source_type: str
    source_priority: int
    authority_name: str
    title: str
    url: str
    kind: str


@dataclass(frozen=True)
class SourceDocument:
    source: SourceRecord
    url: str
    text: str


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_date_str(value: datetime) -> str:
    return value.date().isoformat()


def slugify(text: str) -> str:
    ascii_text = unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("ascii")
    ascii_text = ascii_text.lower()
    ascii_text = re.sub(r"[^a-z0-9]+", "-", ascii_text)
    return ascii_text.strip("-")


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def fold_text(text: str) -> str:
    no_accent = unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("ascii")
    return normalize_spaces(no_accent).replace("’", "'").lower()


def html_to_text(raw_html: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", raw_html)
    return normalize_spaces(html_lib.unescape(without_tags))


def short_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]


def canonicalize_url(url: str) -> str:
    return urldefrag(url.strip())[0]


def is_official_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host.endswith(".gouv.fr")


def source_scope_code(source: SourceRecord) -> str:
    source_marker = fold_text(f"{source.source_id} {source.title} {source.url}")
    if source.source_type in {"LEGIFRANCE", "MINISTERE_MER"}:
        return "france"
    if "namo" in source_marker or "nord-atlantique-manche-ouest" in source_marker:
        return "namo"
    if (
        "memn" in source_marker
        or "manche est mer du nord" in source_marker
        or "manche-est-mer-du-nord" in source_marker
        or "mer du nord" in source_marker
        or "mer-du-nord" in source_marker
    ):
        return "memn"
    if "mediterranee" in source_marker:
        return "mediterranee"
    if "sud-atlantique" in source_marker or "sud-atlantique" in source.source_id:
        return "sud-atlantique"
    return slugify(source.source_id)


def rule_scope_for_text(source: SourceRecord, text: str) -> str:
    scope = detect_scope_in_text(text)
    if scope != "general":
        return scope
    return source_scope_code(source)


def resolve_rule_zone(source: SourceRecord, text: str) -> dict[str, Any]:
    scope = rule_scope_for_text(source, text)
    profile = GEO_ZONE_PROFILES.get(scope)
    if profile:
        return dict(profile)
    fallback = GEO_ZONE_PROFILES.get(source_scope_code(source))
    if fallback:
        return dict(fallback)
    return dict(DEFAULT_ZONE)


def load_source_catalog(path: Path) -> list[SourceRecord]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    out: list[SourceRecord] = []
    for item in payload:
        out.append(
            SourceRecord(
                source_id=item["id"],
                source_type=item["source_type"],
                source_priority=int(item["source_priority"]),
                authority_name=item["authority_name"],
                title=item["title"],
                url=item["url"],
                kind=item["kind"],
            )
        )
    return out


def load_static_legifrance_rules(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


def fetch_url_text(url: str, retries: int = 3) -> str:
    headers = {"User-Agent": "regulations-sync-bot/1.0"}
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            response.encoding = response.encoding or "utf-8"
            return response.text
        except Exception as exc:  # pragma: no cover - network variability
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(f"Echec HTTP sur {url}: {last_error}")


def fetch_url_bytes(url: str, retries: int = 3) -> bytes:
    headers = {"User-Agent": "regulations-sync-bot/1.0"}
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.content
        except Exception as exc:  # pragma: no cover - network variability
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(f"Echec HTTP binaire sur {url}: {last_error}")


def extract_pdf_urls_from_html(html: str, base_url: str, limit: int | None = None) -> list[str]:
    candidates = re.findall(r"href=[\"']([^\"']+\.pdf(?:\?[^\"']*)?)[\"']", html, flags=re.IGNORECASE)
    urls: list[str] = []
    for candidate in candidates:
        absolute_url = canonicalize_url(urljoin(base_url, candidate))
        urls.append(absolute_url)

    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)

    def score(url: str) -> tuple[int, int]:
        lowered = fold_text(url)
        keyword_bonus = sum(1 for keyword in RELEVANT_SOURCE_KEYWORDS if keyword in lowered)
        if "annexe" in lowered:
            keyword_bonus += 2
        return (-keyword_bonus, len(url))

    deduped.sort(key=score)
    if limit is not None:
        return deduped[: max(0, limit)]
    return deduped


def extract_pdf_url_from_html(html: str, base_url: str) -> str:
    candidates = extract_pdf_urls_from_html(html, base_url, limit=1)
    if not candidates:
        raise ValueError("Aucun lien PDF detecte dans la page DIRM.")
    return candidates[0]


def extract_relevant_html_links_from_html(html: str, base_url: str, limit: int) -> list[str]:
    base_host = urlparse(base_url).netloc.lower()
    hrefs = re.findall(r"href=[\"']([^\"']+)[\"']", html, flags=re.IGNORECASE)
    candidates: list[str] = []

    for href in hrefs:
        absolute_url = canonicalize_url(urljoin(base_url, href))
        parsed = urlparse(absolute_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc.lower() != base_host:
            continue
        if absolute_url.lower().endswith(".pdf"):
            continue
        lowered = fold_text(f"{parsed.path} {parsed.query}")
        if any(keyword in lowered for keyword in RELEVANT_SOURCE_KEYWORDS):
            candidates.append(absolute_url)
            continue
        if re.search(r"-a\d+\.html$", parsed.path):
            candidates.append(absolute_url)

    deduped: list[str] = []
    seen: set[str] = {canonicalize_url(base_url)}
    for url in candidates:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)

    def score(url: str) -> tuple[int, int]:
        lowered = fold_text(url)
        keyword_count = sum(1 for keyword in RELEVANT_SOURCE_KEYWORDS if keyword in lowered)
        return (-keyword_count, len(url))

    deduped.sort(key=score)
    return deduped[: max(0, limit)]


def extract_pdf_text(pdf_bytes: bytes) -> str:
    from pypdf import PdfReader

    reader = PdfReader(BytesIO(pdf_bytes))
    chunks: list[str] = []
    for page in reader.pages:
        chunks.append(page.extract_text() or "")
    return "\n".join(chunks)


def should_try_pdf_ocr(
    extracted_text: str,
    *,
    enable_ocr: bool = ENABLE_PDF_OCR,
    min_chars: int = OCR_MIN_TEXT_CHARS,
) -> bool:
    if not enable_ocr:
        return False
    return len(normalize_spaces(extracted_text)) < min_chars


def extract_pdf_text_with_optional_ocr(pdf_bytes: bytes, source_url: str) -> str:
    extracted_text = extract_pdf_text(pdf_bytes)
    if not should_try_pdf_ocr(extracted_text):
        return extracted_text

    try:
        from pdf2image import convert_from_bytes
        import pytesseract
    except Exception as exc:
        print(f"[WARN] OCR indisponible (deps manquantes) url={source_url}: {exc}")
        return extracted_text

    try:
        images = convert_from_bytes(
            pdf_bytes,
            dpi=240,
            first_page=1,
            last_page=max(1, OCR_MAX_PAGES),
            fmt="png",
        )
    except Exception as exc:
        print(f"[WARN] OCR impossible (conversion PDF->image) url={source_url}: {exc}")
        return extracted_text

    ocr_chunks: list[str] = []
    for image in images[: max(1, OCR_MAX_PAGES)]:
        try:
            chunk = pytesseract.image_to_string(image, lang=OCR_LANG, config="--psm 6")
        except Exception as exc:
            print(f"[WARN] OCR page en echec url={source_url}: {exc}")
            continue
        if normalize_spaces(chunk):
            ocr_chunks.append(chunk)

    if not ocr_chunks:
        return extracted_text

    ocr_text = "\n".join(ocr_chunks)
    extracted_len = len(normalize_spaces(extracted_text))
    ocr_len = len(normalize_spaces(ocr_text))
    if ocr_len <= extracted_len:
        return extracted_text

    print(
        f"[INFO] OCR appliquee url={source_url} "
        f"chars_before={extracted_len} chars_after={ocr_len}"
    )
    if extracted_len == 0:
        return ocr_text
    return f"{extracted_text}\n{ocr_text}"


def find_sentence(text: str, required_tokens: list[str]) -> str | None:
    cleaned = normalize_spaces(text).replace("’", "'")
    sentences = re.split(r"(?<=[;:!?])\s+", cleaned)
    if cleaned not in sentences:
        sentences.insert(0, cleaned)

    for sentence in sentences:
        lowered = fold_text(sentence)
        if all(fold_text(token) in lowered for token in required_tokens):
            return sentence.strip()
    return None


def parse_french_number(token: str) -> float | None:
    token_norm = fold_text(token).replace("-", " ").strip()
    word_map = {
        "zero": 0,
        "un": 1,
        "une": 1,
        "deux": 2,
        "trois": 3,
        "quatre": 4,
        "cinq": 5,
        "six": 6,
        "sept": 7,
        "huit": 8,
        "neuf": 9,
        "dix": 10,
        "onze": 11,
        "douze": 12,
        "treize": 13,
        "quatorze": 14,
        "quinze": 15,
        "seize": 16,
    }

    if token_norm in word_map:
        return float(word_map[token_norm])

    numeric = re.sub(r"[^\d,\.]", "", token_norm)
    if numeric:
        try:
            return float(numeric.replace(",", "."))
        except ValueError:
            return None

    return None


def normalize_species_name(raw_name: str) -> str:
    name = fold_text(raw_name)
    name = re.sub(r"\([^)]*\)", " ", name)
    name = re.sub(r"^(de|du|des|d')\s+", "", name)
    name = re.sub(r"^(la|le|les|l')\s+", "", name)
    name = re.sub(r"^(peche|peche sous marine)\s+(de|du|des)\s+", "", name)
    name = re.sub(r"\b(par|pour|en)\b.*$", "", name).strip()
    return normalize_spaces(name)


def detect_species_in_text(text: str) -> str | None:
    lowered = fold_text(text)
    for canonical, aliases in SPECIES_ALIASES.items():
        ordered_aliases = sorted(aliases, key=len, reverse=True)
        for alias in ordered_aliases:
            if re.search(rf"\b{re.escape(fold_text(alias))}\b", lowered):
                return canonical
    return None


def detect_all_species_in_text(text: str) -> list[str]:
    lowered = fold_text(text)
    out: list[str] = []
    for canonical, aliases in SPECIES_ALIASES.items():
        ordered_aliases = sorted(aliases, key=len, reverse=True)
        if any(re.search(rf"\b{re.escape(fold_text(alias))}\b", lowered) for alias in ordered_aliases):
            out.append(canonical)
    return sorted(out)


def canonical_species_name(raw_text: str, context_text: str | None = None) -> str | None:
    candidate = detect_species_in_text(raw_text)
    if candidate:
        return candidate

    normalized = normalize_species_name(raw_text)
    if not normalized:
        return None
    if normalized in INVALID_SPECIES_NAMES:
        return None

    if context_text:
        context_candidate = detect_species_in_text(context_text)
        if context_candidate:
            return context_candidate

    return normalized


def is_plausible_species_name(name: str) -> bool:
    normalized = fold_text(name)
    if normalized == "toutes especes marines":
        return True
    words = [word for word in re.split(r"[\s\-]+", normalized) if word]
    if not words:
        return False
    if len(words) > 4:
        return False
    if any(any(char.isdigit() for char in word) for word in words):
        return False
    if any(word in QUOTA_SPECIES_STOPWORDS for word in words):
        return False
    return True


def detect_scope_in_text(text: str) -> str:
    lowered = fold_text(text)
    if "nord du parallele 48" in lowered:
        return "north-48n"
    if "sud du parallele 48" in lowered:
        return "south-48n"
    if "bretagne" in lowered:
        return "bretagne"
    if "pays de la loire" in lowered:
        return "pays-de-la-loire"
    if "manche est" in lowered or "mer du nord" in lowered:
        return "memn"
    if "mediterranee" in lowered:
        return "mediterranee"
    if "sud-atlantique" in lowered or "sud atlantique" in lowered:
        return "sud-atlantique"
    if "golfe du lion" in lowered:
        return "golfe-du-lion"
    if "calanques" in lowered:
        return "calanques"
    if "corse" in lowered:
        return "corse"
    return "general"


def build_base_rule(
    *,
    rule_key: str,
    rule_type: str,
    title: str,
    description: str,
    source: SourceRecord,
    source_url: str,
    legal_reference: str | None,
    metric_type: str | None,
    metric_value: float | int | None,
    metric_unit: str | None,
    species_common_name: str | None,
    species_scientific_name: str | None,
    needs_manual_review: bool,
    notes: str,
    zone: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fetched_at = now_utc()
    return {
        "rule_key": rule_key,
        "rule_type": rule_type,
        "title": title,
        "description": description,
        "legal_reference": legal_reference,
        "metric_type": metric_type,
        "metric_value": metric_value,
        "metric_unit": metric_unit,
        "species_common_name": species_common_name,
        "species_scientific_name": species_scientific_name,
        "source": {
            "source_type": source.source_type,
            "source_priority": source.source_priority,
            "authority_name": source.authority_name,
            "source_url": source_url,
            "title": source.title,
            "effective_date": to_date_str(fetched_at),
        },
        "zone": dict(zone or DEFAULT_ZONE),
        "needs_manual_review": needs_manual_review,
        "notes": notes,
    }


def parse_legifrance_spearfishing_rules(source: SourceRecord, html: str) -> list[dict[str, Any]]:
    sentence = find_sentence(
        html,
        ["peche", "sous", "marine", "fusil", "harpon", "moins", "ans"],
    )
    if not sentence:
        return []

    age_match = re.search(r"moins de\s*(\d+|seize)\s*ans", sentence, flags=re.IGNORECASE)
    age_value: int | None = None
    if age_match:
        raw = age_match.group(1).lower()
        age_value = 16 if raw == "seize" else int(raw)

    return [
        build_base_rule(
            rule_key="spearfishing.fr.min-age.fusil-harpon",
            rule_type="SPEARFISHING_GENERAL",
            title="Age minimum pour la peche sous-marine au fusil-harpon",
            description=sentence,
            source=source,
            source_url=source.url,
            legal_reference="Code rural et de la peche maritime - Article R921-90",
            metric_type="AGE_MIN_YEARS" if age_value is not None else None,
            metric_value=age_value,
            metric_unit="ans" if age_value is not None else None,
            species_common_name=None,
            species_scientific_name=None,
            needs_manual_review=False,
            notes="Regle generale issue de Legifrance.",
            zone=resolve_rule_zone(source, sentence),
        )
    ]


def parse_legifrance_diving_rules(source: SourceRecord, html: str) -> list[dict[str, Any]]:
    sentence = find_sentence(
        html,
        ["etablissements", "pratique", "plongee", "subaquatique"],
    )
    if not sentence:
        return []

    return [
        build_base_rule(
            rule_key="diving.fr.code-sport.section-a322-71",
            rule_type="DIVING_GENERAL",
            title="Cadre de securite des etablissements de plongee subaquatique",
            description=sentence,
            source=source,
            source_url=source.url,
            legal_reference="Code du sport - Article A322-71",
            metric_type=None,
            metric_value=None,
            metric_unit=None,
            species_common_name=None,
            species_scientific_name=None,
            needs_manual_review=False,
            notes="Regle generale de plongee issue de Legifrance.",
            zone=resolve_rule_zone(source, sentence),
        )
    ]


def parse_ministere_spearfishing_rules(
    source: SourceRecord,
    html: str,
    source_url: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[tuple[str, str]] = []
    normalized = fold_text(html)

    if "ne pas utiliser d'equipement respiratoire" in normalized:
        clauses.append(
            (
                "Interdiction d'utiliser un equipement respiratoire en peche sous-marine",
                "Ne pas utiliser d'equipement respiratoire.",
            )
        )

    if "avoir plus de 16 ans" in normalized:
        clauses.append(
            (
                "Age minimum en peche sous-marine",
                "Avoir plus de 16 ans et une bonne condition physique.",
            )
        )

    if "ne pas pratiquer la peche sous-marine la nuit" in normalized:
        clauses.append(
            (
                "Interdiction de la peche sous-marine de nuit",
                "Ne pas pratiquer la peche sous-marine la nuit.",
            )
        )

    effective_source_url = source_url or source.url
    rules: list[dict[str, Any]] = []
    for title, desc in clauses:
        key = f"spearfishing.fr.ministere.{slugify(title)}"
        rules.append(
            build_base_rule(
                rule_key=key,
                rule_type="SPEARFISHING_GENERAL",
                title=title,
                description=desc,
                source=source,
                source_url=effective_source_url,
                legal_reference="Page ministerielle peche de loisir en mer",
                metric_type=None,
                metric_value=None,
                metric_unit=None,
                species_common_name=None,
                species_scientific_name=None,
                needs_manual_review=True,
                notes="Source operationnelle ministerielle, verification manuelle recommandee.",
                zone=resolve_rule_zone(source, desc),
            )
        )

    return rules


def extract_dirm_size_rules(source: SourceRecord, source_url: str, text: str) -> list[dict[str, Any]]:
    lines = [normalize_spaces(line) for line in text.splitlines() if normalize_spaces(line)]
    rules: list[dict[str, Any]] = []

    patterns = [
        re.compile(
            r"^(?P<name>[A-Za-zÀ-ÿ'’\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+(?:[\.,]\d+)?)\s*cm\b",
            flags=re.IGNORECASE,
        ),
        re.compile(
            r"^(?P<name>[A-Za-zÀ-ÿ'’\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s+(?P<value>\d+(?:[\.,]\d+)?)\s*cm\b",
            flags=re.IGNORECASE,
        ),
    ]

    seen: set[str] = set()
    for line in lines:
        for pattern in patterns:
            match = pattern.search(line)
            if not match:
                continue

            species_name = canonical_species_name(match.group("name"), context_text=line)
            if not species_name:
                continue

            value = parse_french_number(match.group("value"))
            if value is None:
                continue
            scientific = normalize_spaces(match.group("scientific")) if match.group("scientific") else None
            scope = rule_scope_for_text(source, line)
            key = f"species.{slugify(species_name)}.min-size.{slugify(scope)}"
            if key in seen:
                continue
            seen.add(key)

            rules.append(
                build_base_rule(
                    rule_key=key,
                    rule_type="MIN_SIZE",
                    title=f"Taille minimale de capture - {species_name}",
                    description=line,
                    source=source,
                    source_url=source_url,
                    legal_reference="Arrete du 26 octobre 2012 modifie (rappels DIRM)",
                    metric_type="SIZE_MIN_CM",
                    metric_value=float(value),
                    metric_unit="cm",
                    species_common_name=species_name,
                    species_scientific_name=scientific,
                    needs_manual_review=True,
                    notes="Extraction automatique depuis document operationnel, validation manuelle requise.",
                    zone=resolve_rule_zone(source, line),
                )
            )

    return rules


def extract_dirm_quota_rules(source: SourceRecord, source_url: str, text: str) -> list[dict[str, Any]]:
    lines = [normalize_spaces(line) for line in text.splitlines() if normalize_spaces(line)]
    rules: list[dict[str, Any]] = []
    flat_text = fold_text(text)
    has_daily_context = (
        "par pecheur et par jour" in flat_text
        or "par navire et par jour" in flat_text
        or "quantite maxi de peche autorisee par pecheur et par jour" in flat_text
    )

    structured_patterns = [
        (
            re.compile(
                r"^(?P<name>[A-Za-zÀ-ÿ'’\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+)\s*(?:captures?|specimens?|unites?)\s*(?:par|/)\s*jour",
                flags=re.IGNORECASE,
            ),
            False,
            False,
        ),
        (
            re.compile(
                r"^(?P<name>[A-Za-zÀ-ÿ'’\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+(?:[\.,]\d+)?)\s*kg\s*(?:par|/)\s*jour",
                flags=re.IGNORECASE,
            ),
            True,
            False,
        ),
        (
            re.compile(
                r"^(?P<name>[A-Za-zÀ-ÿ'’\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+)\s*(?:captures?|specimens?|unites?)\s*(?:max(?:imum)?)?\s*(?:par|/)\s*(?:pecheur|navire)\s*et\s*par\s*jour",
                flags=re.IGNORECASE,
            ),
            False,
            False,
        ),
        (
            re.compile(
                r"^(?P<name>[A-Za-zÀ-ÿ'’\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+(?:[\.,]\d+)?)\s*kg\s*(?:max(?:imum)?)?\s*(?:par|/)\s*(?:pecheur|navire)\s*et\s*par\s*jour",
                flags=re.IGNORECASE,
            ),
            True,
            False,
        ),
        (
            re.compile(
                r"^(?P<name>[A-Za-zÀ-ÿ'’\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?(?:\s+[A-Za-zÀ-ÿ'’\-\s]{0,80})?\s+(?P<value>\d+(?:[\.,]\d+)?)\s*(?P<unit>kg|kilogrammes?|unites?|specimens?|captures?)\b",
                flags=re.IGNORECASE,
            ),
            False,
            True,
        ),
    ]
    narrative_patterns: list[tuple[re.Pattern[str], bool, str | None]] = [
        (
            re.compile(
                r"pas plus de\s+(?P<value>\d+|un|une|deux|trois|quatre|cinq|six|sept|huit|neuf|dix)\s+specimens?\s+de\s+(?P<name>[a-z'\-\s]{3,}?)\s+(?:ne\s+peuvent|peuvent|sont|doivent|seront|$)[^\.]*?par\s+(?:pecheur|navire)\s+et\s+par\s+jour",
                flags=re.IGNORECASE,
            ),
            False,
            None,
        ),
        (
            re.compile(
                r"peche\s+(?:sous\s*-\s*marine\s+)?(?:du|de|des)\s+(?P<name>[a-z'\-\s]{3,}?)\s+est limitee?\s+a\s+(?P<value>\d+|un|une|deux|trois|quatre|cinq|six|sept|huit|neuf|dix)\s+(?:unites?|specimens?|captures?|individus?)[^\.]*?par\s+(?:pecheur|navire)\s+et\s+par\s+jour",
                flags=re.IGNORECASE,
            ),
            False,
            None,
        ),
        (
            re.compile(
                r"quantite totale de\s+(?P<value>\d+(?:[\.,]\d+)?)\s*kg\s+de\s+(?P<name>[a-z'\-\s]{3,}?)\s+par\s+(?:pecheur|navire)\s+et\s+par\s+jour",
                flags=re.IGNORECASE,
            ),
            True,
            None,
        ),
        (
            re.compile(
                r"captures?\s+sont\s+limitees?\s+par\s+(?:pecheur|navire)\s+et\s+par\s+jour\s+a\s+(?P<value>\d+(?:[\.,]\d+)?)\s*(?:kg|kilogrammes?)[^\.]*toutes?\s+especes?",
                flags=re.IGNORECASE,
            ),
            True,
            "toutes especes marines",
        ),
        (
            re.compile(
                r"(?P<value>\d+|un|une)\s+thon\s+rouge\s+par\s+navire\s+et\s+par\s+jour",
                flags=re.IGNORECASE,
            ),
            False,
            "thon rouge",
        ),
    ]

    seen: set[str] = set()

    def register_quota(
        *,
        species_name: str,
        value: float,
        is_kg: bool,
        description: str,
        scope: str,
        scientific: str | None = None,
    ) -> None:
        if not is_plausible_species_name(species_name):
            return
        quota_kind = "QUOTA_MAX_KG" if is_kg else "QUOTA_MAX_UNITS"
        quota_unit = "kg/jour" if is_kg else "captures/jour"
        key = f"species.{slugify(species_name)}.quota.{slugify(quota_unit)}.{slugify(scope)}"
        if key in seen:
            return
        seen.add(key)
        rules.append(
            build_base_rule(
                rule_key=key,
                rule_type="QUOTA",
                title=f"Quota de capture - {species_name}",
                description=description,
                source=source,
                source_url=source_url,
                legal_reference="Document operationnel DIRM - quotas peche de loisir",
                metric_type=quota_kind,
                metric_value=value,
                metric_unit=quota_unit,
                species_common_name=species_name,
                species_scientific_name=scientific,
                needs_manual_review=True,
                notes="Extraction automatique de quotas, validation manuelle obligatoire.",
                zone=resolve_rule_zone(source, description),
            )
        )

    for line in lines:
        line_for_match = fold_text(line)
        for pattern, default_is_kg, table_like in structured_patterns:
            match = pattern.search(line_for_match)
            if not match:
                continue

            if table_like and not has_daily_context and "par jour" not in line_for_match:
                continue

            scientific_raw = match.groupdict().get("scientific")
            scientific = normalize_spaces(scientific_raw) if scientific_raw else None
            parsed_value = parse_french_number(match.group("value"))
            if parsed_value is None:
                continue

            group_unit = (match.groupdict().get("unit") or "").lower()
            is_kg = default_is_kg or "kg" in group_unit
            species_name = canonical_species_name(match.group("name"), context_text=line)
            if not species_name:
                continue

            register_quota(
                species_name=species_name,
                value=float(parsed_value),
                is_kg=is_kg,
                description=line,
                scope=rule_scope_for_text(source, line),
                scientific=scientific,
            )

    for pattern, is_kg, forced_species in narrative_patterns:
        for match in pattern.finditer(flat_text):
            parsed_value = parse_french_number(match.group("value"))
            if parsed_value is None:
                continue

            context = flat_text[max(0, match.start() - 220): min(len(flat_text), match.end() + 220)]
            raw_name = forced_species or match.groupdict().get("name") or ""
            species_name = canonical_species_name(raw_name, context_text=context)
            if not species_name:
                continue

            register_quota(
                species_name=species_name,
                value=float(parsed_value),
                is_kg=is_kg,
                description=normalize_spaces(context),
                scope=rule_scope_for_text(source, context),
                scientific=None,
            )

    return rules


def extract_dirm_closure_rules(source: SourceRecord, source_url: str, text: str) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    segments = re.split(r"(?<=[\.;!?])\s+", normalize_spaces(text))
    period_pattern = re.compile(
        r"du\s+(?P<start>\d{1,2}\s*(?:er)?\s+[a-z]+(?:\s+\d{4})?)\s+au\s+(?P<end>\d{1,2}\s*(?:er)?\s+[a-z]+(?:\s+\d{4})?)",
        flags=re.IGNORECASE,
    )

    seen: set[str] = set()
    for segment in segments:
        lowered = fold_text(segment)
        if "du " not in lowered or " au " not in lowered:
            continue
        if (
            "interdit" not in lowered
            and "aucun specimen" not in lowered
            and "fermeture" not in lowered
            and "no-kill" not in lowered
        ):
            continue

        period = period_pattern.search(lowered)
        if not period:
            continue
        start = normalize_spaces(period.group("start"))
        end = normalize_spaces(period.group("end"))
        scope = rule_scope_for_text(source, segment)

        species_names = detect_all_species_in_text(segment)
        if not species_names:
            continue

        for species_name in species_names:
            rule_key = (
                f"species.{slugify(species_name)}.closure."
                f"{slugify(start)}.{slugify(end)}.{slugify(scope)}"
            )
            if rule_key in seen:
                continue
            seen.add(rule_key)

            rules.append(
                build_base_rule(
                    rule_key=rule_key,
                    rule_type="CLOSURE_PERIOD",
                    title=f"Periode de fermeture - {species_name}",
                    description=segment,
                    source=source,
                    source_url=source_url,
                    legal_reference="Document operationnel DIRM - restrictions temporelles",
                    metric_type=None,
                    metric_value=None,
                    metric_unit=None,
                    species_common_name=species_name,
                    species_scientific_name=None,
                    needs_manual_review=True,
                    notes="Extraction automatique des periodes de fermeture, validation manuelle obligatoire.",
                    zone=resolve_rule_zone(source, segment),
                )
            )

    return rules


def extract_dirm_protected_species_rules(source: SourceRecord, source_url: str, text: str) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    segments = re.split(r"(?<=[\.;!?])\s+", normalize_spaces(text))
    seen: set[str] = set()

    for segment in segments:
        lowered = fold_text(segment)
        if "interdit" not in lowered:
            continue
        if "peche" not in lowered and "capture" not in lowered and "detention" not in lowered:
            continue

        species_names = detect_all_species_in_text(segment)
        if not species_names:
            continue

        scope = rule_scope_for_text(source, segment)
        for species_name in species_names:
            key = f"species.{slugify(species_name)}.protected.{slugify(scope)}.{short_hash(segment)}"
            if key in seen:
                continue
            seen.add(key)

            rules.append(
                build_base_rule(
                    rule_key=key,
                    rule_type="PROTECTED_SPECIES",
                    title=f"Interdiction de capture - {species_name}",
                    description=segment,
                    source=source,
                    source_url=source_url,
                    legal_reference="Document operationnel DIRM - restrictions par espece",
                    metric_type=None,
                    metric_value=None,
                    metric_unit=None,
                    species_common_name=species_name,
                    species_scientific_name=None,
                    needs_manual_review=True,
                    notes="Extraction automatique des interdictions par espece, validation manuelle obligatoire.",
                    zone=resolve_rule_zone(source, segment),
                )
            )

    return rules


def extract_sensitive_species_declaration_rules(
    source: SourceRecord,
    source_url: str,
    text: str,
) -> list[dict[str, Any]]:
    lowered = fold_text(text)
    if "especes sensibles" not in lowered and "especes concernees" not in lowered:
        return []
    if "recfishing" not in lowered and "declaration" not in lowered and "enregistrement" not in lowered:
        return []

    species_names = detect_all_species_in_text(text)
    if not species_names:
        return []

    rules: list[dict[str, Any]] = []
    seen: set[str] = set()
    scope = rule_scope_for_text(source, text)
    for species_name in species_names:
        key = f"species.{slugify(species_name)}.declaration.{slugify(scope)}"
        if key in seen:
            continue
        seen.add(key)

        sentence = find_sentence(text, [species_name.split()[0], "declar"])
        description = sentence or (
            "Espece sensible soumise a enregistrement/declaration des captures via RecFishing."
        )
        rules.append(
            build_base_rule(
                rule_key=key,
                rule_type="LOCAL_RESTRICTION",
                title=f"Declaration des captures - {species_name}",
                description=description,
                source=source,
                source_url=source_url,
                legal_reference="Reglement UE 2023/2842 et dispositifs nationaux d'application",
                metric_type=None,
                metric_value=None,
                metric_unit=None,
                species_common_name=species_name,
                species_scientific_name=None,
                needs_manual_review=True,
                notes="Extraction automatique des obligations declaratives d'especes sensibles.",
                zone=resolve_rule_zone(source, description),
            )
        )
    return rules


def collect_source_documents(
    source: SourceRecord,
    html_cache: dict[str, str],
    pdf_text_cache: dict[str, str],
) -> list[SourceDocument]:
    documents: list[SourceDocument] = []
    seen_doc_urls: set[str] = set()
    source_kind = fold_text(source.kind)
    source_url = canonicalize_url(source.url)

    def add_document(url: str, text: str) -> None:
        if not text.strip():
            return
        canonical_url = canonicalize_url(url)
        if canonical_url in seen_doc_urls:
            return
        seen_doc_urls.add(canonical_url)
        documents.append(SourceDocument(source=source, url=canonical_url, text=text))

    def fetch_html_cached(url: str) -> str:
        if url not in html_cache:
            html_cache[url] = fetch_url_text(url)
        return html_cache[url]

    def fetch_pdf_text_cached(url: str) -> str:
        if url not in pdf_text_cache:
            pdf_bytes = fetch_url_bytes(url)
            pdf_text_cache[url] = extract_pdf_text_with_optional_ocr(pdf_bytes, url)
        return pdf_text_cache[url]

    if "pdf" == source_kind or source_url.lower().endswith(".pdf"):
        try:
            add_document(source_url, fetch_pdf_text_cached(source_url))
        except Exception as exc:
            print(f"[WARN] Echec fetch PDF source={source.source_id} url={source_url}: {exc}")
        return documents

    if "html" not in source_kind and not source_url.lower().endswith(".html"):
        try:
            add_document(source_url, html_to_text(fetch_html_cached(source_url)))
        except Exception as exc:
            print(f"[WARN] Echec fetch source={source.source_id} url={source_url}: {exc}")
        return documents

    try:
        base_html = fetch_html_cached(source_url)
        add_document(source_url, html_to_text(base_html))
    except Exception as exc:
        print(f"[WARN] Echec fetch source={source.source_id} url={source_url}: {exc}")
        return documents

    should_collect_pdfs = "pdf" in source_kind or source.source_type in {"DIRM", "MINISTERE_MER", "PREFECTURE_MARITIME"}
    if should_collect_pdfs:
        for pdf_url in extract_pdf_urls_from_html(base_html, source_url, limit=MAX_PDF_LINKS_PER_PAGE):
            try:
                add_document(pdf_url, fetch_pdf_text_cached(pdf_url))
            except Exception as exc:
                print(f"[WARN] Echec fetch PDF lie source={source.source_id} url={pdf_url}: {exc}")

    if "links" not in source_kind:
        return documents

    linked_html_urls = extract_relevant_html_links_from_html(base_html, source_url, limit=MAX_LINKED_HTML_PER_SOURCE)
    per_link_pdf_limit = max(2, MAX_PDF_LINKS_PER_PAGE // 2)
    for linked_html_url in linked_html_urls:
        try:
            linked_html = fetch_html_cached(linked_html_url)
            add_document(linked_html_url, html_to_text(linked_html))
        except Exception as exc:
            print(f"[WARN] Echec fetch page liee source={source.source_id} url={linked_html_url}: {exc}")
            continue

        for pdf_url in extract_pdf_urls_from_html(linked_html, linked_html_url, limit=per_link_pdf_limit):
            try:
                add_document(pdf_url, fetch_pdf_text_cached(pdf_url))
            except Exception as exc:
                print(f"[WARN] Echec fetch PDF page liee source={source.source_id} url={pdf_url}: {exc}")

    return documents


def deduplicate_rules(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    for rule in rules:
        key = rule["rule_key"]
        current = out.get(key)
        if current is None:
            out[key] = rule
            continue

        current_priority = int(current["source"]["source_priority"])
        incoming_priority = int(rule["source"]["source_priority"])

        if (
            current.get("metric_value") != rule.get("metric_value")
            or current.get("description") != rule.get("description")
        ):
            kept = "incoming" if incoming_priority < current_priority else "current"
            print(
                "[WARN] Conflit detecte "
                f"rule_key={key} keep={kept} "
                f"current_source={current['source']['source_url']} "
                f"incoming_source={rule['source']['source_url']}"
            )

        if incoming_priority < current_priority:
            out[key] = rule
            continue
        if incoming_priority == current_priority:
            current_manual = bool(current.get("needs_manual_review", False))
            incoming_manual = bool(rule.get("needs_manual_review", False))
            if current_manual and not incoming_manual:
                out[key] = rule

    return sorted(out.values(), key=lambda item: item["rule_key"])


def validate_rule_set(rules: list[dict[str, Any]]) -> None:
    has_diving = any(rule["rule_type"] == "DIVING_GENERAL" for rule in rules)
    has_spearfishing = any(rule["rule_type"] == "SPEARFISHING_GENERAL" for rule in rules)
    has_min_size = any(rule["rule_type"] == "MIN_SIZE" for rule in rules)
    has_quota = any(rule["rule_type"] == "QUOTA" for rule in rules)
    has_crustacean_numeric = any(
        rule["rule_type"] in {"MIN_SIZE", "QUOTA"}
        and any(keyword in fold_text(str(rule.get("species_common_name") or "")) for keyword in CRUSTACEAN_KEYWORDS)
        for rule in rules
    )
    has_fish_quota = any(
        rule["rule_type"] == "QUOTA"
        and not any(keyword in fold_text(str(rule.get("species_common_name") or "")) for keyword in CRUSTACEAN_KEYWORDS)
        for rule in rules
    )

    missing: list[str] = []
    if not has_diving:
        missing.append("DIVING_GENERAL")
    if not has_spearfishing:
        missing.append("SPEARFISHING_GENERAL")
    if not has_min_size:
        missing.append("MIN_SIZE")
    if not has_quota:
        missing.append("QUOTA")
    if not has_crustacean_numeric:
        missing.append("CRUSTACEAN_NUMERIC_RULE")
    if not has_fish_quota:
        missing.append("FISH_QUOTA_RULE")

    if missing:
        raise RuntimeError(f"Jeu de regles incomplet apres fetch/parsing. Categories manquantes: {missing}")


def add_legifrance_rules(
    source_by_id: dict[str, SourceRecord],
    static_legifrance_rules: list[dict[str, Any]],
    rules: list[dict[str, Any]],
) -> None:
    spearfishing_source = source_by_id.get("legifrance_spearfishing")
    if spearfishing_source is not None:
        try:
            spearfishing_html = fetch_url_text(spearfishing_source.url)
            parsed_rules = parse_legifrance_spearfishing_rules(spearfishing_source, spearfishing_html)
            if parsed_rules:
                rules.extend(parsed_rules)
            else:
                raise RuntimeError("Parsing vide pour la source Legifrance spearfishing")
        except Exception as exc:
            print(f"[WARN] Legifrance spearfishing indisponible en live, fallback statique: {exc}")
            rules.extend([rule for rule in static_legifrance_rules if rule.get("source_id") == "legifrance_spearfishing"])

    diving_source = source_by_id.get("legifrance_diving")
    if diving_source is not None:
        try:
            diving_html = fetch_url_text(diving_source.url)
            parsed_rules = parse_legifrance_diving_rules(diving_source, diving_html)
            if parsed_rules:
                rules.extend(parsed_rules)
            else:
                raise RuntimeError("Parsing vide pour la source Legifrance diving")
        except Exception as exc:
            print(f"[WARN] Legifrance diving indisponible en live, fallback statique: {exc}")
            rules.extend([rule for rule in static_legifrance_rules if rule.get("source_id") == "legifrance_diving"])


def add_operational_source_rules(
    source: SourceRecord,
    source_documents: list[SourceDocument],
    rules: list[dict[str, Any]],
) -> None:
    for document in source_documents:
        if source.source_type == "MINISTERE_MER":
            rules.extend(
                parse_ministere_spearfishing_rules(
                    source,
                    document.text,
                    source_url=document.url,
                )
            )

        if source.source_type not in {"DIRM", "MINISTERE_MER", "PREFECTURE_MARITIME", "DATA_GOUV"}:
            continue

        rules.extend(extract_dirm_size_rules(source, document.url, document.text))
        rules.extend(extract_dirm_quota_rules(source, document.url, document.text))
        rules.extend(extract_dirm_closure_rules(source, document.url, document.text))
        rules.extend(extract_dirm_protected_species_rules(source, document.url, document.text))
        rules.extend(extract_sensitive_species_declaration_rules(source, document.url, document.text))


def build_rules_from_sources(catalog: list[SourceRecord]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    static_legifrance_rules = load_static_legifrance_rules(STATIC_LEGIFRANCE_RULES_PATH)
    source_by_id = {source.source_id: source for source in catalog}

    add_legifrance_rules(source_by_id, static_legifrance_rules, rules)

    html_cache: dict[str, str] = {}
    pdf_text_cache: dict[str, str] = {}
    for source in catalog:
        if source.source_type == "LEGIFRANCE":
            continue

        source_documents = collect_source_documents(source, html_cache, pdf_text_cache)
        if not source_documents:
            print(f"[WARN] Aucun document exploitable pour source={source.source_id}")
            continue

        before = len(rules)
        add_operational_source_rules(source, source_documents, rules)
        after = len(rules)
        print(
            f"[INFO] source={source.source_id} docs={len(source_documents)} "
            f"rules_added={max(0, after - before)}"
        )

    rules = deduplicate_rules(rules)
    validate_rule_set(rules)
    return rules


def write_rules(path: Path, rules: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rules, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    catalog = load_source_catalog(SOURCE_CATALOG_PATH)
    rules = build_rules_from_sources(catalog)
    write_rules(OUTPUT_RULES_PATH, rules)

    print(f"[OK] {len(rules)} regles generees -> {OUTPUT_RULES_PATH}")
    counts: dict[str, int] = {}
    for rule in rules:
        counts[rule["rule_type"]] = counts.get(rule["rule_type"], 0) + 1
    print(json.dumps({"counts_by_type": counts}, ensure_ascii=False))


if __name__ == "__main__":
    main()
