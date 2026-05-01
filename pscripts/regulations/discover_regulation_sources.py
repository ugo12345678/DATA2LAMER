from __future__ import annotations

import json
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlparse

import requests

from pscripts.regulations.build_regulations_feed import (
    AI_API_KEY,
    AI_BASE_URL,
    AI_MODEL,
    AI_TIMEOUT_SECONDS,
    HTTP_HEADERS,
    REQUEST_TIMEOUT_SECONDS,
    ai_base_url_is_local,
    ai_request_headers,
    canonicalize_url,
    extract_links_from_html,
    fold_text,
    html_to_text,
    load_source_catalog,
    score_source_candidate,
    short_hash,
)


CONFIG_PATH = Path(os.environ.get("REG_SOURCE_DISCOVERY_CONFIG_FILE", "data/regulations/source_discovery_config.json"))
SOURCE_CATALOG_PATH = Path(os.environ.get("REG_SOURCE_CATALOG_FILE", "data/regulations/source_endpoints.json"))
OUTPUT_PATH = Path(os.environ.get("REG_DISCOVERED_SOURCES_FILE", "data/regulations/generated_source_candidates.json"))
ENABLE_AI_CLASSIFIER = os.environ.get("REG_DISCOVERY_ENABLE_AI_CLASSIFIER", "false").lower() == "true"
MAX_AI_CANDIDATES = int(os.environ.get("REG_DISCOVERY_AI_MAX_CANDIDATES", "80"))


@dataclass(frozen=True)
class DiscoveryDomain:
    host: str
    source_type: str
    authority_name: str
    source_priority: int
    seed_urls: tuple[str, ...]
    sitemap_urls: tuple[str, ...]


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def load_discovery_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError("La configuration de decouverte doit etre un objet JSON.")
    return payload


def discovery_domains(config: dict[str, Any]) -> list[DiscoveryDomain]:
    domains: list[DiscoveryDomain] = []
    for item in config.get("domains") or []:
        if not isinstance(item, dict):
            continue
        seed_urls = tuple(str(url) for url in item.get("seed_urls") or [] if url)
        sitemap_urls = tuple(str(url) for url in item.get("sitemap_urls") or [] if url)
        if not seed_urls and not sitemap_urls:
            continue
        domains.append(
            DiscoveryDomain(
                host=str(item["host"]).lower(),
                source_type=str(item["source_type"]),
                authority_name=str(item["authority_name"]),
                source_priority=int(item.get("source_priority") or 2),
                seed_urls=seed_urls,
                sitemap_urls=sitemap_urls,
            )
        )
    return domains


def allowed_host(url: str, domain: DiscoveryDomain) -> bool:
    host = urlparse(url).netloc.lower()
    return host == domain.host or host.endswith("." + domain.host)


def fetch_text(url: str, retries: int = 2) -> str:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HTTP_HEADERS, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            response.encoding = response.encoding or "utf-8"
            return response.text
        except Exception as exc:  # pragma: no cover - network variability
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(f"Echec decouverte HTTP sur {url}: {last_error}")


def keyword_score(text: str, keywords: list[str], negative_keywords: list[str]) -> tuple[int, list[str]]:
    folded = fold_text(text)
    matches = [keyword for keyword in keywords if fold_text(keyword) in folded]
    negative_matches = [keyword for keyword in negative_keywords if fold_text(keyword) in folded]
    return len(matches) * 3 - len(negative_matches) * 8, matches


def year_score(text: str, config: dict[str, Any], current_year: int | None = None) -> int:
    current_year = current_year or datetime.now().year
    offsets = config.get("current_year_window") or [-1, 0, 1]
    score = 0
    folded = fold_text(text)
    for offset in offsets:
        try:
            year = current_year + int(offset)
        except (TypeError, ValueError):
            continue
        if str(year) in folded:
            score += 4
    return score


def title_from_url(url: str, fallback: str = "") -> str:
    if fallback:
        return re.sub(r"\s+", " ", fallback).strip()
    path = urlparse(url).path.rsplit("/", 1)[-1]
    stem = re.sub(r"\.(html?|pdf)$", "", path, flags=re.IGNORECASE)
    return re.sub(r"[-_]+", " ", stem).strip() or url


def kind_for_url(url: str) -> str:
    return "pdf" if urlparse(url).path.lower().endswith(".pdf") else "html+pdf+links"


def candidate_score(url: str, label: str, config: dict[str, Any]) -> tuple[int, list[str]]:
    keywords = [str(item) for item in config.get("keywords") or []]
    negative_keywords = [str(item) for item in config.get("negative_keywords") or []]
    signal = f"{url} {label}"
    key_score, matches = keyword_score(signal, keywords, negative_keywords)
    source_score = -score_source_candidate(url, label)[0]
    score = key_score + source_score + year_score(signal, config)
    if urlparse(url).path.lower().endswith(".pdf"):
        score += 2
    return score, matches


def candidate_id_for_url(url: str) -> str:
    return f"discovered_{short_hash(canonicalize_url(url))}"


def source_candidate(
    *,
    url: str,
    label: str,
    domain: DiscoveryDomain,
    config: dict[str, Any],
    discovery_method: str,
    seed_url: str | None = None,
    existing_urls: set[str] | None = None,
) -> dict[str, Any]:
    canonical_url = canonicalize_url(url)
    score, matches = candidate_score(canonical_url, label, config)
    min_score = int(config.get("min_auto_accept_score") or 12)
    status = "existing" if canonical_url in (existing_urls or set()) else "auto_accepted" if score >= min_score else "candidate"
    return {
        "id": candidate_id_for_url(canonical_url),
        "kind": kind_for_url(canonical_url),
        "source_type": domain.source_type,
        "source_priority": domain.source_priority,
        "authority_name": domain.authority_name,
        "title": title_from_url(canonical_url, fallback=label),
        "url": canonical_url,
        "status": status,
        "discovery_score": score,
        "matched_keywords": matches,
        "discovery_method": discovery_method,
        "seed_url": seed_url,
        "found_at": now_utc_iso(),
    }


def discover_links_from_html(
    *,
    html: str,
    base_url: str,
    domain: DiscoveryDomain,
    config: dict[str, Any],
    existing_urls: set[str],
    discovery_method: str,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for link in extract_links_from_html(html):
        absolute_url = canonicalize_url(urljoin(base_url, link.href))
        parsed = urlparse(absolute_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not allowed_host(absolute_url, domain):
            continue
        if absolute_url in seen:
            continue
        seen.add(absolute_url)
        candidate = source_candidate(
            url=absolute_url,
            label=link.text,
            domain=domain,
            config=config,
            discovery_method=discovery_method,
            seed_url=base_url,
            existing_urls=existing_urls,
        )
        if candidate["status"] in {"auto_accepted", "candidate"}:
            candidates.append(candidate)
    candidates.sort(key=lambda item: (-int(item["discovery_score"]), len(str(item["url"]))))
    return candidates[: int(config.get("max_candidates_per_seed") or 40)]


def sitemap_locations(xml_text: str) -> list[str]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    locations: list[str] = []
    for node in root.iter():
        if node.tag.lower().endswith("loc") and node.text:
            locations.append(canonicalize_url(node.text.strip()))
    return locations


def discover_from_sitemap(
    *,
    sitemap_url: str,
    domain: DiscoveryDomain,
    config: dict[str, Any],
    existing_urls: set[str],
    fetcher: Callable[[str], str] = fetch_text,
) -> list[dict[str, Any]]:
    try:
        xml_text = fetcher(sitemap_url)
    except Exception as exc:
        print(f"[WARN] Sitemap ignore url={sitemap_url}: {exc}")
        return []
    candidates: list[dict[str, Any]] = []
    for url in sitemap_locations(xml_text):
        if not allowed_host(url, domain):
            continue
        candidate = source_candidate(
            url=url,
            label=title_from_url(url),
            domain=domain,
            config=config,
            discovery_method="sitemap",
            seed_url=sitemap_url,
            existing_urls=existing_urls,
        )
        if candidate["status"] in {"auto_accepted", "candidate"}:
            candidates.append(candidate)
    candidates.sort(key=lambda item: (-int(item["discovery_score"]), len(str(item["url"]))))
    return candidates[: int(config.get("max_candidates_per_seed") or 40)]


def discover_from_seed(
    *,
    seed_url: str,
    domain: DiscoveryDomain,
    config: dict[str, Any],
    existing_urls: set[str],
    fetcher: Callable[[str], str] = fetch_text,
) -> list[dict[str, Any]]:
    max_pages = int(config.get("max_pages_per_seed") or 40)
    queue = [canonicalize_url(seed_url)]
    visited: set[str] = set()
    candidates: dict[str, dict[str, Any]] = {}

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            html = fetcher(url)
        except Exception as exc:
            print(f"[WARN] Seed ignore url={url}: {exc}")
            continue

        page_candidates = discover_links_from_html(
            html=html,
            base_url=url,
            domain=domain,
            config=config,
            existing_urls=existing_urls,
            discovery_method="hub_crawl",
        )
        for candidate in page_candidates:
            candidates.setdefault(str(candidate["url"]), candidate)
            if candidate["kind"].startswith("html") and candidate["status"] == "auto_accepted":
                candidate_url = str(candidate["url"])
                if candidate_url not in visited and candidate_url not in queue:
                    queue.append(candidate_url)

    out = list(candidates.values())
    out.sort(key=lambda item: (-int(item["discovery_score"]), len(str(item["url"]))))
    return out[: int(config.get("max_candidates_per_seed") or 40)]


def classify_candidates_with_ai(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not ENABLE_AI_CLASSIFIER or not candidates:
        return candidates
    if not AI_API_KEY and not ai_base_url_is_local(AI_BASE_URL):
        print("[WARN] Classification IA des sources ignoree: aucune cle API configuree.")
        return candidates

    subset = candidates[:MAX_AI_CANDIDATES]
    prompt = {
        "task": "Classer des sources officielles candidates pour une veille de reglementation de peche maritime de loisir.",
        "expected_json": {
            "items": [
                {
                    "id": "candidate id",
                    "is_relevant": True,
                    "confidence": 0.0,
                    "reason": "court",
                }
            ]
        },
        "candidates": [
            {
                "id": item["id"],
                "url": item["url"],
                "title": item["title"],
                "authority_name": item["authority_name"],
                "matched_keywords": item.get("matched_keywords") or [],
                "score": item.get("discovery_score"),
            }
            for item in subset
        ],
    }
    try:
        response = requests.post(
            f"{AI_BASE_URL}/chat/completions",
            headers=ai_request_headers(AI_BASE_URL, AI_API_KEY),
            json={
                "model": AI_MODEL,
                "messages": [
                    {"role": "system", "content": "Tu reponds uniquement en JSON valide."},
                    {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                ],
                "temperature": 0,
                "response_format": {"type": "json_object"},
            },
            timeout=AI_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        content = response.json()["choices"][0]["message"]["content"]
        payload = json.loads(content)
    except Exception as exc:  # pragma: no cover - external AI variability
        print(f"[WARN] Classification IA des sources ignoree: {exc}")
        return candidates

    by_id = {str(item.get("id")): item for item in payload.get("items") or [] if isinstance(item, dict)}
    out: list[dict[str, Any]] = []
    for candidate in candidates:
        ai_item = by_id.get(str(candidate.get("id")))
        if ai_item:
            candidate = {
                **candidate,
                "ai_relevant": bool(ai_item.get("is_relevant")),
                "ai_confidence": ai_item.get("confidence"),
                "ai_reason": ai_item.get("reason"),
            }
            if candidate["status"] == "candidate" and candidate["ai_relevant"] and float(ai_item.get("confidence") or 0) >= 0.75:
                candidate["status"] = "auto_accepted"
        out.append(candidate)
    return out


def discover_sources(
    config: dict[str, Any],
    existing_urls: set[str],
    fetcher: Callable[[str], str] = fetch_text,
) -> list[dict[str, Any]]:
    candidates_by_url: dict[str, dict[str, Any]] = {}
    for domain in discovery_domains(config):
        for seed_url in domain.seed_urls:
            for candidate in discover_from_seed(
                seed_url=seed_url,
                domain=domain,
                config=config,
                existing_urls=existing_urls,
                fetcher=fetcher,
            ):
                current = candidates_by_url.get(str(candidate["url"]))
                if current is None or int(candidate["discovery_score"]) > int(current["discovery_score"]):
                    candidates_by_url[str(candidate["url"])] = candidate
        for sitemap_url in domain.sitemap_urls:
            for candidate in discover_from_sitemap(
                sitemap_url=sitemap_url,
                domain=domain,
                config=config,
                existing_urls=existing_urls,
                fetcher=fetcher,
            ):
                current = candidates_by_url.get(str(candidate["url"]))
                if current is None or int(candidate["discovery_score"]) > int(current["discovery_score"]):
                    candidates_by_url[str(candidate["url"])] = candidate

    candidates = list(candidates_by_url.values())
    candidates = classify_candidates_with_ai(candidates)
    candidates.sort(key=lambda item: (str(item.get("status") or ""), -int(item.get("discovery_score") or 0), str(item["url"])))
    return candidates


def main() -> None:
    config = load_discovery_config(CONFIG_PATH)
    existing_catalog = load_source_catalog(SOURCE_CATALOG_PATH)
    existing_urls = {canonicalize_url(source.url) for source in existing_catalog}
    candidates = discover_sources(config, existing_urls)
    write_json(OUTPUT_PATH, candidates)

    accepted = sum(1 for item in candidates if item.get("status") == "auto_accepted")
    print(
        json.dumps(
            {
                "candidate_count": len(candidates),
                "auto_accepted_count": accepted,
                "output": str(OUTPUT_PATH),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
