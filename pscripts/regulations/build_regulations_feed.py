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
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urldefrag, urljoin, urlparse

import requests

SOURCE_CATALOG_PATH = Path(os.environ.get("REG_SOURCE_CATALOG_FILE", "data/regulations/source_endpoints.json"))
OUTPUT_RULES_PATH = Path(os.environ.get("REG_GENERATED_RULES_FILE", "data/regulations/generated_rules.json"))
QUALITY_REPORT_PATH = Path(os.environ.get("REG_QUALITY_REPORT_FILE", "data/regulations/quality_report.json"))
OUTPUT_CANDIDATES_PATH = Path(
    os.environ.get("REG_RULE_CANDIDATES_FILE", "data/regulations/generated_rule_candidates.json")
)
OUTPUT_DOCUMENTS_PATH = Path(
    os.environ.get("REG_SOURCE_DOCUMENTS_FILE", "data/regulations/source_documents_manifest.json")
)
STATIC_LEGIFRANCE_RULES_PATH = Path(
    os.environ.get("REG_STATIC_LEGIFRANCE_RULES_FILE", "data/regulations/static_legifrance_rules.json")
)
FETCH_LIVE_LEGIFRANCE = os.environ.get("REG_LEGIFRANCE_FETCH_LIVE", "false").lower() == "true"
REQUEST_TIMEOUT_SECONDS = int(os.environ.get("REG_REQUEST_TIMEOUT_SECONDS", "25"))
MAX_LINKED_HTML_PER_SOURCE = int(os.environ.get("REG_MAX_LINKED_HTML_PER_SOURCE", "10"))
MAX_PDF_LINKS_PER_PAGE = int(os.environ.get("REG_MAX_PDF_LINKS_PER_PAGE", "12"))
ENABLE_PDF_OCR = os.environ.get("REG_ENABLE_PDF_OCR", "false").lower() == "true"
OCR_MIN_TEXT_CHARS = int(os.environ.get("REG_OCR_MIN_TEXT_CHARS", "900"))
OCR_MAX_PAGES = int(os.environ.get("REG_OCR_MAX_PAGES", "8"))
OCR_LANG = os.environ.get("REG_OCR_LANG", "fra+eng")
ENABLE_AI_AUDIT = os.environ.get("REG_ENABLE_AI_AUDIT", "false").lower() == "true"
AI_API_KEY = os.environ.get("REG_AI_API_KEY") or os.environ.get("OPENAI_API_KEY")
AI_BASE_URL = os.environ.get("REG_AI_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
AI_MODEL = os.environ.get("REG_AI_MODEL", "openrouter/free")
AI_TIMEOUT_SECONDS = int(os.environ.get("REG_AI_TIMEOUT_SECONDS", "45"))
AI_MAX_RULES = int(os.environ.get("REG_AI_MAX_RULES", "80"))
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/pdf;q=0.8,*/*;q=0.7",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.6",
    "Connection": "keep-alive",
}
LOCAL_AI_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}

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
    "reglementation",
    "arrete",
    "capture",
    "taille",
    "tailles",
    "quota",
    "quotas",
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
NEGATIVE_SOURCE_KEYWORDS = (
    "accessibilite",
    "contact",
    "newsletter",
    "mentions-legales",
    "plan-du-site",
    "recrutement",
    "marche-public",
    "communique",
    "presse",
)
RULE_TYPE_ORDER = {
    "DIVING_GENERAL": 10,
    "SPEARFISHING_GENERAL": 20,
    "MIN_SIZE": 30,
    "QUOTA": 40,
    "CLOSURE_PERIOD": 50,
    "PROTECTED_SPECIES": 60,
    "LOCAL_RESTRICTION": 70,
}
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
SPECIES_SCIENTIFIC_NAMES: dict[str, str] = {
    "bar": "Dicentrarchus labrax",
    "lieu jaune": "Pollachius pollachius",
    "maquereau": "Scomber scombrus",
    "araignees": "Maja squinado",
    "homard": "Homarus gammarus",
    "tourteau": "Cancer pagurus",
    "thon rouge": "Thunnus thynnus",
    "dorade rose": "Pagellus bogaraveo",
    "dorade coryphene": "Coryphaena hippurus",
    "merou": "Epinephelus marginatus",
    "corb": "Sciaena umbra",
    "espadon": "Xiphias gladius",
    "denti": "Dentex dentex",
}
RULE_ACTIVITY_TYPES = {
    "DIVING_GENERAL": "diving",
    "SPEARFISHING_GENERAL": "spearfishing",
    "MIN_SIZE": "recreational_fishing",
    "QUOTA": "recreational_fishing",
    "CLOSURE_PERIOD": "recreational_fishing",
    "PROTECTED_SPECIES": "recreational_fishing",
    "LOCAL_RESTRICTION": "recreational_fishing",
}
RULE_CONSTRAINT_TYPES = {
    "DIVING_GENERAL": "safety_framework",
    "SPEARFISHING_GENERAL": "practice_rule",
    "MIN_SIZE": "min_size",
    "QUOTA": "quota",
    "CLOSURE_PERIOD": "closure_period",
    "PROTECTED_SPECIES": "forbidden_capture",
    "LOCAL_RESTRICTION": "declaration_or_local_rule",
}


@dataclass(frozen=True)
class HtmlLink:
    href: str
    text: str


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[HtmlLink] = []
        self._open_links: list[dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        href = next((value for key, value in attrs if key.lower() == "href" and value), None)
        if href:
            self._open_links.append({"href": href, "text": []})

    def handle_data(self, data: str) -> None:
        if self._open_links:
            self._open_links[-1]["text"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._open_links:
            return
        item = self._open_links.pop()
        self.links.append(HtmlLink(href=str(item["href"]), text=normalize_spaces(" ".join(item["text"]))))
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
    return normalize_spaces(no_accent).replace("\u2019", "'").lower()


def html_to_text(raw_html: str) -> str:
    without_scripts = re.sub(
        r"<(script|style|noscript|template)\b[^>]*>.*?</\1>",
        " ",
        raw_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    with_line_breaks = re.sub(
        r"<\s*br\s*/?\s*>|</\s*(?:p|li|div|section|article|main|nav|header|footer|h[1-6]|tr|table)\s*>",
        "\n",
        without_scripts,
        flags=re.IGNORECASE,
    )
    without_tags = re.sub(r"<[^>]+>", " ", with_line_breaks)
    decoded = html_lib.unescape(without_tags)
    lines = [normalize_spaces(line) for line in decoded.splitlines()]
    return "\n".join(line for line in lines if line)


def split_text_units(text: str) -> list[str]:
    units: list[str] = []
    for line in text.splitlines() or [text]:
        for item in re.split(r"(?<=[\.;:!?])\s+", line):
            cleaned = normalize_spaces(item).replace("\u2019", "'")
            if cleaned:
                units.append(cleaned)
    return units


def short_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:10]


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonicalize_url(url: str) -> str:
    return urldefrag(url.strip())[0]


def is_official_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host.endswith(".gouv.fr")


def extract_links_from_html(html: str) -> list[HtmlLink]:
    parser = LinkExtractor()
    try:
        parser.feed(html)
    except Exception:
        parser.links = []

    if parser.links:
        return parser.links

    hrefs = re.findall(r"href=[\"']([^\"']+)[\"']", html, flags=re.IGNORECASE)
    return [HtmlLink(href=href, text="") for href in hrefs]


def score_source_candidate(url: str, label: str = "") -> tuple[int, int, str]:
    lowered = fold_text(f"{url} {label}")
    score = 0

    score += 4 * sum(1 for keyword in RELEVANT_SOURCE_KEYWORDS if keyword in lowered)
    score -= 5 * sum(1 for keyword in NEGATIVE_SOURCE_KEYWORDS if keyword in lowered)

    current_year = now_utc().year
    for year in (current_year, current_year - 1, current_year + 1):
        if str(year) in lowered:
            score += 3

    if is_official_url(url):
        score += 2
    if "annexe" in lowered:
        score += 2
    if re.search(r"\b(ar|arrete|decret)[-_]?\d", lowered):
        score += 2

    return (-score, len(url), url)


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


def http_headers_for_url(url: str, *, binary: bool = False) -> dict[str, str]:
    headers = dict(HTTP_HEADERS)
    if binary:
        headers["Accept"] = "application/pdf,application/octet-stream,*/*;q=0.7"
    parsed = urlparse(url)
    if parsed.netloc:
        headers["Referer"] = f"{parsed.scheme}://{parsed.netloc}/"
    return headers


def fetch_url_text(url: str, retries: int = 3) -> str:
    headers = http_headers_for_url(url)
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
    headers = http_headers_for_url(url, binary=True)
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
    candidates: list[tuple[str, str]] = []
    for link in extract_links_from_html(html):
        absolute_url = canonicalize_url(urljoin(base_url, link.href))
        parsed = urlparse(absolute_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.path.lower().endswith(".pdf"):
            candidates.append((absolute_url, link.text))

    deduped: list[tuple[str, str]] = []
    seen: set[str] = set()
    for url, label in candidates:
        if url in seen:
            continue
        seen.add(url)
        deduped.append((url, label))

    deduped.sort(key=lambda item: score_source_candidate(item[0], item[1]))
    urls = [url for url, _ in deduped]
    if limit is not None:
        return urls[: max(0, limit)]
    return urls


def extract_pdf_url_from_html(html: str, base_url: str) -> str:
    candidates = extract_pdf_urls_from_html(html, base_url, limit=1)
    if not candidates:
        raise ValueError("Aucun lien PDF detecte dans la page DIRM.")
    return candidates[0]


def extract_relevant_html_links_from_html(html: str, base_url: str, limit: int) -> list[str]:
    base_host = urlparse(base_url).netloc.lower()
    candidates: list[tuple[str, str]] = []

    for link in extract_links_from_html(html):
        absolute_url = canonicalize_url(urljoin(base_url, link.href))
        parsed = urlparse(absolute_url)
        if parsed.scheme not in {"http", "https"}:
            continue
        if parsed.netloc.lower() != base_host:
            continue
        if parsed.path.lower().endswith(".pdf"):
            continue
        lowered = fold_text(f"{parsed.path} {parsed.query} {link.text}")
        if any(keyword in lowered for keyword in NEGATIVE_SOURCE_KEYWORDS):
            continue
        if any(keyword in lowered for keyword in RELEVANT_SOURCE_KEYWORDS):
            candidates.append((absolute_url, link.text))
            continue
        if re.search(r"-a\d+\.html$", parsed.path):
            candidates.append((absolute_url, link.text))

    deduped: list[tuple[str, str]] = []
    seen: set[str] = {canonicalize_url(base_url)}
    for url, label in candidates:
        if url in seen:
            continue
        seen.add(url)
        deduped.append((url, label))

    deduped.sort(key=lambda item: score_source_candidate(item[0], item[1]))
    return [url for url, _ in deduped[: max(0, limit)]]


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
    units = split_text_units(text)
    for sentence in units:
        lowered = fold_text(sentence)
        if all(fold_text(token) in lowered for token in required_tokens):
            return sentence.strip()

    for index in range(len(units)):
        window = normalize_spaces(" ".join(units[index: index + 3]))
        lowered = fold_text(window)
        if all(fold_text(token) in lowered for token in required_tokens):
            return window.strip()
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


def infer_activity_type(rule: dict[str, Any]) -> str:
    return RULE_ACTIVITY_TYPES.get(str(rule.get("rule_type") or ""), "recreational_fishing")


def infer_constraint_type(rule: dict[str, Any]) -> str:
    metric_type = str(rule.get("metric_type") or "")
    if metric_type == "SIZE_MIN_CM":
        return "min_size"
    if metric_type in {"QUOTA_MAX_UNITS", "QUOTA_MAX_KG"}:
        return "quota"
    return RULE_CONSTRAINT_TYPES.get(str(rule.get("rule_type") or ""), "general_rule")


def infer_rule_status(rule: dict[str, Any]) -> str:
    return "needs_review" if bool(rule.get("needs_manual_review", False)) else "published"


def infer_confidence_score(rule: dict[str, Any]) -> float:
    source = rule.get("source") or {}
    priority = int(source.get("source_priority") or 999)
    score = 0.92 if priority == 1 else 0.78 if priority == 2 else 0.62
    if rule.get("metric_type") and rule.get("metric_value") is not None:
        score += 0.08
    if rule.get("legal_reference"):
        score += 0.04
    if bool(rule.get("needs_manual_review", False)):
        score -= 0.22
    if rule.get("quality_flags"):
        score -= 0.08
    return round(max(0.05, min(0.99, score)), 2)


def infer_taxon_group(species_name: str) -> str:
    normalized = fold_text(species_name)
    if any(token in normalized for token in ("araignee", "homard", "tourteau", "langouste", "crabe")):
        return "crustacean"
    if any(token in normalized for token in ("coquille", "ormeau", "pouce")):
        return "shellfish"
    return "fish"


def species_record_for_rule(rule: dict[str, Any]) -> dict[str, Any] | None:
    common_name = rule.get("species_common_name")
    if not common_name:
        return None
    canonical = fold_text(str(common_name))
    scientific = rule.get("species_scientific_name") or SPECIES_SCIENTIFIC_NAMES.get(canonical)
    aliases = sorted(set(SPECIES_ALIASES.get(canonical, (canonical,))))
    return {
        "canonical_name": canonical,
        "common_name": str(common_name),
        "scientific_name": scientific,
        "taxon_group": infer_taxon_group(canonical),
        "aliases": aliases,
        "external_ids": {},
    }


def citation_quote_for_rule(rule: dict[str, Any], max_chars: int = 700) -> str:
    quote = normalize_spaces(str(rule.get("description") or ""))
    if len(quote) <= max_chars:
        return quote
    return quote[: max_chars - 3].rstrip() + "..."


def source_document_hash_for_rule(rule: dict[str, Any]) -> str:
    source = rule.get("source") or {}
    source_url = canonicalize_url(str(source.get("source_url") or ""))
    seed = "\n".join(
        [
            source_url,
            str(source.get("title") or ""),
            str(source.get("authority_name") or ""),
            str(source.get("effective_date") or ""),
        ]
    )
    return sha256_text(seed)


def citation_for_rule(rule: dict[str, Any]) -> dict[str, Any]:
    source = rule.get("source") or {}
    document_hash = source_document_hash_for_rule(rule)
    return {
        "source_url": canonicalize_url(str(source.get("source_url") or "")),
        "source_title": source.get("title"),
        "authority_name": source.get("authority_name"),
        "quote": citation_quote_for_rule(rule),
        "page_number": None,
        "locator": rule.get("legal_reference") or rule.get("rule_key"),
        "document_hash": document_hash,
        "confidence_score": infer_confidence_score(rule),
    }


def candidate_for_rule(rule: dict[str, Any]) -> dict[str, Any]:
    candidate_key = short_hash(
        "|".join(
            [
                str(rule.get("rule_key") or ""),
                source_document_hash_for_rule(rule),
                rule_content_signature(rule),
            ]
        )
    )
    return {
        "candidate_key": candidate_key,
        "rule_key": rule.get("rule_key"),
        "rule_type": rule.get("rule_type"),
        "activity_type": rule.get("activity_type") or infer_activity_type(rule),
        "constraint_type": rule.get("constraint_type") or infer_constraint_type(rule),
        "title": rule.get("title"),
        "description": rule.get("description"),
        "status": "candidate" if rule.get("status") == "needs_review" else "publishable",
        "confidence_score": rule.get("confidence_score") or infer_confidence_score(rule),
        "confidence_source": rule.get("confidence_source") or "heuristic",
        "confidence_reason": rule.get("confidence_reason"),
        "needs_manual_review": bool(rule.get("needs_manual_review", False)),
        "quality_flags": rule.get("quality_flags") or [],
        "ai_audit": rule.get("ai_audit") or [],
        "document_hash": source_document_hash_for_rule(rule),
        "extracted_payload": {
            "metric_type": rule.get("metric_type"),
            "metric_value": rule.get("metric_value"),
            "metric_unit": rule.get("metric_unit"),
            "species_common_name": rule.get("species_common_name"),
            "species_scientific_name": rule.get("species_scientific_name"),
            "zone": rule.get("zone"),
            "legal_reference": rule.get("legal_reference"),
        },
    }


def enrich_rule_for_publication(rule: dict[str, Any]) -> dict[str, Any]:
    source = rule.get("source") or {}
    enriched = dict(rule)
    enriched["status"] = infer_rule_status(enriched)
    enriched["confidence_score"] = normalize_confidence_value(enriched.get("confidence_score"))
    if enriched["confidence_score"] is None:
        enriched["confidence_score"] = infer_confidence_score(enriched)
    enriched["confidence_source"] = enriched.get("confidence_source") or "heuristic"
    enriched["confidence_reason"] = enriched.get("confidence_reason")
    enriched["activity_type"] = infer_activity_type(enriched)
    enriched["constraint_type"] = infer_constraint_type(enriched)
    enriched["valid_from"] = enriched.get("valid_from") or source.get("effective_date")
    enriched["valid_to"] = enriched.get("valid_to")
    enriched["published_at"] = enriched.get("published_at")
    enriched["superseded_by_rule_key"] = enriched.get("superseded_by_rule_key")
    enriched["citations"] = [citation_for_rule(enriched)]
    species = species_record_for_rule(enriched)
    enriched["species"] = [species] if species else []
    enriched["candidate"] = candidate_for_rule(enriched)
    return enriched


def enrich_rules_for_publication(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [enrich_rule_for_publication(rule) for rule in rules]


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
            r"^(?P<name>[a-z'\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+(?:[\.,]\d+)?)\s*cm\b",
            flags=re.IGNORECASE,
        ),
        re.compile(
            r"^(?P<name>[a-z'\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s+(?P<value>\d+(?:[\.,]\d+)?)\s*cm\b",
            flags=re.IGNORECASE,
        ),
    ]

    seen: set[str] = set()
    for line in lines:
        line_for_match = fold_text(line)
        for pattern in patterns:
            match = pattern.search(line_for_match)
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
                r"^(?P<name>[a-z'\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+)\s*(?:captures?|specimens?|unites?)\s*(?:par|/)\s*jour",
                flags=re.IGNORECASE,
            ),
            False,
            False,
        ),
        (
            re.compile(
                r"^(?P<name>[a-z'\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+(?:[\.,]\d+)?)\s*kg\s*(?:par|/)\s*jour",
                flags=re.IGNORECASE,
            ),
            True,
            False,
        ),
        (
            re.compile(
                r"^(?P<name>[a-z'\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+)\s*(?:captures?|specimens?|unites?)\s*(?:max(?:imum)?)?\s*(?:par|/)\s*(?:pecheur|navire)\s*et\s*par\s*jour",
                flags=re.IGNORECASE,
            ),
            False,
            False,
        ),
        (
            re.compile(
                r"^(?P<name>[a-z'\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?\s*[:;\-]\s*(?P<value>\d+(?:[\.,]\d+)?)\s*kg\s*(?:max(?:imum)?)?\s*(?:par|/)\s*(?:pecheur|navire)\s*et\s*par\s*jour",
                flags=re.IGNORECASE,
            ),
            True,
            False,
        ),
        (
            re.compile(
                r"^(?P<name>[a-z'\-\s]{3,})(?:\s*\((?P<scientific>[^)]+)\))?(?:\s+[a-z'\-\s]{0,80})?\s+(?P<value>\d+(?:[\.,]\d+)?)\s*(?P<unit>kg|kilogrammes?|unites?|specimens?|captures?)\b",
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
    segments = split_text_units(text)
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


def source_supports_declaration_rules(source: SourceRecord, source_url: str) -> bool:
    marker = fold_text(f"{source.source_id} {source.title} {source.url} {source_url}")
    if source.source_type == "MINISTERE_MER":
        return True
    return any(token in marker for token in ("declaration", "enregistrement", "recfishing", "especes-sensibles"))


def is_noisy_context_unit(text: str) -> bool:
    lowered = fold_text(text)
    return any(
        marker in lowered
        for marker in (
            "acceder au contenu",
            "acceder au menu",
            "rechercher menu",
            "sur le meme sujet",
            "mentions legales",
            "gestion des cookies",
            "partager la page",
        )
    )


def declaration_context(text: str) -> str:
    units = split_text_units(text)
    if not units:
        return ""

    selected_indexes: list[int] = []

    def extend_context_range(start: int, end: int) -> None:
        for nearby in range(start, end):
            if is_noisy_context_unit(units[nearby]):
                break
            selected_indexes.append(nearby)

    for index, unit in enumerate(units):
        lowered = fold_text(unit)
        has_species_marker = "especes sensibles" in lowered or "especes concernees" in lowered
        has_declaration_marker = (
            "recfishing" in lowered
            or ("declaration" in lowered and "capture" in lowered)
            or ("enregistrement" in lowered and "pecheur" in lowered)
        )
        if has_species_marker:
            extend_context_range(index, min(len(units), index + 8))
        elif has_declaration_marker:
            nearby_start = max(0, index - 3)
            for candidate in range(index, nearby_start - 1, -1):
                candidate_lowered = fold_text(units[candidate])
                if "especes sensibles" in candidate_lowered or "especes concernees" in candidate_lowered:
                    nearby_start = candidate
                    break
            extend_context_range(nearby_start, min(len(units), index + 4))

    selected: list[str] = []
    seen: set[int] = set()
    for nearby in selected_indexes:
        if nearby in seen or is_noisy_context_unit(units[nearby]):
            continue
        seen.add(nearby)
        selected.append(units[nearby])

    return normalize_spaces(" ".join(selected))


def declaration_description(species_name: str, context: str) -> str:
    sentence = find_sentence(context, [species_name.split()[0]])
    if sentence and len(sentence) <= 420:
        return sentence
    return (
        f"{species_name} figure parmi les especes sensibles soumises a enregistrement "
        "et declaration des captures via RecFishing selon la zone applicable."
    )


def extract_sensitive_species_declaration_rules(
    source: SourceRecord,
    source_url: str,
    text: str,
) -> list[dict[str, Any]]:
    context = declaration_context(text)
    lowered = fold_text(context)
    has_strong_text_signal = (
        ("especes sensibles" in lowered or "especes concernees" in lowered)
        and ("recfishing" in lowered or "declaration" in lowered or "enregistrement" in lowered)
    )
    if not source_supports_declaration_rules(source, source_url) and not has_strong_text_signal:
        return []

    if "especes sensibles" not in lowered and "especes concernees" not in lowered:
        return []
    if "recfishing" not in lowered and "declaration" not in lowered and "enregistrement" not in lowered:
        return []

    species_names = detect_all_species_in_text(context)
    if not species_names:
        return []

    rules: list[dict[str, Any]] = []
    seen: set[str] = set()
    scope = rule_scope_for_text(source, context)
    for species_name in species_names:
        key = f"species.{slugify(species_name)}.declaration.{slugify(scope)}"
        if key in seen:
            continue
        seen.add(key)

        description = declaration_description(species_name, context)
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


def normalize_metric_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return fold_text(str(value))
    return f"{numeric:.3f}".rstrip("0").rstrip(".")


def rule_zone_code(rule: dict[str, Any]) -> str:
    zone = rule.get("zone") or {}
    return str(zone.get("zone_code") or "")


def normalized_rule_description(rule: dict[str, Any], max_chars: int = 360) -> str:
    text = fold_text(str(rule.get("description") or ""))
    text = re.sub(r"\b(article|arrete|decret)\s+[a-z0-9\-\.]+", " ", text)
    text = normalize_spaces(text)
    return text[:max_chars]


def rule_content_signature(rule: dict[str, Any]) -> str:
    rule_type = str(rule.get("rule_type") or "")
    species = fold_text(str(rule.get("species_common_name") or ""))
    zone = rule_zone_code(rule)
    metric_type = str(rule.get("metric_type") or "")
    metric_value = normalize_metric_value(rule.get("metric_value"))
    metric_unit = fold_text(str(rule.get("metric_unit") or ""))

    if rule_type in {"MIN_SIZE", "QUOTA"}:
        parts = [rule_type, species, zone, metric_type, metric_value, metric_unit]
    elif rule_type == "CLOSURE_PERIOD":
        parts = [rule_type, species, zone, short_hash(normalized_rule_description(rule))]
    else:
        parts = [
            rule_type,
            species,
            zone,
            fold_text(str(rule.get("title") or "")),
            short_hash(normalized_rule_description(rule)),
        ]
    return "|".join(parts)


def rule_conflict_group(rule: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(rule.get("rule_type") or ""),
        fold_text(str(rule.get("species_common_name") or "")),
        rule_zone_code(rule),
        str(rule.get("metric_type") or ""),
        fold_text(str(rule.get("metric_unit") or "")),
    )


def rule_sort_key(rule: dict[str, Any]) -> tuple[Any, ...]:
    return (
        rule_zone_code(rule) or "ZZZ",
        fold_text(str(rule.get("species_common_name") or "")) or "zzzz",
        RULE_TYPE_ORDER.get(str(rule.get("rule_type") or ""), 999),
        int((rule.get("source") or {}).get("source_priority") or 999),
        str(rule.get("rule_key") or ""),
    )


def prefer_incoming_rule(current: dict[str, Any], incoming: dict[str, Any]) -> bool:
    def score(rule: dict[str, Any]) -> tuple[int, int, int, str]:
        source = rule.get("source") or {}
        priority = int(source.get("source_priority") or 999)
        manual_review_penalty = 1 if bool(rule.get("needs_manual_review", False)) else 0
        description_len_bonus = -len(str(rule.get("description") or ""))
        return (priority, manual_review_penalty, description_len_bonus, str(rule.get("rule_key") or ""))

    return score(incoming) < score(current)


def append_unique_flag(rule: dict[str, Any], flag: str) -> None:
    flags = rule.setdefault("quality_flags", [])
    if isinstance(flags, list) and flag not in flags:
        flags.append(flag)


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
            if prefer_incoming_rule(current, rule):
                out[key] = rule

    by_signature: dict[str, dict[str, Any]] = {}
    for rule in out.values():
        signature = rule_content_signature(rule)
        current = by_signature.get(signature)
        if current is None:
            by_signature[signature] = rule
            continue

        kept = current
        discarded = rule
        if prefer_incoming_rule(current, rule):
            kept = rule
            discarded = current
            by_signature[signature] = rule

        append_unique_flag(kept, "duplicate_content_merged")
        print(
            "[INFO] Doublon de contenu fusionne "
            f"kept={kept.get('rule_key')} discarded={discarded.get('rule_key')}"
        )

    return sorted(by_signature.values(), key=rule_sort_key)


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


def count_by_key(rules: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for rule in rules:
        value = str(rule.get(key) or "")
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def add_quality_issue(
    issues: list[dict[str, Any]],
    *,
    severity: str,
    category: str,
    message: str,
    rule_key: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    issue = {
        "severity": severity,
        "category": category,
        "message": message,
    }
    if rule_key:
        issue["rule_key"] = rule_key
    if details:
        issue["details"] = details
    issues.append(issue)


def build_quality_report(
    rules: list[dict[str, Any]],
    ai_audit: dict[str, Any] | None = None,
) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    seen_keys: dict[str, int] = {}
    seen_signatures: dict[str, str] = {}
    conflict_values: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}

    for rule in rules:
        rule_key = str(rule.get("rule_key") or "")
        seen_keys[rule_key] = seen_keys.get(rule_key, 0) + 1

        required = ["rule_key", "rule_type", "title", "description", "source", "zone"]
        missing = [field for field in required if not rule.get(field)]
        if missing:
            add_quality_issue(
                issues,
                severity="error",
                category="missing_field",
                rule_key=rule_key or None,
                message="Champs obligatoires manquants.",
                details={"fields": missing},
            )

        description = str(rule.get("description") or "")
        folded_description = fold_text(description)
        if len(description) > 1200:
            add_quality_issue(
                issues,
                severity="warning",
                category="description_too_long",
                rule_key=rule_key or None,
                message="Description tres longue: verifier que l'extraction n'a pas capture une page entiere.",
                details={"length": len(description)},
            )
        if any(
            marker in folded_description
            for marker in (
                "var mediabox_settings",
                "tarteaucitron",
                "acceder au menu",
                "gestion des cookies",
                "ajaxpagestate",
            )
        ):
            add_quality_issue(
                issues,
                severity="warning",
                category="noisy_description",
                rule_key=rule_key or None,
                message="Description probablement polluee par du contenu de navigation ou de script.",
            )

        signature = rule_content_signature(rule)
        previous_key = seen_signatures.get(signature)
        if previous_key and previous_key != rule_key:
            add_quality_issue(
                issues,
                severity="warning",
                category="duplicate_content",
                rule_key=rule_key,
                message="Regle probablement equivalente a une autre regle.",
                details={"duplicate_of": previous_key},
            )
        else:
            seen_signatures[signature] = rule_key

        source = rule.get("source") or {}
        source_url = str(source.get("source_url") or "")
        if source_url and not is_official_url(source_url):
            add_quality_issue(
                issues,
                severity="info",
                category="source_priority",
                rule_key=rule_key,
                message="Source non .gouv.fr: a garder en complement, pas comme source juridique principale.",
                details={"source_url": source_url},
            )

        zone = rule.get("zone") or {}
        if zone.get("strategy") == "CUSTOM_BBOX":
            bbox_fields = ("lat_min", "lat_max", "lon_min", "lon_max")
            missing_bbox = [field for field in bbox_fields if zone.get(field) is None]
            if missing_bbox:
                add_quality_issue(
                    issues,
                    severity="error",
                    category="invalid_zone",
                    rule_key=rule_key,
                    message="Zone CUSTOM_BBOX incomplete.",
                    details={"fields": missing_bbox},
                )

        metric_value = rule.get("metric_value")
        metric_type = str(rule.get("metric_type") or "")
        if metric_value is not None:
            try:
                numeric_value = float(metric_value)
            except (TypeError, ValueError):
                add_quality_issue(
                    issues,
                    severity="error",
                    category="invalid_metric",
                    rule_key=rule_key,
                    message="Valeur metrique non numerique.",
                    details={"metric_value": metric_value},
                )
            else:
                if metric_type == "SIZE_MIN_CM" and not 1 <= numeric_value <= 400:
                    add_quality_issue(
                        issues,
                        severity="warning",
                        category="suspicious_metric",
                        rule_key=rule_key,
                        message="Taille minimale hors plage plausible.",
                        details={"metric_value": numeric_value},
                    )
                if metric_type in {"QUOTA_MAX_UNITS", "QUOTA_MAX_KG"} and numeric_value <= 0:
                    add_quality_issue(
                        issues,
                        severity="error",
                        category="invalid_metric",
                        rule_key=rule_key,
                        message="Quota nul ou negatif.",
                        details={"metric_value": numeric_value},
                    )

        group = rule_conflict_group(rule)
        if group[3] and metric_value is not None:
            current = conflict_values.get(group)
            if current is None:
                conflict_values[group] = {"rule_key": rule_key, "metric_value": normalize_metric_value(metric_value)}
            elif current["metric_value"] != normalize_metric_value(metric_value):
                add_quality_issue(
                    issues,
                    severity="warning",
                    category="metric_conflict",
                    rule_key=rule_key,
                    message="Deux regles proches portent des valeurs differentes.",
                    details={
                        "other_rule_key": current["rule_key"],
                        "other_metric_value": current["metric_value"],
                        "metric_value": normalize_metric_value(metric_value),
                    },
                )

    duplicate_keys = [key for key, count in seen_keys.items() if key and count > 1]
    for key in duplicate_keys:
        add_quality_issue(
            issues,
            severity="error",
            category="duplicate_key",
            rule_key=key,
            message="rule_key dupliquee apres dedoublonnage.",
            details={"count": seen_keys[key]},
        )

    counts_by_type = count_by_key(rules, "rule_type")
    counts_by_zone: dict[str, int] = {}
    for rule in rules:
        zone_code = rule_zone_code(rule) or "UNKNOWN"
        counts_by_zone[zone_code] = counts_by_zone.get(zone_code, 0) + 1

    return {
        "generated_at": now_utc().isoformat(),
        "rules_count": len(rules),
        "needs_manual_review_count": sum(1 for rule in rules if bool(rule.get("needs_manual_review"))),
        "ai_confidence_count": len((ai_audit or {}).get("confidence_scores") or {}),
        "counts_by_type": counts_by_type,
        "counts_by_zone": dict(sorted(counts_by_zone.items())),
        "issue_count": len(issues),
        "issues": issues,
        "ai_audit": ai_audit or {"enabled": False, "status": "disabled", "issues": []},
    }


def compact_rule_for_ai(rule: dict[str, Any]) -> dict[str, Any]:
    source = rule.get("source") or {}
    return {
        "rule_key": rule.get("rule_key"),
        "rule_type": rule.get("rule_type"),
        "title": rule.get("title"),
        "description": str(rule.get("description") or "")[:700],
        "metric_type": rule.get("metric_type"),
        "metric_value": rule.get("metric_value"),
        "metric_unit": rule.get("metric_unit"),
        "species_common_name": rule.get("species_common_name"),
        "zone_code": rule_zone_code(rule),
        "source_priority": source.get("source_priority"),
        "source_url": source.get("source_url"),
        "needs_manual_review": bool(rule.get("needs_manual_review", False)),
    }


def build_source_documents_manifest(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_hash: dict[str, dict[str, Any]] = {}
    for rule in rules:
        source = rule.get("source") or {}
        citations = rule.get("citations") or []
        document_hash = source_document_hash_for_rule(rule)
        citation = citations[0] if citations else citation_for_rule(rule)
        current = by_hash.get(document_hash)
        if current is None:
            by_hash[document_hash] = {
                "document_hash": document_hash,
                "source_url": citation.get("source_url") or source.get("source_url"),
                "canonical_url": citation.get("source_url") or source.get("source_url"),
                "source_type": source.get("source_type"),
                "authority_name": source.get("authority_name"),
                "title": source.get("title"),
                "document_type": "pdf" if str(source.get("source_url") or "").lower().endswith(".pdf") else "html",
                "content_length": 0,
                "fetched_at": None,
                "checked_at": now_utc().isoformat(),
                "rule_keys": [rule.get("rule_key")],
                "chunks": [
                    {
                        "chunk_index": 0,
                        "chunk_hash": sha256_text(f"{document_hash}:0:{citation.get('quote') or ''}"),
                        "text_excerpt": citation.get("quote") or "",
                        "token_estimate": max(1, len(str(citation.get("quote") or "").split())),
                        "page_number": citation.get("page_number"),
                        "locator": citation.get("locator"),
                    }
                ],
            }
            continue

        current["rule_keys"].append(rule.get("rule_key"))
        quote = str(citation.get("quote") or "")
        if quote:
            chunk_index = len(current["chunks"])
            chunk_hash = sha256_text(f"{document_hash}:{chunk_index}:{quote}")
            if all(chunk["chunk_hash"] != chunk_hash for chunk in current["chunks"]):
                current["chunks"].append(
                    {
                        "chunk_index": chunk_index,
                        "chunk_hash": chunk_hash,
                        "text_excerpt": quote,
                        "token_estimate": max(1, len(quote.split())),
                        "page_number": citation.get("page_number"),
                        "locator": citation.get("locator"),
                    }
                )

    for item in by_hash.values():
        item["content_length"] = sum(len(chunk["text_excerpt"]) for chunk in item["chunks"])
        item["rule_keys"] = sorted(set(str(key) for key in item["rule_keys"] if key))
    return sorted(by_hash.values(), key=lambda item: str(item.get("source_url") or ""))


def build_rule_candidates(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [rule.get("candidate") or candidate_for_rule(rule) for rule in rules],
        key=lambda item: str(item.get("rule_key") or ""),
    )


def normalize_ai_issues(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    raw_issues = payload.get("issues")
    if not isinstance(raw_issues, list):
        return []

    issues: list[dict[str, Any]] = []
    for item in raw_issues:
        if not isinstance(item, dict):
            continue
        severity = str(item.get("severity") or "warning").lower()
        if severity not in {"info", "warning", "error"}:
            severity = "warning"
        issues.append(
            {
                "severity": severity,
                "category": str(item.get("category") or "ai_audit"),
                "rule_key": item.get("rule_key"),
                "message": str(item.get("message") or "").strip()[:600],
                "suggested_action": str(item.get("suggested_action") or "").strip()[:600],
            }
        )
    return issues


def normalize_confidence_value(value: Any) -> float | None:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if score > 1 and score <= 100:
        score = score / 100
    return round(max(0.0, min(1.0, score)), 2)


def normalize_ai_confidence_scores(payload: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(payload, dict):
        return {}

    raw_scores = payload.get("confidence_scores")
    if raw_scores is None:
        raw_scores = payload.get("rule_confidence")

    normalized: dict[str, dict[str, Any]] = {}
    if isinstance(raw_scores, dict):
        iterable = []
        for rule_key, item in raw_scores.items():
            if isinstance(item, dict):
                iterable.append({"rule_key": rule_key, **item})
            else:
                iterable.append({"rule_key": rule_key, "confidence_score": item})
    elif isinstance(raw_scores, list):
        iterable = raw_scores
    else:
        iterable = []

    for item in iterable:
        if not isinstance(item, dict):
            continue
        rule_key = str(item.get("rule_key") or "").strip()
        score = normalize_confidence_value(
            item.get("confidence_score")
            if item.get("confidence_score") is not None
            else item.get("score")
        )
        if not rule_key or score is None:
            continue
        normalized[rule_key] = {
            "confidence_score": score,
            "confidence_reason": str(item.get("confidence_reason") or item.get("reason") or "").strip()[:600],
        }
    return normalized


def ai_message_content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            text_value = item.get("text")
            if isinstance(text_value, str):
                parts.append(text_value)
                continue
            if item.get("type") == "output_text" and isinstance(item.get("text"), str):
                parts.append(str(item.get("text")))
        return "\n".join(part for part in parts if part).strip()
    if isinstance(content, dict):
        text_value = content.get("text")
        if isinstance(text_value, str):
            return text_value
    return ""


def extract_first_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise ValueError("Reponse IA vide.")

    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = stripped.find("{")
    while start != -1:
        depth = 0
        in_string = False
        escape = False
        for index in range(start, len(stripped)):
            char = stripped[index]
            if in_string:
                if escape:
                    escape = False
                elif char == "\\":
                    escape = True
                elif char == "\"":
                    in_string = False
                continue
            if char == "\"":
                in_string = True
                continue
            if char == "{":
                depth += 1
            elif char == "}":
                depth -= 1
                if depth == 0:
                    candidate = stripped[start : index + 1]
                    parsed = json.loads(candidate)
                    if isinstance(parsed, dict):
                        return parsed
                    break
        start = stripped.find("{", start + 1)

    raise ValueError("Aucun objet JSON exploitable dans la reponse IA.")


def ai_base_url_is_local(base_url: str = AI_BASE_URL) -> bool:
    parsed = urlparse(base_url)
    return parsed.hostname in LOCAL_AI_HOSTS


def ai_base_url_is_openrouter(base_url: str = AI_BASE_URL) -> bool:
    parsed = urlparse(base_url)
    return (parsed.hostname or "").endswith("openrouter.ai")


def ai_request_headers(base_url: str = AI_BASE_URL, api_key: str | None = AI_API_KEY) -> dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def build_ai_request_payload(prompt_payload: dict[str, Any], strict_json: bool = True) -> dict[str, Any]:
    system_content = (
        "Tu es un auditeur qualite de donnees juridiques maritimes. "
        "Tu reponds en JSON strict."
    )
    if not strict_json:
        system_content = (
            "Tu es un auditeur qualite de donnees juridiques maritimes. "
            "Retourne un unique objet JSON brut, sans commentaire avant ou apres."
        )

    request_payload = {
        "model": AI_MODEL,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": json.dumps(prompt_payload, ensure_ascii=False)},
        ],
    }
    if strict_json:
        request_payload["response_format"] = {"type": "json_object"}
    return request_payload


def summarize_ai_response_payload(payload: Any) -> str:
    if isinstance(payload, dict):
        parts: list[str] = []
        keys = sorted(str(key) for key in payload.keys())
        if keys:
            parts.append(f"keys={','.join(keys[:8])}")
        object_type = payload.get("object")
        if object_type:
            parts.append(f"object={object_type}")
        status = payload.get("status")
        if status:
            parts.append(f"status={status}")
        error = payload.get("error")
        if isinstance(error, dict):
            code = error.get("code")
            message = normalize_spaces(str(error.get("message") or ""))[:220]
            if code not in (None, ""):
                parts.append(f"error_code={code}")
            if message:
                parts.append(f"error={message}")
        return "; ".join(parts) or "payload dict vide"
    if isinstance(payload, list):
        return f"payload list[{len(payload)}]"
    return f"payload {type(payload).__name__}"


def parse_ai_response_payload(response_payload: Any) -> dict[str, Any]:
    if not isinstance(response_payload, dict):
        raise ValueError(f"Reponse IA non supportee ({summarize_ai_response_payload(response_payload)}).")

    error = response_payload.get("error")
    if isinstance(error, dict):
        message = normalize_spaces(str(error.get("message") or "Erreur fournisseur IA."))[:300]
        code = error.get("code")
        if code not in (None, ""):
            raise ValueError(f"Erreur IA {code}: {message}")
        raise ValueError(f"Erreur IA: {message}")

    choices = response_payload.get("choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0] if isinstance(choices[0], dict) else {}
        if not isinstance(first_choice, dict):
            raise ValueError("Reponse IA avec choices invalides.")

        finish_reason = first_choice.get("finish_reason")
        if finish_reason == "error":
            raise ValueError(f"Generation IA en erreur ({summarize_ai_response_payload(response_payload)}).")

        text_candidates = [
            ai_message_content_to_text((first_choice.get("message") or {}).get("content")),
            ai_message_content_to_text((first_choice.get("delta") or {}).get("content")),
        ]
        if isinstance(first_choice.get("text"), str):
            text_candidates.append(str(first_choice.get("text")))

        tool_calls = (first_choice.get("message") or {}).get("tool_calls") or []
        if isinstance(tool_calls, list) and tool_calls:
            arguments = ((tool_calls[0].get("function") or {}).get("arguments"))
            if isinstance(arguments, str):
                text_candidates.append(arguments)

        for candidate in text_candidates:
            if candidate.strip():
                return extract_first_json_object(candidate)

        raise ValueError("Reponse IA sans contenu exploitable dans choices[0].")

    output = response_payload.get("output")
    if isinstance(output, list):
        output_texts: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content_text = ai_message_content_to_text(item.get("content"))
            if content_text:
                output_texts.append(content_text)
                continue
            if isinstance(item.get("text"), str):
                output_texts.append(str(item.get("text")))
        if output_texts:
            return extract_first_json_object("\n".join(output_texts))

    top_level_content = ai_message_content_to_text(response_payload.get("content"))
    if top_level_content:
        return extract_first_json_object(top_level_content)

    raise ValueError(f"Reponse IA sans contenu exploitable ({summarize_ai_response_payload(response_payload)}).")


def run_ai_rule_audit(rules: list[dict[str, Any]]) -> dict[str, Any]:
    if not ENABLE_AI_AUDIT:
        return {"enabled": False, "status": "disabled", "issues": []}
    if not AI_API_KEY and not ai_base_url_is_local():
        return {
            "enabled": True,
            "status": "skipped",
            "error": "REG_AI_API_KEY ou OPENAI_API_KEY absent pour un endpoint distant.",
            "issues": [],
        }

    sample_rules = sorted(rules, key=rule_sort_key)[: max(0, AI_MAX_RULES)]
    prompt_payload = {
        "instructions": (
            "Audite des regles de reglementation maritime extraites automatiquement. "
            "Ne cree pas de nouvelles regles. Signale seulement incoherences, doublons probables, "
            "valeurs suspectes, zone incoherente ou source faible. "
            "Attribue aussi un niveau de confiance entre 0 et 1 a chaque regle. "
            "Base le score sur la precision de l'extrait, la source, la structure de la valeur, "
            "la coherence zone/espece et le besoin de verification humaine. Reponds uniquement en JSON."
        ),
        "schema": {
            "confidence_scores": [
                {
                    "rule_key": "rule_key concernee",
                    "confidence_score": "nombre entre 0 et 1",
                    "confidence_reason": "raison courte du score",
                }
            ],
            "issues": [
                {
                    "severity": "info|warning|error",
                    "category": "duplicate|incoherent_metric|bad_zone|weak_source|needs_review",
                    "rule_key": "rule_key concernee ou null",
                    "message": "constat court",
                    "suggested_action": "action concrete",
                }
            ]
        },
        "rules": [compact_rule_for_ai(rule) for rule in sample_rules],
    }
    attempt_specs = [{"strict_json": True, "label": "strict_json"}]
    if ai_base_url_is_openrouter():
        attempt_specs.append({"strict_json": False, "label": "openrouter_plain_json_retry"})

    last_error: Exception | None = None
    last_response_summary = ""
    for attempt_index, attempt in enumerate(attempt_specs, start=1):
        response_payload: Any = None
        try:
            response = requests.post(
                f"{AI_BASE_URL}/chat/completions",
                headers=ai_request_headers(),
                json=build_ai_request_payload(prompt_payload, strict_json=bool(attempt["strict_json"])),
                timeout=AI_TIMEOUT_SECONDS,
            )
            try:
                response_payload = response.json()
            except ValueError:
                response_payload = {"raw_text": response.text[:800]}

            if response.status_code >= 400:
                raise ValueError(
                    f"HTTP {response.status_code} - {summarize_ai_response_payload(response_payload)}"
                )

            parsed = parse_ai_response_payload(response_payload)
            return {
                "enabled": True,
                "status": "ok",
                "model": AI_MODEL,
                "rules_sent": len(sample_rules),
                "issues": normalize_ai_issues(parsed),
                "confidence_scores": normalize_ai_confidence_scores(parsed),
                "attempt_count": attempt_index,
            }
        except Exception as exc:  # pragma: no cover - external provider variability
            last_error = exc
            if response_payload is not None:
                last_response_summary = summarize_ai_response_payload(response_payload)

    return {
        "enabled": True,
        "status": "failed",
        "model": AI_MODEL,
        "error": str(last_error) if last_error else "Echec IA inconnu.",
        "response_summary": last_response_summary,
        "rules_sent": len(sample_rules),
        "issues": [],
        "attempt_count": len(attempt_specs),
    }


def apply_ai_audit_to_rules(rules: list[dict[str, Any]], ai_audit: dict[str, Any]) -> None:
    if ai_audit.get("status") != "ok":
        return
    by_key = {str(rule.get("rule_key")): rule for rule in rules}

    for rule_key, confidence in (ai_audit.get("confidence_scores") or {}).items():
        rule = by_key.get(str(rule_key))
        if not rule or not isinstance(confidence, dict):
            continue
        score = normalize_confidence_value(confidence.get("confidence_score"))
        if score is None:
            continue
        rule["confidence_score"] = score
        rule["confidence_source"] = "ai"
        reason = str(confidence.get("confidence_reason") or "").strip()
        if reason:
            rule["confidence_reason"] = reason
        audit_items = rule.setdefault("ai_audit", [])
        if isinstance(audit_items, list):
            audit_items.append(
                {
                    "severity": "info",
                    "category": "confidence_score",
                    "message": f"Score de confiance IA: {score}",
                    "suggested_action": reason,
                }
            )
        if score < 0.65:
            rule["needs_manual_review"] = True
            append_unique_flag(rule, "ai_low_confidence")

    for issue in ai_audit.get("issues") or []:
        rule_key = str(issue.get("rule_key") or "")
        rule = by_key.get(rule_key)
        if not rule:
            continue
        severity = str(issue.get("severity") or "warning")
        category = str(issue.get("category") or "ai_audit")
        append_unique_flag(rule, f"ai_{category}")
        if severity in {"warning", "error"}:
            rule["needs_manual_review"] = True
        audit_items = rule.setdefault("ai_audit", [])
        if isinstance(audit_items, list):
            audit_items.append(
                {
                    "severity": severity,
                    "category": category,
                    "message": issue.get("message"),
                    "suggested_action": issue.get("suggested_action"),
                }
            )


def add_legifrance_rules(
    source_by_id: dict[str, SourceRecord],
    static_legifrance_rules: list[dict[str, Any]],
    rules: list[dict[str, Any]],
) -> None:
    if not FETCH_LIVE_LEGIFRANCE:
        rules.extend(static_legifrance_rules)
        print("[INFO] Legifrance live desactive, socle statique utilise.")
        return

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


def write_quality_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    catalog = load_source_catalog(SOURCE_CATALOG_PATH)
    rules = build_rules_from_sources(catalog)
    ai_audit = run_ai_rule_audit(rules)
    apply_ai_audit_to_rules(rules, ai_audit)
    rules = enrich_rules_for_publication(rules)
    source_documents = build_source_documents_manifest(rules)
    rule_candidates = build_rule_candidates(rules)
    quality_report = build_quality_report(rules, ai_audit=ai_audit)
    write_rules(OUTPUT_RULES_PATH, rules)
    write_quality_report(QUALITY_REPORT_PATH, quality_report)
    write_json(OUTPUT_DOCUMENTS_PATH, source_documents)
    write_json(OUTPUT_CANDIDATES_PATH, rule_candidates)

    print(f"[OK] {len(rules)} regles generees -> {OUTPUT_RULES_PATH}")
    print(f"[OK] Rapport qualite -> {QUALITY_REPORT_PATH}")
    print(f"[OK] {len(source_documents)} documents sources -> {OUTPUT_DOCUMENTS_PATH}")
    print(f"[OK] {len(rule_candidates)} candidats -> {OUTPUT_CANDIDATES_PATH}")
    counts: dict[str, int] = {}
    for rule in rules:
        counts[rule["rule_type"]] = counts.get(rule["rule_type"], 0) + 1
    print(
        json.dumps(
            {
                "counts_by_type": counts,
                "quality_issue_count": quality_report["issue_count"],
                "ai_audit_status": ai_audit.get("status"),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
