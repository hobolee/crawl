from __future__ import annotations

import re
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

from .models import ArticleRecord


ARTICLE_SLUG_RE = r"(?:s41467-\d{3}-\d{5}-[0-9a-z]|ncomms\d+)"
ARTICLE_URL_RE = re.compile(rf"^https://www\.nature\.com/articles/({ARTICLE_SLUG_RE})\??.*$")
DATE_RE = re.compile(r"Published:\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})")
ACCEPTED_RE = re.compile(r"Accepted:\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})")
RECEIVED_RE = re.compile(r"Received:\s*([0-9]{1,2}\s+\w+\s+[0-9]{4})")
DOI_RE = re.compile(rf"(10\.1038/({ARTICLE_SLUG_RE}))")
PEER_REVIEW_URL_RE = re.compile(
    r"https://static-content\.springer\.com/[^\s\"')>]+MOESM\d+_ESM\.pdf",
    re.IGNORECASE,
)
DOWNLOAD_PDF_LINE_RE = re.compile(r"\[Download PDF\]\((https://www\.nature\.com/articles/[^\)]+\.pdf)\)")
MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\((https?://[^\)]+)\)")
NATURE_ARTICLE_LINK_RE = re.compile(
    rf"https://www\.nature\.com/articles/{ARTICLE_SLUG_RE}(?:[?#][^\s\"')>]*)?"
)
DEFAULT_ARCHIVE_URL = "https://www.nature.com/ncomms/research-articles?type=article"
DEFAULT_SITE_ARCHIVE_URLS = [
    "https://www.nature.com/ncomms/research-articles?type=article",
]


def normalize_article_url(url: str) -> str:
    stripped = url.strip()
    parsed = urlparse(stripped)
    normalized = parsed._replace(query="", fragment="").geturl()
    return normalized.rstrip("/")


def normalize_listing_url(url: str) -> str:
    stripped = url.strip()
    parsed = urlparse(stripped)
    normalized = parsed._replace(fragment="").geturl()
    return normalized.rstrip("/")


def listing_url_with_year(base_url: str, year: int | None) -> str:
    if year is None:
        return normalize_listing_url(base_url)
    normalized = normalize_listing_url(base_url)
    parsed = urlparse(normalized)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["year"] = str(year)
    normalized_query = urlencode(query)
    return parsed._replace(query=normalized_query).geturl()


def is_nc_article_url(url: str) -> bool:
    return bool(ARTICLE_URL_RE.match(normalize_article_url(url)))


def slug_from_article_url(url: str) -> str:
    return normalize_article_url(url).rsplit("/", 1)[-1]


def archive_page_url(base_url: str, page_number: int) -> str:
    normalized = normalize_listing_url(base_url)
    parsed = urlparse(normalized)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if page_number <= 1:
        query.pop("page", None)
        normalized_query = urlencode(query)
        return parsed._replace(query=normalized_query).geturl().rstrip("?")

    query["page"] = str(page_number)
    if parsed.path.endswith("/research-articles") and query.get("type") == "article":
        query.setdefault("searchType", "journalSearch")
        query.setdefault("sort", "PubDate")
    normalized_query = urlencode(query)
    return parsed._replace(query=normalized_query).geturl()


def extract_article_urls_from_listing(scrape_result: dict[str, Any], base_url: str | None = None) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def maybe_add(url: str) -> None:
        candidate = normalize_article_url(urljoin(base_url or "", url))
        if not is_nc_article_url(candidate) or candidate in seen:
            return
        seen.add(candidate)
        urls.append(candidate)

    links = scrape_result.get("links") or []
    for link in links:
        if isinstance(link, str):
            maybe_add(link)

    markdown = scrape_result.get("markdown") or ""
    for _, url in MARKDOWN_LINK_RE.findall(markdown):
        maybe_add(url)
    for url in NATURE_ARTICLE_LINK_RE.findall(markdown):
        maybe_add(url)

    html = scrape_result.get("html") or ""
    for url in NATURE_ARTICLE_LINK_RE.findall(html):
        maybe_add(url)
    raw_html = scrape_result.get("raw_html") or scrape_result.get("rawHtml") or ""
    for url in NATURE_ARTICLE_LINK_RE.findall(raw_html):
        maybe_add(url)

    return urls


def extract_peer_review_url(scrape_result: dict[str, Any]) -> str | None:
    # Primary: parse HTML to find the supplementary item explicitly labeled as peer review.
    # The page renders multiple MOESM links (Supplementary Info, Reporting Summary,
    # Peer Review file, Source Data…) inside div[data-test="supp-item"] elements.
    # Each link carries a data-track-label that uniquely identifies which file it is.
    html_source = scrape_result.get("html") or scrape_result.get("raw_html") or ""
    if html_source:
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html_source, "html.parser")
            for item in soup.find_all("div", attrs={"data-test": "supp-item"}):
                link = item.find("a", href=True)
                if not link:
                    continue
                label = (link.get("data-track-label") or "").lower()
                heading = item.find(["h2", "h3"])
                heading_text = (heading.get_text(" ", strip=True) if heading else "").lower()
                if "peer review" in label or "peer review" in heading_text:
                    href = link["href"]
                    if href.lower().endswith(".pdf"):
                        return href
        except Exception:
            pass

    # Fallback: firecrawl-rendered markdown may contain labelled links.
    markdown = scrape_result.get("markdown") or ""
    for label, url in MARKDOWN_LINK_RE.findall(markdown):
        if "peer review" in label.lower() and url.lower().endswith(".pdf"):
            return url

    lower_markdown = markdown.lower()
    if "transparent peer review file" in lower_markdown or "peer review file is available" in lower_markdown:
        match = PEER_REVIEW_URL_RE.search(markdown)
        if match:
            return match.group(0)

    return None


def _as_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        items = [part.strip() for part in re.split(r"[;,]", value) if part.strip()]
        if items:
            return items
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                result.append(item.strip())
        return result
    return []


def extract_authors(scrape_result: dict[str, Any]) -> list[str]:
    metadata = scrape_result.get("metadata") or {}
    for key in ("authors", "author", "citation_author", "dc.creator", "dc_creator"):
        authors = _as_string_list(metadata.get(key))
        if authors:
            return authors

    markdown = scrape_result.get("markdown") or ""
    lines = [line.strip() for line in markdown.splitlines() if line.strip()]
    if len(lines) >= 2 and not lines[1].startswith("## "):
        possible_authors = _as_string_list(lines[1].replace(" & ", ", "))
        if possible_authors and all(len(name.split()) <= 6 for name in possible_authors):
            return possible_authors
    return []


def extract_section_headings(scrape_result: dict[str, Any]) -> list[str]:
    markdown = scrape_result.get("markdown") or ""
    headings: list[str] = []
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            headings.append(stripped[3:].strip())
    return headings


def extract_keywords(scrape_result: dict[str, Any]) -> list[str]:
    metadata = scrape_result.get("metadata") or {}
    for key in ("keywords", "keyword", "citation_keywords"):
        keywords = _as_string_list(metadata.get(key))
        if keywords:
            return keywords
    return []


def extract_reference_count(scrape_result: dict[str, Any]) -> int | None:
    markdown = scrape_result.get("markdown") or ""
    if "## References" not in markdown:
        return None

    tail = markdown.split("## References", 1)[1]
    count = 0
    for line in tail.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("## "):
            break
        if re.match(r"^\d+\.", stripped) or re.match(r"^\[\d+\]", stripped):
            count += 1
    return count or None


def extract_article_pdf_url(scrape_result: dict[str, Any], article_url: str) -> str | None:
    links = scrape_result.get("links") or []
    normalized_article_url = normalize_article_url(article_url)
    for link in links:
        if not isinstance(link, str):
            continue
        if link.startswith(normalized_article_url) and link.endswith(".pdf"):
            return link

    markdown = scrape_result.get("markdown") or ""
    for label, url in MARKDOWN_LINK_RE.findall(markdown):
        if label.strip().lower() == "download pdf" and url.lower().endswith(".pdf"):
            return url

    match = DOWNLOAD_PDF_LINE_RE.search(markdown)
    if match:
        return match.group(1)

    html = scrape_result.get("html") or ""
    if "Download PDF" in html or "download pdf" in html.lower():
        return f"{normalized_article_url}.pdf"

    metadata = scrape_result.get("metadata") or {}
    title = metadata.get("title")
    if isinstance(title, str) and title.strip():
        return f"{normalized_article_url}.pdf"

    return None


def extract_title(scrape_result: dict[str, Any], article_url: str) -> str | None:
    metadata = scrape_result.get("metadata") or {}
    raw_title = metadata.get("title")
    if isinstance(raw_title, str) and raw_title.strip():
        title = raw_title.strip()
        suffix = " | Nature Communications"
        if title.endswith(suffix):
            return title[: -len(suffix)].strip()
        return title

    markdown = scrape_result.get("markdown") or ""
    slug = slug_from_article_url(article_url)
    for line in markdown.splitlines():
        if line.startswith("# "):
            headline = line[2:].strip()
            if headline and headline != slug:
                return headline
    return None


def extract_abstract(scrape_result: dict[str, Any]) -> str | None:
    markdown = scrape_result.get("markdown") or ""
    marker = "## Abstract"
    if marker not in markdown:
        return None

    tail = markdown.split(marker, 1)[1]
    lines: list[str] = []
    for raw_line in tail.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("## "):
            break
        lines.append(line)
    if not lines:
        return None
    return " ".join(lines)


def extract_doi(scrape_result: dict[str, Any], article_url: str) -> str | None:
    markdown = scrape_result.get("markdown") or ""
    match = DOI_RE.search(markdown)
    if match:
        return match.group(1)
    slug = slug_from_article_url(article_url)
    return f"10.1038/{slug}"


def extract_published_date(scrape_result: dict[str, Any]) -> str | None:
    markdown = scrape_result.get("markdown") or ""
    match = DATE_RE.search(markdown)
    if match:
        return match.group(1)
    return None


def extract_accepted_date(scrape_result: dict[str, Any]) -> str | None:
    markdown = scrape_result.get("markdown") or ""
    match = ACCEPTED_RE.search(markdown)
    if match:
        return match.group(1)
    return None


def extract_received_date(scrape_result: dict[str, Any]) -> str | None:
    markdown = scrape_result.get("markdown") or ""
    match = RECEIVED_RE.search(markdown)
    if match:
        return match.group(1)
    return None


def build_detailed_metadata(article_url: str, scrape_result: dict[str, Any]) -> dict[str, Any]:
    metadata = scrape_result.get("metadata") or {}
    body_markdown = scrape_result.get("markdown")
    abstract = extract_abstract(scrape_result)
    authors = extract_authors(scrape_result)
    section_headings = extract_section_headings(scrape_result)
    keywords = extract_keywords(scrape_result)

    return {
        "source": "firecrawl",
        "source_url": normalize_article_url(article_url),
        "scrape_formats": ["markdown", "html", "links"],
        "content_stats": {
            "body_markdown_chars": len(body_markdown) if isinstance(body_markdown, str) else 0,
            "body_markdown_words": len(body_markdown.split()) if isinstance(body_markdown, str) and body_markdown.strip() else 0,
            "abstract_chars": len(abstract) if isinstance(abstract, str) else 0,
            "abstract_words": len(abstract.split()) if isinstance(abstract, str) and abstract.strip() else 0,
            "section_count": len(section_headings),
            "reference_count": extract_reference_count(scrape_result),
        },
        "dates": {
            "received": extract_received_date(scrape_result),
            "accepted": extract_accepted_date(scrape_result),
            "published": extract_published_date(scrape_result),
        },
        "authors": authors,
        "keywords": keywords,
        "section_headings": section_headings,
        "links": {
            "article_url": normalize_article_url(article_url),
            "article_pdf_url": extract_article_pdf_url(scrape_result, article_url),
            "peer_review_pdf_url": extract_peer_review_url(scrape_result),
        },
        "raw_metadata_keys": sorted(metadata.keys()) if isinstance(metadata, dict) else [],
        "metadata_summary": {
            "title": metadata.get("title") if isinstance(metadata, dict) else None,
            "description": metadata.get("description") if isinstance(metadata, dict) else None,
            "language": metadata.get("language") if isinstance(metadata, dict) else None,
            "url": metadata.get("url") if isinstance(metadata, dict) else None,
            "og_title": metadata.get("og_title") if isinstance(metadata, dict) else None,
            "og_description": metadata.get("og_description") if isinstance(metadata, dict) else None,
        },
    }


def record_from_scrape(article_url: str, scrape_result: dict[str, Any]) -> ArticleRecord:
    metadata = scrape_result.get("metadata") or {}
    markdown = scrape_result.get("markdown")
    article_pdf_url = extract_article_pdf_url(scrape_result, article_url)
    peer_review_pdf_url = extract_peer_review_url(scrape_result)
    return ArticleRecord(
        article_url=normalize_article_url(article_url),
        slug=slug_from_article_url(article_url),
        title=extract_title(scrape_result, article_url),
        doi=extract_doi(scrape_result, article_url),
        journal="Nature Communications",
        published_date=extract_published_date(scrape_result),
        abstract=extract_abstract(scrape_result),
        body_markdown=markdown if isinstance(markdown, str) and markdown.strip() else None,
        article_pdf_url=article_pdf_url,
        peer_review_pdf_url=peer_review_pdf_url,
        detailed_metadata=build_detailed_metadata(article_url, scrape_result),
        metadata=metadata if isinstance(metadata, dict) else {},
    )
