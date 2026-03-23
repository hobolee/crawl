from __future__ import annotations

import re
from typing import Any, Callable

import requests

from .nature import (
    DEFAULT_SITE_ARCHIVE_URLS,
    archive_page_url,
    extract_article_urls_from_listing,
    is_nc_article_url,
    normalize_article_url,
    normalize_listing_url,
)


class NativeNatureClient:
    def __init__(self, timeout_seconds: int = 90) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
                )
            }
        )

    def search_article_urls(self, query: str, limit: int) -> list[str]:
        raise RuntimeError("The native engine does not support search-based discovery. Use --archive or --site.")

    def scrape_listing(self, url: str, retry_index: int = 0) -> dict[str, Any]:
        normalized = normalize_listing_url(url)
        response = self.session.get(normalized, timeout=self.timeout_seconds)
        if response.status_code == 404:
            # Beyond the last page — return an empty result so the caller can
            # detect end-of-archive cleanly without raising an exception.
            return {"html": "", "raw_html": "", "links": [], "markdown": "", "metadata": {"url": normalized}}
        response.raise_for_status()
        html = response.text
        return {
            "html": html,
            "raw_html": html,
            "links": self._extract_links(html, normalized),
            "markdown": "",
            "metadata": {"url": normalized, "title": self._extract_title(html)},
        }

    def scrape_article(self, url: str) -> dict[str, Any]:
        normalized = normalize_article_url(url)
        response = self.session.get(normalized, timeout=self.timeout_seconds)
        response.raise_for_status()
        html = response.text
        BeautifulSoup = _load_bs4()
        soup = BeautifulSoup(html, "html.parser")
        article_body = self._extract_article_markdown(soup)
        links = self._extract_links(html, normalized)
        metadata = self._extract_metadata(soup, normalized)
        return {
            "html": html,
            "raw_html": html,
            "links": links,
            "markdown": article_body,
            "metadata": metadata,
        }

    def iter_archive_article_url_pages(
        self,
        archive_url: str,
        max_pages: int | None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        start_page: int = 1,
    ):
        seen: set[str] = set()
        page_number = max(1, start_page)
        cumulative = 0
        while True:
            if max_pages is not None and page_number > max_pages:
                break
            page_url = archive_page_url(archive_url, page_number)
            retry_count = 0
            page_urls: list[str] = []
            while True:
                scrape_result = self.scrape_listing(page_url, retry_index=retry_count)
                page_urls = extract_article_urls_from_listing(scrape_result, base_url=page_url)
                if not page_urls or len(page_urls) >= 20 or retry_count >= 1:
                    break
                retry_count += 1
            new_urls: list[str] = []
            for article_url in page_urls:
                if article_url in seen:
                    continue
                seen.add(article_url)
                new_urls.append(article_url)
            cumulative += len(new_urls)
            if progress_callback is not None:
                progress_callback(
                    {
                        "scope": "archive",
                        "archive_url": archive_url,
                        "page_number": page_number,
                        "page_url": page_url,
                        "page_discovered": len(page_urls),
                        "page_new": len(new_urls),
                        "cumulative_discovered": cumulative,
                        "retry_count": retry_count,
                        "underfilled": bool(page_urls) and len(page_urls) < 20,
                    }
                )
            yield page_number, page_url, new_urls, page_urls
            if not page_urls:
                # Page is empty → we have reached the end of the archive.
                break
            # NOTE: do NOT break when new_urls is empty.  That only means all
            # articles on this page were already seen in the current run (e.g.
            # the archive shifted between requests).  Pages beyond may still
            # have unseen articles.
            page_number += 1

    def discover_archive_article_urls(
        self,
        archive_url: str,
        max_pages: int | None,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[str]:
        urls: list[str] = []
        for _, _, new_urls, _ in self.iter_archive_article_url_pages(
            archive_url=archive_url,
            max_pages=max_pages,
            progress_callback=progress_callback,
        ):
            urls.extend(new_urls)
        return urls

    def discover_site_article_urls(
        self,
        archive_urls: list[str] | None = None,
        max_pages: int | None = 5,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for archive_url in archive_urls or DEFAULT_SITE_ARCHIVE_URLS:
            for url in self.discover_archive_article_urls(
                archive_url=archive_url,
                max_pages=max_pages,
                progress_callback=progress_callback,
            ):
                if url in seen:
                    continue
                seen.add(url)
                urls.append(url)
        return urls

    @staticmethod
    def _extract_links(html: str, base_url: str) -> list[str]:
        BeautifulSoup = _load_bs4()
        soup = BeautifulSoup(html, "html.parser")
        links: list[str] = []
        seen: set[str] = set()
        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href:
                continue
            if href.startswith("/"):
                href = f"https://www.nature.com{href}"
            if href.startswith("http") and href not in seen:
                seen.add(href)
                links.append(href)
        return links

    @staticmethod
    def _extract_title(html: str) -> str | None:
        BeautifulSoup = _load_bs4()
        soup = BeautifulSoup(html, "html.parser")
        if soup.title and soup.title.string:
            return soup.title.string.strip()
        return None

    @staticmethod
    def _extract_metadata(soup: Any, url: str) -> dict[str, Any]:
        metadata: dict[str, Any] = {"url": url}
        if soup.title and soup.title.string:
            metadata["title"] = soup.title.string.strip()
        for tag in soup.find_all("meta"):
            key = tag.get("name") or tag.get("property")
            content = tag.get("content")
            if key and content:
                metadata[key.replace(":", "_").replace(".", "_")] = content.strip()
        return metadata

    @staticmethod
    def _extract_article_markdown(soup: Any) -> str:
        parts: list[str] = []

        title = soup.select_one("h1.c-article-title, h1")
        if title:
            parts.append(f"# {title.get_text(' ', strip=True)}")

        authors = [node.get_text(" ", strip=True) for node in soup.select("ul.c-article-author-list li")]
        if authors:
            parts.append(", ".join(authors))

        abstract = soup.select_one("div.c-article-section__content, section[aria-labelledby*=abstract] .c-article-section__content")
        if abstract:
            parts.append("## Abstract")
            parts.append(abstract.get_text("\n", strip=True))

        body_sections = soup.select("section.c-article-section")
        for section in body_sections:
            heading = section.find(["h2", "h3"])
            heading_text = heading.get_text(" ", strip=True) if heading else None
            if heading_text:
                parts.append(f"## {heading_text}")
            paragraphs = [p.get_text(" ", strip=True) for p in section.find_all(["p", "li"]) if p.get_text(" ", strip=True)]
            if paragraphs:
                parts.append("\n".join(paragraphs))

        if len(parts) <= 2:
            main = soup.select_one("main")
            if main:
                text = main.get_text("\n", strip=True)
                if text:
                    parts.append(text)

        markdown = "\n\n".join(part for part in parts if part)
        markdown = re.sub(r"\n{3,}", "\n\n", markdown).strip()
        return markdown


def _load_bs4():
    try:
        from bs4 import BeautifulSoup
    except ImportError as exc:
        raise RuntimeError("beautifulsoup4 is not installed. Run `pip install -e .` first.") from exc
    return BeautifulSoup
