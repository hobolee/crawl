from __future__ import annotations

from typing import Any, Callable

from .nature import (
    DEFAULT_SITE_ARCHIVE_URLS,
    archive_page_url,
    extract_article_urls_from_listing,
    is_nc_article_url,
    normalize_article_url,
    normalize_listing_url,
)


class FirecrawlClient:
    def __init__(self, api_key: str) -> None:
        try:
            from firecrawl import Firecrawl as ClientClass
        except ImportError as exc:
            try:
                from firecrawl import FirecrawlApp as ClientClass
            except ImportError:
                raise RuntimeError(
                    "firecrawl-py is not installed. Run `pip install -e '.[dev]'` first."
                ) from exc

        self._client = ClientClass(api_key=api_key)

    def search_article_urls(self, query: str, limit: int) -> list[str]:
        response = self._normalize_response(self._client.search(query=query, limit=limit))
        web_results = response.get("web")
        if not isinstance(web_results, list):
            data = response.get("data")
            if isinstance(data, dict):
                web_results = data.get("web", [])
            else:
                web_results = []
        urls: list[str] = []
        seen: set[str] = set()
        for item in web_results:
            normalized_item = self._normalize_response(item)
            if not isinstance(normalized_item, dict):
                continue
            url = normalized_item.get("url")
            if not isinstance(url, str):
                continue
            normalized = normalize_article_url(url)
            if not is_nc_article_url(normalized) or normalized in seen:
                continue
            seen.add(normalized)
            urls.append(normalized)
        return urls

    def scrape_listing(self, url: str, retry_index: int = 0) -> dict[str, Any]:
        normalized = normalize_listing_url(url)
        actions = [
            {"type": "wait", "milliseconds": 1000 + retry_index * 1000},
            {"type": "scroll", "direction": "down"},
            {"type": "wait", "milliseconds": 1000},
            {"type": "scroll", "direction": "down"},
            {"type": "wait", "milliseconds": 1000},
        ]
        return self._scrape(
            normalized,
            formats=["markdown", "html", "rawHtml", "links"],
            only_main_content=False,
            timeout=120000,
            wait_for=2000 + retry_index * 1500,
            max_age=0,
            actions=actions,
        )

    def scrape_article(self, url: str) -> dict[str, Any]:
        normalized = normalize_article_url(url) if is_nc_article_url(url) else normalize_listing_url(url)
        result = self._scrape(
            normalized,
            formats=["markdown", "html", "links"],
            only_main_content=False,
            timeout=120000,
        )
        return result

    def _scrape(self, normalized: str, **kwargs: Any) -> dict[str, Any]:
        if hasattr(self._client, "scrape"):
            result = self._client.scrape(normalized, **kwargs)
        else:
            result = self._client.scrape_url(normalized, **kwargs)
        normalized_result = self._normalize_response(result)
        if not isinstance(normalized_result, dict):
            raise RuntimeError(f"Unexpected scrape response type for {normalized!r}: {type(result)!r}")
        return normalized_result

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
                # Non-final pages on Nature listings should usually have 20 articles.
                if not page_urls or len(page_urls) >= 20 or retry_count >= 1:
                    break
                retry_count += 1
            new_urls: list[str] = []
            for url in page_urls:
                if url in seen:
                    continue
                seen.add(url)
                new_urls.append(url)
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
                break
            # Do NOT break when new_urls is empty — pages beyond may still have unseen articles.
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
    def _normalize_response(value: Any) -> Any:
        if isinstance(value, dict):
            return value
        if hasattr(value, "model_dump"):
            return value.model_dump()
        if hasattr(value, "dict"):
            return value.dict()
        return value
