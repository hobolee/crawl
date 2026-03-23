from __future__ import annotations

from collections import deque
from typing import Any, Callable
from xml.etree import ElementTree

import requests

from .nature import is_nc_article_url, normalize_article_url


DEFAULT_SITEMAP_URL = "https://www.nature.com/sitemap.xml"


class NatureSitemapDiscoverer:
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

    def iter_article_url_batches(
        self,
        sitemap_url: str = DEFAULT_SITEMAP_URL,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ):
        pending = deque([sitemap_url])
        visited_sitemaps: set[str] = set()
        seen_article_urls: set[str] = set()
        processed_sitemaps = 0

        while pending:
            current = pending.popleft()
            if current in visited_sitemaps:
                continue
            visited_sitemaps.add(current)
            xml_text = self._fetch_text(current)
            root = ElementTree.fromstring(xml_text)
            namespace = self._namespace(root.tag)
            sitemap_entries = self._find_locs(root, f".//{{{namespace}}}sitemap/{{{namespace}}}loc")
            if sitemap_entries:
                for loc in sitemap_entries:
                    if loc not in visited_sitemaps:
                        pending.append(loc)
                processed_sitemaps += 1
                if progress_callback is not None:
                    progress_callback(
                        {
                            "scope": "sitemap",
                            "sitemap_url": current,
                            "kind": "index",
                            "queued_sitemaps": len(sitemap_entries),
                            "processed_sitemaps": processed_sitemaps,
                            "cumulative_discovered": len(seen_article_urls),
                        }
                    )
                continue

            url_entries = self._find_locs(root, f".//{{{namespace}}}url/{{{namespace}}}loc")
            new_urls: list[str] = []
            for loc in url_entries:
                normalized = normalize_article_url(loc)
                if not is_nc_article_url(normalized) or normalized in seen_article_urls:
                    continue
                seen_article_urls.add(normalized)
                new_urls.append(normalized)
            processed_sitemaps += 1
            if progress_callback is not None:
                progress_callback(
                    {
                        "scope": "sitemap",
                        "sitemap_url": current,
                        "kind": "urlset",
                        "url_count": len(url_entries),
                        "new_count": len(new_urls),
                        "processed_sitemaps": processed_sitemaps,
                        "cumulative_discovered": len(seen_article_urls),
                    }
                )
            if new_urls:
                yield current, new_urls

    def discover_article_urls(
        self,
        sitemap_url: str = DEFAULT_SITEMAP_URL,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[str]:
        urls: list[str] = []
        for _, batch in self.iter_article_url_batches(sitemap_url=sitemap_url, progress_callback=progress_callback):
            urls.extend(batch)
        return urls

    def _fetch_text(self, url: str) -> str:
        response = self.session.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.text

    @staticmethod
    def _namespace(tag: str) -> str:
        if tag.startswith("{"):
            return tag[1:].split("}", 1)[0]
        return ""

    @staticmethod
    def _find_locs(root: ElementTree.Element, path: str) -> list[str]:
        locs: list[str] = []
        for node in root.findall(path):
            if node.text and node.text.strip():
                locs.append(node.text.strip())
        return locs
