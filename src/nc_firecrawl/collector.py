from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import sys
import threading
from pathlib import Path
from typing import Any, Callable, Iterable

from .config import Settings
from .firecrawl_client import FirecrawlClient
from .native_client import NativeNatureClient
from .models import ArticleRecord
from .nature import (
    DEFAULT_ARCHIVE_URL,
    DEFAULT_SITE_ARCHIVE_URLS,
    is_nc_article_url,
    normalize_article_url,
    record_from_scrape,
    slug_from_article_url,
)
from .rate_limit import RateLimiter
from .sitemap import DEFAULT_SITEMAP_URL, NatureSitemapDiscoverer
from .storage import Storage


class Collector:
    def __init__(
        self,
        settings: Settings,
        client: Any | None = None,
        storage: Storage | None = None,
    ) -> None:
        self.settings = settings
        self.client = client or FirecrawlClient(api_key=settings.firecrawl_api_key)
        self.storage = storage or Storage(
            output_dir=settings.output_dir,
            timeout_seconds=settings.request_timeout_seconds,
        )
        self.rate_limiter = RateLimiter(settings.requests_per_second)
        self.sitemap_discoverer = NatureSitemapDiscoverer(timeout_seconds=settings.request_timeout_seconds)

    def discover(self, query: str, limit: int) -> list[str]:
        return self.client.search_article_urls(query=query, limit=limit)

    def discover_archive(
        self,
        archive_url: str = DEFAULT_ARCHIVE_URL,
        max_pages: int | None = 1,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[str]:
        return self.client.discover_archive_article_urls(
            archive_url=archive_url,
            max_pages=max_pages,
            progress_callback=progress_callback,
        )

    def discover_site(
        self,
        archive_urls: list[str] | None = None,
        max_pages: int | None = 5,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[str]:
        return self.client.discover_site_article_urls(
            archive_urls=archive_urls or DEFAULT_SITE_ARCHIVE_URLS,
            max_pages=max_pages,
            progress_callback=progress_callback,
        )

    def discover_sitemap(
        self,
        sitemap_url: str = DEFAULT_SITEMAP_URL,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[str]:
        return self.sitemap_discoverer.discover_article_urls(
            sitemap_url=sitemap_url,
            progress_callback=progress_callback,
        )

    def collect_urls(
        self,
        urls: Iterable[str],
        download_peer_reviews: bool = True,
        resume: bool = True,
        retry_failed: bool = True,
    ) -> list[ArticleRecord]:
        self.storage.ensure_layout()
        records: list[ArticleRecord] = []
        result_slugs: set[str] = set()
        seen: set[str] = set()
        existing_records = self.storage.load_existing_records() if resume else {}
        crawl_cache = self.storage.load_crawl_cache()
        duplicate_index = self._build_duplicate_index(existing_records.values())
        batch_duplicate_index: dict[str, ArticleRecord] = {}
        lock = threading.Lock()
        pending_urls: list[str] = []

        for raw_url in urls:
            normalized = normalize_article_url(raw_url)
            if normalized in seen or not is_nc_article_url(normalized):
                continue
            seen.add(normalized)
            slug = slug_from_article_url(normalized)
            cache_entry = crawl_cache.get(normalized)
            if resume and self._should_skip_cached_url(cache_entry, retry_failed=retry_failed):
                cached_record = self._record_from_cache(cache_entry, existing_records)
                if cached_record is not None:
                    self._append_unique_record(records, result_slugs, cached_record)
                continue
            duplicate_match = self._find_existing_duplicate(
                article_url=normalized,
                slug=slug,
                existing_records=existing_records,
                duplicate_index=duplicate_index,
            )
            if resume and duplicate_match is not None:
                self._mark_success_cache(normalized, duplicate_match.slug)
                self._append_unique_record(records, result_slugs, duplicate_match)
                continue
            pending_urls.append(normalized)

        if not pending_urls:
            return records

        max_workers = max(1, self.settings.max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._scrape_single, url, download_peer_reviews) for url in pending_urls]
            for future in as_completed(futures):
                normalized_url, record, error_message = future.result()
                with lock:
                    if error_message is not None:
                        attempted_at = self._utc_now()
                        self.storage.mark_crawl_attempt(
                            url=normalized_url,
                            normalized_url=normalized_url,
                            status="failed",
                            attempted_at=attempted_at,
                            error_message=error_message,
                        )
                        continue
                    duplicate_match = self._find_duplicate_record(record, existing_records, duplicate_index)
                    if resume and duplicate_match is not None:
                        self._mark_success_cache(normalized_url, duplicate_match.slug)
                        self._append_unique_record(records, result_slugs, duplicate_match)
                        continue
                    batch_duplicate_match = self._find_record_in_index(record, batch_duplicate_index)
                    if batch_duplicate_match is not None:
                        self._mark_success_cache(normalized_url, batch_duplicate_match.slug)
                        self._append_unique_record(records, result_slugs, batch_duplicate_match)
                        continue
                    self.storage.append_record(record)
                    self._mark_success_cache(normalized_url, record.slug)
                    existing_records[record.slug] = record
                    self._add_to_duplicate_index(duplicate_index, record)
                    self._add_to_duplicate_index(batch_duplicate_index, record)
                    self._append_unique_record(records, result_slugs, record)

        return records

    def collect_archive(
        self,
        archive_url: str = DEFAULT_ARCHIVE_URL,
        max_pages: int | None = 1,
        download_peer_reviews: bool = True,
        resume: bool = True,
        retry_failed: bool = True,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
        _start_page: int | None = None,
    ) -> list[ArticleRecord]:
        self.storage.ensure_layout()
        records: list[ArticleRecord] = []

        # Page-level resume: find the first page not yet fully visited and skip ahead.
        if _start_page is not None:
            start_page = _start_page
        elif resume:
            start_page = self.storage.get_resume_page_number(archive_url)
            if start_page > 1:
                print(
                    f"[page-resume] Resuming from page {start_page} "
                    f"(pages 1–{start_page - 1} already fully crawled for {archive_url})",
                    file=sys.stderr,
                )
        else:
            start_page = 1

        for page_number, page_url, new_urls, page_urls in self.client.iter_archive_article_url_pages(
            archive_url=archive_url,
            max_pages=max_pages,
            progress_callback=progress_callback,
            start_page=start_page,
        ):
            if page_urls and len(page_urls) < 20:
                self.storage.append_underfilled_page(
                    {
                        "archive_url": archive_url,
                        "page_number": page_number,
                        "page_url": page_url,
                        "page_discovered": len(page_urls),
                    }
                )
            if new_urls:
                records.extend(
                    self.collect_urls(
                        urls=new_urls,
                        download_peer_reviews=download_peer_reviews,
                        resume=resume,
                        retry_failed=retry_failed,
                    )
                )
            # Mark the page as visited AFTER articles are processed so that a
            # crash mid-page does not permanently hide its articles on the next
            # resume (the page will be re-visited and article-level crawl_cache
            # deduplication takes care of already-downloaded articles).
            self.storage.mark_page_visited(archive_url, page_number, len(page_urls))
        return records

    def fill_gaps_archive(
        self,
        archive_url: str = DEFAULT_ARCHIVE_URL,
        max_pages: int | None = None,
        download_peer_reviews: bool = True,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[ArticleRecord]:
        """Re-visit every archive page to pick up articles missed in previous runs.

        Clears the page_crawl_log for *archive_url* so that all pages are
        re-checked from the beginning.  Articles already successfully crawled
        are skipped instantly via the article-level crawl_cache, so only
        genuinely missing articles are downloaded.
        """
        self.storage.ensure_layout()
        self.storage.clear_page_log(archive_url)
        print("[fill-gaps] Re-visiting all pages from page 1 (already-crawled articles will be skipped).", file=sys.stderr)
        return self.collect_archive(
            archive_url=archive_url,
            max_pages=max_pages,
            download_peer_reviews=download_peer_reviews,
            resume=True,
            _start_page=1,
            retry_failed=True,
            progress_callback=progress_callback,
        )

    def collect_site(
        self,
        archive_urls: list[str] | None = None,
        max_pages: int | None = 5,
        download_peer_reviews: bool = True,
        resume: bool = True,
        retry_failed: bool = True,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[ArticleRecord]:
        records: list[ArticleRecord] = []
        for archive_url in archive_urls or DEFAULT_SITE_ARCHIVE_URLS:
            records.extend(
                self.collect_archive(
                    archive_url=archive_url,
                    max_pages=max_pages,
                    download_peer_reviews=download_peer_reviews,
                    resume=resume,
                    retry_failed=retry_failed,
                    progress_callback=progress_callback,
                )
            )
        return records

    def collect_sitemap(
        self,
        sitemap_url: str = DEFAULT_SITEMAP_URL,
        download_peer_reviews: bool = True,
        resume: bool = True,
        retry_failed: bool = True,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> list[ArticleRecord]:
        records: list[ArticleRecord] = []
        for _, new_urls in self.sitemap_discoverer.iter_article_url_batches(
            sitemap_url=sitemap_url,
            progress_callback=progress_callback,
        ):
            if not new_urls:
                continue
            records.extend(
                self.collect_urls(
                    urls=new_urls,
                    download_peer_reviews=download_peer_reviews,
                    resume=resume,
                    retry_failed=retry_failed,
                )
            )
        return records

    def stats(self) -> dict[str, int | str | None]:
        return self.storage.stats()

    def _scrape_single(self, normalized_url: str, download_peer_reviews: bool) -> tuple[str, ArticleRecord | None, str | None]:
        try:
            self.rate_limiter.wait()
            scrape_result = self.client.scrape_article(normalized_url)
            record = record_from_scrape(normalized_url, scrape_result)

            markdown = scrape_result.get("markdown")
            if isinstance(markdown, str) and markdown.strip():
                markdown_path = self.storage.save_markdown(record.slug, markdown)
                record.markdown_path = str(markdown_path)

            if record.article_pdf_url:
                self.rate_limiter.wait()
                article_pdf_path = self.storage.article_pdf_dir / f"{record.slug}.pdf"
                self.storage.download_binary(record.article_pdf_url, article_pdf_path)
                record.article_pdf_path = str(article_pdf_path)

            if download_peer_reviews and record.peer_review_pdf_url:
                self.rate_limiter.wait()
                pdf_path = self.storage.peer_review_dir / f"{record.slug}-peer-review.pdf"
                self.storage.download_binary(record.peer_review_pdf_url, pdf_path)
                record.peer_review_pdf_path = str(pdf_path)

            return normalized_url, record, None
        except Exception as exc:
            return normalized_url, None, str(exc)

    @staticmethod
    def _build_duplicate_index(records: Iterable[ArticleRecord]) -> dict[str, ArticleRecord]:
        index: dict[str, ArticleRecord] = {}
        for record in records:
            for key in record.duplicate_keys():
                index.setdefault(key, record)
        return index

    @staticmethod
    def _add_to_duplicate_index(index: dict[str, ArticleRecord], record: ArticleRecord) -> None:
        for key in record.duplicate_keys():
            index.setdefault(key, record)

    @staticmethod
    def _find_existing_duplicate(
        article_url: str,
        slug: str,
        existing_records: dict[str, ArticleRecord],
        duplicate_index: dict[str, ArticleRecord],
    ) -> ArticleRecord | None:
        if slug in existing_records:
            return existing_records[slug]
        return duplicate_index.get(f"url:{article_url.lower()}")

    @staticmethod
    def _find_duplicate_record(
        record: ArticleRecord,
        existing_records: dict[str, ArticleRecord],
        duplicate_index: dict[str, ArticleRecord],
    ) -> ArticleRecord | None:
        if record.slug in existing_records:
            return existing_records[record.slug]
        for key in record.duplicate_keys():
            matched = duplicate_index.get(key)
            if matched is not None:
                return matched
        return None

    @staticmethod
    def _find_record_in_index(record: ArticleRecord, duplicate_index: dict[str, ArticleRecord]) -> ArticleRecord | None:
        for key in record.duplicate_keys():
            matched = duplicate_index.get(key)
            if matched is not None:
                return matched
        return None

    @staticmethod
    def _append_unique_record(records: list[ArticleRecord], result_slugs: set[str], record: ArticleRecord) -> None:
        if record.slug in result_slugs:
            return
        result_slugs.add(record.slug)
        records.append(record)

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _should_skip_cached_url(cache_entry: dict[str, str | None] | None, retry_failed: bool) -> bool:
        if not cache_entry:
            return False
        status = cache_entry.get("status")
        if status == "success":
            return True
        if status == "failed" and not retry_failed:
            return True
        return False

    @staticmethod
    def _record_from_cache(
        cache_entry: dict[str, str | None] | None,
        existing_records: dict[str, ArticleRecord],
    ) -> ArticleRecord | None:
        if not cache_entry:
            return None
        article_slug = cache_entry.get("article_slug")
        if not article_slug:
            return None
        return existing_records.get(article_slug)

    def _mark_success_cache(self, normalized_url: str, article_slug: str) -> None:
        now = self._utc_now()
        self.storage.mark_crawl_attempt(
            url=normalized_url,
            normalized_url=normalized_url,
            status="success",
            attempted_at=now,
            succeeded_at=now,
            article_slug=article_slug,
        )


def load_urls_from_file(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]
