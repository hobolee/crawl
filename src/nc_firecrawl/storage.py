from __future__ import annotations

import json
from pathlib import Path

import requests

from .models import ArticleRecord
from .sqlite_index import SQLiteIndex


class Storage:
    def __init__(self, output_dir: Path, timeout_seconds: int = 90) -> None:
        self.output_dir = output_dir
        self.timeout_seconds = timeout_seconds
        self.markdown_dir = output_dir / "article_markdown"
        self.article_pdf_dir = output_dir / "article_pdfs"
        self.records_dir = output_dir / "article_records"
        self.peer_review_dir = output_dir / "peer_reviews"
        self.articles_jsonl = output_dir / "articles.jsonl"
        self.underfilled_pages_jsonl = output_dir / "underfilled_pages.jsonl"
        self.index_db = output_dir / "articles.sqlite"
        self.sqlite_index = SQLiteIndex(self.index_db)

    def ensure_layout(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.markdown_dir.mkdir(parents=True, exist_ok=True)
        self.article_pdf_dir.mkdir(parents=True, exist_ok=True)
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.peer_review_dir.mkdir(parents=True, exist_ok=True)
        self.sqlite_index.initialize()

    def save_markdown(self, slug: str, markdown: str) -> Path:
        path = self.markdown_dir / f"{slug}.md"
        path.write_text(markdown, encoding="utf-8")
        return path

    def download_binary(self, url: str, destination: Path) -> Path:
        response = requests.get(url, timeout=self.timeout_seconds)
        response.raise_for_status()
        destination.write_bytes(response.content)
        return destination

    def append_record(self, record: ArticleRecord) -> None:
        record.record_path = str(self.records_dir / f"{record.slug}.json")
        self.save_full_record(record)
        with self.articles_jsonl.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record.to_summary_dict(), ensure_ascii=False) + "\n")
        self.sqlite_index.upsert_record(record)

    def save_full_record(self, record: ArticleRecord) -> Path:
        path = Path(record.record_path) if record.record_path else self.records_dir / f"{record.slug}.json"
        path.write_text(json.dumps(record.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def load_existing_records(self) -> dict[str, ArticleRecord]:
        sqlite_records = self.sqlite_index.load_all_records()
        if sqlite_records:
            return sqlite_records
        if not self.articles_jsonl.exists():
            return {}

        records: dict[str, ArticleRecord] = {}
        with self.articles_jsonl.open("r", encoding="utf-8") as fh:
            for line in fh:
                stripped = line.strip()
                if not stripped:
                    continue
                payload = json.loads(stripped)
                if not isinstance(payload, dict):
                    continue
                slug = payload.get("slug")
                if not isinstance(slug, str) or not slug:
                    continue
                normalized_payload = {
                    "article_url": payload.get("article_url"),
                    "slug": slug,
                    "title": payload.get("title"),
                    "doi": payload.get("doi"),
                    "journal": payload.get("journal"),
                    "published_date": payload.get("published_date"),
                    "abstract": payload.get("abstract"),
                    "body_markdown": payload.get("body_markdown"),
                    "article_pdf_url": payload.get("article_pdf_url"),
                    "peer_review_pdf_url": payload.get("peer_review_pdf_url"),
                    "markdown_path": payload.get("markdown_path"),
                    "article_pdf_path": payload.get("article_pdf_path"),
                    "peer_review_pdf_path": payload.get("peer_review_pdf_path"),
                    "record_path": payload.get("record_path"),
                    "detailed_metadata": payload.get("detailed_metadata") or {},
                    "metadata": payload.get("metadata") or {},
                }
                records[slug] = ArticleRecord(**normalized_payload)
        return records

    def append_underfilled_page(self, payload: dict[str, object]) -> None:
        with self.underfilled_pages_jsonl.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def load_crawl_cache(self) -> dict[str, dict[str, str | None]]:
        return self.sqlite_index.load_crawl_cache()

    def mark_crawl_attempt(
        self,
        url: str,
        normalized_url: str,
        status: str,
        attempted_at: str,
        article_slug: str | None = None,
        error_message: str | None = None,
        succeeded_at: str | None = None,
    ) -> None:
        self.sqlite_index.mark_crawl_attempt(
            url=url,
            normalized_url=normalized_url,
            status=status,
            attempted_at=attempted_at,
            article_slug=article_slug,
            error_message=error_message,
            succeeded_at=succeeded_at,
        )

    def clear_page_log(self, archive_url: str) -> int:
        return self.sqlite_index.clear_page_log(archive_url)

    def mark_page_visited(self, archive_url: str, page_number: int, articles_found: int) -> None:
        from datetime import datetime, timezone
        visited_at = datetime.now(timezone.utc).isoformat()
        self.sqlite_index.mark_page_visited(archive_url, page_number, articles_found, visited_at)

    def get_resume_page_number(self, archive_url: str) -> int:
        return self.sqlite_index.get_resume_page_number(archive_url)

    def load_failed_urls(self) -> list[dict[str, str | None]]:
        return self.sqlite_index.load_failed_urls()

    def stats(self) -> dict[str, int | str | None]:
        return self.sqlite_index.crawl_cache_stats()
