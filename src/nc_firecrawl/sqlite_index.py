from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

from .models import ArticleRecord


class SQLiteIndex:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS articles (
                    slug TEXT PRIMARY KEY,
                    article_url TEXT,
                    doi TEXT,
                    title TEXT,
                    journal TEXT,
                    article_pdf_url TEXT,
                    peer_review_pdf_url TEXT,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS crawl_cache (
                    url TEXT PRIMARY KEY,
                    normalized_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    last_attempted_at TEXT NOT NULL,
                    last_succeeded_at TEXT,
                    article_slug TEXT,
                    error_message TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS page_crawl_log (
                    archive_url TEXT NOT NULL,
                    page_number INTEGER NOT NULL,
                    articles_found INTEGER NOT NULL DEFAULT 0,
                    visited_at TEXT NOT NULL,
                    PRIMARY KEY (archive_url, page_number)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(article_url)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_doi ON articles(doi)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_title_journal ON articles(title, journal)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_cache_status ON crawl_cache(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_cache_slug ON crawl_cache(article_slug)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_page_crawl_log ON page_crawl_log(archive_url, page_number)")
            conn.commit()

    def upsert_record(self, record: ArticleRecord) -> None:
        payload_json = json.dumps(record.to_dict(), ensure_ascii=False)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO articles (
                        slug, article_url, doi, title, journal, article_pdf_url, peer_review_pdf_url, payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(slug) DO UPDATE SET
                        article_url=excluded.article_url,
                        doi=excluded.doi,
                        title=excluded.title,
                        journal=excluded.journal,
                        article_pdf_url=excluded.article_pdf_url,
                        peer_review_pdf_url=excluded.peer_review_pdf_url,
                        payload_json=excluded.payload_json
                    """,
                    (
                        record.slug,
                        record.article_url,
                        record.doi,
                        record.title,
                        record.journal,
                        record.article_pdf_url,
                        record.peer_review_pdf_url,
                        payload_json,
                    ),
                )
                conn.commit()

    def load_all_records(self) -> dict[str, ArticleRecord]:
        with self._connect() as conn:
            rows = conn.execute("SELECT payload_json FROM articles").fetchall()
        records: dict[str, ArticleRecord] = {}
        for (payload_json,) in rows:
            payload = json.loads(payload_json)
            record = ArticleRecord(**payload)
            records[record.slug] = record
        return records

    def count(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM articles").fetchone()
        return int(row[0]) if row else 0

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
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO crawl_cache (
                        url, normalized_url, status, last_attempted_at, last_succeeded_at, article_slug, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        normalized_url=excluded.normalized_url,
                        status=excluded.status,
                        last_attempted_at=excluded.last_attempted_at,
                        last_succeeded_at=CASE
                            WHEN excluded.last_succeeded_at IS NOT NULL THEN excluded.last_succeeded_at
                            ELSE crawl_cache.last_succeeded_at
                        END,
                        article_slug=CASE
                            WHEN excluded.article_slug IS NOT NULL THEN excluded.article_slug
                            ELSE crawl_cache.article_slug
                        END,
                        error_message=excluded.error_message
                    """,
                    (url, normalized_url, status, attempted_at, succeeded_at, article_slug, error_message),
                )
                conn.commit()

    def load_crawl_cache(self) -> dict[str, dict[str, str | None]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT url, normalized_url, status, last_attempted_at, last_succeeded_at, article_slug, error_message
                FROM crawl_cache
                """
            ).fetchall()
        cache: dict[str, dict[str, str | None]] = {}
        for row in rows:
            cache[row[0]] = {
                "url": row[0],
                "normalized_url": row[1],
                "status": row[2],
                "last_attempted_at": row[3],
                "last_succeeded_at": row[4],
                "article_slug": row[5],
                "error_message": row[6],
            }
        return cache

    def load_failed_urls(self) -> list[dict[str, str | None]]:
        """Return all crawl_cache rows with status='failed'."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT url, error_message, last_attempted_at FROM crawl_cache WHERE status = 'failed' ORDER BY url"
            ).fetchall()
        return [{"url": r[0], "error": r[1], "attempted_at": r[2]} for r in rows]

    def crawl_cache_stats(self) -> dict[str, int | str | None]:
        with self._connect() as conn:
            total_urls_row = conn.execute("SELECT COUNT(*) FROM crawl_cache").fetchone()
            success_row = conn.execute("SELECT COUNT(*) FROM crawl_cache WHERE status = 'success'").fetchone()
            failed_row = conn.execute("SELECT COUNT(*) FROM crawl_cache WHERE status = 'failed'").fetchone()
            latest_attempt_row = conn.execute("SELECT MAX(last_attempted_at) FROM crawl_cache").fetchone()
            latest_success_row = conn.execute("SELECT MAX(last_succeeded_at) FROM crawl_cache").fetchone()
            article_count_row = conn.execute("SELECT COUNT(*) FROM articles").fetchone()
        return {
            "article_records": int(article_count_row[0]) if article_count_row else 0,
            "cached_urls": int(total_urls_row[0]) if total_urls_row else 0,
            "successful_urls": int(success_row[0]) if success_row else 0,
            "failed_urls": int(failed_row[0]) if failed_row else 0,
            "latest_attempted_at": latest_attempt_row[0] if latest_attempt_row else None,
            "latest_succeeded_at": latest_success_row[0] if latest_success_row else None,
        }

    def clear_page_log(self, archive_url: str) -> int:
        """Delete all page_crawl_log rows for *archive_url*.  Returns the number of rows removed."""
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    "DELETE FROM page_crawl_log WHERE archive_url = ?", (archive_url,)
                )
                conn.commit()
                return cur.rowcount

    def mark_page_visited(self, archive_url: str, page_number: int, articles_found: int, visited_at: str) -> None:
        """Record that an archive listing page has been fully processed."""
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO page_crawl_log (archive_url, page_number, articles_found, visited_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(archive_url, page_number) DO UPDATE SET
                        articles_found = excluded.articles_found,
                        visited_at = excluded.visited_at
                    """,
                    (archive_url, page_number, articles_found, visited_at),
                )
                conn.commit()

    def get_resume_page_number(self, archive_url: str) -> int:
        """Return the first page number not yet fully visited (consecutive from 1).

        Priority:
        1. page_crawl_log table (accurate, populated during crawling).
        2. Fallback: estimate from article count when no log exists yet
           (e.g. databases created before this feature was added).
        """
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT page_number FROM page_crawl_log WHERE archive_url = ? ORDER BY page_number ASC",
                (archive_url,),
            ).fetchall()

        if rows:
            visited = {row[0] for row in rows}
            page = 1
            while page in visited:
                page += 1
            return page

        # Fallback: no page log yet — estimate from how many articles are stored.
        # integer-divide by 20 (articles per page) to get a conservative estimate
        # of completed pages.  It may be slightly low when pages were underfilled
        # or some articles failed, but it's safe: re-checking a few already-seen
        # pages costs only one listing-page request each and articles are skipped
        # via the article-level crawl_cache.
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM articles").fetchone()
        article_count = int(row[0]) if row else 0
        if article_count >= 20:
            return article_count // 20
        return 1

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path, check_same_thread=False)
