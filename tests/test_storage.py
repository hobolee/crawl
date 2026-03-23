from __future__ import annotations

from nc_firecrawl.models import ArticleRecord
from nc_firecrawl.storage import Storage


def test_storage_writes_and_reads_sqlite_index(tmp_path) -> None:
    storage = Storage(tmp_path / "data")
    storage.ensure_layout()

    record = ArticleRecord(
        article_url="https://www.nature.com/articles/s41467-026-70240-6",
        slug="s41467-026-70240-6",
        title="Title",
        doi="10.1038/s41467-026-70240-6",
        journal="Nature Communications",
        detailed_metadata={"authors": ["Alice Smith"]},
        metadata={"title": "Title | Nature Communications"},
    )

    storage.append_record(record)

    loaded = storage.load_existing_records()
    assert "s41467-026-70240-6" in loaded
    assert loaded["s41467-026-70240-6"].doi == "10.1038/s41467-026-70240-6"
    assert storage.sqlite_index.count() == 1
    summary_line = storage.articles_jsonl.read_text(encoding="utf-8").strip()
    assert '"body_markdown"' not in summary_line
    assert '"metadata"' not in summary_line
    full_record = (storage.records_dir / "s41467-026-70240-6.json").read_text(encoding="utf-8")
    assert '"metadata"' in full_record
    assert '"record_path"' in full_record


def test_storage_persists_crawl_cache(tmp_path) -> None:
    storage = Storage(tmp_path / "data")
    storage.ensure_layout()

    storage.mark_crawl_attempt(
        url="https://www.nature.com/articles/s41467-026-70240-6",
        normalized_url="https://www.nature.com/articles/s41467-026-70240-6",
        status="success",
        attempted_at="2026-03-17T00:00:00+00:00",
        succeeded_at="2026-03-17T00:00:01+00:00",
        article_slug="s41467-026-70240-6",
    )

    cache = storage.load_crawl_cache()
    assert cache["https://www.nature.com/articles/s41467-026-70240-6"]["status"] == "success"
    assert cache["https://www.nature.com/articles/s41467-026-70240-6"]["article_slug"] == "s41467-026-70240-6"


def test_storage_stats_reports_article_and_cache_counts(tmp_path) -> None:
    storage = Storage(tmp_path / "data")
    storage.ensure_layout()
    storage.append_record(
        ArticleRecord(
            article_url="https://www.nature.com/articles/ncomms12345",
            slug="ncomms12345",
            title="Legacy",
            doi="10.1038/ncomms12345",
            journal="Nature Communications",
        )
    )
    storage.mark_crawl_attempt(
        url="https://www.nature.com/articles/ncomms12345",
        normalized_url="https://www.nature.com/articles/ncomms12345",
        status="success",
        attempted_at="2026-03-17T01:00:00+00:00",
        succeeded_at="2026-03-17T01:00:01+00:00",
        article_slug="ncomms12345",
    )
    storage.mark_crawl_attempt(
        url="https://www.nature.com/articles/s41467-026-70240-6",
        normalized_url="https://www.nature.com/articles/s41467-026-70240-6",
        status="failed",
        attempted_at="2026-03-17T02:00:00+00:00",
        error_message="boom",
    )

    stats = storage.stats()
    assert stats["article_records"] == 1
    assert stats["cached_urls"] == 2
    assert stats["successful_urls"] == 1
    assert stats["failed_urls"] == 1
    assert stats["latest_attempted_at"] == "2026-03-17T02:00:00+00:00"
