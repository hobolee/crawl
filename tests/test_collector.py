from __future__ import annotations

import json

from nc_firecrawl.collector import Collector
from nc_firecrawl.config import Settings
from nc_firecrawl.models import ArticleRecord
from nc_firecrawl.storage import Storage


class FakeClient:
    def __init__(self, fail_urls: set[str] | None = None) -> None:
        self.scrape_calls: list[str] = []
        self.fail_urls = fail_urls or set()

    def scrape_article(self, url: str):
        self.scrape_calls.append(url)
        if url in self.fail_urls:
            raise RuntimeError(f"boom for {url}")
        doi = "10.1038/s41467-026-70240-6"
        title = "Title | Nature Communications"
        if url.endswith("s41467-026-80000-1"):
            doi = "10.1038/s41467-026-70240-6"
            title = "Title | Nature Communications"
        return {
            "markdown": f"# Title\n{doi}\n## Abstract\nBody",
            "metadata": {"title": title, "authors": ["Alice Smith", "Bob Lee"]},
        }

    def search_article_urls(self, query: str, limit: int):
        return []

    def discover_archive_article_urls(self, archive_url: str, max_pages: int):
        return []

    def iter_archive_article_url_pages(self, archive_url: str, max_pages: int | None, progress_callback=None):
        pages = [
            (
                1,
                "https://www.nature.com/ncomms/research-articles?type=article",
                ["https://www.nature.com/articles/s41467-026-70240-6"],
                ["https://www.nature.com/articles/s41467-026-70240-6"],
            ),
            (
                2,
                "https://www.nature.com/ncomms/research-articles?type=article&page=2&searchType=journalSearch&sort=PubDate",
                ["https://www.nature.com/articles/s41467-026-70241-5"],
                ["https://www.nature.com/articles/s41467-026-70241-5"],
            ),
        ]
        cumulative = 0
        for page_number, page_url, new_urls, page_urls in pages:
            cumulative += len(new_urls)
            if progress_callback is not None:
                progress_callback(
                    {
                        "page_number": page_number,
                        "page_url": page_url,
                        "page_discovered": len(page_urls),
                        "page_new": len(new_urls),
                        "cumulative_discovered": cumulative,
                    }
                )
            yield page_number, page_url, new_urls, page_urls


class FakeStorage(Storage):
    def __init__(self, output_dir):
        super().__init__(output_dir)
        self.downloads: list[tuple[str, str]] = []

    def download_binary(self, url: str, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"fake")
        self.downloads.append((url, str(destination)))
        return destination


def test_collect_urls_resumes_from_existing_jsonl(tmp_path) -> None:
    output_dir = tmp_path / "data"
    output_dir.mkdir()
    existing_payload = {
        "article_url": "https://www.nature.com/articles/s41467-026-70240-6",
        "slug": "s41467-026-70240-6",
        "title": "Existing",
        "doi": "10.1038/s41467-026-70240-6",
        "journal": "Nature Communications",
        "published_date": "16 March 2026",
        "abstract": "Abstract",
        "body_markdown": "# Existing",
        "article_pdf_url": "https://www.nature.com/articles/s41467-026-70240-6.pdf",
        "peer_review_pdf_url": None,
        "markdown_path": None,
        "peer_review_pdf_path": None,
        "detailed_metadata": {},
        "metadata": {},
    }
    (output_dir / "articles.jsonl").write_text(json.dumps(existing_payload) + "\n", encoding="utf-8")

    fake_client = FakeClient()
    collector = Collector(
        Settings(firecrawl_api_key="test-key", output_dir=output_dir),
        client=fake_client,
        storage=FakeStorage(output_dir),
    )

    records = collector.collect_urls(["https://www.nature.com/articles/s41467-026-70240-6"], resume=True)

    assert len(records) == 1
    assert records[0].title == "Existing"
    assert fake_client.scrape_calls == []


def test_collect_urls_force_rescrapes_existing_jsonl(tmp_path) -> None:
    output_dir = tmp_path / "data"
    output_dir.mkdir()
    existing_payload = {
        "article_url": "https://www.nature.com/articles/s41467-026-70240-6",
        "slug": "s41467-026-70240-6",
        "title": "Existing",
        "doi": "10.1038/s41467-026-70240-6",
        "journal": "Nature Communications",
        "published_date": "16 March 2026",
        "abstract": "Abstract",
        "body_markdown": "# Existing",
        "article_pdf_url": "https://www.nature.com/articles/s41467-026-70240-6.pdf",
        "peer_review_pdf_url": None,
        "markdown_path": None,
        "peer_review_pdf_path": None,
        "detailed_metadata": {},
        "metadata": {},
    }
    (output_dir / "articles.jsonl").write_text(json.dumps(existing_payload) + "\n", encoding="utf-8")

    fake_client = FakeClient()
    collector = Collector(
        Settings(firecrawl_api_key="test-key", output_dir=output_dir),
        client=fake_client,
        storage=FakeStorage(output_dir),
    )

    records = collector.collect_urls(
        ["https://www.nature.com/articles/s41467-026-70240-6"],
        download_peer_reviews=False,
        resume=False,
    )

    assert len(records) == 1
    assert records[0].title == "Title"
    assert fake_client.scrape_calls == ["https://www.nature.com/articles/s41467-026-70240-6"]
    assert records[0].detailed_metadata["authors"] == ["Alice Smith", "Bob Lee"]


def test_collect_urls_resumes_from_existing_record_by_doi(tmp_path) -> None:
    output_dir = tmp_path / "data"
    output_dir.mkdir()
    existing_payload = {
        "article_url": "https://www.nature.com/articles/s41467-026-70240-6",
        "slug": "s41467-026-70240-6",
        "title": "Title",
        "doi": "10.1038/s41467-026-70240-6",
        "journal": "Nature Communications",
        "published_date": "16 March 2026",
        "abstract": "Abstract",
        "body_markdown": "# Existing",
        "article_pdf_url": "https://www.nature.com/articles/s41467-026-70240-6.pdf",
        "peer_review_pdf_url": None,
        "markdown_path": None,
        "peer_review_pdf_path": None,
        "detailed_metadata": {},
        "metadata": {},
    }
    (output_dir / "articles.jsonl").write_text(json.dumps(existing_payload) + "\n", encoding="utf-8")

    fake_client = FakeClient()
    collector = Collector(
        Settings(firecrawl_api_key="test-key", output_dir=output_dir),
        client=fake_client,
        storage=FakeStorage(output_dir),
    )

    records = collector.collect_urls(["https://www.nature.com/articles/s41467-026-80000-1"], resume=True)

    assert len(records) == 1
    assert records[0].slug == "s41467-026-70240-6"
    assert fake_client.scrape_calls == ["https://www.nature.com/articles/s41467-026-80000-1"]


def test_collect_urls_dedupes_same_batch_under_concurrency(tmp_path) -> None:
    output_dir = tmp_path / "data"
    fake_client = FakeClient()
    settings = Settings(
        firecrawl_api_key="test-key",
        output_dir=output_dir,
        max_workers=2,
        requests_per_second=100.0,
    )
    collector = Collector(settings, client=fake_client)
    collector.storage = FakeStorage(output_dir)

    records = collector.collect_urls(
        [
            "https://www.nature.com/articles/s41467-026-70240-6",
            "https://www.nature.com/articles/s41467-026-80000-1",
        ],
        download_peer_reviews=False,
        resume=False,
    )

    assert len(records) == 1
    assert records[0].doi == "10.1038/s41467-026-70240-6"


def test_collect_urls_skips_cached_success_url(tmp_path) -> None:
    output_dir = tmp_path / "data"
    fake_client = FakeClient()
    collector = Collector(
        Settings(firecrawl_api_key="test-key", output_dir=output_dir),
        client=fake_client,
        storage=FakeStorage(output_dir),
    )
    collector.storage.ensure_layout()
    collector.storage.mark_crawl_attempt(
        url="https://www.nature.com/articles/s41467-026-70240-6",
        normalized_url="https://www.nature.com/articles/s41467-026-70240-6",
        status="success",
        attempted_at="2026-03-17T00:00:00+00:00",
        succeeded_at="2026-03-17T00:00:01+00:00",
        article_slug="s41467-026-70240-6",
    )
    collector.storage.append_record(
        ArticleRecord(
            article_url="https://www.nature.com/articles/s41467-026-70240-6",
            slug="s41467-026-70240-6",
            title="Existing",
            doi="10.1038/s41467-026-70240-6",
            journal="Nature Communications",
        )
    )

    records = collector.collect_urls(["https://www.nature.com/articles/s41467-026-70240-6"], resume=True)

    assert len(records) == 1
    assert fake_client.scrape_calls == []


def test_collect_urls_records_failed_cache_and_can_skip_retry(tmp_path) -> None:
    output_dir = tmp_path / "data"
    failing_url = "https://www.nature.com/articles/s41467-026-70240-6"
    fake_client = FakeClient(fail_urls={failing_url})
    collector = Collector(
        Settings(firecrawl_api_key="test-key", output_dir=output_dir),
        client=fake_client,
        storage=FakeStorage(output_dir),
    )

    records = collector.collect_urls([failing_url], resume=True, retry_failed=True)
    assert records == []
    cache = collector.storage.load_crawl_cache()
    assert cache[failing_url]["status"] == "failed"
    assert fake_client.scrape_calls == [failing_url]

    fake_client.scrape_calls.clear()
    records = collector.collect_urls([failing_url], resume=True, retry_failed=False)
    assert records == []
    assert fake_client.scrape_calls == []


def test_collect_archive_streams_pages_into_collection(tmp_path) -> None:
    output_dir = tmp_path / "data"
    fake_client = FakeClient()
    storage = FakeStorage(output_dir)
    collector = Collector(
        Settings(firecrawl_api_key="test-key", output_dir=output_dir),
        client=fake_client,
        storage=storage,
    )
    events: list[dict[str, object]] = []

    records = collector.collect_archive(
        archive_url="https://www.nature.com/ncomms/research-articles?type=article",
        max_pages=None,
        download_peer_reviews=False,
        progress_callback=events.append,
    )

    assert len(records) == 2
    assert fake_client.scrape_calls == [
        "https://www.nature.com/articles/s41467-026-70240-6",
        "https://www.nature.com/articles/s41467-026-70241-5",
    ]
    assert [event["page_number"] for event in events] == [1, 2]
    assert len(storage.downloads) == 2
