from __future__ import annotations

from nc_firecrawl.firecrawl_client import FirecrawlClient


class FakeModel:
    def __init__(self, payload):
        self._payload = payload

    def model_dump(self):
        return self._payload


def test_normalize_response_supports_pydantic_style_models() -> None:
    payload = {"markdown": "# Title", "links": ["https://example.com/file.pdf"]}
    model = FakeModel(payload)
    assert FirecrawlClient._normalize_response(model) == payload


def test_scrape_article_preserves_listing_query_parameters() -> None:
    client = object.__new__(FirecrawlClient)
    captured: list[str] = []

    class FakeSDK:
        @staticmethod
        def scrape(url, **kwargs):
            captured.append(url)
            return {"markdown": "", "links": []}

    client._client = FakeSDK()
    client.scrape_article(
        "https://www.nature.com/ncomms/research-articles?searchType=journalSearch&sort=PubDate&type=article&page=2"
    )

    assert captured == [
        "https://www.nature.com/ncomms/research-articles?searchType=journalSearch&sort=PubDate&type=article&page=2"
    ]


def test_search_result_parses_nested_data_web_shape() -> None:
    client = object.__new__(FirecrawlClient)
    client._client = type(
        "SearchClient",
        (),
        {
            "search": staticmethod(
                lambda query, limit: FakeModel(
                    {
                        "success": True,
                        "data": {
                            "web": [
                                {"url": "https://www.nature.com/articles/s41467-026-70240-6"},
                                {"url": "https://www.nature.com/articles/s41467-026-70240-6?foo=bar"},
                                {"url": "https://example.com/not-nc"},
                            ]
                        },
                    }
                )
            )
        },
    )()

    urls = client.search_article_urls("nc", 10)
    assert urls == ["https://www.nature.com/articles/s41467-026-70240-6"]


def test_discover_archive_article_urls_stops_when_page_has_no_new_results() -> None:
    client = object.__new__(FirecrawlClient)
    calls: list[str] = []
    page_results = {
        "https://www.nature.com/ncomms/research-articles": {
            "links": ["https://www.nature.com/articles/s41467-026-70240-6"]
        },
        "https://www.nature.com/ncomms/research-articles?page=2": {
            "links": [
                "https://www.nature.com/articles/s41467-026-70240-6",
                "https://www.nature.com/articles/s41467-026-70241-5",
            ]
        },
        "https://www.nature.com/ncomms/research-articles?page=3": {
            "links": ["https://www.nature.com/articles/s41467-026-70241-5"]
        },
    }

    def fake_scrape(url: str, retry_index: int = 0):
        calls.append(url)
        return page_results[url]

    client.scrape_listing = fake_scrape

    urls = client.discover_archive_article_urls("https://www.nature.com/ncomms/research-articles", 5)

    assert urls == [
        "https://www.nature.com/articles/s41467-026-70240-6",
        "https://www.nature.com/articles/s41467-026-70241-5",
    ]
    assert "https://www.nature.com/ncomms/research-articles" in calls
    assert "https://www.nature.com/ncomms/research-articles?page=2" in calls
    assert "https://www.nature.com/ncomms/research-articles?page=3" in calls
    assert calls[-1] == "https://www.nature.com/ncomms/research-articles?page=3"


def test_discover_site_article_urls_merges_multiple_archive_sections() -> None:
    client = object.__new__(FirecrawlClient)
    calls: list[tuple[str, int]] = []

    def fake_discover_archive(archive_url: str, max_pages: int, progress_callback=None):
        calls.append((archive_url, max_pages))
        if archive_url.endswith("research-articles"):
            return [
                "https://www.nature.com/articles/s41467-026-70240-6",
                "https://www.nature.com/articles/s41467-026-70241-5",
            ]
        return [
            "https://www.nature.com/articles/s41467-026-70241-5",
            "https://www.nature.com/articles/s41467-026-70242-4",
        ]

    client.discover_archive_article_urls = fake_discover_archive

    urls = client.discover_site_article_urls(
        archive_urls=[
            "https://www.nature.com/ncomms/research-articles",
            "https://www.nature.com/ncomms/reviews",
        ],
        max_pages=12,
    )

    assert urls == [
        "https://www.nature.com/articles/s41467-026-70240-6",
        "https://www.nature.com/articles/s41467-026-70241-5",
        "https://www.nature.com/articles/s41467-026-70242-4",
    ]
    assert calls == [
        ("https://www.nature.com/ncomms/research-articles", 12),
        ("https://www.nature.com/ncomms/reviews", 12),
    ]


def test_discover_archive_article_urls_all_pages_stops_on_empty_page() -> None:
    client = object.__new__(FirecrawlClient)
    calls: list[str] = []
    page_results = {
        "https://www.nature.com/ncomms/research-articles?type=article": {
            "links": ["https://www.nature.com/articles/ncomms12345"]
        },
        "https://www.nature.com/ncomms/research-articles?type=article&page=2&searchType=journalSearch&sort=PubDate": {
            "links": ["https://www.nature.com/articles/s41467-026-70240-6"]
        },
        "https://www.nature.com/ncomms/research-articles?type=article&page=3&searchType=journalSearch&sort=PubDate": {
            "links": []
        },
    }

    def fake_scrape(url: str, retry_index: int = 0):
        calls.append(url)
        return page_results[url]

    client.scrape_listing = fake_scrape

    urls = client.discover_archive_article_urls("https://www.nature.com/ncomms/research-articles?type=article", None)

    assert urls == [
        "https://www.nature.com/articles/ncomms12345",
        "https://www.nature.com/articles/s41467-026-70240-6",
    ]
    assert "https://www.nature.com/ncomms/research-articles?type=article" in calls
    assert (
        "https://www.nature.com/ncomms/research-articles?type=article&page=2&searchType=journalSearch&sort=PubDate"
        in calls
    )
    assert (
        "https://www.nature.com/ncomms/research-articles?type=article&page=3&searchType=journalSearch&sort=PubDate"
        in calls
    )
    assert (
        calls[-1]
        == "https://www.nature.com/ncomms/research-articles?type=article&page=3&searchType=journalSearch&sort=PubDate"
    )


def test_discover_archive_article_urls_emits_progress_callback() -> None:
    client = object.__new__(FirecrawlClient)
    events: list[dict[str, object]] = []
    page_results = {
        "https://www.nature.com/ncomms/research-articles?type=article": {
            "links": ["https://www.nature.com/articles/ncomms12345"]
        },
        "https://www.nature.com/ncomms/research-articles?type=article&page=2&searchType=journalSearch&sort=PubDate": {
            "links": ["https://www.nature.com/articles/s41467-026-70240-6"]
        },
        "https://www.nature.com/ncomms/research-articles?type=article&page=3&searchType=journalSearch&sort=PubDate": {
            "links": []
        },
    }

    def fake_scrape(url: str, retry_index: int = 0):
        return page_results[url]

    client.scrape_listing = fake_scrape

    urls = client.discover_archive_article_urls(
        "https://www.nature.com/ncomms/research-articles?type=article",
        None,
        progress_callback=events.append,
    )

    assert urls == [
        "https://www.nature.com/articles/ncomms12345",
        "https://www.nature.com/articles/s41467-026-70240-6",
    ]
    assert [event["page_number"] for event in events] == [1, 2, 3]
    assert events[0]["page_new"] == 1
    assert events[1]["cumulative_discovered"] == 2
    assert events[2]["page_discovered"] == 0
