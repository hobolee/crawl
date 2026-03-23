from nc_firecrawl.nature import (
    archive_page_url,
    extract_article_urls_from_listing,
    extract_article_pdf_url,
    extract_abstract,
    extract_peer_review_url,
    is_nc_article_url,
    listing_url_with_year,
    normalize_article_url,
    record_from_scrape,
)


def test_is_nc_article_url() -> None:
    assert is_nc_article_url("https://www.nature.com/articles/s41467-026-70240-6")
    assert is_nc_article_url("https://www.nature.com/articles/s41467-026-70240-6?error=cookies_not_supported")
    assert is_nc_article_url("https://www.nature.com/articles/ncomms12345")
    assert not is_nc_article_url("https://www.nature.com/articles/d41586-026-00001-1")


def test_extract_peer_review_url_from_links() -> None:
    scrape_result = {
        "links": [
            "https://www.nature.com/articles/s41467-026-70240-6.pdf",
            "https://static-content.springer.com/esm/art%3A10.1038%2Fs41467-026-70240-6/MediaObjects/41467_2026_70240_MOESM4_ESM.pdf",
        ]
    }
    assert (
        extract_peer_review_url(scrape_result)
        == "https://static-content.springer.com/esm/art%3A10.1038%2Fs41467-026-70240-6/MediaObjects/41467_2026_70240_MOESM4_ESM.pdf"
    )


def test_record_from_scrape_keeps_body_markdown() -> None:
    scrape_result = {
        "markdown": "\n".join(
            [
                "# Title",
                "Alice Smith, Bob Lee",
                "Received: 01 January 2026",
                "Accepted: 10 February 2026",
                "Published: 16 March 2026",
                "## Abstract",
                "Body",
                "## Methods",
                "Stuff",
                "## References",
                "1. Ref",
            ]
        ),
        "metadata": {"title": "Title | Nature Communications", "keywords": ["vision", "latency"]},
    }
    record = record_from_scrape("https://www.nature.com/articles/s41467-026-70240-6", scrape_result)
    assert record.body_markdown is not None
    assert record.detailed_metadata["authors"] == ["Alice Smith", "Bob Lee"]
    assert record.detailed_metadata["dates"]["received"] == "01 January 2026"
    assert record.detailed_metadata["dates"]["accepted"] == "10 February 2026"
    assert record.detailed_metadata["dates"]["published"] == "16 March 2026"
    assert record.detailed_metadata["content_stats"]["reference_count"] == 1
    assert record.detailed_metadata["keywords"] == ["vision", "latency"]


def test_extract_article_urls_from_archive_listing() -> None:
    scrape_result = {
        "links": [
            "https://www.nature.com/articles/s41467-026-70240-6",
            "https://www.nature.com/articles/ncomms12345",
            "https://www.nature.com/articles/s41467-026-70241-5?foo=bar",
            "https://www.nature.com/articles/d41586-026-00001-1",
        ],
        "markdown": "[Paper](https://www.nature.com/articles/s41467-026-70240-6)",
    }
    assert extract_article_urls_from_listing(scrape_result) == [
        "https://www.nature.com/articles/s41467-026-70240-6",
        "https://www.nature.com/articles/ncomms12345",
        "https://www.nature.com/articles/s41467-026-70241-5",
    ]


def test_archive_page_url_matches_nature_research_articles_pattern() -> None:
    base_url = "https://www.nature.com/ncomms/research-articles?type=article"
    assert archive_page_url(base_url, 1) == "https://www.nature.com/ncomms/research-articles?type=article"
    assert (
        archive_page_url(base_url, 2)
        == "https://www.nature.com/ncomms/research-articles?type=article&page=2&searchType=journalSearch&sort=PubDate"
    )
    assert (
        archive_page_url(base_url, 4009)
        == "https://www.nature.com/ncomms/research-articles?type=article&page=4009&searchType=journalSearch&sort=PubDate"
    )


def test_archive_page_url_preserves_year_filter() -> None:
    base_url = listing_url_with_year("https://www.nature.com/ncomms/research-articles?type=article", 2025)
    assert base_url == "https://www.nature.com/ncomms/research-articles?type=article&year=2025"
    assert (
        archive_page_url(base_url, 2)
        == "https://www.nature.com/ncomms/research-articles?type=article&year=2025&page=2&searchType=journalSearch&sort=PubDate"
    )


def test_record_from_scrape_supports_legacy_ncomms_doi() -> None:
    scrape_result = {
        "markdown": "# Legacy paper\n10.1038/ncomms12345\n## Abstract\nBody",
        "metadata": {"title": "Legacy paper | Nature Communications"},
    }
    record = record_from_scrape("https://www.nature.com/articles/ncomms12345", scrape_result)
    assert record.slug == "ncomms12345"
    assert record.doi == "10.1038/ncomms12345"


def test_extract_abstract() -> None:
    scrape_result = {
        "markdown": "\n".join(
            [
                "# Example title",
                "## Abstract",
                "Sentence one.",
                "Sentence two.",
                "## Introduction",
                "Rest.",
            ]
        )
    }
    assert extract_abstract(scrape_result) == "Sentence one. Sentence two."


def test_record_from_scrape_normalizes_cookie_suffix() -> None:
    scrape_result = {
        "markdown": "\n".join(
            [
                "# Bridging the latency gap",
                "Published: 16 March 2026",
                "## Abstract",
                "Abstract text.",
                "## Introduction",
                "Stuff.",
            ]
        ),
        "links": [
            "https://static-content.springer.com/esm/art%3A10.1038%2Fs41467-026-70240-6/MediaObjects/41467_2026_70240_MOESM4_ESM.pdf"
        ],
        "metadata": {
            "title": "Bridging the latency gap with a continuous stream evaluation framework in event-driven perception | Nature Communications"
        },
    }
    record = record_from_scrape(
        "https://www.nature.com/articles/s41467-026-70240-6?error=cookies_not_supported",
        scrape_result,
    )
    assert normalize_article_url(record.article_url) == "https://www.nature.com/articles/s41467-026-70240-6"
    assert record.slug == "s41467-026-70240-6"
    assert record.peer_review_pdf_url is not None
    assert record.doi == "10.1038/s41467-026-70240-6"


def test_extract_pdf_urls_from_nature_style_markdown() -> None:
    article_url = "https://www.nature.com/articles/s41467-026-70240-6"
    scrape_result = {
        "markdown": "\n".join(
            [
                "# Bridging the latency gap with a continuous stream evaluation framework in event-driven perception",
                "[Download PDF](https://www.nature.com/articles/s41467-026-70240-6.pdf)",
                "[Transparent Peer Review file](https://static-content.springer.com/esm/art%3A10.1038%2Fs41467-026-70240-6/MediaObjects/41467_2026_70240_MOESM4_ESM.pdf)",
            ]
        ),
        "html": '<a href="https://www.nature.com/articles/s41467-026-70240-6.pdf">Download PDF</a>',
        "metadata": {
            "title": "Bridging the latency gap with a continuous stream evaluation framework in event-driven perception | Nature Communications"
        },
    }
    assert extract_article_pdf_url(scrape_result, article_url) == "https://www.nature.com/articles/s41467-026-70240-6.pdf"
    assert (
        extract_peer_review_url(scrape_result)
        == "https://static-content.springer.com/esm/art%3A10.1038%2Fs41467-026-70240-6/MediaObjects/41467_2026_70240_MOESM4_ESM.pdf"
    )
