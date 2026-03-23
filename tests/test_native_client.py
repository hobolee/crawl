from __future__ import annotations

import pytest

pytest.importorskip("bs4")

from nc_firecrawl.native_client import NativeNatureClient


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_native_listing_extracts_all_article_links() -> None:
    client = NativeNatureClient(timeout_seconds=5)
    html = """
    <html><body>
      <a href="/articles/ncomms12345">Legacy</a>
      <a href="/articles/s41467-026-70240-6">New</a>
      <a href="/articles/d41586-026-00001-1">Not NC</a>
    </body></html>
    """
    client.session.get = lambda url, timeout: FakeResponse(html)  # type: ignore[assignment]

    result = client.scrape_listing("https://www.nature.com/ncomms/research-articles?type=article")

    assert "https://www.nature.com/articles/ncomms12345" in result["links"]
    assert "https://www.nature.com/articles/s41467-026-70240-6" in result["links"]


def test_native_article_scrape_extracts_body_markdown() -> None:
    client = NativeNatureClient(timeout_seconds=5)
    html = """
    <html>
      <head>
        <title>Example | Nature Communications</title>
        <meta name="keywords" content="vision, latency" />
      </head>
      <body>
        <main>
          <h1 class="c-article-title">Example Paper</h1>
          <ul class="c-article-author-list">
            <li>Alice Smith</li>
            <li>Bob Lee</li>
          </ul>
          <section class="c-article-section">
            <h2>Abstract</h2>
            <div class="c-article-section__content"><p>Abstract text.</p></div>
          </section>
          <section class="c-article-section">
            <h2>Results</h2>
            <p>Result one.</p>
            <p>Result two.</p>
          </section>
          <a href="https://www.nature.com/articles/ncomms12345.pdf">Download PDF</a>
          <a href="https://static-content.springer.com/esm/.../MOESM1_ESM.pdf">Transparent Peer Review file</a>
        </main>
      </body>
    </html>
    """
    client.session.get = lambda url, timeout: FakeResponse(html)  # type: ignore[assignment]

    result = client.scrape_article("https://www.nature.com/articles/ncomms12345")

    assert "# Example Paper" in result["markdown"]
    assert "## Results" in result["markdown"]
    assert "Result one." in result["markdown"]
    assert result["metadata"]["title"] == "Example | Nature Communications"
