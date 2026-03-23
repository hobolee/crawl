from __future__ import annotations

from nc_firecrawl.sitemap import NatureSitemapDiscoverer


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_sitemap_discoverer_follows_index_and_filters_nc_article_urls() -> None:
    discoverer = NatureSitemapDiscoverer(timeout_seconds=5)
    sitemap_index = """
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <sitemap><loc>https://www.nature.com/sitemap-a.xml</loc></sitemap>
      <sitemap><loc>https://www.nature.com/sitemap-b.xml</loc></sitemap>
    </sitemapindex>
    """
    sitemap_a = """
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://www.nature.com/articles/ncomms12345</loc></url>
      <url><loc>https://www.nature.com/articles/s41467-026-70240-6</loc></url>
      <url><loc>https://www.nature.com/articles/d41586-026-00001-1</loc></url>
    </urlset>
    """
    sitemap_b = """
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://www.nature.com/articles/ncomms12345</loc></url>
      <url><loc>https://www.nature.com/articles/s41467-026-70241-5</loc></url>
    </urlset>
    """
    payloads = {
        "https://www.nature.com/sitemap.xml": sitemap_index,
        "https://www.nature.com/sitemap-a.xml": sitemap_a,
        "https://www.nature.com/sitemap-b.xml": sitemap_b,
    }
    discoverer.session.get = lambda url, timeout: FakeResponse(payloads[url])  # type: ignore[assignment]

    urls = discoverer.discover_article_urls("https://www.nature.com/sitemap.xml")

    assert urls == [
        "https://www.nature.com/articles/ncomms12345",
        "https://www.nature.com/articles/s41467-026-70240-6",
        "https://www.nature.com/articles/s41467-026-70241-5",
    ]
