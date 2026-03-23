# nc-firecrawl

Small Python CLI for collecting Nature Communications article pages and their transparent peer review PDFs with [Firecrawl](https://docs.firecrawl.dev/introduction).

The project supports two crawler engines:

- `firecrawl`: Firecrawl-backed discovery and scraping
- `native`: direct `requests + BeautifulSoup` fetching against Nature pages

The current implementation is optimized for article URLs like:

- `https://www.nature.com/articles/s41467-026-70240-6`

It scrapes the article page, extracts metadata, saves markdown, and downloads the "Transparent Peer Review file" PDF when present.

## What it does

- Discover candidate Nature Communications article URLs with Firecrawl search.
- Discover article URLs from the Nature Communications article listing page.
- Discover article URLs from Nature sitemap files.
- Support the legacy `ncomms12345` article URLs as well as newer `s41467-...` URLs.
- Scrape article pages with `markdown`, `html`, and `links` output so supplementary links are retained.
- Extract:
  - title
  - DOI
  - publication date
  - abstract
  - full article markdown in `body_markdown`
  - article PDF URL
  - transparent peer review PDF URL
  - structured `detailed_metadata` for downstream analysis
- Save structured results to `articles.jsonl`
- Save full per-article payloads to `article_records/`
- Maintain a SQLite index in `articles.sqlite` for faster duplicate checks and later querying
- Maintain a persistent crawl cache in SQLite so already successful URLs are not fetched again
- Save article markdown to `article_markdown/`
- Download peer review PDFs to `peer_reviews/`
- Resume by default from the SQLite index to avoid re-scraping the same paper.
- Support controlled concurrent scraping with a global rate limit.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
```

Then set your Firecrawl API key in `.env`.

If you plan to use only `--engine native`, `FIRECRAWL_API_KEY` is not required.

The project follows the official Python SDK style:

```python
from firecrawl import Firecrawl

app = Firecrawl(api_key="fc-...")
result = app.scrape("https://www.nature.com/articles/s41467-026-70240-6")
```

## Usage

### 1. Collect from explicit article URLs

```bash
nc-firecrawl collect \
  --url https://www.nature.com/articles/s41467-026-70240-6
```

### 2. Collect from a text file of URLs

Create a text file with one article URL per line, then:

```bash
nc-firecrawl collect --input urls.txt
```

### 3. Discover candidate Nature Communications papers with Firecrawl search

```bash
nc-firecrawl discover \
  --query 'site:nature.com/articles/ "Nature Communications"' \
  --limit 20
```

### 4. Search and collect in one command

```bash
nc-firecrawl collect \
  --query 'site:nature.com/articles/ "Nature Communications"' \
  --limit 20
```

### 5. Discover from the Nature Communications article listing

```bash
nc-firecrawl discover-archive \
  --archive-url 'https://www.nature.com/ncomms/research-articles?type=article' \
  --all-pages
```

### 6. Collect directly from the article listing

```bash
nc-firecrawl collect \
  --archive \
  --archive-url 'https://www.nature.com/ncomms/research-articles?type=article' \
  --archive-all-pages \
  --max-workers 6 \
  --requests-per-second 1.5
```

### 6b. Run the same crawl with the native engine for comparison

```bash
nc-firecrawl collect \
  --engine native \
  --archive \
  --archive-url 'https://www.nature.com/ncomms/research-articles?type=article' \
  --archive-all-pages \
  --max-workers 2 \
  --requests-per-second 0.2
```

### 7. Force a re-scrape even if a record already exists

```bash
nc-firecrawl collect \
  --url https://www.nature.com/articles/s41467-026-70240-6 \
  --force
```

### 8. Tune concurrency and rate limiting

```bash
nc-firecrawl collect \
  --archive \
  --archive-url 'https://www.nature.com/ncomms/research-articles?type=article' \
  --archive-all-pages \
  --max-workers 6 \
  --requests-per-second 1.5
```

### 9. Skip URLs that failed before instead of retrying them

```bash
nc-firecrawl collect \
  --archive \
  --archive-url 'https://www.nature.com/ncomms/research-articles?type=article' \
  --archive-all-pages \
  --skip-failed-cache
```

### 10. Check crawl progress stats

```bash
nc-firecrawl stats --json
```

### 11. Discover from Nature sitemap files

```bash
nc-firecrawl discover-sitemap \
  --sitemap-url 'https://www.nature.com/sitemap.xml'
```

### 12. Collect from Nature sitemap files

```bash
nc-firecrawl collect \
  --engine native \
  --sitemap \
  --sitemap-url 'https://www.nature.com/sitemap.xml' \
  --max-workers 2 \
  --requests-per-second 0.2
```

## Output layout

```text
data/
  articles.jsonl
  articles.sqlite
  article_records/
    s41467-026-70240-6.json
  article_markdown/
    s41467-026-70240-6.md
  peer_reviews/
    s41467-026-70240-6-peer-review.pdf
```

`articles.jsonl` is now a slim index for large runs. It keeps only high-value lookup fields plus file paths.

Full payloads, including `body_markdown`, raw `metadata`, and `detailed_metadata`, are stored in `article_records/<slug>.json`.

Resume behavior:

- Default: if an existing record matches by `slug`, normalized article URL, DOI, article PDF URL, peer review PDF URL, or normalized `journal + title`, the CLI returns that existing record and skips writing a duplicate.
- Use `--force` to re-scrape and append a fresh record.
- Duplicate checks now come from `articles.sqlite` first, with `articles.jsonl` used as a fallback for older runs before the SQLite index exists.

Cache behavior:

- Every attempted article URL is recorded in SQLite crawl cache with status such as `success` or `failed`.
- A URL with cached `success` is skipped by default on later runs, so already completed pages are not fetched again.
- Failed URLs are recorded with the last error message and attempt time.
- Failed URLs are retried by default, because many failures are transient.
- Use `--skip-failed-cache` if you want to skip URLs that previously failed.
- Use `--force` to bypass the resume/cache skip behavior and re-scrape URLs anyway.

Metadata behavior:

- `metadata`: raw metadata returned by Firecrawl.
- `detailed_metadata`: normalized fields extracted from the page, including dates, authors, keywords, section headings, content stats, link inventory, and a summary of key raw metadata fields.

Concurrency behavior:

- `--max-workers` controls concurrent article scrape workers. Default comes from `NC_MAX_WORKERS` or falls back to `4`.
- `--requests-per-second` applies a global rate limit across article scrapes and peer review PDF downloads. Default comes from `NC_REQUESTS_PER_SECOND` or falls back to `1.0`.
- `collect` also prints a one-line progress summary to stderr with total records, cached URLs, successful URLs, and failed URLs.
- Archive discovery now prints page-level progress to stderr, for example:
  `discover_progress page=2 page_discovered=13 page_new=13 cumulative=26 page_url=https://...`
- Sitemap discovery prints `sitemap_progress` lines to stderr with processed sitemap counts and cumulative discovered article URLs.

## Notes

- Firecrawl usage is based on the current v2 docs:
  - [`scrape`](https://docs.firecrawl.dev/features/scrape)
  - [`search`](https://docs.firecrawl.dev/introduction)
  - Python SDK install via [`firecrawl-py`](https://docs.firecrawl.dev/sdks/python)
- The code now prefers the official `from firecrawl import Firecrawl` interface and keeps `FirecrawlApp` only as a backward-compatible fallback.
- The native engine does not use Firecrawl for listing/article fetches. It exists specifically to compare site coverage and content completeness against the Firecrawl-backed engine.
- The native engine does not support `discover --query`; use `--archive` or `--site` when running with `--engine native`.
- Nature article pages must be scraped with full page content, not "main content only", otherwise supplementary links can be omitted.
- Search discovery is practical but not guaranteed to be exhaustive for the full journal archive.
- Archive discovery currently assumes Nature-style pagination on the listing URL. The default target is `https://www.nature.com/ncomms/research-articles?type=article`, and for page 2+ the crawler adds the observed Nature query parameters such as `searchType=journalSearch` and `sort=PubDate`.
