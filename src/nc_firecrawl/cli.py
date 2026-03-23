from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .collector import Collector, load_urls_from_file
from .config import Settings
from .firecrawl_client import FirecrawlClient
from .native_client import NativeNatureClient
from .nature import DEFAULT_ARCHIVE_URL, DEFAULT_SITE_ARCHIVE_URLS, listing_url_with_year
from .sitemap import DEFAULT_SITEMAP_URL


DEFAULT_QUERY = 'site:nature.com/articles/ "Nature Communications"'


def emit_discovery_progress(payload: dict[str, object]) -> None:
    if payload.get("scope") == "sitemap":
        print(
            "sitemap_progress"
            f" kind={payload.get('kind')}"
            f" processed_sitemaps={payload.get('processed_sitemaps')}"
            f" new_count={payload.get('new_count')}"
            f" cumulative={payload.get('cumulative_discovered')}"
            f" sitemap_url={payload.get('sitemap_url')}",
            file=sys.stderr,
        )
        return
    print(
        "discover_progress"
        f" page={payload.get('page_number')}"
        f" page_discovered={payload.get('page_discovered')}"
        f" page_new={payload.get('page_new')}"
        f" cumulative={payload.get('cumulative_discovered')}"
        f" page_url={payload.get('page_url')}",
        file=sys.stderr,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nc-firecrawl",
        description="Collect Nature Communications papers and transparent peer review files.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    discover_parser = subparsers.add_parser("discover", help="Discover article URLs via Firecrawl search.")
    discover_parser.add_argument(
        "--engine",
        choices=["firecrawl", "native"],
        default="firecrawl",
        help="Crawler engine to use.",
    )
    discover_parser.add_argument("--query", default=DEFAULT_QUERY, help="Search query to send to Firecrawl.")
    discover_parser.add_argument("--limit", type=int, default=20, help="Maximum number of search results.")

    archive_parser = subparsers.add_parser(
        "discover-archive",
        help="Discover article URLs from Nature Communications archive pages.",
    )
    archive_parser.add_argument(
        "--engine",
        choices=["firecrawl", "native"],
        default="firecrawl",
        help="Crawler engine to use.",
    )
    archive_parser.add_argument(
        "--archive-url",
        default=DEFAULT_ARCHIVE_URL,
        help="Archive listing URL to scrape.",
    )
    archive_parser.add_argument(
        "--year",
        type=int,
        help="Restrict archive crawling to a single publication year.",
    )
    archive_parser.add_argument(
        "--pages",
        type=int,
        default=5,
        help="Maximum number of archive pages to inspect.",
    )
    archive_parser.add_argument(
        "--all-pages",
        action="store_true",
        help="Keep paging until no new article URLs are found.",
    )

    site_parser = subparsers.add_parser(
        "discover-site",
        help="Discover article URLs across multiple Nature Communications archive sections.",
    )
    site_parser.add_argument(
        "--engine",
        choices=["firecrawl", "native"],
        default="firecrawl",
        help="Crawler engine to use.",
    )
    site_parser.add_argument(
        "--archive-url",
        action="append",
        dest="archive_urls",
        default=[],
        help="Archive listing URL to include. Repeatable. Defaults to the built-in NC sections.",
    )
    site_parser.add_argument(
        "--pages",
        type=int,
        default=20,
        help="Maximum number of pages to inspect per archive section.",
    )
    site_parser.add_argument(
        "--all-pages",
        action="store_true",
        help="Keep paging each archive section until no new article URLs are found.",
    )

    sitemap_parser = subparsers.add_parser(
        "discover-sitemap",
        help="Discover NC article URLs from Nature sitemap files.",
    )
    sitemap_parser.add_argument(
        "--sitemap-url",
        default=DEFAULT_SITEMAP_URL,
        help="Root sitemap URL to crawl.",
    )

    stats_parser = subparsers.add_parser("stats", help="Show crawl/index progress statistics.")
    stats_parser.add_argument(
        "--json",
        action="store_true",
        help="Print stats as JSON.",
    )
    stats_parser.add_argument(
        "--list-failed",
        action="store_true",
        help="Print URLs that failed to scrape (one per line).",
    )
    stats_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Root output directory to read the database from (overrides NC_OUTPUT_DIR).",
    )

    collect_parser = subparsers.add_parser("collect", help="Scrape article pages and download peer review PDFs.")
    collect_parser.add_argument(
        "--engine",
        choices=["firecrawl", "native"],
        default="firecrawl",
        help="Crawler engine to use.",
    )
    collect_parser.add_argument("--url", action="append", default=[], help="Explicit Nature article URL. Repeatable.")
    collect_parser.add_argument("--input", type=Path, help="Text file containing one article URL per line.")
    collect_parser.add_argument("--query", help="Optional Firecrawl search query to discover article URLs first.")
    collect_parser.add_argument("--limit", type=int, default=20, help="Maximum number of discovered URLs.")
    collect_parser.add_argument(
        "--archive",
        action="store_true",
        help="Discover article URLs from the Nature Communications archive before collecting.",
    )
    collect_parser.add_argument(
        "--site",
        action="store_true",
        help="Discover article URLs across multiple built-in NC archive sections before collecting.",
    )
    collect_parser.add_argument(
        "--sitemap",
        action="store_true",
        help="Discover article URLs from Nature sitemap files before collecting.",
    )
    collect_parser.add_argument(
        "--archive-url",
        default=DEFAULT_ARCHIVE_URL,
        help="Archive listing URL to scrape when --archive is used.",
    )
    collect_parser.add_argument(
        "--year",
        type=int,
        help="Restrict archive crawling to a single publication year.",
    )
    collect_parser.add_argument(
        "--archive-pages",
        type=int,
        default=5,
        help="Maximum number of archive pages to inspect when --archive is used.",
    )
    collect_parser.add_argument(
        "--archive-all-pages",
        action="store_true",
        help="Keep paging the archive until no new article URLs are found.",
    )
    collect_parser.add_argument(
        "--site-pages",
        type=int,
        default=20,
        help="Maximum number of pages to inspect per archive section when --site is used.",
    )
    collect_parser.add_argument(
        "--site-all-pages",
        action="store_true",
        help="Keep paging each site archive section until no new article URLs are found.",
    )
    collect_parser.add_argument(
        "--site-archive-url",
        action="append",
        dest="site_archive_urls",
        default=[],
        help="Archive section URL to include with --site. Repeatable. Defaults to built-in NC sections.",
    )
    collect_parser.add_argument(
        "--sitemap-url",
        default=DEFAULT_SITEMAP_URL,
        help="Root sitemap URL to crawl when --sitemap is used.",
    )
    collect_parser.add_argument(
        "--skip-peer-reviews",
        action="store_true",
        help="Do not download peer review PDFs.",
    )
    collect_parser.add_argument(
        "--force",
        action="store_true",
        help="Re-scrape URLs even if they already exist in articles.jsonl.",
    )
    collect_parser.add_argument(
        "--skip-failed-cache",
        action="store_true",
        help="Skip URLs that previously failed instead of retrying them.",
    )
    collect_parser.add_argument(
        "--fill-gaps",
        action="store_true",
        help=(
            "Re-visit every archive page to download articles missed in previous runs. "
            "Clears the page-level checkpoint so all pages are re-checked; "
            "already-crawled articles are skipped via the article-level cache."
        ),
    )
    collect_parser.add_argument(
        "--json-output",
        action="store_true",
        help="Print newly collected records as JSON to stdout (default: off).",
    )
    collect_parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of concurrent article scrape workers.",
    )
    collect_parser.add_argument(
        "--requests-per-second",
        type=float,
        help="Global rate limit across scrapes and peer review downloads.",
    )
    collect_parser.add_argument(
        "--output-dir",
        type=Path,
        help="Root output directory for all saved files (overrides NC_OUTPUT_DIR).",
    )
    return parser


def run_discover(collector: Collector, args: argparse.Namespace) -> int:
    urls = collector.discover(query=args.query, limit=args.limit)
    for url in urls:
        print(url)
    return 0


def run_discover_archive(collector: Collector, args: argparse.Namespace) -> int:
    max_pages = None if args.all_pages else args.pages
    archive_url = listing_url_with_year(args.archive_url, args.year)
    urls = collector.discover_archive(
        archive_url=archive_url,
        max_pages=max_pages,
        progress_callback=emit_discovery_progress,
    )
    for url in urls:
        print(url)
    return 0


def run_discover_site(collector: Collector, args: argparse.Namespace) -> int:
    archive_urls = args.archive_urls or DEFAULT_SITE_ARCHIVE_URLS
    max_pages = None if args.all_pages else args.pages
    urls = collector.discover_site(
        archive_urls=archive_urls,
        max_pages=max_pages,
        progress_callback=emit_discovery_progress,
    )
    for url in urls:
        print(url)
    return 0


def run_discover_sitemap(collector: Collector, args: argparse.Namespace) -> int:
    urls = collector.discover_sitemap(
        sitemap_url=args.sitemap_url,
        progress_callback=emit_discovery_progress,
    )
    for url in urls:
        print(url)
    return 0


def run_stats(collector: Collector, args: argparse.Namespace) -> int:
    if getattr(args, "list_failed", False):
        failed = collector.storage.load_failed_urls()
        if not failed:
            print("No failed URLs.", file=sys.stderr)
        else:
            print(f"{len(failed)} failed URL(s):", file=sys.stderr)
            for entry in failed:
                print(f"  {entry['url']}  [{entry['error']}]")
        return 0
    stats = collector.stats()
    if args.json:
        print(json.dumps(stats, ensure_ascii=False, indent=2))
    else:
        for key, value in stats.items():
            print(f"{key}: {value}")
    return 0


def run_collect(collector: Collector, args: argparse.Namespace) -> int:
    urls = list(args.url)
    records = []
    if args.input:
        urls.extend(load_urls_from_file(args.input))
    if args.query:
        urls.extend(collector.discover(query=args.query, limit=args.limit))
    if not urls and not args.archive and not args.site and not args.sitemap:
        raise SystemExit("No input URLs provided. Use --url, --input, --query, --archive, --site, or --sitemap.")

    if urls:
        records.extend(
            collector.collect_urls(
                urls=urls,
                download_peer_reviews=not args.skip_peer_reviews,
                resume=not args.force,
                retry_failed=not args.skip_failed_cache,
            )
        )
    if args.archive:
        archive_max_pages = None if args.archive_all_pages else args.archive_pages
        archive_url = listing_url_with_year(args.archive_url, args.year)
        if getattr(args, "fill_gaps", False):
            records.extend(
                collector.fill_gaps_archive(
                    archive_url=archive_url,
                    max_pages=archive_max_pages,
                    download_peer_reviews=not args.skip_peer_reviews,
                    progress_callback=emit_discovery_progress,
                )
            )
        else:
            records.extend(
                collector.collect_archive(
                    archive_url=archive_url,
                    max_pages=archive_max_pages,
                    download_peer_reviews=not args.skip_peer_reviews,
                    resume=not args.force,
                    retry_failed=not args.skip_failed_cache,
                    progress_callback=emit_discovery_progress,
                )
            )
    if args.site:
        site_max_pages = None if args.site_all_pages else args.site_pages
        records.extend(
            collector.collect_site(
                archive_urls=args.site_archive_urls or DEFAULT_SITE_ARCHIVE_URLS,
                max_pages=site_max_pages,
                download_peer_reviews=not args.skip_peer_reviews,
                resume=not args.force,
                retry_failed=not args.skip_failed_cache,
                progress_callback=emit_discovery_progress,
            )
        )
    if args.sitemap:
        records.extend(
            collector.collect_sitemap(
                sitemap_url=args.sitemap_url,
                download_peer_reviews=not args.skip_peer_reviews,
                resume=not args.force,
                retry_failed=not args.skip_failed_cache,
                progress_callback=emit_discovery_progress,
            )
        )
    stats = collector.stats()
    print(
        f"[done] new={len(records)} total={stats['article_records']} "
        f"failed={stats['failed_urls']}",
        file=sys.stderr,
    )
    if getattr(args, "json_output", False):
        print(json.dumps([record.to_dict() for record in records], ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings.from_env()
    if getattr(args, "max_workers", None) is not None:
        settings.max_workers = args.max_workers
    if getattr(args, "requests_per_second", None) is not None:
        settings.requests_per_second = args.requests_per_second
    if getattr(args, "output_dir", None) is not None:
        settings.output_dir = args.output_dir.expanduser()
    if getattr(args, "engine", "firecrawl") == "native":
        client = NativeNatureClient(timeout_seconds=settings.request_timeout_seconds)
    else:
        if not settings.firecrawl_api_key:
            raise ValueError("FIRECRAWL_API_KEY is not set.")
        client = FirecrawlClient(api_key=settings.firecrawl_api_key)
    collector = Collector(settings, client=client)

    if args.command == "discover":
        return run_discover(collector, args)
    if args.command == "discover-archive":
        return run_discover_archive(collector, args)
    if args.command == "discover-site":
        return run_discover_site(collector, args)
    if args.command == "discover-sitemap":
        return run_discover_sitemap(collector, args)
    if args.command == "stats":
        return run_stats(collector, args)
    if args.command == "collect":
        return run_collect(collector, args)

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
