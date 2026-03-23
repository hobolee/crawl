from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any
import re


WHITESPACE_RE = re.compile(r"\s+")


def normalize_text_key(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = WHITESPACE_RE.sub(" ", value.strip().lower())
    return normalized or None


@dataclass
class ArticleRecord:
    article_url: str
    slug: str
    title: str | None = None
    doi: str | None = None
    journal: str | None = None
    published_date: str | None = None
    abstract: str | None = None
    body_markdown: str | None = None
    article_pdf_url: str | None = None
    peer_review_pdf_url: str | None = None
    markdown_path: str | None = None
    article_pdf_path: str | None = None
    peer_review_pdf_path: str | None = None
    record_path: str | None = None
    detailed_metadata: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_summary_dict(self) -> dict[str, Any]:
        return {
            "article_url": self.article_url,
            "slug": self.slug,
            "title": self.title,
            "doi": self.doi,
            "journal": self.journal,
            "published_date": self.published_date,
            "abstract": self.abstract,
            "article_pdf_url": self.article_pdf_url,
            "peer_review_pdf_url": self.peer_review_pdf_url,
            "markdown_path": self.markdown_path,
            "article_pdf_path": self.article_pdf_path,
            "peer_review_pdf_path": self.peer_review_pdf_path,
            "record_path": self.record_path,
        }

    def duplicate_keys(self) -> set[str]:
        keys: set[str] = set()
        if self.slug:
            keys.add(f"slug:{self.slug}")
        if self.article_url:
            keys.add(f"url:{self.article_url.lower()}")
        if self.doi:
            keys.add(f"doi:{self.doi.lower()}")
        if self.article_pdf_url:
            keys.add(f"article_pdf:{self.article_pdf_url.lower()}")
        if self.peer_review_pdf_url:
            keys.add(f"peer_review_pdf:{self.peer_review_pdf_url.lower()}")

        title_key = normalize_text_key(self.title)
        journal_key = normalize_text_key(self.journal)
        if title_key and journal_key:
            keys.add(f"title_journal:{journal_key}:{title_key}")
        elif title_key:
            keys.add(f"title:{title_key}")

        return keys
