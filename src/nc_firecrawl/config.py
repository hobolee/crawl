from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


load_dotenv()


@dataclass
class Settings:
    output_dir: Path
    firecrawl_api_key: str = ""
    request_timeout_seconds: int = 90
    max_workers: int = 4
    requests_per_second: float = 1.0

    @classmethod
    def from_env(cls) -> "Settings":
        api_key = os.getenv("FIRECRAWL_API_KEY", "").strip()
        raw_output_dir = os.getenv("NC_OUTPUT_DIR", "./data").strip() or "./data"
        max_workers = int(os.getenv("NC_MAX_WORKERS", "4").strip() or "4")
        requests_per_second = float(os.getenv("NC_REQUESTS_PER_SECOND", "1.0").strip() or "1.0")
        return cls(
            firecrawl_api_key=api_key,
            output_dir=Path(raw_output_dir).expanduser(),
            max_workers=max_workers,
            requests_per_second=requests_per_second,
        )
