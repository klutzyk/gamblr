from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[4]
RAW_ROOT = ROOT_DIR / "data" / "raw" / "mlb"


@dataclass(slots=True)
class RawFileRecord:
    relative_path: str
    absolute_path: Path
    fetched_at: datetime


def _timestamp_slug(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)
    return current.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "_", value.strip())
    return slug.strip("._-") or "payload"


def write_json_payload(
    source: str,
    resource_type: str,
    name: str,
    payload: object,
) -> RawFileRecord:
    fetched_at = datetime.now(timezone.utc)
    target_dir = RAW_ROOT / _safe_slug(source) / _safe_slug(resource_type)
    target_dir.mkdir(parents=True, exist_ok=True)
    absolute_path = target_dir / f"{_safe_slug(name)}_{_timestamp_slug(fetched_at)}.json"
    absolute_path.write_text(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return RawFileRecord(
        relative_path=str(absolute_path.relative_to(ROOT_DIR)),
        absolute_path=absolute_path,
        fetched_at=fetched_at,
    )


def write_text_payload(
    source: str,
    resource_type: str,
    name: str,
    text: str,
    extension: str = "csv",
) -> RawFileRecord:
    fetched_at = datetime.now(timezone.utc)
    target_dir = RAW_ROOT / _safe_slug(source) / _safe_slug(resource_type)
    target_dir.mkdir(parents=True, exist_ok=True)
    absolute_path = target_dir / f"{_safe_slug(name)}_{_timestamp_slug(fetched_at)}.{_safe_slug(extension)}"
    absolute_path.write_text(text, encoding="utf-8")
    return RawFileRecord(
        relative_path=str(absolute_path.relative_to(ROOT_DIR)),
        absolute_path=absolute_path,
        fetched_at=fetched_at,
    )
