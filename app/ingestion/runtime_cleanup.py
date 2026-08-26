from __future__ import annotations

import gc
import time
from pathlib import Path
from typing import Any, Iterable

from app.core.config import Settings, get_settings


MEDIA_SUFFIXES = {
    ".3gp",
    ".aac",
    ".flv",
    ".jpeg",
    ".jpg",
    ".m4a",
    ".m4v",
    ".mov",
    ".mp3",
    ".mp4",
    ".part",
    ".png",
    ".ts",
    ".webm",
    ".webp",
}


def cleanup_after_collection(*, settings: Settings | None = None) -> dict[str, Any]:
    resolved = settings or get_settings()
    files_removed = 0
    bytes_removed = 0
    dirs_removed = 0
    errors: list[str] = []

    if resolved.collector_cleanup_enabled:
        cutoff = time.time() - max(0, resolved.collector_cleanup_retention_hours) * 3600
        for root in _platform_roots(resolved):
            removed = _remove_stale_media(root, cutoff=cutoff)
            files_removed += removed["filesRemoved"]
            bytes_removed += removed["bytesRemoved"]
            errors.extend(removed["errors"])
            if resolved.collector_cleanup_empty_dirs:
                empty_removed = _remove_empty_dirs(root)
                dirs_removed += empty_removed["dirsRemoved"]
                errors.extend(empty_removed["errors"])

    gc_collected = gc.collect() if resolved.collector_memory_gc_enabled else 0
    return {
        "status": "success" if not errors else "partial",
        "filesRemoved": files_removed,
        "bytesRemoved": bytes_removed,
        "dirsRemoved": dirs_removed,
        "gcCollected": gc_collected,
        "errors": errors[:20],
    }


def cleanup_related_media_files(path: Path | str) -> dict[str, Any]:
    base = Path(path)
    candidates = _related_media_candidates(base)
    files_removed = 0
    bytes_removed = 0
    errors: list[str] = []
    for candidate in candidates:
        try:
            if not candidate.is_file():
                continue
            size = candidate.stat().st_size
            candidate.unlink()
            files_removed += 1
            bytes_removed += size
        except OSError as exc:
            errors.append(f"{candidate}: {exc}")
    return {
        "status": "success" if not errors else "partial",
        "filesRemoved": files_removed,
        "bytesRemoved": bytes_removed,
        "errors": errors[:20],
    }


def _platform_roots(settings: Settings) -> list[Path]:
    return [
        settings.youtube_output_root,
        settings.tiktok_output_root,
        settings.facebook_output_root,
        settings.x_output_root,
        settings.instagram_output_root,
    ]


def _remove_stale_media(root: Path, *, cutoff: float) -> dict[str, Any]:
    files_removed = 0
    bytes_removed = 0
    errors: list[str] = []
    if not root.exists():
        return {"filesRemoved": 0, "bytesRemoved": 0, "errors": []}
    for path in _safe_files(root):
        if path.suffix.lower() not in MEDIA_SUFFIXES:
            continue
        try:
            stat = path.stat()
            if stat.st_mtime > cutoff:
                continue
            path.unlink()
            files_removed += 1
            bytes_removed += stat.st_size
        except OSError as exc:
            errors.append(f"{path}: {exc}")
    return {"filesRemoved": files_removed, "bytesRemoved": bytes_removed, "errors": errors}


def _remove_empty_dirs(root: Path) -> dict[str, Any]:
    dirs_removed = 0
    errors: list[str] = []
    if not root.exists():
        return {"dirsRemoved": 0, "errors": []}
    for path in sorted((item for item in root.rglob("*") if item.is_dir()), key=lambda item: len(item.parts), reverse=True):
        try:
            path.rmdir()
            dirs_removed += 1
        except OSError:
            continue
    return {"dirsRemoved": dirs_removed, "errors": errors}


def _safe_files(root: Path) -> Iterable[Path]:
    try:
        yield from (item for item in root.rglob("*") if item.is_file())
    except OSError:
        return


def _related_media_candidates(path: Path) -> list[Path]:
    candidates = [path]
    if path.suffix:
        candidates.append(path.with_name(f"{path.name}.part"))
        candidates.append(path.with_name(f"{path.stem}-collector-compatible{path.suffix}"))
    candidates.extend(path.parent.glob(f"{path.stem}.*.part"))
    candidates.extend(path.parent.glob(f"{path.stem}-collector-compatible.*"))

    seen: set[Path] = set()
    unique: list[Path] = []
    for candidate in candidates:
        resolved = candidate
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    return unique
