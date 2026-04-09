from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any

CSV_FIELDNAMES = [
    "source",
    "company_name",
    "company_slug",
    "batch",
    "website",
    "job_title",
    "location",
    "job_url",
    "apply_url",
    "careers_url",
    "salary_range",
    "job_type",
    "job_source",
    "stage",
    "error",
]


def _stem_and_parent_for_shards(output_base: str) -> tuple[Path, str]:
    """Directory + stem used in jobs-shard0.csv (handles -o jobs vs -o data/out vs -o x.jsonl)."""
    p = Path(output_base)
    parent = p.parent
    suf = p.suffix.lower()
    if suf in (".jsonl", ".json", ".csv"):
        return parent, p.stem
    return parent, p.name


def glob_shard_csv_paths(output_base: str) -> list[Path]:
    parent, stem = _stem_and_parent_for_shards(output_base)
    paths = list(parent.glob(f"{stem}-shard*.csv"))

    def sort_key(x: Path) -> tuple[int, str]:
        m = re.search(r"-shard(\d+)", x.stem, re.I)
        return (int(m.group(1)) if m else 0, x.name)

    return sorted(paths, key=sort_key)


def merge_shard_csv_files(output_base: str, *, destination: Path | None = None) -> Path | None:
    """
    Concatenate all {stem}-shard*.csv from the same directory into one CSV.
    Returns the written path, or None if no shard files exist.
    """
    paths = glob_shard_csv_paths(output_base)
    if not paths:
        return None
    parent, stem = _stem_and_parent_for_shards(output_base)
    if destination is None:
        destination = parent / f"{stem}.csv"

    fieldnames: list[str] | None = None
    rows: list[dict[str, Any]] = []
    for fp in paths:
        with fp.open(encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            if fieldnames is None:
                fieldnames = list(r.fieldnames or CSV_FIELDNAMES)
            for row in r:
                rows.append({k: (row.get(k) or "") for k in fieldnames})

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames or CSV_FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return destination


def flatten_record(rec: dict[str, Any]) -> dict[str, Any]:
    """One flat row per record for JSON array / CSV."""
    co = rec.get("company") or {}
    if rec.get("source") == "error":
        return {
            "source": "error",
            "company_name": co.get("yc_name"),
            "company_slug": co.get("yc_slug"),
            "batch": co.get("batch"),
            "website": co.get("website"),
            "stage": rec.get("stage"),
            "error": rec.get("error"),
            "job_title": "",
            "location": "",
            "job_url": "",
            "apply_url": "",
            "careers_url": "",
            "salary_range": "",
            "job_type": "",
            "job_source": "",
        }

    job = rec.get("job") or {}
    if rec.get("source") == "yc_work_at_startup":
        return {
            "source": "yc_work_at_startup",
            "company_name": co.get("yc_name"),
            "company_slug": co.get("yc_slug"),
            "batch": co.get("batch"),
            "website": co.get("website"),
            "careers_url": "",
            "job_title": job.get("title") or "",
            "location": job.get("location") or "",
            "job_url": job.get("url") or "",
            "apply_url": job.get("apply_url") or "",
            "salary_range": job.get("salary_range") or "",
            "job_type": job.get("type") or "",
            "job_source": "",
            "stage": "",
            "error": "",
        }

    # company_careers
    return {
        "source": "company_careers",
        "company_name": co.get("yc_name"),
        "company_slug": co.get("yc_slug"),
        "batch": co.get("batch"),
        "website": co.get("website"),
        "careers_url": rec.get("careers_url") or "",
        "job_title": job.get("title") or "",
        "location": job.get("location") or "",
        "job_url": job.get("url") or "",
        "apply_url": job.get("apply_url") or "",
        "salary_range": "",
        "job_type": "",
        "job_source": job.get("source") or "",
        "stage": "",
        "error": rec.get("note") or "",
    }


def write_pretty_json(path: Path, flat_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(flat_rows, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def write_csv(path: Path, flat_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        for row in flat_rows:
            w.writerow({k: row.get(k, "") for k in CSV_FIELDNAMES})
