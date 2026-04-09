from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, TextIO

from scraper.careers import fetch_external_jobs
from scraper.export import flatten_record, merge_shard_csv_files, write_csv, write_pretty_json
from scraper.yc_algolia import iter_companies
from scraper.yc_waas import fetch_yc_listed_jobs


def _parse_parallel_parent(argv: list[str]) -> tuple[list[str], int, bool]:
    """Extract --parallel N and --no-merge-csv; return (rest argv, N, skip_csv_merge)."""
    parallel_n = 0
    no_merge_csv = False
    rest: list[str] = []
    i = 0
    while i < len(argv):
        if argv[i] == "--parallel" and i + 1 < len(argv):
            parallel_n = int(argv[i + 1])
            i += 2
            continue
        if argv[i].startswith("--parallel="):
            parallel_n = int(argv[i].split("=", 1)[1])
            i += 1
            continue
        if argv[i] == "--no-merge-csv":
            no_merge_csv = True
            i += 1
            continue
        rest.append(argv[i])
        i += 1
    return rest, parallel_n, no_merge_csv


def _output_base_from_argv(argv: list[str]) -> str:
    for i, a in enumerate(argv):
        if a in ("-o", "--output") and i + 1 < len(argv):
            return argv[i + 1]
    return "jobs"


def _strip_shard_cli_args(argv: list[str]) -> list[str]:
    """Remove --shards / --shard so a parent can inject its own."""
    out: list[str] = []
    i = 0
    while i < len(argv):
        if argv[i] in ("--shards", "--shard") and i + 1 < len(argv):
            i += 2
            continue
        out.append(argv[i])
        i += 1
    return out


def _shard_output_base(output: str, shard: int, num_shards: int) -> str:
    """Avoid workers overwriting the same files (jobs → jobs-shard0, export.jsonl → export-shard0.jsonl)."""
    if num_shards <= 1 or output == "-":
        return output
    p = Path(output)
    suf = p.suffix.lower()
    if suf == ".jsonl":
        return str(p.with_name(f"{p.stem}-shard{shard}{p.suffix}"))
    if suf in (".json", ".csv"):
        return str(p.with_name(f"{p.stem}-shard{shard}{p.suffix}"))
    return str(p.with_name(f"{p.name}-shard{shard}"))


def _output_paths(output: str) -> tuple[Path | None, Path | None, Path | None, bool]:
    """Returns (jsonl_path, json_path, csv_path, use_stdout)."""
    if output == "-":
        return None, None, None, True
    base = Path(output)
    if base.suffix == ".jsonl":
        stem = base.stem
        parent = base.parent
        return base, parent / f"{stem}.json", parent / f"{stem}.csv", False
    return base.with_suffix(".jsonl"), base.with_suffix(".json"), base.with_suffix(".csv"), False


def _log(msg: str, *, quiet: bool) -> None:
    if not quiet:
        print(msg, file=sys.stderr)


def _emit_job_terminal(
    company_name: str,
    title: str,
    url: str | None,
    *,
    source: str,
    quiet: bool,
) -> None:
    if quiet:
        return
    u = (url or "").strip()
    line = f"  ✓ [{source}] {title}"
    if u:
        line += f"\n      {u}"
    print(line, file=sys.stderr)


def run(argv: list[str] | None = None) -> int:
    raw = list(argv if argv is not None else sys.argv[1:])
    raw, parallel_n, no_merge_csv = _parse_parallel_parent(raw)
    if parallel_n >= 2:
        child_argv = _strip_shard_cli_args(raw)
        procs = [
            subprocess.Popen(
                [sys.executable, "-m", "scraper", *child_argv, "--shards", str(parallel_n), "--shard", str(w)]
            )
            for w in range(parallel_n)
        ]
        codes = [p.wait() for p in procs]
        ok = all(c == 0 for c in codes)
        if ok and not no_merge_csv:
            out_base = _output_base_from_argv(child_argv)
            if out_base != "-":
                merged = merge_shard_csv_files(out_base)
                if merged:
                    print(f"Combined shard CSVs → {merged.resolve()}", file=sys.stderr)
        return 0 if ok else 1

    p = argparse.ArgumentParser(
        description=(
            "YC directory → for each company, scrape the public careers site first, "
            "then optionally YC Work at a Startup. External is never skipped just because YC has jobs; "
            "use --skip-external to turn the website crawl off. "
            "Use --shards N --shard I for disjoint workers, or --parallel N to spawn N processes."
        )
    )
    p.add_argument(
        "-o",
        "--output",
        default="jobs",
        help='Output base path: writes BASE.jsonl, BASE.json, BASE.csv (default: jobs). Use "-" for JSONL on stdout only.',
    )
    p.add_argument(
        "--max-companies",
        type=int,
        default=0,
        help="Stop after N companies (0 = no limit)",
    )
    p.add_argument(
        "--all-companies",
        action="store_true",
        help="Include YC directory companies not marked as hiring (much slower)",
    )
    p.add_argument(
        "--include-yc-jobs",
        action="store_true",
        help="Also fetch jobs from ycombinator.com/companies/{slug}/jobs (Work at a Startup)",
    )
    p.add_argument(
        "--skip-external",
        action="store_true",
        help="Do not crawl company websites (only useful with --include-yc-jobs)",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Seconds to sleep between companies (be polite)",
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Include JSONL / terminal notes when no careers page or no parseable jobs",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="No terminal progress (files only)",
    )
    p.add_argument(
        "--playwright",
        action="store_true",
        help="Load career pages with Chromium (Playwright) so JS-rendered job lists appear",
    )
    p.add_argument(
        "--playwright-wait-ms",
        type=int,
        default=6000,
        help="Extra wait after load for async job widgets (default: 6000)",
    )
    p.add_argument(
        "--playwright-fallback",
        action="store_true",
        help="If plain HTTP finds no jobs, retry career + hub URLs once with Playwright",
    )
    p.add_argument(
        "--shards",
        type=int,
        default=1,
        metavar="N",
        help="Split the YC directory into N disjoint slices (by stable slug hash). Default: 1 (all companies).",
    )
    p.add_argument(
        "--shard",
        type=int,
        default=0,
        metavar="I",
        help="Which slice this process handles: 0 .. N-1 (use with --shards).",
    )
    args = p.parse_args(argv)

    if args.skip_external and not args.include_yc_jobs:
        p.error("Use at least one of: (default) external careers, or --include-yc-jobs")

    if args.playwright or args.playwright_fallback:
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
        except ImportError:
            p.error(
                "Playwright is not installed for this Python interpreter. Run:\n"
                "  python -m pip install playwright\n"
                "  python -m playwright install chromium\n"
                "Use the same `python` as `python -m scraper` (conda: not a separate system pip)."
            )

    if args.shards < 1:
        p.error("--shards must be >= 1")
    if not (0 <= args.shard < args.shards):
        p.error("--shard must satisfy 0 <= shard < --shards")

    out_base = _shard_output_base(args.output, args.shard, args.shards)
    jsonl_path, json_path, csv_path, use_stdout = _output_paths(out_base)

    jsonl_fp: TextIO | None = None
    if jsonl_path is not None:
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        jsonl_fp = jsonl_path.open("w", encoding="utf-8")
    elif use_stdout:
        jsonl_fp = sys.stdout

    all_records: list[dict[str, Any]] = []

    def write_record(rec: dict[str, Any]) -> None:
        all_records.append(rec)
        assert jsonl_fp is not None
        jsonl_fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
        jsonl_fp.flush()

    try:
        n = 0
        total_hint = f" (max {args.max_companies})" if args.max_companies else ""
        _log(f"Starting scrape{total_hint}…", quiet=args.quiet)
        if args.shards > 1:
            _log(
                f"Shard {args.shard}/{args.shards} — only companies whose slug maps to this slice (no overlap).",
                quiet=args.quiet,
            )
        if jsonl_path:
            _log(f"Writing: {jsonl_path}, {json_path}, {csv_path}", quiet=args.quiet)
        elif use_stdout:
            _log("JSONL → stdout; progress below", quiet=args.quiet)
        if args.playwright:
            _log("External careers: using Playwright (Chromium) — slower but sees JS listings.", quiet=args.quiet)
        elif args.playwright_fallback:
            _log("External careers: Playwright fallback if HTTP returns no jobs.", quiet=args.quiet)

        for hit in iter_companies(
            only_hiring=not args.all_companies,
            shard=args.shard,
            num_shards=args.shards,
        ):
            if args.max_companies and n >= args.max_companies:
                break
            slug = hit.get("slug") or ""
            name = hit.get("name") or ""
            website = hit.get("website")
            company = {
                "yc_slug": slug,
                "yc_name": name,
                "website": website,
                "batch": hit.get("batch"),
            }

            _log(f"\n[{n + 1}] {name}  |  {website or 'no website'}  |  yc.com/{slug}", quiet=args.quiet)

            # Company careers site first (often more up to date than YC). Always runs unless --skip-external.
            if not args.skip_external:
                try:
                    _log("  → External careers site…", quiet=args.quiet)
                    careers_url, jobs = fetch_external_jobs(
                        website or "",
                        use_playwright=args.playwright,
                        playwright_wait_ms=args.playwright_wait_ms,
                    )
                    if not jobs and args.playwright_fallback and not args.playwright:
                        careers_url, jobs = fetch_external_jobs(
                            website or "",
                            use_playwright=True,
                            playwright_wait_ms=args.playwright_wait_ms,
                        )
                        if jobs and not args.quiet:
                            _log("     (Playwright fallback found jobs)", quiet=False)
                    if careers_url:
                        _log(f"     Opened: {careers_url}", quiet=args.quiet)
                    else:
                        _log("     No careers URL resolved from website", quiet=args.quiet)

                    if jobs:
                        _log(f"     Parsed {len(jobs)} job(s) from careers site", quiet=args.quiet)
                    else:
                        _log(
                            "     0 jobs parsed from HTML/ATS (page may be JS-only or unsupported board)",
                            quiet=args.quiet,
                        )

                    for job in jobs:
                        rec = {
                            "source": "company_careers",
                            "company": company,
                            "careers_url": careers_url,
                            "job": job,
                        }
                        write_record(rec)
                        _emit_job_terminal(
                            name,
                            str(job.get("title") or ""),
                            job.get("url"),
                            source=str(job.get("source") or "careers"),
                            quiet=args.quiet,
                        )

                    if args.verbose:
                        if not jobs and careers_url:
                            write_record(
                                {
                                    "source": "company_careers",
                                    "company": company,
                                    "careers_url": careers_url,
                                    "job": None,
                                    "note": "No jobs parsed (unknown ATS or empty page)",
                                }
                            )
                        if not careers_url:
                            write_record(
                                {
                                    "source": "company_careers",
                                    "company": company,
                                    "careers_url": None,
                                    "note": "Could not find careers page from website",
                                }
                            )
                except Exception as e:
                    err = {
                        "source": "error",
                        "company": company,
                        "stage": "external_careers",
                        "error": str(e),
                    }
                    write_record(err)
                    _log(f"  ! External careers error: {e}", quiet=args.quiet)

            if args.include_yc_jobs:
                try:
                    _log("  → YC /jobs (Work at a Startup)…", quiet=args.quiet)
                    yc_jobs = fetch_yc_listed_jobs(slug)
                    if not yc_jobs:
                        _log("     No listings on ycombinator.com/.../jobs", quiet=args.quiet)
                    else:
                        _log(f"     {len(yc_jobs)} listing(s) on YC", quiet=args.quiet)
                    for jp in yc_jobs:
                        url = jp.get("url")
                        if isinstance(url, str) and url.startswith("/"):
                            url = "https://www.ycombinator.com" + url
                        rec = {
                            "source": "yc_work_at_startup",
                            "company": company,
                            "job": {
                                "title": jp.get("title"),
                                "location": jp.get("location"),
                                "type": jp.get("type"),
                                "url": url,
                                "apply_url": jp.get("applyUrl"),
                                "salary_range": jp.get("salaryRange"),
                            },
                        }
                        write_record(rec)
                        _emit_job_terminal(
                            name,
                            str(jp.get("title") or ""),
                            url,
                            source="YC",
                            quiet=args.quiet,
                        )
                except Exception as e:
                    err = {
                        "source": "error",
                        "company": company,
                        "stage": "yc_jobs",
                        "error": str(e),
                    }
                    write_record(err)
                    _log(f"  ! YC jobs error: {e}", quiet=args.quiet)

            n += 1
            if args.delay > 0:
                time.sleep(args.delay)

        flat = [flatten_record(r) for r in all_records]
        job_like = sum(1 for r in flat if (r.get("job_title") or "").strip())

        if json_path is not None and csv_path is not None:
            write_pretty_json(json_path, flat)
            write_csv(csv_path, flat)

        summary = f"\nDone. {len(all_records)} rows ({job_like} with a job title)."
        if jsonl_path:
            summary += f"\n  JSONL: {jsonl_path.resolve()}"
        elif use_stdout:
            summary += "\n  JSONL: (stdout)"
        if json_path and csv_path:
            summary += f"\n  JSON:  {json_path.resolve()}\n  CSV:   {csv_path.resolve()}"
        _log(summary, quiet=args.quiet)
    finally:
        if jsonl_fp is not None and jsonl_fp is not sys.stdout:
            jsonl_fp.close()

    return 0


def merge_csv_main(argv: list[str] | None = None) -> int:
    """CLI: python -m scraper merge -o jobs"""
    ap = argparse.ArgumentParser(
        description="Merge jobs-shard0.csv, jobs-shard1.csv, … into a single CSV (same -o base as scrape).",
    )
    ap.add_argument(
        "-o",
        "--output",
        default="jobs",
        metavar="BASE",
        help="Output base used when scraping (default: jobs → reads jobs-shard*.csv, writes jobs.csv)",
    )
    ap.add_argument(
        "--dest",
        default="",
        metavar="PATH",
        help="Merged CSV path (default: {BASE}.csv next to shard files)",
    )
    args = ap.parse_args(argv)
    dest = Path(args.dest) if args.dest.strip() else None
    merged = merge_shard_csv_files(args.output, destination=dest)
    if not merged:
        print("No *-shard*.csv files found for that output base.", file=sys.stderr)
        return 1
    print(f"Wrote {merged.resolve()}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
