#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

console = Console()


# ----------------------------
# Helpers
# ----------------------------
def normalize_url(u: str) -> Optional[str]:
    if not u:
        return None
    u = u.strip().strip('"').strip("'")
    if not u:
        return None
    if u.startswith("http://") or u.startswith("https://"):
        return u
    return "https://" + u


def safe_filename(s: str, max_len: int = 140) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-zA-Z0-9._ -]+", "_", s)
    s = s.replace(" ", "_")
    if len(s) > max_len:
        s = s[:max_len].rstrip("_")
    return s or "page"


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


@dataclass
class Job:
    url: str
    title: Optional[str] = None
    program_id: Optional[str] = None
    nsf_pd_num: Optional[str] = None


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return s


def fetch_html(
    session: requests.Session,
    url: str,
    timeout: float,
    retries: int,
    backoff: float,
) -> Tuple[bool, Optional[int], str]:
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if 200 <= r.status_code < 300:
                return True, r.status_code, r.text
            last_err = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last_err = f"{type(e).__name__}: {e}"

        if attempt < retries:
            time.sleep(backoff * (2 ** attempt))

    return False, None, (last_err or "Unknown error")


def load_jobs_from_csv(path: str) -> List[Job]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")

    jobs: List[Job] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV has no header row.")

        target_col = "Solicitation URL"
        if target_col not in reader.fieldnames:
            raise ValueError(
                f'CSV missing required column "{target_col}". '
                f"Found: {reader.fieldnames}"
            )

        for row in reader:
            url = normalize_url(row.get(target_col, ""))
            if not url:
                continue
            jobs.append(
                Job(
                    url=url,
                    title=(row.get("Title") or "").strip() or None,
                    program_id=(row.get("Program ID") or "").strip() or None,
                    nsf_pd_num=(row.get("NSF/PD Num") or "").strip() or None,
                )
            )

    # Deduplicate URLs, keep order
    seen = set()
    uniq: List[Job] = []
    for j in jobs:
        if j.url in seen:
            continue
        seen.add(j.url)
        uniq.append(j)
    return uniq


def output_path_for(job: Job, out_dir: str) -> str:
    parts = []
    if job.nsf_pd_num:
        parts.append(job.nsf_pd_num)
    if job.program_id:
        parts.append(job.program_id)
    if job.title:
        parts.append(job.title)

    base = safe_filename("__".join(parts)) if parts else safe_filename(job.url)
    h = sha1(job.url)[:10]
    return os.path.join(out_dir, f"{base}__{h}.html")


def process_one(
    job: Job,
    out_dir: str,
    timeout: float,
    retries: int,
    backoff: float,
) -> Dict:
    session = build_session()
    ok, status, payload = fetch_html(session, job.url, timeout, retries, backoff)

    res = {
        "url": job.url,
        "ok": ok,
        "http_status": status,
        "error": None if ok else payload,
        "saved_path": None,
    }

    if not ok:
        return res

    soup = BeautifulSoup(payload, "html.parser")
    html_pretty = soup.prettify()

    os.makedirs(out_dir, exist_ok=True)
    path = output_path_for(job, out_dir)
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(html_pretty)

    res["saved_path"] = path
    return res


def run_scraping_jobs(jobs, out_dir, threads=16, timeout=25.0, retries=2, backoff=0.8, results_file="results.jsonl"):
    os.makedirs(out_dir, exist_ok=True)

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )

    task_id = progress.add_task("Scraping", total=len(jobs))

    ok_count = 0
    fail_count = 0
    failures: List[Dict] = []

    with open(results_file, "w", encoding="utf-8") as results_fp:
        with progress:
            with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
                futs = [
                    ex.submit(process_one, j, out_dir, timeout, retries, backoff)
                    for j in jobs
                ]
                for fut in as_completed(futs):
                    res = fut.result()
                    results_fp.write(json.dumps(res, ensure_ascii=False) + "\n")
                    results_fp.flush()

                    if res["ok"]:
                        ok_count += 1
                    else:
                        fail_count += 1
                        failures.append(res)

                    progress.advance(task_id)

    table = Table(title="Scrape Summary")
    table.add_column("Total", justify="right")
    table.add_column("OK", justify="right", style="green")
    table.add_column("Failed", justify="right", style="red")
    table.add_row(str(len(jobs)), str(ok_count), str(fail_count))
    console.print(table)

    if failures:
        console.print("[bold red]Failures (first 10):[/bold red]")
        for f in failures[:10]:
            console.print(f"- {f['url']} :: {f['error']}")

    console.print(f"\nSaved HTML to: [bold]{out_dir}[/bold]")
    return ok_count, fail_count


def main():
    parser = argparse.ArgumentParser(
        description="Scrape solicitation pages (save full HTML). Either --url OR -n N from CSV."
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--url", type=str, help="Scrape a single URL")
    group.add_argument(
        "-n",
        type=int,
        metavar="N",
        default = 1,
        help="Scrape first N URLs from CSV (column: 'Solicitation URL')",
    )

    parser.add_argument("--csv", type=str, default="nsf_opps_20260221_143120.csv", help="CSV path (default: input.csv)")
    parser.add_argument("--out", type=str, default="html_out", help="Output directory")
    parser.add_argument("--threads", type=int, default=16, help="Thread count")
    parser.add_argument("--timeout", type=float, default=25.0, help="Request timeout seconds")
    parser.add_argument("--retries", type=int, default=2, help="Retries per URL")
    parser.add_argument("--backoff", type=float, default=0.8, help="Retry backoff base seconds")
    parser.add_argument("--results", type=str, default="results.jsonl", help="JSONL results log")

    args = parser.parse_args()

    # Build jobs
    if args.url:
        u = normalize_url(args.url)
        if not u:
            console.print("[red]Invalid --url[/red]")
            sys.exit(2)
        jobs = [Job(url=u)]
    else:
        if args.n <= 0:
            console.print("[red]-n must be a positive integer[/red]")
            sys.exit(2)
        jobs_all = load_jobs_from_csv(args.csv)
        jobs = jobs_all[: args.n]

    if not jobs:
        console.print("[yellow]No URLs to scrape.[/yellow]")
        sys.exit(0)

    run_scraping_jobs(jobs, args.out, args.threads, args.timeout, args.retries, args.backoff, args.results)


if __name__ == "__main__":
    main()