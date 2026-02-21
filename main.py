import argparse
import os
import shutil
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

# Import our refactored modules
import scrape_nsf
import scrape_nsf_url
import parse_html

console = Console()

def run_url_workflow(url: str, out_dir: Path):
    """Workflow 1: Scrape single URL and parse."""
    html_out = Path("html_out")
    html_out.mkdir(parents=True, exist_ok=True)
    
    console.print(Panel(f"[bold cyan]Workflow:[/bold cyan] Single URL Scrape & Parse\n[bold white]URL:[/bold white] {url}", border_style="blue"))
    
    # 1. Scrape
    u = scrape_nsf_url.normalize_url(url)
    job = scrape_nsf_url.Job(url=u)
    scrape_nsf_url.run_scraping_jobs([job], str(html_out))
    
    # 2. Parse
    json_dir = out_dir / "json"
    parse_html.run_batch_parsing(html_out, out_dir, json_dir)
    
    # 3. Cleanup
    console.print(f"[dim]→ Cleaning up intermediate files in {html_out}...[/dim]")
    robust_rmtree(html_out)
    
    # Also cleanup results.jsonl if it exists
    results_file = Path("results.jsonl")
    if results_file.exists():
        results_file.unlink()

    console.print(f"\n[bold green]✓ Done! Results in {out_dir}[/bold green]")

def robust_rmtree(path: Path):
    """Windows-friendly rmtree with retries and fallback to shell rmdir."""
    if not path.exists():
        return
    
    import time
    import stat
    
    def on_error(func, path, exc_info):
        """Fix read-only files and retry."""
        os.chmod(path, stat.S_IWRITE)
        func(path)

    for i in range(3):
        try:
            shutil.rmtree(path, onerror=on_error)
            return
        except Exception:
            time.sleep(1)
    
    # Final fallback via shell
    try:
        os.system(f'rmdir /s /q "{path}"')
    except Exception as e:
        console.print(f"[yellow]⚠ Warning: Could not clean up {path}: {e}[/yellow]")

def run_batch_workflow(n: int, out_dir: Path):
    """Workflow 2: Fetch CSV, scrape N URLs, parse, and cleanup."""
    html_out = Path("html_out")
    
    # 0. Pre-Cleanup
    robust_rmtree(html_out)
    html_out.mkdir(parents=True, exist_ok=True)
    
    console.print(Panel(f"[bold cyan]Workflow:[/bold cyan] Batch Fetch ({n} records)\n[bold white]Output:[/bold white] {out_dir}", border_style="blue"))
    
    # 1. Fetch CSV
    existing_csvs = list(Path(".").glob("nsf_*.csv"))
    if existing_csvs:
        csv_path = existing_csvs[0]
        console.print(f"[dim]→ Using existing CSV: {csv_path}[/dim]")
    else:
        csv_path = scrape_nsf.fetch_nsf_csv()
    if not csv_path:
        console.print("[red]✗ Failed to fetch NSF CSV. Aborting.[/red]")
        return

    # 2. Load jobs
    jobs_all = scrape_nsf_url.load_jobs_from_csv(csv_path)
    jobs = jobs_all[:n]
    
    # 3. Scrape
    scrape_nsf_url.run_scraping_jobs(jobs, str(html_out))
    
    # 4. Parse
    json_dir = out_dir / "json"
    parse_html.run_batch_parsing(html_out, out_dir, json_dir)
    
    # 5. Cleanup
    console.print(f"[dim]→ Cleaning up intermediate files in {html_out}...[/dim]")
    robust_rmtree(html_out)
    
    # Also cleanup results.jsonl if it exists
    results_file = Path("results.jsonl")
    if results_file.exists():
        results_file.unlink()

    console.print(f"\n[bold green]✓ Done! Processed {len(jobs)} records. Results in {out_dir}[/bold green]")

def main():
    parser = argparse.ArgumentParser(description="NSF FOA Unified Tool")
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--url", type=str, help="Scrape and parse a single URL")
    group.add_argument("-n", type=int, help="Fetch CSV, scrape N URLs, parse, and cleanup")
    
    parser.add_argument("--out_dir", type=str, default="./out", help="Output directory (default: ./out)")
    
    args = parser.parse_args()
    out_dir = Path(args.out_dir)

    try:
        if args.url:
            run_url_workflow(args.url, out_dir)
        elif args.n:
            run_batch_workflow(args.n, out_dir)
    except Exception as e:
        console.print(f"[bold red]FATAL ERROR:[/bold red] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
