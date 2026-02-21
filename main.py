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
    
    console.print(f"\n[bold green]✓ Done! Results in {out_dir}[/bold green]")

def run_batch_workflow(n: int, out_dir: Path):
    """Workflow 2: Fetch CSV, scrape N URLs, parse, and cleanup."""
    html_out = Path("html_out")
    html_out.mkdir(parents=True, exist_ok=True)
    
    console.print(Panel(f"[bold cyan]Workflow:[/bold cyan] Batch Fetch ({n} records)\n[bold white]Output:[/bold white] {out_dir}", border_style="blue"))
    
    # 1. Fetch CSV
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
    if html_out.exists():
        console.print(f"[dim]→ Cleaning up intermediate files in {html_out}...[/dim]")
        shutil.rmtree(html_out)
    
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
