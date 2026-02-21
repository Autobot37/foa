#!/usr/bin/env python3
# fetch nsf funding opportunities csv with auto waf token via headless chrome

import argparse
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from curl_cffi import requests as creq
from rich.console import Console
from rich.panel import Panel

console = Console()
CSV_URL = "https://www.nsf.gov/funding/opps/csvexport?page&_format=csv"
MAIN_URL = "https://www.nsf.gov/funding/opportunities"
IMPERSONATE = "chrome131"

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "dnt": "1",
    "sec-ch-ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
}


def get_waf_token():
    # launch headless chrome, visit nsf.gov, grab waf cookie
    console.print("[dim]→ launching chrome to solve waf...[/dim]")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=opts)
    # remove webdriver flag
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })

    try:
        driver.get(MAIN_URL)
        for i in range(25):
            time.sleep(1)
            cookies = {c["name"]: c["value"] for c in driver.get_cookies()}
            if "aws-waf-token" in cookies:
                console.print(f"[dim]→ got waf token after {i+1}s[/dim]")
                return cookies["aws-waf-token"]
        console.print("[yellow]⚠ waf token not found after 25s[/yellow]")
        return None
    finally:
        driver.quit()


def fetch_nsf_csv(output_path=None, cookie=None):
    console.print(Panel(f"[bold]target:[/bold] {CSV_URL}", title="[bold cyan]nsf csv fetcher[/bold cyan]", border_style="cyan"))

    # get waf token
    token = cookie
    if token:
        console.print("[dim]→ using provided waf token[/dim]")
    else:
        token = get_waf_token()

    if not token:
        console.print("[red]✗ failed to get waf token — use --cookie flag[/red]")
        return None

    # fetch csv with token
    session = creq.Session()
    session.cookies.set("aws-waf-token", token, domain=".nsf.gov")

    with console.status("[bold blue]fetching csv..."):
        headers = {**HEADERS, "referer": "https://www.nsf.gov/funding/opportunities?page=1"}
        resp = session.get(CSV_URL, headers=headers, impersonate=IMPERSONATE, timeout=60)
        resp.raise_for_status()

    content = resp.text
    if content.strip().startswith("<!DOCTYPE") or content.strip().startswith("<html"):
        console.print("[red]✗ waf blocked — token may have expired[/red]")
        return None

    fname = output_path or f"nsf_opps_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(content)

    n = max(len(content.strip().splitlines()) - 1, 0)
    console.print(f"\n[bold green]✓ saved {n} records → {fname}[/bold green]")
    return fname


def main():
    parser = argparse.ArgumentParser(description="fetch nsf funding opportunities csv")
    parser.add_argument("-o", "--output", type=str, default=None, help="output filename")
    parser.add_argument("--cookie", type=str, default=None, help="manually provide aws-waf-token")
    args = parser.parse_args()

    fetch_nsf_csv(args.output, args.cookie)


if __name__ == "__main__":
    main()
