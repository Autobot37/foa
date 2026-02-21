import re
import csv
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup, Tag
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

# -----------------------------
# CONFIG (DEFAULTS)
# -----------------------------
INPUT_DIR = Path("html_out")
OUT_DIR = Path("out")
JSON_DIR = OUT_DIR / "json"

# -----------------------------
# RULE-BASED ONTOLOGY TAGS
# -----------------------------
ONTOLOGY_KEYWORDS = {
    "research_domains": {
        "Molecular Biology": [r"\bmolecular biology\b", r"\bbiomolecules?\b"],
        "Cellular Biology": [r"\bcellular\b", r"\bcells?\b", r"\bcellular biosciences?\b"],
        "Biophysics": [r"\bbiophysics?\b", r"\bmolecular biophysics\b"],
        "Genetics": [r"\bgenetics?\b", r"\bgenetic\b"],
        "Epigenetics": [r"\bepigenetic[s]?\b"],
        "Synthetic Biology": [r"\bsynthetic biology\b"],
        "Systems Biology": [r"\bsystems biology\b"],
        "Biotechnology": [r"\bbiotechnology\b"],
        "Mathematics": [r"\bmathematics?\b", r"\bmathematical\b"],
        "Computer Science": [r"\bcomputer science\b", r"\bcomputing\b", r"\bopen[- ]source\b", r"\bsoftware\b"],
        "Engineering": [r"\bengineering\b"],
        "Physics": [r"\bphysics?\b", r"\bphysical sciences?\b"],
        "Chemistry": [r"\bchemistry\b", r"\bchemical\b"],
        "Quantum": [r"\bquantum\b"],
        "Nanotechnology": [r"\bnanotechnology\b", r"\bnano\b"],
        "Cybersecurity": [r"\bcybersecurity\b", r"\bsecurity\b", r"\bsecure\b"],
        "AI": [r"\bartificial intelligence\b", r"\bai\b"],
        "Machine Learning": [r"\bmachine learning\b", r"\bdeep learning\b"],
    },
    "methods_approaches": {
        "Experimental": [r"\bexperimental\b", r"\bexperiments?\b"],
        "Computational": [r"\bcomputational\b", r"\bcomputation\b"],
        "Theoretical": [r"\btheoretical\b", r"\btheory\b"],
        "Modeling": [r"\bmodeling\b", r"\bmodelling\b", r"\bmodels?\b"],
        "Mechanistic": [r"\bmechanistic\b"],
        "Quantitative": [r"\bquantitative\b"],
        "Predictive": [r"\bpredictive\b"],
        "Integrative": [r"\bintegrative\b"],
        "Data Science": [r"\bdata science\b"],
        "Machine Learning": [r"\bmachine learning\b", r"\bdeep learning\b"],
    },
    "populations": {
        "Underrepresented groups": [r"\bunderrepresented\b", r"\bbroaden(ing)? participation\b"],
        "EPSCoR": [r"\bepscor\b"],
        "STEM workforce": [r"\bstem workforce\b"],
        "K-12": [r"\bk-?12\b"],
        "Postdoctoral": [r"\bpostdoc(s)?\b", r"\bpostdoctoral\b"],
        "Graduate students": [r"\bgraduate students?\b"],
        "Undergraduates": [r"\bundergraduates?\b", r"\bundergraduate\b"],
        "Early-career": [r"\bearly[- ]career\b"],
        "Mid-career": [r"\bmid[- ]career\b"],
    },
    "sponsor_themes": {
        "Broadening Participation": [r"\bbroadening participation\b"],
        "Workforce Development": [r"\bworkforce\b", r"\btraining\b", r"\bmentoring\b", r"\beducation\b"],
        "Infrastructure": [r"\binfrastructure\b"],
        "Interdisciplinary Research": [r"\binterdisciplinary\b", r"\bcross[- ]disciplinary\b"],
        "Basic Research": [r"\bbasic research\b", r"\bfoundational research\b"],
    }
}

# -----------------------------
# HELPERS
# -----------------------------
MONTH_RE = r"(January|February|March|April|May|June|July|August|September|October|November|December)"

def clean_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def safe_text(tag: Tag) -> str:
    if not tag:
        return ""
    return clean_text(tag.get_text(" ", strip=True))

def page_text(soup: BeautifulSoup) -> str:
    root = soup.find("main") or soup.body or soup
    txt = clean_text(root.get_text(" ", strip=True))
    txt = re.sub(r"\bSkip to main content\b", " ", txt, flags=re.I)
    txt = re.sub(r"\bNational Science Foundation\b\s+Search", " ", txt, flags=re.I)
    return clean_text(txt)

def normalize_iso_date_from_text(text: str):
    if not text:
        return None
    m = re.search(rf"\b{MONTH_RE}\s+(\d{{1,2}}),\s*(\d{{4}})\b", text, re.I)
    if not m:
        return None
    month_name = m.group(1).lower()
    day = int(m.group(2))
    year = int(m.group(3))
    month_map = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12
    }
    month = month_map[month_name]
    return f"{year:04d}-{month:02d}-{day:02d}"

def get_canonical_url(soup: BeautifulSoup) -> str:
    link = soup.find("link", rel="canonical")
    if link and link.get("href"):
        return link["href"].strip()
    og = soup.find("meta", attrs={"property": "og:url"})
    if og and og.get("content"):
        return og["content"].strip()
    return ""

def extract_title_and_foa_id(soup: BeautifulSoup, file_stem: str):
    h1 = soup.find("h1", class_=re.compile(r"solicitation__title"))
    full_title = safe_text(h1)

    if not full_title:
        title_tag = soup.find("title")
        full_title = safe_text(title_tag)

    foa_id = ""
    title = full_title or ""

    m = re.search(r"\bNSF\s*\d{2}-\d+\b", title, re.I)
    if m:
        foa_id = re.sub(r"\s+", " ", m.group(0)).strip()

    title = re.sub(r"\|\s*NSF.*$", "", title, flags=re.I).strip()

    if foa_id:
        title = re.sub(rf"^{re.escape(foa_id)}\s*:\s*", "", title, flags=re.I).strip()

    if not foa_id:
        m2 = re.search(r"nsf(\d{2})(\d{3,})", file_stem, re.I)
        if m2:
            foa_id = f"NSF {m2.group(1)}-{m2.group(2)}"

    if not title:
        title = file_stem.replace("__", " ")

    return foa_id, title

def extract_agency(soup: BeautifulSoup) -> str:
    txt = page_text(soup)
    if "National Science Foundation" in txt or "NSF - U.S. National Science Foundation" in txt:
        return "National Science Foundation (NSF)"
    return "Unknown"

def extract_posted_date_as_open_date(soup: BeautifulSoup):
    # DOM path
    for span in soup.find_all("span", class_=re.compile(r"document-info_label")):
        if re.fullmatch(r"Posted:\s*", safe_text(span), re.I):
            li = span.find_parent("li")
            if li:
                raw = re.sub(r"^Posted:\s*", "", safe_text(li), flags=re.I).strip()
                return normalize_iso_date_from_text(raw), raw

    # Text fallback
    txt = page_text(soup)
    m = re.search(rf"\bPosted\s*:\s*{MONTH_RE}\s+\d{{1,2}},\s+\d{{4}}\b", txt, re.I)
    if m:
        raw = re.sub(r"^Posted\s*:\s*", "", m.group(0), flags=re.I).strip()
        return normalize_iso_date_from_text(raw), raw
    return None, ""

# -----------------------------
# SECTION SLICER (TEXT-BASED)
# -----------------------------
def slice_between_markers(txt: str, start_patterns, end_patterns):
    """
    Returns text after first matching start marker until first matching end marker.
    """
    if not txt:
        return ""

    start_idx = None
    start_end = None

    for sp in start_patterns:
        m = re.search(sp, txt, flags=re.I)
        if m:
            curr_start = m.start()
            if start_idx is None or curr_start < start_idx:
                start_idx = curr_start
                start_end = m.end()

    if start_idx is None:
        return ""

    tail = txt[start_end:]

    end_pos = len(tail)
    for ep in end_patterns:
        m = re.search(ep, tail, flags=re.I)
        if m and m.start() < end_pos:
            end_pos = m.start()

    return clean_text(tail[:end_pos])

# -----------------------------
# REQUESTED CHANGES
# 1) DUE DATES:
#    - handle explicit date and "Proposals Accepted Anytime"
#    - if accepted anytime => no due date (close_date=None), don't keep 5pm text
# 2) DESCRIPTION:
#    - ONLY under "II. Program Description" (not introduction etc)
# 3) AWARDS:
#    - ONLY under "III. Award Information"
#    - extract dollar-starting amount/range only
# -----------------------------
def extract_due_dates(soup: BeautifulSoup):
    """
    Returns (dates_raw, close_date)
    dates_raw will be:
      - "May 14, 2026" if found
      - "Proposals Accepted Anytime" if found
      - "" if not found
    close_date:
      - ISO date if explicit due date found
      - None if Proposals Accepted Anytime / not found
    """
    txt = page_text(soup)

    # Search around Full Proposal Deadline label occurrences
    label_iter = list(re.finditer(r"Full Proposal Deadline(?:\(s\))?", txt, flags=re.I))

    for m in label_iter:
        window = txt[m.start(): m.start() + 500]  # local window only to avoid page-wide contamination

        if re.search(r"Proposals Accepted Anytime", window, re.I):
            return "Proposals Accepted Anytime", None

        d = re.search(rf"\b{MONTH_RE}\s+\d{{1,2}},\s+\d{{4}}\b", window, re.I)
        if d:
            raw_date = d.group(0)
            return raw_date, normalize_iso_date_from_text(raw_date)

    # Fallback: if page mentions proposals accepted anytime anywhere (and no labeled date found)
    if re.search(r"\bProposals Accepted Anytime\b", txt, re.I):
        return "Proposals Accepted Anytime", None

    return "", None

def extract_program_description(soup: BeautifulSoup) -> str:
    """
    ONLY extract from II. Program Description to III. Award Information.
    """
    txt = page_text(soup)

    desc = slice_between_markers(
        txt,
        start_patterns=[
            r"\bII\.\s*Program Description\b\s*:?",
        ],
        end_patterns=[
            r"\bOverall Approach\b",
            r"\bIII\.\s*Award Information\b",
        ],
    )

    if not desc:
        return ""

    # Remove accidental leading date if present
    desc = re.sub(rf"^{MONTH_RE}\s+\d{{1,2}},\s+\d{{4}}\s*", "", desc, flags=re.I)
    desc = clean_text(desc)

    # Minimal safety filter
    if len(desc) < 40:
        return ""

    return desc

def extract_award_data(soup: BeautifulSoup):
    """
    Extracts total_award and award_range from Section III.
    Returns: (total_award_str, award_range_str)
    """
    txt = page_text(soup)

    # 1. Isolate Section III: Award Information
    award_section = slice_between_markers(
        txt,
        start_patterns=[r"\bIII\.\s*Award Information\b"],
        end_patterns=[
            r"\bIV\.\s*Eligibility Information\b",
            r"\bIV\.\s*Eligibility\b",
            r"\bV\.\s*Proposal Preparation\b"
        ]
    )

    if not award_section:
        return None, None

    # 2. Regex for money tokens: catches $1,000, $15M, $14,000,000, etc.
    money_regex = r"\$\s*\d[\d,]*(?:\.\d+)?(?:\s*(?:[KMB]|million|billion))?"

    # Find all individual money instances in the text
    all_amounts_raw = re.findall(money_regex, award_section, flags=re.I)

    if not all_amounts_raw:
        return None, None

    # Convert strings to numbers for comparison to find min/max
    def to_num(s):
        s = s.replace('$', '').replace(',', '').lower().strip()
        multiplier = 1
        if 'm' in s or 'million' in s: multiplier = 1_000_000
        elif 'b' in s or 'billion' in s: multiplier = 1_000_000_000
        elif 'k' in s: multiplier = 1_000
        # Clean non-numeric characters for float conversion
        val = re.sub(r'[^\d.]', '', s)
        try:
            return float(val) * multiplier if val else 0
        except ValueError:
            return 0

    # Create a list of tuples (original_string, numeric_value)
    parsed_amounts = [(amt, to_num(amt)) for amt in all_amounts_raw]
    parsed_amounts = [p for p in parsed_amounts if p[1] > 0]

    if not parsed_amounts:
        return None, None

    # 3. Logic to separate Total from Range
    # The largest number in the section is almost always the "Anticipated Funding Amount" (Total)
    parsed_amounts.sort(key=lambda x: x[1])

    total_award = parsed_amounts[-1][0] # The biggest one
    other_amounts = parsed_amounts[:-1] # Everything else

    award_range = None
    if len(other_amounts) >= 2:
        # If we have at least two smaller numbers, we have a min and a max for a range
        award_range = f"{other_amounts[0][0]} - {other_amounts[-1][0]}"
    elif len(other_amounts) == 1:
        # If only one smaller number is found, it's a single value award
        award_range = other_amounts[0][0]

    return clean_text(total_award), award_range

def extract_eligibility(soup: BeautifulSoup) -> str:
    txt = page_text(soup)

    # Try "IV. Eligibility Information" section first
    sec = slice_between_markers(
        txt,
        start_patterns=[
            r"\bIV\.\s*Eligibility Information\b\s*:?",
            r"\bIV\.\s*Eligibility\b\s*:?",
        ],
        end_patterns=[
            r"\bV\.\s*Proposal Preparation and Submission Instructions\b",
            r"\bV\.\s*Proposal Preparation\b",
            r"\bVI\.\s*NSF Proposal Processing\b",
            r"\bVI\.\s*Proposal Review Information\b",
        ],
    )
    if sec:
        # If "Who May Submit Proposals:" exists inside, prefer content after that label
        m = re.search(
            r"Who May Submit Proposals\s*:?\s*(.+)",
            sec,
            re.I,
        )
        if m:
            cand = clean_text(m.group(1))
            if len(cand) > 30:
                return cand
        if len(sec) > 30:
            return sec

    # Fallback global label extraction
    m = re.search(
        r"Who May Submit Proposals\s*:?\s*(.+?)(?:Proposal Preparation and Submission Instructions|C\.\s*Due Dates|Merit Review Criteria|Award Administration Information)",
        txt,
        re.I,
    )
    if m:
        cand = clean_text(m.group(1))
        if len(cand) > 30:
            return cand

    return ""

# -----------------------------
# TAGGING
# -----------------------------
def apply_semantic_tagging(text: str):
    tags = {
        "research_domains": [],
        "methods_approaches": [],
        "populations": [],
        "sponsor_themes": [],
    }
    text = text or ""

    for category, label_map in ONTOLOGY_KEYWORDS.items():
        for label, patterns in label_map.items():
            for pat in patterns:
                if re.search(pat, text, re.I):
                    tags[category].append(label)
                    break
    return tags

# -----------------------------
# PARSER
# -----------------------------
def parse_nsf_html(html_path: Path):
    with open(html_path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "html.parser")

    foa_id, title = extract_title_and_foa_id(soup, html_path.stem)
    agency = extract_agency(soup)
    source_url = get_canonical_url(soup)

    posted_iso, _posted_raw = extract_posted_date_as_open_date(soup)
    dates_raw, close_date = extract_due_dates(soup)
    open_date = posted_iso

    eligibility = extract_eligibility(soup)
    description = extract_program_description(soup)
    total_award, award_range = extract_award_data(soup)

    tag_text = " ".join([title, description, eligibility, award_range or "", total_award or "", dates_raw])
    semantic_tags = apply_semantic_tagging(tag_text)

    return {
        "foa_id": foa_id or "",
        "title": title or "",
        "agency": agency or "",
        "open_date": open_date,            # posted date (best effort)
        "close_date": close_date,          # due date only; None if Proposals Accepted Anytime
        "dates_raw": dates_raw or "",      # e.g. "May 14, 2026" or "Proposals Accepted Anytime"
        "eligibility_text": eligibility or "",
        "program_description": description or "",
        "total_award": total_award or "",
        "award_range": award_range or "",
        "source_url": source_url or "",
        "semantic_tags": semantic_tags,
        "parser_source": "nsf_html_local",
    }

# -----------------------------
# CSV FLATTEN
# -----------------------------
def flatten_for_csv(record: dict):
    tags = record.get("semantic_tags", {})
    return {
        "foa_id": record.get("foa_id", ""),
        "title": record.get("title", ""),
        "agency": record.get("agency", ""),
        "open_date": record.get("open_date", "") or "",
        "close_date": record.get("close_date", "") or "",
        "dates_raw": record.get("dates_raw", ""),
        "eligibility_text": record.get("eligibility_text", ""),
        "program_description": record.get("program_description", ""),
        "total_award": record.get("total_award", ""),
        "award_range": record.get("award_range", ""),
        "source_url": record.get("source_url", ""),
        "tags_research_domains": "; ".join(tags.get("research_domains", [])),
        "tags_methods_approaches": "; ".join(tags.get("methods_approaches", [])),
        "tags_populations": "; ".join(tags.get("populations", [])),
        "tags_sponsor_themes": "; ".join(tags.get("sponsor_themes", [])),
    }

# -----------------------------
# MAIN BATCH RUN
# -----------------------------
def process_file(html_file: Path):
    """Helper to parse a single file and save its JSON."""
    try:
        rec = parse_nsf_html(html_file)
        with open(JSON_DIR / f"{html_file.stem}.json", "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        return rec
    except Exception as e:
        # We can't easily print here without breaking the progress bar, 
        # but we can return None or log it elsewhere.
        return None

def run_batch_parsing(input_dir: Path, out_dir: Path, json_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    json_dir.mkdir(parents=True, exist_ok=True)

    html_files = sorted(input_dir.glob("*.html"))
    if not html_files:
        print(f"No HTML files found in {input_dir.resolve()}")
        return []

    all_records = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task("[cyan]Parsing HTML files...", total=len(html_files))
        
        with ThreadPoolExecutor() as executor:
            # map maintains order, but we can also use as_completed
            futures = [executor.submit(process_file, f) for f in html_files]
            for future in futures:
                res = future.result()
                if res:
                    all_records.append(res)
                progress.update(task, advance=1)

    # Combined JSON
    if all_records:
        with open(out_dir / "foas.json", "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)

        # Combined CSV
        rows = [flatten_for_csv(r) for r in all_records]
        with open(out_dir / "foas.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)

    print(f"Done: {len(all_records)} files processed.")
    print(f"Combined JSON: {out_dir / 'foas.json'}")
    print(f"Combined CSV: {out_dir / 'foas.csv'}")
    return all_records


def main():
    run_batch_parsing(INPUT_DIR, OUT_DIR, JSON_DIR)

if __name__ == "__main__":
    main()