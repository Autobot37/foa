# NSF FOA Scraper & Parser

A tool to scrape and parse National Science Foundation (NSF) Funding Opportunity Announcements (FOAs).

## Installation

First, clone the repository and install the dependencies:

```powershell
git clone https://github.com/Autobot37/foa
cd foa
pip install -r requirements.txt
```

## Usage

### 1. Scrape and Parse a Single URL
Use this to process a specific NSF solicitation page.
```powershell
python main.py --url "https://www.nsf.gov/funding/opportunities/mcb-division-molecular-cellular-biosciences-core-programs/nsf24-539/solicitation"
```

### 2. Batch Scrape N URLs
This command fetches the latest CSV from NSF, extracts N solicitation URLs, scrapes the HTML, and parses them all.
```powershell
python main.py -n 100
```

## Limitations (Rule-Based Parsing)
*   **Narrative Complexity**: Information is often embedded in full sentences rather than structured fields, making it difficult to extract precise values like award ranges.
*   **Semantic Tagging**: Fields such as "Research Domain" are identified using keyword-based tagging which requires careful rule management to maintain accuracy.
