# Task 1: County Pattern Analysis

## How to Run

Run:
python src/pattern_analyzer.py
Output will be written to `outputs/county_patterns.json`.

### Dependencies & Requirements
**Python version:** 3.10+
All dependencies are part of the Python standard library — no `pip install` required.

## How It Works

The script streams the JSONL file line by line, never loading the entire file into memory. 
For each record, it updates running summaries per county using dataclasses. 
At the end it generates the report and writes it to JSON in a single pass.
This approach keeps memory usage suitable for very large datasets.

### Data Structures

- `InstrumentPatternInfo`: tracks pattern count and a single example per pattern
- `BookPatternInfo`: tracks page patterns (as a Counter), and numeric min/max for page numbers
- `DateRange`: tracks earliest/latest dates , flags anomalies
- `DocTypeTracker`: wraps a Counter for doc type distribution, unique count, and top 10
- `CountyData`:  aggregates all of the above per county
- `ParsedRecord`:  validates and cleans each raw JSON row before processing

### Date Anomaly Detection

Dates before `1900-01-01` or after today are flagged as anomalies and excluded from the min/max date range, since they likely represent data entry errors.

### Null Handling

All fields are cleaned through `ParsedRecord.from_row()` before processing. Empty strings and `None` values are handled consistently — no field processing is attempted on empty values.


## Assumptions

- bp prefixed instrument numbers are treated as a legitimate pattern.
- After checking, book numbers and pages may contain letters in some counties despite initial expectations, the script handles both numeric and alphanumeric formats.
- Date comparison uses the record's date truncated to `YYYY-MM-DD` (first 10 characters of the ISO string).
- A "suspicious" date is defined as anything before 1900 or after today's date.
- All uniques of doc_type and book pattern fit in memmory

## Answeres to Question - Doc catagory & type
doc_type and doc_category do relate. i have created a heat map between the two vaules.
seems like doc types that explicitly contain 'DEED' are catagorized as DEEDs (eg REL DEED).
all other doc types are have other catagories.
results and code are under outputs/analyzed data

## Additional Notes
I have runned scripts to analyze some of the data and look for any patterns:
1. I wasnt sure hoe to format the book json and what type of data the code needs ro handle (nummeric or also alphabetic).
After searching for any patterns between book numbers/page numbers i have concluded that page numbers are supposed to be numeric - yet some values contain alphabetic chars because there's a mistake in reading the page number.
some values are very high numbers for the same reason (192194 readed from '192-194)
2. there is no pattern between book number and page number



# Task 2: Seminole County, FL  Official Records Scraper

A scraper for the Seminole County Official Records system that searches by name and returns all matching property records as structured JSON, normalized to match the NC records data format.


## How to Run

### Dependencies & Requirements

**Python version:** 3.10+

Third-party packages (install required):
`playwright`
`beautifulsoup4`

**Install:**
pip install playwright beautifulsoup4
playwright install chromium
playwright install chromium`

**Run the scraper:**
python src/seminole_scraper.py

Enter a name when prompted:
Enter name to search: [INPUT]

Results are written to outputs/seminole_results.json


## How It Works

In the terminal a name is provided as an input.
The scraper uses Playwright to drive a real Chromium browser. The site is a JavaScript-heavy Infragistics grid application.
I have noticed simple network requests can access its data, yet the instructions specify to extract data from the interface.
the scraper is async to max performance, scalability and complex scenarios.

**1. Launch & Navigate**
Opens a Chromium browser and navigates to the recording portal, waiting for the page to reach a fully idle network state before proceeding. 
Since I've noticed the site tends to be slow i have set the timeout to 2 minutes.

**2. Accept Disclaimer**
The site presents a legal disclaimer on every visit, clicks "Agreed & Enter" to proceed.

**3. Submit Search**
Fills the name field with the validated, uppercased input and clicks Search. Waits for the loading spinner to disappear, confirming the results grid has fully rendered.

**4. Check for Empty Results**
Reads the pager label (e.g. `"1 - 60 of 347"`) before attempting any parsing. If it contains `"0 - 0 of 0"` the scraper returns an empty list immediately.

**5. Maximize Page Size**
Clicks the page size dropdown and selects 60 (the largest option). This minimizes the number of pagination steps required and reduces total scrape time. Confirms the change by waiting for the pager label to update before continuing.

**6. Paginate & Parse (Async)**
The scraper uses two levels of parallelism to maximize performance:

*Page level*: as soon as the HTML of a page is captured, a background parse task is fired immediately via. The scraper does not wait for it and moves straight to navigating the next page.
*Row level*: every row is dispatched to a thread pool, so all rows on a page are parsed in true parallelism (BeautifulSoup is CPU-bound, so threads are used).

After all pages have been navigated, `asyncio.gather()` collects all background parse tasks in order.

**7. Output**
Writes all collected records to `outputs/seminole_results.json` as a JSON array.

---

## Challenges Encountered

The site is very javascript based, so finding native HTML compounds wasn't an option. To solve that simply, i have used playwright's testing tool to figure out the best locator, even if the developer changes attributes like ids (as possible).

**Page size dropdown**
The results grid uses a custom Infragistics dropdown, not a native HTML `<select>`. Setting the page size required clicking the dropdown toggle, waiting for the option list to appear, and then clicking the target option. Simply clicking without waiting caused the interaction to fail silently. A further wait on the pager label changing was added to confirm the table had actually re-rendered at the new size before proceeding.

**Pagination detection**
The grid's Next Page button has no standard `disabled` attribute. Instead, the Infragistics component swaps between two CSS classes: `ui-iggrid-nextpagelabel` (active) and `ui-iggrid-nextpagelabeldisabled` (last page). The scraper reads this class after each page to determine whether to continue.

**Page re-render confirmation**
After clicking Next Page, `wait_for_selector("td[role='gridcell']")` alone was not reliable as old rows could still be in the DOM while new ones loaded. The fix was to additionally wait for the pager label text to change, which only happens once the grid has fully re-rendered with the new page's data.

---

## Assumptions

- in the table grantors & grantees separate names with  ' ,' 
- Fields not in grid: parcel_number, doc_category, original_doc_type, book_type, and consideration are not present in the search results table. They are only available on individual document detail pages. Per the task instructions, document detail pages are not retrieved, so these fields are consistently set to null. Yet county and state are reffered.



## Edge Cases

- No results:  Pager label is checked for `"0 - 0 of 0"` before any parsing. Returns empty list immediately. 
- Few rsults: Pager label is checked to be less than 30, adjusting scraping actions.
- Page load timeout: Wrapped in `try/except PlaywrightTimeout`. Logs error, returns empty list. 
- Table load timeout: separate timeout guard on the loading spinner. 
- Unparseable date: `parse_date()` catches `ValueError`, logs a warning, returns `null` for that field. 
- Invalid row: Rows missing `grid_inst_num` are skipped silently in `parse_row()`. 
- Fields not in grid: `parcel_number`, `doc_category`, `original_doc_type`, `book_type`, `consideration` are set to `null`, these are only available on individual document detail pages, which the scraper does not retrieve. 
- Unexpected errors: Top-level `except Exception` in `scrape()` logs the full traceback via `logging.exception()` and returns an empty list. 

---

## Rate Limiting

A 0.5s delay is applied before each Next Page click. This keeps the scraper respectful to the county's server without significantly impacting performance.

---

## Test Results

1. For the name 'TAL':
- 2000 records were written (0.09s for a record)
- in total: 187s  ()
- as the site was slow, until the page loaded (accept btn pressed and filled in search): 116s
- table took around 60s to load, so the extracting og the table data itself was very fast and took only 8s (0.004s for a record)

2. For the name 'Y7#7DJD8DJD':
- 0 records were written (no result)
- in total: 215s
- until the page loaded: 134s
- until table loaded: 213s

3. For the name 'TALAA,KENNETH STEPHEN JOHN'
- 2 records were written
- in total: 162s
- until page loaded: 161s


---

## Estimated Performance

Extraction speed (excluding server wait time) is approximately 15,000 records/minute (~0.004s per record), based on the TAL search which extracted 2,000 records in ~8s.
However, end-to-end throughput is bottlenecked entirely by the county server. Real-world performance including server wait time is closer to 640 records/minute (2,000 records in 187s total).
MetricValueExtraction speed (parsing only)~15,000 records/minEnd-to-end speed (inc. server wait)~640 records/minAvg. server page load time2–3 minutesRecords per page60 (max)

## Additional Notes

- The scraper runs in headed mode (headless=False) by default so browser interactions are visible. To run headlessly, change headless=False to headless=True in scrape().
- Logging is set to DEBUG level by default, printing every step to the console. To reduce noise in production, change logging.DEBUG to logging.INFO in main().