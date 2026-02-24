# Task 2: Seminole County, FL  Official Records Scraper

A scraper for the Seminole County Official Records system that searches by name and returns all matching property records as structured JSON, normalized to match the NC records data format.


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

## Assumptions ##

- in the table grantors & grantees separate names with  ' ,' 
- 



## Edge Cases

- No results:  Pager label is checked for `"0 - 0 of 0"` before any parsing. Returns empty list immediately. 
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

| Name | Type | Records Found | Pages | Time |
|------|------|--------------|-------|------|
| `<!-- NAME -->` | Common | `<!-- COUNT -->` | `<!-- PAGES -->` | `<!-- TIME -->s` |
| `<!-- NAME -->` | Medium | `<!-- COUNT -->` | `<!-- PAGES -->` | `<!-- TIME -->s` |
| `<!-- NAME -->` | Rare | `<!-- COUNT -->` | 1 | `<!-- TIME -->s` |

---

## Estimated Performance

`<!-- e.g. ~300 records/minute, based on NAME search (COUNT records in TIMEs) -->`