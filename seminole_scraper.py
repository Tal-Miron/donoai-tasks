import json
import logging
import os
import asyncio
from playwright.async_api import async_playwright
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import TimeoutError as PlaywrightTimeout
import time

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

URL = "https://recording.seminoleclerk.org/DuProcessWebInquiry/index.html"
DATE_FORMAT = "%m/%d/%Y, %I:%M:%S %p"
OUTPUT_PATH = "outputs/seminole_results.json"
NAME_MIN_LENGTH = 2
NAME_MAX_LENGTH = 50
PAGER_NO_CONTENT_LABEL = "0 - 0 of 0"
REQUEST_DELAY_SECONDS = 0.5

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_names(value: str | None) -> list[str]:
    """Split a delimited name string into a list of uppercase names.

    Not sure, but assuming the site separates names with ' ,'
    Returns an empty list if value is None or empty
    """
    if not value:
        return []
    return [name.strip().upper() for name in value.split(" ,") if name.strip()]

def parse_date(raw: str) -> str | None:
    """Parse a date string from the grid into ISO 8601 format.
    Returns None if the string cannot be parsed.
    """
    try:
        return datetime.strptime(raw, DATE_FORMAT).isoformat()
    except ValueError:
        logging.warning(f"Could not parse date: '{raw}'")
        return None

def parse_row(row) -> dict | None:
    """Parse a single grid row into a record dict.
    Returns None if the row is invalid or empty
    Runs as a sync function so it can be dispatched to a thread pool.
    """
    logging.debug("async - parsing row")
    
    #Just for testing:
    #time.sleep(1)
    
    cells = row.find_all("td", {"role": "gridcell"})
    if not cells:
        return None

    # Map aria-describedby attribute (column ID) → cell text
    data = {
        cell.get("aria-describedby"): cell.text.strip()
        for cell in cells
    }

    if not data.get("grid_inst_num"):
        return None

    return {
        "instrument_number":  data.get("grid_inst_num"),
        "book":               data.get("grid_book_reel"),
        "page":               data.get("grid_page"),
        "doc_type":           data.get("grid_instrument_type"),
        "date":               parse_date(data.get("grid_file_date", "")),
        "grantors":           parse_names(data.get("grid_party_name")),
        "grantees":           parse_names(data.get("grid_cross_party_name")),
        # Not available in the search results grid
        "parcel_number":      None,
        "county":             "seminole",
        "state":              "FL",
        "doc_category":       None,
        "original_doc_type":  None,
        "book_type":          None,
        "consideration":      None,
    }

async def parse_table(html: str) -> list[dict]:
    """Extract records from the search results grid HTML.
    Parses all rows concurrently using thread pool.
    """
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr", {"role": "row"})

    # parse every row in its own thread
    results = await asyncio.gather(*[
        asyncio.to_thread(parse_row, row) for row in rows
    ])

    parsed = [r for r in results if r is not None]
    logging.debug(f"async - Parsed {len(parsed)} records from page")
    return parsed

# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

async def scrape(name: str):
    """Searches the Seminole County recording system and return all matching records.
    gets name to search for- Validated & Uppercased
    Returns a list of record dicts. 
    Returns an empty list on error or no result.
    """
    result = []

    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(headless=True)#, slow_mo=100)
            page = await browser.new_page()
            page.set_default_timeout(120000)  # 2 minutes

            #Navigate
            try:
                await page.goto(URL)
                await page.wait_for_load_state("networkidle")
                logging.info("Page loaded")
            except PlaywrightTimeout:
                logging.error("Timed out waiting for page to load")
                return []

            #Accept disclaimer
            await page.get_by_text("Agreed & Enter").click()
            logging.info("Accepted disclaimer")

            #Search
            textbox = page.get_by_role("textbox", name="Name (lastname, firstname)")
            await textbox.wait_for(state="visible")
            await textbox.fill(name)
            #await page.get_by_role("textbox", name="Name (lastname, firstname)").fill(name)
            await page.get_by_text("Search").nth(1).click()
            logging.info(f"Starting to search for: '{name}'")

            #Wait for table to load
            try:
                logging.info("Waiting for table to load")
                # wait for spinner to appear (search has started)
                await page.wait_for_selector("img[src*='loading_small']", state="visible")
                # wait for spinner to disappear (results are ready)
                await page.wait_for_selector("img[src*='loading_small']", state="hidden")
            except PlaywrightTimeout:
                logging.error("Timed out waiting for results table to load")
                return []
            
            #Check for empty results
            try:
                logging.info("Waitig for page label to load")
                pager_label = page.locator("#grid_pager_label")
                await pager_label.wait_for(state="visible")
                pager_label_text = await pager_label.inner_text()
            except PlaywrightTimeout:
                logging.error("Timed out waiting for results table to load")
                return []
            
            if PAGER_NO_CONTENT_LABEL in pager_label_text:
                logging.info(f"No results found for '{name}'")
                return []

            await table_size_to_max(page)
            
            #Paginate and collect
            # Fire off parse tasks immediately
            # navigate to the next page straight away
            # Collect all results at the end

            tasks = []
            page_num = 1
            has_next_page = True
            
            while has_next_page:
                logging.info(f"Scraping page {page_num}")
                html = await page.content()
                tasks.append(asyncio.create_task(parse_table(html)))
                next_span = page.locator(
                    ".ui-iggrid-nextpagelabel, .ui-iggrid-nextpagelabeldisabled"
                )

                has_next_page = "ui-iggrid-nextpagelabeldisabled" not in await next_span.get_attribute("class")

                if has_next_page:
                    await navigate_next(page)
                    page_num += 1
            
            # When all pages navigated collect all parse task results
            for parsed in await asyncio.gather(*tasks):
                result.extend(parsed)
                logging.debug(f"In Total - Parsed {len(result)} records from current page")                

        except Exception:
            logging.exception("Unexpected error during scrape")
            return []

        finally:
            if browser:
                await browser.close()

    logging.info(f"Total records found: {len(result)}")
    return result

# ---------------------------------------------------------------------------
# Navigation helper
# ---------------------------------------------------------------------------

async def table_size_to_max(page):
    #Set page size to maximum if needed (more than 30 results)

    old_label = await page.locator("#grid_pager_label").inner_text()
    total = int(old_label.split("of")[1].strip().split()[0])
    if(total < 31):
        logging.info("No need to expand table")
        return

    await page.locator("#grid_editor_dropDownButton").click()
    option = page.get_by_role("option", name="60")
    await option.wait_for(state="visible")
    await option.click()
    
    # wait until pager label changes, confirms table has rerendered
    await page.wait_for_function(
        f"document.querySelector('#grid_pager_label').innerText !== '{old_label}'"
    )

    logging.info("Expanded table for optimized search")

async def navigate_next(page):
    """Clicks to the next table page and waits for the grid to re-render."""
    await asyncio.sleep(REQUEST_DELAY_SECONDS)  # rate limit
    old_label = await page.locator("#grid_pager_label").inner_text()

    await page.get_by_text("Next", exact=True).click()
    await page.wait_for_selector("td[role='gridcell']")
    
    # wait until pager label changes, confirms table has rerendered
    await page.wait_for_function(
        f"document.querySelector('#grid_pager_label').innerText !== '{old_label}'"
    )
    logging.info("Cliked on next page")

# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------

def validate_name(value: str) -> str:
    """Validate and normalise inputted name.
    Raises ValueError with a descriptive message on invalid input.
    Returns the name stripped and uppercased.
    """
    value = value.strip()

    if not value:
        raise ValueError("Name cannot be empty")
    if len(value) < NAME_MIN_LENGTH:
        raise ValueError(f"Name must be at least {NAME_MIN_LENGTH} characters long")
    if len(value) > NAME_MAX_LENGTH:
        raise ValueError(f"Name is too long (max {NAME_MAX_LENGTH} characters)")

    return value.upper()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")

    try:
        raw_name = input("Enter name to search: ")
        name = validate_name(raw_name)
    except ValueError as e:
        raise SystemExit(f"Invalid input: {e}")

    print(f"Searching for: {name}")
    start = time.perf_counter()
    result = await scrape(name)
    elapsed = round(time.perf_counter() - start, 2)
    print(f"Done - {len(result)} record(s) in {elapsed}s written to {OUTPUT_PATH}")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Done - {len(result)} record(s) written to {OUTPUT_PATH}")


if __name__ == "__main__":
    asyncio.run(main())