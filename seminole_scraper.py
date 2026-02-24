import json
import logging
import os
from datetime import datetime

from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeout
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

URL = "https://recording.seminoleclerk.org/DuProcessWebInquiry/index.html"
DATE_FORMAT = "%m/%d/%Y, %I:%M:%S %p"
OUTPUT_PATH = "outputs/seminole_results.json"
NAME_MIN_LENGTH = 2
NAME_MAX_LENGTH = 50

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
        return None


def parse_table(html: str) -> list[dict]:
    """Extract records from the search results grid HTML.
    Fields not present in the grid (parcel_number, doc_category, etc.)
    are set to None - they are only available on individual document pages,
    which this scraper does not retrieve.
    """
    records = []
    soup = BeautifulSoup(html, "html.parser")

    for row in soup.find_all("tr", {"role": "row"}):
        cells = row.find_all("td", {"role": "gridcell"})
        if not cells:
            continue

        # Map aria-describedby attribute (column ID) → cell text
        data = {
            cell.get("aria-describedby"): cell.text.strip()
            for cell in cells
        }

        if not data.get("grid_inst_num"):
            continue

        records.append({
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
        })

    return records

# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

def scrape(name: str) -> list[dict]:
    """Searches the Seminole County recording system and return all matching records.
    Args:
        name: Validated, uppercased name to search for.
    Returns:
        List of record dicts. Returns an empty list on error or no results.
    """
    result = []

    with sync_playwright() as p:
        browser = None
        try:
            browser = p.chromium.launch(headless=False, slow_mo=500)
            page = browser.new_page()
            page.set_default_timeout(300_000)  # 5 minutes

            # --- Navigate ---
            try:
                page.goto(URL)
                page.wait_for_load_state("networkidle")
                logging.info("Page loaded")
            except PlaywrightTimeout:
                logging.error("Timed out waiting for page to load")
                return []

            # --- Accept disclaimer & submit search ---
            page.click("text=Agreed & Enter")
            page.fill("#criteria_full_name", name)
            page.locator("a.btn.btn-success.w-40").filter(has_text="Search").nth(0).click()

            try:
                page.wait_for_selector("img[src*='loading_small']", state="hidden")
            except PlaywrightTimeout:
                logging.error("Timed out waiting for results table to load")
                return []

            # --- Check for empty results ---
            pager_label = page.locator("#grid_pager_label").inner_text()
            if "0 - 0 of 0" in pager_label:
                logging.info(f"No results found for '{name}'")
                return []

            # --- Set page size to maximum (list item 4 = largest option) ---
            page.click("#grid_editor_dropDownButton")
            page.wait_for_selector("#grid_editor_list_item_4", state="visible")
            page.click("#grid_editor_list_item_4")
            page.wait_for_timeout(1000)

            # --- Paginate and collect ---
            while True:
                result.extend(parse_table(page.content()))

                next_span = page.locator(
                    ".ui-iggrid-nextpagelabel, .ui-iggrid-nextpagelabeldisabled"
                )
                if "ui-iggrid-nextpagelabeldisabled" in next_span.get_attribute("class"):
                    break

                page.click(".ui-iggrid-nextpage")
                page.wait_for_selector("td[role='gridcell']")
                page.wait_for_timeout(500)

        except Exception:
            logging.exception("Unexpected error during scrape")
            return []

        finally:
            if browser:
                browser.close()

    logging.info(f"Total records found: {len(result)}")
    return result

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

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    try:
        raw_name = input("Enter name to search: ")
        name = validate_name(raw_name)
    except ValueError as e:
        raise SystemExit(f"Invalid input: {e}")

    print(f"Searching for: {name}")
    result = scrape(name)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(result, f, indent=2)

    print(f"Done - {len(result)} record(s) written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()