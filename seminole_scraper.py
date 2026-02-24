from datetime import datetime
from bs4 import BeautifulSoup
import logging
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import json

def parse_names(value: str | None) -> list[str]:
    if not value:
        return []
    return [name.strip() for name in value.split(" ,") if name.strip()]

def parse_table(page) -> list:
    records = []
    
    html = page.content()
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr", {"role": "row"})

    for row in rows:
        cells = row.find_all("td", {"role": "gridcell"})
        
        # skip empty rows
        if not cells:
            continue

        # map column name to value
        data = {}
        for cell in cells:
            column = cell.get("aria-describedby")
            value = cell.text.strip()
            data[column] = value

        # skip if missing key fields
        if not data.get("grid_inst_num"):
            continue

        # parse date
        raw_date = data.get("grid_file_date", "")
        try:
            dt = datetime.strptime(raw_date, "%m/%d/%Y, %I:%M:%S %p")
            iso_date = dt.isoformat()
        except ValueError:
            iso_date = None

        record = {
            "instrument_number": data.get("grid_inst_num", None),
            "book": data.get("grid_book_reel", None),
            "page": data.get("grid_page", None),
            "doc_type": data.get("grid_instrument_type", None),
            "date": iso_date,
            "grantors": parse_names(data.get("grid_party_name")),
            "grantees": parse_names(data.get("grid_cross_party_name")),
            # fields not in table
            "parcel_number": None,
            "county": "seminole",
            "state": "FL",
            "doc_category": None,
            "original_doc_type": None,
            "book_type": None,
            "consideration": None,
        }

        records.append(record)
        
    return records

def scrape(name) -> list:
    result = []

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False, slow_mo=500)  # the browser itself
            page = browser.new_page()      # a tab inside the browser
            page.set_default_timeout(300000) #5 minutes

            # navigate
            try:
                page.goto("https://recording.seminoleclerk.org/DuProcessWebInquiry/index.html")  # navigate
                page.wait_for_load_state("networkidle")
                logging.info("Page loaded")
            except PlaywrightTimeout:
                logging.error("Failed to load page")
                return []
            
            #enter & search
            page.click("text=Agreed & Enter")
            page.fill("#criteria_full_name", name)
            page.locator("a.btn.btn-success.w-40").filter(has_text="Search").nth(0).click()
            # wait for loading spinner to disappear
            try:
                page.wait_for_selector("img[src*='loading_small']", state="hidden")
            except PlaywrightTimeout:
                logging.error("Table Loading Timeout")
                return []

            # validate results in table
            pager_label = page.locator("#grid_pager_label").inner_text()

            if "0 - 0 of 0" in pager_label:
                logging.warning(f"No results found for '{name}'")
                return result

            page.click("#grid_editor_dropDownButton")
            page.wait_for_selector("#grid_editor_list_item_4", state="visible")
            page.click("#grid_editor_list_item_4")
            page.wait_for_timeout(1000)

        except Exception as e:
            logging.error(f"❌ Unexpected error: {e}")
            return []

        
        while True:
            # scrape current page
            result.extend(parse_table(page))
            
            # check if the SPAN has disabled class
            next_span = page.locator(".ui-iggrid-nextpagelabel, .ui-iggrid-nextpagelabeldisabled")
            next_class = next_span.get_attribute("class")
            
            if "ui-iggrid-nextpagelabeldisabled" in next_class:
                break  # last page, stop
            
            # click the outer div (not the span)
            page.click(".ui-iggrid-nextpage")
            
            # wait for grid to re-render
            page.wait_for_selector("td[role='gridcell']")
            page.wait_for_timeout(500)
    
    logging.info(f"Total records found: {len(result)}")
    return result

def validate_name(value) -> str:
    value = value.strip()

    if not value:
        raise ValueError("Name cannot be empty")

    if len(value) < 2:
        raise ValueError("Name must be at least 2 characters long")

    if len(value) > 50:
        raise ValueError("Name is too long (max 50 characters)")

    return value.upper()


def main():

    try:
            raw_name = input("Enter name to search: ")
            name = validate_name(raw_name)
            print(f"Searching for: {name}")
    except ValueError as e:
        raise SystemExit(f"Error: {e}")
    
    result = scrape(name)
    with open("outputs/seminole_results.json", "w") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__" :
    main()
