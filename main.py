import json
import os 
from collections import defaultdict, Counter, namedtuple
from dataclasses import dataclass, field
import re
from typing import Optional
import datetime
from typing import Any


@dataclass
class InstrumentPatternInfo:
    example: str = ""
    count: int = 0

@dataclass
class BookPatternInfo:
    page_patterns: Counter = field(default_factory=Counter)
    min_value: float = float('inf')
    max_value: float = float('-inf')

    def add_page(self, value: str):
        if not value:
            return
        pattern = _get_pattern(value)
        self.page_patterns[pattern] += 1

        if value.isdigit():
            num = int(value)
            self.min_value = min(self.min_value, num)
            self.max_value = max(self.max_value, num)

@dataclass
class DateRange:
    earliest: datetime.date = None
    latest: datetime.date = None
    anomalies: set[str] = field(default_factory=set)

    def update(self, new_date: Optional[datetime.date]) -> None: 
        if new_date is None:
            return
        
        # FIX: Ensure you are comparing date to date, not date to datetime
        # Also using date.today() is cleaner
        if new_date < datetime.date(1900, 1, 1) or new_date > datetime.date.today():
            self.anomalies.add(str(new_date))
            return
        
        # "No ifs" logic using min/max and filtering out None values
        self.earliest = min(filter(None, [self.earliest, new_date]))
        self.latest = max(filter(None, [self.latest, new_date]))

@dataclass
class DocTypeTracker:
    counts: Counter = field(default_factory=Counter)

    def add(self, doc_type: str | None) -> None:
        if not doc_type:
            return
        self.counts[doc_type] += 1

    def unique_count(self) -> int:
        return len(self.counts)

    def top_n(self, n: int) -> list[tuple[str, int]]:
        return self.counts.most_common(n)

    def seen_once(self) -> list[str]:
        return [k for k, v in self.counts.items() if v == 1]

@dataclass
class CountyData:
    record_count: int = 0
    patterns: defaultdict[str, InstrumentPatternInfo] = field(default_factory=lambda: defaultdict(InstrumentPatternInfo))
    book_patterns: defaultdict[str, BookPatternInfo] = field(default_factory=lambda: defaultdict(BookPatternInfo))
    date_range: DateRange = field(default_factory=DateRange)
    doc_types: DocTypeTracker = field(default_factory=DocTypeTracker)

# Top-level collection
counties: defaultdict[str, CountyData] = defaultdict(CountyData)

@dataclass
class ParsedRecord:
    county: str
    instrument_number: str
    book: str
    date: datetime.date | None
    doc_type: str
    page : str

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "ParsedRecord":
        """Factory method to clean and validate incoming dictionary data."""
        return cls(
            county=_clean_str(row.get("county")),
            instrument_number=_clean_str(row.get("instrument_number")),
            book=_clean_str(row.get("book")),
            date=_parse_date(row.get("date")),
            doc_type=_clean_str(row.get("doc_type")),
            page= _clean_str(row.get("page"))
        )


def stream_jsonl(filepath: str):
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)
    
# To Utils:
@staticmethod
def _clean_str(value: Any) -> str:
        """Ensures we always have a string, even if the input is None."""
        return str(value or "").strip()

@staticmethod
def _parse_date(raw_date: Any) -> datetime.date | None:
        """Safely parses ISO date strings or returns None if invalid/missing."""
        if not isinstance(raw_date, str) or len(raw_date) < 10:
            return None
        try:
            return datetime.date.fromisoformat(raw_date[:10])
        except ValueError:
            return None

@staticmethod
def _get_pattern(value: str) -> str:
    def replace(m):
        ch = m.group()
        if ch.isdigit():
            return f'\\d'           # or keep length: f'\\d{{{len(m.group())}}}'
        if ch.islower():
            return f'\\l'
        if ch.isupper():
            return f'\\u'
        return ch

    # Single pass — each character replaced once, no cross-contamination
    pattern = re.sub(r'\d+|[a-z]+|[A-Z]+', 
                     lambda m: f'\\d{{{len(m.group())}}}' if m.group()[0].isdigit()
                               else f'\\l{{{len(m.group())}}}' if m.group()[0].islower()
                               else f'\\u{{{len(m.group())}}}',
                     value)
    return pattern

def generate_report() -> dict:
    report = {}

    for county, data in counties.items():
        total = data.record_count  # see fix in main() below

        # --- Instrument patterns ---
        instrument_patterns = []
        for pattern, info in data.patterns.items():
            instrument_patterns.append({
                "pattern": pattern,
                "regex": f"^{pattern}$",
                "example": info.example,
                "count": info.count,
                "percentage": round((info.count / total) * 100, 2) if total else 0
            })
        # Sort descending by count so dominant patterns appear first
        instrument_patterns.sort(key=lambda x: x["count"], reverse=True)

        # --- Book patterns ---
        book_patterns = []
        for book_pat, bp_info in data.book_patterns.items():
            top_page_patterns = [
                {"pattern": pat, "count": cnt}
                for pat, cnt in bp_info.page_patterns.most_common()
            ]
            book_patterns.append({
                "pattern": book_pat,
                "regex": f"^{book_pat}$",
                "page_patterns": top_page_patterns,
                "page_min": bp_info.min_value if bp_info.min_value != float('inf') else None,
                "page_max": bp_info.max_value if bp_info.max_value != float('-inf') else None,
            })
        book_patterns.sort(key=lambda x: sum(p["count"] for p in x["page_patterns"]), reverse=True)

        # --- Date range ---
        date_range = {
            "earliest": str(data.date_range.earliest) if data.date_range.earliest else None,
            "latest": str(data.date_range.latest) if data.date_range.latest else None,
            "anomalies": sorted(data.date_range.anomalies)
        }

        # --- Doc type distribution ---
        doc_type_dist = dict(data.doc_types.counts.most_common(10))

        report[county] = {
            "record_count": total,
            "instrument_patterns": instrument_patterns,
            "book_patterns": book_patterns,
            "date_range": date_range,
            "doc_type_distribution": doc_type_dist,
            "unique_doc_types": data.doc_types.unique_count()
        }

    return report

def main():

    # Start Streaming
    for row in stream_jsonl(os.path.join(os.curdir, "nc_records_assessment.jsonl")):
        record = ParsedRecord.from_row(row)
        instrument_pattern = _get_pattern(record.instrument_number)
        book_pattern = _get_pattern(record.book)

        #create add def:
        counties[record.county].record_count += 1
        counties[record.county].patterns[instrument_pattern].count += 1
        counties[record.county].patterns[instrument_pattern].example = record.instrument_number
        
        # book pattern
        counties[record.county].book_patterns[book_pattern].add_page(record.page)
        
        # date
        counties[record.county].date_range.update(record.date)
        
        # doc type
        counties[record.county].doc_types.add(record.doc_type)

    report = generate_report()
    with open("county_patterns.json", "w") as f:
        json.dump(report, f, indent=2)
    print(f"Written county_patterns.json with {len(report)} counties")


        




if __name__ == "__main__" :
    main()
