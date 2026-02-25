import json
import os
import re
import datetime
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _clean_str(value: Any) -> str:
    """Return a stripped string, or empty string if value is None/falsy."""
    return str(value or "").strip()


def _parse_date(raw: Any) -> Optional[datetime.date]:
    """Parse the first 10 characters of an ISO date string, or return None."""
    if not isinstance(raw, str) or len(raw) < 10:
        return None
    try:
        return datetime.date.fromisoformat(raw[:10])
    except ValueError:
        return None


def _get_pattern(value: str) -> str:
    """
    Convert a string into a compact pattern by replacing runs of:
      - digits       → \\d{n}
      - lowercase    → \\l{n}
      - uppercase    → \\u{n}
    Special characters are kept as-is.

    Example: "2023-0012345" → "\\d{4}-\\d{7}"
    """
    def replacer(m: re.Match) -> str:
        token = m.group()
        n = len(token)
        if token[0].isdigit():
            return f'\\d{{{n}}}'
        if token[0].islower():
            return f'\\l{{{n}}}'
        return f'\\u{{{n}}}'

    return re.sub(r'\d+|[a-z]+|[A-Z]+', replacer, value)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class InstrumentPatternInfo:
    example: str = ""
    count: int = 0


@dataclass
class BookPatternInfo:
    """Tracks page-number patterns and the numeric min/max page seen."""
    page_patterns: Counter = field(default_factory=Counter)
    min_page: float = float('inf')
    max_page: float = float('-inf')

    def add_page(self, value: str) -> None:
        if not value:
            return
        self.page_patterns[_get_pattern(value)] += 1
        if value.isdigit():
            num = int(value)
            self.min_page = min(self.min_page, num)
            self.max_page = max(self.max_page, num)


@dataclass
class DateRange:
    """Tracks earliest/latest valid dates and flags anomalies."""
    earliest: Optional[datetime.date] = None
    latest: Optional[datetime.date] = None
    anomalies: set = field(default_factory=set)

    _MIN_DATE = datetime.date(1900, 1, 1)

    def update(self, date: Optional[datetime.date]) -> None:
        if date is None:
            return
        if date < self._MIN_DATE or date > datetime.date.today():
            self.anomalies.add(str(date))
            return
        self.earliest = min(filter(None, [self.earliest, date]))
        self.latest = max(filter(None, [self.latest, date]))


@dataclass
class DocTypeTracker:
    """Counts document type occurrences."""
    counts: Counter = field(default_factory=Counter)

    def add(self, doc_type: str) -> None:
        if doc_type:
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
    instrument_patterns: defaultdict = field(default_factory=lambda: defaultdict(InstrumentPatternInfo))
    book_patterns: defaultdict = field(default_factory=lambda: defaultdict(BookPatternInfo))
    date_range: DateRange = field(default_factory=DateRange)
    doc_types: DocTypeTracker = field(default_factory=DocTypeTracker)


@dataclass
class ParsedRecord:
    county: str
    instrument_number: str
    book: str
    page: str
    date: Optional[datetime.date]
    doc_type: str

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "ParsedRecord":
        return cls(
            county=_clean_str(row.get("county")),
            instrument_number=_clean_str(row.get("instrument_number")),
            book=_clean_str(row.get("book")),
            page=_clean_str(row.get("page")),
            date=_parse_date(row.get("date")),
            doc_type=_clean_str(row.get("doc_type")),
        )


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

def stream_jsonl(filepath: str):
    """Yield parsed JSON objects one line at a time."""
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


# ---------------------------------------------------------------------------
# Processing
# ---------------------------------------------------------------------------

def process_record(record: ParsedRecord, counties: defaultdict) -> None:
    county = counties[record.county]
    county.record_count += 1

    # Instrument pattern
    inst_pat = _get_pattern(record.instrument_number)
    county.instrument_patterns[inst_pat].count += 1
    county.instrument_patterns[inst_pat].example = record.instrument_number

    # Book + page pattern
    book_pat = _get_pattern(record.book)
    county.book_patterns[book_pat].add_page(record.page)

    # Date
    county.date_range.update(record.date)

    # Doc type
    county.doc_types.add(record.doc_type)


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def _build_instrument_patterns(data: CountyData, total: int) -> list[dict]:
    patterns = [
        {
            "pattern": pat,
            "regex": f"^{pat}$",
            "example": info.example,
            "count": info.count,
            "percentage": round((info.count / total) * 100, 2) if total else 0,
        }
        for pat, info in data.instrument_patterns.items()
    ]
    return sorted(patterns, key=lambda x: x["count"], reverse=True)


def _build_book_patterns(data: CountyData) -> list[dict]:
    patterns = [
        {
            "pattern": book_pat,
            "regex": f"^{book_pat}$",
            "page_patterns": [
                {"pattern": p, "count": c}
                for p, c in bp.page_patterns.most_common()
            ],
            "page_min": bp.min_page if bp.min_page != float('inf') else None,
            "page_max": bp.max_page if bp.max_page != float('-inf') else None,
        }
        for book_pat, bp in data.book_patterns.items()
    ]
    return sorted(patterns, key=lambda x: sum(p["count"] for p in x["page_patterns"]), reverse=True)


def generate_report(counties: defaultdict) -> dict:
    report = {}
    for county, data in counties.items():
        report[county] = {
            "record_count": data.record_count,
            "instrument_patterns": _build_instrument_patterns(data, data.record_count),
            "book_patterns": _build_book_patterns(data),
            "date_range": {
                "earliest": str(data.date_range.earliest) if data.date_range.earliest else None,
                "latest": str(data.date_range.latest) if data.date_range.latest else None,
                "anomalies": sorted(data.date_range.anomalies),
            },
            "doc_type_distribution": dict(data.doc_types.top_n(10)),
            "unique_doc_types": data.doc_types.unique_count(),
        }
    return report


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    filepath = os.path.join(os.curdir, "nc_records_assessment.jsonl")
    counties: defaultdict[str, CountyData] = defaultdict(CountyData)

    for row in stream_jsonl(filepath):
        record = ParsedRecord.from_row(row)
        process_record(record, counties)

    report = generate_report(counties)

    output_path = "county_patterns.json"
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Written {output_path} with {len(report)} counties")


if __name__ == "__main__":
    main()