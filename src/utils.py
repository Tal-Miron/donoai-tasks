import re
import datetime
from typing import Any, Optional

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

