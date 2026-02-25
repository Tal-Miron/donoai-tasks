"""
doc_type Normalization Layer
=============================
Streams a JSONL file record by record, applies a full 7-step normalization
pipeline to the doc_type field, and reports deduplication results.

Steps:
  1. Case standardization       → uppercase everything
  2. Punctuation removal        → strip . - / & , ( ) # *
  3. Stop word removal          → drop OF AND THE OR FOR TO A
  4. Whitespace collapsing      → strip + collapse double spaces
  5. Alphabetical word sorting  → fix word-order inversions
  6. Abbreviation expansion     → expand known shorthand
  7. Suffix stemming            → reduce plural/verb forms to root
"""

import os
import json
import re
from collections import defaultdict


# ─────────────────────────────────────────────
# STEP 2 - Abbreviation expansion dictionary
# ─────────────────────────────────────────────
ABBREVIATIONS = {
    # Trust / Deed variants
    "D/T":      "DEED TRUST",
    "DT":      "DEED TRUST",
    "DOT":      "DEED TRUST",
    "D TR":     "DEED TRUST",
    "DTR":      "DEED TRUST",
    "WARR":     "WARRANTY",
    "WD":       "WARRANTY DEED",
    "W/D":      "WARRANTY DEED",
    # Mortgage
    "MTG":      "MORTGAGE",
    "MORG":     "MORTGAGE",
    "MORTG":    "MORTGAGE",
    # Release / Satisfaction
    "REL":      "RELEASE",
    "SAT":      "SATISFACTION",
    "SATIS":    "SATISFACTION",
    # Lien
    "LN":       "LIEN",
    # Assignment
    "ASSN":     "ASSIGNMENT",
    "ASSGN":    "ASSIGNMENT",
    "ASGN":     "ASSIGNMENT",
    # Easement
    "ESMT":     "EASEMENT",
    # Amendment
    "AMEND":    "AMENDMENT",
    "AMD":      "AMENDMENT",
    # Agreement
    "AGRMT":    "AGREEMENT",
    "AGR":      "AGREEMENT",
    # Declaration
    "DECL":     "DECLARATION",
    # Power of Attorney
    "POA":      "POWER ATTORNEY",
    "P/A":      "POWER ATTORNEY",
    # Subordination
    "SUBORD":   "SUBORDINATION",
    "SUB":      "SUBORDINATION",
    # Foreclosure
    "FORECL":   "FORECLOSURE",
    # Substitution
    "SUBST":    "SUBSTITUTION",
    # Notice
    "NTC":      "NOTICE",
    "NOT":      "NOTICE",
    # Judgment
    "JUDG":     "JUDGMENT",
    "JDG":      "JUDGMENT",
    # Certificate
    "CERT":     "CERTIFICATE",
    # Restriction / Covenant
    "RESTRIC":  "RESTRICTION",
    "COVEN":    "COVENANT",
    # Trustee
    "TR":       "TRUSTEE",
    "TRST":     "TRUSTEE",
    # Corporation / Company
    "CORP":     "CORPORATION",
    "CO":       "COMPANY",
    # North Carolina
    "NC":       "NORTH CAROLINA",
}

# ─────────────────────────────────────────────
# STEP 3 - Stop words
# ─────────────────────────────────────────────
STOP_WORDS = {"OF", "AND", "THE", "OR", "FOR", "TO", "A", "AN", "IN", "AT", "BY"}

# ─────────────────────────────────────────────
# STEP 7 - Simple rule-based stemmer
# Maps common suffixes to root forms without any library
# ─────────────────────────────────────────────
STEM_RULES = [
    # order matters — longest suffix first
    ("CATIONS",  "CATE"),
    ("CATION",   "CATE"),
    ("ATIONS",   "ATE"),
    ("ATION",    "ATE"),
    ("MENTS",    "MENT"),  # keep MENT (AGREEMENT → AGREEMENT)
    ("NESSES",   ""),
    ("NESSES",   ""),
    ("INGS",     ""),
    ("ING",      ""),
    ("ANCES",    "ANCE"),
    ("ENCES",    "ENCE"),
    ("ITIES",    "ITY"),
    ("TIONS",    "TION"),
    ("SIONS",    "SION"),
    ("URES",     "URE"),
    ("URES",     "URE"),
    ("ERS",      "ER"),
    ("IES",      "Y"),
    ("VES",      "VE"),  # RELEASES → ... handled below
    ("SES",      "SE"),
    ("ES",       "E"),
]

# Words that should NOT be stemmed (stem would mangle them)
STEM_PROTECT = {
    "DEED", "TRUST", "LIEN", "NOTICE", "LEASE", "RELEASE",
    "TRUSTEE", "MORTGAGE", "CERTIFICATE", "AGREEMENT",
    "JUDGMENT", "ASSIGNMENT", "EASEMENT", "AMENDMENT",
    "DECLARATION", "SUBORDINATION", "FORECLOSURE",
    "SATISFACTION", "WARRANTY", "RESTRICTION", "COVENANT",
    "POWER", "ATTORNEY", "MODIFICATION", "AFFIDAVIT",
}


def stem_word(word: str) -> str:
    """Apply simple suffix-stripping to reduce a word to its root."""
    if word in STEM_PROTECT or len(word) <= 4:
        return word
    for suffix, replacement in STEM_RULES:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[: -len(suffix)] + replacement
    # Plain plural: DEEDS → DEED, LIENS → LIEN
    if word.endswith("S") and not word.endswith("SS") and len(word) > 4:
        return word[:-1]
    return word


# ─────────────────────────────────────────────
# THE NORMALIZATION PIPELINE
# ─────────────────────────────────────────────

def normalize(raw: str) -> str:
    if not raw:
        return ""

    # STEP 1 - Uppercase
    s = raw.upper()

    # STEP 2 - Expand multi-word abbreviations BEFORE tokenizing
    for abbr, expansion in ABBREVIATIONS.items():
        # Match whole-word abbreviation
        s = re.sub(rf"(?<!\w){re.escape(abbr)}(?!\w)", expansion, s)

    # STEP 3 - Remove/replace punctuation and special characters
    s = re.sub(r"[.\-/&,()#*'\"\\]", " ", s)

    # STEP 4 (first pass)  Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()

    # Tokenize
    words = s.split()

    # STEP 6 (token-level) — Expand single-token abbreviations
    words = [ABBREVIATIONS.get(w, w) for w in words]
    # Re-tokenize in case expansion introduced spaces
    words = " ".join(words).split()

    # STEP 3 — Remove stop words
    words = [w for w in words if w not in STOP_WORDS]

    # STEP 7 — Stemming
    words = [stem_word(w) for w in words]

    # Remove any empty strings left after stemming
    words = [w for w in words if w]

    # STEP 5 — Alphabetical sort (fixes inversions)
    words = sorted(words)

    # STEP 4 (final) — Rejoin and collapse
    result = " ".join(words).strip()

    return result


# ─────────────────────────────────────────────
# STREAMING PROCESSOR
# ─────────────────────────────────────────────

def stream_and_normalize(filepath: str):
    """
    Streams the JSONL file line by line, normalizes each doc_type,
    and collects mapping statistics.
    """
    raw_counts:        Counter_type = defaultdict(int)   # raw  → count
    normalized_counts: Counter_type = defaultdict(int)   # norm → count
    raw_to_norm:       dict         = {}                 # raw  → norm (first seen)
    norm_to_raws:      dict         = defaultdict(set)   # norm → set of raws

    total_records = 0
    missing = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            total_records += 1
            raw = record.get("doc_type", "")

            if not raw:
                missing += 1
                continue

            norm = normalize(raw)

            raw_counts[raw] += 1
            normalized_counts[norm] += 1
            raw_to_norm[raw] = norm
            norm_to_raws[norm].add(raw)

    return {
        "total_records":     total_records,
        "missing":           missing,
        "unique_raw":        len(raw_counts),
        "unique_normalized": len(normalized_counts),
        "raw_counts":        dict(raw_counts),
        "normalized_counts": dict(normalized_counts),
        "raw_to_norm":       raw_to_norm,
        "norm_to_raws":      {k: sorted(v) for k, v in norm_to_raws.items()},
    }


def run_on_file(filepath: str):
    print("=" * 65)
    print(f"  STREAMING: {filepath}")
    print("=" * 65)

    results = stream_and_normalize(filepath)

    raw_u  = results["unique_raw"]
    norm_u = results["unique_normalized"]
    reduction = (1 - norm_u / raw_u) * 100 if raw_u else 0

    print(f"\n  Total records streamed : {results['total_records']:,}")
    print(f"  Missing doc_type       : {results['missing']:,}")
    print(f"  Unique raw values      : {raw_u:,}")
    print(f"  Unique after normalize : {norm_u:,}")
    print(f"  Deduplication gain     : {reduction:.1f}%\n")

    # Show groups where normalization collapsed multiple raws
    collapsed = {
        norm: raws
        for norm, raws in results["norm_to_raws"].items()
        if len(raws) > 1
    }
    if collapsed:
        print(f"  Collapsed groups ({len(collapsed)} normalized forms with 2+ raw variants):")
        print("  " + "-" * 55)
        for norm, raws in sorted(collapsed.items(), key=lambda x: -len(x[1])):
            print(f"\n  ✦ '{norm}'  ← {len(raws)} variants:")
            for r in raws:
                count = results["raw_counts"].get(r, 0)
                print(f"      • {r:<35} ({count:,} records)")

    # Save full mapping to JSON for reuse
    mapping_path = "doc_type_mapping.json"
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(results["raw_to_norm"], f, indent=2)
    print(f"\n  ✓ Full raw→normalized mapping saved to: {mapping_path}")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

# Fix missing type hint reference
from collections import defaultdict as Counter_type  # noqa: F811 (alias for annotation only)

if __name__ == "__main__":
    filepath = os.path.join(os.curdir, "nc_records_assessment.jsonl")
    run_on_file(filepath)