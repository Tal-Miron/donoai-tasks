"""
doc_type LLM Classification — Gemini 1.5 Flash
================================================
1. Streams the JSONL file and builds a list of unique NORMALIZED doc_types
   (reuses the normalization pipeline from normalize_doc_types.py)
2. Sends them in batches to Gemini 1.5 Flash for classification
3. Builds a final mapping:  raw_doc_type → canonical_category
4. Streams the JSONL again and writes a new enriched JSONL with
   a `doc_type_category` field added to every record

Requirements:
    pip install google-generativeai

Usage:
    export GEMINI_API_KEY="your_key_here"
    python classify_doc_types.py
"""

import os
import json
import re
import time
from collections import defaultdict

import google.generativeai as genai

genai.configure(api_key=os.environ.get("AIzaSyB-9thReKtZscgnoQ3qQIzz0UMY55mZKv0"))
model = genai.GenerativeModel("gemini-1.5-flash")

# ─────────────────────────────────────────────────────────────
# PASTE YOUR NORMALIZATION PIPELINE HERE (from normalize_doc_types.py)
# or import it if both files are in the same directory:
#   from normalize_doc_types import normalize
# ─────────────────────────────────────────────────────────────

ABBREVIATIONS = {
    "D/T": "DEED TRUST", "DOT": "DEED TRUST", "D TR": "DEED TRUST", "DTR": "DEED TRUST",
    "WARR": "WARRANTY", "WD": "WARRANTY DEED", "W/D": "WARRANTY DEED",
    "MTG": "MORTGAGE", "MORG": "MORTGAGE", "MORTG": "MORTGAGE",
    "REL": "RELEASE", "SAT": "SATISFACTION", "SATIS": "SATISFACTION",
    "LN": "LIEN", "ASSN": "ASSIGNMENT", "ASSGN": "ASSIGNMENT", "ASGN": "ASSIGNMENT",
    "ESMT": "EASEMENT", "AMEND": "AMENDMENT", "AMD": "AMENDMENT",
    "AGRMT": "AGREEMENT", "AGR": "AGREEMENT", "DECL": "DECLARATION",
    "POA": "POWER ATTORNEY", "P/A": "POWER ATTORNEY",
    "SUBORD": "SUBORDINATION", "SUB": "SUBORDINATION",
    "FORECL": "FORECLOSURE", "NTC": "NOTICE", "NOT": "NOTICE",
    "JUDG": "JUDGMENT", "JDG": "JUDGMENT", "CERT": "CERTIFICATE",
    "TR": "TRUSTEE", "TRST": "TRUSTEE", "CORP": "CORPORATION",
}

STOP_WORDS = {"OF", "AND", "THE", "OR", "FOR", "TO", "A", "AN", "IN", "AT", "BY"}

STEM_PROTECT = {
    "DEED", "TRUST", "LIEN", "NOTICE", "LEASE", "RELEASE", "TRUSTEE",
    "MORTGAGE", "CERTIFICATE", "AGREEMENT", "JUDGMENT", "ASSIGNMENT",
    "EASEMENT", "AMENDMENT", "DECLARATION", "SUBORDINATION", "FORECLOSURE",
    "SATISFACTION", "WARRANTY", "RESTRICTION", "COVENANT", "POWER",
    "ATTORNEY", "MODIFICATION", "AFFIDAVIT",
}

STEM_RULES = [
    ("CATIONS", "CATE"), ("CATION", "CATE"), ("ATIONS", "ATE"), ("ATION", "ATE"),
    ("INGS", ""), ("ING", ""), ("ANCES", "ANCE"), ("ENCES", "ENCE"),
    ("ITIES", "ITY"), ("TIONS", "TION"), ("SIONS", "SION"),
    ("SES", "SE"), ("IES", "Y"), ("ES", "E"),
]

def stem_word(word):
    if word in STEM_PROTECT or len(word) <= 4:
        return word
    for suffix, replacement in STEM_RULES:
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            return word[:-len(suffix)] + replacement
    if word.endswith("S") and not word.endswith("SS") and len(word) > 4:
        return word[:-1]
    return word

def normalize(raw):
    if not raw:
        return ""
    s = raw.upper()
    for abbr, expansion in ABBREVIATIONS.items():
        s = re.sub(rf"(?<!\w){re.escape(abbr)}(?!\w)", expansion, s)
    s = re.sub(r"[.\-/&,()#*'\"\\]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    words = s.split()
    words = [ABBREVIATIONS.get(w, w) for w in words]
    words = " ".join(words).split()
    words = [w for w in words if w not in STOP_WORDS]
    words = [stem_word(w) for w in words]
    words = [w for w in words if w]
    words = sorted(words)
    return " ".join(words).strip()


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

INPUT_FILE  = os.path.join(os.curdir, "nc_records_assessment.jsonl")
OUTPUT_FILE = os.path.join(os.curdir, "nc_records_classified.jsonl")

CATEGORIES = [
    "SALE_DEED",
    "MORTGAGE",
    "DEED_OF_TRUST",
    "RELEASE",
    "LIEN",
    "PLAT",
    "EASEMENT",
    "LEASE",
    "MISC",
]

BATCH_SIZE  = 50     # how many normalized types to classify per API call
SLEEP_SEC   = 1.0    # pause between batches to stay within rate limits


# ─────────────────────────────────────────────────────────────
# STEP 1 — Stream JSONL → collect unique normalized doc_types
# ─────────────────────────────────────────────────────────────

def collect_unique_normalized(filepath):
    """
    Returns:
        raw_to_norm  : dict  { raw_value → normalized_value }
        unique_norms : list  of unique normalized strings
    """
    raw_to_norm = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            raw = record.get("doc_type", "")
            if raw and raw not in raw_to_norm:
                raw_to_norm[raw] = normalize(raw)

    unique_norms = sorted(set(raw_to_norm.values()))
    return raw_to_norm, unique_norms


# ─────────────────────────────────────────────────────────────
# STEP 2 — Call Gemini to classify a batch of normalized types
# ─────────────────────────────────────────────────────────────

def build_prompt(batch):
    categories_str = ", ".join(CATEGORIES)
    example = json.dumps({batch[0]: "DEED_OF_TRUST"}) if batch else '{"DEED TRUST": "DEED_OF_TRUST"}'
    return f"""You are a real estate data expert.

Map the following document types to these EXACT categories:
{categories_str}

Rules:
- Use MISC for anything that does not clearly fit another category.
- Return ONLY a valid JSON object. No explanation, no markdown, no code fences.
- Every key from the input list must appear in the output.

Input list:
{json.dumps(batch, indent=2)}

Example format:
{example}"""


def classify_batch(model, batch):
    """Send one batch to Gemini, return a dict {norm → category}."""
    prompt = build_prompt(batch)
    response = model.generate_content(prompt)

    # Strip markdown code fences if the model adds them anyway
    text = response.text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    try:
        result = json.loads(text)
    except json.JSONDecodeError as e:
        print(f"  ⚠ JSON parse error: {e}")
        print(f"  Raw response: {text[:300]}")
        # Fall back: mark everything in this batch as MISC
        result = {k: "MISC" for k in batch}

    # Validate every key is present and value is a known category
    validated = {}
    for key in batch:
        cat = result.get(key, "MISC").upper()
        if cat not in CATEGORIES:
            cat = "MISC"
        validated[key] = cat

    return validated


def classify_all(unique_norms):
    """
    Classify all unique normalized doc_types in batches.
    Returns dict { normalized_value → category }
    """
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "GEMINI_API_KEY environment variable not set.\n"
            "  export GEMINI_API_KEY='your_key_here'"
        )

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")

    norm_to_category = {}
    total_batches = (len(unique_norms) + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"\n  Classifying {len(unique_norms)} unique normalized types "
          f"in {total_batches} batches (size={BATCH_SIZE})...\n")

    for i in range(0, len(unique_norms), BATCH_SIZE):
        batch      = unique_norms[i: i + BATCH_SIZE]
        batch_num  = i // BATCH_SIZE + 1
        print(f"  Batch {batch_num}/{total_batches} — {len(batch)} items...", end=" ", flush=True)

        result = classify_batch(model, batch)
        norm_to_category.update(result)

        # Show a quick summary of what came back
        from collections import Counter
        counts = Counter(result.values())
        summary = "  ".join(f"{cat}:{n}" for cat, n in counts.most_common())
        print(f"✓  [{summary}]")

        if i + BATCH_SIZE < len(unique_norms):
            time.sleep(SLEEP_SEC)

    return norm_to_category


# ─────────────────────────────────────────────────────────────
# STEP 3 — Build final raw → category mapping
# ─────────────────────────────────────────────────────────────

def build_raw_to_category(raw_to_norm, norm_to_category):
    """
    Combines:
        raw  →  normalized  →  category
    into a single flat lookup:
        raw  →  category
    """
    return {
        raw: norm_to_category.get(norm, "MISC")
        for raw, norm in raw_to_norm.items()
    }


# ─────────────────────────────────────────────────────────────
# STEP 4 — Stream JSONL again, enrich each record, write output
# ─────────────────────────────────────────────────────────────

def enrich_and_write(input_path, output_path, raw_to_category):
    written = 0
    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            raw    = record.get("doc_type", "")

            # Inject the new fields
            record["doc_type_normalized"] = normalize(raw) if raw else ""
            record["doc_type_category"]   = raw_to_category.get(raw, "MISC")

            fout.write(json.dumps(record) + "\n")
            written += 1

    return written


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  doc_type Classification Pipeline — Gemini 1.5 Flash")
    print("=" * 65)

    # 1. Collect unique normalized values
    print(f"\n[1/4] Streaming '{INPUT_FILE}' to collect unique doc_types...")
    raw_to_norm, unique_norms = collect_unique_normalized(INPUT_FILE)
    print(f"      Raw unique   : {len(raw_to_norm):,}")
    print(f"      Norm unique  : {len(unique_norms):,}  "
          f"({len(raw_to_norm) - len(unique_norms):,} collapsed by normalization)")

    # 2. Classify with Gemini
    print(f"\n[2/4] Sending to Gemini 1.5 Flash for classification...")
    norm_to_category = classify_all(unique_norms)

    # 3. Build flat raw → category lookup
    print(f"\n[3/4] Building raw → category mapping...")
    raw_to_category = build_raw_to_category(raw_to_norm, norm_to_category)

    # Save mappings for auditing / reuse
    mappings = {
        "raw_to_normalized": raw_to_norm,
        "normalized_to_category": norm_to_category,
        "raw_to_category": raw_to_category,
    }
    with open("doc_type_mappings.json", "w", encoding="utf-8") as f:
        json.dump(mappings, f, indent=2)
    print(f"      Saved full mapping → doc_type_mappings.json")

    # Print category distribution
    from collections import Counter
    cat_counts = Counter(raw_to_category.values())
    print(f"\n      Category distribution (by unique raw values):")
    for cat, count in cat_counts.most_common():
        bar = "▓" * count
        print(f"        {cat:<20} {count:>5}  {bar}")

    # 4. Enrich JSONL
    print(f"\n[4/4] Writing enriched JSONL → '{OUTPUT_FILE}'...")
    written = enrich_and_write(INPUT_FILE, OUTPUT_FILE, raw_to_category)
    print(f"      {written:,} records written.")

    print(f"\n{'=' * 65}")
    print(f"  ✓ Done. Output: {OUTPUT_FILE}")
    print(f"  Each record now has two new fields:")
    print(f"    doc_type_normalized  — after normalization pipeline")
    print(f"    doc_type_category    — one of: {', '.join(CATEGORIES)}")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()