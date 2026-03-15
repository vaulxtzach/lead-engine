import re
from datetime import datetime

CANONICAL_FIELDS = [
    "email",
    "phone",
    "first_name",
    "last_name",
    "full_name",
    "state",
    "city",
    "address",
    "zip_code",
    "company",
    "source",
    "campaign",
    "amount_owed",
    "language",
    "case_id",
    "date",
]

NULL_LIKE = {
    "", " ", "n/a", "na", "none", "null", "unknown", "test", "-", "--"
}

US_STATE_ABBR = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC"
}

US_STATE_NAMES = {
    "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
    "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
    "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA","kansas":"KS",
    "kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD","massachusetts":"MA",
    "michigan":"MI","minnesota":"MN","mississippi":"MS","missouri":"MO","montana":"MT",
    "nebraska":"NE","nevada":"NV","new hampshire":"NH","new jersey":"NJ","new mexico":"NM",
    "new york":"NY","north carolina":"NC","north dakota":"ND","ohio":"OH","oklahoma":"OK",
    "oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC",
    "south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT",
    "virginia":"VA","washington":"WA","west virginia":"WV","wisconsin":"WI","wyoming":"WY",
    "district of columbia":"DC"
}

FIELD_ALIASES = {
    "email": ["email", "e-mail", "email address", "email_address", "mail"],
    "phone": ["phone", "phone number", "cell", "mobile", "tel", "phone1", "cell phone"],
    "first_name": ["first name", "firstname", "fname", "first_name"],
    "last_name": ["last name", "lastname", "lname", "last_name"],
    "full_name": ["name", "full name", "contact name"],
    "state": ["state", "st", "state name"],
    "city": ["city", "town"],
    "address": ["address", "street", "street address"],
    "zip_code": ["zip", "zipcode", "postal", "postal code"],
    "company": ["company", "business", "company name"],
    "source": ["source", "vendor", "provider"],
    "campaign": ["campaign", "campaign name"],
    "amount_owed": ["amount owed", "balance", "debt", "amt owed", "amount"],
    "language": ["language", "lang"],
    "case_id": ["case #", "case", "case id", "case number"],
    "date": ["date", "created date", "import date"],
}

def normalize_text(value):
    if value is None:
        return ""
    v = str(value).strip()
    if v.lower() in NULL_LIKE:
        return ""
    return v

def canonicalize_key(key):
    k = normalize_text(key).lower()
    k = k.replace("_", " ").replace("-", " ")
    k = re.sub(r"\s+", " ", k)
    return k

def match_header_to_canonical(raw_key):
    ck = canonicalize_key(raw_key)
    for canonical, aliases in FIELD_ALIASES.items():
        if ck == canonicalize_key(canonical):
            return canonical
        for alias in aliases:
            if ck == canonicalize_key(alias):
                return canonical
    return None

def normalize_email(email):
    v = normalize_text(email).lower()
    if not v:
        return ""
    return v

def normalize_phone(phone):
    v = normalize_text(phone)
    if not v:
        return ""
    digits = re.sub(r"\D", "", v)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits

def normalize_state(state):
    v = normalize_text(state)
    if not v:
        return ""
    upper = v.upper()
    if upper in US_STATE_ABBR:
        return upper
    lower = v.lower()
    return US_STATE_NAMES.get(lower, upper if len(upper) <= 3 else v)

def normalize_zip(zip_code):
    v = normalize_text(zip_code)
    if not v:
        return ""
    digits = re.sub(r"\D", "", v)
    if len(digits) >= 5:
        return digits[:5]
    return digits

def normalize_currency(amount):
    v = normalize_text(amount)
    if not v:
        return ""
    cleaned = re.sub(r"[^0-9.\-]", "", v)
    return cleaned

def normalize_date(value):
    v = normalize_text(value)
    if not v:
        return ""
    fmts = [
        "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d",
        "%m-%d-%Y", "%m-%d-%y", "%b %d %Y", "%B %d %Y"
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return v

def looks_like_email(v):
    return "@" in v and "." in v.split("@")[-1]

def looks_like_phone(v):
    digits = re.sub(r"\D", "", v)
    return len(digits) in (10, 11)

def looks_like_currency(v):
    return bool(re.search(r"[\$,\d]", v)) and any(ch.isdigit() for ch in v)

def looks_like_date(v):
    return bool(re.match(r"^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$", v) or re.match(r"^\d{4}-\d{2}-\d{2}$", v))

def looks_like_name(v):
    parts = v.strip().split()
    if len(parts) < 2:
        return False
    return all(any(ch.isalpha() for ch in p) for p in parts[:2])

def infer_canonical_from_value(value):
    v = normalize_text(value)
    if not v:
        return None
    if looks_like_email(v):
        return "email"
    if looks_like_phone(v):
        return "phone"
    if normalize_state(v) in US_STATE_ABBR:
        return "state"
    if looks_like_date(v):
        return "date"
    if looks_like_currency(v):
        return "amount_owed"
    if looks_like_name(v):
        return "full_name"
    return None

def normalize_field_value(field, value):
    if field == "email":
        return normalize_email(value)
    if field == "phone":
        return normalize_phone(value)
    if field == "state":
        return normalize_state(value)
    if field == "zip_code":
        return normalize_zip(value)
    if field == "amount_owed":
        return normalize_currency(value)
    if field == "date":
        return normalize_date(value)
    return normalize_text(value)

def split_full_name(full_name):
    v = normalize_text(full_name)
    if not v:
        return "", ""
    parts = v.split()
    first = parts[0]
    last = " ".join(parts[1:]) if len(parts) > 1 else ""
    return first, last

def post_normalize_record(record):
    if record.get("full_name") and not record.get("first_name") and not record.get("last_name"):
        first, last = split_full_name(record["full_name"])
        record["first_name"] = first
        record["last_name"] = last

    if record.get("email"):
        record["email"] = normalize_email(record["email"])

    if record.get("phone"):
        record["phone"] = normalize_phone(record["phone"])

    if record.get("state"):
        record["state"] = normalize_state(record["state"])

    if record.get("zip_code"):
        record["zip_code"] = normalize_zip(record["zip_code"])

    if record.get("amount_owed"):
        record["amount_owed"] = normalize_currency(record["amount_owed"])

    if record.get("date"):
        record["date"] = normalize_date(record["date"])

    return record

def normalize_any_row(row):
    raw = dict(row)
    normalized = {field: "" for field in CANONICAL_FIELDS}
    mapping = {}
    detected = {}

    for raw_key, raw_value in row.items():
        canonical = match_header_to_canonical(raw_key)

        if canonical:
            mapping[raw_key] = canonical
            normalized[canonical] = normalize_field_value(canonical, raw_value)
            detected[canonical] = infer_canonical_from_value(str(raw_value)) or canonical
        else:
            inferred = infer_canonical_from_value(raw_value)
            if inferred and not normalized.get(inferred):
                mapping[raw_key] = inferred
                normalized[inferred] = normalize_field_value(inferred, raw_value)
                detected[inferred] = inferred

    normalized = post_normalize_record(normalized)

    extras = {}
    for raw_key, raw_value in row.items():
        if raw_key not in mapping:
            extras[raw_key] = raw_value

    return {
        "normalized": normalized,
        "raw": raw,
        "mapping": mapping,
        "detected_types": detected,
        "extra_fields": extras,
    }

if __name__ == "__main__":
    sample = {
        "Email Address": " JOHNDOE@GMAIL.COM ",
        "Cell": "(410) 627-6516",
        "Name": "John Doe",
        "State Name": "california",
        "Amt Owed": "$15,000",
        "Case #": "ABC123"
    }
    print(normalize_any_row(sample))


def detect_value_type(value):
    v = normalize_text(value)
    if not v:
        return "empty"
    if looks_like_email(v):
        return "email"
    if looks_like_phone(v):
        return "phone"
    if normalize_state(v) in US_STATE_ABBR:
        return "state"
    if looks_like_date(v):
        return "date"
    if looks_like_currency(v):
        return "amount_owed"
    if looks_like_name(v):
        return "full_name"
    return "text"

def detect_schema(rows):
    """
    Analyze a sample of rows and return:
    {
      "column_map": {"Raw Header": "canonical_field"},
      "detected_types": {"Raw Header": "email" | "phone" | ...}
    }
    """
    if not rows:
        return {"column_map": {}, "detected_types": {}}

    scores = {}
    detected_types = {}

    for row in rows:
        for raw_key, raw_value in row.items():
            raw_key = str(raw_key)
            scores.setdefault(raw_key, {})
            header_match = match_header_to_canonical(raw_key)
            value_match = infer_canonical_from_value(raw_value)
            value_type = detect_value_type(raw_value)

            if header_match:
                scores[raw_key][header_match] = scores[raw_key].get(header_match, 0) + 5

            if value_match:
                scores[raw_key][value_match] = scores[raw_key].get(value_match, 0) + 1

            if raw_key not in detected_types:
                detected_types[raw_key] = {}
            detected_types[raw_key][value_type] = detected_types[raw_key].get(value_type, 0) + 1

    column_map = {}
    for raw_key, field_scores in scores.items():
        if field_scores:
            best = sorted(field_scores.items(), key=lambda x: x[1], reverse=True)[0][0]
            column_map[raw_key] = best

    final_types = {}
    for raw_key, type_scores in detected_types.items():
        best = sorted(type_scores.items(), key=lambda x: x[1], reverse=True)[0][0]
        final_types[raw_key] = best

    return {
        "column_map": column_map,
        "detected_types": final_types,
    }

def normalize_row_with_schema(row, schema_map):
    raw = dict(row)
    normalized = {field: "" for field in CANONICAL_FIELDS}
    mapping = {}
    detected = {}

    for raw_key, raw_value in row.items():
        canonical = schema_map.get(raw_key) or match_header_to_canonical(raw_key)

        if canonical:
            mapping[raw_key] = canonical
            normalized[canonical] = normalize_field_value(canonical, raw_value)
            detected[canonical] = detect_value_type(str(raw_value))
        else:
            inferred = infer_canonical_from_value(raw_value)
            if inferred and not normalized.get(inferred):
                mapping[raw_key] = inferred
                normalized[inferred] = normalize_field_value(inferred, raw_value)
                detected[inferred] = detect_value_type(str(raw_value))

    normalized = post_normalize_record(normalized)

    extras = {}
    for raw_key, raw_value in row.items():
        if raw_key not in mapping:
            extras[raw_key] = raw_value

    return {
        "normalized": normalized,
        "raw": raw,
        "mapping": mapping,
        "detected_types": detected,
        "extra_fields": extras,
    }
