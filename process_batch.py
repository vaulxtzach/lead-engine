import sqlite3
import re
from datetime import datetime, UTC

DB = "lead_engine.db"
BATCH_SIZE = 5000

STATE_MAP = {
    "AL":"AL","AK":"AK","AZ":"AZ","AR":"AR","CA":"CA","CO":"CO","CT":"CT","DE":"DE","FL":"FL","GA":"GA",
    "HI":"HI","ID":"ID","IL":"IL","IN":"IN","IA":"IA","KS":"KS","KY":"KY","LA":"LA","ME":"ME","MD":"MD",
    "MA":"MA","MI":"MI","MN":"MN","MS":"MS","MO":"MO","MT":"MT","NE":"NE","NV":"NV","NH":"NH","NJ":"NJ",
    "NM":"NM","NY":"NY","NC":"NC","ND":"ND","OH":"OH","OK":"OK","OR":"OR","PA":"PA","RI":"RI","SC":"SC",
    "SD":"SD","TN":"TN","TX":"TX","UT":"UT","VT":"VT","VA":"VA","WA":"WA","WV":"WV","WI":"WI","WY":"WY",
    "DC":"DC"
}
EMAIL_RE = re.compile(r"^[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}$", re.I)

def now():
    return datetime.now(UTC).isoformat()

def normalize_phone(p):
    digits = "".join(c for c in str(p or "") if c.isdigit())
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) == 10 else None

def normalize_email(e):
    e = str(e or "").strip().lower()
    return e if EMAIL_RE.match(e) else None

def normalize_state(raw_state, company):
    s = str(raw_state or "").strip().upper()
    if s in STATE_MAP:
        return s
    c = str(company or "").strip().upper()
    if c in STATE_MAP:
        return c
    return None

def score_row(phone, email, state, first_name, last_name):
    score = 0
    reasons = []
    if phone:
        score += 50
        reasons.append("valid_phone")
    if email:
        score += 10
        reasons.append("valid_email")
    if state:
        score += 15
        reasons.append("state")
    if first_name:
        score += 10
        reasons.append("first_name")
    if last_name:
        score += 10
        reasons.append("last_name")
    if phone and (first_name or last_name):
        score += 5
        reasons.append("call_ready")
    grade = "A" if score >= 80 else "B" if score >= 60 else "C" if score >= 45 else "D"
    return score, grade, ",".join(reasons)

def ensure_meta(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS engine_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_record_id ON raw_data(record_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clean_contact_id ON clean_data(contact_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clean_email ON clean_data(email)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clean_phone ON clean_data(phone)")

def get_last_id(cur):
    row = cur.execute("SELECT value FROM engine_meta WHERE key='last_processed_record_id'").fetchone()
    return int(row[0]) if row and row[0] else 0

def set_last_id(cur, last_id):
    cur.execute("""
        INSERT INTO engine_meta(key, value) VALUES('last_processed_record_id', ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (str(last_id),))

def process_once():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    ensure_meta(cur)
    conn.commit()

    last_id = get_last_id(cur)

    rows = cur.execute("""
        SELECT *
        FROM raw_data
        WHERE record_id > ?
        ORDER BY record_id ASC
        LIMIT ?
    """, (last_id, BATCH_SIZE)).fetchall()

    if not rows:
        print("No more rows to process.")
        conn.close()
        return False

    clean_inserted = 0
    rejected_inserted = 0
    lead_inserted = 0
    score_inserted = 0
    skipped_clean_dupes = 0
    skipped_lead_dupes = 0

    max_seen_id = last_id

    for r in rows:
        record_id = r["record_id"]
        max_seen_id = max(max_seen_id, record_id)

        first_name = str(r["first_name"] or "").strip()
        last_name = str(r["last_name"] or "").strip()
        raw_phone = str(r["phone"] or "").strip()
        raw_email = str(r["email"] or "").strip()
        company = str(r["company"] or "").strip()
        source = str(r["source"] or "").strip()
        campaign = str(r["campaign"] or "").strip()

        phone = normalize_phone(raw_phone)
        email = normalize_email(raw_email)
        state = normalize_state("", company)

        has_identity = bool(first_name or last_name or company or email)

        reject_reasons = []
        if not phone:
            reject_reasons.append("invalid_phone")
        if not has_identity:
            reject_reasons.append("missing_identity")

        if reject_reasons:
            cur.execute("""
                INSERT INTO rejected_data (
                    email, first_name, last_name, phone, company, reject_reason, raw_payload, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                raw_email if raw_email else None,
                first_name or None,
                last_name or None,
                raw_phone if raw_phone else None,
                company if company else None,
                ",".join(reject_reasons),
                str(dict(r)),
                now()
            ))
            rejected_inserted += 1
            continue

        existing_clean = None
        if email:
            existing_clean = cur.execute(
                "SELECT 1 FROM clean_data WHERE email = ? LIMIT 1", (email,)
            ).fetchone()
        if not existing_clean and phone:
            existing_clean = cur.execute(
                "SELECT 1 FROM clean_data WHERE phone = ? LIMIT 1", (phone,)
            ).fetchone()

        if existing_clean:
            skipped_clean_dupes += 1
            continue

        existing_lead = cur.execute(
            "SELECT 1 FROM leads WHERE phone = ? LIMIT 1", (phone,)
        ).fetchone()
        if existing_lead:
            skipped_lead_dupes += 1
            continue

        score, grade, reason = score_row(phone, email, state, first_name, last_name)
        dedupe_key = email if email else phone

        cur.execute("""
            INSERT INTO clean_data (
                contact_id, email, first_name, last_name, phone, company, source, campaign,
                validation_status, dedupe_key, created_at, updated_at, intent_score, intent_grade,
                intent_reason, inferred_state, timezone, dial_window, carrier, line_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record_id, email, first_name or None, last_name or None, phone, company or None,
            source or None, campaign or None, "format_valid", dedupe_key,
            now(), now(), score, grade, reason, state, None, None, None, None
        ))
        clean_inserted += 1

        cur.execute("""
            INSERT INTO lead_scores (contact_id, score, grade, reason, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (record_id, score, grade, reason, now()))
        score_inserted += 1

        cur.execute("""
            INSERT INTO leads (
                first_name, last_name, phone, email, state, age, dob, mortgage_amount,
                loan_to_value, net_worth, source, campaign, vertical, score, status,
                reject_reason, assigned_to, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            first_name or None, last_name or None, phone, email, state,
            None, None, None, None, None,
            source or None, campaign or None, "tax", score, "clean",
            None, None, now()
        ))
        lead_inserted += 1

    set_last_id(cur, max_seen_id)
    conn.commit()

    print(f"Processed record_id > {last_id} up to {max_seen_id}")
    print("Inserted into clean_data:", clean_inserted)
    print("Inserted into rejected_data:", rejected_inserted)
    print("Inserted into lead_scores:", score_inserted)
    print("Inserted into leads:", lead_inserted)
    print("Skipped clean dupes:", skipped_clean_dupes)
    print("Skipped lead dupes:", skipped_lead_dupes)

    totals = {}
    for t in ["raw_data", "clean_data", "leads", "lead_scores", "exports", "rejected_data"]:
        totals[t] = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print("Totals:", totals)

    conn.close()
    return True

if __name__ == "__main__":
    process_once()
