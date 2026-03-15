from flask import Flask, request, jsonify, render_template_string
import sqlite3
import csv
import os
import re
from email_validator import validate_email, EmailNotValidError
from normalizer import normalize_any_row

app = Flask(__name__)
DB_FILE = "lead_engine.db"
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

HTML = """
<!doctype html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lead Engine</title>
  <style>
    body{font-family:Arial;background:#0d1117;color:#fff;padding:20px;max-width:1000px;margin:auto}
    .card{background:#161b22;padding:16px;border-radius:12px;margin-bottom:16px}
    input,button{padding:10px;border-radius:8px;border:none;margin:4px 0}
    button{cursor:pointer}
    table{width:100%;border-collapse:collapse}
    td,th{padding:8px;border-bottom:1px solid #333;text-align:left}
  </style>
</head>
<body>
  <h1>Lead Engine</h1>

  <div class="card">
    <h2>Upload CSV</h2>
    <form action="/upload" method="post" enctype="multipart/form-data">
      <input type="file" name="file" required>
      <button type="submit">Upload + Process</button>
    </form>
  </div>

  <div class="card">
    <h2>Stats</h2>
    <p>Raw Records: {{ raw_count }}</p>
    <p>Clean Contacts: {{ clean_count }}</p>
    <p>Scored Leads: {{ score_count }}</p>
  </div>

  <div class="card">
    <h2>Top Leads</h2>
    <table>
      <tr><th>Email</th><th>Name</th><th>Phone</th><th>Company</th><th>Score</th><th>Grade</th></tr>
      {% for row in leads %}
      <tr>
        <td>{{ row[0] or '' }}</td>
        <td>{{ row[1] or '' }} {{ row[2] or '' }}</td>
        <td>{{ row[3] or '' }}</td>
        <td>{{ row[4] or '' }}</td>
        <td>{{ row[5] or 0 }}</td>
        <td>{{ row[6] or '' }}</td>
      </tr>
      {% endfor %}
    </table>
  </div>
</body>
</html>
"""

def conn():
    c = sqlite3.connect(DB_FILE)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    c = conn()
    cur = c.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS raw_data (
        record_id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT,
        first_name TEXT,
        last_name TEXT,
        phone TEXT,
        company TEXT,
        source TEXT DEFAULT 'import',
        campaign TEXT DEFAULT 'default_import',
        import_date TEXT DEFAULT CURRENT_DATE,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS clean_data (
        contact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE,
        first_name TEXT,
        last_name TEXT,
        phone TEXT,
        company TEXT,
        source TEXT,
        campaign TEXT,
        validation_status TEXT DEFAULT 'pending',
        dedupe_key TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS lead_scores (
        score_id INTEGER PRIMARY KEY AUTOINCREMENT,
        contact_id INTEGER UNIQUE,
        score INTEGER DEFAULT 0,
        grade TEXT DEFAULT 'C',
        reason TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(contact_id) REFERENCES clean_data(contact_id)
    )
    """)

    c.commit()
    c.close()

def normalize_phone(phone):
    if not phone:
        return ""
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits

def clean_email(email):
    email = (email or "").strip().lower()
    if not email:
        return None
    try:
        return validate_email(email, check_deliverability=False).normalized
    except EmailNotValidError:
        return None



def import_csv(path):
    c = conn()
    cur = c.cursor()
    count = 0

    with open(path, newline="", encoding="latin-1", errors="replace") as f:
        first_line = f.readline().strip()
        f.seek(0)

        parts = [x.strip() for x in first_line.split(",")]
        simple_state_phone = (
            len(parts) == 2
            and len(parts[0]) <= 3
            and parts[0].isalpha()
            and any(ch.isdigit() for ch in parts[1])
        )

        if simple_state_phone:
            reader = csv.reader(f)
            for row in reader:
                if len(row) < 2:
                    continue

                state = (row[0] or "").strip()
                phone = normalize_phone(row[1] or "")

                if not phone:
                    continue

                cur.execute("""
                    INSERT INTO raw_data (email, first_name, last_name, phone, company)
                    VALUES (?, ?, ?, ?, ?)
                """, ("", "", "", phone, state))
                count += 1

        else:
            reader = csv.DictReader(f)

            for row in reader:
                record = normalize_any_row(row)["normalized"]

                email = record["email"]
                phone = record["phone"]
                first_name = record["first_name"]
                last_name = record["last_name"]
                company = record["company"] or record["state"]

                cur.execute("""
                    INSERT INTO raw_data (email, first_name, last_name, phone, company)
                    VALUES (?, ?, ?, ?, ?)
                """, (email, first_name, last_name, phone, company))
                count += 1

    c.commit()
    c.close()
    return count

def process_raw():
    c = conn()
    cur = c.cursor()

    rows = cur.execute("""
        SELECT record_id, email, first_name, last_name, phone, company, source, campaign
        FROM raw_data ORDER BY record_id ASC
    """).fetchall()

    inserted = 0
    updated = 0

    for r in rows:
        email = clean_email(r["email"])
        first_name = (r["first_name"] or "").strip()
        last_name = (r["last_name"] or "").strip()
        phone = normalize_phone(r["phone"] or "")
        company = (r["company"] or "").strip()
        source = r["source"] or "import"
        campaign = r["campaign"] or "default_import"

        dedupe_key = email or phone or None
        if not dedupe_key:
            continue

        validation_status = "valid" if email else "phone_only"

        existing = None
        if email:
            existing = cur.execute("SELECT contact_id FROM clean_data WHERE email = ?", (email,)).fetchone()

        if existing:
            cur.execute("""
                UPDATE clean_data
                SET first_name = COALESCE(NULLIF(?, ''), first_name),
                    last_name = COALESCE(NULLIF(?, ''), last_name),
                    phone = COALESCE(NULLIF(?, ''), phone),
                    company = COALESCE(NULLIF(?, ''), company),
                    source = COALESCE(NULLIF(?, ''), source),
                    campaign = COALESCE(NULLIF(?, ''), campaign),
                    validation_status = ?,
                    dedupe_key = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE contact_id = ?
            """, (first_name, last_name, phone, company, source, campaign, validation_status, dedupe_key, existing["contact_id"]))
            updated += 1
        else:
            try:
                cur.execute("""
                    INSERT INTO clean_data
                    (email, first_name, last_name, phone, company, source, campaign, validation_status, dedupe_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (email, first_name, last_name, phone, company, source, campaign, validation_status, dedupe_key))
                inserted += 1
            except sqlite3.IntegrityError:
                pass

    c.commit()
    c.close()
    return {"inserted": inserted, "updated": updated}

def score_leads():
    c = conn()
    cur = c.cursor()

    contacts = cur.execute("""
        SELECT contact_id, email, phone, company FROM clean_data
    """).fetchall()

    for row in contacts:
        score = 0
        reasons = []

        if row["email"]:
            score += 30
            reasons.append("has email")
        if row["phone"]:
            score += 20
            reasons.append("has phone")
        if row["company"]:
            score += 10
            reasons.append("has company")

        if score >= 70:
            grade = "A"
        elif score >= 50:
            grade = "B"
        else:
            grade = "C"

        cur.execute("""
            INSERT INTO lead_scores (contact_id, score, grade, reason, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(contact_id) DO UPDATE SET
              score=excluded.score,
              grade=excluded.grade,
              reason=excluded.reason,
              updated_at=CURRENT_TIMESTAMP
        """, (row["contact_id"], score, grade, ", ".join(reasons)))

    c.commit()
    c.close()

@app.route("/")
def index():
    c = conn()
    cur = c.cursor()

    raw_count = cur.execute("SELECT COUNT(*) FROM raw_data").fetchone()[0]
    clean_count = cur.execute("SELECT COUNT(*) FROM clean_data").fetchone()[0]
    score_count = cur.execute("SELECT COUNT(*) FROM lead_scores").fetchone()[0]

    leads = cur.execute("""
        SELECT c.email, c.first_name, c.last_name, c.phone, c.company, ls.score, ls.grade
        FROM clean_data c
        LEFT JOIN lead_scores ls ON c.contact_id = ls.contact_id
        ORDER BY ls.score DESC, c.contact_id DESC
        LIMIT 25
    """).fetchall()

    c.close()
    return render_template_string(HTML, raw_count=raw_count, clean_count=clean_count, score_count=score_count, leads=leads)

@app.route("/upload", methods=["POST"])
def upload():
    if "file" not in request.files:
        return "No file uploaded", 400

    file = request.files["file"]
    if not file.filename:
        return "No selected file", 400

    path = os.path.join(UPLOAD_DIR, file.filename)
    file.save(path)

    imported = import_csv(path)
    processed = process_raw()
    score_leads()

    return jsonify({
        "status": "ok",
        "file": file.filename,
        "imported_raw": imported,
        "processed": processed,
        "scored": True
    })

@app.route("/api/leads")
def api_leads():
    c = conn()
    rows = c.execute("""
        SELECT c.contact_id, c.email, c.first_name, c.last_name, c.phone, c.company, ls.score, ls.grade, ls.reason
        FROM clean_data c
        LEFT JOIN lead_scores ls ON c.contact_id = ls.contact_id
        ORDER BY ls.score DESC, c.contact_id DESC
        LIMIT 100
    """).fetchall()
    c.close()

    return jsonify([dict(r) for r in rows])

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
