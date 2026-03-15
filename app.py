from flask import Flask, request, jsonify, render_template_string
import sqlite3
import csv
import os
import re
from email_validator import validate_email, EmailNotValidError
from normalizer import normalize_any_row, detect_schema, normalize_row_with_schema, validate_record, is_valid_phone, is_valid_email
from normalizer import normalize_any_row, detect_schema, normalize_row_with_schema, validate_record, is_valid_phone, is_valid_email
from db import get_conn

app = Flask(__name__)
DB_FILE = "lead_engine.db"
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)



NAV_BAR = """
<div style="background:#111;padding:14px 18px;margin-bottom:22px;border-radius:10px;
display:flex;flex-wrap:wrap;gap:18px;font-family:Arial">

<a href="/" style="color:white;text-decoration:none;font-weight:600">Dashboard</a>
<a href="/upload" style="color:white;text-decoration:none;font-weight:600">Upload</a>
<a href="/leads" style="color:white;text-decoration:none;font-weight:600">Leads</a>
<a href="/rejects" style="color:white;text-decoration:none;font-weight:600">Rejects</a>
<a href="/export" style="color:white;text-decoration:none;font-weight:600">Export</a>

</div>
"""


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
  {NAV_BAR}
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
    <p>Unprocessed Raw: {{ unprocessed_count or 0 }}</p>
    <p>Clean Contacts: {{ clean_count }}</p>
    <p>Rejected Rows: {{ rejected_count or 0 }}</p>
    <p>Scored Leads: {{ score_count }}</p>
  </div>

  <div class="card">
    <h2>Top Reject Reasons</h2>
    <table>
      <tr><th>Reason</th><th>Count</th></tr>
      {% for row in reject_reasons %}
      <tr>
        <td>{{ row[0] or '' }}</td>
        <td>{{ row[1] or 0 }}</td>
      </tr>
      {% endfor %}
    </table>
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


def init_db():
    c = get_conn()
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
        processed INTEGER DEFAULT 0,
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS rejected_data (
        reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT,
        first_name TEXT,
        last_name TEXT,
        phone TEXT,
        company TEXT,
        reject_reason TEXT,
        raw_payload TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    try:
        cur.execute("ALTER TABLE raw_data ADD COLUMN processed INTEGER DEFAULT 0")
    except Exception:
        pass

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
    c = get_conn()
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

                if count % 1000 == 0:
                    c.commit()

        else:
            reader = csv.DictReader(f)

            sample_rows = []
            for _ in range(100):
                try:
                    row = next(reader)
                    sample_rows.append(row)
                except StopIteration:
                    break

            schema_info = detect_schema(sample_rows)
            schema_map = schema_info["column_map"]
            print("DETECTED SCHEMA:", schema_map)

            for row in sample_rows:
                record = normalize_row_with_schema(row, schema_map)["normalized"]

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

            for row in reader:
                record = normalize_row_with_schema(row, schema_map)["normalized"]

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

                if count % 1000 == 0:
                    c.commit()

    c.commit()
    c.close()
    return count

def process_raw():
    import json

    c = get_conn()
    cur = c.cursor()

    rows = cur.execute("""
        SELECT record_id, email, first_name, last_name, phone, company, source, campaign
        FROM raw_data
        WHERE processed = 0
        ORDER BY record_id ASC
    """).fetchall()

    inserted = 0
    updated = 0
    rejected = 0
    processed_ids = []

    for r in rows:
        record_id = r["record_id"]
        email = clean_email(r["email"])
        first_name = (r["first_name"] or "").strip()
        last_name = (r["last_name"] or "").strip()
        phone = normalize_phone(r["phone"] or "")
        company = (r["company"] or "").strip()
        source = r["source"] or "import"
        campaign = r["campaign"] or "default_import"

        record = {
            "email": email or "",
            "first_name": first_name,
            "last_name": last_name,
            "phone": phone,
            "company": company,
            "source": source,
            "campaign": campaign,
        }

        ok, reason = validate_record(record)
        if not ok:
            cur.execute("""
                INSERT INTO rejected_data
                (email, first_name, last_name, phone, company, reject_reason, raw_payload)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                email or "",
                first_name,
                last_name,
                phone,
                company,
                reason,
                json.dumps(record),
            ))
            rejected += 1
            processed_ids.append(record_id)
            continue

        dedupe_key = email or phone or None
        if not dedupe_key:
            cur.execute("""
                INSERT INTO rejected_data
                (email, first_name, last_name, phone, company, reject_reason, raw_payload)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                email or "",
                first_name,
                last_name,
                phone,
                company,
                "missing_dedupe_key",
                json.dumps(record),
            ))
            rejected += 1
            processed_ids.append(record_id)
            continue

        validation_status = "valid_email" if email else "valid_phone"

        existing = None
        if email:
            existing = cur.execute(
                "SELECT contact_id FROM clean_data WHERE email = ?",
                (email,)
            ).fetchone()

        if not existing and phone:
            existing = cur.execute(
                "SELECT contact_id FROM clean_data WHERE phone = ?",
                (phone,)
            ).fetchone()

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
            """, (
                first_name, last_name, phone, company,
                source, campaign, validation_status,
                dedupe_key, existing["contact_id"]
            ))
            updated += 1
        else:
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO clean_data
                    (email, first_name, last_name, phone, company, source, campaign, validation_status, dedupe_key)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    email, first_name, last_name, phone, company,
                    source, campaign, validation_status, dedupe_key
                ))
                if cur.rowcount > 0:
                    inserted += 1
                else:
                    updated += 1
            except sqlite3.IntegrityError:
                rejected += 1
                cur.execute("""
                    INSERT INTO rejected_data
                    (email, first_name, last_name, phone, company, reject_reason, raw_payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    email or "",
                    first_name,
                    last_name,
                    phone,
                    company,
                    "integrity_error",
                    json.dumps(record),
                ))

        processed_ids.append(record_id)

    if processed_ids:
        placeholders = ",".join(["?"] * len(processed_ids))
        cur.execute(
            f"UPDATE raw_data SET processed = 1 WHERE record_id IN ({placeholders})",
            processed_ids
        )

    c.commit()
    c.close()
    return {"inserted": inserted, "updated": updated, "rejected": rejected, "processed_rows": len(processed_ids)}

def score_leads():
    c = get_conn()
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
    c = get_conn()
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
    return render_template_string(HTML.replace('{NAV_BAR}', NAV_BAR), raw_count=raw_count, clean_count=clean_count, score_count=score_count, leads=leads)

@app.route("/upload", methods=["GET", "POST"])
def upload():
    if request.method == "GET":
        html = """
        <!doctype html>
        <html>
        <head>
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Upload</title>
          <style>
            body{font-family:Arial;background:#0d1117;color:#fff;padding:20px;max-width:900px;margin:auto}
            .card{background:#161b22;padding:16px;border-radius:12px;margin-bottom:16px}
            input,button{padding:10px;border-radius:8px;border:none;margin:4px 0}
          </style>
        </head>
        <body>
          {NAV_BAR}
          <div class="card">
            <h1>Upload File</h1>
            <form action="/upload" method="post" enctype="multipart/form-data">
              <input type="file" name="file" required>
              <button type="submit">Upload + Process</button>
            </form>
            <p>Test files created in project folder:</p>
            <p>bad_test_leads.json</p>
            <p>bad_test_leads.csv</p>
          </div>
        </body>
        </html>
        """
        return render_template_string(html.replace("{NAV_BAR}", NAV_BAR))

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



@app.route("/leads")
def leads_view():
    q = (request.args.get("q") or "").strip()
    state = (request.args.get("state") or "").strip().upper()
    limit = request.args.get("limit", "100")

    try:
        limit = max(1, min(int(limit), 1000))
    except ValueError:
        limit = 100

    c = get_conn()
    cur = c.cursor()

    sql = """
        SELECT c.contact_id, c.email, c.first_name, c.last_name, c.phone, c.company, ls.score, ls.grade
        FROM clean_data c
        LEFT JOIN lead_scores ls ON c.contact_id = ls.contact_id
        WHERE 1=1
    """
    params = []

    if state:
        sql += " AND c.company = ?"
        params.append(state)

    if q:
        sql += " AND (c.phone LIKE ? OR c.email LIKE ? OR c.first_name LIKE ? OR c.last_name LIKE ?)"
        like = f"%{q}%"
        params.extend([like, like, like, like])

    sql += " ORDER BY c.contact_id DESC LIMIT ?"
    params.append(limit)

    rows = cur.execute(sql, params).fetchall()
    c.close()

    html = """
    <!doctype html>
    <html>
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Clean Data Viewer</title>
      <style>
        body{font-family:Arial;background:#0d1117;color:#fff;padding:20px;max-width:1200px;margin:auto}
        .card{background:#161b22;padding:16px;border-radius:12px;margin-bottom:16px}
        input,button{padding:10px;border-radius:8px;border:none;margin:4px}
        table{width:100%;border-collapse:collapse;font-size:14px}
        td,th{padding:8px;border-bottom:1px solid #333;text-align:left}
        a{color:#58a6ff}
      </style>
    </head>
    <body>
      {NAV_BAR}
      <h1>Clean Data Viewer</h1>

      <div class="card">
        <form method="get" action="/leads">
          <input name="q" placeholder="Search phone, email, first, last" value="{{ q }}">
          <input name="state" placeholder="State e.g. CA" value="{{ state }}">
          <input name="limit" placeholder="Limit" value="{{ limit }}">
          <button type="submit">Search</button>
          <a href="/export?state={{ state }}&q={{ q }}">Export CSV</a>
        </form>
      </div>

      <div class="card">
        <p>Rows shown: {{ rows|length }}</p>
        <table>
          <tr>
            <th>ID</th><th>Email</th><th>First</th><th>Last</th><th>Phone</th><th>State/Company</th><th>Score</th><th>Grade</th>
          </tr>
          {% for r in rows %}
          <tr>
            <td>{{ r[0] }}</td>
            <td>{{ r[1] or '' }}</td>
            <td>{{ r[2] or '' }}</td>
            <td>{{ r[3] or '' }}</td>
            <td>{{ r[4] or '' }}</td>
            <td>{{ r[5] or '' }}</td>
            <td>{{ r[6] or '' }}</td>
            <td>{{ r[7] or '' }}</td>
          </tr>
          {% endfor %}
        </table>
      </div>
    </body>
    </html>
    """
    return render_template_string(html.replace('{NAV_BAR}', NAV_BAR), rows=rows, q=q, state=state, limit=limit)


@app.route("/export")
def export_csv():
    import io
    import csv as pycsv
    from flask import Response

    q = (request.args.get("q") or "").strip()
    state = (request.args.get("state") or "").strip().upper()

    c = get_conn()
    cur = c.cursor()

    sql = """
        SELECT c.contact_id, c.email, c.first_name, c.last_name, c.phone, c.company, ls.score, ls.grade
        FROM clean_data c
        LEFT JOIN lead_scores ls ON c.contact_id = ls.contact_id
        WHERE 1=1
    """
    params = []

    if state:
        sql += " AND c.company = ?"
        params.append(state)

    if q:
        sql += " AND (c.phone LIKE ? OR c.email LIKE ? OR c.first_name LIKE ? OR c.last_name LIKE ?)"
        like = f"%{q}%"
        params.extend([like, like, like, like])

    sql += " ORDER BY c.contact_id DESC LIMIT 50000"

    rows = cur.execute(sql, params).fetchall()
    c.close()

    output = io.StringIO()
    writer = pycsv.writer(output)
    writer.writerow(["contact_id","email","first_name","last_name","phone","state_or_company","score","grade"])
    writer.writerows(rows)

    filename = "clean_export.csv"
    if state:
        filename = f"{state.lower()}_export.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )



@app.route("/rejects")
def rejects_view():
    limit_raw = (request.args.get("limit") or "100").strip()
    reason = (request.args.get("reason") or "").strip()

    try:
        limit = max(1, min(int(limit_raw), 1000))
    except ValueError:
        limit = 100

    c = get_conn()
    cur = c.cursor()

    sql = """
        SELECT reject_id, email, first_name, last_name, phone, company, reject_reason, created_at
        FROM rejected_data
        WHERE 1=1
    """
    params = []

    if reason:
        sql += " AND reject_reason = ?"
        params.append(reason)

    sql += " ORDER BY reject_id DESC LIMIT ?"
    params.append(limit)

    rows = cur.execute(sql, params).fetchall()
    c.close()

    html = """
    <!doctype html>
    <html>
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Rejected Data</title>
      <style>
        body{font-family:Arial;background:#0d1117;color:#fff;padding:20px;max-width:1200px;margin:auto}
        .card{background:#161b22;padding:16px;border-radius:12px;margin-bottom:16px}
        input,button{padding:10px;border-radius:8px;border:none;margin:4px}
        table{width:100%;border-collapse:collapse;font-size:14px}
        td,th{padding:8px;border-bottom:1px solid #333;text-align:left}
        a{color:#58a6ff;text-decoration:none}
      </style>
    </head>
    <body>
      {NAV_BAR}
      <h1>Rejected Data</h1>
      <div class="card">
        <a href="/">Dashboard</a> |
        <a href="/leads">Viewer</a> |
        <a href="/rejects">Rejects</a>
      </div>
      <div class="card">
        <form method="get" action="/rejects">
          <input name="reason" placeholder="reject reason" value="{{ reason }}">
          <input name="limit" placeholder="limit" value="{{ limit }}">
          <button type="submit">Filter</button>
        </form>
      </div>
      <div class="card">
        <p>Rows shown: {{ rows|length }}</p>
        <table>
          <tr>
            <th>ID</th><th>Email</th><th>First</th><th>Last</th><th>Phone</th><th>State/Company</th><th>Reason</th><th>Created</th>
          </tr>
          {% for r in rows %}
          <tr>
            <td>{{ r[0] }}</td>
            <td>{{ r[1] or '' }}</td>
            <td>{{ r[2] or '' }}</td>
            <td>{{ r[3] or '' }}</td>
            <td>{{ r[4] or '' }}</td>
            <td>{{ r[5] or '' }}</td>
            <td>{{ r[6] or '' }}</td>
            <td>{{ r[7] or '' }}</td>
          </tr>
          {% endfor %}
        </table>
      </div>
    </body>
    </html>
    """
    return render_template_string(html.replace('{NAV_BAR}', NAV_BAR), rows=rows, reason=reason, limit=limit)

@app.route("/api/leads")
def api_leads():
    c = get_conn()
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
