import csv
import os
import sqlite3
from datetime import datetime, UTC
from verticals import VERTICALS

DB = "lead_engine.db"
EXPORT_DIR = "exports_out"
os.makedirs(EXPORT_DIR, exist_ok=True)

def export_leads(vertical="tax", limit=100, tier=None):
    if vertical not in VERTICALS:
        raise ValueError(f"Invalid vertical: {vertical}")

    label = VERTICALS[vertical]["label"]

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    sql = """
        SELECT first_name, last_name, phone, state, email, source, campaign, score, tier
        FROM leads
        WHERE phone IS NOT NULL AND TRIM(phone) != ''
    """
    params = []

    if tier:
        sql += " AND tier = ?"
        params.append(tier)

    sql += " ORDER BY score DESC, id DESC LIMIT ?"
    params.append(limit)

    rows = cur.execute(sql, params).fetchall()

    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    tier_part = f"_{tier}" if tier else ""
    path = os.path.join(EXPORT_DIR, f"{vertical}{tier_part}_dialer_{ts}.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Name","Phone","State","Email","Vertical","Source","Score","Tier"])

        for r in rows:
            name = f"{r['first_name'] or ''} {r['last_name'] or ''}".strip() or "Unknown"
            w.writerow([
                name,
                r["phone"] or "",
                r["state"] or "",
                r["email"] or "",
                label,
                r["source"] or "engine",
                r["score"] or 0,
                r["tier"] or ""
            ])

    conn.close()
    return path, len(rows)
