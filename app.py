from flask import Flask, render_template, jsonify, request
import sqlite3
import os
import requests
from bs4 import BeautifulSoup
import time
import threading
import json
from datetime import datetime

app = Flask(__name__)
DB_PATH = "brokers.db"

scrape_status = {
    "running": False,
    "total": 0,
    "scraped": 0,
    "message": "Idle"
}

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS brokers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            firm TEXT,
            city TEXT,
            state TEXT,
            email TEXT,
            phone TEXT,
            website TEXT,
            profile_url TEXT UNIQUE,
            bouncer_status TEXT DEFAULT 'unchecked',
            in_reply BOOLEAN DEFAULT 0,
            notes TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def scrape_ibba():
    global scrape_status
    scrape_status["running"] = True
    scrape_status["message"] = "Starting scrape..."
    scrape_status["scraped"] = 0

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    base_url = "https://www.ibba.org/find-a-business-broker/"
    page = 1
    all_brokers = []

    try:
        while True:
            scrape_status["message"] = f"Scraping page {page}..."
            url = f"{base_url}?pg={page}" if page > 1 else base_url

            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                scrape_status["message"] = f"Got status {resp.status_code} on page {page}, stopping."
                break

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find broker listings — IBBA uses varying markup, try common patterns
            broker_cards = soup.select(".member-directory-result, .broker-card, article.broker, .directory-item, .member-result")
            if not broker_cards:
                # Fallback: look for any repeated card-like structure
                broker_cards = soup.select(".wpbdp-listing, .listing-item")

            if not broker_cards:
                scrape_status["message"] = f"No more broker cards found on page {page}. Done."
                break

            for card in broker_cards:
                name = ""
                firm = ""
                city = ""
                state = ""
                email = ""
                phone = ""
                website = ""
                profile_url = ""

                name_el = card.select_one("h2, h3, .name, .broker-name, .listing-title")
                if name_el:
                    name = name_el.get_text(strip=True)

                link_el = card.select_one("a[href*='broker'], a[href*='member'], h2 a, h3 a")
                if link_el:
                    profile_url = link_el.get("href", "")

                for el in card.select("p, li, span, div"):
                    text = el.get_text(strip=True)
                    if "@" in text and not email:
                        email = text.strip()
                    if any(x in text.lower() for x in ["phone", "tel", "call"]) and not phone:
                        phone = text.replace("Phone:", "").replace("Tel:", "").strip()

                # Try to extract city/state from address or location fields
                location_el = card.select_one(".location, .city, .address, [class*='location'], [class*='city']")
                if location_el:
                    loc_text = location_el.get_text(strip=True)
                    parts = loc_text.split(",")
                    if len(parts) >= 2:
                        city = parts[0].strip()
                        state = parts[1].strip().split()[0] if parts[1].strip() else ""

                firm_el = card.select_one(".company, .firm, .organization, [class*='company'], [class*='firm']")
                if firm_el:
                    firm = firm_el.get_text(strip=True)

                if name or email:
                    all_brokers.append({
                        "name": name,
                        "firm": firm,
                        "city": city,
                        "state": state,
                        "email": email,
                        "phone": phone,
                        "website": website,
                        "profile_url": profile_url
                    })

            scrape_status["scraped"] = len(all_brokers)

            # Check for next page
            next_btn = soup.select_one("a.next, a[rel='next'], .pagination .next a")
            if not next_btn:
                break

            page += 1
            time.sleep(1.5)  # polite delay

    except Exception as e:
        scrape_status["message"] = f"Error: {str(e)}"
        scrape_status["running"] = False
        return

    # Save to DB
    conn = get_db()
    inserted = 0
    for b in all_brokers:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO brokers (name, firm, city, state, email, phone, website, profile_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (b["name"], b["firm"], b["city"], b["state"], b["email"], b["phone"], b["website"], b["profile_url"]))
            inserted += 1
        except Exception:
            pass
    conn.commit()
    conn.close()

    scrape_status["running"] = False
    scrape_status["total"] = inserted
    scrape_status["message"] = f"Done! Scraped and saved {inserted} brokers."

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/brokers")
def get_brokers():
    search = request.args.get("search", "").strip()
    state = request.args.get("state", "").strip()
    city = request.args.get("city", "").strip()
    email_only = request.args.get("email_only", "false") == "true"
    page = int(request.args.get("page", 1))
    per_page = 50

    conn = get_db()
    query = "SELECT * FROM brokers WHERE 1=1"
    params = []

    if search:
        query += " AND (name LIKE ? OR firm LIKE ? OR email LIKE ?)"
        params += [f"%{search}%", f"%{search}%", f"%{search}%"]
    if state:
        query += " AND state LIKE ?"
        params.append(f"%{state}%")
    if city:
        query += " AND city LIKE ?"
        params.append(f"%{city}%")
    if email_only:
        query += " AND email != '' AND email IS NOT NULL"

    count_query = query.replace("SELECT *", "SELECT COUNT(*)")
    total = conn.execute(count_query, params).fetchone()[0]

    query += f" ORDER BY state, name LIMIT {per_page} OFFSET {(page-1)*per_page}"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    return jsonify({
        "brokers": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "pages": (total + per_page - 1) // per_page
    })

@app.route("/api/states")
def get_states():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT state FROM brokers WHERE state != '' ORDER BY state").fetchall()
    conn.close()
    return jsonify([r["state"] for r in rows])

@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    if scrape_status["running"]:
        return jsonify({"error": "Scrape already running"}), 400
    t = threading.Thread(target=scrape_ibba)
    t.daemon = True
    t.start()
    return jsonify({"message": "Scrape started"})

@app.route("/api/scrape/status")
def scrape_status_route():
    return jsonify(scrape_status)

@app.route("/api/brokers/<int:broker_id>", methods=["PATCH"])
def update_broker(broker_id):
    data = request.json
    allowed = ["notes", "bouncer_status", "in_reply"]
    conn = get_db()
    for field in allowed:
        if field in data:
            conn.execute(f"UPDATE brokers SET {field}=? WHERE id=?", (data[field], broker_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/export")
def export_csv():
    from flask import Response
    import csv
    import io

    state = request.args.get("state", "")
    email_only = request.args.get("email_only", "false") == "true"

    conn = get_db()
    query = "SELECT name, firm, city, state, email, phone FROM brokers WHERE 1=1"
    params = []
    if state:
        query += " AND state LIKE ?"
        params.append(f"%{state}%")
    if email_only:
        query += " AND email != '' AND email IS NOT NULL"
    rows = conn.execute(query, params).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Name", "Firm", "City", "State", "Email", "Phone"])
    for r in rows:
        writer.writerow([r["name"], r["firm"], r["city"], r["state"], r["email"], r["phone"]])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename=ibba_brokers.csv"}
    )

@app.route("/api/stats")
def stats():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM brokers").fetchone()[0]
    with_email = conn.execute("SELECT COUNT(*) FROM brokers WHERE email != '' AND email IS NOT NULL").fetchone()[0]
    states = conn.execute("SELECT COUNT(DISTINCT state) FROM brokers WHERE state != ''").fetchone()[0]
    conn.close()
    return jsonify({"total": total, "with_email": with_email, "states": states})

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
