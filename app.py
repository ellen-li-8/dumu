from flask import Flask, render_template, jsonify, request, Response
import sqlite3
import os
import requests
from bs4 import BeautifulSoup
import time
import threading
import csv
import io
import traceback

app = Flask(__name__)

# Use /tmp on Railway since the app directory may be read-only
DB_PATH = "/tmp/brokers.db"

scrape_status = {
    "running": False,
    "total": 0,
    "scraped": 0,
    "message": "Idle",
    "last_error": ""
}

STATES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new-hampshire", "new-jersey", "new-mexico", "new-york",
    "north-carolina", "north-dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode-island", "south-carolina", "south-dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west-virginia", "wisconsin", "wyoming"
]

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
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
            profile_url TEXT,
            bouncer_status TEXT DEFAULT 'unchecked',
            in_reply INTEGER DEFAULT 0,
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
    scrape_status["last_error"] = ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    inserted = 0

    try:
        for state_slug in STATES:
            state_name = state_slug.replace("-", " ").title()
            scrape_status["message"] = f"Scraping {state_name}... ({inserted} saved)"

            url = f"https://www.ibba.org/state/{state_slug}/"
            try:
                resp = requests.get(url, headers=headers, timeout=20)
                if resp.status_code != 200:
                    continue
            except Exception as e:
                scrape_status["last_error"] = f"Request error on {state_name}: {str(e)}"
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            broker_links = soup.select("h4 a[href*='/broker-profile/']")

            for link_el in broker_links:
                name = link_el.get_text(strip=True)
                profile_url = link_el.get("href", "")

                h4 = link_el.parent
                firm = ""
                city = ""
                phone = ""

                sibling = h4.find_next_sibling()
                texts = []
                while sibling and sibling.name not in ["h4", "h3", "h2", "hr"]:
                    t = sibling.get_text(strip=True)
                    if t:
                        texts.append(t)
                    phone_link = sibling.find("a", href=lambda x: x and x.startswith("tel:"))
                    if phone_link and not phone:
                        phone = phone_link.get_text(strip=True)
                    sibling = sibling.find_next_sibling()

                for t in texts:
                    if "," in t and state_name.lower() in t.lower():
                        city = t.split(",")[0].strip()
                    elif t and not firm and t != name and "more details" not in t.lower():
                        firm = t

                try:
                    conn.execute(
                        "INSERT INTO brokers (name, firm, city, state, email, phone, website, profile_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (name, firm, city, state_name, "", phone, "", profile_url)
                    )
                    inserted += 1
                except Exception as e:
                    scrape_status["last_error"] = f"Insert error: {str(e)}"

            conn.commit()
            scrape_status["scraped"] = inserted
            time.sleep(1)

    except Exception as e:
        scrape_status["last_error"] = traceback.format_exc()
        scrape_status["message"] = f"Fatal error: {str(e)}"
    finally:
        try:
            conn.commit()
            conn.close()
        except:
            pass

    scrape_status["running"] = False
    scrape_status["total"] = inserted
    scrape_status["message"] = f"Done! Saved {inserted} brokers."

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

    try:
        conn = get_db()
        query = "SELECT * FROM brokers WHERE 1=1"
        params = []

        if search:
            query += " AND (name LIKE ? OR firm LIKE ? OR email LIKE ?)"
            params += [f"%{search}%", f"%{search}%", f"%{search}%"]
        if state:
            query += " AND state = ?"
            params.append(state)
        if city:
            query += " AND city LIKE ?"
            params.append(f"%{city}%")
        if email_only:
            query += " AND email != '' AND email IS NOT NULL"

        total = conn.execute(query.replace("SELECT *", "SELECT COUNT(*)"), params).fetchone()[0]
        query += f" ORDER BY state, name LIMIT {per_page} OFFSET {(page-1)*per_page}"
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return jsonify({"brokers": [dict(r) for r in rows], "total": total, "page": page, "pages": max(1, (total + per_page - 1) // per_page)})
    except Exception as e:
        return jsonify({"brokers": [], "total": 0, "page": 1, "pages": 1, "error": str(e)})

@app.route("/api/states")
def get_states():
    try:
        conn = get_db()
        rows = conn.execute("SELECT DISTINCT state FROM brokers WHERE state != '' AND state IS NOT NULL ORDER BY state").fetchall()
        conn.close()
        return jsonify([r["state"] for r in rows])
    except Exception as e:
        return jsonify([])

@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    if scrape_status["running"]:
        return jsonify({"error": "Scrape already running"}), 400
    # Re-init DB each scrape to be safe
    init_db()
    t = threading.Thread(target=scrape_ibba)
    t.daemon = True
    t.start()
    return jsonify({"message": "Scrape started"})

@app.route("/api/scrape/status")
def get_scrape_status():
    return jsonify(scrape_status)

@app.route("/api/debug")
def debug():
    """Quick diagnostic endpoint"""
    try:
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) FROM brokers").fetchone()[0]
        sample = conn.execute("SELECT name, state FROM brokers LIMIT 3").fetchall()
        conn.close()
        return jsonify({
            "db_path": DB_PATH,
            "db_exists": os.path.exists(DB_PATH),
            "broker_count": count,
            "sample": [dict(r) for r in sample],
            "last_error": scrape_status.get("last_error", "")
        })
    except Exception as e:
        return jsonify({"error": str(e), "db_path": DB_PATH, "db_exists": os.path.exists(DB_PATH)})

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
    state = request.args.get("state", "")
    conn = get_db()
    query = "SELECT name, firm, city, state, email, phone, profile_url FROM brokers WHERE 1=1"
    params = []
    if state:
        query += " AND state = ?"
        params.append(state)
    rows = conn.execute(query, params).fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Name", "Firm", "City", "State", "Email", "Phone", "Profile URL"])
    for r in rows:
        writer.writerow([r["name"], r["firm"], r["city"], r["state"], r["email"], r["phone"], r["profile_url"]])

    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=ibba_brokers.csv"})

@app.route("/api/stats")
def stats():
    try:
        conn = get_db()
        total = conn.execute("SELECT COUNT(*) FROM brokers").fetchone()[0]
        with_email = conn.execute("SELECT COUNT(*) FROM brokers WHERE email != ''").fetchone()[0]
        states = conn.execute("SELECT COUNT(DISTINCT state) FROM brokers").fetchone()[0]
        conn.close()
        return jsonify({"total": total, "with_email": with_email, "states": states})
    except:
        return jsonify({"total": 0, "with_email": 0, "states": 0})

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
