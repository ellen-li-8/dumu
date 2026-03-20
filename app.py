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
import re

app = Flask(__name__)
DB_PATH = "/tmp/brokers.db"

scrape_status = {
    "running": False,
    "total": 0,
    "scraped": 0,
    "message": "Idle",
    "last_error": ""
}

enrich_status = {
    "running": False,
    "total": 0,
    "done": 0,
    "message": "Idle"
}

STATES = [
    "alabama","alaska","arizona","arkansas","california","colorado",
    "connecticut","delaware","florida","georgia","hawaii","idaho",
    "illinois","indiana","iowa","kansas","kentucky","louisiana",
    "maine","maryland","massachusetts","michigan","minnesota",
    "mississippi","missouri","montana","nebraska","nevada",
    "new-hampshire","new-jersey","new-mexico","new-york",
    "north-carolina","north-dakota","ohio","oklahoma","oregon",
    "pennsylvania","rhode-island","south-carolina","south-dakota",
    "tennessee","texas","utah","vermont","virginia","washington",
    "west-virginia","wisconsin","wyoming"
]

SKIP_FIRMS = {"cbi","mcbi","m&ami","mami","cm&ap","m","more details »","view profile",""}

CREDENTIALS = {
    "cbi","mcbi","m&ami","mami","cm&ap","cmap","cbb","cbb","mba","cpa","cva","cfe","cgma",
    "dba","jd","esq","lcbb","lcbi","cepa","cvb","phd","ms","bs","ba","ma"
}

def clean_name_credentials(text):
    """Remove credential suffixes from names — keep the human name only"""
    if not text:
        return text
    # Split on comma — credentials usually follow a comma
    parts = text.split(",")
    name = parts[0].strip()
    return name

def clean_firm(text):
    """Remove CBI/MCBI/credential badges that got scraped as firm name"""
    if not text:
        return text
    tokens = re.split(r"\s+", text.strip())
    cleaned = []
    for tok in tokens:
        if tok.lower().strip(".,") not in CREDENTIALS:
            cleaned.append(tok)
    result = " ".join(cleaned).strip()
    # If result is empty or only credential chars, return empty
    if not result or result.lower() in CREDENTIALS:
        return ""
    return result



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
            bio TEXT DEFAULT '',
            specialties TEXT DEFAULT '',
            enriched INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Add enriched column if upgrading from old schema
    try:
        conn.execute("ALTER TABLE brokers ADD COLUMN enriched INTEGER DEFAULT 0")
    except:
        pass
    try:
        conn.execute("ALTER TABLE brokers ADD COLUMN bio TEXT DEFAULT ''")
    except:
        pass
    try:
        conn.execute("ALTER TABLE brokers ADD COLUMN specialties TEXT DEFAULT ''")
    except:
        pass
    conn.commit()
    conn.close()

def decode_cloudflare_email(encoded):
    """Decode Cloudflare email obfuscation"""
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return ''.join(chr(b ^ key) for b in enc[1:])
    except:
        return ""

def scrape_profile(profile_url, headers):
    """Scrape a single broker profile page for email, firm, website, phone, bio"""
    result = {"email": "", "firm": "", "website": "", "phone": "", "bio": "", "specialties": ""}
    try:
        resp = requests.get(profile_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")

        # Phone
        phone_link = soup.find("a", href=lambda x: x and x.startswith("tel:"))
        if phone_link:
            result["phone"] = phone_link.get_text(strip=True)

        # Email — Cloudflare encoded
        for el in soup.select("a[href*='email-protection']"):
            encoded = el.get("href", "").split("#")[-1]
            decoded = decode_cloudflare_email(encoded)
            if "@" in decoded:
                result["email"] = decoded
                break
        if not result["email"]:
            for el in soup.select("[data-cfemail]"):
                decoded = decode_cloudflare_email(el.get("data-cfemail", ""))
                if "@" in decoded:
                    result["email"] = decoded
                    break

        # Website
        visit_link = soup.find("a", string=lambda s: s and "visit website" in s.lower())
        if visit_link:
            result["website"] = visit_link.get("href", "")

        # Firm — text after apartment material icon
        for el in soup.find_all(string=True):
            parent = el.parent
            if not parent or parent.name in ["script","style","a","title"]:
                continue
            text = el.strip()
            if not text or len(text) < 2:
                continue
            prev_sib = parent.find_previous_sibling()
            if prev_sib and "apartment" in str(prev_sib):
                cleaned = clean_firm(text)
                if cleaned:
                    result["firm"] = cleaned
                    break

        # Fallback firm from website domain
        if not result["firm"] and result["website"]:
            domain = re.sub(r'https?://(www\.)?', '', result["website"]).split('/')[0].split('.')[0]
            result["firm"] = domain.replace("-", " ").replace("_", " ").title()

        # Bio — longest meaningful paragraph
        best = ""
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if (len(text) > 80
                    and "copyright" not in text.lower()
                    and "newsletter" not in text.lower()
                    and "captcha" not in text.lower()
                    and text[:30].lower().count("ibba") == 0):
                if len(text) > len(best):
                    best = text
        result["bio"] = best[:800]

        # Specialties — listed as <li> items under a "Specialty Areas" heading
        specialties = []
        spec_heading = None
        for heading in soup.find_all(["h4","h5","h3","strong"]):
            if "specialty" in heading.get_text(strip=True).lower():
                spec_heading = heading
                break
        if spec_heading:
            ul = spec_heading.find_next("ul")
            if ul:
                specialties = [li.get_text(strip=True) for li in ul.find_all("li") if li.get_text(strip=True)]
        result["specialties"] = ", ".join(specialties)

    except Exception:
        pass
    return result

def scrape_ibba():
    global scrape_status
    scrape_status.update({"running": True, "message": "Starting...", "scraped": 0, "last_error": ""})

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
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
                scrape_status["last_error"] = str(e)
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
                while sibling and sibling.name not in ["h4","h3","h2","hr"]:
                    t = sibling.get_text(strip=True)
                    if t:
                        texts.append(t)
                    pl = sibling.find("a", href=lambda x: x and x.startswith("tel:"))
                    if pl and not phone:
                        phone = pl.get_text(strip=True)
                    sibling = sibling.find_next_sibling()

                for t in texts:
                    # Skip location lines (contain state name) and badge text
                    if state_name.lower() in t.lower():
                        continue
                    if t and not firm and t != name and t.lower().strip() not in SKIP_FIRMS and "more details" not in t.lower():
                        firm = clean_firm(t)

                try:
                    conn.execute(
                        "INSERT INTO brokers (name,firm,city,state,email,phone,website,profile_url) VALUES (?,?,?,?,?,?,?,?)",
                        (name, firm, "", state_name, "", phone, "", profile_url)
                    )
                    inserted += 1
                except Exception as e:
                    scrape_status["last_error"] = str(e)

            conn.commit()
            scrape_status["scraped"] = inserted
            time.sleep(1)

    except Exception as e:
        scrape_status["last_error"] = traceback.format_exc()
    finally:
        try:
            conn.commit()
            conn.close()
        except:
            pass

    scrape_status.update({"running": False, "total": inserted, "message": f"Done! Saved {inserted} brokers. Now run Enrich Profiles to get emails."})

def enrich_profiles_worker(limit):
    global enrich_status
    enrich_status.update({"running": True, "done": 0, "total": 0, "message": "Starting enrichment..."})
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        rows = list(conn.execute("SELECT id, profile_url FROM brokers WHERE enriched=0 AND profile_url != '' LIMIT ?", (limit,)).fetchall())
        enrich_status["total"] = len(rows)
        if not rows:
            enrich_status.update({"running": False, "message": "No unenriched profiles found."})
            return
        for i, row in enumerate(rows):
            enrich_status["message"] = f"Enriching {i+1} of {len(rows)}..."
            enrich_status["done"] = i
            try:
                data = scrape_profile(row["profile_url"], headers)
                conn.execute("UPDATE brokers SET email=?, phone=COALESCE(NULLIF(phone,''),?), website=?, firm=COALESCE(NULLIF(firm,''),?), bio=?, specialties=?, enriched=1 WHERE id=?", (data["email"], data["phone"], data["website"], data["firm"], data.get("bio",""), data.get("specialties",""), row["id"]))
                conn.commit()
            except Exception:
                pass
            enrich_status["done"] = i + 1
            time.sleep(0.8)
    except Exception as e:
        enrich_status["message"] = f"Error: {str(e)}"
    finally:
        try:
            if conn: conn.commit(); conn.close()
        except: pass
        enrich_status["running"] = False
        enrich_status["message"] = f"Done! Enriched {enrich_status['done']} profiles."

# ── Routes ──────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/brokers")
def get_brokers():
    search = request.args.get("search","").strip()
    state  = request.args.get("state","").strip()
    city   = request.args.get("city","").strip()
    email_only = request.args.get("email_only","false") == "true"
    specialty  = request.args.get("specialty","").strip()
    page   = int(request.args.get("page",1))
    per_page = 50

    try:
        conn = get_db()
        q = "SELECT * FROM brokers WHERE 1=1"
        p = []
        if search:
            q += " AND (name LIKE ? OR firm LIKE ? OR email LIKE ? OR phone LIKE ?)"
            p += [f"%{search}%"]*4
        if state:
            q += " AND state = ?"
            p.append(state)
        if city:
            q += " AND city LIKE ?"
            p.append(f"%{city}%")
        if email_only:
            q += " AND email != '' AND email IS NOT NULL"
        if specialty:
            q += " AND specialties LIKE ?"
            p.append(f"%{specialty}%")

        total = conn.execute(q.replace("SELECT *","SELECT COUNT(*)"), p).fetchone()[0]
        q += f" ORDER BY state, name LIMIT {per_page} OFFSET {(page-1)*per_page}"
        rows = conn.execute(q, p).fetchall()
        conn.close()
        return jsonify({"brokers":[dict(r) for r in rows],"total":total,"page":page,"pages":max(1,(total+per_page-1)//per_page)})
    except Exception as e:
        return jsonify({"brokers":[],"total":0,"page":1,"pages":1,"error":str(e)})

@app.route("/api/states")
def get_states():
    try:
        conn = get_db()
        rows = conn.execute("SELECT DISTINCT state FROM brokers WHERE state != '' ORDER BY state").fetchall()
        conn.close()
        return jsonify([r["state"] for r in rows])
    except:
        return jsonify([])

@app.route("/api/specialties")
def get_specialties():
    try:
        conn = get_db()
        rows = conn.execute("SELECT specialties FROM brokers WHERE specialties != '' AND specialties IS NOT NULL").fetchall()
        conn.close()
        counts = {}
        for row in rows:
            for s in row["specialties"].split(","):
                s = s.strip()
                if s:
                    counts[s] = counts.get(s, 0) + 1
        # Sort by frequency
        sorted_specs = sorted(counts.keys(), key=lambda x: -counts[x])
        return jsonify(sorted_specs)
    except Exception as e:
        return jsonify([])

@app.route("/api/stats")
def stats():
    try:
        conn = get_db()
        total     = conn.execute("SELECT COUNT(*) FROM brokers").fetchone()[0]
        with_email= conn.execute("SELECT COUNT(*) FROM brokers WHERE email != ''").fetchone()[0]
        states    = conn.execute("SELECT COUNT(DISTINCT state) FROM brokers").fetchone()[0]
        enriched  = conn.execute("SELECT COUNT(*) FROM brokers WHERE enriched=1").fetchone()[0]
        conn.close()
        return jsonify({"total":total,"with_email":with_email,"states":states,"enriched":enriched})
    except:
        return jsonify({"total":0,"with_email":0,"states":0,"enriched":0})

@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    if scrape_status["running"]:
        return jsonify({"error":"Scrape already running"}),400
    init_db()
    threading.Thread(target=scrape_ibba, daemon=True).start()
    return jsonify({"message":"Scrape started"})

@app.route("/api/scrape/status")
def get_scrape_status():
    return jsonify(scrape_status)

@app.route("/api/enrich", methods=["POST"])
def start_enrich():
    if enrich_status["running"]:
        return jsonify({"error":"Enrichment already running"}),400
    limit = int(request.json.get("limit", 100))
    threading.Thread(target=enrich_profiles_worker, args=(limit,), daemon=True).start()
    return jsonify({"message":f"Enrichment started for up to {limit} profiles"})

@app.route("/api/enrich/status")
def get_enrich_status():
    return jsonify(enrich_status)

@app.route("/api/brokers/<int:broker_id>", methods=["PATCH"])
def update_broker(broker_id):
    data = request.json
    conn = get_db()
    for field in ["name","state","firm","email","phone","bio","specialties","notes","bouncer_status","in_reply"]:
        if field in data:
            conn.execute(f"UPDATE brokers SET {field}=? WHERE id=?", (data[field], broker_id))
    conn.commit()
    conn.close()
    return jsonify({"ok":True})

@app.route("/api/export")
def export_csv():
    state = request.args.get("state","")
    email_only = request.args.get("email_only","false") == "true"
    conn = get_db()
    q = "SELECT name,firm,state,email,phone,website,profile_url,bio FROM brokers WHERE 1=1"
    p = []
    if state:
        q += " AND state=?"; p.append(state)
    if email_only:
        q += " AND email != ''"
    rows = conn.execute(q,p).fetchall()
    conn.close()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Name","Firm","State","Email","Phone","Website","Profile URL","Bio"])
    for r in rows:
        w.writerow([r["name"],r["firm"],r["state"],r["email"],r["phone"],r["website"],r["profile_url"],r["bio"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=ibba_brokers.csv"})

@app.route("/api/debug")
def debug():
    try:
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) FROM brokers").fetchone()[0]
        sample = conn.execute("SELECT name,state,email,firm FROM brokers WHERE email != '' LIMIT 3").fetchall()
        conn.close()
        return jsonify({"db_path":DB_PATH,"db_exists":os.path.exists(DB_PATH),"broker_count":count,"sample":[dict(r) for r in sample],"last_error":scrape_status.get("last_error","")})
    except Exception as e:
        return jsonify({"error":str(e)})

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0", port=port, debug=False)
