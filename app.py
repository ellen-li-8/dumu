from flask import Flask, render_template, jsonify, request, Response
import os
import requests
from bs4 import BeautifulSoup
import time
import threading
import csv
import io
import traceback
import re
import psycopg2
import psycopg2.extras

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")

scrape_status = {
    "running": False, "total": 0, "scraped": 0,
    "message": "Idle", "last_error": ""
}
enrich_status = {
    "running": False, "total": 0, "done": 0, "message": "Idle"
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
    "cbi","mcbi","m&ami","mami","cm&ap","cmap","cbb","mba","cpa","cva","cfe","cgma",
    "dba","jd","esq","lcbb","lcbi","cepa","cvb","phd","ms","bs","ba","ma"
}

def clean_firm(text):
    if not text:
        return text
    tokens = re.split(r"\s+", text.strip())
    cleaned = [t for t in tokens if t.lower().strip(".,") not in CREDENTIALS]
    result = " ".join(cleaned).strip()
    return result if result and result.lower() not in CREDENTIALS else ""

def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS brokers (
            id SERIAL PRIMARY KEY,
            name TEXT,
            firm TEXT DEFAULT '',
            state TEXT DEFAULT '',
            email TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            website TEXT DEFAULT '',
            profile_url TEXT,
            bouncer_status TEXT DEFAULT 'unchecked',
            in_reply INTEGER DEFAULT 0,
            notes TEXT DEFAULT '',
            bio TEXT DEFAULT '',
            specialties TEXT DEFAULT '',
            enriched INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def decode_cloudflare_email(encoded):
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return ''.join(chr(b ^ key) for b in enc[1:])
    except:
        return ""

def scrape_profile(profile_url, headers):
    result = {"email":"","firm":"","website":"","phone":"","bio":"","specialties":""}
    try:
        resp = requests.get(profile_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")

        phone_link = soup.find("a", href=lambda x: x and x.startswith("tel:"))
        if phone_link:
            result["phone"] = phone_link.get_text(strip=True)

        for el in soup.select("a[href*='email-protection']"):
            encoded = el.get("href","").split("#")[-1]
            decoded = decode_cloudflare_email(encoded)
            if "@" in decoded:
                result["email"] = decoded
                break
        if not result["email"]:
            for el in soup.select("[data-cfemail]"):
                decoded = decode_cloudflare_email(el.get("data-cfemail",""))
                if "@" in decoded:
                    result["email"] = decoded
                    break

        visit_link = soup.find("a", string=lambda s: s and "visit website" in s.lower())
        if visit_link:
            result["website"] = visit_link.get("href","")

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

        if not result["firm"] and result["website"]:
            domain = re.sub(r'https?://(www\.)?', '', result["website"]).split('/')[0].split('.')[0]
            result["firm"] = domain.replace("-"," ").replace("_"," ").title()

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

        specialties = []
        for heading in soup.find_all(["h4","h5","h3","strong"]):
            if "specialty" in heading.get_text(strip=True).lower():
                ul = heading.find_next("ul")
                if ul:
                    specialties = [li.get_text(strip=True) for li in ul.find_all("li") if li.get_text(strip=True)]
                break
        result["specialties"] = ", ".join(specialties)

    except Exception:
        pass
    return result

def scrape_ibba():
    global scrape_status
    scrape_status.update({"running":True,"message":"Starting...","scraped":0,"last_error":""})
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    conn = get_db()
    cur = conn.cursor()
    inserted = 0

    try:
        # Clear existing data for fresh scrape
        cur.execute("DELETE FROM brokers")
        conn.commit()

        for state_slug in STATES:
            state_name = state_slug.replace("-"," ").title()
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
                profile_url = link_el.get("href","")
                h4 = link_el.parent
                firm = ""
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
                    if state_name.lower() in t.lower():
                        continue
                    if t and not firm and t != name and t.lower().strip() not in SKIP_FIRMS and "more details" not in t.lower():
                        firm = clean_firm(t)

                try:
                    cur.execute(
                        "INSERT INTO brokers (name,firm,state,email,phone,website,profile_url) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (name, firm, state_name, "", phone, "", profile_url)
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
            cur.close()
            conn.close()
        except:
            pass

    scrape_status.update({"running":False,"total":inserted,"message":f"Done! Saved {inserted} brokers."})

def enrich_profiles_worker(limit, state="", specialty="", search=""):
    global enrich_status
    enrich_status.update({"running":True,"done":0,"total":0,"message":"Starting enrichment..."})
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Build query matching current view filters, unenriched only
        q = "SELECT id, profile_url FROM brokers WHERE enriched=0 AND profile_url != ''"
        p = []
        if state:
            q += " AND state = %s"; p.append(state)
        if specialty:
            q += " AND specialties ILIKE %s"; p.append(f"%{specialty}%")
        if search:
            q += " AND (name ILIKE %s OR firm ILIKE %s OR email ILIKE %s)"
            p += [f"%{search}%"]*3
        q += " ORDER BY state, name LIMIT %s"; p.append(limit)
        cur.execute(q, p)
        rows = list(cur.fetchall())
        enrich_status["total"] = len(rows)

        if not rows:
            enrich_status.update({"running":False,"message":"No unenriched profiles found."})
            return

        for i, row in enumerate(rows):
            enrich_status["message"] = f"Enriching {i+1} of {len(rows)}..."
            enrich_status["done"] = i
            try:
                data = scrape_profile(row["profile_url"], headers)
                cur.execute("""
                    UPDATE brokers SET
                        email=%s,
                        phone=COALESCE(NULLIF(phone,''),%s),
                        website=%s,
                        firm=COALESCE(NULLIF(firm,''),%s),
                        bio=%s,
                        specialties=%s,
                        enriched=1
                    WHERE id=%s
                """, (data["email"], data["phone"], data["website"], data["firm"],
                      data.get("bio",""), data.get("specialties",""), row["id"]))
                conn.commit()
            except Exception:
                pass
            enrich_status["done"] = i + 1
            time.sleep(0.8)

    except Exception as e:
        enrich_status["message"] = f"Error: {str(e)}"
    finally:
        try:
            if conn:
                conn.commit()
                cur.close()
                conn.close()
        except:
            pass
        enrich_status["running"] = False
        enrich_status["message"] = f"Done! Enriched {enrich_status['done']} profiles."

# ── Routes ──────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/brokers")
def get_brokers():
    search    = request.args.get("search","").strip()
    state     = request.args.get("state","").strip()
    specialty = request.args.get("specialty","").strip()
    email_only= request.args.get("email_only","false") == "true"
    page      = int(request.args.get("page",1))
    per_page  = 50

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        q = "SELECT * FROM brokers WHERE 1=1"
        p = []
        if search:
            q += " AND (name ILIKE %s OR firm ILIKE %s OR email ILIKE %s)"
            p += [f"%{search}%"]*3
        if state:
            q += " AND state = %s"
            p.append(state)
        if specialty:
            q += " AND specialties ILIKE %s"
            p.append(f"%{specialty}%")
        if email_only:
            q += " AND email != '' AND email IS NOT NULL"

        count_q = q.replace("SELECT *","SELECT COUNT(*)")
        cur.execute(count_q, p)
        total = cur.fetchone()["count"]

        q += f" ORDER BY state, name LIMIT {per_page} OFFSET {(page-1)*per_page}"
        cur.execute(q, p)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({"brokers":[dict(r) for r in rows],"total":total,"page":page,"pages":max(1,(total+per_page-1)//per_page)})
    except Exception as e:
        return jsonify({"brokers":[],"total":0,"page":1,"pages":1,"error":str(e)})

@app.route("/api/states")
def get_states():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT state FROM brokers WHERE state != '' ORDER BY state")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify([r[0] for r in rows])
    except:
        return jsonify([])

@app.route("/api/specialties")
def get_specialties():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT specialties FROM brokers WHERE specialties != '' AND specialties IS NOT NULL")
        rows = cur.fetchall()
        cur.close(); conn.close()
        counts = {}
        for row in rows:
            for s in row[0].split(","):
                s = s.strip()
                if s:
                    counts[s] = counts.get(s,0) + 1
        return jsonify(sorted(counts.keys(), key=lambda x: -counts[x]))
    except:
        return jsonify([])

@app.route("/api/stats")
def stats():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM brokers")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM brokers WHERE email != ''")
        with_email = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT state) FROM brokers")
        states = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM brokers WHERE enriched=1")
        enriched = cur.fetchone()[0]
        cur.close(); conn.close()
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
    data    = request.json or {}
    limit   = int(data.get("limit", 100))
    state   = data.get("state", "")
    specialty = data.get("specialty", "")
    search  = data.get("search", "")
    threading.Thread(target=enrich_profiles_worker, args=(limit, state, specialty, search), daemon=True).start()
    return jsonify({"message":f"Enrichment started for top {limit} of current view"})

@app.route("/api/enrich/status")
def get_enrich_status():
    return jsonify(enrich_status)

@app.route("/api/brokers/<int:broker_id>", methods=["PATCH"])
def update_broker(broker_id):
    data = request.json
    allowed = ["name","state","firm","email","phone","bio","specialties","notes","bouncer_status","in_reply"]
    conn = get_db()
    cur = conn.cursor()
    for field in allowed:
        if field in data:
            cur.execute(f"UPDATE brokers SET {field}=%s WHERE id=%s", (data[field], broker_id))
    conn.commit()
    cur.close(); conn.close()
    return jsonify({"ok":True})

@app.route("/api/export")
def export_csv():
    state     = request.args.get("state","")
    email_only= request.args.get("email_only","false") == "true"
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    q = "SELECT name,firm,state,email,phone,website,profile_url,specialties,bio FROM brokers WHERE 1=1"
    p = []
    if state:
        q += " AND state=%s"; p.append(state)
    if email_only:
        q += " AND email != ''"
    cur.execute(q,p)
    rows = cur.fetchall()
    cur.close(); conn.close()

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Name","Firm","State","Email","Phone","Website","Profile URL","Specialties","Bio"])
    for r in rows:
        w.writerow([r["name"],r["firm"],r["state"],r["email"],r["phone"],r["website"],r["profile_url"],r["specialties"],r["bio"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=ibba_brokers.csv"})

@app.route("/api/debug")
def debug():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM brokers")
        count = cur.fetchone()[0]
        cur.execute("SELECT name, state FROM brokers LIMIT 3")
        sample = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({"broker_count":count,"sample":[{"name":r[0],"state":r[1]} for r in sample],"db":"postgresql"})
    except Exception as e:
        return jsonify({"error":str(e)})

if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0", port=port, debug=False)
