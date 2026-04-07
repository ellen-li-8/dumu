from flask import Flask, render_template, jsonify, request, Response
import os, requests, time, threading, csv, io, traceback, re
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# ── Database setup ────────────────────────────────────────
DATABASE_URL = (
    os.environ.get("DATABASE_URL") or
    os.environ.get("DATABASE_PRIVATE_URL") or
    os.environ.get("DATABASE_PUBLIC_URL") or ""
)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

APOLLO_API_KEY = os.environ.get("APOLLO_API_KEY", "")

# ── Global state ──────────────────────────────────────────
scrape_status  = {"running": False, "total": 0, "scraped": 0, "message": "Idle", "last_error": ""}
enrich_status  = {"running": False, "total": 0, "done": 0,    "message": "Idle"}

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

CREDENTIAL_TOKENS = {
    "cbi","mcbi","m&ami","mami","cm&ap","cmap","cbb","mba","cpa","cva","cfe",
    "cgma","dba","jd","esq","lcbb","lcbi","cepa","cvb","phd","ms","bs","ba","ma",
    "m","more details","view profile"
}

# ── Helpers ───────────────────────────────────────────────
def clean_name(text):
    if not text:
        return ""
    return text.split(",")[0].strip()

def clean_firm(text):
    if not text:
        return ""
    if text.strip().lower() in CREDENTIAL_TOKENS:
        return ""
    tokens = re.split(r"\s+", text.strip())
    cleaned = [t for t in tokens if t.lower().strip(".,()") not in CREDENTIAL_TOKENS]
    result = " ".join(cleaned).strip()
    return result if result.lower() not in CREDENTIAL_TOKENS else ""

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
    return conn

def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS brokers (
            id            SERIAL PRIMARY KEY,
            name          TEXT NOT NULL DEFAULT '',
            firm          TEXT NOT NULL DEFAULT '',
            state         TEXT NOT NULL DEFAULT '',
            email         TEXT NOT NULL DEFAULT '',
            phone         TEXT NOT NULL DEFAULT '',
            website       TEXT NOT NULL DEFAULT '',
            profile_url   TEXT UNIQUE,
            bouncer_status TEXT NOT NULL DEFAULT 'unchecked',
            in_reply      BOOLEAN NOT NULL DEFAULT FALSE,
            notes         TEXT NOT NULL DEFAULT '',
            bio           TEXT NOT NULL DEFAULT '',
            specialties   TEXT NOT NULL DEFAULT '',
            enriched      BOOLEAN NOT NULL DEFAULT FALSE,
            created_at    TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def decode_cloudflare_email(encoded):
    try:
        enc = bytes.fromhex(encoded)
        key = enc[0]
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""

# ── Apollo email lookup ───────────────────────────────────
def apollo_find_email(name, firm, state):
    """
    Use Apollo.io People Match API to find a verified email for a broker.
    Requires APOLLO_API_KEY env var set in Railway.
    Returns email string or empty string.
    """
    if not APOLLO_API_KEY or not name:
        return ""
    try:
        parts = name.strip().split()
        first = parts[0] if parts else ""
        last  = parts[-1] if len(parts) > 1 else ""
        payload = {
            "api_key":       APOLLO_API_KEY,
            "first_name":    first,
            "last_name":     last,
            "organization_name": firm or "",
            "reveal_personal_emails": False,
        }
        resp = requests.post(
            "https://api.apollo.io/v1/people/match",
            json=payload,
            timeout=10
        )
        if resp.status_code != 200:
            return ""
        data = resp.json()
        person = data.get("person") or {}
        # Prefer work email, fall back to personal
        email = person.get("email") or ""
        if not email:
            emails = person.get("email_addresses") or []
            for e in emails:
                val = e.get("email","")
                if val and "@" in val:
                    email = val
                    break
        return email
    except Exception:
        return ""

# ── IBBA profile scraper ──────────────────────────────────
def scrape_profile(profile_url, headers):
    result = {"email":"","firm":"","website":"","phone":"","bio":"","specialties":""}
    try:
        resp = requests.get(profile_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")

        # Phone
        phone_link = soup.find("a", href=lambda x: x and x.startswith("tel:"))
        if phone_link:
            result["phone"] = phone_link.get_text(strip=True)

        # Email — Cloudflare XOR decode
        for el in soup.find_all("a", href=True):
            href = el.get("href","")
            if "email-protection" in href and "#" in href:
                decoded = decode_cloudflare_email(href.split("#")[-1])
                if "@" in decoded and "ibba.org" not in decoded:
                    result["email"] = decoded
                    break
        if not result["email"]:
            for el in soup.select("[data-cfemail]"):
                decoded = decode_cloudflare_email(el.get("data-cfemail",""))
                if "@" in decoded and "ibba.org" not in decoded:
                    result["email"] = decoded
                    break

        # Website
        visit_link = soup.find("a", string=lambda s: s and "visit website" in s.lower())
        if visit_link:
            result["website"] = visit_link.get("href","")

        # Firm — text after apartment material icon
        for el in soup.find_all(string=True):
            parent = el.parent
            if not parent or parent.name in ["script","style","a","title"]:
                continue
            text = el.strip()
            if not text or len(text) < 2:
                continue
            prev = parent.find_previous_sibling()
            if prev and "apartment" in str(prev):
                cleaned = clean_firm(text)
                if cleaned:
                    result["firm"] = cleaned
                    break

        # Fallback firm from website domain
        if not result["firm"] and result["website"]:
            domain = re.sub(r"https?://(www\.)?","",result["website"]).split("/")[0].split(".")[0]
            result["firm"] = domain.replace("-"," ").replace("_"," ").title()

        # Bio — longest meaningful paragraph
        best = ""
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if (len(text) > 80
                    and "copyright"  not in text.lower()
                    and "newsletter" not in text.lower()
                    and "captcha"    not in text.lower()
                    and text[:40].lower().count("ibba") == 0):
                if len(text) > len(best):
                    best = text
        result["bio"] = best[:800]

        # Specialties
        for heading in soup.find_all(["h3","h4","h5","strong"]):
            if "specialty" in heading.get_text(strip=True).lower():
                ul = heading.find_next("ul")
                if ul:
                    items = [li.get_text(strip=True) for li in ul.find_all("li") if li.get_text(strip=True)]
                    result["specialties"] = ", ".join(items)
                break

    except Exception:
        pass
    return result

# ── Scrape all state pages ────────────────────────────────
def scrape_ibba():
    global scrape_status
    scrape_status.update({"running":True,"message":"Connecting to database...","scraped":0,"last_error":""})
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    conn = None
    inserted = 0
    try:
        conn = get_db()
        cur  = conn.cursor()
        scrape_status["message"] = "Clearing old data..."
        cur.execute("DELETE FROM brokers")
        conn.commit()

        for state_slug in STATES:
            state_name = state_slug.replace("-"," ").title()
            scrape_status["message"] = f"Scraping {state_name}... ({inserted} saved)"

            try:
                resp = requests.get(
                    f"https://www.ibba.org/state/{state_slug}/",
                    headers=headers, timeout=20
                )
                if resp.status_code != 200:
                    continue
            except Exception as e:
                scrape_status["last_error"] = str(e)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for link_el in soup.select("h4 a[href*='/broker-profile/']"):
                raw_name    = link_el.get_text(strip=True)
                name        = clean_name(raw_name)
                profile_url = link_el.get("href","")
                h4   = link_el.parent
                firm = ""
                phone= ""

                sibling = h4.find_next_sibling()
                texts   = []
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
                    if t and not firm and t != raw_name and "more details" not in t.lower():
                        firm = clean_firm(t)

                try:
                    cur.execute("""
                        INSERT INTO brokers (name, firm, state, email, phone, website, profile_url)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (profile_url) DO UPDATE SET
                            name=EXCLUDED.name, firm=EXCLUDED.firm,
                            state=EXCLUDED.state, phone=EXCLUDED.phone
                    """, (name, firm, state_name, "", phone, "", profile_url))
                    inserted += 1
                except Exception as e:
                    scrape_status["last_error"] = str(e)

            conn.commit()
            scrape_status["scraped"] = inserted
            time.sleep(1)

    except Exception as e:
        scrape_status["last_error"] = traceback.format_exc()
        scrape_status["message"] = f"Error: {str(e)}"
    finally:
        try:
            if conn:
                conn.commit()
                cur.close()
                conn.close()
        except Exception:
            pass
        scrape_status.update({
            "running": False,
            "total":   inserted,
            "message": f"Done! Saved {inserted} brokers. Now click Enrich Profiles."
        })

# ── Enrich profiles (IBBA + Apollo fallback) ─────────────
def enrich_profiles_worker(limit, state="", search=""):
    global enrich_status
    enrich_status.update({"running":True,"done":0,"total":0,"message":"Starting enrichment..."})
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    conn = None
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        q = "SELECT id, name, firm, state, profile_url FROM brokers WHERE enriched=FALSE AND profile_url IS NOT NULL AND profile_url != ''"
        p = []
        if state:
            q += " AND state = %s"; p.append(state)
        if search:
            q += " AND (name ILIKE %s OR firm ILIKE %s)"; p += [f"%{search}%"]*2
        q += " ORDER BY state, name LIMIT %s"; p.append(limit)

        cur.execute(q, p)
        rows = list(cur.fetchall())
        enrich_status["total"] = len(rows)

        if not rows:
            enrich_status.update({"running":False,"message":"No unenriched profiles found."})
            return

        for i, row in enumerate(rows):
            enrich_status["message"] = f"Enriching {i+1} of {len(rows)}: {row['name']}..."
            enrich_status["done"] = i
            try:
                # Step 1: scrape IBBA profile page
                data = scrape_profile(row["profile_url"], headers)

                # Step 2: if no email from IBBA, try Apollo
                if not data["email"] and APOLLO_API_KEY:
                    firm_for_lookup = data["firm"] or row.get("firm","")
                    apollo_email = apollo_find_email(row["name"], firm_for_lookup, row["state"])
                    if apollo_email:
                        data["email"] = apollo_email

                write_cur = conn.cursor()
                write_cur.execute("""
                    UPDATE brokers SET
                        email      = %s,
                        phone      = CASE WHEN phone='' THEN %s ELSE phone END,
                        website    = %s,
                        firm       = CASE WHEN firm='' THEN %s ELSE firm END,
                        bio        = %s,
                        specialties= %s,
                        enriched   = TRUE
                    WHERE id = %s
                """, (
                    data["email"], data["phone"], data["website"], data["firm"],
                    data.get("bio",""), data.get("specialties",""), row["id"]
                ))
                conn.commit()
                write_cur.close()
            except Exception as e:
                scrape_status["last_error"] = str(e)

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
        except Exception:
            pass
        enrich_status["running"] = False
        enrich_status["message"] = f"Done! Enriched {enrich_status['done']} profiles."

# ── Shared filter query builder ───────────────────────────
def build_broker_query(search, state, specialty, email_only):
    q = "SELECT * FROM brokers WHERE 1=1"
    p = []
    if search:
        q += " AND (name ILIKE %s OR firm ILIKE %s OR email ILIKE %s OR phone ILIKE %s)"
        p += [f"%{search}%"] * 4
    if state:
        q += " AND state = %s"; p.append(state)
    if specialty:
        q += " AND specialties ILIKE %s"; p.append(f"%{specialty}%")
    if email_only:
        q += " AND email != '' AND email IS NOT NULL"
    return q, p

# ── Routes ────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/health")
def health():
    """Railway healthcheck — always returns 200, never requires DB."""
    return jsonify({"status":"ok"}), 200

@app.route("/api/brokers")
def get_brokers():
    search     = request.args.get("search","").strip()
    state      = request.args.get("state","").strip()
    specialty  = request.args.get("specialty","").strip()
    email_only = request.args.get("email_only","false") == "true"
    page       = max(1, int(request.args.get("page", 1)))
    per_page   = 50
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        q, p = build_broker_query(search, state, specialty, email_only)
        count_q = q.replace("SELECT *","SELECT COUNT(*)", 1)
        cur.execute(count_q, p)
        total = cur.fetchone()["count"]
        q += " ORDER BY state, name LIMIT %s OFFSET %s"
        p += [per_page, (page-1)*per_page]
        cur.execute(q, p)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({
            "brokers": [dict(r) for r in rows],
            "total":   total,
            "page":    page,
            "pages":   max(1,(total+per_page-1)//per_page)
        })
    except Exception as e:
        return jsonify({"brokers":[],"total":0,"page":1,"pages":1,"error":str(e)})

@app.route("/api/states")
def get_states():
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT DISTINCT state FROM brokers WHERE state!='' ORDER BY state")
        rows = cur.fetchall(); cur.close(); conn.close()
        return jsonify([r[0] for r in rows])
    except Exception:
        return jsonify([])

@app.route("/api/specialties")
def get_specialties():
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT specialties FROM brokers WHERE specialties!='' AND specialties IS NOT NULL")
        rows = cur.fetchall(); cur.close(); conn.close()
        counts = {}
        for row in rows:
            for s in row[0].split(","):
                s = s.strip()
                if s: counts[s] = counts.get(s,0)+1
        return jsonify(sorted(counts.keys(), key=lambda x: -counts[x]))
    except Exception:
        return jsonify([])

@app.route("/api/stats")
def stats():
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM brokers");                     total      = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM brokers WHERE email!=''");     with_email = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT state) FROM brokers");        states     = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM brokers WHERE enriched=TRUE"); enriched   = cur.fetchone()[0]
        cur.close(); conn.close()
        return jsonify({"total":total,"with_email":with_email,"states":states,"enriched":enriched})
    except Exception as e:
        return jsonify({"total":0,"with_email":0,"states":0,"enriched":0,"error":str(e)})

@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    if scrape_status["running"]:
        return jsonify({"error":"Scrape already running"}), 400
    try:
        init_db()
    except Exception as e:
        return jsonify({"error": f"Database not ready: {str(e)}"}), 503
    threading.Thread(target=scrape_ibba, daemon=True).start()
    return jsonify({"message":"Scrape started"})

@app.route("/api/scrape/status")
def get_scrape_status():
    return jsonify(scrape_status)

@app.route("/api/enrich", methods=["POST"])
def start_enrich():
    if enrich_status["running"]:
        return jsonify({"error":"Enrichment already running"}), 400
    data   = request.json or {}
    limit  = int(data.get("limit", 50))
    state  = data.get("state","")
    search = data.get("search","")
    threading.Thread(target=enrich_profiles_worker, args=(limit,state,search), daemon=True).start()
    return jsonify({"message":f"Enriching top {limit} brokers"})

@app.route("/api/enrich/status")
def get_enrich_status():
    return jsonify(enrich_status)

@app.route("/api/brokers/<int:broker_id>", methods=["PATCH"])
def update_broker(broker_id):
    data    = request.json or {}
    allowed = ["name","state","firm","email","phone","bio","specialties","notes","bouncer_status","in_reply"]
    conn = get_db(); cur = conn.cursor()
    for field in allowed:
        if field in data:
            cur.execute(f"UPDATE brokers SET {field}=%s WHERE id=%s", (data[field], broker_id))
    conn.commit(); cur.close(); conn.close()
    return jsonify({"ok":True})

@app.route("/api/export")
def export_csv():
    search     = request.args.get("search","").strip()
    state      = request.args.get("state","").strip()
    specialty  = request.args.get("specialty","").strip()
    email_only = request.args.get("email_only","false") == "true"
    conn = get_db()
    cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    q, p = build_broker_query(search, state, specialty, email_only)
    q = q.replace("SELECT *","SELECT name,firm,state,email,phone,website,profile_url,specialties,bio",1)
    q += " ORDER BY state, name"
    cur.execute(q, p); rows = cur.fetchall(); cur.close(); conn.close()
    out = io.StringIO()
    w   = csv.writer(out)
    w.writerow(["Name","Firm","State","Email","Phone","Website","Profile URL","Specialties","Bio"])
    for r in rows:
        w.writerow([r["name"],r["firm"],r["state"],r["email"],r["phone"],
                    r["website"],r["profile_url"],r["specialties"],r["bio"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=ibba_brokers.csv"})

@app.route("/api/debug")
def debug():
    try:
        conn = get_db(); cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM brokers");                     total    = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM brokers WHERE enriched=TRUE"); enriched = cur.fetchone()[0]
        cur.execute("SELECT name,state,email FROM brokers WHERE email!='' LIMIT 3")
        sample = cur.fetchall(); cur.close(); conn.close()
        return jsonify({
            "broker_count":       total,
            "enriched_count":     enriched,
            "apollo_configured":  bool(APOLLO_API_KEY),
            "db_url_set":         bool(DATABASE_URL),
            "sample_with_email":  [{"name":r[0],"state":r[1],"email":r[2]} for r in sample]
        })
    except Exception as e:
        return jsonify({"error": str(e), "db_url_set": bool(DATABASE_URL)})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
