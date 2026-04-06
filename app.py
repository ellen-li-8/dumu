from flask import Flask, render_template, jsonify, request, Response
import os, requests, time, threading, csv, io, traceback, re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import psycopg2
import psycopg2.extras

app = Flask(__name__)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Startup check: warn clearly if DATABASE_URL is missing
if not DATABASE_URL:
    print("=" * 60)
    print("WARNING: DATABASE_URL is not set!")
    print("The app will start but all API routes will return 503")
    print("until a valid PostgreSQL connection is available.")
    print("Set DATABASE_URL in your Railway environment variables.")
    print("=" * 60)

db_available = False

scrape_status = {"running":False,"total":0,"scraped":0,"message":"Idle","last_error":""}
enrich_status = {"running":False,"total":0,"done":0,"message":"Idle"}

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

# Tokens that should never appear as a firm name or in a broker name
CREDENTIAL_TOKENS = {
    "cbi","mcbi","m&ami","mami","cm&ap","cmap","cbb","mba","cpa","cva","cfe",
    "cgma","dba","jd","esq","lcbb","lcbi","cepa","cvb","phd","ms","bs","ba","ma",
    "m","more details","view profile"
}

def clean_name(text):
    """Strip credential suffixes after a comma — keep human name only."""
    if not text:
        return ""
    return text.split(",")[0].strip()

def clean_firm(text):
    """Remove credential badge tokens from firm text."""
    if not text:
        return ""
    # If the entire string is a known credential, discard it
    if text.strip().lower() in CREDENTIAL_TOKENS:
        return ""
    # Strip individual tokens that are credentials
    tokens = re.split(r"\s+", text.strip())
    cleaned = [t for t in tokens if t.lower().strip(".,()") not in CREDENTIAL_TOKENS]
    result = " ".join(cleaned).strip()
    return result if result.lower() not in CREDENTIAL_TOKENS else ""

def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg2.connect(DATABASE_URL, connect_timeout=5)


def check_db():
    """Try to connect to the database. Returns True if successful."""
    global db_available
    try:
        conn = get_db()
        conn.close()
        db_available = True
        return True
    except Exception as e:
        print(f"DB connection failed: {e}")
        db_available = False
        return False


def require_db(f):
    """Decorator that returns 503 if database is not available."""
    from functools import wraps
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not db_available:
            if not check_db():
                return jsonify({"error": "Database unavailable — please try again shortly"}), 503
            init_db()
        return f(*args, **kwargs)
    return wrapper


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS brokers (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            firm TEXT NOT NULL DEFAULT '',
            state TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            website TEXT NOT NULL DEFAULT '',
            profile_url TEXT UNIQUE,
            bouncer_status TEXT NOT NULL DEFAULT 'unchecked',
            in_reply BOOLEAN NOT NULL DEFAULT FALSE,
            notes TEXT NOT NULL DEFAULT '',
            bio TEXT NOT NULL DEFAULT '',
            specialties TEXT NOT NULL DEFAULT '',
            enriched BOOLEAN NOT NULL DEFAULT FALSE,
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
        return "".join(chr(b ^ key) for b in enc[1:])
    except Exception:
        return ""

def scrape_profile(profile_url, headers):
    result = {"email":"","firm":"","website":"","phone":"","bio":"","specialties":""}
    try:
        resp = requests.get(profile_url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")

        # Phone — first tel: link on page
        phone_link = soup.find("a", href=lambda x: x and x.startswith("tel:"))
        if phone_link:
            result["phone"] = phone_link.get_text(strip=True)

        # Email — Cloudflare XOR cipher decoded from cdn-cgi email-protection URLs
        for el in soup.find_all("a", href=True):
            href = el.get("href", "")
            if "email-protection" in href and "#" in href:
                decoded = decode_cloudflare_email(href.split("#")[-1])
                if "@" in decoded and "ibba.org" not in decoded:
                    result["email"] = decoded
                    break
        # Fallback: data-cfemail attribute
        if not result["email"]:
            for el in soup.select("[data-cfemail]"):
                decoded = decode_cloudflare_email(el.get("data-cfemail", ""))
                if "@" in decoded and "ibba.org" not in decoded:
                    result["email"] = decoded
                    break

        # Website
        visit_link = soup.find("a", string=lambda s: s and "visit website" in s.lower())
        if visit_link:
            result["website"] = visit_link.get("href", "")

        # Firm — text node immediately after the "apartment" material icon
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
            domain = re.sub(r"https?://(www\.)?", "", result["website"]).split("/")[0].split(".")[0]
            result["firm"] = domain.replace("-"," ").replace("_"," ").title()

        # Bio — longest meaningful paragraph on page
        best = ""
        for p in soup.find_all("p"):
            text = p.get_text(strip=True)
            if (len(text) > 80
                    and "copyright" not in text.lower()
                    and "newsletter" not in text.lower()
                    and "captcha" not in text.lower()
                    and text[:40].lower().count("ibba") == 0):
                if len(text) > len(best):
                    best = text
        result["bio"] = best[:800]

        # Specialties — <li> items under a "Specialty Areas" heading
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

def scrape_ibba():
    global scrape_status
    scrape_status.update({"running":True,"message":"Starting...","scraped":0,"last_error":""})
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    conn = None
    inserted = 0
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("DELETE FROM brokers")
        conn.commit()

        for state_slug in STATES:
            state_name = state_slug.replace("-"," ").title()
            scrape_status["message"] = f"Scraping {state_name}... ({inserted} saved)"

            try:
                resp = requests.get(f"https://www.ibba.org/state/{state_slug}/", headers=headers, timeout=20)
                if resp.status_code != 200:
                    continue
            except Exception as e:
                scrape_status["last_error"] = str(e)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for link_el in soup.select("h4 a[href*='/broker-profile/']"):
                raw_name = link_el.get_text(strip=True)
                name = clean_name(raw_name)  # strip credential suffixes
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
                    if t and not firm and t != raw_name and "more details" not in t.lower():
                        firm = clean_firm(t)

                try:
                    cur.execute("""
                        INSERT INTO brokers (name, firm, state, email, phone, website, profile_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (profile_url) DO UPDATE SET
                            name=EXCLUDED.name, firm=EXCLUDED.firm, state=EXCLUDED.state, phone=EXCLUDED.phone
                    """, (name, firm, state_name, "", phone, "", profile_url))
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
            if conn:
                conn.commit()
                cur.close()
                conn.close()
        except Exception:
            pass
        scrape_status.update({"running":False,"total":inserted,"message":f"Done! Saved {inserted} brokers."})

def enrich_profiles_worker(limit, state="", search=""):
    """
    Enrich top N unenriched brokers from the current filtered view.
    Note: specialty filter is intentionally excluded here — unenriched brokers
    don't have specialties yet, so filtering by specialty before enriching
    would always return 0. Filter by state/search only.
    """
    global enrich_status
    enrich_status.update({"running":True,"done":0,"total":0,"message":"Starting enrichment..."})
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        q = "SELECT id, profile_url FROM brokers WHERE enriched=FALSE AND profile_url IS NOT NULL AND profile_url != ''"
        p = []
        if state:
            q += " AND state = %s"; p.append(state)
        if search:
            q += " AND (name ILIKE %s OR firm ILIKE %s OR email ILIKE %s)"
            p += [f"%{search}%"] * 3
        q += " ORDER BY state, name LIMIT %s"; p.append(limit)

        cur.execute(q, p)
        rows = list(cur.fetchall())
        enrich_status["total"] = len(rows)

        if not rows:
            enrich_status.update({"running":False,"message":"No unenriched profiles found for this filter."})
            return

        for i, row in enumerate(rows):
            enrich_status["message"] = f"Enriching {i+1} of {len(rows)}..."
            enrich_status["done"] = i
            try:
                data = scrape_profile(row["profile_url"], headers)
                cur.execute("""
                    UPDATE brokers SET
                        email = %s,
                        phone = CASE WHEN phone = '' THEN %s ELSE phone END,
                        website = %s,
                        firm = CASE WHEN firm = '' THEN %s ELSE firm END,
                        bio = %s,
                        specialties = %s,
                        enriched = TRUE
                    WHERE id = %s
                """, (
                    data["email"], data["phone"], data["website"], data["firm"],
                    data.get("bio",""), data.get("specialties",""), row["id"]
                ))
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
        except Exception:
            pass
        enrich_status["running"] = False
        enrich_status["message"] = f"Done! Enriched {enrich_status['done']} profiles."

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')
JUNK_EMAIL_DOMAINS = {
    "example.com","sentry.io","ibba.org","wordpress.org","w3.org","schema.org",
    "google.com","facebook.com","twitter.com","linkedin.com","instagram.com",
    "youtube.com","gravatar.com","wixpress.com","squarespace.com","cloudflare.com",
    "googleapis.com","gstatic.com","wpengine.com","wp.com","mailchimp.com",
    "constantcontact.com","hubspot.com","salesforce.com","zendesk.com",
}

def scrape_website_for_contact(url, broker_name, headers):
    """Visit a company website and its contact page to find email and phone."""
    result = {"email":"","phone":""}
    if not url:
        return result
    if not url.startswith("http"):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.replace("www.","")
    except Exception:
        return result

    pages_to_try = [url]
    base = f"{parsed.scheme}://{parsed.netloc}"
    for suffix in ["/contact","/contact-us","/about","/about-us","/team","/our-team"]:
        pages_to_try.append(base + suffix)

    name_parts = [p.lower() for p in broker_name.split() if len(p) > 2]
    all_emails = []
    all_phones = []

    for page_url in pages_to_try[:4]:
        try:
            resp = requests.get(page_url, headers=headers, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                continue
            text = resp.text
            for email in EMAIL_RE.findall(text):
                edom = email.lower().split("@")[1]
                if edom in JUNK_EMAIL_DOMAINS:
                    continue
                if domain in edom or edom in domain:
                    all_emails.insert(0, email)
                else:
                    all_emails.append(email)
            for phone in PHONE_RE.findall(text):
                digits = re.sub(r'\D','',phone)
                if 10 <= len(digits) <= 11:
                    all_phones.append(phone.strip())
        except Exception:
            continue
        time.sleep(0.4)

    # Prefer email matching broker name on company domain
    for email in all_emails:
        local = email.lower().split("@")[0]
        if any(part in local for part in name_parts):
            result["email"] = email
            break
    if not result["email"]:
        for email in all_emails:
            if domain in email.lower():
                result["email"] = email
                break
    if all_phones:
        result["phone"] = all_phones[0]
    return result


def search_web_for_broker(name, firm, state, headers):
    """Search DuckDuckGo for a broker's contact info when IBBA doesn't have it."""
    result = {"email":"","phone":"","website":""}
    if not name:
        return result

    query = f'"{name}" {firm} {state} business broker'
    try:
        resp = requests.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers=headers, timeout=15
        )
        if resp.status_code != 200:
            return result
        soup = BeautifulSoup(resp.text, "html.parser")

        name_parts = [p.lower() for p in name.split() if len(p) > 2]
        firm_lower = firm.lower().strip() if firm else ""

        # Extract emails from result snippets — only if broker name appears nearby
        for snippet in soup.select(".result__snippet"):
            text = snippet.get_text()
            context_lower = text.lower()
            if not any(part in context_lower for part in name_parts):
                continue
            for email in EMAIL_RE.findall(text):
                edom = email.lower().split("@")[1]
                if edom not in JUNK_EMAIL_DOMAINS:
                    result["email"] = email
                    break
            if result["email"]:
                break

        # Find company website from result links
        for link in soup.select(".result__a"):
            href = link.get("href","")
            link_text = link.get_text(strip=True).lower()
            parent_text = (link.parent.get_text() if link.parent else "").lower()
            # Skip IBBA, social media, and directory sites
            skip = ["ibba.org","linkedin.com","facebook.com","yelp.com","bbb.org",
                    "mapquest.com","yellowpages.com","duckduckgo.com","wikipedia.org"]
            if any(s in href.lower() for s in skip):
                continue
            # Check if firm name or broker name appears in result
            if (firm_lower and any(w in link_text or w in parent_text for w in firm_lower.split() if len(w)>3)) \
               or any(part in parent_text for part in name_parts):
                url_match = re.search(r'https?://[^\s"<>&]+', href)
                if url_match:
                    result["website"] = url_match.group().split("?")[0].rstrip("/")
                    break

    except Exception:
        pass
    return result


def web_enrich_worker():
    """Third enrichment pass: find missing email/phone via company websites and web search.
    Only saves data when confident it belongs to the right person."""
    global enrich_status
    enrich_status.update({"running":True,"done":0,"total":0,"message":"Web enrichment: finding missing contact info..."})
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    conn = None
    found = 0
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, name, firm, state, website, phone
            FROM brokers
            WHERE enriched=TRUE AND (email = '' OR email IS NULL)
            ORDER BY state, name
        """)
        rows = list(cur.fetchall())
        enrich_status["total"] = len(rows)

        if not rows:
            enrich_status.update({"running":False,"message":"All brokers already have emails."})
            return

        for i, row in enumerate(rows):
            enrich_status["message"] = f"Web enriching {i+1} of {len(rows)}: {row['name']}..."
            enrich_status["done"] = i

            email = ""
            phone = ""
            website = row.get("website","")

            # Step 1: scrape company website if we have one
            if website:
                data = scrape_website_for_contact(website, row["name"], headers)
                email = data.get("email","")
                if not row.get("phone",""):
                    phone = data.get("phone","")

            # Step 2: web search fallback
            if not email and row.get("name",""):
                search_data = search_web_for_broker(row["name"], row.get("firm",""), row.get("state",""), headers)
                if not email:
                    email = search_data.get("email","")
                if not website and search_data.get("website"):
                    website = search_data["website"]
                    # Scrape newly found website for email
                    if not email:
                        data2 = scrape_website_for_contact(website, row["name"], headers)
                        email = data2.get("email","")
                        if not phone and not row.get("phone",""):
                            phone = data2.get("phone","")
                if not phone and not row.get("phone",""):
                    phone = search_data.get("phone","")

            # Save anything we found
            updates = []
            params = []
            if email:
                updates.append("email = %s"); params.append(email)
                found += 1
            if phone and not row.get("phone",""):
                updates.append("phone = CASE WHEN phone = '' THEN %s ELSE phone END"); params.append(phone)
            if website and not row.get("website",""):
                updates.append("website = %s"); params.append(website)
            if updates:
                params.append(row["id"])
                cur.execute(f"UPDATE brokers SET {', '.join(updates)} WHERE id = %s", params)
                conn.commit()

            enrich_status["done"] = i + 1
            time.sleep(1.2)

    except Exception as e:
        enrich_status["message"] = f"Web enrich error: {str(e)}"
    finally:
        try:
            if conn:
                conn.commit(); cur.close(); conn.close()
        except Exception:
            pass
        enrich_status["running"] = False
        enrich_status["message"] = f"Done! Found {found} additional emails via web search."


def build_broker_query(search, state, specialty, email_only):
    """Single source of truth for broker filter query — used by list and export."""
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

# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/brokers")
@require_db
def get_brokers():
    search     = request.args.get("search","").strip()
    state      = request.args.get("state","").strip()
    specialty  = request.args.get("specialty","").strip()
    email_only = request.args.get("email_only","false") == "true"
    page       = max(1, int(request.args.get("page",1)))
    per_page   = 50

    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        q, p = build_broker_query(search, state, specialty, email_only)

        # Count
        count_q = q.replace("SELECT *", "SELECT COUNT(*)", 1)
        cur.execute(count_q, p)
        total = cur.fetchone()["count"]

        # Page
        q += " ORDER BY state, name LIMIT %s OFFSET %s"
        p += [per_page, (page-1)*per_page]
        cur.execute(q, p)
        rows = cur.fetchall()
        cur.close(); conn.close()

        return jsonify({
            "brokers": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "pages": max(1, (total + per_page - 1) // per_page)
        })
    except Exception as e:
        return jsonify({"brokers":[],"total":0,"page":1,"pages":1,"error":str(e)})

@app.route("/api/states")
@require_db
def get_states():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT state FROM brokers WHERE state != '' ORDER BY state")
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify([r[0] for r in rows])
    except Exception:
        return jsonify([])

@app.route("/api/specialties")
@require_db
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
                    counts[s] = counts.get(s, 0) + 1
        return jsonify(sorted(counts.keys(), key=lambda x: -counts[x]))
    except Exception:
        return jsonify([])

@app.route("/api/stats")
@require_db
def stats():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM brokers"); total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM brokers WHERE email != ''"); with_email = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT state) FROM brokers"); states = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM brokers WHERE enriched=TRUE"); enriched = cur.fetchone()[0]
        cur.close(); conn.close()
        return jsonify({"total":total,"with_email":with_email,"states":states,"enriched":enriched})
    except Exception:
        return jsonify({"total":0,"with_email":0,"states":0,"enriched":0})

@app.route("/api/scrape", methods=["POST"])
@require_db
def start_scrape():
    if scrape_status["running"]:
        return jsonify({"error":"Scrape already running"}), 400
    init_db()
    threading.Thread(target=scrape_ibba, daemon=True).start()
    return jsonify({"message":"Scrape started"})

@app.route("/api/scrape/status")
def get_scrape_status():
    return jsonify(scrape_status)

@app.route("/api/enrich", methods=["POST"])
@require_db
def start_enrich():
    if enrich_status["running"]:
        return jsonify({"error":"Enrichment already running"}), 400
    data    = request.json or {}
    limit   = int(data.get("limit", 50))
    state   = data.get("state", "")
    search  = data.get("search", "")
    # Note: specialty intentionally not passed — unenriched brokers have no specialties yet
    threading.Thread(target=enrich_profiles_worker, args=(limit, state, search), daemon=True).start()
    return jsonify({"message":f"Enriching top {limit} unenriched brokers"})

@app.route("/api/enrich/status")
def get_enrich_status():
    return jsonify(enrich_status)

@app.route("/api/brokers/<int:broker_id>", methods=["PATCH"])
@require_db
def update_broker(broker_id):
    data = request.json or {}
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
@require_db
def export_csv():
    search     = request.args.get("search","").strip()
    state      = request.args.get("state","").strip()
    specialty  = request.args.get("specialty","").strip()
    email_only = request.args.get("email_only","false") == "true"

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    q, p = build_broker_query(search, state, specialty, email_only)
    q = q.replace("SELECT *", "SELECT name,firm,state,email,phone,website,profile_url,specialties,bio", 1)
    q += " ORDER BY state, name"
    cur.execute(q, p)
    rows = cur.fetchall()
    cur.close(); conn.close()

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["Name","Firm","State","Email","Phone","Website","Profile URL","Specialties","Bio"])
    for r in rows:
        w.writerow([r["name"],r["firm"],r["state"],r["email"],r["phone"],
                    r["website"],r["profile_url"],r["specialties"],r["bio"]])
    return Response(out.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition":"attachment;filename=ibba_brokers.csv"})

@app.route("/api/debug")
@require_db
def debug():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM brokers"); count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM brokers WHERE enriched=TRUE"); enriched = cur.fetchone()[0]
        cur.execute("SELECT name, state, email FROM brokers WHERE email != '' LIMIT 3")
        sample = cur.fetchall()
        cur.close(); conn.close()
        return jsonify({
            "broker_count": count,
            "enriched_count": enriched,
            "sample_with_email": [{"name":r[0],"state":r[1],"email":r[2]} for r in sample],
            "db": "postgresql"
        })
    except Exception as e:
        return jsonify({"error":str(e)})

def _auto_scrape_and_enrich():
    """On startup, wait for DB, then auto-scrape and enrich if the table is empty."""
    # Wait for DB to become available (retry every 3s for up to 60s)
    for _ in range(20):
        if check_db():
            break
        time.sleep(3)
    else:
        print("Could not connect to database after 60s — giving up auto-scrape.")
        return

    init_db()

    # Check if brokers table is empty
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM brokers")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Auto-scrape check failed: {e}")
        return

    if count > 0:
        print(f"Database already has {count} brokers — skipping auto-scrape.")
        return

    print("Database is empty — starting auto-scrape...")
    scrape_ibba()
    print("Auto-scrape complete — starting IBBA profile enrichment...")
    enrich_profiles_worker(limit=10000)
    print("IBBA enrichment complete — starting web enrichment for missing data...")
    web_enrich_worker()
    print("All enrichment complete.")

# Run in background so gunicorn can bind the port immediately for healthcheck
threading.Thread(target=_auto_scrape_and_enrich, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
