# IBBA Broker Scraper — Dumu Holdings

A web app to scrape, search, and manage IBBA broker contacts for deal sourcing outreach.

## Features
- Scrapes all brokers from ibba.org/find-a-business-broker
- Search and filter by name, firm, state, city
- Track email validation status (Bouncer)
- Flag contacts already in Reply.io sequences
- Add notes per broker
- Export filtered lists to CSV

## Local Setup

```bash
pip install -r requirements.txt
python app.py
```
Then open http://localhost:5000

## Deploy to Railway

### One-time setup:
1. Push this repo to GitHub
2. Go to railway.app → New Project → Deploy from GitHub repo
3. Select this repo
4. Railway auto-detects Python and deploys

### After that, to redeploy:
```bash
git add .
git commit -m "your message"
git push
```
Railway auto-redeploys on every push.

## Stack
- Python / Flask
- SQLite (auto-created on first run)
- BeautifulSoup4 for scraping
- Gunicorn for production serving

## Next integrations
- Bouncer API: add `BOUNCER_API_KEY` env var in Railway settings
- Reply.io API: add `REPLYIO_API_KEY` env var in Railway settings
