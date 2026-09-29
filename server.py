import asyncio, calendar, html, re, time, pathlib
from contextlib import asynccontextmanager
import feedparser, httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

def gnews(q):
    return f"https://news.google.com/rss/search?q={q}&hl=ar&gl=TD&ceid=TD:ar"

FEEDS = {
    "chad": [("أخبار تشاد", gnews("تشاد+when:2d"))],
    "world": [
        ("BBC عربي", "https://feeds.bbci.co.uk/arabic/rss.xml"),
        ("فرانس 24", "https://www.france24.com/ar/rss"),
        ("RFI", "https://www.rfi.fr/ar/rss"),
    ],
    "economy": [("اقتصاد", gnews("اقتصاد+when:2d"))],
    "sports": [("رياضة", gnews("رياضة+when:2d"))],
    "tech": [("تكنولوجيا", gnews("تكنولوجيا+when:2d"))],
}
REFRESH = 300
cache = {"news": {}, "weather": None, "rates": None, "updated": 0}

def clean(t, n=170):
    t = html.unescape(re.sub(r"<[^>]+>", " ", t or ""))
    t = re.sub(r"\s+", " ", t).strip()
    return t if len(t) <= n else t[:n].rsplit(" ", 1)[0] + "…"

async def fetch_feed(client, cat, name, url):
    try:
        r = await client.get(url, timeout=15, follow_redirects=True)
        feed = feedparser.parse(r.content)
    except Exception:
        return []
    out = []
    for e in feed.entries[:25]:
        ts = calendar.timegm(e.published_parsed) if e.get("published_parsed") else 0
        title = clean(e.get("title"), 200)
        src = (e.get("source") or {}).get("title") or name
        if src and title.endswith(" - " + src):
            title = title[: -len(src) - 3]
        out.append({"title": title, "summary": clean(e.get("summary")) if "google" not in url else "",
                    "source": src, "link": e.get("link"), "ts": ts, "category": cat})
    return out

async def refresh_news(client):
    news = {}
    for cat, feeds in FEEDS.items():
        res = await asyncio.gather(*[fetch_feed(client, cat, n, u) for n, u in feeds])
        seen, items = set(), []
        for it in sorted([i for r in res for i in r], key=lambda i: -i["ts"]):
            if it["title"] and it["link"] and it["title"] not in seen:
                seen.add(it["title"]); items.append(it)
        news[cat] = items[:40]
    if any(news.values()):
        cache["news"] = news

async def refresh_weather(client):
    try:
        r = await client.get("https://api.open-meteo.com/v1/forecast", params={
            "latitude": 12.13, "longitude": 15.05, "timezone": "Africa/Ndjamena",
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
            "daily": "temperature_2m_max,weather_code"}, timeout=15)
        cache["weather"] = r.json()
    except Exception:
        pass

async def refresh_rates(client):
    try:
        r = await client.get("https://open.er-api.com/v6/latest/USD", timeout=15)
        rt = r.json()["rates"]
        cache["rates"] = {"USD_XAF": rt["XAF"], "EUR_XAF": 655.957, "USD_EUR": rt["EUR"]}
    except Exception:
        pass

async def loop():
    async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0 LahzaNews"}) as c:
        while True:
            await asyncio.gather(refresh_news(c), refresh_weather(c), refresh_rates(c))
            cache["updated"] = int(time.time())
            await asyncio.sleep(REFRESH)

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(loop())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"])

@app.get("/api/news")
def news(category: str = "all"):
    n = cache["news"]
    items = [i for v in n.values() for i in v] if category == "all" else n.get(category, [])
    return {"updated": cache["updated"], "items": sorted(items, key=lambda i: -i["ts"])[:60]}

@app.get("/api/services")
def services():
    return {"weather": cache["weather"], "rates": cache["rates"], "updated": cache["updated"]}

@app.get("/")
def index():
    return FileResponse(pathlib.Path(__file__).parent / "index.html")

@app.get("/health")
def health():
    return {"ok": True, "updated": cache["updated"]}