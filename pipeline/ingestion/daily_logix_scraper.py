"""
Ingestion stage — Westwood Daily Logix scraper.

Fetches today's drilling news headlines and bodies.
Falls back to fixture files when `use_fixtures=true` in settings.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from config import settings

logger = logging.getLogger(__name__)

_SOURCE = "daily_logix"
_BASE_URL = os.environ.get("DAILY_LOGIX_URL", "https://www.westwoodenergy.com/news")
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; RigLogixRouter/0.1; +https://github.com/riglogix)"
    )
}


def _make_id(url: str, title: str) -> str:
    digest = hashlib.sha256(f"{url}|{title}".encode()).hexdigest()
    return digest[:16]


def _fetch_live(max_items: int) -> list[dict]:
    """Scrape Westwood Energy news page for today's articles."""
    logger.info("Fetching live news from %s", _BASE_URL)
    resp = requests.get(_BASE_URL, headers=_HEADERS, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    articles = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    # Westwood Energy uses article cards — adapt selectors as needed
    for card in soup.select("article, .news-item, .post-item, .entry")[:max_items]:
        title_el = card.find(["h2", "h3", "h4"])
        link_el = card.find("a", href=True)
        body_el = card.find(["p", ".excerpt", ".summary", ".content"])

        title = title_el.get_text(strip=True) if title_el else ""
        url = link_el["href"] if link_el else ""
        if url and not url.startswith("http"):
            url = "https://www.westwoodenergy.com" + url
        body = body_el.get_text(strip=True) if body_el else ""

        if not title:
            continue

        # Fetch full article body if only excerpt available
        if url and len(body) < 200:
            try:
                article_resp = requests.get(url, headers=_HEADERS, timeout=20)
                article_resp.raise_for_status()
                article_soup = BeautifulSoup(article_resp.text, "lxml")
                content_el = article_soup.find(
                    ["article", ".article-body", ".post-content", "main"]
                )
                if content_el:
                    body = content_el.get_text(separator=" ", strip=True)[:3000]
            except Exception as e:
                logger.warning("Failed to fetch full article %s: %s", url, e)

        articles.append(
            {
                "id": _make_id(url, title),
                "source": _SOURCE,
                "title": title,
                "body": body,
                "source_url": url,
                "published_at": None,
                "fetched_at": fetched_at,
            }
        )

    logger.info("Fetched %d articles", len(articles))
    return articles


def _fetch_fixtures(fixtures_path: str) -> list[dict]:
    """Load news from local JSON fixture files (for testing / offline use)."""
    path = Path(fixtures_path)
    items = []
    fetched_at = datetime.now(timezone.utc).isoformat()

    for fixture_file in sorted(path.glob("*.json")):
        data = json.loads(fixture_file.read_text())
        if isinstance(data, list):
            for item in data:
                item.setdefault("source", _SOURCE)
                item.setdefault("fetched_at", fetched_at)
                item.setdefault("id", _make_id(item.get("source_url", ""), item["title"]))
                items.append(item)
        elif isinstance(data, dict):
            data.setdefault("source", _SOURCE)
            data.setdefault("fetched_at", fetched_at)
            data.setdefault("id", _make_id(data.get("source_url", ""), data["title"]))
            items.append(data)

    logger.info("Loaded %d fixture articles from %s", len(items), fixtures_path)
    return items


def fetch_news(run_id: str) -> list[dict]:
    """Entry point for the ingestion stage. Returns raw news dicts with run_id injected."""
    cfg = settings["ingestion"]
    use_fixtures = cfg.get("use_fixtures", False)
    max_items = cfg.get("max_news_per_run", 15)

    if use_fixtures:
        items = _fetch_fixtures(cfg["fixtures_path"])
    else:
        items = _fetch_live(max_items)

    for item in items:
        item["run_id"] = run_id

    return items[:max_items]
