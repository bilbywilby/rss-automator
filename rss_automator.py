#!/usr/bin/env python3
"""
RSS Automator — Production-grade feed polling service for Termux/Android.
Run: python3 ~/rss-automator/rss_automator.py [--query|--stats]
"""
from __future__ import annotations

import hashlib, logging, os, signal, sqlite3, threading, sys, time
from contextlib import closing
from datetime import datetime, timezone
from typing import Any

import feedparser, requests

# Configuration
RSS_HOME = os.path.expanduser(os.environ.get("RSS_HOME", "~/rss-automator"))
DB_FILE = os.path.join(RSS_HOME, "feeds.db")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "1800"))
HTTP_TIMEOUT = (10, 30)
MAX_BACKOFF = 21600
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
USER_AGENT = "rss-automator/1.0 (+https://github.com/yourname/rss-automator)"

# FEED DEFINITIONS — Modify this dictionary to add/remove feeds
TOPICS = {
    "Tech": [
        "https://news.ycombinator.com/rss",
        "https://arstechnica.com/index.xml",
        "https://techcrunch.com/feed/",
    ],
    "Finance": [
        "https://feeds.bloomberg.com/markets/news.rss",
        "https://www.marketwatch.com/rss/topstories/",
    ],
    "Science": [
        "https://arxiv.org/list/cs/new/rss",
        "https://phys.org/rss_feed/",
    ],
    "General": [
        "https://feeds.bbci.co.uk/news/world/rss.xml",
        "https://rss.ap.org/Public/TopStories.xml",
    ],
}

_log = logging.getLogger("rss-automator")
_stop = threading.Event()

def _handle_signal(signum: int, _frame: Any) -> None:
    _log.info("Received signal %s, exiting...", signal.Signals(signum).name)
    _stop.set()

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)

def init_db() -> sqlite3.Connection:
    os.makedirs(RSS_HOME, exist_ok=True)
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_url TEXT NOT NULL,
            title TEXT NOT NULL,
            link TEXT NOT NULL,
            pub_date TEXT,
            guid TEXT NOT NULL,
            topic TEXT NOT NULL,
            seen INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(feed_url, guid)
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entries_topic_seen ON entries(topic, seen)"
    )
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feed_state (
            feed_url TEXT PRIMARY KEY,
            etag TEXT,
            last_modified TEXT,
            consecutive_failures INTEGER DEFAULT 0,
            next_poll_at REAL DEFAULT 0
        )
    """)
    conn.commit()
    return conn

def entry_guid(entry: feedparser.FeedParserDict, feed_url: str) -> str:
    raw = entry.get("id") or entry.get("link")
    if raw:
        return str(raw)[:512]
    basis = "|".join([feed_url, entry.get("title") or "", entry.get("published") or ""])
    return "sha256:" + hashlib.sha256(basis.encode("utf-8")).hexdigest()

def load_feed_state(conn: sqlite3.Connection, feed_url: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT etag, last_modified, consecutive_failures, next_poll_at "
        "FROM feed_state WHERE feed_url = ?", (feed_url,)
    ).fetchone()
    if row is None:
        conn.execute("INSERT INTO feed_state (feed_url) VALUES (?)", (feed_url,))
        conn.commit()
        return {"etag": None, "last_modified": None, "consecutive_failures": 0, "next_poll_at": 0.0}
    return {"etag": row[0], "last_modified": row[1], "consecutive_failures": row[2] or 0, "next_poll_at": row[3] or 0.0}

def record_success(conn: sqlite3.Connection, feed_url: str, etag: str | None, last_modified: str | None) -> None:
    conn.execute(
        "UPDATE feed_state SET etag=?, last_modified=?, consecutive_failures=0, next_poll_at=0 WHERE feed_url=?",
        (etag, last_modified, feed_url)
    )

def record_failure(conn: sqlite3.Connection, feed_url: str, failures: int) -> float:
    delay = min(60 * (2 ** max(failures - 1, 0)), MAX_BACKOFF)
    conn.execute(
        "UPDATE feed_state SET consecutive_failures=?, next_poll_at=? WHERE feed_url=?",
        (failures, time.time() + delay, feed_url)
    )
    conn.commit()
    return delay

def poll_feed(session: requests.Session, conn: sqlite3.Connection, feed_url: str, topic: str) -> int:
    state = load_feed_state(conn, feed_url)
    if state["next_poll_at"] > time.time():
        return 0

    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8"
    }
    if state["etag"]:
        headers["If-None-Match"] = state["etag"]
    if state["last_modified"]:
        headers["If-Modified-Since"] = state["last_modified"]

    try:
        resp = session.get(feed_url, headers=headers, timeout=HTTP_TIMEOUT)
        if resp.status_code == 304:
            record_success(conn, feed_url, state["etag"], state["last_modified"])
            return 0
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        delay = record_failure(conn, feed_url, state["consecutive_failures"] + 1)
        _log.warning("%s: %s, backing off %ds", feed_url, exc, delay)
        return 0

    parsed = feedparser.parse(resp.content)
    if parsed.bozo and not parsed.entries:
        delay = record_failure(conn, feed_url, state["consecutive_failures"] + 1)
        _log.warning("%s: parse error, backing off %ds", feed_url, delay)
        return 0

    record_success(conn, feed_url, resp.headers.get("ETag"), resp.headers.get("Last-Modified"))

    inserted = 0
    for entry in parsed.entries:
        title = entry.get("title")
        link = entry.get("link")
        if not title or not link:
            continue
        pub_date = entry.get("published") or entry.get("updated") or datetime.now(timezone.utc).isoformat()
        guid = entry_guid(entry, feed_url)
        cursor = conn.execute(
            "INSERT INTO entries (feed_url, title, link, pub_date, guid, topic, seen) "
            "VALUES (?, ?, ?, ?, ?, ?, 0) ON CONFLICT(feed_url, guid) DO NOTHING",
            (feed_url, title, link, pub_date, guid, topic)
        )
        inserted += cursor.rowcount

    if inserted > 0:
        _log.info("Inserted %d entries from %s", inserted, feed_url)
    return inserted

def run_cycle(session: requests.Session, conn: sqlite3.Connection) -> int:
    total = 0
    for topic, urls in TOPICS.items():
        if _stop.is_set():
            break
        for feed_url in urls:
            if _stop.is_set():
                break
            total += poll_feed(session, conn, feed_url, topic)
    return total

def query_unread(limit: int = 50, topic: str | None = None) -> list[tuple]:
    conn = sqlite3.connect(DB_FILE)
    if topic:
        rows = conn.execute(
            "SELECT feed_url, title, link, pub_date FROM entries WHERE seen=0 AND topic=? ORDER BY pub_date DESC LIMIT ?",
            (topic, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT feed_url, title, link, pub_date FROM entries WHERE seen=0 ORDER BY pub_date DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return rows

def get_stats() -> dict[str, int]:
    conn = sqlite3.connect(DB_FILE)
    total = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
    unread = conn.execute("SELECT COUNT(*) FROM entries WHERE seen=0").fetchone()[0]
    feeds = conn.execute("SELECT COUNT(*) FROM feed_state").fetchone()[0]
    conn.close()
    return {"total": total, "unread": unread, "feeds": feeds}

def main():
    logging.basicConfig(level=getattr(logging, LOG_LEVEL), format="%(asctime)s %(levelname)s: %(message)s", handlers=[logging.StreamHandler()])
    conn = init_db()
    _log.info("Starting RSS Automator (db=%s, interval=%ds)", DB_FILE, POLL_INTERVAL)

    try:
        with closing(conn), requests.Session() as session:
            session.headers.update({"User-Agent": USER_AGENT})
            while not _stop.is_set():
                try:
                    count = run_cycle(session, conn)
                    _log.info("Cycle complete: %d new entries", count)
                except Exception as exc:
                    _log.exception("Unexpected error during cycle")
                _stop.wait(POLL_INTERVAL)
    finally:
        _log.info("Shutdown complete")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--stats":
        s = get_stats()
        print(f"Total: {s['total']}, Unread: {s['unread']}, Feeds: {s['feeds']}")
    elif len(sys.argv) > 1 and sys.argv[1] == "--query":
        for f, t, l, p in query_unread(20):
            print(f"[{p}] {t}")
            print(f"  {l}")
    else:
        main()
