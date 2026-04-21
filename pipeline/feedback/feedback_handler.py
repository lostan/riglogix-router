"""
Feedback stage — parse and persist seller feedback from email replies.

The seller replies to the digest email with lines like:
  #1: 4 — bom fit, mas cliente está em freeze
  #3: 2

Format: #{rank}: {rating 1-5} [— optional comment]

A CLI command `python main.py feedback` reads stdin or a file and ingests it.
"""

import logging
import re
import sqlite3
from datetime import datetime, timezone

from storage.db import connect, insert_feedback

logger = logging.getLogger(__name__)

_FEEDBACK_RE = re.compile(
    r"#(\d+)\s*:\s*([1-5])(?:\s*[—\-–]\s*(.+))?",
    re.IGNORECASE,
)


def parse_feedback_text(text: str) -> list[dict]:
    """
    Parse free-form feedback text into structured feedback items.

    Returns list of dicts with keys: rank (int), rating (int), comment (str|None).
    """
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        match = _FEEDBACK_RE.search(line)
        if match:
            rank = int(match.group(1))
            rating = int(match.group(2))
            comment = match.group(3).strip() if match.group(3) else None
            items.append({"rank": rank, "rating": rating, "comment": comment})
    return items


def ingest_feedback(
    conn: sqlite3.Connection,
    feedback_items: list[dict],
    digest_id: int,
    ranked_signals: list[dict],
) -> None:
    """
    Persist parsed feedback, linking each item to the correct product signal.

    `ranked_signals` is the ordered list used in the digest (rank = index+1).
    """
    now = datetime.now(timezone.utc).isoformat()
    signal_by_rank = {i + 1: sig for i, sig in enumerate(ranked_signals)}

    for fb in feedback_items:
        rank = fb["rank"]
        signal = signal_by_rank.get(rank)
        if signal is None:
            logger.warning("Feedback rank #%d has no matching signal — skipped", rank)
            continue

        signal_id = signal.get("db_id")
        if signal_id is None:
            logger.warning("Signal for rank #%d has no db_id — skipped", rank)
            continue

        insert_feedback(
            conn,
            {
                "signal_id": signal_id,
                "digest_id": digest_id,
                "rating": fb["rating"],
                "comment": fb.get("comment"),
                "received_at": now,
            },
        )
        logger.info(
            "Feedback ingested: rank #%d / signal %d / rating %d",
            rank,
            signal_id,
            fb["rating"],
        )


def ingest_from_text(text: str, digest_id: int, ranked_signals: list[dict]) -> int:
    """
    Parse text feedback and persist to DB. Returns number of items ingested.
    """
    items = parse_feedback_text(text)
    if not items:
        logger.warning("No feedback lines parsed from input")
        return 0

    with connect() as conn:
        ingest_feedback(conn, items, digest_id, ranked_signals)

    return len(items)
