import sqlite3
import json
from contextlib import contextmanager
from pathlib import Path

from config import settings

_DB_PATH = Path(settings["storage"]["db_path"])
_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def init_db() -> None:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with connect() as conn:
        conn.executescript(_SCHEMA_PATH.read_text())


@contextmanager
def connect():
    conn = sqlite3.connect(_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── news_items ──────────────────────────────────────────────────────────────

def insert_news_item(conn: sqlite3.Connection, item: dict) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO news_items
            (id, source, title, body, source_url, published_at, fetched_at, run_id)
        VALUES (:id, :source, :title, :body, :source_url, :published_at, :fetched_at, :run_id)
        """,
        item,
    )


def get_unprocessed_news(conn: sqlite3.Connection, run_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT n.* FROM news_items n
        LEFT JOIN structured_news s ON n.id = s.news_id
        WHERE n.run_id = ? AND s.news_id IS NULL
        """,
        (run_id,),
    ).fetchall()


# ── structured_news ──────────────────────────────────────────────────────────

def insert_structured_news(conn: sqlite3.Connection, data: dict) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO structured_news
            (news_id, client, geography, operation_type, wells, asset, phase,
             timing_raw, environment, depth_m, contractor, history_notes, structured_at)
        VALUES (:news_id, :client, :geography, :operation_type, :wells, :asset,
                :phase, :timing_raw, :environment, :depth_m, :contractor,
                :history_notes, :structured_at)
        """,
        data,
    )


# ── enriched_news ────────────────────────────────────────────────────────────

def insert_enriched_news(conn: sqlite3.Connection, data: dict) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO enriched_news
            (news_id, depth_m, conditions, wells_json, timeline, rig, contractor,
             phase_inferred, relationships_json, uncertainty, enriched_at)
        VALUES (:news_id, :depth_m, :conditions, :wells_json, :timeline, :rig,
                :contractor, :phase_inferred, :relationships_json, :uncertainty,
                :enriched_at)
        """,
        data,
    )


# ── product_signals ──────────────────────────────────────────────────────────

def insert_signal(conn: sqlite3.Connection, data: dict) -> int:
    cur = conn.execute(
        """
        INSERT INTO product_signals
            (news_id, run_id, product, technical_fit, timing_fit,
             commercial_priority, composite_score, rationale, uncertainty, evaluated_at)
        VALUES (:news_id, :run_id, :product, :technical_fit, :timing_fit,
                :commercial_priority, :composite_score, :rationale, :uncertainty,
                :evaluated_at)
        """,
        data,
    )
    return cur.lastrowid


def get_top_signals(conn: sqlite3.Connection, run_id: str, min_score: float, limit: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT ps.*, n.title, n.body, n.source_url,
               e.rig, e.contractor, e.timeline, e.uncertainty AS enrich_uncertainty,
               s.client, s.geography, s.operation_type, s.phase, s.environment
        FROM product_signals ps
        JOIN news_items n ON ps.news_id = n.id
        LEFT JOIN enriched_news e ON ps.news_id = e.news_id
        LEFT JOIN structured_news s ON ps.news_id = s.news_id
        WHERE ps.run_id = ? AND ps.composite_score >= ?
        ORDER BY ps.composite_score DESC
        LIMIT ?
        """,
        (run_id, min_score, limit),
    ).fetchall()


# ── opportunity_runs ─────────────────────────────────────────────────────────

def insert_run(conn: sqlite3.Connection, run_id: str, started_at: str) -> None:
    conn.execute(
        "INSERT INTO opportunity_runs (run_id, started_at) VALUES (?, ?)",
        (run_id, started_at),
    )


def update_run(conn: sqlite3.Connection, run_id: str, **kwargs) -> None:
    fields = ", ".join(f"{k} = ?" for k in kwargs)
    conn.execute(
        f"UPDATE opportunity_runs SET {fields} WHERE run_id = ?",
        (*kwargs.values(), run_id),
    )


# ── digests ──────────────────────────────────────────────────────────────────

def insert_digest(conn: sqlite3.Connection, data: dict) -> int:
    cur = conn.execute(
        """
        INSERT INTO digests (run_id, sent_at, recipient, subject, body_html, signal_ids)
        VALUES (:run_id, :sent_at, :recipient, :subject, :body_html, :signal_ids)
        """,
        data,
    )
    return cur.lastrowid


# ── feedback ─────────────────────────────────────────────────────────────────

def insert_feedback(conn: sqlite3.Connection, data: dict) -> None:
    conn.execute(
        """
        INSERT INTO feedback (signal_id, digest_id, rating, comment, received_at)
        VALUES (:signal_id, :digest_id, :rating, :comment, :received_at)
        """,
        data,
    )
