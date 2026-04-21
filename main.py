"""
RigLogix Router — main entry point.

Usage:
  python main.py run          # Execute the full daily pipeline
  python main.py run --dry    # Run without sending email
  python main.py feedback     # Ingest feedback from stdin
  python main.py status       # Show last run summary
"""

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone

from config import settings
from storage.db import (
    connect,
    init_db,
    insert_news_item,
    insert_run,
    update_run,
    get_unprocessed_news,
    get_top_signals,
)
from pipeline.ingestion.daily_logix_scraper import fetch_news
from pipeline.structuring.news_parser import run as run_structuring
from pipeline.enrichment.enricher import run as run_enrichment
from pipeline.classification.product_classifier import run as run_classification
from pipeline.timing.timing_evaluator import run as run_timing
from pipeline.routing.opportunity_router import route
from pipeline.distribution.email_composer import run as run_distribution
from pipeline.feedback.feedback_handler import ingest_from_text

logging.basicConfig(
    level=getattr(logging, settings["app"]["log_level"], logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("riglogix")


def cmd_run(dry_run: bool = False) -> None:
    init_db()

    run_id = uuid.uuid4().hex[:12]
    started_at = datetime.now(timezone.utc).isoformat()

    logger.info("=== RigLogix Router run started — %s ===", run_id)

    with connect() as conn:
        insert_run(conn, run_id, started_at)

    try:
        # ── 1. Ingestion ─────────────────────────────────────────────────────
        logger.info("[1/7] Ingestion")
        raw_news = fetch_news(run_id)

        with connect() as conn:
            for item in raw_news:
                insert_news_item(conn, item)
            update_run(conn, run_id, news_fetched=len(raw_news))

        if not raw_news:
            logger.warning("No news fetched — aborting run")
            with connect() as conn:
                update_run(conn, run_id, status="success", finished_at=datetime.now(timezone.utc).isoformat())
            return

        # ── 2. Structuring ───────────────────────────────────────────────────
        logger.info("[2/7] Structuring (%d items)", len(raw_news))
        with connect() as conn:
            unprocessed = get_unprocessed_news(conn, run_id)
            unprocessed_dicts = [dict(row) for row in unprocessed]

        # Merge body from raw_news (not stored in news_items query)
        raw_by_id = {n["id"]: n for n in raw_news}
        for item in unprocessed_dicts:
            item["body"] = raw_by_id.get(item["id"], {}).get("body", "")

        with connect() as conn:
            structured = run_structuring(conn, unprocessed_dicts)
            update_run(conn, run_id, news_processed=len(structured))

        # ── 3. Enrichment ────────────────────────────────────────────────────
        logger.info("[3/7] Enrichment (%d items)", len(structured))
        with connect() as conn:
            enriched = run_enrichment(conn, structured)

        # ── 4. Classification ────────────────────────────────────────────────
        logger.info("[4/7] Classification")
        with connect() as conn:
            signals = run_classification(conn, enriched, run_id)

        # ── 5. Timing ────────────────────────────────────────────────────────
        logger.info("[5/7] Timing evaluation (%d signals)", len(signals))
        with connect() as conn:
            evaluated = run_timing(conn, signals, enriched)

        # ── 6. Routing ───────────────────────────────────────────────────────
        logger.info("[6/7] Routing")
        with connect() as conn:
            top_signals = route(conn, evaluated)
            update_run(conn, run_id, signals_created=len(evaluated))

        logger.info("Top %d opportunities selected for digest", len(top_signals))

        if not top_signals:
            logger.info("No opportunities above threshold — no digest sent")
            with connect() as conn:
                update_run(
                    conn,
                    run_id,
                    status="success",
                    finished_at=datetime.now(timezone.utc).isoformat(),
                )
            return

        # ── 7. Distribution ──────────────────────────────────────────────────
        if dry_run:
            logger.info("[7/7] Distribution SKIPPED (--dry mode)")
        else:
            logger.info("[7/7] Distribution — sending digest")
            with connect() as conn:
                sent = run_distribution(conn, top_signals, run_id)
                update_run(conn, run_id, digest_sent=1 if sent else 0)

        with connect() as conn:
            update_run(
                conn,
                run_id,
                status="success",
                finished_at=datetime.now(timezone.utc).isoformat(),
            )

        logger.info("=== Run %s completed successfully ===", run_id)

    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        with connect() as conn:
            update_run(
                conn,
                run_id,
                status="error",
                error_message=str(exc),
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
        sys.exit(1)


def cmd_feedback() -> None:
    init_db()
    print("Paste feedback text (Ctrl+D to submit):")
    text = sys.stdin.read()
    if not text.strip():
        print("No input received.")
        return

    # For CLI feedback, we need the latest digest_id and ranked signals
    # This is a simplified implementation — full replay from DB
    with connect() as conn:
        row = conn.execute(
            "SELECT id, run_id, signal_ids FROM digests ORDER BY sent_at DESC LIMIT 1"
        ).fetchone()

    if not row:
        print("No digest found in DB — run the pipeline first.")
        return

    digest_id = row["id"]
    run_id = row["run_id"]
    signal_ids = __import__("json").loads(row["signal_ids"])

    with connect() as conn:
        pipeline_cfg = settings["pipeline"]
        top = get_top_signals(
            conn,
            run_id,
            pipeline_cfg.get("min_opportunity_score", 0.0),
            pipeline_cfg.get("max_opportunities_in_digest", 10),
        )
        ranked = [dict(r) for r in top]

    count = ingest_from_text(text, digest_id, ranked)
    print(f"Feedback ingested: {count} item(s)")


def cmd_status() -> None:
    init_db()
    with connect() as conn:
        row = conn.execute(
            """
            SELECT run_id, started_at, finished_at, news_fetched,
                   news_processed, signals_created, digest_sent, status
            FROM opportunity_runs
            ORDER BY started_at DESC LIMIT 1
            """
        ).fetchone()

    if not row:
        print("No runs found.")
        return

    print(f"""
Last run: {row['run_id']}
  Started:          {row['started_at']}
  Finished:         {row['finished_at'] or '—'}
  Status:           {row['status']}
  News fetched:     {row['news_fetched']}
  News processed:   {row['news_processed']}
  Signals created:  {row['signals_created']}
  Digest sent:      {'Yes' if row['digest_sent'] else 'No'}
""")


def main() -> None:
    parser = argparse.ArgumentParser(description="RigLogix Router")
    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Execute the full pipeline")
    run_parser.add_argument("--dry", action="store_true", help="Skip email sending")

    subparsers.add_parser("feedback", help="Ingest seller feedback from stdin")
    subparsers.add_parser("status", help="Show last run summary")

    args = parser.parse_args()

    if args.command == "run":
        cmd_run(dry_run=getattr(args, "dry", False))
    elif args.command == "feedback":
        cmd_feedback()
    elif args.command == "status":
        cmd_status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
