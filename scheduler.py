"""
Scheduler — runs the pipeline daily at the configured time.

Usage:
  python scheduler.py          # Start scheduler (blocking)
  python scheduler.py --once   # Run immediately then exit (useful for cron)
"""

import argparse
import logging

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config import settings
from main import cmd_run

logging.basicConfig(
    level=getattr(logging, settings["app"]["log_level"], logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("riglogix.scheduler")


def start_scheduler() -> None:
    cron_cfg = settings["scheduler"]["cron"]
    timezone = cron_cfg.get("timezone", "America/Sao_Paulo")
    hour = cron_cfg.get("hour", 7)
    minute = cron_cfg.get("minute", 0)

    scheduler = BlockingScheduler(timezone=timezone)
    scheduler.add_job(
        cmd_run,
        trigger=CronTrigger(hour=hour, minute=minute, timezone=timezone),
        id="daily_pipeline",
        name="RigLogix Router — Daily Pipeline",
        misfire_grace_time=3600,
    )

    logger.info(
        "Scheduler started — pipeline will run daily at %02d:%02d (%s)",
        hour,
        minute,
        timezone,
    )

    try:
        scheduler.start()
    except KeyboardInterrupt:
        logger.info("Scheduler stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="RigLogix Router Scheduler")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run the pipeline immediately and exit (no scheduling)",
    )
    args = parser.parse_args()

    if args.once:
        logger.info("--once flag set: running pipeline immediately")
        cmd_run()
    else:
        start_scheduler()


if __name__ == "__main__":
    main()
