"""
Distribution stage — compose and send the daily digest email.
"""

import json
import logging
import os
import smtplib
import sqlite3
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from jinja2 import Environment, BaseLoader

from config import settings
from llm.client import complete
from storage.db import insert_digest, update_run

logger = logging.getLogger(__name__)

_SCORE_LABEL_MAP = [
    (0.70, "Alta prioridade"),
    (0.50, "Prioridade média"),
    (0.00, "Para acompanhar"),
]

_EMAIL_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <style>
    body { font-family: Arial, sans-serif; font-size: 14px; color: #222; max-width: 680px; margin: 0 auto; }
    h1 { color: #1a3a5c; font-size: 20px; border-bottom: 2px solid #1a3a5c; padding-bottom: 8px; }
    .opportunity { background: #f9f9f9; border-left: 4px solid #1a3a5c; padding: 14px 18px; margin: 18px 0; border-radius: 2px; }
    .opportunity.alta { border-left-color: #c0392b; }
    .opportunity.media { border-left-color: #e67e22; }
    .opportunity.acompanhar { border-left-color: #2980b9; }
    .badge { display: inline-block; padding: 2px 10px; border-radius: 10px; font-size: 11px; font-weight: bold; color: white; }
    .badge-alta { background: #c0392b; }
    .badge-media { background: #e67e22; }
    .badge-acompanhar { background: #2980b9; }
    .score { font-size: 12px; color: #666; margin-left: 8px; }
    .headline { font-size: 16px; font-weight: bold; margin: 6px 0; }
    .meta { font-size: 12px; color: #555; margin-bottom: 8px; }
    .summary { margin: 8px 0; }
    .rationale { background: #eef4fb; padding: 8px 12px; border-radius: 4px; margin: 8px 0; }
    .rationale ul { margin: 4px 0; padding-left: 18px; }
    .rationale li { font-size: 13px; margin: 3px 0; }
    .action { background: #eafaf1; padding: 8px 12px; border-radius: 4px; font-size: 13px; }
    .uncertainty { background: #fef9e7; border: 1px solid #f39c12; padding: 6px 10px; border-radius: 4px; font-size: 12px; margin: 6px 0; }
    .timing { font-size: 13px; color: #444; margin: 6px 0; }
    .source { font-size: 11px; color: #888; font-style: italic; }
    .footer { margin-top: 24px; padding: 14px; background: #f0f0f0; border-radius: 4px; font-size: 13px; }
    hr { border: none; border-top: 1px solid #ddd; margin: 20px 0; }
  </style>
</head>
<body>
  <h1>📡 RigLogix Router — Digest Diário</h1>
  <p>{{ intro }}</p>

  {% for opp in opportunities %}
  {% set badge_class = 'alta' if opp.composite_score >= 0.7 else ('media' if opp.composite_score >= 0.5 else 'acompanhar') %}
  {% set badge_label_class = 'badge-alta' if opp.composite_score >= 0.7 else ('badge-media' if opp.composite_score >= 0.5 else 'badge-acompanhar') %}
  <div class="opportunity {{ badge_class }}">
    <span class="badge {{ badge_label_class }}">{{ opp.score_label }}</span>
    <span class="score">Score: {{ '%.2f' % opp.composite_score }}</span>

    <div class="headline">{{ opp.rank }}. {{ opp.headline }}</div>
    <div class="meta">
      <strong>Cliente:</strong> {{ opp.client or 'N/D' }} &nbsp;|&nbsp;
      <strong>Produto:</strong> {{ opp.product }}
    </div>

    {% if opp.uncertainty_flag %}
    <div class="uncertainty">⚠️ {{ opp.uncertainty_flag }}</div>
    {% endif %}

    <div class="summary">{{ opp.summary }}</div>

    <div class="rationale">
      <strong>Fundamentação:</strong>
      <ul>
        {% for r in opp.rationale %}
        <li>{{ r }}</li>
        {% endfor %}
      </ul>
    </div>

    <div class="timing">🕐 {{ opp.timing }}</div>

    <div class="action">
      <strong>Ação sugerida:</strong> {{ opp.recommended_action }}
    </div>

    <div class="source">Fonte: {{ opp.source_title }}</div>
  </div>
  {% endfor %}

  <hr>
  <div class="footer">
    {{ footer }}
  </div>
</body>
</html>
"""


def _score_label(score: float) -> str:
    for threshold, label in _SCORE_LABEL_MAP:
        if score >= threshold:
            return label
    return "Para acompanhar"


def compose_digest(top_signals: list[dict]) -> dict:
    """
    Call LLM to compose the digest content, then render to HTML.
    Returns dict with subject, body_html, and signal_ids.
    """
    if not top_signals:
        return {
            "subject": f"RigLogix Router — {datetime.now().strftime('%Y-%m-%d')} | Sem oportunidades identificadas",
            "body_html": "<p>Nenhuma oportunidade acima do limiar identificada hoje.</p>",
            "signal_ids": [],
        }

    user_message = (
        f"## Today's Date\n{datetime.now().strftime('%Y-%m-%d')}\n\n"
        "## Top Opportunities (ranked)\n\n"
        + json.dumps(
            [
                {
                    "rank": i + 1,
                    "product": sig["product"],
                    "composite_score": sig.get("composite_score", 0.0),
                    "technical_fit": sig.get("technical_fit", 0.0),
                    "timing_fit": sig.get("timing_fit", 0.0),
                    "commercial_priority": sig.get("commercial_priority", 0.0),
                    "client": sig.get("client"),
                    "geography": sig.get("geography"),
                    "operation_type": sig.get("operation_type"),
                    "phase": sig.get("phase"),
                    "environment": sig.get("environment"),
                    "uncertainty": sig.get("uncertainty", "medium"),
                    "timeline": sig.get("timeline"),
                    "window_description": sig.get("window_description", ""),
                    "recommended_action": sig.get("recommended_action", ""),
                    "rationale": json.loads(sig.get("rationale", "[]"))
                    if isinstance(sig.get("rationale"), str)
                    else sig.get("rationale", []),
                    "source_title": sig.get("title", ""),
                }
                for i, sig in enumerate(top_signals)
            ],
            ensure_ascii=False,
            indent=2,
        )
    )

    composed = complete("compose_email", user_message)
    opportunities = composed.get("opportunities", [])

    # Render HTML via Jinja2
    env = Environment(loader=BaseLoader())
    template = env.from_string(_EMAIL_TEMPLATE)
    body_html = template.render(
        intro=composed.get("intro", ""),
        opportunities=opportunities,
        footer=composed.get("footer", ""),
    )

    return {
        "subject": composed.get("subject", "RigLogix Router — Digest"),
        "body_html": body_html,
        "signal_ids": [sig.get("db_id") for sig in top_signals if sig.get("db_id")],
    }


def _send_email(subject: str, body_html: str, recipient: str) -> None:
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", 587))
    smtp_user = os.environ["SMTP_USER"]
    smtp_password = os.environ["SMTP_PASSWORD"]
    email_from = os.environ.get("EMAIL_FROM", smtp_user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = recipient
    msg.attach(MIMEText(body_html, "html", "utf-8"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.ehlo()
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(email_from, [recipient], msg.as_string())

    logger.info("Email sent to %s", recipient)


def run(
    conn: sqlite3.Connection,
    top_signals: list[dict],
    run_id: str,
) -> bool:
    """
    Compose and send the digest email.
    Returns True if sent successfully.
    """
    recipient = os.environ.get("EMAIL_TO", "")
    if not recipient:
        logger.error("EMAIL_TO not set — cannot send digest")
        return False

    logger.info("Composing digest for %d opportunities", len(top_signals))
    digest = compose_digest(top_signals)

    now = datetime.now(timezone.utc).isoformat()

    try:
        _send_email(digest["subject"], digest["body_html"], recipient)
        sent = True
    except Exception as e:
        logger.error("Failed to send email: %s", e)
        sent = False

    insert_digest(
        conn,
        {
            "run_id": run_id,
            "sent_at": now,
            "recipient": recipient,
            "subject": digest["subject"],
            "body_html": digest["body_html"],
            "signal_ids": json.dumps(digest["signal_ids"]),
        },
    )

    return sent
