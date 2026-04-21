"""
Shared pytest fixtures for RigLogix Router test suite.

Design decisions:
- All LLM calls are mocked via the `mock_llm` fixture (patches llm.client.complete)
- All DB tests use a temporary SQLite file (auto-deleted after each test)
- The `db_conn` fixture initialises schema and yields a live connection
- Raw news dicts and signal dicts are provided as standalone fixtures so
  each stage test can be written without depending on prior stages
"""

import json
import os
import sqlite3
import tempfile
from contextlib import ExitStack
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest

# ── ensure project root is on sys.path when running from tests/ ─────────────
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-not-used")
os.environ.setdefault("SMTP_USER", "test@example.com")
os.environ.setdefault("SMTP_PASSWORD", "test-password")
os.environ.setdefault("EMAIL_TO", "seller@example.com")
os.environ.setdefault("EMAIL_FROM", "test@example.com")


# ── temporary database ───────────────────────────────────────────────────────

@pytest.fixture()
def tmp_db_path(tmp_path) -> Path:
    return tmp_path / "test_riglogix.db"


@pytest.fixture()
def db_conn(tmp_db_path, monkeypatch) -> Generator[sqlite3.Connection, None, None]:
    """
    Initialise schema in a temp SQLite file and yield a raw connection.
    Patches storage.db._DB_PATH so all helpers use the temp file.
    """
    from storage import db as db_module
    monkeypatch.setattr(db_module, "_DB_PATH", tmp_db_path)

    schema_sql = (Path(__file__).parent.parent / "storage" / "schema.sql").read_text()
    conn = sqlite3.connect(tmp_db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(schema_sql)
    conn.commit()
    yield conn
    conn.close()


# ── sample data factories ────────────────────────────────────────────────────

RUN_ID = "testrun001"

@pytest.fixture()
def raw_news_item() -> dict:
    return {
        "id": "abc123def456abcd",
        "source": "daily_logix",
        "title": "Petrobras Awards Drillship Contract for Santos Basin",
        "body": (
            "Petrobras has awarded a three-year contract to Transocean for the drillship "
            "Deepwater Titan for a multi-well campaign in the Santos Basin pre-salt polygon. "
            "The campaign starts Q3 2025 at 2000m+ water depth. DP3 classified vessel."
        ),
        "source_url": "https://fixture.example.com/petrobras-contract",
        "published_at": "2025-04-15",
        "fetched_at": "2025-04-15T07:00:00+00:00",
        "run_id": RUN_ID,
    }


@pytest.fixture()
def raw_news_list(raw_news_item) -> list[dict]:
    items = [raw_news_item]
    for i in range(1, 4):
        item = {**raw_news_item, "id": f"news{i:012x}", "title": f"News Item {i}"}
        items.append(item)
    return items


@pytest.fixture()
def structured_item(raw_news_item) -> dict:
    return {
        **raw_news_item,
        "client": "Petrobras",
        "geography": "Brazil — Santos Basin",
        "operation_type": "drilling",
        "wells": json.dumps(["12 production wells", "4 injectors"]),
        "asset": "Buzios / Itapu",
        "phase": "development",
        "timing_raw": "campaign expected Q3 2025",
        "environment": "ultra-deepwater",
        "depth_m": 2000.0,
        "contractor": "Transocean",
        "history_notes": "DP3 drillship, multi-vessel SIMOPS",
        "structured_at": "2025-04-15T07:01:00+00:00",
    }


@pytest.fixture()
def enriched_item(structured_item) -> dict:
    return {
        **structured_item,
        "conditions": "Pre-salt, strong currents, deepwater soft clay",
        "wells_json": json.dumps(["12 production wells", "4 injectors"]),
        "timeline": "2025-Q3 / 2027-Q3",
        "rig": "Deepwater Titan (drillship, DP3)",
        "contractor": "Transocean",
        "phase_inferred": "development",
        "relationships_json": json.dumps({
            "operator": "Petrobras",
            "drilling_contractor": "Transocean",
            "service_companies": [],
        }),
        "uncertainty": "low",
        "enriched_at": "2025-04-15T07:02:00+00:00",
    }


@pytest.fixture()
def signal_base(enriched_item) -> dict:
    """A product signal after classification (timing fields still at 0)."""
    return {
        "news_id": enriched_item["id"],
        "run_id": RUN_ID,
        "product": "DynOps",
        "technical_fit": 0.85,
        "rationale": json.dumps([
            "Drillship is DP3-classified — direct DynOps trigger",
            "SIMOPS configuration mentioned — multi-vessel DP scope",
        ]),
        "uncertainty": "low",
        "evaluated_at": "2025-04-15T07:03:00+00:00",
        "timing_fit": 0.0,
        "commercial_priority": 0.0,
        "composite_score": 0.0,
    }


@pytest.fixture()
def evaluated_signal(signal_base, enriched_item) -> dict:
    """A fully-evaluated signal ready for routing."""
    return {
        **signal_base,
        **enriched_item,
        "timing_fit": 0.80,
        "commercial_priority": 0.75,
        "composite_score": round(0.4 * 0.85 + 0.35 * 0.80 + 0.25 * 0.75, 3),
        "window_description": "3–6 months before rig mobilisation",
        "window_open": "2025-Q2",
        "window_close": "2025-Q3",
        "urgency": "near_term",
        "timing_rationale": ["Campaign starts Q3 2025; 2-month engagement window open now"],
        "recommended_action": "Contact Petrobras procurement for DynOps scoping call",
    }


# ── LLM mock helpers ─────────────────────────────────────────────────────────

def make_llm_response(payload) -> MagicMock:
    """Return a mock that makes llm.client.complete return `payload`."""
    m = MagicMock(return_value=payload)
    return m


_LLM_PATCH_TARGETS = [
    # Patch at each usage site (from llm.client import complete captures a local ref)
    "pipeline.structuring.news_parser.complete",
    "pipeline.enrichment.enricher.complete",
    "pipeline.classification.product_classifier.complete",
    "pipeline.timing.timing_evaluator.complete",
    "pipeline.distribution.email_composer.complete",
    # Also patch the definition site for any direct llm.client.complete() calls
    "llm.client.complete",
]


@pytest.fixture()
def mock_llm():
    """
    Patch llm.client.complete at every usage site with a single shared MagicMock.

    All pipeline modules do `from llm.client import complete`, binding a local
    reference that is unreachable by patching just `llm.client.complete`.
    We patch every module's own reference so a single `mock_llm.return_value = X`
    controls all calls.

    Usage in tests:
        mock_llm.return_value = {...}   # set per-test response
        mock_llm.side_effect = [r1, r2] # sequence of responses
    """
    mock = MagicMock()
    with ExitStack() as stack:
        for target in _LLM_PATCH_TARGETS:
            stack.enter_context(patch(target, mock))
        yield mock


# ── LLM canned responses (realistic shapes per prompt) ──────────────────────

STRUCTURE_RESPONSE = {
    "client": "Petrobras",
    "geography": "Brazil — Santos Basin",
    "operation_type": "drilling",
    "wells": ["12 production wells", "4 injectors"],
    "asset": "Buzios",
    "phase": "development",
    "timing_raw": "Q3 2025",
    "environment": "ultra-deepwater",
    "depth_m": 2000.0,
    "contractor": "Transocean",
    "history_notes": "DP3 drillship",
}

ENRICH_RESPONSE = {
    "depth_m": 2000.0,
    "conditions": "Pre-salt, strong currents",
    "wells_json": ["12 production wells", "4 injectors"],
    "timeline": "2025-Q3 / 2027-Q3",
    "rig": "Deepwater Titan",
    "contractor": "Transocean",
    "phase_inferred": "development",
    "relationships_json": {
        "operator": "Petrobras",
        "drilling_contractor": "Transocean",
        "service_companies": [],
    },
    "uncertainty": "low",
}

CLASSIFY_RESPONSE = [
    {
        "product": "SWIM",
        "technical_fit": 0.05,
        "rationale": ["Ultra-deepwater — SWIM not applicable"],
        "key_signals": [],
        "disqualifiers": ["Water depth >1500m"],
    },
    {
        "product": "DynOps",
        "technical_fit": 0.85,
        "rationale": ["DP3 drillship", "SIMOPS configuration"],
        "key_signals": ["drillship", "DP3", "SIMOPS"],
        "disqualifiers": [],
    },
    {
        "product": "Conductor Analysis",
        "technical_fit": 0.50,
        "rationale": ["New development wells — conductor design relevant"],
        "key_signals": ["12 production wells"],
        "disqualifiers": [],
    },
    {
        "product": "Riser Analysis",
        "technical_fit": 0.75,
        "rationale": ["Ultra-deepwater drilling riser required"],
        "key_signals": ["ultra-deepwater", "drillship"],
        "disqualifiers": [],
    },
    {
        "product": "DP Feasibility Study",
        "technical_fit": 0.60,
        "rationale": ["DP3 drillship in new field configuration"],
        "key_signals": ["DP3", "Santos Basin"],
        "disqualifiers": [],
    },
]

TIMING_RESPONSE = {
    "timing_fit": 0.80,
    "commercial_priority": 0.75,
    "window_description": "3–6 months before rig mobilisation",
    "window_open": "2025-Q2",
    "window_close": "2025-Q3",
    "urgency": "near_term",
    "timing_rationale": ["Campaign starts Q3 2025"],
    "recommended_action": "Contact Petrobras procurement for DynOps scoping call",
}

COMPOSE_RESPONSE = {
    "subject": "RigLogix Router — 2025-04-15 | 1 oportunidade identificada",
    "preview_text": "DynOps — Petrobras Santos Basin",
    "intro": "Uma oportunidade de alta prioridade identificada hoje.",
    "opportunities": [
        {
            "rank": 1,
            "headline": "DynOps — Petrobras Santos Basin (pré-sal)",
            "client": "Petrobras",
            "product": "DynOps",
            "composite_score": 0.807,
            "score_label": "Alta prioridade",
            "summary": "Petrobras contratou drillship DP3 para campanha de 12 poços.",
            "rationale": ["DP3 drillship classificado", "Configuração SIMOPS"],
            "timing": "Janela aberta: Q2–Q3 2025",
            "uncertainty_flag": None,
            "recommended_action": "Contatar Petrobras para reunião de escopo DynOps",
            "source_title": "Petrobras Awards Drillship Contract for Santos Basin",
        }
    ],
    "footer": "Responda com: #1: nota (1–5) — comentário opcional",
}
