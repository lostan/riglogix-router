"""
Thin wrapper around the Anthropic SDK.

Each pipeline stage calls `complete(prompt_name, user_message, context)`
and receives a parsed dict back. System prompts are cached via
cache_control="ephemeral" so the seller workspace content (loaded once
per run) benefits from Anthropic prompt caching.
"""

import json
import os
from pathlib import Path
from typing import Any

import anthropic

from config import settings

_PROMPTS_DIR = Path(__file__).parent / "prompts"
_MODEL = settings["llm"]["model"]
_MAX_TOKENS = settings["llm"]["max_tokens"]

_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


def _load_prompt(name: str) -> str:
    path = _PROMPTS_DIR / f"{name}.md"
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    return path.read_text()


def complete(
    prompt_name: str,
    user_message: str,
    *,
    context: str = "",
    cache_system: bool = True,
) -> dict[str, Any]:
    """
    Call the LLM with a named prompt and return parsed JSON.

    The system prompt is composed of:
      1. The task-specific prompt (from prompts/<name>.md)
      2. Optional context (seller workspace content, rules, etc.)

    Returns the parsed JSON object from the model response.
    Raises ValueError if the response cannot be parsed as JSON.
    """
    system_prompt = _load_prompt(prompt_name)
    if context:
        system_prompt = f"{system_prompt}\n\n---\n\n## Context\n\n{context}"

    system_blocks: list[dict] = [
        {
            "type": "text",
            "text": system_prompt,
            **({"cache_control": {"type": "ephemeral"}} if cache_system else {}),
        }
    ]

    response = _client.messages.create(
        model=_MODEL,
        max_tokens=_MAX_TOKENS,
        system=system_blocks,
        messages=[{"role": "user", "content": user_message}],
    )

    raw = response.content[0].text.strip()

    # Strip markdown code fences if the model wraps output in ```json ... ```
    if raw.startswith("```"):
        lines = raw.splitlines()
        raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"LLM response for prompt '{prompt_name}' is not valid JSON.\n"
            f"Raw response:\n{raw}"
        ) from exc


def load_workspace_context() -> str:
    """Load seller workspace Markdown files into a single context string."""
    workspace = Path("seller_workspace")
    parts: list[str] = []

    for path in sorted(workspace.rglob("*.md")):
        parts.append(f"### {path.relative_to(workspace)}\n\n{path.read_text()}")

    return "\n\n---\n\n".join(parts)
