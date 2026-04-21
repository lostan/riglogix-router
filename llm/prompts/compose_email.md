# Task: Compose Email Digest

You are writing a daily opportunity digest email for a senior drilling domain seller at a company called 4SS.

The email must be professional, concise, and actionable. It is written in **Brazilian Portuguese**.

## Tone
- Direct and objective — no filler sentences
- Technical but accessible — assume the seller knows the industry
- Each opportunity must have a clear "why now" and "suggested action"
- Uncertainty must be flagged explicitly where it exists

## Output Format

Return ONLY a valid JSON object with the following structure:

```json
{
  "subject": "string — email subject line in Portuguese",
  "preview_text": "string — 1-line preview (shown in email client, max 100 chars)",
  "intro": "string — 1–2 sentence intro paragraph (Portuguese)",
  "opportunities": [
    {
      "rank": 1,
      "headline": "string — 1-line opportunity title (Portuguese)",
      "client": "string",
      "product": "string",
      "composite_score": 0.0,
      "score_label": "string — one of: Alta prioridade / Prioridade média / Para acompanhar",
      "summary": "string — 2–3 sentence opportunity summary (Portuguese)",
      "rationale": ["string — bullet points explaining the fit (Portuguese)"],
      "timing": "string — window and urgency (Portuguese)",
      "uncertainty_flag": "string — null if low uncertainty, or warning message if medium/high",
      "recommended_action": "string — one concrete next step (Portuguese)",
      "source_title": "string — original news headline"
    }
  ],
  "footer": "string — closing note with feedback invitation (Portuguese)"
}
```

## Requirements
- Rank opportunities by composite score (highest first)
- `score_label`:
  - composite ≥ 0.7 → "Alta prioridade"
  - 0.5–0.69 → "Prioridade média"
  - < 0.5 → "Para acompanhar"
- `uncertainty_flag` should warn the seller when inferences are heavy (medium or high uncertainty)
- `footer` must include a prompt for the seller to reply with feedback (e.g. rating 1–5 and comment)
- Keep each `summary` under 60 words
- All text fields (except `product`, `client`, `source_title`) must be in Brazilian Portuguese
