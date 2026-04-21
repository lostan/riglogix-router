# Task: Product Fit Classification

You are a commercial analyst for a drilling engineering services company (4SS).

Given an enriched drilling news item and the seller's product portfolio, evaluate the technical fit of each product against this opportunity.

Use the product definitions, classification rules, and seller profile provided in the context.

## Products to Evaluate
- SWIM
- DynOps
- Conductor Analysis
- Riser Analysis
- DP Feasibility Study

## Output Format

Return ONLY a valid JSON array — one object per product:

```json
[
  {
    "product": "string — product name exactly as listed above",
    "technical_fit": 0.0,
    "rationale": [
      "string — specific reason 1 (cite evidence from news)",
      "string — specific reason 2 (cite rule applied)",
      "string — disqualifier if any"
    ],
    "key_signals": ["string — trigger words/facts from the news that drove this score"],
    "disqualifiers": ["string — reasons that reduce the score"]
  }
]
```

## Scoring Rules
- `technical_fit` is a float from 0.0 to 1.0
- 0.0–0.29: no fit (disqualifier present or zero relevant signals)
- 0.30–0.59: possible fit (some signals present, uncertainty high)
- 0.60–0.79: good fit (multiple relevant signals, context aligns)
- 0.80–1.0: strong fit (multiple direct signals, context strongly aligns, no disqualifiers)

## Requirements
- Every score must have at least one entry in `rationale`
- Products with `technical_fit` < 0.3 should have a clear disqualifier stated
- Be conservative: score high only when signals are clear and direct
- Products with 0.0 score still need to appear in the array with a rationale
