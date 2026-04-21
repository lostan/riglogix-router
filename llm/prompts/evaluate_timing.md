# Task: Opportunity Timing Evaluation

You are a commercial strategist for a drilling engineering services company.

Given a product signal (product + technical fit + enriched news), evaluate the timing fit and commercial priority for this opportunity.

## Output Format

Return ONLY a valid JSON object:

```json
{
  "timing_fit": 0.0,
  "commercial_priority": 0.0,
  "window_description": "string — describe the opportunity window (e.g. '3–6 months before rig mobilization')",
  "window_open": "string — estimated start of engagement window (YYYY-MM or YYYY-QN)",
  "window_close": "string — estimated end of engagement window (YYYY-MM or YYYY-QN)",
  "urgency": "string — one of: immediate, near_term, medium_term, long_term, unknown",
  "timing_rationale": [
    "string — evidence for timing assessment"
  ],
  "recommended_action": "string — one concrete next step the seller should take"
}
```

## Scoring Guidelines

### timing_fit (0.0–1.0)
- 1.0: engagement window is open NOW (campaign starting in <3 months)
- 0.8: window opens in 3–6 months (pre-campaign planning phase)
- 0.6: window opens in 6–12 months (early FEED or contract award)
- 0.4: window opens in 12–18 months (license/block award stage)
- 0.2: window is speculative or >18 months out
- 0.0: operation already completed or no timing signal

### commercial_priority (0.0–1.0)
Consider:
- Is this a strategic account for the seller? (use seller profile)
- Is the operator a key target client?
- Is the geography in the seller's coverage area?
- Is this a product where 4SS has a known competitive edge?
- Does the news represent a repeat/existing relationship or new entry?

### urgency mapping
- immediate: operation starting <3 months
- near_term: 3–6 months
- medium_term: 6–12 months
- long_term: >12 months
- unknown: no timing signal available

## Requirements
- `recommended_action` must be one specific sentence (e.g. "Contact Petrobras procurement for pre-campaign DynOps scoping call in Q3 2025")
- `timing_rationale` must cite specific evidence from the news or enrichment
