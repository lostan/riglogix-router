# Enrichment Rules

Guidelines for the enrichment stage when inferring fields not explicitly stated in the news.

## Depth Inference
- "pre-salt" + Brazil → infer deepwater/ultra-deepwater (1800–3000m) if not stated
- "Campos Basin" → typically 100–2000m; use "medium confidence"
- "Santos Basin" → typically 1000–2500m; use "medium confidence"
- "North Sea" shelf → typically 70–200m
- "Gulf of Mexico" deepwater → >1000m if drillship mentioned

## Phase Inference
- "exploration license awarded" or "new block" → phase = exploration
- "appraisal well" or "delineation" → phase = appraisal
- "development drilling" or "infill well" or "producer/injector" → phase = development
- "first oil" or "plateau production" → phase = production
- "workover" or "intervention" or "P&A" → phase = late-life / production

## Rig Inference
- "drillship" → DP2/DP3, ultra-deepwater capable
- "semi-submersible" → DP or moored, deepwater
- "jack-up" → shelf / shallow water, moored
- Specific rig names: resolve against known fleet if possible (e.g., Petrobras fleet)

## Contractor Inference
- Petrobras-operated wells → often use BW Offshore, SBM, Saipem, Subsea7
- GoM deepwater → Transocean, Diamond Offshore, Valaris
- West Africa → Borr Drilling, Vantage Drilling

## Timeline Inference
- "campaign expected in Q[X] [year]" → convert to ISO date range
- "spud planned [month]" → start_date = first day of that month
- "rig contract awarded" → assume operations start in 3–6 months
- "license awarded" → assume exploration well in 12–18 months

## Uncertainty Levels
- low: key fields directly stated in news
- medium: some fields inferred from context or geography
- high: most fields inferred; limited hard data in news
