# Classification Rules

Rules applied by the product classifier when scoring technical fit.
Scores are 0.0–1.0. A score < 0.3 means no fit; 0.3–0.6 possible fit; > 0.6 strong fit.

## Rule Set

### R1 — Environment Depth
- ultra-deepwater (>1500m): boost Riser Analysis +0.2, DynOps +0.15
- deepwater (500–1500m): boost Riser Analysis +0.1, DynOps +0.1
- shallow water / shelf (<200m): boost SWIM +0.2, disqualify Riser Analysis if <100m

### R2 — Rig Type
- drillship → DynOps (DP) HIGH, DP Feasibility HIGH
- semi-submersible → DynOps MEDIUM, Riser Analysis MEDIUM
- jack-up → SWIM HIGH, disqualify DynOps and Riser Analysis
- platform / fixed → SWIM MEDIUM, disqualify DynOps

### R3 — Phase
- exploration / appraisal → Conductor Analysis HIGH, DP Feasibility MEDIUM
- development → DynOps MEDIUM, Riser Analysis MEDIUM
- production / workover → SWIM HIGH, DynOps LOW
- P&A / decommissioning → SWIM MEDIUM, Conductor Analysis LOW

### R4 — Operation Type
- new well drilling → Conductor Analysis boost +0.25
- SIMOPS / concurrent ops → DynOps boost +0.3
- FPSO installation / hook-up → Riser Analysis boost +0.25, DynOps boost +0.15
- well intervention / workover → SWIM boost +0.25

### R5 — Geography Modifiers
- Brazil pre-salt → Riser Analysis +0.1, DynOps +0.1 (high current, complex ops)
- North Sea → SWIM +0.1, Riser Analysis +0.1 (harsh env)
- West Africa → DynOps +0.1, Conductor Analysis +0.05 (soft seabed)
- GoM deep → Riser Analysis +0.1 (Loop Current risk)

### R6 — Disqualifiers (override to 0.0)
- "onshore" or "land rig" → disqualify all offshore products
- "decommissioned" (already done) → disqualify all except Conductor if reused
- operation already completed → reduce all scores by 0.5
