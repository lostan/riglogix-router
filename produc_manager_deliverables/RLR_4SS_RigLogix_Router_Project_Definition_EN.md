# Project Definition  
**RLR-4SS RigLogix Router**  
*MVP discovery and functional blueprint document*  
Version 1.0 | Developed from the product discovery process  

---

## Executive Summary
Create a commercial assistant specialized in drilling opportunities, capable of collecting news from Westwood Daily Logix, structuring and enriching data, ranking fit by product and opportunity window, and distributing actionable hypotheses via email—always with explainable rationale and room for seller feedback.

---

## 1. Context and Problem
The commercial team spends significant time manually reading news from multiple sources to identify opportunity signals. This process is intensive, unstructured, and highly dependent on the seller’s tacit knowledge about the client, geography, project phase, operation type, technical history, and contracting window.

In the 4SS drilling context, value is not only in “finding news,” but in recognizing when a piece of news—combined with accumulated commercial and technical knowledge—justifies an engagement hypothesis.

The core problem, therefore, is transforming scattered information into prioritized, explainable, and actionable opportunities.

---

## 2. Project Objective
Build an assistive system, initially single-user, functioning as an opportunity radar and commercial copilot.

The system should:
- Collect news from Daily Logix  
- Structure and enrich relevant data  
- Evaluate product fit and opportunity timing  
- Deliver curated insights via email based on seller preferences  

In a second phase, the system should evolve into a copilot that absorbs learnings from meetings, feedback, and seller observations.

---

## 3. Product Principles
- Assistant, not decision-maker  
- Mandatory explainability  
- Operational humility  
- Manual-first knowledge  
- Single-user focus  

---

## 4. Target User and Usage Mode
Initial user: drilling-domain seller.

Primary usage: email digest  
Secondary usage: conversational copilot  

---

## 5. MVP Functional Scope — Phase 1
- News ingestion  
- Data structuring  
- Data enrichment  
- Product classification  
- Opportunity timing evaluation  
- Email digest delivery  
- Feedback capture  

---

## 6. Evolution Scope — Phase 2
- Conversational copilot  
- Knowledge updates  
- Agent orchestration  
- Governance  

---

## 7. Initial Product Portfolio
- SWIM  
- DynOps  
- Conductor Analysis  
- Riser Analysis  
- DP Feasibility Study  

---

## 8. Opportunity Evaluation Logic
- Technical Fit  
- Timing Fit  
- Commercial Priority  

---

## 9. News Interpretation Criteria
Includes:
- Client  
- Geography  
- Operation type  
- Wells  
- Asset  
- Phase  
- Timing  
- Environment  
- Depth  
- Contractor  
- History  

---

## 10. Data Enrichment
Adds:
- Depth  
- Conditions  
- Wells  
- Timeline  
- Rig  
- Contractor  
- Phase inference  
- Relationships  

---

## 11. Email Digest Interface
- Summary  
- Product suggestions  
- Rationale  
- Timing  
- Uncertainty  
- Feedback  

---

## 12. Copilot Interface
- Meeting insights  
- Knowledge extraction  
- Profile updates  

---

## 13. Conceptual Architecture
1. Ingestion  
2. Structuring  
3. Enrichment  
4. Classification  
5. Timing  
6. Routing  
7. Distribution  
8. Feedback  

---

## 14. Seller File Structure
```
/seller_workspace/
  seller_profile.md
  /products/
  /accounts/
  classification_rules.md
  enrichment_rules.md
```

---

## 15. Core Artifacts
- Seller profile  
- Products  
- Accounts  
- Classification rules  
- Enrichment rules  

---

## 16. Seller Profile
- Geography  
- Accounts  
- Products  
- Pain points  
- Frequency  
- Preferences  

---

## 17. Decision Model
System outputs hypotheses, not decisions.

---

## 18. KPIs
- Time reduction  
- Opportunities/week  
- Satisfaction  
- Accuracy  

---

## 19. Constraints
- 10–15 news/day  
- Single source  
- Python + Cloud  

---

## 20. Tech Architecture
- Python  
- Cloud  
- Database  
- Markdown  
- LLM  

---

## 21. Roadmap
Phase 1: Email pipeline  
Phase 2: Copilot  

---

## 22. Risks
- Data ingestion  
- Weak classification  
- Low feedback  
- Overengineering  

---

## 23. Implementation Recommendations
- Start simple  
- Use Markdown  
- Separate data stages  
- Persist rationale  

---

## 24. Minimum MVP
Working pipeline + email delivery.

---

## 25. Conclusion
Assistive system enhancing seller intelligence, starting small but scalable.
