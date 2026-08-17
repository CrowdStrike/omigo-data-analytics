# Real-World Distribution Gallery — Discussion Notes

## Domain 1: E-Commerce / Marketplace

1. **Position CTR — Geometric decay**
   - Shape: perfect exponential decay by rank (#1=30%, #2=15%, #3=8%...)
   - Insight: smoothness proves humans scan with fixed give-up probability, not evaluating quality
   - The distribution IS the evidence that position trust > content quality

2. **Cart value — Log-normal with spikes**
   - Shape: right-skewed hill (~$35 center) with needle spikes at $25, $35, $49, $75
   - Insight: spikes are behavioral scars from free-shipping thresholds
   - You can read the company's shipping policy from the distribution shape alone

3. **Time-to-first-purchase — Bimodal with dead zone**
   - Shape: spike at day 0-1, gap at days 3-10, second bump at day 12-14
   - Insight: two populations (impulse buyers vs retargeted researchers) mixed
   - Dead zone = nobody lives there, "average" describes neither group

4. **Star ratings — J-curve / U-shaped bounded**
   - Shape: spike at 5★, dip at 4★, nothing at 2-3★, spike at 1★
   - Insight: reflects motivation to review, not product quality
   - 3-star experiences produce silence — selection bias made visible

5. **Revenue per user — Zero-inflated Pareto**
   - Shape: 70% at exactly $0, then power law among spenders (1% = 40% revenue)
   - Insight: "average revenue per user" is meaningless across two universes
   - t-tests on this shape = nonsense

---

## Review Needed: Claims vs Examples

These cards need a thorough review. Many are making bold claims instead of simply highlighting distribution examples. 

Example: "Daily Steps — You're Either Sedentary or Active (No In-Between)" — this is an arbitrary assertion, not a demonstrated pattern. The cards should show what the data shape looks like and let the reader draw conclusions, not declare sweeping truths about human behavior.

**Rule:** Show the shape, describe what you see, note why it's interesting. Don't assert causation or universality.

## Domain 2: Web Search / Ranking

1. **Query frequency — Zipf's law (power law)**
   - Shape: steep power law — top 100 queries = 20% of traffic, billions of unique queries in long tail
   - Insight: head needs hand-tuning, tail has no training signal — two halves need different engineering
   - You can't serve both with one system

2. **CTR by position — Exponential decay (same as e-commerce)**
   - Shape: geometric decay (#1=30%, #2=15%...) — same curve regardless of content swapped
   - Insight: Google proved via swap experiments CTR barely changes per position
   - Ad auction model charges for position trust, not relevance — revenue IS this distribution

3. **Dwell time on clicked result — Bimodal (bounce vs engaged)**
   - Shape: spike at 2-5 sec (bounce), dead zone 5-15 sec, broad hill at 30-120 sec
   - Insight: no "moderate interest" exists — you bounced or committed
   - "Average dwell time" mixes two completely different intents

4. **Queries per session — Geometric distribution**
   - Shape: 40% do 1 query, 25% do 2, 15% do 3... constant "stop" probability
   - Insight: each query = coin flip of "did I find it?" — success rate readable from slope
   - Paradox: better search = steeper decay = fewer queries = less ad revenue

5. **Query reformulation gap — Mixture (spike + exponential tail)**
   - Shape: spike at 3-8 sec (quick reword), then exponential tail to minutes
   - Insight: fast spike = failure/rewording, long tail = new intent
   - Boundary between these = where "one search task" ends — session stitching depends on this

---

