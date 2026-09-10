# Stakeholder Favorite Examples

**Page type:** detail page (two-column obj-table layout: text left 40%, two stacked canvases right 60%, single row)
**HTML title tag:** Stakeholder Favorite Examples — Common Bad Practices

**Subtitle:** Exploiting the Boss's Confirmation Bias — Learn which specific example the decision-maker always tests, then optimize for exactly that input. They walk away "validated." The system barely works on everything else.

## Section 1: The Practice

Every important stakeholder has a pet query. The VP of Search always types "red shoes size 8." The director of ML always checks "fraud detection on transaction #4471." The CPO always asks "what happens if I search for 'cancel my subscription'?"

The developer learns these queries — from demo meetings, slack threads, past reviews — and ensures those specific inputs produce flawless results. The stakeholder tests their query, sees it works beautifully, and their prior belief ("this team is delivering") gets confirmed. They stop looking.

- **Search team:** VP always searches "red shoes size 8." Developer adds a boost rule for that exact query. VP tests in review → perfect results. NDCG on real traffic: 0.41.
- **Fraud model:** Director always checks transaction #4471 (a known fraud case from last year's incident). Model correctly flags it every time. But it's in the training set. Novel fraud patterns: 34% recall.
- **Chatbot:** CPO always asks "cancel my subscription." Developer hardcodes that intent. CPO is delighted. 200 other cancel-adjacent phrasings: 50% misroute.
- **Recommendation engine:** Head of Product always checks their own profile. Developer notices, seeds the rec model with that user's history. Beautiful personalized results. Cold-start users: random garbage.

**Why it's worse than demo-driven:** Demo-driven is overt — everyone knows the demo is curated. This is covert. The stakeholder believes they're doing an independent spot-check. They think they're being rigorous. "I tested it myself!" becomes the shield against any criticism. Their confirmation bias (belief that the team delivers) is being weaponized against them.

**The feedback loop:** Stakeholder tests their query → works → praises team → team learns which queries to protect → stakeholder's confidence grows → they test less → system rots on everything except the protected queries.

**The tell:** Ask: "Which queries does [stakeholder] always check?" If the team can list them instantly, the system is probably optimized for those exact inputs. Run the stakeholder's queries against the same eval set as everything else — if their queries score in the 99th percentile while the median is at the 50th, it's capture.

**Defense:** Stakeholders should test with RANDOM inputs they've never seen before. Better: have someone ELSE generate the test queries. Best: automate evaluation so no human's pet query matters.

### Visualization (canvas `c1`, 720×460)

Horizontal bar chart: quality scores for stakeholder pet queries vs real traffic, four systems in alternating pairs.

- **Titles (left-aligned at x=170):** 13px `#1a5276` "Quality scores: Stakeholder's queries vs. real traffic"; 11px `#666` "Same system, same day — wildly different experience".
- **Margins:** top 60, right 30, bottom 55, left 170.
- **Rows (8, top to bottom; stakeholder rows are green/bold with a faint `rgba(39,174,96,0.05)` row background, real-traffic rows red):**
  - "Search (VP's query)" — 97% (stakeholder)
  - "Search (real traffic)" — 41%
  - "Fraud (Dir's case)" — 99% (stakeholder)
  - "Fraud (novel patterns)" — 34%
  - "Chatbot (CPO's input)" — 95% (stakeholder)
  - "Chatbot (real users)" — 50%
  - "Recs (HoP's profile)" — 92% (stakeholder)
  - "Recs (cold-start users)" — 28%
- **Bars:** stakeholder bars fill `rgba(39,174,96,0.5)` with `#27ae60` stroke width 1.5; real-traffic bars fill `rgba(231,76,60,0.3)` no stroke. Value labels ("97%" etc.) 10px to the right of each bar in the row color (bold for stakeholder rows). Row labels right-aligned in 11px (bold for stakeholder rows), green `#27ae60` or red `#e74c3c`.
- **X axis:** vertical gridlines `#e0e0e0` at 0%–100% every 20%, 9px `#999` percent labels below.
- **Legend (bottom-left):** green 10×10 swatch "Stakeholder's pet query"; red swatch "Real traffic / novel inputs" (bold 10px).
- **Punchline (bold 11px `#1a5276`, bottom center):** '"I tested it myself!" — the most dangerous sentence in product review'.

### Visualization (canvas `c2`, 720×380)

Line chart: how representative the stakeholder's favorite example remains over 12 months, vs. how often it is still cited.

- **Titles (left-aligned at x=55):** 13px `#1a5276` "The favorite example decays — the citations don't"; 11px `#666` "Product and user base move on; the anecdote stays frozen at month 0".
- **Margins:** top 55, right 150, bottom 55, left 55.
- **Axes:** Y 0–100% with horizontal gridlines `#e0e0e0` every 20% and 10px `#999` labels; X months 0–12, 10px `#999` tick labels every 2 months ("0", "2", … "12"), 11px `#666` axis label "Months since the example was first cited" centered below.
- **Series 1 — "Still cited in every meeting" (flat, orange `#e67e22`):** dashed line (dash 6,4), width 2, constant 100% across months 0–12; bold 11px orange label to the right of its endpoint, two lines: "Still cited in" / "every meeting (100%)".
- **Series 2 — "Match with current aggregate data" (decay, blue `#1a5276`):** solid line width 2.5 with 3.5px filled dots at each month; hardcoded deterministic values by month 0–12: 92, 84, 77, 70, 63, 57, 51, 46, 41, 36, 32, 28, 25 (percent); bold 11px blue label to the right of its endpoint, two lines: "Match with today's" / "users (25%)".
- **50% reference:** thin dotted `#999` horizontal line at 50% with 10px `#999` label "50% — example now misleads more than it informs" left-aligned just above it; the decay line crosses it between months 6 and 7.
- **Insight annotation (bold 11px `#e74c3c`):** vertical red bracket at month 12 spanning the gap between the two endpoints (100% down to 25%), with two-line text to its left: "12 months on: cited at 100%," / "representative of 25% of users".
- **Caption (italic 11px `#666`, bottom center):** "The anecdote is a snapshot of a user base that no longer exists — but it still decides roadmaps."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with `border-collapse: collapse`, a single `<tr>`; left `<td>` (40%) holds one `.obj-title` ("The Practice") followed by two paragraphs, a `<ul>` of four bullets, and five bold-lead paragraphs; right `<td>` (60%, centered) holds the two canvases stacked vertically.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` `#333` 0.95em; `ul` 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (720×460 and 720×380); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fills `rgba(39,174,96,0.5)` and `rgba(231,76,60,0.3)`, gray text `#666`/`#999`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
