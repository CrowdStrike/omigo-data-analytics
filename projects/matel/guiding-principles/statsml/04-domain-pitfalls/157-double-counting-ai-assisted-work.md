# Double-Counting AI-Assisted Work

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 157. Double-Counting AI-Assisted Work

**Subtitle:** An assistant writes the email for the sender, then an assistant summarizes it for the receiver. Both sides log a productivity event, and the human information transferred stays flat — or shrinks.

## Callout (philosophy box)

**The fundamental problem:** Activity counts and information transferred are two different quantities. AI assistance drives the cost of producing an artifact toward zero, so the count of artifacts rises without limit while the information those artifacts carry is bounded by what the human actually knew. A dashboard that counts events measures generation cost, not output.

## The Expand-Then-Compress Round Trip

**Three Bullets → 400 Words → Three Bullets, Net Zero Gain**

- **The setup:** Alice types 3 bullets carrying 25 content items and asks an assistant to write the email.
- **Expansion:** The model returns 400 words, so volume rises 16× while Alice's contribution stays 25 items.
- **The added words are derived:** All 375 extra words are a function of Alice's 25 items plus model priors.
- **Compression:** Bob's assistant summarizes the 400 words back to 3 bullets, keeping 22 of the 25 items.
- **Information is capped:** intent → email → summary is a Markov chain, so I(intent; summary) ≤ H(intent).
- **Retention 88%:** 22 of 25 items survive the round trip, so information out is 0.88× information in.
- **Events tripled:** One logged event (Alice sends) becomes three: draft, send, summarize.
- **The scoreboard reads:** activity 3.0×, information 0.88× — the two numbers move in opposite directions.

### Visualization (canvas `c1`, 720×360)

Three stacked volume bars on a shared words-per-pixel scale showing expansion then compression, with a computed ledger below.

- **Title (bold 17px `#1a5276`, top center):** "Expand ×16, Compress Back: 3 Logged Events, Same 25 Items"
- **Scale:** `pxPerWord = 560 / 400 = 1.4`. Item counts are drawn at their word-equivalent width (1 item ≈ 1 word).
- **Box A (Alice's intent):** rect at (60, 78) width `25*1.4` height 34, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`. Left caption above in `#1a5276`: "Alice's intent — 3 bullets"; value label to the right of the bar in `#1a5276`: "25 items".
- **Arrow 1:** `#e67e22` line from (75,118) to (75,144) with arrowhead; label right of it in `#e67e22`: "assistant expands" plus the render-time computed factor "(×16.0)".
- **Box B (sent email):** rect at (60, 148) width `400*1.4` height 34, fill `rgba(230,126,34,0.30)`, stroke `#e67e22`, centered label in `#e67e22`: "400 words — 375 of them added by the model".
- **Arrow 2:** `#e67e22` line from (75,188) to (75,214) with arrowhead; label in `#e67e22`: "receiver's assistant summarizes" plus computed "(÷18.2)".
- **Box C (Bob receives):** rect at (60, 218) width `22*1.4` height 34, fill `rgba(39,174,96,0.30)`, stroke `#27ae60`. Caption above in `#27ae60`: "Bob reads — 3 bullets"; value label right of bar in `#27ae60`: "22 items".
- **Computed ledger line (`#555`, centered, y=280):** built at render time from the plotted values — "Expansion 16.0× → compression 1/18.2 → net retention 88.0%".
- **Bold red (`#e74c3c`, centered, y=306):** "Logged events: 3 (draft, send, summarize). Before: 1. Activity 3.0×, information 0.88×."
- **Gray (`#555`, centered, y=328):** "Markov chain intent → email → summary: I(intent; summary) ≤ H(intent). Expansion cannot add information."
- **Gray (`#555`, centered, y=348):** "Illustrative Example."

## Artifact Count Measures Generation Cost, Not Output

**1,800 Min/Month Buys 20 Docs at 90 Min, or 100 at 18 Min**

- **Fixed budget:** A writer has 1,800 minutes a month to spend on documents, before and after adoption.
- **Before:** At 90 minutes per document that budget produces 1,800 ÷ 90 = 20 documents per month.
- **After:** At 18 minutes per document the same budget produces 1,800 ÷ 18 = 100 documents per month.
- **What moved:** Document count rose 5.00×; human minutes spent rose 1.00× — exactly not at all.
- **The identity:** count = budget ÷ cost, so counting artifacts at fixed budget measures 1/cost.
- **Generalizes:** PRs, tickets, comments, and design docs all got cheaper, so all of their counts rose.
- **The wrong inference:** Reading a 5× count rise as a 5× output rise assumes cost per artifact is constant.

### Visualization (canvas `c2`, 720×320)

Two grouped bar pairs — document count and human minutes — before vs after, with computed ratios.

- **Title (bold 17px `#1a5276`, top center):** "Same 1,800 Min/Month: 20 Docs at 90 Min → 100 Docs at 18 Min"
- **Layout:** baseline y=230; two groups centered at x=200 ("Docs / month", max 100) and x=520 ("Human min / month", max 1800). Each group has a "before" bar and an "after" bar, width 60, 24px apart. Bar height = `value / groupMax * 150`.
- **Before bars:** fill `rgba(26,82,118,0.35)`, stroke `#1a5276`. Values 20 and 1800.
- **After bars:** fill `#e67e22`. Values 100 and 1800.
- **Value labels** above each bar in its bar color; group name below the baseline in bold `#1a5276`; "before" / "after" beneath each bar in `#555`.
- **Computed ratio labels (drawn per group, `#e74c3c` bold, above the taller bar):** `after/before` printed at render time from the plotted values → "×5.00" for docs and "×1.00" for minutes.
- **Bold red (`#e74c3c`, centered, y=272):** "Doc count ×5.00. Human time ×1.00. The metric moved because cost fell, not output."
- **Gray (`#555`, centered, y=296):** "Minutes per doc: 1,800 ÷ 20 = 90 before, 1,800 ÷ 100 = 18 after. Illustrative Example."

## Review Capacity Is the Bottleneck Production Speed Ignores

**Production 20 → 100/Month Against Fixed Review of 25: Backlog +75/Month**

- **Production rate:** Documents produced per month rises from a = 20 to a′ = 100 after adoption.
- **Review rate:** A reviewer still needs 60 minutes per document, so r = 1,500 ÷ 60 = 25 per month.
- **Before, no queue:** a = 20 < r = 25, so everything produced is reviewed and throughput equals 20.
- **After, a queue:** a′ = 100 > r = 25, so the backlog grows by a′ − r = 75 items every month.
- **Time to 100 deep:** 100 ÷ 75 = 1.33 months before the queue holds more than a month of production.
- **Month 12:** backlog = 75 × 12 = 900 items, which is 900 ÷ 25 = 36.0 months of review wait.
- **Throughput barely moves:** min(a′, r) = 25 vs min(a, r) = 20 — a 25% gain against a 400% production gain.
- **The binding constraint:** Reviewing is human attention, and no amount of generation speed relaxes it.

### Visualization (canvas `c3`, 720×360)

Cumulative line chart of items produced vs items reviewed over 12 months, with the growing gap shaded as backlog.

- **Title (bold 17px `#1a5276`, top center):** "Production 20 → 100/Month, Review Fixed at 25: Backlog Grows 75/Month"
- **Axes:** plot box x from 70 to 680, y from 60 (top, value 1200) to 275 (baseline, value 0). Horizontal gridlines `#eee` at 0/300/600/900/1200 with left labels in `#555`; x ticks at months 0,2,4,6,8,10,12 labeled beneath in `#555`; axis label "month" centered under the box.
- **Data (computed, not random):** for n = 0..12, `produced[n] = 100*n`, `reviewed[n] = 25*n`.
- **Shaded backlog:** polygon between the two lines filled `rgba(231,76,60,0.18)`.
- **Produced line:** `#e67e22`, width 2.5, right-end label "produced (100/mo)" in `#e67e22`.
- **Reviewed line:** `#27ae60`, width 2.5, right-end label "reviewed (25/mo)" in `#27ae60`.
- **Backlog annotation:** at month 12, a red (`#e74c3c`) vertical double-headed arrow between the two lines, labeled at render time with the computed gap `produced[12] − reviewed[12]` → "backlog 900".
- **Bold red (`#e74c3c`, centered, y=316):** computed from the plotted series — "Month 12 backlog 900 items = 36.0 months of review at 25/month."
- **Gray (`#555`, centered, y=340):** "Throughput rose 20 → 25/month (+25%) while production rose 400%. Illustrative Example."

## Both Sides Book Credit for the Same Exchange

**One Send Plus p−1 Summaries Logs p Events for One Exchange**

- **Sender side:** Alice's tool logs "1 document drafted with assistance" when the email goes out.
- **Receiver side:** Each of the p − 1 recipients logs "1 message summarized with assistance".
- **Per-message total:** 1 + (p − 1) = p logged events for a single exchange, so the factor is exactly p.
- **A 5-person thread:** p = 5, so 10 messages produce 10 × 5 = 50 logged assistance events.
- **What is real:** 10 exchanges happened, and the information transferred is bounded by what Alice knew.
- **Why it survives review:** Each team's number is individually correct; only the sum double-counts.
- **The aggregation error:** Rolling per-seat counts up to a team total adds events on both sides of one edge.
- **Reply-all amplifies it:** Widening the thread raises p, so the inflation factor rises with distribution, not work.

### Visualization (canvas `c4`, 720×320)

Bar chart of logged-event inflation factor against thread size p, with the p = 5 case highlighted.

- **Title (bold 17px `#1a5276`, top center):** "One Email, p Participants: 1 Send + (p−1) Summaries = p Logged Events"
- **Data (deterministic):** p = 2..8, `factor[p] = p` (i.e. 2,3,4,5,6,7,8). Bar height = `factor / 8 * 170`, baseline y=235, bars width 52 starting x=80 with 32px gaps.
- **Bars:** fill `rgba(26,82,118,0.35)` stroke `#1a5276`, except p = 5 filled `#e74c3c`.
- **Labels:** computed factor printed above each bar as "×2.0" … "×8.0" in the bar's color; "p = 2" … "p = 8" beneath the baseline in `#555`.
- **Highlight callout:** above the p = 5 bar in bold `#e74c3c`, computed at render as `10 * 5` → "10 messages → 50 events".
- **Bold red (`#e74c3c`, centered, y=278):** "Team dashboard sums both sides: 10 real exchanges become 50 productivity events (5.0×)."
- **Gray (`#555`, centered, y=302):** "Real exchanges: 10. Human information transferred: unchanged. Illustrative Example."

## The Baseline Breaks When the Unit of Work Changes

**A Year-Over-Year Count Needs a Constant Unit, and the Unit Changed**

- **What a YoY count assumes:** that one artifact this year means the same amount of work as last year.
- **What happened instead:** the cost of an artifact fell mid-series, so the unit changed under the metric.
- **The plotted series:** monthly artifacts average 20.5 over months 1-6 and 74.8 over months 7-12 → 3.65×.
- **The outcome series:** decisions shipped average 12.3 then 12.5 over the same halves → 1.01×.
- **Same period, two verdicts:** one series says the team nearly quadrupled, the other says it stood still.
- **Not a measurement error:** both counts are accurate; they simply count different things.
- **The valid comparison:** hold the unit fixed — compare decisions shipped, or minutes spent, not artifacts.
- **Structural break:** the pre- and post-adoption segments are separate series and should not be differenced.

### Visualization (canvas `c5`, 720×360)

Monthly artifact bars on a left axis with a flat decisions-shipped line on a right axis, split by an adoption marker.

- **Title (bold 17px `#1a5276`, top center):** "Artifacts Climb, Decisions Shipped Stay Flat"
- **Data (hardcoded literal arrays — the shape carries the lesson):**
  - `artifacts = [20,21,19,22,20,21,38,55,72,88,96,100]`
  - `decisions  = [12,13,12,12,13,12,13,12,13,12,12,13]`
- **Axes:** plot box x 70 to 660, baseline y=265, top y=60. Left axis 0-120 artifacts with ticks 0/40/80/120 labeled `#1a5276`; right axis 0-30 decisions with ticks 0/10/20/30 labeled `#27ae60`. Month numbers 1-12 beneath each bar in `#555`.
- **Artifact bars:** fill `rgba(26,82,118,0.35)` stroke `#1a5276`, height = `value/120 * 205`, width = plot width / 12 minus 8px gap.
- **Decisions line:** `#27ae60` width 2.5 with 3.5px dots, y = `265 − value/30 * 205`.
- **Adoption marker:** dashed 6/4 `#e74c3c` vertical line at the boundary between month 6 and 7, labeled above in `#e74c3c`: "assistant rollout".
- **Computed half-means (drawn at render from the two arrays):** bold `#e74c3c` centered at y=310 — "Artifacts: pre 20.5 → post 74.8 (3.65×). Decisions: pre 12.3 → post 12.5 (1.01×)."
- **Gray (`#555`, centered, y=336):** "The pre and post segments do not measure the same unit of work, so their ratio is not a growth rate."
- **Gray (`#555`, centered, y=354):** "Illustrative Example."

## Artifact Counts Need a Deflator and Nobody Computes One

**Nominal Index 500, Deflator 5.0×, Real Index 100 — Zero Real Growth**

- **The analogy:** an artifact count is a nominal quantity, priced in units whose cost changed.
- **Nominal index:** base month 20 docs = 100, current month 100 docs → 100 × 100 ÷ 20 = 500.
- **The deflator:** cost per artifact before ÷ after = 90 min ÷ 18 min = 5.0, so unit "prices" fell 5-fold.
- **Real index:** 500 ÷ 5.0 = 100, identical to base — real growth is 0.0%.
- **Where the headline came from:** the entire "+400%" is the deflator, not any change in real output.
- **Economics does this routinely:** nominal GDP is deflated before anyone compares two years.
- **Analytics does not:** no productivity dashboard tracks minutes-per-artifact, so no deflator exists.
- **Cheap to fix:** log authoring time per artifact and the deflator falls out of data you already have.

### Visualization (canvas `c6`, 720×320)

Three-bar index comparison — base, nominal, and deflated real — with the division shown explicitly.

- **Title (bold 17px `#1a5276`, top center):** "Nominal Artifact Index 500 ÷ Deflator 5.0 = Real Index 100"
- **Layout:** baseline y=240; three bars width 90 centered at x=160, 360, 560. Bar height = `value / 500 * 175`.
- **Bars:** "Base month" 100 in `rgba(26,82,118,0.35)` stroke `#1a5276`; "Nominal (count)" 500 in `#e67e22`; "Real (deflated)" 100 in `#27ae60`.
- **Labels:** value printed above each bar in its color; name beneath the baseline in `#555`; a dashed `#555` horizontal reference line at the base-index level (value 100) extended across the plot so the base and real bars visibly coincide.
- **Division annotation:** between the nominal and real bars, "÷ 5.0" in bold `#e74c3c` with a small arrow, where 5.0 is computed at render as `90/18`.
- **Real value computed at render:** the real bar's height and label come from `nominal / deflator`, not a literal.
- **Bold red (`#e74c3c`, centered, y=282):** "Real growth 0.0%. The entire +400% headline is the deflator."
- **Gray (`#555`, centered, y=304):** "Deflator = min per artifact before ÷ after = 90 ÷ 18 = 5.0. Illustrative Example."

## Every Extra AI Hop Loses Fidelity While Adding Events

**Retention 0.88 per Hop: 4 Hops Keep 15.0 of 25 Items and Log 8 Events**

- **A hop is a pair:** each relay expands the sender's intent and compresses it again for the receiver.
- **Per-hop retention:** the round trip keeps ρ = 0.88 of the items, so n hops keep ρⁿ.
- **Loss compounds:** 0.88⁴ = 0.600, so 25 items become 25 × 0.600 = 15.0 after four hops.
- **Events add linearly:** 2 logged events per hop gives 8 events across the chain, up from 1 unassisted send.
- **Opposite directions:** information falls 40.0% while logged activity rises 8-fold over the same chain.
- **Why "or shrinks" is literal:** the decay is multiplicative, not a rhetorical exaggeration.
- **No hop is the culprit:** each 88% step looks acceptable in isolation; only the product is visibly bad.
- **Chains are getting longer:** the more relays that are assisted, the further ρⁿ falls below 1.

### Visualization (canvas `c7`, 720×360)

Retained-items decay curve on a left axis over logged-event bars on a right axis, across 0-4 hops.

- **Title (bold 17px `#1a5276`, top center):** "Retention 0.88 per Hop: Information Decays While Events Accumulate"
- **Data (computed from ρ = 0.88 and I₀ = 25, not random):** `retained[n] = 25 * Math.pow(0.88, n)` for n = 0..4 → 25.00, 22.00, 19.36, 17.04, 14.99. `events[n] = 2*n` → 0, 2, 4, 6, 8.
- **Axes:** plot box x 80 to 660, baseline y=270, top y=60. Left axis 0-25 items with ticks 0/5/10/15/20/25 labeled `#e74c3c`; right axis 0-10 events with ticks 0/2/4/6/8/10 labeled `#1a5276`. Hop labels "0" … "4" beneath in `#555`.
- **Event bars:** fill `rgba(26,82,118,0.35)` stroke `#1a5276`, width 46, height = `events/10 * 210`, drawn behind the curve.
- **Retention curve:** `#e74c3c` width 2.5 with 4px dots, y = `270 − retained/25 * 210`; each point labeled with its computed value to one decimal in `#e74c3c`.
- **Bold red (`#e74c3c`, centered, y=312):** computed at render from the plotted arrays — "After 4 hops: 15.0 of 25 items retained (−40.0%), 8 logged events (was 1)."
- **Gray (`#555`, centered, y=336):** "Loss is multiplicative (0.88^4 = 0.600); events are additive (2 per hop). Illustrative Example."

## Accepted Work Is Flat While Produced Work Multiplies

**Produced ×5.00, Accepted ×1.06, Yield 85.0% → 18.0%**

- **Before:** 20 documents produced, all 20 reviewed, 85% accepted → 17 accepted per month.
- **After:** 100 documents produced, review capacity caps it at 25 reviewed, 72% accepted → 18 accepted.
- **Accepted barely moves:** 17 → 18 is +5.9%, against a +400% rise in documents produced.
- **Yield collapses:** accepted ÷ produced falls from 17/20 = 85.0% to 18/100 = 18.0%, a 4.7-fold drop.
- **Quality of reviewed work also fell:** acceptance among reviewed items went 85.0% → 72.0%.
- **Why acceptance fell:** cheaper drafting lowers the bar for what gets submitted for review at all.
- **The metric to keep:** accepted artifacts per month, which is bounded by review, not by generation.
- **The metric to drop:** produced artifacts per month, which is bounded only by tooling speed.

### Visualization (canvas `c8`, 720×320)

Two stacked horizontal funnels — produced → reviewed → accepted — for before and after, on one shared scale.

- **Title (bold 17px `#1a5276`, top center):** "Produced ×5.00, Accepted ×1.06 — the Funnel Narrows, the Exit Does Not"
- **Scale:** `pxPerItem = 480 / 100 = 4.8`, bars start at x=110.
- **Before row (y=70, height 30):** nested rects — produced 20 in `rgba(26,82,118,0.35)` stroke `#1a5276`, reviewed 20 in `rgba(230,126,34,0.45)`, accepted 17 in `#27ae60`. Row label "before" at x=60 in `#555`; segment values printed to the right of the row in `#333` as "produced 20 · reviewed 20 · accepted 17".
- **After row (y=140, height 30):** produced 100 in `rgba(26,82,118,0.35)` stroke `#1a5276`, reviewed 25 in `rgba(230,126,34,0.45)`, accepted 18 in `#27ae60`. Row label "after"; values "produced 100 · reviewed 25 · accepted 18".
- **Yield labels (computed at render from the plotted segment values):** to the right of each row in bold — before "yield 85.0%" in `#27ae60`, after "yield 18.0%" in `#e74c3c`.
- **Accepted-edge guide:** a dashed 5/4 `#27ae60` vertical line at the before-row accepted edge (x = 110 + 17×4.8), carried down through the after row so the near-identical accepted widths line up visually.
- **Bold red (`#e74c3c`, centered, y=232):** "Accepted 17 → 18 (+5.9%) while produced 20 → 100 (+400%)."
- **Gray (`#555`, centered, y=258):** "Acceptance rate among reviewed items: 85.0% → 72.0%. Yield on produced: 85.0% → 18.0%."
- **Gray (`#555`, centered, y=282):** "Illustrative Example."

## Numeric reconciliation

All figures on the page derive from one shared scenario, so they must agree across prose, tables, and chart labels:

| Quantity | Before | After | Source |
|---|---|---|---|
| Authoring budget (min/month) | 1,800 | 1,800 | assumption, held fixed |
| Minutes per artifact | 90 | 18 | assumption (the deflator) |
| Artifacts produced / month | 20 | 100 | 1,800 ÷ 90, 1,800 ÷ 18 |
| Review budget (min/month) | 1,500 | 1,500 | assumption, held fixed |
| Minutes per review | 60 | 60 | unchanged by assistance |
| Review capacity r / month | 25 | 25 | 1,500 ÷ 60 |
| Throughput min(a, r) | 20 | 25 | +25% |
| Backlog growth / month | 0 | 75 | 100 − 25 |
| Backlog at month 12 | 0 | 900 | 75 × 12 = 36.0 months at r = 25 |
| Nominal index (base 100) | 100 | 500 | 100 × 100 ÷ 20 |
| Deflator | — | 5.0 | 90 ÷ 18 |
| Real index | 100 | 100 | 500 ÷ 5.0 → 0.0% real growth |
| Accepted / month | 17 | 18 | 0.85 × 20, 0.72 × 25 |
| Yield (accepted ÷ produced) | 85.0% | 18.0% | 17 ÷ 20, 18 ÷ 100 |
| Round-trip retention ρ | — | 0.88 | 22 of 25 items |
| 4-hop retention | — | 0.600 | 0.88⁴ → 15.0 of 25 items |

Every one of these was computed, not asserted. The whole scenario is labeled **Illustrative Example** in the charts.

## Regeneration instructions

- **Layout:** detail page. h1 (no index number) + `.subtitle` + one `.philosophy` callout, then one unnumbered `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (50%) holds an `.obj-title` div — a refined restatement of the pitfall name, never a copy of it — plus a `<ul>` of labeled bullets; right `<td>` (50%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px. No nav bar, no back/home links, no cross-references, no `<thead>`, no status badges.
- **Columns stay 50/50.** If a chart needs to be smaller, cap the canvas via its own `width`/`height` attributes and `style.maxWidth`; never narrow the `<td>`.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart (720 wide, 320-360 tall); shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart text uses 17px -apple-system. Draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **No random data anywhere on this page.** Every series is either a hardcoded literal array (`artifacts`, `decisions`) or a closed-form expression of the stated parameters (`100*n`, `25*n`, `25*0.88^n`, `factor = p`). `Math.random()` must never appear. If a future edit needs jitter, add the canonical seeded generator per chart with its own fixed seed: `function lcg(seed){var s=seed;return function(){s=(s*16807)%2147483647;return s/2147483647;};}`
- **Every statistic printed beside a chart is computed at render time from the plotted values** — the ratios `×5.00` / `×1.00`, the backlog `900` and `36.0 months`, the half-means `20.5 / 74.8 / 12.3 / 12.5` and their ratios `3.65× / 1.01×`, the real index `500/5.0`, the retention `15.0` and `−40.0%`, and both yields `85.0% / 18.0%`. Do not hardcode any of these as string literals.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#333`. Category label for this page: AI TOOLS, `#8e44ad`.
- **Tone:** professional and neutral, model-agnostic ("an assistant", "the model") — never a commercial product name. People are Alice and Bob; companies would be "Vendor A".
- In regenerated HTML, any card links use `.html` extensions (this page has none).
