# Data Lineage / Provenance Tracking

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 118. Data Lineage / Provenance Tracking

**Subtitle:** Without lineage, numbers can't be traced, schema changes break consumers silently, and compliance questions become unanswerable.

## Can't Trace How a Number Was Computed

- 3 teams, 5 transforms later — where did this value come from?
- Dashboard shows "$4.2M revenue" but nobody can reproduce it

**Example:** CFO asks "why does this report say $4.2M when Finance says $3.8M?" Investigation takes 3 weeks across 5 teams — transforms applied in different order.

### Visualization (canvas `c1`, 720×200)

Pipeline flow diagram of six stages with arrows and question marks where the value changed.

- **Background:** `#eaf2f8`. **Title (17px, `#1a5276`):** "Value Transformation: $3.8M → $4.2M (Where?)" at (195, 18).
- **Stages (two-line box labels, 90×50px boxes at x=40+i×115, y=80):** "Raw $3.8M", "ETL Team A", "Join Team B", "Agg Team C", "Filter Team D", "Dashboard $4.2M"; underlying values `[3.8, 3.8, 4.1, 4.1, 4.2, 4.2]`.
- **Box fills:** pale green `#d4efdf` where the value is unchanged from the previous stage; pale orange `#fdebd0` where it changed (Join Team B, Filter Team D). Borders blue `#2980b9` 1.5px; gray `#7f8c8d` connector arrows between boxes.
- **Annotations:** bold 20px red `#e74c3c` "?" above the two value-changing stages; 11px red caption "$400K discrepancy introduced somewhere — but WHERE?" at (180,170).

## Stale Lineage (Pipeline Changed, Docs Didn't)

- Documentation says data flows A→B→C, reality is A→D→C
- Teams make decisions based on incorrect lineage understanding

**Example:** Lineage doc shows "customer_score" from CRM. Actually changed 8 months ago to pull from ML model. Team debugging CRM for a model issue.

### Visualization (canvas `c2`, 720×200)

Two-row flow diagram comparing documented lineage (crossed out) with reality.

- **Background:** `#fef9e7`. **Title:** "Documented Lineage vs Reality" at (260, 18).
- **Documented row (labeled "Documented:" in green `#27ae60` at 20,50):** nodes CRM → ETL → Warehouse → Dashboard as 100×25px pale-green `#d4efdf` boxes with green borders at x=120+i×150, y=38, connected by green lines. A red X (two 3px `#e74c3c` strokes) drawn over the documented path near (350-380, 35-65).
- **Reality row (labeled "Reality:" in red at 20,120):** nodes CRM → ML Model → Cache → Warehouse → Dashboard as 95×25px pale-orange `#fdebd0` boxes with red `#e74c3c` borders at x=100+i×125, y=108, connected by red lines.
- **Caption (11px red, at 180,165):** "Team debugging CRM for an ML model issue — wasted 2 weeks".

## Circular Dependencies Undetected

- A feeds B feeds C feeds A — circular loop creates amplification/instability
- Without lineage tracking, cycles can run for months undetected

**Example:** Risk score feeds pricing model feeds customer behavior prediction feeds risk score. Positive feedback loop amplified errors 340% over 6 months before detection.

### Visualization (canvas `c3`, 720×200)

Cycle diagram (left) plus exponential error-growth line (right).

- **Background:** `#f4ecf7`. **Title:** "Circular Dependency: Error Amplification Over Time" at (185, 18).
- **Cycle (left):** three 28px-radius circles ("Risk Score", "Pricing Model", "Behavior Pred", two-line 9px labels) placed on a circle of radius 60 around center (180,110) at angles −π/2, π/6, 5π/6; fills lavender `#e8daef`, borders purple `#8e44ad` 1.5px, connected by purple 2px arrows forming a loop.
- **Error growth (right):** red `#e74c3c` 2px line of `[1, 1.3, 1.7, 2.3, 3.1, 4.2, 5.8, 8.1, 11.4, 16, 22.4]`, scale max 25 over 150px, baseline y=180, x=320+i×35.
- **Labels (11px):** red "Error magnitude (340% in 6mo)" at (400,40); `#1a5276` "Months →" at (600,195).

## Schema Changes Propagating Untracked

- Upstream team changes a column type — 47 downstream consumers break silently
- No lineage = no impact analysis before schema changes

**Example:** Team changed "amount" from cents (int) to dollars (float). 23 downstream pipelines silently processed values 100x too small for 2 weeks.

### Visualization (canvas `c4`, 720×200)

Fan-out impact diagram from a changed source column to downstream consumers.

- **Background:** `#eafaf1`. **Title:** "Schema Change Impact: 1 Column → 47 Broken Consumers" at (170, 18).
- **Source node:** red `#e74c3c` filled circle radius 25 at (80,100) with white 9px labels "amount" / "int→float".
- **First tier:** four 15px-radius pale-orange `#fdebd0` circles with red borders at (200,45), (200,85), (200,125), (200,165), connected to the source by red 1px lines (consumers per layer: 12, 15, 11, 9).
- **Second tier:** from each first-tier node, three 8px-radius pale-yellow `#fef9e7` circles with orange `#f39c12` borders at x=320+j×60, connected by orange lines.
- **Text (12px `#1a5276`, right side):** "47 downstream consumers" / "processing values 100x" / "too small for 2 weeks"; then 11px red: "Silent failure — no errors thrown" / "Just wrong numbers everywhere".

## Debugging Impossible Without Lineage

- Value is wrong — but WHERE in the pipeline did it go wrong?
- Without lineage, debugging = manually tracing through every system

**Example:** A KPI is wrong by 12%. With lineage: found root cause in 2 hours (join condition). Without lineage: took 3 weeks of 4 engineers searching manually.

### Visualization (canvas `c5`, 720×200)

Grouped bar chart of debugging hours with vs without lineage per issue type.

- **Background:** `#ebf5fb`. **Title:** "Debugging Time: With vs Without Lineage" at (225, 18).
- **Issues:** Join error, Missing data, Wrong agg, Type mismatch, Stale cache.
- **With lineage (green `#27ae60`, minimum 3px height):** `[2, 1, 3, 1.5, 0.5]` hours; **Without (red `#e74c3c`):** `[120, 80, 160, 60, 40]` hours. Scale max 180 over 140px, baseline y=175, bars 35px wide at x=70+i×130 (without offset +40). "Nh" labels above bars, issue names below (9px `#1a5276`).
- **Legend (top right):** green "With lineage", red "Without"; purple `#8e44ad` annotation "Avg 50x faster with lineage" at (440,80).

## Compliance/Audit Failure (GDPR)

- "Show me all processing of this user's data" — can't answer without lineage
- GDPR Article 30 requires processing records; without lineage = non-compliant

**Example:** GDPR audit: "Where is user X's data processed?" Answer took 6 weeks, found data in 34 systems (expected: 8). Fine: EUR 2.3M for inadequate records.

### Visualization (canvas `c6`, 720×200)

Grouped bar chart of expected vs actual audit findings.

- **Background:** `#fdedec`. **Title:** 'GDPR Audit: "Where is User X\'s Data?"' at (235, 18).
- **Categories (two-line labels):** "Known Systems", "Found During Audit", "Total", "Time to Answer".
- **Expected (blue `#2980b9`, drawn only when > 0):** `[8, 0, 8, 0]`; **Actual (red `#e74c3c`):** `[8, 26, 34, 42]` — the last actual value labeled "42 days". Scale max 45 over 130px, baseline y=170, bars 45px wide at x=70+i×170 (actual offset +50).
- **Legend (right):** blue "Expected", red "Reality"; red 11px annotation "Fine: EUR 2.3M" at (520,85).

## Lineage as Compliance Prerequisite — Not Afterthought

- **Data residency laws:** GDPR (EU), CCPA (California), PIPEDA (Canada), LGPD (Brazil) each impose duties.
- **What they require:** knowing WHERE data lives, HOW it flows, WHO processes it for every data set.
- **Burden of proof:** without lineage you cannot prove compliance with any of these four regimes.
- **Right to deletion:** User requests data deletion — you delete from the primary DB and call it done.
- **Missed copies:** without lineage you miss 12 downstream copies, cached features, analytics exports.
- **Silent non-compliance:** model training snapshots keep the data — you're non-compliant and don't know it.
- **Data minimization:** Regulations require collecting ONLY what's needed, nothing held in reserve.
- **No audit trail:** without lineage you can't audit what data flows where, so you can't prove minimization.
- **Just-in-case collection:** fields kept "just in case" propagate to 30 systems with no way to trace or remove them.
- **Cross-border transfer:** Data crosses the EU→US boundary during processing, unnoticed by anyone.
- **Geographic blind spot:** without lineage on geographic flow you violate transfer restrictions unknowingly.
- **Perfect and illegal:** the pipeline works perfectly — and violates the law on geography alone.

**The design failure:** Teams build pipelines first, then discover compliance requirements. Retrofitting lineage into an existing system costs 10-50× more than building it in from day one. The architecture that "works" technically may be legally inoperable.

### Visualization (canvas `c9`, 720×200)

Deletion-request diagram: one deleted primary copy fanning out to missed downstream copies.

- **Background:** `#eaf2f8`. **Title:** "Right to Deletion: 1 Copy Deleted, 12 Missed" at (200, 20).
- **Primary DB box:** 110×44px pale-green `#d4efdf` rectangle with green `#27ae60` border at (40,80), labeled "Primary DB" / "deleted ✓" (11px `#1a5276`).
- **Missed copies:** six 130×40px pale-red `#fdedec` rectangles with red `#e74c3c` borders in a 3×2 grid at x=230+(i%3)×160, y=55+floor(i/3)×70, labeled "Cache — missed", "Features — missed", "Training set — missed", "Analytics — missed", "Backups — missed", "Exports — missed" (11px red). Gray `#7f8c8d` connector lines from the primary DB to each.
- **Caption (bold 12px red, at 180,190):** "Without lineage: non-compliant and unaware of it".

## Data Quality Issues Untraceable to Source

- Bad data detected at consumption — but which of 12 sources introduced it?
- Without lineage, you fix symptoms not root causes

**Example:** 15% of customer records have invalid phone numbers. Issue traced (eventually) to a migration 2 years ago that truncated international prefixes. Without lineage: unfixable.

### Visualization (canvas `c7`, 720×200)

Area/line chart with dots: days to find root cause vs number of potential sources.

- **Background:** `#fef9e7`. **Title:** "Data Quality: Time to Find Root Cause by Source Count" at (160, 18).
- **Sources (x):** `[1, 2, 3, 5, 8, 12, 20]`, scaled value/22 over 580px from x=70; **Days:** `[0.5, 1, 3, 8, 18, 45, 120]`, scaled value/130 over 155px, baseline y=185.
- **Area fill:** `rgba(231,76,60,0.1)`; **line:** red `#e74c3c` width 2.5 with 4px-radius red dots; "Nd" labels beside points (10px `#1a5276`).
- **Labels (11px):** `#1a5276` "# Potential Sources →" at (500,198), "Days to find root cause ↑" at (5,35); red "Exponential search without lineage" at (350,50).

## Model Predictions Unexplainable Without Feature Lineage

- Model trained on derived feature — can't explain what raw data influenced prediction
- Explainability requires tracing features back to original sources

**Example:** Model rejects loan application. Regulation requires explanation. Feature "risk_composite_v3" is derived from 47 raw fields across 8 systems — can't explain to customer.

### Visualization (canvas `c8`, 720×200)

Four-level feature lineage tree from one derived feature down to raw source fields.

- **Background:** `#f4ecf7`. **Title:** "Model Feature Lineage Depth vs Explainability" at (205, 18).
- **Root:** purple `#8e44ad` filled circle radius 20 at (360,45) labeled "risk_v3" in white 8px.
- **Level 2:** three 12px-radius circles `#d2b4de` at (200,90), (360,90), (520,90), connected to root with purple lines.
- **Level 3:** twelve 8px-radius circles `#ebdef0` at x = 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650 (y=140), connected to level-2 parents (four children each) with thin `#bb8fce` lines.
- **Level 4 (raw sources):** three tiny 6×6px `#f5eef8` squares under each level-3 node at y=172.
- **Labels (11px):** `#1a5276` "47 raw fields" / "8 systems" at left (15,175/190); red `#e74c3c` '"Why was my loan rejected?"' / "Cannot answer without full lineage" at right (440,175/190).

## Regeneration instructions

- **Layout:** domains detail-page convention: h1 + `.subtitle`, then per pitfall an unnumbered `<h2>` followed by a `.obj-table` (full-width, border-collapse) with a single `<tr>`: left `<td>` (40%) holds `.obj-title` (repeating the h2 text), a `<ul>` of bullets, and a `<p>` paragraph (bolded "Example:" lead, except the compliance-prerequisite section which uses bolded bullet leads and a bolded "The design failure:" paragraph); right `<td>` (60%, centered) holds the canvas. Even rows get background `#fafcfe`. No thead, no nav, no badges, no cross-page links.
- **Section/canvas order:** document order is c1-c6, then the compliance-prerequisite section with canvas `c9`, then c7, c8 — preserve this ordering and the canvas ids.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border 4px `#2980b9`) though unused on this page.
- **Canvas:** HTML attributes declare `width="720" height="300"`, but the shared `setupCanvas(id)` helper resets each canvas to 720×200 CSS pixels, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All charts drawn in a 720×200 coordinate space.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, purple `#8e44ad`, pale fills `#d4efdf`/`#fdebd0`/`#fdedec`/`#e8daef`, gray `#7f8c8d`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
