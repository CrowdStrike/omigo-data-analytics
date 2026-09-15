# Pitfall: Silent Join Fanout (Row Multiplication)

**Page type:** detail page (three `.card-section` blocks, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Silent Join Fanout (Row Multiplication)

**Subtitle:** When a many-to-many join silently multiplies rows, inflating counts, metrics, and statistical results without warning.

## The Problem

**Tags:** `the trap` (red pill), `joins` (blue pill)

- **Row multiplication** — duplicate keys on both sides make a join emit more rows than it takes in
- **Silent** — the query succeeds with no warning that 1,000 users just became 5,000 rows
- **Any join type** — LEFT, INNER, and FULL all fan out when one side has many rows per key
- **Grain mismatch** — fanout comes from an unexpected table grain, not from the join keyword
- **Cascading inflation** — SUM and COUNT scale by the fanout factor; later joins multiply again
- **Skewed averages** — AVG drifts toward heavily-duplicated rows whenever fanout is uneven

*Example:* Joining 1,000 users to 5 events each yields 5,000 rows, so COUNT(*) and SUM(revenue) come out 5x inflated.

**Impact:** Counts and revenue sums inflate by the fanout factor, and models see the same training example weighted 5x.

### Visualization (canvas `c1`, 720×300)

Before/after diagram of a join exploding row counts.

- **Title (bold 14px, `#1a5276`, top center):** "Join Fanout: Row Count Explosion".
- **Left side, labeled "BEFORE JOIN" (12px `#444`, centered at x=140, y=55):**
  - "Users Table" box: 160×60 at (60,70), 2px `#1a5276` stroke, bold blue title, green (`#27ae60`) 11px count "1,000 rows".
  - "Events Table" box: 160×60 at (60,145), same styling, green count "5,000 rows (5 per user)".
- **Center arrow:** gray `#666` 2px line from x=240 to x=340 at y=150 with filled arrowhead; label "JOIN ON user_id" (11px `#444`) above it.
- **Right side, labeled "AFTER JOIN" (centered at x=520, y=55):** "Result Table" box 240×100 at (400,90), 3px `#e74c3c` stroke; red bold 13px "Result Table", bold 16px "5,000 rows", 11px "(1,000 users × 5 events each)".
- **Warning annotations (red, centered at x=520):** bold 11px "5x inflation!" at y=225; 10px "COUNT(*), SUM() inflated 5x; AVG skews if fanout uneven" at y=245.

## Why It Happens

**Tags:** `root cause` (orange pill), `grain` (blue pill)

- **By design** — fanout is the defined semantics of a relational join, not a malfunction
- **Assumed uniqueness** — nobody verifies the join key is unique with a cardinality check
- **No pre-aggregation** — the detail table joins directly instead of rolling up to parent grain
- **No row assertions** — pipelines never check whether output rows exceed input rows
- **Mixed granularities** — user-level and event-level tables meet without documented grain

*Example:* An analyst LEFT JOINs orders to shipments, split shipments match 8% of orders twice, and revenue per order inflates by the same share.

**Root Cause:** The database matches every left row to every matching right row and has no concept of expected output size.

### Visualization (canvas `c2`, 720×300)

Flow diagram showing two tables entering a JOIN with no pre-aggregation, producing a fanned-out result.

- **Title (bold 14px, `#1a5276`, top center):** "How Fanout Occurs: No Pre-Aggregation".
- **"Table A: Users" box:** 160×55 at (30,60), 2px `#1a5276` stroke; bold blue 11px title, green (`#27ae60`) 11px "1 row per user".
- **"Table B: Events" box:** 160×55 at (30,140), same stroke; orange (`#e67e22`) 11px "N rows per user".
- **JOIN box:** solid orange `#e67e22` fill 100×40 at (290,125), white bold 13px "JOIN"; gray 2px lines connect both tables into it, gray arrow out to the result.
- **Result box:** 220×70 at (465,110), 3px `#e74c3c` stroke; red bold 12px "Result: N rows per user", 11px "(fanout = N×)", bold 11px "SUM/COUNT inflated N×!".
- **Red annotation:** bold 12px "No COUNT check!" centered at (575,220) inside a dashed red rectangle (dash 4/3, 130×22 at (510,205)).
- **Bottom note (11px `#555`, centered):** "Pipeline proceeds with no warning — SUM/COUNT inflate N×; uneven N per key skews AVG".

## The Correct Approach

**Tags:** `the fix` (green pill), `grain checks` (blue pill)

- **Explicit grain** — document the expected output grain of every join and verify it each run
- **Count distinct keys** — if distinct users before the join equals after, no fanout occurred
- **Aggregate first** — GROUP BY user_id on events before joining makes it one-to-one
- **Assert cardinality** — `assert len(df_joined) == len(df_users)` makes fanout fail loudly
- **Window functions** — OVER(PARTITION BY user_id) computes aggregates without multiplying rows

*Example:* A pipeline pre-aggregates events to one row per user, joins, and asserts the output row count equals the users table.

**Fix:** Every join needs a documented expected grain and an assertion that verifies it on every run.

### Visualization (canvas `c3`, 720×300)

Flow diagram of the correct pipeline: pre-aggregate events, then join one-to-one.

- **Title (bold 14px, `#1a5276`, top center):** "Correct: Pre-Aggregate Before Joining".
- **Top row (green flow):** "Table B: Events" box (130×55 at (20,70), 2px `#1a5276` stroke, orange 10px "N rows per user") → green arrow → "GROUP BY / user_id" box (solid `#27ae60` fill, 120×45 at (190,75), white text) → green arrow → "1 row per user" box (120×45 at (350,75), 2px `#27ae60` stroke, green bold text) with a green ✓ beside it.
- **Down arrow** from the aggregated box to the JOIN step.
- **Bottom row:** "Table A: Users" box (120×45 at (220,160), 2px `#1a5276` stroke, green 10px "1 row per user") → gray line into JOIN box (solid `#27ae60` fill 70×40 at (375,155), white bold "JOIN") → green arrow → result box (190×45 at (505,155), 3px `#27ae60` stroke, green bold 12px "Result: 1 row per user" plus ✓).
- **Bottom notes (centered):** bold green 11px "Row count preserved — metrics are correct" at y=240; 11px `#555` "assert len(result) == len(table_a)  ✓" at y=265.

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (full width, border-collapse) with one row: `.text-col` `<td>` (45%) and `.viz-col` `<td>` (55%). Text cell holds a `.tags` div of pill spans, a `<ul>` of `<li><b>Label</b> — sentence</li>` bullets, an italic `.example` paragraph, and a `.key-point` callout (`#f8f9fa` background, 3px `#e74c3c` left border, `<strong>` lead word). The "Assert cardinality" bullet wraps its code in `<code>`.
- **Tag pills:** inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width: 100%` with 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#444`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
