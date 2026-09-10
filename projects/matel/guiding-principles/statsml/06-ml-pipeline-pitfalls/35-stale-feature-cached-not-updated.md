# Pitfall: Stale Feature (Cached / Not Updated)

**Page type:** detail page (card-section layout: one `.card-section` per h2 with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Stale Feature (Cached / Not Updated)

**Subtitle:** When a cached feature value becomes outdated because the refresh pipeline failed, but the model keeps serving predictions with stale data.

## The Problem

**Tags:** `the trap` (red), `staleness` (blue)

- **Cached serving** — features are precomputed into a store (Redis, views) for fast lookup
- **Silent failure** — the refresh job fails or lags, yet serving continues without any alert
- **Stale values** — the cache says "logged in 2 days ago" when the user logged in 5 min ago
- **Train-serve drift** — trained on fresh features, scored on day-old values, accuracy drops
- **Detection gap** — monitoring tracks slowly decaying accuracy, not instantly broken freshness

*Example:* A fraud model's batch job fails at 2pm with no alert, so predictions until midnight miss 10 hours of new transactions.

**Impact:** A real-time prediction task can lose 10-30 points of AUC when its features lag by a day.

### Visualization (canvas `c1`, 720×300)

Timeline plus two line charts contrasting real user activity with a frozen cached feature.

- **Title (bold 14px `#1a5276`, top center):** "Stale Feature: Cached Value Lags Behind Reality".
- **Timeline:** gray `#999` (2px) horizontal line at y=80 from x=60 to x=660 with tick marks and 10px `#444` labels "8am", "10am", "12pm", "2pm", "4pm", "6pm", "8pm" evenly spaced (6 segments).
- **Fresh segment:** green `#27ae60` 4px line 20px above the timeline from 8am to 2pm, labeled "Fresh features" (11px green).
- **Stale segment:** red `#e74c3c` 4px line from 2pm to 8pm, labeled "STALE (using 2pm data)" (11px red).
- **Failure marker:** small red filled triangle above the 2pm tick with bold 10px red label "Refresh job fails".
- **Reality panel:** 12px `#444` left-aligned label "Reality (true user activity):" at y=140; blue `#1a5276` 2px sinusoidal line across the full width — 61 points, activity = 5 + 3·sin(i/5) with a +2 step after the midpoint (i > 30), scaled ×5 below y=150.
- **Cached panel:** label "Cached feature (what model sees):" at y=230; green `#27ae60` 2px line tracing the same sinusoid for the first half only (i = 0..30, no step); then a red `#e74c3c` 3px flat horizontal line from the 50% mark to x=660, frozen at the last computed value; bold 10px red annotation "Frozen at 2pm value" above the flat line near the right edge.

## Why It Happens

**Tags:** `root cause` (orange), `caching` (blue)

- **Decoupled moments** — caching separates when a feature is computed from when it is used
- **Diffuse ownership** — every layer assumes another layer is watching for the freshness gap
- **Silent ETL failures** — jobs die without alerts; nobody notices until accuracy drops later
- **Arbitrary TTLs** — a 24h expiry gets set without checking how often the data really changes
- **No freshness metric** — teams track AUC and precision but never feature age, the leading signal
- **Upstream drift** — source systems change update cadence without telling feature pipelines

**Root Cause:** The serving layer has no concept of "too old" — it serves whatever is cached, regardless of age.

### Visualization (canvas `c2`, 720×300)

Pipeline flow diagram with a silently failed ETL stage, plus a problem timeline.

- **Title (bold 14px `#1a5276`, top center):** "Why It Happens: Silent Pipeline Failure".
- **Four boxes in a row at y=60, height 55, connected by gray `#666` 1.5px arrows:**
  - "Source DB" (stroked `#1a5276` 2px, at x=30, 130 wide): 10px `#27ae60` "Updated every 5min" and "✓ Fresh data".
  - "ETL Job" (stroked `#e74c3c` 3px, at x=215, 130 wide): bold 14px red "✖ Failed silently"; 10px `#666` "No alert triggered".
  - "Feature Cache" (stroked `#e67e22` 2px, at x=400, 140 wide): 10px `#444` "Serving old values"; bold 10px orange "⏱ 5 hours old".
  - "Model" (stroked `#1a5276` 2px, at x=595, 100 wide): 18px `#444` "?" and 10px "No idea it's stale".
- **Problem timeline (left-aligned text):** 12px `#444` "Problem timeline:" at (40,155); 11px `#666` bullets at x=60: "• 2:00pm — ETL job fails (no alert)", "• 2:00pm to midnight — Model serves predictions using 2pm cached features", "• Next day — Someone notices model AUC dropped 15%", "• Root cause: 10 hours of stale features, not model bug".
- **Bottom line (bold 11px `#e74c3c`, centered, y=270):** "Gap: Monitoring watches model output (lagging), not feature freshness (leading)".

## The Correct Approach

**Tags:** `the fix` (green), `freshness` (blue)

- **Freshness as property** — give every feature an owner, an age threshold, and an alarm
- **Timestamp everything** — every cached value carries a last_updated marking its compute time
- **Age SLA alerts** — fire immediately when feature age exceeds its SLA, e.g. 30 min for real-time
- **Age as input** — feed feature_age_minutes to the model so it learns to discount stale signals
- **TTL with fallback** — recompute stale values on demand or fall back to a flagged safe default

**Fix:** Freshness is a feature property, not an operational afterthought — monitor it like latency, with SLAs, alerts, and fallbacks.

### Visualization (canvas `c3`, 720×300)

Feature freshness monitoring dashboard rendered as a table with status badges, plus an action box.

- **Title (bold 14px `#1a5276`, top center):** "Correct Approach: Feature Freshness Monitoring Dashboard".
- **Table:** header row filled `#1a5276` with white bold 10px column labels "feature_name", "last_updated", "age_min", "SLA_min", "status" (column widths 150/140/100/100/80, table at x=40, y=50, 570 wide, 22px header, 28px row pitch, alternating `#f8f9fa`/`#fff` rows with `#e0e0e0` borders). Rows (10px `#444` text, bold colored status text with a matching 4px status dot):
  - txn_last_hour | 2024-03-15 14:55 | 5 | 15 | FRESH (`#27ae60`)
  - user_login_recency | 2024-03-15 14:20 | 40 | 60 | WARNING (`#e67e22`)
  - fraud_score_24h | 2024-03-15 09:00 | 360 | 30 | STALE (`#e74c3c`)
- **Action box:** fill `rgba(231,76,60,0.06)` with 1px `#e74c3c` stroke, 570×40 below the table; bold 11px red "Action: REJECT prediction if any critical feature exceeds SLA"; 10px `#444` "fraud_score_24h age (360 min) > SLA (30 min) → Block serving, trigger recompute, alert on-call".
- **Bottom:** bold 11px `#1a5276` "Bonus: Pass feature_age_minutes as input to model"; 11px `#27ae60` "Model learns: \"if feature is old, discount its signal\" → graceful degradation".

## Regeneration instructions

- **Layout:** three `.card-section` blocks, each with an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) containing one `<tr>`: left `td.text-col` (45%) with `.tags` pills, a `<ul>` of labeled bullets, optional `.example` italic paragraph, and a `.key-point` callout; right `td.viz-col` (55%) with one canvas.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. Bullets 0.92rem with `<b>` labels in `#1a5276`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 each, CSS `width: 100%`, 1px `#e0e0e0` border, 4px radius; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
