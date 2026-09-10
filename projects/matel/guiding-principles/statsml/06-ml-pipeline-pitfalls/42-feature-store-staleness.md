# Pitfall: Feature Store Staleness

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Feature Store Staleness

**Subtitle:** Feature store serves outdated values with no monitoring, silently degrading model performance.

## The Problem

Tags: `the trap` (red), `freshness` (blue)

- **Silent staleness** — the store caches values but never monitors how old they have become
- **Blind TTL** — a 1-hour cache expiry is picked without checking the real update cadence
- **Cadence drift** — a 15-minute refresh pipeline quietly slows to hourly and no alert fires
- **No read check** — nothing compares timestamps at read time; existing values are assumed fresh
- **False confidence** — models decide on hours-old features while believing they are current
- **Gradual decay** — accuracy erodes slowly as features age, so no single event triggers review

*Example:* A 10-minute session-count feature keeps serving 2 PM values after a silent upstream failure until 8 PM, blocking 14% of legitimate traffic.

**Impact:** Models systematically miss recent events, and the gradual accuracy drop stays invisible until a major incident.

### Visualization (canvas `c1`, 720×300)

Timeline chart of feature staleness before and after a pipeline failure.

- **Title (bold 14px, `#1a5276`, top center):** "Feature Store Staleness Over Time".
- **Timeline axis:** horizontal `#999` line at y=220 from x=60 to x=680, with 10px `#666` tick labels "10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00" evenly spaced.
- **Fresh period (first 4/6 of the width):** shaded region 60→210 vertically, fill `rgba(39,174,96,0.3)`, stroke `#27ae60` width 2; centered bold 12px `#27ae60` label "FRESH" and 11px line "Updates every 15 min".
- **Stale period (last 2/6):** fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` width 2; centered bold 12px `#e74c3c` label "STALE" and 11px lines "Pipeline failure — no updates" / "Cache serves old values".
- **Staleness curve:** orange line (`#e67e22`, width 3, 50 segments): small oscillation (5 + 3·sin(40t)) during the fresh period, then linear growth (5 + (t−4/6)·600, capped at 120px rise) after the failure; bold 11px `#e67e22` label "Staleness (minutes)" near the top right.
- **Alert threshold:** horizontal dashed red line (`#e74c3c`, dash 6/4, width 2) at y=130, labeled "Alert threshold (30 min)" in 10px `#e74c3c` at the left.
- **Failure marker:** filled red circle (radius 8) at the boundary on the staleness curve, bold 10px `#e74c3c` label "Pipeline fails" above it, and a fine dashed red drop line (dash 2/2) down to the axis.

## Why It Happens

Tags: `root cause` (orange), `ownership gaps` (blue)

- **Ownership gap** — the pipeline owner and store owner each look healthy; nobody watches age
- **No pipeline SLA** — refresh jobs have no enforced SLA, so failures stay silent for hours
- **No freshness SLA** — no max age is tracked per feature; all features are treated identically
- **Generic TTLs** — cache expiry defaults to one hour regardless of upstream update cadence
- **No alert fan-out** — upstream pipeline failures never reach downstream feature consumers

**Root Cause:** Feature stores are optimized for low-latency reads while freshness monitoring remains an afterthought.

### Visualization (canvas `c2`, 720×300)

Diagram of a feature store with mixed-freshness features feeding a model with no monitoring.

- **Title (bold 14px, `#1a5276`, top center):** "Feature Store Without Freshness Monitoring".
- **Feature Store box:** 400×180 at (50,50), fill `#f8f9fa`, stroke `#1a5276` width 2, bold 12px `#1a5276` corner label "FEATURE STORE".
- **Three feature rows inside (each 370×35, stroke width 1.5):**
  - "Feature A: user_clicks" / "Last updated: 5 min ago" — fill `rgba(39,174,96,0.15)`, stroke `#27ae60`, right-aligned bold "FRESH" in `#27ae60`.
  - "Feature B: session_count" / "Last updated: 3 hours ago" — fill `rgba(231,76,60,0.15)`, stroke `#e74c3c`, right-aligned bold "STALE" in `#e74c3c`.
  - "Feature C: purchase_history" / "Last updated: 6 hours ago" — same red styling, "STALE".
- **Missing-monitoring box:** dashed orange rectangle (`#e67e22`, dash 6/4, width 2) 160×60 at (500,80) containing a large bold 28px "?" and 10px caption "No monitoring".
- **Model box:** 140×50 at right (white fill, stroke `#1a5276` width 2), connected from the store by a gray `#999` arrow; bold 11px `#1a5276` "MODEL" plus 10px `#e74c3c` lines "Receives all features" / "without knowing age".
- **Bottom warning (centered, bold 11px `#e74c3c`):** "Model has no idea which features are stale — silent degradation".

## The Correct Approach

Tags: `the fix` (green), `freshness SLAs` (blue)

- **Freshness as data** — every read carries the value's age instead of assuming the cache is fresh
- **Per-read timestamps** — store a last_updated timestamp for every feature of every entity
- **Matched SLAs** — define max_age per feature: 15 min for activity, 24 hours for demographics
- **Alert on breach** — page the moment a feature exceeds its SLA, not when model metrics sag
- **Refuse stale reads** — serve an uncertainty flag instead of a prediction when features are stale

**Fix:** Every feature read should carry its age so the serving layer knows whether it is deciding on fresh or stale data.

### Visualization (canvas `c3`, 720×300)

Freshness monitoring dashboard rendered as a status table, framed in green.

- **Frame:** green border (`#27ae60`, width 3) inset 5px around the whole canvas.
- **Title (bold 14px, `#1a5276`, top center):** "Freshness Monitoring Dashboard".
- **Table (origin (40,55), 620px wide):** header bar filled `#1a5276` with white bold 11px column labels "Feature", "Last Updated", "Age", "SLA", "Status" (column offsets 0, 140, 280, 360, 440); rows 28px tall with `#ddd` hairline borders and 11px `#2c3e50` text:
  - `user_activity` | 2 min ago | 2 min | 15 min | **OK** (bold `#27ae60`), row tint `rgba(39,174,96,0.08)`.
  - `session_count` | 12 min ago | 12 min | 15 min | **WARNING** (bold `#e67e22`), row tint `rgba(230,126,34,0.08)`.
  - `purchase_history` | 3 hours ago | 180 min | 60 min | **BREACH** (bold `#e74c3c`), row tint `rgba(231,76,60,0.08)`.
- **Decision logic box:** 520×50 below the table, fill `rgba(231,76,60,0.05)`, stroke `#e74c3c` width 2; centered bold 11px `#e74c3c` heading "DECISION LOGIC" and 11px `#2c3e50` line "If any critical feature > SLA: return uncertainty flag instead of prediction".
- **Bottom annotation (centered, bold 11px `#27ae60`):** "Every feature carries its age. Every prediction knows its confidence."

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each an h2 with `2px solid #2980b9` bottom border, followed by a full-width `table.layout` with one row: left `td.text-col` (45%) holding tag pills, a bullet list, optional `.example` italic line and a `.key-point` callout; right `td.viz-col` (55%) holding one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; ul 0.92rem with `li b` in `#1a5276`; `.metric strong` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, border-radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, with bold lead-in word. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (`canvas.width = 720*dpr`, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray axes/arrows `#999`, text `#444`/`#666`/`#2c3e50`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
