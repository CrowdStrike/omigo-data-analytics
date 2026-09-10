# Pitfall: Batch vs Online Feature Mismatch (Training-Serving Skew)

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Batch vs Online Feature Mismatch (Training-Serving Skew)

**Subtitle:** Features computed differently in batch training vs real-time serving create systematic prediction errors.

## The Problem

Tags: `the trap` (red), `serving` (blue)

- **Split computation** — training reads batch features; serving recomputes them with new logic
- **Window shift** — batch computes "last 30 days" at midnight; serving counts from request time
- **Logic drift** — batch runs exact SQL scans; serving uses cached or approximate aggregates
- **Freshness gap** — training sees complete backfilled data; serving misses late-arriving events
- **Timezone rounding** — batch truncates to UTC dates; serving uses local time, shifting windows
- **Learned mismatch** — the model learns batch patterns that never appear at serving time

*Example:* A "7-day activity" feature computed at midnight reads 23 in training but 19 at a 3:42 PM request, and accuracy drops 8% in production.

**Impact:** The model degrades in production despite good offline metrics, because its features are systematically different at serving time.

### Visualization (canvas `c1`, 720×300)

Flow diagram contrasting the training and serving feature computation paths.

- **Title (bold 14px, `#1a5276`, top center):** "Training vs Serving Feature Computation".
- **Training path (top, y≈80):** bold 12px `#27ae60` label "TRAINING (Batch)"; a green-outlined box (140×40, stroke `#27ae60` width 2, white fill) with 11px `#2c3e50` lines "SQL: Full scan" / "00:00 UTC window"; green arrow to a green-outlined result box (120×50) containing bold 13px `#27ae60` "Feature = 23".
- **Serving path (bottom, y≈200):** bold 12px `#e74c3c` label "SERVING (Real-time)"; a red-outlined box (140×40, stroke `#e74c3c`) with lines "Cache lookup" / "15:42 timestamp"; red arrow to a red-outlined result box (120×50) containing bold 13px `#e74c3c` "Feature = 19".
- **Consequence box (right side, 280×80 at (400,110), stroke `#e67e22` width 3, white fill):** bold 13px `#e67e22` heading "SKEW DETECTED"; 11px `#2c3e50` lines "Different computation logic", "Different time boundaries", "→ Model sees shifted distribution".
- **Connectors:** light gray dashed lines (`#ccc`, dash 4/4, width 1.5) from both result boxes to the consequence box.

## Why It Happens

Tags: `root cause` (orange), `two pipelines` (blue)

- **Two codebases** — batch uses SQL or Spark while serving uses Flink or Redis lookups
- **Separate teams** — training and serving are built independently, tuned for throughput vs latency
- **Gradual drift** — skew is rarely one bug; the two implementations diverge slowly over time
- **Boundary mismatch** — midnight batch runs see complete data; mid-day serving sees partial data
- **Version drift** — training pins library v2.1 while the serving container upgrades to v2.3
- **Timezone handling** — batch truncates to the UTC date; real-time uses local timestamps

**Root Cause:** The "same feature" is actually two implementations that happen to share a name.

### Visualization (canvas `c2`, 720×300)

Two parallel four-box pipelines diverging to different feature values.

- **Title (bold 14px, `#1a5276`, top center):** "Two Paths, One Feature Name".
- **TRAINING row (y=80):** bold 13px `#27ae60` row label "TRAINING"; four boxes (140×40 at x = 20 + i·170, white fill, stroke width 2) with gray (`#999`) connecting arrows: "Spark SQL", "Full table scan", "UTC midnight" (stroke `#1a5276`, 11px `#2c3e50` text), and "Feature = 23" (stroke and bold 12px text `#27ae60`).
- **SERVING row (y=190):** bold 13px `#e74c3c` row label "SERVING"; four boxes in the same positions: "Redis lookup", "Partial data", "3:42 PM local" (stroke `#1a5276`), and "Feature = 19" (stroke and bold 12px text `#e74c3c`).
- **Skew marker:** thick red vertical double-headed arrow (`#e74c3c`, width 3, filled triangular heads at both ends) between the two result boxes, with centered bold 11px `#e74c3c` two-line label "SKEW: same feature," / "different values!".

## The Correct Approach

Tags: `the fix` (green), `one pipeline` (blue)

- **Unified pipeline** — one implementation computes features for both training and serving
- **Log and compare** — record actual served feature values and diff them against batch versions
- **Distribution checks** — compare train and serve histograms daily; alert on KL-divergence spikes
- **Feature store** — a central store guarantees identical values in batch and online retrieval
- **Shared validation** — if the two paths cannot share code, they must share equivalence checks

**Fix:** One definition, one computation, verified daily against the serving path.

### Visualization (canvas `c3`, 720×300)

Unified architecture diagram: one feature codebase feeding both training and serving, with a comparison stage.

- **Title (bold 14px, `#1a5276`, top center):** "Unified Feature Architecture".
- **Top box (200×40, centered, y=45):** fill `rgba(26,82,118,0.1)`, stroke `#1a5276` width 3, bold 13px `#1a5276` text "Feature Logic (single codebase)".
- **Fan-out:** two `#1a5276` arrows (width 2, filled heads) down-left and down-right to two green boxes (120×35 each, fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2) labeled "Training" and "Serving" in bold 11px `#27ae60`.
- **Log & Compare box (280×80, centered, below):** fill `rgba(26,82,118,0.05)`, stroke `#1a5276` width 2; bold 12px `#1a5276` heading "Log & Compare Distributions"; inside, two interleaved bar histograms of 7 bars each — training bars fill `rgba(39,174,96,0.4)` heights `[10, 18, 30, 35, 28, 15, 8]`, serving bars fill `rgba(26,82,118,0.4)` heights `[9, 17, 29, 34, 27, 14, 7]`, offset 13px; small legend swatches labeled "Serve" (blue) and "Train" (green) in 10px `#444`.
- **Bottom annotation (centered, bold 12px `#27ae60`):** "Same code, same result, verified daily".

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each an h2 with `2px solid #2980b9` bottom border, followed by a full-width `table.layout` with one row: left `td.text-col` (45%) holding tag pills, a bullet list, optional `.example` italic line and a `.key-point` callout; right `td.viz-col` (55%) holding one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; ul 0.92rem with `li b` in `#1a5276`; `.metric strong` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, border-radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, with bold lead-in word. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (`canvas.width = 720*dpr`, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray arrows `#999`, dark text `#2c3e50`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
