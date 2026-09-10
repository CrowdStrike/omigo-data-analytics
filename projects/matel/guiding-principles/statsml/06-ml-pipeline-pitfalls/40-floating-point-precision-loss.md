# Pitfall: Floating Point / Precision Loss

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Floating Point / Precision Loss

**Subtitle:** When numerical precision issues create silent errors in aggregations, comparisons, and financial calculations.

## The Problem

Tags: `the trap` (red), `float math` (blue)

- **Inexact by design** — most decimal values have no exact binary form, so every op can round
- **Accumulation** — summing 1M float32 values of 0.01 yields 9865.22 instead of 10000, a 1.35% silent error
- **Comparison** — 0.1 + 0.2 != 0.3 in floats, so WHERE revenue = 0.3 matches nothing
- **Currency** — $19.99 is stored as 19.989999771 in float32, and rounding compounds downstream
- **Large IDs** — above 2^53 float64 rounds integers, so ID 9007199254740993 collides with another
- **Silent failure** — no exception is ever raised; the errors surface only as wrong numbers

*Example:* A float32 revenue SUM over 1M daily transactions averaging $25 comes out about 1.3% low — a six-figure daily discrepancy that no exception ever flags.

**Impact:** Financial reports come out off by thousands, user IDs silently collide, and features drift when batch and online scoring round differently.

### Visualization (canvas `c1`, 720×300)

Four outlined example boxes (2×2 grid) plus a solutions list.

- **Title (bold 14px, `#1a5276`, top center):** "Floating Point Precision Loss".
- **All four boxes:** 280×70, stroke `#1a5276` width 2, no fill; bold 12px `#1a5276` heading, then monospace 11px `#444` content, with red (`#e74c3c`) bold warnings.
  - Box 1 (x=50, y=60): "Example 1: Simple Addition"; monospace "0.1 + 0.2 = 0.30000000000000004"; red bold 11px "Not 0.3! Equality check fails."
  - Box 2 (x=390, y=60): "Example 2: Currency Sum"; monospace lines "$0.01 × 1M times (float32):" and "= $9865.22 (not $10000)"; red bold 10px "1.35% silent error".
  - Box 3 (x=50, y=150): "Example 3: Large Integer ID"; monospace "ID: 9007199254740993"; red bold 10px lines "Stored as: 9007199254740992" and "(precision loss beyond 2^53)".
  - Box 4 (x=390, y=150): "Example 4: Filter Fails"; monospace "WHERE price = 19.99"; red bold 10px lines "Matches nothing!" and "(stored as 19.989999771)".
- **Bottom (y≈240):** centered bold 12px `#27ae60` heading "Solutions:", then three left-aligned 11px `#444` bullets: "• Use DECIMAL/NUMERIC for currency (stores exact values)", "• Use INTEGER/BIGINT for IDs (no precision loss)", "• Use epsilon tolerance for comparisons: abs(a - b) < 1e-9".

## Why It Happens

Tags: `root cause` (orange), `IEEE 754` (blue)

- **Default type** — IEEE 754 floats are the default in nearly every language and database
- **Opt-out loss** — precision loss happens unless you actively choose exact numeric types
- **Binary fractions** — decimals like 0.1 and 19.99 have no exact binary representation
- **float32 aggregation** — using float32 for SUM/AVG lets error grow with every row added
- **NaN and Inf** — 1.0/0.0 gives Inf and 0.0/0.0 gives NaN, and both propagate silently
- **Round-trips** — JSON/CSV export truncates precision, so re-imported values differ

**Root Cause:** Each float operation adds a tiny binary rounding error, and millions of operations compound it into a plausible-looking wrong number rather than an error message.

### Visualization (canvas `c2`, 720×300)

Three red failure-mode boxes in a row.

- **Title (bold 14px, `#1a5276`, top center):** "Three Failure Modes".
- **All three boxes:** 200×110 at y=50, fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` width 2, bold 11px `#e74c3c` heading centered at top.
  - Box 1 (x=30): "float32 SUM" — a red curve (width 2) rising quadratically from lower-left to upper-right (y = base − i²·0.04 over 40 steps) with 10px `#666` caption "Error grows with N".
  - Box 2 (x=260): "Equality Check" — centered monospace 12px `#444` stacked lines "0.1 + 0.2", "!=", "0.3"; a red X (two crossed strokes, width 3) in the upper right of the box.
  - Box 3 (x=490): "Division by Zero" — centered monospace 11px lines "1.0 / 0.0 = Inf" and "0.0 / 0.0 = NaN"; a red rightward arrow (width 2, filled head) below them; 10px `#e74c3c` caption "Inf/NaN propagate through pipeline".
- **Bottom annotation (centered, bold 12px `#e74c3c`):** "All three are silent — no exception, no warning, just wrong results".

## The Correct Approach

Tags: `the fix` (green), `numeric types` (blue)

- **Right type per role** — choose the numeric type for the job instead of defaulting to float
- **Aggregations** — accumulate in float64 or use Kahan summation to cancel rounding error
- **Comparisons** — never join or filter on floats; compare with abs(a-b) < 1e-9 tolerance
- **Checks** — validate explicitly for NaN/Inf after every division or log operation
- **Currency** — store money as integer cents or DECIMAL, so $19.99 becomes exactly 1999
- **IDs** — keep identifiers in INTEGER/BIGINT columns so values past 2^53 never round

**Fix:** Assume every float operation can lose precision, and design pipelines to detect and prevent error accumulation.

### Visualization (canvas `c3`, 720×300)

2×2 grid of green solution boxes.

- **Title (bold 14px, `#1a5276`, top center):** "Defensive Numerical Programming".
- **All four boxes:** 300×100, grid origin (40, 50) with 20px gaps, fill `rgba(39,174,96,0.08)`, stroke `#27ae60` width 2; each has a large green checkmark "✓" (bold 20px) at the left, a bold 12px `#27ae60` heading, and two 11px `#444` detail lines.
  - Top-left: "Aggregations: float64" — "SUM, AVG, COUNT always in" / "double precision (64-bit)".
  - Top-right: "Currency: INTEGER (cents)" — "$19.99 stored as 1999" / "No precision loss ever".
  - Bottom-left: "Comparisons: |a-b| < epsilon" — "Never use == on floats" / "Tolerance: 1e-9 or relative".
  - Bottom-right: "Post-op: check NaN/Inf" — "After every division/log:" / "assert isfinite(result)".
- **Bottom annotation (centered, bold 13px `#1a5276`):** "Defensive numerical programming".

## Regeneration instructions

- **Layout:** three `.card-section` blocks ("The Problem", "Why It Happens", "The Correct Approach"), each an h2 with `2px solid #2980b9` bottom border, followed by a full-width `table.layout` with one row: left `td.text-col` (45%) holding tag pills, a bullet list, optional `.example` italic line and a `.key-point` callout; right `td.viz-col` (55%) holding one canvas.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; subtitle `#666` 0.95rem; ul 0.92rem with `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, border-radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, with bold lead-in word. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (`canvas.width = 720*dpr`, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#444`/`#666`; monospace font for code snippets inside canvases. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
