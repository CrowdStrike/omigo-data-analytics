# Pitfall: Non-Representative Training Data

**Page type:** detail page (card-sections with h2 headers, two-column layout table per section: text left 45%, canvas right 55%)
**HTML title tag:** Non-Representative Training Data

**Subtitle:** Model trained on one population but deployed on a different population with different characteristics

## The Problem

Tags: `the trap` (red), `sampling bias` (blue)

- **Convenient slice** — training uses whatever data was easiest to get, not the full population
- **Population subset** — a model trained on power users meets novices it has never seen
- **Geographic bias** — US-only training data misses patterns when deployed globally
- **Temporal bias** — a summer-trained model fails when winter shifts the distribution
- **Completeness bias** — trained on complete records, it never learned sparse inputs
- **Quiet failure** — patterns hold only in the sampled slice, so it underperforms elsewhere

*Example:* A loan model trained on FICO > 700 borrowers scores 40% worse on the FICO < 650 segment.

**Impact:** Aggregate metrics look acceptable while hiding severe performance gaps in underrepresented subgroups.

### Visualization (canvas `c1`, 720×300)

Population overlap (Venn-style circle) diagram of training vs production coverage.

- **Title (bold 14px, top center, `#1a5276`):** "Training vs Production Population Mismatch".
- **Production circle:** large circle, center (360, 150), radius 100; fill `rgba(231,76,60,0.2)`, stroke `#e74c3c` width 3. Labels above center in red: bold 13px "PRODUCTION", then 11px lines "All users", "All regions", "All time periods".
- **Training circle:** smaller circle inside, center (320, 170), radius 60; fill `rgba(26,82,118,0.4)`, stroke `#1a5276` width 3. Labels in blue: bold 12px "TRAINING", then 10px "Power users", "US only".
- **Underrepresented segments:** three small dashed-outline circles (radius 20, fill `rgba(231,76,60,0.4)`, stroke `#e74c3c` width 2 dashed 4/3) at (420,100) labeled "Novice / users" (two lines), (450,160) labeled "APAC", (420,210) labeled "Seasonal / patterns" (two lines); labels bold 9px red centered.
- **Warning annotation (bold 11px red, left-aligned at x=50, y≈240-270, three lines):** "Model trained on small biased sample" / "Fails on underrepresented segments" / "(shown as dashed circles)".

## Why It Happens

Tags: `root cause` (orange), `convenience data` (blue)

- **Availability rules** — collection follows convenience and precedent, not the target population
- **Early-adopter data** — the first data comes from beta testers who behave unlike typical users
- **Survivorship bias** — only completed outcomes leave records; failed cases never enter training
- **Convenience sampling** — teams grab the easiest data instead of sampling from production
- **Historical bias** — past data reflects old products and user mixes production no longer has
- **No internal evidence** — a biased sample looks complete from inside, so nobody questions it

*Example:* A model trained on 80% iPhone users deploys to a 60% Android market and scores 25% lower there.

**Root Cause:** Data availability drives sampling, and a biased sample contains no internal evidence of its own bias.

### Visualization (canvas `c2`, 720×300)

Bar chart of F1 score by user segment, exposing hidden per-segment gaps behind a healthy aggregate.

- **Title (bold 14px, top center, `#1a5276`):** "Hidden Performance Gaps by Segment".
- **Plot area:** left=100, right=660, top=70, bottom=240; gray (`#999`) L-shaped axes; light gray (`#e0e0e0`) horizontal gridlines at each y label.
- **Axes:** x label "User Segment" centered below; rotated y label "F1 Score"; y tick labels 0% to 100% in 20% steps (11px `#666`).
- **Bars (width 70, value label in white bold 11px inside bar top, segment name in 10px below axis, plus a gray 9px "N% of / training" note under each):**

| Segment | F1 | Bar color | % of training |
|---|---|---|---|
| Aggregate (reported) | 82% | `#27ae60` | 100% |
| Power Users | 88% | `#1a5276` | 80% |
| Regular Users | 76% | `#e67e22` | 15% |
| Novice Users | 52% | `#e74c3c` | 5% |
| APAC | 48% | `#e74c3c` | 2% |

- **Threshold line:** horizontal dashed green (`#27ae60`, width 2, dash 6/4) at 75%, labeled "Target: 75%" in 10px green at left.

## The Correct Approach

Tags: `the fix` (green), `stratified sampling` (blue)

- **Design it** — treat the production population as the sampling frame, not an afterthought
- **Profile production** — measure segment, geography, device, and time mix before training
- **Stratify** — match the training distribution to production, segment by segment
- **Fill gaps** — collect targeted samples or apply weights where minority segments lack data
- **Evaluate per segment** — report metrics broken down by segment, never only in aggregate
- **Detect** — compare training vs production distributions; large gaps signal bias

*Example:* A fraud model stratified by transaction size reveals high-value transactions need more features.

**Fix:** Maintain a training-vs-production representation matrix and flag any segment whose performance delta exceeds an agreed tolerance.

### Visualization (canvas `c3`, 720×300)

Stratified-sampling match table plus best-practices bullet list.

- **Title (bold 14px, top center, `#1a5276`):** "Correct Approach: Stratified Sampling".
- **Table:** header row (bold 11px `#1a5276`) with columns "Segment" (left-aligned at x=80), "Production %" (centered at x=280), "Training %" (centered at x=480); rows 28px tall spanning x=60 to 620, alternating `#f8f9fa`/white with `#e0e0e0` borders, text 11px `#2c3e50`; each row ends with a green (`#27ae60`) checkmark indicating a match:

| Segment | Production % | Training % | Match |
|---|---|---|---|
| Power Users | 20% | 20% | yes |
| Regular Users | 50% | 50% | yes |
| Novice Users | 25% | 25% | yes |
| Other | 5% | 5% | yes |

- **Best practices list (below table):** heading bold 12px `#1a5276` "Best Practices:", then four 10px `#2c3e50` items each with a small green (`#27ae60`) bullet dot:
  - Match training distribution to production distribution
  - Ensure minimum 100-1000 samples per segment
  - Use weighting if segments are too small
  - Report per-segment metrics, not just aggregate

## Regeneration instructions

- **Layout:** repeated `.card-section` blocks, one per section. Each has an `<h2>` (1.3rem `#1a5276`, bottom border `2px solid #2980b9`) followed by a `table.layout` (full width, border-collapse) with a single `<tr>`: left `td.text-col` (45%) containing `.tags` pills, a `<ul>` of labeled bullets, a `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) containing one `<canvas width="720" height="300">`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `2px solid #2980b9` bottom border. `.subtitle` `#666` 0.95rem. `ul` 0.92rem with `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; intrinsic size 720×300, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
