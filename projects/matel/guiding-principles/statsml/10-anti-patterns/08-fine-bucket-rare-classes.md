# Fine-Bucket Rare Classes

**Page type:** detail page (anti-pattern/design-pattern pair: two `.card-section` blocks, each a two-column layout table — text left 45%, canvas right 55%)
**HTML title tag:** Fine-Bucket Rare Classes

**Subtitle:** 20 buckets × 0.01% positive = 12 positives per bucket — below any statistical minimum

## The Anti-Pattern

Too many buckets for rare events. Each bucket has 0–2 positives — unvalidatable noise.

**Key point (red-left-border callout):** Splitting rare positives across fine-grained buckets destroys the sample size needed for any meaningful statistical test.

*Domain examples:*

- Fraud detection (0.01–0.1% positive rate)
- Rare disease diagnosis
- Security attack detection
- Extreme class imbalance (< 1% prevalence)

### Visualization (canvas `c1`, 720×300)

20 fine-grained bucket bars, each holding 0–2 positives shown as red dots.

- **Title (bold 14px `#1a5276`, top center):** "20 Fine-Grained Buckets with Rare Positives".
- **Bars:** 20 buckets, 24px wide, 6px gaps, centered horizontally; uniform height 140px above baseline y = h−50. Fill `rgba(26, 82, 118, 0.2)`, stroke `#1a5276` width 1.
- **Positives:** per-bucket counts `[1, 0, 2, 0, 1, 0, 0, 1, 2, 0, 1, 0, 0, 1, 0, 2, 0, 1, 0, 0]`, drawn as 5px-radius red `#e74c3c` dots stacked inside the top of each bar (16px vertical spacing).
- **Count labels:** the count number in 11px above each bar — red `#e74c3c` when > 0, gray `#999` when 0.
- **Bucket labels:** every 4th bucket labeled "B1", "B5", "B9", "B13", "B17" in 10px `#666` below the baseline.
- **X-axis:** thin `#999` line along the baseline, extending 10px past the bars on each side.
- **Warning (bold 16px `#e74c3c`, centered 38px below baseline):** "n < 30 → unvalidatable!".
- **Legend (bottom-left):** a 5px red dot followed by "= 1 positive sample" in 11px `#666`.

## The Design Pattern

Fewer buckets (max = total_positives / 30). Or skip bucketing entirely — use Mann-Whitney rank test.

**Key point (red-left-border callout):** Constrain bucket count so each bucket has at least 30 positives for reliable inference.

- **Formula:** max_buckets = total_positives / 30
- If positives < 30, do not bucket at all
- Alternative: rank-based tests (Mann-Whitney) that don't require bucketing

### Visualization (canvas `c2`, 720×300)

3 coarse bucket bars, each with a visible green block of positives, plus a formula box.

- **Title (bold 14px `#1a5276`, top center):** "Fewer Buckets = Sufficient Positives Per Bucket".
- **Bars:** 3 buckets, 120px wide, 60px gaps, centered horizontally; uniform height 160px above baseline y = h−60. Fill `rgba(26, 82, 118, 0.15)`, stroke `#1a5276` width 1.5.
- **Positives:** counts `[8, 12, 10]` out of totals `[200, 250, 210]`; each bar has a solid green `#27ae60` block at its base with height (positives/total)×barHeight×8 (scaled for visibility), containing white bold 16px text "8 positives", "12 positives", "10 positives".
- **Labels:** "Bucket 1/2/3" in 12px `#1a5276` below each bar; "✓ n ≥ 8" in bold 13px `#27ae60` above each bar.
- **X-axis:** thin `#999` line along the baseline, extending 20px past the bars on each side.
- **Formula box:** 360×28 rectangle centered horizontally, 28px below the baseline; fill `#f0f7ec`, stroke `#27ae60` width 2, containing bold 13px `#1a5276` text: "max_buckets = positives / 30     |     60 positives → max 2 buckets".

## Regeneration instructions

- **Layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, then two `.card-section` divs ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a full-width `table.layout` with one row — `td.text-col` (45%) holding the paragraph, `.key-point` callout, optional `.example` italic lead-in and `<ul>`; `td.viz-col` (55%) holding one `<canvas width="720" height="300">`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; `.subtitle` `#666` 0.95rem; table cells padding 12px, vertical-align top; canvas `width:100%`, border `1px solid #e0e0e0`, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`.
- In regenerated HTML, any card links use `.html` extensions.
