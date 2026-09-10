# Underpowered Tests (Too Small N)

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Underpowered Tests — A/B Testing Pitfalls

**Subtitle:** Statistical Sin — N=500 when you need N=50,000. "No effect found" means nothing.

## Section 1: The Problem

- Power analysis says 50K per arm to detect 3% lift. You run with 500. See no significance. "A/B test showed no difference — ship whichever."
- At N=500, you can only detect MASSIVE effects (>20%). A real 3% lift is invisible. Absence of evidence ≠ evidence of absence.
- **The flip side:** Small N tests that DO show significance → effect is inflated (winner's curse). Signal must be enormous to overcome noise in small samples.
- **The misunderstanding:** "We don't have enough traffic for a big test." Then DON'T test. Running an underpowered test gives worse-than-no-information: it gives FALSE confidence that there's "no effect."

**Correct approach:** Power analysis BEFORE the test. If you can't reach required N in reasonable time, don't run the test. Use qualitative research, expert review, or historical analysis instead.

**The tell:** Ask "what's the minimum detectable effect at this sample size?" If the answer is >10% for a UI change — the test can only detect changes so large you wouldn't need a test to notice them.

### Visualization (canvas `c1`, 720×340)

Two overlapping bell curves with a tiny mean shift, showing that a small real effect is invisible at low N.

- **Curves:** two Gaussians over x ∈ [−3.5, 3.5], σ=1; Control at μ=0, Treatment at μ=0.03; 200 sampled segments each; plot area 500×140 starting at y=30, horizontally centered; curve height scale ×2.5.
- **Control:** stroke `#1a5276` width 2.5, fill under curve `rgba(26,82,118,0.15)`.
- **Treatment:** stroke `#e74c3c` width 2.5, fill `rgba(231,76,60,0.10)`.
- **Legend labels (bold 16px, top-left of plot):** "Control" in `#1a5276` at plot x+10; "Treatment (+3%)" in `#e74c3c` at plot x+90.
- **Shift arrow:** tiny orange (`#e67e22`, width 2) rightward arrow at plot center, 20px below plot baseline, spanning only ±5px with a small arrowhead — visually emphasizing the tiny shift.
- **Captions (bottom center, italic 14px `#666`, two lines):** "With N=500: these look identical. Need N=50,000 to tell them apart." / "Your test detected nothing — but the effect IS there."

## Section 2: Real Example: Bing's Wins Are Tiny

- Microsoft's experimentation team, which has run thousands of tests on Bing, published that most ideas simply don't work, and even the successful ones usually improve key metrics by only about 1-2%.
- A team that sizes its test to spot a 10% jump (because that needs far less traffic) will therefore miss almost every improvement that actually exists — the test reports "no difference" and a genuinely good idea gets thrown away.
- The remedy Microsoft describes is to decide up front how small a lift is worth detecting, then gather enough users to see it, even when that means running the test much longer.

### Visualization (canvas `c2`, 720×300)

Bar chart of realistic winning lifts vs the detection floor of an underpowered test.

- **Title (bold 17px `#2a2a2a`, top center at x=360, y=28):** "Real Winning Ideas at Bing Move Metrics ~1-2%".
- **Bars:** six bars, values `[0.8, 1.2, 1.5, 2.0, 1.0, 1.8]` percent; scale max 12% over 160px plot height; baseline y=225; bars start at x=90, width 60, gap 30; fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1.5; value labels ("0.8%" etc., 14px `#1a5276`) above each bar; x-axis captions "idea 1"…"idea 6" in `#666` below.
- **Baseline:** thin gray (`#999`) horizontal line from x=70 to x=630 at y=225.
- **Detection floor:** dashed red line (`#e74c3c`, dash 6/4, width 2) at 10% height, full plot width, labeled above in bold 15px red: "smallest lift a small (underpowered) test can see: 10%".
- **Takeaway (bottom center, 15px `#333`):** "Every real winner sits below the line — the small test calls all of them "no difference"".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this detail page has no links).
