# Twyman's Law Ignored

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Twyman's Law Ignored — A/B Testing Pitfalls

**Subtitle:** Organizational — Result too good to be true? Ship before anyone questions it!

## Section 1: Organizational — Result too good to be true? Ship before anyone questions it!

- +40% conversion from button color. Correct response: "Instrumentation is broken." Actual: "SHIP NOW!"
- Twyman's Law: any interesting/unusual statistic is probably wrong. 40% from button color violates everything about UX.
- Common causes: logging bug (double-count), bot traffic in one arm, inconsistent triggers, user duplication.

**Correct approach:** For any effect >3x expected, investigate MEASUREMENT before celebrating. Is it physically plausible?

**The tell:** Is effect size plausible for the change made? Button color → 40% = broken measurement, guaranteed.

### Visualization (canvas `c1`, 720×340)

Bell curve of realistic A/B effect sizes with a far-right outlier dot, on light gray background (`#f8f9fa`).

- **Chart area:** x=40, width 380, top y=30, height 150; gray baseline axis (`#999`, width 1) along the bottom.
- **Bell curve:** Gaussian with mean 1.5, sd 1.5 over an x-range of −2% to 8% (drawn across the left 60% of the chart width, peak height chartH−20); stroke `rgba(26,82,118,0.8)` width 2, fill `rgba(26,82,118,0.15)`, closed to the baseline.
- **Axis labels (16px `#666`, centered below baseline):** "-2%", "0%", "3%", "5%".
- **Curve label (16px `#1a5276`, centered near top of chart):** "Realistic: 1-5%".
- **Outlier:** red `#e74c3c` filled dot, radius 8, at 88% of chart width, 70% of chart height above baseline; short red arrow (width 2) pointing to it from lower-left; bold 16px red label "+40%" above the dot.
- **Right-side text block (left-aligned at x=w−200):** bold 18px red lines "This is a" (y=50) / "measurement bug." (y=66); then 16px blue `#1a5276` lines "Realistic: 1-5%" (y=100) and "If you see 40%:" (y=120); then bold 18px blue "check your pipes." (y=140).
- **Bottom label (17px `#555`, centered, 10px above bottom):** "Twyman's Law: any interesting statistic is probably wrong"

## Section 2: Real Example: Bing's Too-Good-To-Be-True Wins

- Microsoft's Bing team runs thousands of experiments a year, and Ron Kohavi, who led their experimentation platform, wrote that the most spectacular results were almost never real.
- Time after time, a metric that jumped overnight traced back to something mundane like a broken logging pipeline counting clicks twice, or automated bot traffic landing mostly in one group. (These are instrumentation errors — the measuring tool failing, not users actually changing.)
- Their standing rule became simple: the more exciting the number looks, the more effort goes into checking how it was measured before anyone is allowed to celebrate.

### Visualization (canvas `c2`, 720×300)

Time-series line chart: metric flat, sudden spike during a bug window, back to flat after the fix. Light gray background (`#f8f9fa`).

- **Title (bold 17px `#2a2a2a`, centered at y=28):** "The Overnight Miracle That Was a Logging Bug".
- **Axes:** L-shaped gray `#999` width 1: vertical at x=70 from y=55 to y=240, horizontal baseline from x=70 to x=660 at y=240.
- **Bug window:** shaded rectangle fill `rgba(231,76,60,0.08)` from x=350 to x=510, spanning y=55 to y=240.
- **Metric line:** blue `#1a5276`, width 2.5, through points `[(70,190),(150,188),(230,191),(310,189),(390,92),(470,90),(550,187),(630,189)]` (canvas coordinates; flat → spike → flat).
- **Spike markers:** red `#e74c3c` filled dots, radius 5, at (390,92) and (470,90).
- **Annotations:** bold 15px red at (430, 70): "\"Huge win!\" — clicks counted twice"; 14px green `#27ae60` at (575, 165): "bug fixed, \"win\" vanishes".
- **X-axis label (14px `#666`, centered below baseline):** "days".
- **Takeaway (15px `#555`, centered, 10px above bottom):** "The metric jumped because the measurement broke, not because users changed."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`/`#555`.
