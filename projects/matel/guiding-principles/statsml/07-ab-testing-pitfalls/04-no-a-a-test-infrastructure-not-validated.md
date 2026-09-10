# No A/A Test (Infrastructure Not Validated)

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** No A/A Test — A/B Testing Pitfalls

**Subtitle:** Statistical Sin — Never verified that the testing system itself works correctly.

## Section 1: The Problem

- An A/A test: run your test framework with IDENTICAL experiences in both arms. If the system works correctly, you should see NO significant difference (at most 5% of A/A tests should show p<0.05).
- If your A/A tests show significance 20-30% of the time → your ENTIRE testing infrastructure is broken.
- **What A/A catches:** Randomization bugs, logging discrepancies, unequal bot traffic, sample ratio mismatch (SRM), clock synchronization issues, cookie churn differences.
- **The devastating implication:** If you've NEVER run an A/A test, you have NO IDEA if your A/B test results are valid. Every "significant" result you've ever shipped could be an infrastructure artifact.
- **Common finding:** Companies that run their first A/A test discover their framework shows significant differences 15-25% of the time. YEARS of past tests are now questionable.

**Correct approach:** Run A/A tests REGULARLY (monthly). If >5% show significance, debug the infrastructure before running any A/B test. A/A is your calibration — without it, your instrument is uncalibrated.

**The tell:** Ask "how often do you run A/A tests?" If the answer is "never" or "what's that?" — every A/B test result they've ever produced is suspect.

### Visualization (canvas `c1`, 720×340)

Two side-by-side p-value histograms: healthy uniform vs broken low-clustered.

- **Left histogram (healthy):** plot area x=40, y=30, 280×140; title above in bold 16px `#27ae60`: "Healthy A/A: Uniform p-values"; 10 bins with counts `[12, 10, 11, 9, 13, 10, 11, 12, 9, 10]`, scale max 15; bars filled `rgba(39,174,96,0.5)`, stroked `#27ae60` width 1, 2px gap.
- **Right histogram (broken):** plot area x=390, y=30, 280×140; title in bold 16px `#e74c3c`: "Broken: p-values cluster low"; 10 bins with counts `[28, 22, 15, 10, 8, 5, 4, 3, 3, 2]`, scale max 30; bars filled `rgba(231,76,60,0.5)`, stroked `#e74c3c`.
- **Axes (both):** gray `#666` L-shaped axes; x labels "0", "p-value", "1" (16px `#666`) at left, center, right below the baseline.
- **Caption (bottom center, italic 14px `#666`):** "If A/A shows significance, your entire A/B testing history is unreliable."

## Section 2: Real Example: Microsoft's Always-On A/A Tests

- Microsoft's experimentation team continuously runs tests where both groups see the exact same experience, so any "difference" that shows up must be a bug in the machinery rather than a real effect.
- These deliberately boring tests have caught real problems — users not being split evenly between groups, and one group's activity being recorded differently — as documented by Ron Kohavi and colleagues.
- Kohavi also reports that teams running this check for the first time are often shocked: the identical groups come out "different" far more often than the expected 1 in 20, which means their past A/B results were never trustworthy.

### Visualization (canvas `c2`, 720×300)

Timeline of continuous A/A runs acting as a smoke alarm: a row of "ok" blocks with one alert.

- **Title (bold 17px `#2a2a2a`, top center at x=360, y=28):** "Always-On A/A Tests: A Smoke Alarm for the Test System".
- **Blocks:** 12 blocks in a row starting at x=55, y=95, each 45px wide (51px pitch minus 6px gap) × 60px tall. Eleven "ok" blocks: fill `rgba(39,174,96,0.2)`, stroke `#27ae60` width 2, centered bold 15px green text "ok". One alert block at index 7: fill `rgba(231,76,60,0.3)`, stroke `#e74c3c`, bold red "!".
- **Alert callout:** short vertical red line rising from the alert block to a bold 15px red label (offset left): ""identical" groups differ — randomization/logging bug caught".
- **Time axis:** thin gray (`#999`) horizontal arrow below the blocks pointing right; label below in 14px `#666`: "time (A/A tests running continuously alongside real A/B tests)".
- **Takeaway (bottom center, 15px `#333`):** "The bug is found by a test that should show nothing — before it quietly poisons real A/B decisions".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this detail page has no links).
