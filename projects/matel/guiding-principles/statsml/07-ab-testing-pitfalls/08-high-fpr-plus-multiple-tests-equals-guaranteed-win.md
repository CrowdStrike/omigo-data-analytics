# High FPR + Multiple Tests = Guaranteed "Win"

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** High FPR + Multiple Tests — A/B Testing Pitfalls

**Subtitle:** Statistical Sin — 20% false positive rate x 5 tests = 67% chance of finding a "significant" winner.

## Section 1: The Problem

- Most teams don't properly compute sample sizes, resulting in actual false positive rates of 15-20% (not 5%). Peeking, multiple metrics, wrong tests — all inflate FPR. Now run 5 such tests.
- **The math:** If each test has 20% FPR, probability of AT LEAST ONE showing significance = 1 - (1-0.20)^5 = 1 - 0.328 = 67%.
- **The organizational pattern:** Team runs 5 experiments per quarter. Each is underpowered (actual FPR ~20%). Each quarter, 1-2 "win." These get shipped. The wins are NOISE — mathematically guaranteed by running multiple bad tests.
- **Compounding:** Ship the false positives. They don't actually work. Metrics don't improve. But: "the A/B test showed it worked!" becomes the defense. The false positive is now in production forever.

**Correct approach:** Get your individual test FPR down to 5% FIRST (proper sample, proper duration, no peeking, validated via A/A tests). THEN worry about multiple testing correction. If your base FPR is 20%, no correction saves you.

**The tell:** How many tests per quarter? What % "win"? If >50% win → either extraordinary at picking winners (unlikely) or individual test FPR is inflated. True win rate with proper testing: 30-40%.

### Visualization (canvas `c1`, 720×340)

Probability-tree diagram: one inflated single-test FPR fanning out to 5 test boxes, converging to the combined probability of at least one fake winner.

- **Left label (bold 16px red `#e74c3c`, left-aligned at x=40):** two lines "P(single test FP)" / "= 20%", with 16px gray `#666` line below: "(from bad methodology)".
- **Test boxes:** 5 stacked boxes at x=220, each 70×32 with 8px vertical gaps starting at y=25; fill `rgba(26,82,118,0.15)`, stroke `#1a5276` width 1, 16px `#1a5276` centered labels "Test 1" … "Test 5". Thin gray `#999` connector lines from the left label area (x=200, vertical center of canvas) fanning to each box's left edge.
- **Right result (bold 17px red `#e74c3c`, left-aligned at x=380):** two lines "P(at least 1 "winner")" / "= 67%"; a red arrow (width 2, with arrowhead) from the right of the test boxes to the result at the canvas vertical center.
- **Formula (17px `#1a5276` at x=380, y=100):** "1 - (1-0.20)⁵ = 1 - 0.328 = 0.67".
- **Caption (bottom center, italic 14px `#666`, two lines):** "Run enough bad tests → guaranteed to find "significant" results by pure chance." / "This is not science. This is a random number generator that occasionally says "yes.""

## Section 2: Real Example: Bing's Lucky Winners

- Microsoft's Bing team runs thousands of experiments a year, and they have written openly about a hard lesson from that scale: when you try many variants and slice the results by many user groups, some combination will look like a winner purely by luck.
- Their published practice is to re-run surprising wins before believing them, because a repeat test is the cheapest way to tell a lucky fluke from a real improvement — and many celebrated "wins" quietly vanish on the second run.
- The plain intuition is a coin-flip contest: let enough people flip ten coins each and someone will get nine heads. That person is not a coin-flipping genius, and neither is the lucky variant.

### Visualization (canvas `c2`, 720×300)

Bar chart: probability of at least one false positive vs number of tests run (at 5% error per test).

- **Title (bold 17px `#2a2a2a`, centered at x=360, y=28):** "Chance of at Least One Fake "Winner" (5% error per test)".
- **Data:** number of tests `[1, 5, 10, 20, 40]` → probability values `[5, 23, 40, 64, 87]` (percent).
- **Bars:** width 70, centered at x = 95, 225, 355, 485, 615; baseline y=230, height scaled as value/100 × 160. First bar green (fill `rgba(39,174,96,0.35)`, stroke `#27ae60`); last bar red (fill `rgba(231,76,60,0.35)`, stroke `#e74c3c`); middle bars blue (fill `rgba(26,82,118,0.35)`, stroke `#1a5276`); stroke width 2.
- **Labels:** bold 16px value labels ("5%", "23%", "40%", "64%", "87%") above each bar in the bar's stroke color; 14px `#333` x labels below baseline: "1 test", "5 tests", "10 tests", "20 tests", "40 tests".
- **Baseline:** thin gray `#999` line from x=40 to x=680 at y=230.
- **Takeaway (bottom center, italic 14px `#666`):** "The more variants you try, the more certain a lucky fake winner becomes — retest before you believe".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; lists 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#333`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
