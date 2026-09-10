# Streaks & the Clustering Illusion

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Streaks & the Clustering Illusion

**Subtitle:** Real coin flips come in clumps and streaks — data that looks "too even" is the suspicious kind.

## Which of These Was Made by a Real Coin?

**Tags:** `core idea` (blue), `running example` (green)

- **Two sequences** — 100 flips each: one from a real fair coin, one typed by a person faking it
- **The fake** — alternates politely; its longest streak is just 2 in a row
- **The real one** — contains a streak of 7 heads and another of 6 — from a fair coin
- **The instinct** — streaks feel non-random, so fakers avoid them and give themselves away
- **The name** — reading meaning into random clumps is the clustering illusion

*Example:* Statistics teachers spot faked coin-flip homework instantly: the fake never streaks.

**Key point:** Randomness has no memory, so it repeats itself freely — clumps and streaks are its signature, not its opposite.

### Visualization (canvas `c1`, 720×300)

Two horizontal 100-cell strips of H/T flips, with runs of 5+ highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "100 Flips: Human Fake vs Real Fair Coin".
- **Fake sequence (top strip at y=80, title bold 13px magenta `#d55181`: "typed by a person \"at random\" — longest streak: 2"):**
  `HTHTTHTHHTHTHTTHHTHTHHTTHTHTHHTHTTHTHTHHTTHHTHTHTTHTHHTHTHTTHHTHTHTHHTTHTHHTHTHTHHTHTTHTHHTHTTHHTHTT`
- **Real sequence (bottom strip at y=185, title bold 13px green `#008300`: "real fair coin — streaks of 6 and 7 heads"):**
  `HHTHTHHTHTHHHTTTHTHHTTHTTTTHTTHTTHTTTHHHHHHTHHTHHTHTTHTTHTHTTHTHTTHHTHTHHTHHHHHHHTTHTTHTTTHHTTTHTHHT`
- **Strip rendering:** 100 cells starting at x=60, 6px per cell, 30px tall; heads = solid blue `#2a78d6`, tails = light `rgba(42,120,214,0.15)`.
- **Run highlighting:** every run of 5+ identical flips gets a 3px orange `#d95926` outline box and a bold 12px orange label beneath it reading "N-streak" (computed from the sequences: the real strip shows its 6- and 7-streaks; the fake has none).
- **Footnotes:** 12px gray `#6b7280` bottom-left "dark = heads, light = tails"; bold 13px orange bottom-right "the streaky one is the real coin".

## Counting the Streaks by Hand

**Tags:** `worked example` (green), `hand math` (blue)

- **One window** — 5 flips in a row all matching: chance is 2 × (1/2)^5 = 1/16
- **Many windows** — 100 flips contain 96 overlapping windows of length 5
- **Expected clumps** — 96 × 1/16 = 6 all-same windows expected per 100 flips
- **Our real sequence** — has 5 such windows, right in line with the math
- **Almost guaranteed** — roughly 97 in 100 sequences contain at least one 5+ streak

*Example:* If your "100 coin flips" has no streak of 5, that absence is itself the ~3-in-100 event.

**Key point:** A 5-streak in 100 flips isn't a red flag — its absence is.

### Visualization (canvas `c2`, 720×300)

Bar chart: distribution of the longest streak in 100 fair flips.

- **Title (bold 15px, `#1a5276`, top center):** "Longest Streak in 100 Fair Flips — Share of Sequences (illustrative)".
- **Data:** categories `['4 or less', '5', '6', '7', '8', '9', '10', '11+']`, shares `[3, 14, 26, 24, 15, 9, 5, 4]`%. First bar gray `rgba(107,114,128,0.45)`, the rest blue `#2a78d6`. Bars 62px wide, evenly spaced across the plot.
- **Axes:** y 0–30% with gridlines/labels every 10%; padding top 50, bottom 56, left 62, right 30; grid `#e5e9ef`, axis `#999`. Value labels bold 12px `#2c3e50` above bars; category labels 12px gray below; x caption "longest streak (heads or tails) in the 100 flips".
- **Bracket annotation:** green `#008300` bracket (width 2) spanning bars "5" through "11+" at the top, with bold 13px green text centered beneath it: "~97% of sequences contain a streak of 5 or more".

## Clumps in Your Dashboard

**Tags:** `where it's used` (blue), `common mistake` (red)

- **The timeline** — 25 random alerts over 60 days: a 9-day silence, then 5 alerts in 3 days
- **The meeting** — "what changed this week?" Often the honest answer is: nothing, that's chance
- **Hot hands** — a salesperson closing 4 deals in a row usually needs no special explanation
- **The test** — before explaining a cluster, ask how clumpy pure chance looks at this rate
- **Real signals exist** — but they must beat the clumpiness chance produces for free

*Example:* WWII bomb strikes on London looked targeted by neighborhood; analysis showed the pattern matched pure chance.

**Key point:** Every random process produces its own false alarms — know that baseline before you page the on-call.

### Visualization (canvas `c3`, 720×300)

Event timeline: 25 random alerts on a 60-day axis with clusters and a quiet gap annotated.

- **Title (bold 15px, `#1a5276`, top center):** "25 Alerts Dropped Uniformly at Random on 60 Days".
- **Data (uniform random times, generated once with a fixed seed):** day positions `[3.5, 4.5, 5.1, 5.7, 6.8, 8.4, 11.7, 19.4, 19.6, 23.9, 27.2, 28, 29.2, 30.2, 31.8, 31.9, 34.6, 34.6, 36.5, 39.2, 42.8, 45.4, 49.3, 50.1, 59.2]`.
- **Timeline:** horizontal axis at midY=150 from x=60 to x=680, tick marks and "day 0".."day 60" labels every 10 days (12px gray). Each alert is a vertical blue `#2a78d6` tick (width 2.5, 26px tall) rising above the line.
- **Cluster halos:** dashed orange `#d95926` ellipses (dash 6/4, width 2.5) around days 3.5–6.8 labeled bold 13px orange "5 alerts in 3 days", and around days 27.2–31.9 labeled "6 alerts in 5 days" (labels at y=80).
- **Quiet gap:** aqua `#199e70` underline (width 2) between days 50.1 and 59.2 below the axis, labeled bold 13px aqua "9 quiet days".
- **Bottom annotation (bold 13px orange, centered, y=272):** "both \"incidents\" are pure chance — no cause exists to find".

## Even Spacing Is the Designed Look

**Tags:** `common confusion` (orange)

- **The flip side** — points spread suspiciously evenly were almost certainly arranged
- **Random points** — land independently, so some pairs sit close and some areas stay empty
- **Spread-out points** — need a rule pushing them apart, and rules are the opposite of chance
- **Music shuffle** — players famously re-space same-artist songs because true shuffle felt broken
- **Quick check** — clumpy scatter and uneven gaps suggest chance; too-neat suggests human hands

*Example:* Users reported shuffle as "broken" when one artist played twice in a row — that's just what shuffle does.

**Key point:** If the data looks perfectly spread out, suspect a person; if it looks clumpy, suspect a coin.

### Visualization (canvas `c4`, 720×300)

Two-panel scatter: truly random points vs evenly arranged points.

- **Title (bold 15px, `#1a5276`, top center):** "40 Points, Two Ways: Which Panel Is Random?".
- **Left panel** (280×190 box at (65, 55), light gray `#bbb` border, caption bold 13px blue `#2a78d6` below: "independent random points — clumps + empty zones"): 40 blue 4px dots at unit-square coordinates (generated once with a fixed seed, hardcoded): `[[0.09,0.07],[0.63,0.31],[0.32,0.21],[0.51,0.95],[0.83,0.01],[0.71,0.21],[0.09,0.99],[0.84,0.47],[0.74,0.06],[0.45,0.66],[0.99,0.21],[0.85,0.23],[0.19,0.78],[0.16,0.72],[0.37,0.51],[0.95,0.77],[0.89,0.91],[0.65,0.81],[0.71,0.22],[0.31,0.52],[0.9,0.95],[0.81,0.65],[0.32,0.91],[0.2,0.39],[0.8,0.16],[0.68,0.9],[0.58,0.53],[0.75,0.36],[0.29,0.05],[0.01,0.2],[0.65,0.09],[0.58,0.94],[0.69,0.18],[0.17,0.73],[0.55,0.31],[0.07,0.83],[0.73,0.84],[0.71,0.02],[0.17,0.66],[0.44,0.13]]`, mapped into the panel with a 10px inset. A dashed orange `#d95926` ellipse (46×26) circles the clump near (0.73, 0.09) with bold 12px orange label "a clump" beneath.
- **Right panel** (280×190 box at (385, 55), caption bold 13px violet `#4a3aa7`: "evenly arranged points — needs a rule to make"): 40 violet 4px dots on an 8×5 grid (24px/22px insets) with small deterministic jitter `jx = sin(k*3.7)*5`, `jy = sin(k*5.3)*4` per point index k.
- **Bottom annotation (bold 13px orange, centered, y=292):** "the clumpy left panel is the random one".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 + `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: `.text-col` td (50%) and `.viz-col` td (50%), 12px padding.
- **Left column structure per section:** a `.tags` row of colored pill spans, a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold terms in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) starting with `<strong>Key point:</strong>`.
- **Tag pill styles:** 0.72rem, weight 600, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** each 720×300 intrinsic, `width:100%` CSS, `1px solid #e0e0e0` border, radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). All data as hardcoded literal strings/arrays — no `Math.random()`; c1 computes run spans of length ≥5 from the two flip strings.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
