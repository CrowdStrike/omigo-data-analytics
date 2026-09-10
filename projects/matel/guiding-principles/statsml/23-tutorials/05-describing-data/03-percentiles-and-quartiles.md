# Percentiles & Quartiles

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Percentiles & Quartiles

**Subtitle:** Sort the data, then read off positions — p50 is the middle, p95 is where the slowest 5% begins

**Shared data (used across charts):** the 20 sorted page-load times `LOADS = [0.7, 0.8, 0.9, 1.0, 1.0, 1.0, 1.1, 1.1, 1.2, 1.2, 1.2, 1.3, 1.4, 1.6, 1.8, 2.2, 2.6, 3.2, 4.8, 9.0]` (seconds).

## Twenty Page Loads, Read as Positions

**Tags:** `core idea` (blue), `running example` (green)

- **The data** — 20 timed page loads, from 0.7s up to one slow 9.0s load
- **p50 = 1.2s** — half the loads finish in 1.2s or less; p50 is just the median
- **p95 = 4.8s** — 95% of loads are at or below it; only the 9.0s one is beyond
- **Reading "pX"** — the value that X% of the data sits at or below
- **The mean is 2.0s** here — yet 15 of the 20 loads were faster than that

*Example:* "Half our pages load under 1.2 seconds, but 1 in 20 users waits about 5 seconds or more."

**Key point:** A percentile is a position in the sorted line-up. No formula — sort, count, read off the value.

### Visualization (canvas `c1`, 720×300)

Histogram of the 20 load times with p50 / mean / p95 markers.

- **Title (bold 15px, `#1a5276`, top center):** "20 Page Loads: Most Are Fast, a Few Drag a Long Tail".
- **Axes:** L-shaped axis (`#999`), padding top 52 / bottom 52 / left 55 / right 25; x from 0 to 9.5s with tick labels "1s", "3s", "5s", "7s", "9s"; axis title "load time" bottom center; y count scale 0–10 with labels 0, 5, 10 (12px `#6b7280`).
- **Bars:** LOADS binned into 0.5s bins starting at 0.5s; bars filled blue `#2a78d6`, except bins at ≥4.5s filled orange `#d95926`; each nonzero bin labeled with its count above the bar (12px `#2c3e50`). Bin counts (0.5s bins from 0.5): [1, 5, 5, 3, 1, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1] — the 9.0s load lands in the last bin.
- **Markers** (vertical dashed lines, width 2, dash 6/4, label to the right in bold 12px):
  - green `#008300` at 1.2 labeled "p50 = 1.2s"
  - violet `#4a3aa7` at 2.0 labeled "mean = 2.0s"
  - orange `#d95926` at 4.8 labeled "p95 = 4.8s"
- **Annotation (bold 13px orange, mid-plot right of x=2.4):** "half load under 1.2s — but 1 in 20 waits ~5s or more".

## Finding p50, the Quartiles, and p95 by Hand

**Tags:** `worked example` (green)

- **Sort** the 20 load times from fastest to slowest, positions #1 to #20
- **p50** — halfway: average positions #10 and #11: (1.2 + 1.2) ÷ 2 = 1.2s
- **Q1 (p25)** — a quarter of the way: positions #5–#6 → (1.0 + 1.0) ÷ 2 = 1.0s
- **Q3 (p75)** — three quarters: positions #15–#16 → (1.8 + 2.2) ÷ 2 = 2.0s
- **p95** — 95% of 20 = position #19 → 4.8s; only #20 (9.0s) lies beyond

*Example:* "p95 = 4.8s" literally means: of these 20 loads, only the 9.0-second one was slower.

**Key point:** Quartiles are just the p25 / p50 / p75 percentiles — they cut the sorted data into four equal groups of 5.

### Visualization (canvas `c2`, 720×300)

Sorted rank strip of 20 dots with Q1, p50, Q3, p95 positions highlighted by brackets.

- **Title (bold 15px, `#1a5276`, top center):** "Sorted Positions #1 to #20 — Percentiles Are Just Counting".
- **Strip:** horizontal light gridline (`#e5e9ef`) at y=150 from x=45 to x=675; 20 dots evenly spaced along it. Default dots radius 6 in `rgba(42,120,214,0.45)`; highlighted positions radius 9: #5–#6 aqua `#199e70`, #10–#11 green `#008300`, #15–#16 violet `#4a3aa7`, #19 orange `#d95926`.
- **Brackets** (2px square brackets spanning the highlighted dots, with two-line bold 12px label in matching color):
  - above: aqua "Q1 (p25): #5-#6" / "= 1.0s"; violet "Q3 (p75): #15-#16" / "= 2.0s"
  - below: green "p50: #10-#11" / "= 1.2s"; orange "p95: #19" / "= 4.8s"
- **Position/value labels (11px `#6b7280`):** "#1" below and "0.7s" above the first dot; "#20" below and "9.0s" above the last dot; "#5", "#15" below; "#10", "#19" above their dots.
- **Caption (bold 13px magenta `#d55181`, bottom center):** "each quartile block holds exactly 5 of the 20 loads".

## Why Dashboards Track p95, Not the Average

**Tags:** `where it's used` (blue), `tail risk` (orange)

- **Averages hide the tail** — mean 2.0s looks fine while 1 in 20 users waits 5s or more
- **Tail users complain** — the slow experiences drive support tickets, not the median ones
- **Regressions hit tails first** — a cache bug can double p95 while p50 never moves
- **Promises are percentiles** — "p95 under 5s" is testable; "average under 2s" hides failures
- **At scale, 1% is huge** — a slow p99 on a million loads is 10,000 bad experiences a day

*Example:* After a bad deploy, p50 stayed at 1.2s while p95 jumped from 4.8s to 8.1s — only tail users suffered.

**Key point:** Track p50 for the typical user and p95/p99 for the unlucky ones — one number cannot watch both.

### Visualization (canvas `c3`, 720×300)

Line chart: 7 days of p50 vs p95 — the deploy that only the tail noticed.

- **Title (bold 15px, `#1a5276`, top center):** "A Week of Load Times: the Regression Only p95 Caught".
- **Axes:** L-shaped axis (`#999`), padding top 52 / bottom 50 / left 55 / right 150; y 0–9s with labels "0s", "2s", "4s", "6s", "8s" and light gridlines `#e5e9ef`; x labels Mon–Sun (12px `#6b7280`).
- **Data:** p50 by day = [1.2, 1.2, 1.3, 1.2, 1.2, 1.3, 1.2]; p95 by day = [4.8, 4.7, 4.9, 4.8, 8.1, 8.0, 8.2].
- **Series:** connected lines width 3 with 4px dots — p50 in green `#008300`, p95 in orange `#d95926`.
- **Deploy marker:** vertical dashed red line (`#e74c3c`, width 2, dash 5/4) between Thu and Fri, labeled above in bold 12px red: "bad deploy".
- **Annotations:** bold 13px orange near the p95 jump: "p95: 4.8s -> 8.1s"; bold 12px green above the p50 line: "p50 never moved (1.2s)".
- **Legend (right panel):** orange swatch "p95 (slowest 5%)", green swatch "p50 (typical user)" (12px `#2c3e50`), plus 11px muted note "illustrative week".

## The Confusion: What p99 Does NOT Mean

**Tags:** `common mistake` (red)

- **"p99 = 9s"** does not mean pages take 9s — it means 99% are faster than 9s
- **Percentile ≠ score** — "90th percentile" means faster than 90% of loads, not "90% good"
- **Median is p50** — same number, two names; quartiles are p25 / p50 / p75
- **Small samples wobble** — with 20 values, "p99" is basically just the maximum

*Example:* "Our p99 is 9 seconds, so the site takes 9 seconds to load" — no: 99 of 100 loads are faster than that.

**Common mistake:** Reading pX as "the typical value". It is a boundary: X% of the data sits below it, the rest above.

### Visualization (canvas `c4`, 720×300)

100-dot waffle chart — what "p99 = 9s" means — plus a YES/NO reading panel.

- **Title (bold 15px, `#1a5276`, top center):** "\"p99 = 9s\" on 100 Loads: 99 Faster, 1 Slower".
- **Waffle:** 10×10 grid of dots (radius 7, cell 21px, origin x=90 y=52); 99 dots blue `#2a78d6`, the 100th (bottom-right) red `#e74c3c`. Below the grid, bold 12px blue label: "99 loads faster than 9s".
- **Arrow:** red 2px arrow pointing at the red dot from the label "the 1 load beyond p99" (bold 12px red).
- **Right panel (starting x=420, y=70):** heading bold 13px `#2c3e50` "How to read it:"; then 13px lines — green `#008300`: "YES: \"99% of loads beat 9s\"" and "YES: \"1 in 100 is slower than 9s\""; red `#e74c3c`: "NO: \"pages take 9s to load\"" and "NO: \"99% of loads take 9s\"".
- **Takeaway (bold 13px violet `#4a3aa7`, two lines):** "pX is a boundary, not a typical value:" / "X% below it, the rest above it".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
