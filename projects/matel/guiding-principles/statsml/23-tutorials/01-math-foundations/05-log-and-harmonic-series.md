# Log & Harmonic Series

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Log & Harmonic Series

**Subtitle:** Adding 1 + 1/2 + 1/3 + ... grows forever but at the speed of a logarithm — slow enough to fool the eye, and exactly the sum that prices "collect them all" problems

## Collecting All 10 Cereal-Box Toys

**Tags:** `core idea` (blue), `running example` (green), `coupon collector` (orange)

- **The hunt** — each cereal box hides 1 of 10 toys, equally likely; you want the full set.
- **Early luck** — box one always gives a new toy; box two is new 9 times out of 10.
- **Late grind** — with 1 toy left, each box hits it 1 time in 10, so it takes 10 boxes on average.
- **The pattern** — with k toys still missing, the next new toy costs 10/k boxes on average.
- **Add it up** — 10/10 + 10/9 + ... + 10/1 ≈ 29.3 boxes, not the "about 10" most people guess.

*Example (italic):* A kid budgeted 15 boxes for the set of 10; the hunt actually averages about 29 boxes, and 10 of those go to the final toy.

**Key point:** The total is 10 × (1 + 1/2 + 1/3 + ... + 1/10). That sum in brackets is the harmonic series — the whole page is about how it behaves.

### Visualization (canvas `c1`, 720×300)

Bar chart of the expected number of boxes to land each successive new toy (10/k when k toys are missing), with the final bar highlighted in orange.

- **Title (bold 15px, `#1a5276`, top center):** "Average Boxes to Land Each New Toy (set of 10)".
- **Data:** values `[1.00, 1.11, 1.25, 1.43, 1.67, 2.00, 2.50, 3.33, 5.00, 10.00]`; x labels `['1st', '2nd', '3rd', '4th', '5th', '6th', '7th', '8th', '9th', '10th']`.
- **Axes:** origin x=60, width 610, baseline y=245, chart height 180, y scale max 10.8; 1px `#999` L-shaped axis.
- **Bars:** width = 610/10 with 5px insets each side; bars 1–9 fill `rgba(42,120,214,0.45)`, bar 10 fill `rgba(217,89,38,0.6)`; each bar topped by its value to 1 decimal in bold 11px (blue `#2a78d6` for bars 1–9, orange `#d95926` for bar 10); ordinal labels 12px `#444` below the baseline.
- **Annotations:** magenta `#d55181` bold 13px left-aligned "total: about 29.3 boxes for the full set" near the top-left of the plot; orange bold 12px right-aligned two-line note "last toy: 10 boxes," / "a third of the hunt" near the last bar.
- **Caption (12px `#444`, bottom center):** "which new toy the next boxes deliver".

## The Sum Behind the Box Count

**Tags:** `worked example` (blue), `log link` (green)

- **The definition** — H(n) = 1 + 1/2 + 1/3 + ... + 1/n; the toy hunt cost 10 × H(10).
- **Small sums** — H(10) = 2.93, H(100) = 5.19, so 10× the terms adds barely 2.3 to the sum.
- **The log link** — H(n) ≈ ln(n) + 0.577; the sum grows like the log of the term count.
- **Doubling rule** — doubling n adds about 0.69 (= ln 2) to the sum, no matter how big n is.
- **Tight fit** — the smooth curve ln(n) + 0.577 hugs the staircase almost exactly past n = 5.

*Example (italic):* H(100) = 5.19 while ln(100) + 0.577 = 5.18 — the log approximation is off by less than 0.01.

**Key point:** Harmonic sums are logarithms in disguise: H(n) ≈ ln(n) + 0.577. Anywhere you meet 1 + 1/2 + ... + 1/n, you can read "roughly log n".

### Visualization (canvas `c2`, 720×300)

Line chart: hardcoded H(n) points (blue dots) overlaid on the smooth dashed curve ln(n) + 0.5772 computed deterministically for n = 1..100.

- **Title (bold 15px, `#1a5276`, top center):** "H(n) = 1 + 1/2 + ... + 1/n Tracks ln(n) + 0.577".
- **Data (harmonic points as [n, H(n)] pairs):** `[[1, 1.00], [2, 1.50], [3, 1.83], [4, 2.08], [5, 2.28], [6, 2.45], [7, 2.59], [8, 2.72], [9, 2.83], [10, 2.93], [20, 3.60], [30, 4.00], [50, 4.50], [70, 4.83], [100, 5.19]]`.
- **Axes:** origin x=60, width 610, baseline y=250, chart height 195, y range 0–6; 1px `#999` axis; horizontal gridlines `#e5e9ef` at y = 0, 2, 4, 6 with 12px `#444` labels right-aligned left of the axis; x ticks at n = 1, 25, 50, 75, 100 labeled "n = N" 12px `#444` below the baseline; x maps (n−1)/99 across the width.
- **Smooth curve:** green `#008300` 2px dashed line (dash 6/4) plotting `Math.log(n) + 0.5772` for n = 1 to 100.
- **Points:** blue `#2a78d6` filled 4px-radius dots at each harmonic pair.
- **Annotations:** blue bold 12px "H(10) = 2.93" just right/below its point and "H(100) = 5.19" left/above its point; green bold 13px "dashed curve: ln(n) + 0.577" at (x of n=35, y of 2.2); orange `#d95926` bold 12px "doubling n adds only ~0.69" at (x of n=35, y of 1.5).

## It Grows Forever — Just Absurdly Slowly

**Tags:** `common mistake` (red), `divergence` (orange)

- **It diverges** — keep adding terms and H(n) passes any number you name; it never settles.
- **The proof** — 1/3 + 1/4 > 1/2 and 1/5 + ... + 1/8 > 1/2: each doubling block adds over 1/2.
- **The catch** — passing 10 takes 12,367 terms; passing 20 takes about 272 million terms.
- **×148 rule** — every extra 5 you want on the sum multiplies the terms needed by about 148.
- **Fooled eyes** — any plot you can afford to draw looks like it flattens out near 5 to 10.

*Example (italic):* Pushing the sum past 100 would take roughly 10^43 terms — a term count with 44 digits, far beyond any computer.

**Common mistake:** Eyeballing partial sums and declaring convergence. The harmonic series is the textbook sum that grows without bound while looking completely flat.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of the number of terms needed to push the sum past 5, 10, 15, 20 — bar lengths drawn on a log10 scale.

- **Title (bold 15px, `#1a5276`, top center):** "Terms Needed Before the Sum Passes Each Milestone".
- **Data:** row labels `['sum > 5', 'sum > 10', 'sum > 15', 'sum > 20']`; term labels `['83 terms', '12,367 terms', '1.84 million terms', '272 million terms']`; log10 bar lengths `[1.92, 4.09, 6.26, 8.44]`.
- **Layout:** bars start at x=150, max width 400 (scale max log10 = 8.44), 24px tall, rows at y = 58 + i×50; row label bold 13px `#444` right-aligned left of each bar; term count bold 12px right of each bar in the row's color.
- **Colors per row:** blue `#2a78d6` / fill `rgba(42,120,214,0.5)`, aqua `#199e70` / `rgba(25,158,112,0.5)`, yellow `#c98500` / `rgba(201,133,0,0.5)`, magenta `#d55181` / `rgba(213,81,129,0.5)`.
- **Annotations (bottom center):** magenta bold 13px "each +5 on the sum costs ~148× more terms (e^5 ≈ 148)" at y=272; caption 12px `#444` "bar length is log10(terms) — each equal step rightward means ×10 more terms" at y=291.

## Coupon Collecting in a Data Job

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Coupon collector** — seeing all n equally likely categories takes about n × H(n) samples.
- **Rule of thumb** — budget n × ln(n) + 0.58n draws; for 100 categories that is about 519.
- **Naive guess** — people budget n samples, "one per category", and come up 5× short at n = 100.
- **Where it bites** — random tests hitting every code path, cache warm-ups, panel coverage.
- **Long tail** — the last unseen category dominates: the final one alone costs n draws on average.

*Example (italic):* A QA script sampling user types at random needed about 519 runs, not 100, to see all 100 types at least once.

**Key point:** Whenever "see every one at least once" meets random sampling, the price is n log n, not n — the harmonic series is the reason.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart comparing the naive budget (n samples) vs the actual coupon-collector cost (n × H(n) draws) at three category counts.

- **Title (bold 15px, `#1a5276`, top center):** "Random Samples Needed to See Every Category Once".
- **Data:** groups `['n = 10', 'n = 50', 'n = 100']`; naive `[10, 50, 100]`; actual `[29, 225, 519]`.
- **Axes:** origin x=70, width 590, baseline y=250, chart height 180, y scale max 560; 1px `#999` axis; group labels 12px `#444` below the baseline.
- **Bars:** each group centered in a 590/3 slot; two 60px-wide bars 6px either side of center — naive fill `rgba(42,120,214,0.5)` (left), actual fill `rgba(217,89,38,0.6)` (right); values in bold 12px above each bar (blue `#2a78d6` for naive, orange `#d95926` for actual).
- **Legend (top-left):** 14×14 swatches at x = axis+12, y = 46 and 68, with 12px `#444` labels "naive guess: n samples" and "actual: n × H(n) draws".
- **Annotations:** magenta `#d55181` bold 13px right-aligned "5.2× the naive budget at n = 100" at top-right of the plot (y=58); caption 12px `#444` bottom center "categories, all equally likely (illustrative rounding)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
