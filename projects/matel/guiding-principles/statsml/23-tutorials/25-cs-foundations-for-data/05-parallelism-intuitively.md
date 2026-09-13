# Parallelism, Intuitively

**Page type:** detail page (tutorial page: 4 card-sections, each an h2 + two-column layout table, text left 50%, canvas right 50%)
**HTML title tag:** Parallelism, Intuitively

**Subtitle:** Eight cooks in one kitchen: eight salads finish 8× faster, but one soup takes just as long — the part that must happen in order caps every speedup

## Eight Cooks: Salads vs Soup

Tags: `core idea` (blue), `running example` (green)

- **The salads** — 8 salads, 10 min each: one cook needs 80 min; eight cooks need 10
- **Why it works** — no salad depends on another; the work splits cleanly
- **The soup** — chop → sauté → simmer → season: each step needs the previous one done
- **Why it doesn't** — 40 min of ordered steps stay 40 min with one cook or eight
- **The vocabulary** — salads are "parallel" work; the soup is "serial" (sequential) work

*Example:* Nine women cannot make a baby in one month — some work simply refuses to split.

**Key point:** Parallel speedup comes from independence. Ask "does step B need step A's output?" — if yes, cooks wait.

### Visualization (canvas `c1`, 720×300)

Split-panel timeline diagram: 8 parallel salad bars on the left vs one sequential 4-step soup timeline on the right; vertical dashed gray divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, ink `#1a5276`, top center):** "Timelines: 8 Salads Split 8 Ways vs One Soup in Order".
- **Left panel** — panel label bold 12px `#444` centered at x=210: "salads: 8 cooks, 10 minutes". Eight rows (row height 18px starting at y=70, bars at x=100, width 200, height 14): each row has right-aligned gray (`#6b7280`) 11px label "cook 1" … "cook 8", then a green (`#008300`, alpha 0.7) bar with white bold 11px centered text "salad N — 10 min" (N = 1..8). Below the rows, green bold 13px centered: "all done at minute 10 (was 80 solo): 8×".
- **Right panel** — panel label bold 12px `#444` centered at x=535: "soup: 4 steps, each waits for the last". One horizontal stacked timeline at x=395, width 280, y=100, height 34, scaled to 0–40 min, four sequential segments (alpha 0.7, white bold 11px labels): "chop 10" (blue `#2a78d6`), "sauté 10" (violet `#4a3aa7`), "simmer 15" (orange `#d95926`), "season 5" (magenta `#d55181`); "→" in `#555` 11px between segments below the bar. Thin gray (`#999`) axis line under the bar with 11px mute labels "0 min" and "40 min".
- **Right annotations:** red `#e74c3c` bold 13px centered at x=535: "40 min with 1 cook or 8: 1×"; below it mute (`#6b7280`) 11px: "seven cooks stand and watch the pot".

## A 100-Minute Job With a 20-Minute Soup Inside

Tags: `worked example` (green), `rule of thumb` (blue)

- **The job** — 100 min of work: 80 min splits like salads, 20 min is soup (serial)
- **8 cooks** — 80 ÷ 8 + 20 = 30 min: a 3.3× speedup, not 8×
- **Infinite cooks** — 0 + 20 = 20 min: the speedup can never pass 100 ÷ 20 = 5×
- **Amdahl's law** — that's the whole law: the serial fraction sets a hard ceiling
- **Feel the curve** — 2 cooks: 1.7×; 4: 2.5×; 8: 3.3×; 64: 4.7× — crawling toward 5×

*Example:* Doubling from 8 to 16 cooks buys 30 → 25 min — each new cook helps less than the last.

**Key point:** A job that is 20% serial maxes out at 5× — no budget, cluster, or patience changes that ceiling.

### Visualization (canvas `c2`, 720×300)

Line chart: Amdahl's law speedup curve vs number of cooks, with ideal diagonal and a 5× ceiling line.

- **Title (bold 15px, `#1a5276`, top center):** "Speedup vs Cooks: 80 Parallel Minutes + 20 Serial Minutes".
- **Axes:** x = number of cooks 1..64 on a log2 scale with ticks at 1, 2, 4, 8, 16, 32, 64; y = speedup 0..8.5 with tick labels "2×", "4×", "6×", "8×" (right-aligned, mute gray `#6b7280` 12px). Axis lines `#999`; x-axis caption "number of cooks (cores)" in `#444` 12px centered. Padding: top 50, bottom 50, left 66, right 170.
- **Ideal diagonal:** dashed (5/4) light grid-gray `#e5e9ef` line, width 2, speedup = cooks from (1,1) toward (8.5,8.5); labeled in bold 12px mute gray: "the 8× dream".
- **Ceiling:** horizontal dashed (7/5) red `#e74c3c` line width 2 at speedup 5, labeled bold 13px red above it: "ceiling: 5× (the 20-min soup)".
- **Curve:** blue `#2a78d6` line width 3 plotting S = 100 / (20 + 80/N) for N from 1 to 64 (log-spaced samples).
- **Points:** blue 4px dots at (cooks, speedup) = (1, 1), (2, 1.67), (4, 2.5), (8, 3.33), (16, 4), (64, 4.7); bold 12px centered labels "3.3×" below the N=8 point and "4.7×" below the N=64 point.
- **Legend (right side, 12px):** blue line width 3 sample + "actual speedup"; dashed grid-gray sample + "perfect split".

## Salads and Soups in a Data Scientist's Day

Tags: `where it's used` (blue)

- **Salad-shaped** — cross-validation folds, grid search fits, per-group aggregations, random forest trees
- **Soup-shaped** — gradient boosting rounds: tree 57 needs tree 56's errors first
- **The switch** — `n_jobs=-1` in sklearn parallelizes the salad-shaped parts only
- **Spark's whole idea** — cut data into partitions so many machines chop at once
- **Illustrative timing** — 8 cores: forest 100 s → ~13 s; boosting rounds can't be split: ~100 s

*Example:* Same button, same 8 cores: the forest flies; the booster's chained rounds barely move.

**Key point:** Before reaching for more cores, name the shape — cores multiply salads, never soups. Real GBM libraries still parallelize inside each round, so cores help — just not by splitting rounds.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar comparison: two model groups, each with a 1-core bar and an 8-core bar (scale: 100 s = 460px wide, bars start at x=60, height 20).

- **Title (bold 15px, `#1a5276`, top center):** "Same 8 Cores, Two Models: 100 Trees Each (illustrative)".
- **Group 1 (label bold 13px `#333`):** "random forest — trees are independent (salad)". Gray (`#6b7280`, alpha 0.5) bar at 100 with 12px label "100 s on 1 core"; green `#008300` (alpha 0.8) bar at 13 with bold 12px label "~13 s on 8 cores  ≈7.7× faster".
- **Group 2:** "gradient boosting — each round needs the last (soup)". Gray bar at 100 labeled "100 s on 1 core"; magenta `#d55181` (alpha 0.8) bar at 95 labeled "~95 s on 8 cores  ≈1.05× — barely moves". Italic magenta 12px caveat below the group (x=60, y=244): "real GBMs still parallelize within each round — cores help, just not across rounds".
- **Bottom annotation (orange `#d95926` bold 13px centered):** "n_jobs=-1 multiplies the salad-shaped work only — name the shape first".

## Coordination Has a Cost: More Cooks, Slower Kitchen

Tags: `common mistake` (red), `trade-off` (orange)

- **The overhead** — splitting work, briefing cooks, and merging results all take time
- **A small job** — 10 s of work plus ~0.3 s of coordination per cook
- **The numbers** — 1 cook: 10.3 s; 2: 5.6 s; 4: 3.7 s; 8: 3.7 s; 16: 5.4 s — worse!
- **The U-shape** — past the sweet spot, coordination grows faster than work shrinks
- **Rule of thumb** — parallelize big chunks; tiny tasks are cheaper done on one core

*Example:* Sixteen cooks crowding one small salad spend more time talking than chopping.

**Common mistake:** Assuming 8 cores = 8× faster. Serial parts cap the gain, and coordination can turn extra workers into pure cost.

### Visualization (canvas `c4`, 720×300)

Line chart: U-shaped total-time curve vs workers plus a dashed coordination-only line.

- **Title (bold 15px, `#1a5276`, top center):** "Small Job (10 s of Work + 0.3 s Coordination per Cook)".
- **Axes:** x = workers 1..16 on a log2 scale with ticks at 1, 2, 4, 8, 16; y = seconds 0..12 with tick labels "3 s", "6 s", "9 s", "12 s" (mute gray 12px). X-axis caption "number of workers" in `#444` 12px. Padding: top 50, bottom 50, left 66, right 175. Axis lines `#999`.
- **Total-time curve:** violet `#4a3aa7` line width 3 plotting t = 10/n + 0.3n for n from 1 to 16 (log-spaced samples).
- **Coordination-only line:** orange `#d95926` dashed (6/4) line width 2 plotting t = 0.3n.
- **Points:** violet 4.5px dots at (workers, seconds) = (1, 10.3), (2, 5.6), (4, 3.7), (8, 3.65), (16, 5.4); bold 12px labels "10.3 s" near (1, 10.3), "3.7 s" above (4, 3.7), "5.4 s" above (16, 5.4).
- **Annotations:** red `#e74c3c` bold 13px centered near (8, 8.2): "16 workers slower than 4"; orange bold 12px left-aligned near (6, 2.1): "coordination cost grows".
- **Legend (right side, 12px):** violet line width 3 sample + "total time"; orange dashed sample + "coordination only".

## Regeneration instructions

- **Template/layout:** tutorial detail page (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Page = `<h1>` + `.subtitle` paragraph, then 4 `.card-section` blocks. Each `.card-section` has an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%), cell padding 12px, vertical-align top.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) whose `<strong>` prefix is "Key point:" (or "Common mistake:" in section 4).
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<code>` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- Grid cards elsewhere linking here use `.html` extensions in regenerated HTML.
