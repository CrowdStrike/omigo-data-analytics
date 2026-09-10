# Winsorizing & Trimming

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Winsorizing &amp; Trimming

**Subtitle:** Two rule-based ways to tame extreme values — cap them at a percentile (winsorize) or drop them (trim) — and one duty: say that you did it

## Cap the $50,000 Day, or Drop It?

Tags: `core idea` (blue), `running example` (green)

- **The problem** — one $50,000 corporate order makes the mean $5,425, useless for planning
- **Winsorize** — keep the day, but cap its value: $50,000 is recorded as $720, the next-highest day
- **Trim** — drop the most extreme day entirely: nine days remain
- **Both are rules** — applied to a tail percentile (say, top 1%), not to one point by hand
- **Scale note** — capping one of ten days is a 10% cap; a 1% cap on a year hits the top 3-4 days

*Example:* "Cap at the 90th percentile" means any day above that percentile's value gets recorded as that value.

**Key point:** **Key point:** Winsorizing changes values but keeps every row; trimming removes rows entirely. Same goal, different footprint in the data.

### Visualization (canvas `c1`, 720×300)

Three dot-columns of the same ten days: raw, winsorized, and trimmed.

- **Title (bold 15px, `#1a5276`, top center):** "Same Ten Days, Three Versions of the Data"
- **Shared data:** `NINE = [290, 330, 380, 420, 450, 510, 540, 610, 720]`; the tenth value is $50,000.
- **Axes:** shared y axis at x=60 from baseline y=240 up to y=52, scale $0–$800 with gridlines `#e5e9ef` and labels `$0`, `$400`, `$800` (11px `#6b7280`); column names (bold 12px `#1a5276`) below the baseline.
- **Three columns (x = 150, 375, 590):** "RAW" in blue `#2a78d6`, "WINSORIZED" in aqua `#199e70`, "TRIMMED" in green `#008300`. Each column shows the nine normal days as 5px-radius dots with deterministic horizontal jitter (`sin(i × 2.1) × 18`).
- **RAW column extra:** the $50,000 point as a 7px orange `#d95926` dot at y=60, above two slanted gray `#6b7280` axis-break strokes; bold 12px orange label "$50,000".
- **WINSORIZED column extra:** dashed orange line (width 2, dash 5/3) with an orange arrowhead pulling from y=60 down to the $720 level; a 7px orange dot at $720 ringed by a 2px `#1a5276` circle (11px radius); bold 12px orange label "capped at $720"; 11px `#6b7280` note "still 10 days" at the top.
- **TRIMMED column extra:** a dashed gray `#6b7280` ghost circle (7px radius, dash 3/3) at y=60 where the point used to be; bold 11px `#6b7280` note "dropped — 9 days left".
- **Bottom annotation (bold 13px violet `#4a3aa7`, centered, y=285):** "winsorize moves the point · trim removes it — the other nine days never change"

## Three Means from the Same Ten Days

Tags: `worked example` (green)

- **Raw mean** — 54,250 / 10 = $5,425
- **Winsorized mean** — replace 50,000 with 720: 4,970 / 10 = $497
- **Trimmed mean** — drop the 50,000: 4,250 / 9 = $472
- **The $25 gap** — winsorized sits above trimmed: the capped day still counts as a big day
- **Both tails** — symmetric trimming drops 290 and 50,000: 3,960 / 8 = $495

*Example:* All three adjusted means land near the median ($480) — the raw mean was the odd one out.

**Key point:** **Key point:** Winsorizing keeps the information "day 5 was the biggest day", just not its full size; trimming forgets day 5 existed.

### Visualization (canvas `c2`, 720×300)

Bar chart: the mean under each policy, with the median as a reference line.

- **Title (bold 15px, `#1a5276`, top center):** "The Mean Under Each Policy"
- **Data (baseline y=225, chart height 155, scale max 5800; bars 120px wide from x=100 with 75px gaps, 75% alpha, min height 4px; bold 14px colored `$` value labels above, 12px `#333` names and 11px `#6b7280` formulas below):**
  - "$5,425" raw mean, orange `#d95926`, formula "54,250 / 10"
  - "$497" winsorized mean, aqua `#199e70`, formula "4,970 / 10"
  - "$472" trimmed mean, green `#008300`, formula "4,250 / 9"
- **Median reference:** dashed violet `#4a3aa7` horizontal line (width 1.5, dash 6/4) at $480 across the plot, labeled bold 11px violet "median $480" at the right end.
- **Baseline:** gray `#999` line from x=60 to x=660.
- **Annotations:** bold 12px orange "nothing like a typical day" above the raw bar (y=52); bold 12px `#1a5276` "$25 apart — the capped day still counts as a big day" near the median line (centered at x=460).
- **Bottom annotation (bold 13px violet, centered, y=285):** "both policies land the mean near the median — hand-check: 4,970/10 and 4,250/9"

## Always Say That You Did It

Tags: `where it's used` (blue), `watch out` (red)

- **Totals change** — winsorized total $4,970 vs real $54,250: revenue understated 11x
- **Silent capping** — a reader who was not told sees a calm week that never happened
- **One line fixes it** — "values capped at the 90th percentile ($720); 1 of 10 days affected"
- **Right number, right job** — adjusted mean for planning a typical day; raw total for finance
- **Stated rule** — the threshold must come from a written rule anyone can re-run

*Example:* Finance asking "where did $49,280 go?" is how silent winsorizing gets discovered.

**Key point:** **Key point:** Winsorizing and trimming are legitimate only when declared — the method, the threshold, and how many points were affected.

### Visualization (canvas `c3`, 720×300)

Horizontal bar comparison: real total vs winsorized and trimmed totals, plus the disclosure line.

- **Title (bold 15px, `#1a5276`, top center):** "The Adjusted Data Is Not the Real Money"
- **Rows (horizontal bars from x=240, plot width 400, row height 42 with 22px gaps starting at y=60; scale max 56,000; 75% alpha fills, min width 4px; right-aligned 12px `#333` row labels, bold 13px colored `$` values after each bar):**
  - "real ten-day total" — $54,250, orange `#d95926`
  - "winsorized total" — $4,970, aqua `#199e70`
  - "trimmed total" — $4,250, green `#008300`
- **Gap bracket:** short red `#e74c3c` 2px vertical tick between rows 1 and 2 near the winsorized bar end, with bold 13px red label "$49,280 vanishes if nobody says so".
- **Disclosure lines (centered):** bold 12px `#1a5276` "the one line that makes it honest:" (y=240); italic 12px `#333` "\"values capped at the 90th percentile ($720); 1 of 10 days affected\"" (y=260).
- **Bottom annotation (bold 13px violet `#4a3aa7`, centered, y=288):** "adjusted mean for planning · raw total for finance — never swap them"

## What Winsorizing Is Not

Tags: `common mistake` (red), `judgment call` (orange)

- **Not error-fixing** — a typo gets corrected to its true value, not capped at a percentile
- **Not deletion** — the winsorized day stays in the data at $720; row counts are unchanged
- **Not investigation** — trimming by rule skips the question of what the $50,000 actually was
- **Not a fixed dollar cap** — next year's 99th percentile is a different dollar amount
- **Order matters** — settle error vs real first; winsorize or trim only what is real but extreme

*Example:* If the $50,000 is a typo for $500, capping it at $720 is still wrong — fix the typo.

**Key point:** **Common mistake:** Using winsorizing as a substitute for investigating. It is a summary-taming tool for real-but-extreme values, applied after the error check.

### Visualization (canvas `c4`, 720×300)

Decision flow diagram: investigate first, then branch into typo / real-but-extreme / different-population.

- **Title (bold 15px, `#1a5276`, top center):** "Where Winsorizing Sits in the Decision"
- **Step 1 box (180×50 at x=270, y=44, ink `#1a5276` styling — 12% alpha fill, 2px border, bold 12px heading + 11px `#333` line):** "STEP 1 — investigate" / "what was the $50,000?"
- **Three arrows** fan out from the bottom of the step-1 box (magenta left, orange straight down, green right; 2px strokes with filled arrowheads).
- **Branch boxes (each 180×64 at y=140, same box style in their color):**
  - Magenta `#d55181` at x=70: "it was a typo" / "fix it to the true value" / "(never cap a typo)"
  - Orange `#d95926` at x=270: "real but extreme" / "winsorize or trim," / "by a declared rule"
  - Green `#008300` at x=480: "different population" / "analyze the corporate" / "channel separately"
- **Mini-note (bold 11px orange, centered at x=360, y=226):** "winsorize: 10 days kept · trim: 9"
- **Bottom annotations (centered):** bold 13px red `#e74c3c` "the taming tools apply only to the middle branch — after the error check" (y=262); 12px `#6b7280` "and the cap is a percentile rule, not this year’s dollar amount" (y=284).

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (100% width, collapsed) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both 12px padding, top-aligned.
- **Text cell structure:** `.tags` pill row, `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms `#1a5276`), one italic `.example` paragraph, one `.key-point` callout.
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared data array `NINE = [290, 330, 380, 420, 450, 510, 540, 610, 720]` plus the $50,000 day; all data hardcoded/deterministic (`Math.sin` jitter only, no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Links:** this page has no card links; any grid page linking here uses the `.html` extension in regenerated HTML.
