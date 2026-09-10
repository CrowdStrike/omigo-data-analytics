# Robust Statistics

**Page type:** detail page (tutorial topic page: `.card-section` blocks, each an h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Robust Statistics

**Subtitle:** A robust summary is one that a single weird value cannot hijack — the median barely moves while the mean explodes

## One Day Moves the Mean 11x — and the Median $30

Tags: `core idea` (blue), `running example` (green)

- **Same sales log** — nine days between $290 and $720, plus one $50,000 corporate order
- **Mean explodes** — $472 without day 5, $5,425 with it: an 11x jump from one value
- **Std dev explodes too** — from about $130 to about $14,860: over 100x
- **Median shrugs** — $450 without, $480 with: it moved thirty dollars
- **Robust** — the name for summaries, like the median, that one crazy value cannot drag

*Example:* The mean adds every dollar in; the median only asks which value sits in the middle.

**Key point:** **Key point:** One value can drag the mean anywhere it wants. The median only cares about order, so an extreme value counts as just "one value above the middle".

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: mean and median, each without vs with the $50,000 day.

- **Title (bold 15px, `#1a5276`, top center):** "Add One $50,000 Day: Mean ×11.5, Median +$30"
- **Data (baseline y=230, chart height 155, scale max 5800; bars 82px wide, 30px gap within a group; "without day 5" bar at 40% alpha, "with day 5" bar at 80% alpha; bold 13px colored `$` value labels above, 11px `#333` labels "without day 5" / "with day 5" below, bold 13px `#1a5276` group name below those):**
  - MEAN group (orange `#d95926`, starting x=110): without 472, with 5,425.
  - MEDIAN group (green `#008300`, starting x=420): without 450, with 480.
- **Baseline:** gray `#999` line from x=60 to x=660.
- **Annotations:** bold 13px orange "one value dragged the mean up 11x" at (220, 56); bold 13px green "the median moved $30" at (530, 130); bold 13px violet `#4a3aa7` bottom line (centered, y=285): "std dev tells the same story: $130 → $14,860".

## Median and MAD, Computed by Hand

Tags: `worked example` (green)

- **Sort the ten days** — 290, 330, 380, 420, 450, 510, 540, 610, 720, 50,000
- **Median** — the middle two are 450 and 510, so median = (450 + 510) / 2 = 480
- **Distances** — each day's distance from 480: 30, 30, 60, 60, 100, 130, 150, 190, 240, 49,520
- **MAD** — the median of those distances = (100 + 130) / 2 = 115
- **Compare** — std dev says spread is $14,860; MAD says a typical day is $115 from the middle

*Example:* The 49,520 distance sits at the far end of the sorted list — the median never looks there.

**Key point:** **Key point:** MAD (median absolute deviation) is the median's spread partner: the typical distance from the middle, immune to how extreme the extreme is.

### Visualization (canvas `c2`, 720×300)

Two stacked number-line strips: sorted values (median) and sorted distances (MAD), each with an axis break for the extreme.

- **Title (bold 15px, `#1a5276`, top center):** "Both Times: Sort, Then Take the Middle"
- **Shared scale:** each strip's main line runs x=60, width 440, scale 0–800, ticks/labels every 200 (11px `#6b7280`); after it, two slanted gray `#6b7280` break strokes and a short continuation line holding a 6px-radius orange `#d95926` dot for the extreme value (bold 11px orange label above).
- **Strip 1 (line at y=120):** strip label (bold 12px `#1a5276`, left-aligned) "STEP 1 — the ten sorted values, in dollars"; nine 5px-radius blue `#2a78d6` dots at `[290, 330, 380, 420, 450, 510, 540, 610, 720]`; extreme labeled "50,000"; dashed red `#e74c3c` median marker (width 2, dash 5/3) at 480 labeled bold 12px red "median = 480".
- **Strip 2 (line at y=240):** strip label "STEP 2 — each value’s distance from 480, sorted"; nine 5px-radius aqua `#199e70` dots at `[30, 30, 60, 60, 100, 130, 150, 190, 240]`; extreme labeled "49,520"; dashed red marker at 115 labeled "MAD = 115".
- **Bottom annotation (bold 13px violet `#4a3aa7`, centered, y=285):** "the extreme sits at the end of both lists — the middle never sees it"

## The Outlier Hides Inside the Std Dev It Inflated

Tags: `where it's used` (blue), `watch out` (red)

- **Classic alarm** — flag any day more than 3 standard deviations from the mean
- **Self-hiding** — (50,000 − 5,425) / 14,860 = 3.0: the outlier barely trips its own alarm
- **Why** — the $50,000 inflated both the mean and the std dev used to judge it
- **Robust score** — (50,000 − 480) / 115 = 431 MADs from the median: unmistakable
- **In practice** — alerts and feature scaling built on mean/std dev bend with every extreme

*Example:* Every normal day scores about −0.3 std devs — the scale is stretched so far that nothing stands out.

**Key point:** **Key point:** Outliers corrupt the very yardstick used to detect them. A robust yardstick (median, MAD) does not bend, so the outlier has nowhere to hide.

### Visualization (canvas `c3`, 720×300)

Split panel: sd-scores vs robust (MAD) scores on two number lines, side by side.

- **Title (bold 15px, `#1a5276`, top center):** "\"How Many Spreads Away?\" — Two Yardsticks, Two Verdicts"
- **Left panel (line at y=150, x=55, width 280, scale −1 to 4, integer ticks 11px `#6b7280`):**
  - Panel heading (bold 13px `#1a5276`, centered, y=60): "mean / std dev score"
  - Alarm: dashed red `#e74c3c` vertical line (width 2, dash 5/3) at z=3, labeled bold 11px red "alarm: 3 sd".
  - Nine blue `#2a78d6` 4px dots clustered at sd-scores `[-0.337, -0.340, -0.331, -0.346, -0.324, -0.335, -0.343, -0.317, -0.329]` (stacked in 3 mini-rows); 11px blue label "nine normal days: ≈ −0.3".
  - Outlier: 6px orange `#d95926` dot at 3.0, bold 11px orange label "day 5: 3.0".
  - Captions below (centered): bold 12px red "barely trips its own alarm" (y=200); 11px `#6b7280` "(50,000 − 5,425) / 14,860 = 3.0" (y=218).
- **Divider:** dashed vertical `#bdc3c7` line at x=370 (dash 4/3).
- **Right panel (line at y=150, x=405, width 250, scale −2 to 5, integer ticks):**
  - Panel heading (bold 13px `#1a5276`, centered, y=60): "median / MAD score"
  - Nine aqua `#199e70` 4px dots at robust scores `[-1.65, -1.30, -0.87, -0.52, -0.26, 0.26, 0.52, 1.13, 2.09]`; 11px aqua label "nine normal days: −1.7 to 2.1".
  - Outlier off-scale: thick orange arrow (width 3) running off the right edge of the line, bold 12px orange label "day 5: 431 →".
  - Captions below (centered): bold 12px green `#008300` "431 MADs out — unmistakable" (y=200); 11px `#6b7280` "(50,000 − 480) / 115 = 431" (y=218).
- **Bottom annotation (bold 13px violet `#4a3aa7`, centered, y=285):** "the outlier stretched the sd yardstick 114x — the MAD yardstick did not bend"

## Robust Does Not Mean "Outliers Deleted"

Tags: `common mistake` (red), `judgment call` (orange)

- **Nothing is removed** — robust statistics keep all ten days; they just refuse to be dragged
- **Cheap insurance** — on the nine clean days, mean $472 vs median $450: nearly the same answer
- **Spread agrees too** — clean std dev $130 vs MAD $90: same story, no drama
- **The trade** — lose a little precision when data is clean, keep your summary when it is not
- **Still investigate** — the median hides the $50,000 from the summary, not from the business

*Example:* Report the robust summary for "typical day" and the raw total for revenue — they answer different questions.

**Key point:** **Common mistake:** Thinking robust = cleaned. Robust summaries describe dirty data safely; deciding whether the $50,000 is an error is still your job.

### Visualization (canvas `c4`, 720×300)

Bar chart: on the nine clean days, classic and robust summaries nearly agree.

- **Title (bold 15px, `#1a5276`, top center):** "On the Nine Clean Days, the Twins Nearly Agree"
- **Data (baseline y=225, chart height 150, scale max 550; bars 80px wide, 75% alpha; bold 13px colored `$` value labels above, 12px `#333` names below):**
  - "$472" mean, orange `#d95926`, x=90
  - "$450" median, green `#008300`, x=195
  - "$130" std dev, blue `#2a78d6`, x=390
  - "$90" MAD, aqua `#199e70`, x=495
- **Baseline:** gray `#999` line from x=60 to x=660.
- **Pair annotations (bold 12px `#6b7280`, centered):** "center: $22 apart" at (182, 62); "spread: $40 apart" at (482, 130).
- **Divider:** dashed vertical `#bdc3c7` line at x=330 (dash 4/3) separating the center pair from the spread pair.
- **Bottom annotation (bold 13px violet `#4a3aa7`, centered, y=285):** "clean data: robust costs almost nothing · dirty data: it saves the summary"

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by `table.layout` (100% width, collapsed) with one row: `td.text-col` (50%) and `td.viz-col` (50%), both 12px padding, top-aligned.
- **Text cell structure:** `.tags` pill row, `<ul>` of 5 one-line bullets each opening with `<b>bold term</b>` (bold terms `#1a5276`), one italic `.example` paragraph, one `.key-point` callout.
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` text `#1a5276`; green: bg `rgba(39,174,96,0.15)` text `#27ae60`; red: bg `rgba(231,76,60,0.12)` text `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` text `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic, `#555`, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. All data hardcoded/deterministic (no `Math.random()`); same sales-log example as the Outliers page (nine days $290–$720 plus one $50,000 day). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Links:** this page has no card links; any grid page linking here uses the `.html` extension in regenerated HTML.
