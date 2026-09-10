# Bayes' Theorem

**Page type:** detail page (tutorial page: 4 `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Bayes' Theorem

**Subtitle:** How to flip a probability around: from "how often the test is right" to "how likely you actually have it"

## A 99% Accurate Test for a 1-in-1,000 Condition

**Tags:** `core idea` (blue), `surprise` (orange)

- **The setup** — a screening test is right 99% of the time; the condition affects 1 in 1,000
- **The question** — you test positive; how likely is it that you actually have the condition?
- **Gut answer** — most people say "about 99%, that's the test's accuracy"
- **Real answer** — about 9%, because healthy people vastly outnumber sick ones
- **Bayes' theorem** — the rule that flips "positive if sick" into "sick if positive"

*Example:* Out of 100,000 people tested, 1,098 get a positive result — but only 99 of them are sick.

**Key point:** A test's accuracy is not your probability of being sick — the rarity of the condition matters just as much.

### Visualization (canvas `c1`, 720×300)

Counting-tree diagram: 100,000 people split into the four test outcomes via labeled boxes and arrows.

- **Title (bold 15px ink `#1a5276`, centered):** "100,000 People Take the 99% Test".
- **Boxes** (filled rect + 2px colored stroke; line 1 bold 13px, line 2 plain 12px, both in the stroke color):
  - Top (x=280, y=38, 160×34): fill `#eaf2fb`, stroke blue `#2a78d6`, text "100,000 tested".
  - Row 2 left (x=90, y=118, 180×40): fill `rgba(213,81,129,0.10)`, stroke magenta `#d55181`, "100 have it" / "(1 in 1,000)".
  - Row 2 right (x=440, y=118, 200×40): fill `rgba(0,131,0,0.08)`, stroke green `#008300`, "99,900 do not" / "(the other 999 in 1,000)".
  - Row 3, four boxes at y=218: "99 test +" / "true positives" (magenta fill/stroke as above, 140×40); "1 tests −" / "missed" (fill `#f4f6f8`, stroke mute `#6b7280`, 120×40); "999 test +" / "false positives" (fill `rgba(217,89,38,0.12)`, stroke orange `#d95926`, 150×40); "98,901 test −" / "correctly clear" (fill `#f4f6f8`, stroke mute, 140×40).
- **Arrows:** mute gray `#6b7280`, 1.5px, with filled triangular heads, connecting top box to row 2 and row 2 boxes to their row-3 children.
- **Bottom line (bold 13px orange `#d95926`, centered):** "Two positive piles: 99 real vs 999 false — the false pile is 10x bigger".

## Count It Out: 100,000 People, No Formula Needed

**Tags:** `worked example` (green), `natural frequencies` (blue)

- **Step 1** — of 100,000 people, 1 in 1,000 have it: 100 sick, 99,900 healthy
- **Step 2** — the test catches 99% of the 100 sick: 99 true positives, 1 missed
- **Step 3** — it also errs on 1% of the 99,900 healthy: 999 false positives
- **Step 4** — the positive pile holds 99 + 999 = 1,098 people
- **Step 5** — chance a positive is real: 99 / 1,098 ≈ 9%

*Example:* The 999 false alarms come from a huge healthy crowd; the 99 real hits come from a tiny sick one.

**Key point:** Counting whole people (natural frequencies) gives the same answer as the formula — with none of the algebra.

### Visualization (canvas `c2`, 720×300)

Two-bar chart: false positives vs true positives inside the positive pile.

- **Title (bold 15px ink, centered):** "Inside the Positive Pile (1,098 people)".
- **Axes:** padding top 55, bottom 55, left 70, right 40; L-shaped axis `#999`. Y from 0 to 1,000 with labels every 250 (12px mute `#6b7280`), gridlines `#e5e9ef`.
- **Bars (170px wide, centered at ~25% and ~72% of plot width):**
  - 999, orange `#d95926`, value label bold 14px "999" above, 12px `#2c3e50` label "false positives (healthy)" below.
  - 99, magenta `#d55181`, value label "99", label "true positives (sick)".
- **Bottom line (bold 13px violet `#4a3aa7`, centered):** "Only 99 of 1,098 positives are real: 99 / 1,098 ≈ 9%".

## Where a Data Scientist Meets This Every Week

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Spam filters** — a "99% accurate" filter still misfires when spam is rare in a folder
- **Fraud alerts** — a rare-event model's alert queue is mostly innocent cases
- **Medical screening** — screening a whole population produces mostly false positives
- **Monitoring** — a pager alert on a rarely-failing system is usually a false alarm
- **Rule of thumb** — the rarer the condition, the less a positive result means

*Example:* The same 99% test gives a 50% believable positive when the condition affects 1 person in 100.

**Key point:** Before trusting any detector, ask for the base rate — accuracy alone tells you almost nothing.

### Visualization (canvas `c3`, 720×300)

Four-bar chart: P(sick | positive) for the same 99% test at different base rates.

- **Title (bold 15px ink, centered):** "Same 99% Test, Different Populations".
- **Axes:** padding top 55, bottom 70, left 70, right 40; L-shaped axis `#999`. Y 0–100% with labels every 25% (12px mute), gridlines `#e5e9ef`.
- **Bars (90px wide, 4 evenly spaced):**
  | Base rate | Value | Color |
  |---|---|---|
  | 1 in 10,000 | 1% | orange `#d95926` |
  | 1 in 1,000 | 9% | violet `#4a3aa7` |
  | 1 in 100 | 50% | blue `#2a78d6` |
  | 1 in 10 | 92% | green `#008300` |
  Value in bold 13px (bar color) above each bar; base-rate label 12px `#2c3e50` below.
- **X-axis caption (12px mute, centered):** "how common the condition is (base rate)".
- **Bottom line (bold 13px orange, centered):** "P(sick | positive) runs from 1% to 92% — the test never changed".

## The Common Confusion: Two Very Different Percentages

**Tags:** `common mistake` (red)

- **Two directions** — "positive if sick" and "sick if positive" are different questions
- **The test's 99%** — is P(positive | sick): how the test behaves on sick people
- **Your 9%** — is P(sick | positive): what a positive result means for you
- **Why they differ** — the two groups (100 sick vs 1,098 positive) have different sizes
- **The swap error** — mixing them up is so common it's called the inverse fallacy

*Example:* "99% of sick people test positive" quietly becomes "99% of positives are sick" — off by a factor of 11.

**Key point:** Bayes' theorem is exactly the tool that converts one direction into the other — never assume they are equal.

### Visualization (canvas `c4`, 720×300)

Two-bar chart contrasting the two conditional probabilities.

- **Title (bold 15px ink, centered):** "Two Questions, Two Answers".
- **Axes:** padding top 60, bottom 78, left 70, right 40; L-shaped axis `#999`. Y 0–100% with labels every 25% (12px mute), gridlines `#e5e9ef`.
- **Bars (180px wide, centered at ~27% and ~73% of plot width):**
  - 99%, green `#008300`, value label bold 15px "99%" above; below the baseline: bold 12px `#2c3e50` "P(positive | sick)" then 12px mute "out of the 100 sick people".
  - 9%, violet `#4a3aa7`, value label "9%"; "P(sick | positive)" / "out of the 1,098 positives".
- **Bottom line (bold 13px magenta `#d55181`, centered):** "Same test, swapped question — off by a factor of 11".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` concept name (no index number), `.subtitle` line, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a 5-bullet `<ul>` (each `<li>` opens with a `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; table cells padded 12px, no borders; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px solid `#e74c3c` left border, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. The tree diagram uses `box()` and `arrow()` helper functions. Chart titles bold 15px, labels 12–13px. Hardcoded literal data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
