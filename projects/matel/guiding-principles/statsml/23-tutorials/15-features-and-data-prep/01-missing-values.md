# Missing Values

**Page type:** detail page (tutorial page: h1 + subtitle, then four card-sections each with a two-column table layout — text left 50%, canvas right 50%)
**HTML title tag:** Missing Values

**Subtitle:** A blank cell is not just "no data" — the reason it is blank decides whether your averages can be trusted

## Three Blanks in the Income Column

Tags: `core idea` (blue), `running example` (green)

- **The data** — 10 customers filled a signup form; the income box was optional
- **The blanks** — 7 wrote an income, 3 left it empty: the column is 30% missing
- **Story A** — people skipped it at random: busy, in a hurry, no pattern at all
- **Story B** — high earners skipped it on purpose: they dislike sharing income
- **Same spreadsheet** — both stories produce identical blanks; the reason is invisible

*Example (italic):* Under Story B, every blank cell secretly whispers "probably a high income" — the blank itself is information.

**Key point:** WHY a cell is blank matters more than THAT it is blank. Random blanks are a nuisance; blanks with a reason bend every number you compute.

### Visualization (canvas `c1`, 720×300)

A drawn spreadsheet table (left) plus two story boxes (right).

- **Title (bold 15px `#1a5276`, top center):** "The Income Column: 3 of 10 Cells Are Blank".
- **Table (left, starting at x=55, y=44; rows 22px tall; name column 100px, income column 120px; cell borders in grid gray `#e5e9ef`):** column headers "customer" and "income ($k)" in bold 12px mute `#6b7280`. Rows: Ana 38, Ben 42, Cara (blank), Dev 45, Eli 50, Fay (blank), Gus 52, Hana 58, Ivan (blank), Jo 65. Known incomes in 12px blue `#2a78d6`; blank cells shaded `rgba(217,89,38,0.14)` with "(blank)" in bold 12px orange `#d95926`.
- **Right side (x=330):** orange bold 14px line "3 of 10 blank = 30% missing"; then two outlined story boxes (350×62, 2px stroke):
  - Aqua `#199e70` box — title "Story A — missing at random"; body lines (12px `#2c3e50`): "the optional field was skipped haphazardly;" / "the blanks carry no message".
  - Violet `#4a3aa7` box — title "Story B — missing for a reason"; body lines: "high earners chose not to answer;" / "each blank hints at a high income".
- **Bottom annotations:** magenta `#d55181` bold 13px "the spreadsheet looks identical under both stories"; mute 12px "you cannot tell A from B by staring at the blanks".

## Drop, Fill, or Flag — Three Choices, Three Answers

Tags: `worked example` (green), `rule of thumb` (blue)

- **The 7 known incomes ($k)** — 38, 42, 45, 50, 52, 58, 65; sum 350, mean 50
- **Choice 1: drop** — delete the 3 blank rows: mean = 350 / 7 = $50k
- **Choice 2: fill with 0** — mean = (350 + 0) / 10 = $35k
- **Choice 3: fill with the mean** — mean = (350 + 3 × 50) / 10 = $50k
- **Story B truth** — the skippers earned 90, 110, 130: real mean = 680 / 10 = $68k

*Example (italic):* The same three blanks give $35k, $50k, or $50k depending on your choice — and the honest answer was $68k.

**Key point:** The fill choice changes the answer. If blanks are random, drop and mean-fill are roughly fine; if blanks have a reason, every choice quietly reports the wrong number.

### Visualization (canvas `c2`, 720×300)

Four-bar chart comparing the average income under each handling choice.

- **Data:** bars labeled `fill with 0` = $35k (yellow `#c98500`), `drop 3 rows` = $50k (blue `#2a78d6`), `fill with mean` = $50k (aqua `#199e70`), `truth (Story B)` = $68k (green `#008300`). Bars 92px wide, evenly spaced.
- **Title:** "Same Blanks, Four Answers for \"Average Income\"".
- **Axes:** L-shaped `#999` axis, padding top 56 / bottom 62 / left 60 / right 25; y scale $0–$80k with gridlines (`#e5e9ef`) and labels "$0k"/"$20k"/"$40k"/"$60k"/"$80k" (mute 12px).
- **Labels:** each bar's value ("$35k" etc.) in bold 13px `#2c3e50` above it; choice labels in 12px below; a second mute 12px caption row under the axis: "(350+0)/10        350/7        (350+150)/10        680/10".
- **Annotation (magenta `#d55181`, bold 13px, centered under the title):** "the fill choice moves the answer from $35k to $50k — and truth was $68k".

## Why a Data Scientist Cares

Tags: `where it's used` (blue), `detective work` (orange)

- **Models refuse blanks** — most algorithms error on empty cells, so you must decide something
- **Dropping 30% of rows** — also throws away those customers' clicks, orders, everything
- **The flag column** — add "income_was_missing" so a model can learn the blank is a signal
- **Cheap test for Story B** — split customers another way and compare blank rates
- **Unequal rates** — if one segment skips far more often, the blanks are not random

*Example (italic):* In a larger version of this data, the blank rate climbs with spending: 10% for low spenders, 25% mid, 60% high — Story B confirmed.

**Rule of thumb:** Before choosing drop, fill, or flag, compare the missing rate across groups you CAN see. A pattern in who is blank means the blanks mean something.

### Visualization (canvas `c3`, 720×300)

Three-bar chart: blank rate by spending tier.

- **Data:** `low spenders` = 10% (blue `#2a78d6`), `mid spenders` = 25% (yellow `#c98500`), `high spenders` = 60% (orange `#d95926`). Bars 130px wide.
- **Title:** "Blank Rate by Spending Tier (100-customer version, illustrative)".
- **Axes:** L-shaped `#999` axis, padding top 56 / bottom 60 / left 60 / right 25; y scale 0–70% with gridlines and labels "0%"/"20%"/"40%"/"60%" (mute 12px).
- **Labels:** "10% blank" / "25% blank" / "60% blank" in bold 13px `#2c3e50` above the bars; tier labels in 12px below.
- **Annotations:** magenta `#d55181` bold 13px centered under the title: "the blank rate climbs with spending — these blanks have a reason"; mute 12px below the axis: "if blanks were random, all three bars would be about the same height".

## The Common Confusion: Filling Is Not Fixing

Tags: `common mistake` (red), `core idea` (blue)

- **"I imputed it, it's handled"** — filling makes the code run; it does not recover the truth
- **Mean-fill illusion** — the column keeps its mean but loses its spread and its tail
- **Three fake twins** — after mean-fill, three "customers" all earn exactly $50k
- **Zero-fill illusion** — three fake $0 earners drag the mean down to $35k
- **No fill knows Story B** — only the flag column carries "this was blank" forward

*Example (italic):* After mean-filling, the three filled customers are the most "typical" people in the dataset — and none of them are real.

**Common mistake:** Treating a fill as a repair. It is an assumption — write down which one you made (drop, fill value, flag), because it is now part of your answer.

### Visualization (canvas `c4`, 720×300)

Dot strip on a dollar line: known values, mean-filled values, and the hidden truth.

- **Data:** known incomes `[38, 42, 45, 50, 52, 58, 65]` ($k); 3 mean-filled values all at $50k; hidden true incomes `[90, 110, 130]` ($k). X scale $0–$140k.
- **Title:** "Where the Filled Values Sit vs Where the Truth Was".
- **Axis:** horizontal dollar line at y=210, ticks at $0–$140k step $20k with labels "$0k"…"$140k" (mute 12px). Left/right padding 60/30.
- **Marks:** 7 known incomes as 6px solid blue `#2a78d6` dots just above the axis; 3 mean-filled values as 6px solid magenta `#d55181` dots stacked vertically at $50k (16px apart); 3 true incomes as 7px green `#008300` rings (3px stroke) above the axis.
- **Annotations (bold):** magenta 12px "3 filled at exactly $50k" above the $50k stack; green 13px "the truth lived out here" near $110k.
- **Legend (bottom left, y=250):** blue dot + "7 known incomes"; magenta dot + "3 mean-filled values"; green ring + "3 hidden true incomes (Story B)" (12px `#2c3e50`).
- **Takeaway (orange `#d95926`, bold 13px, bottom center):** "filling put three dots where the data was thickest — the truth was in the tail".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). `<h1>` + `.subtitle`, then four `.card-section` blocks, each an `<h2>` with a bottom border and a `table.layout` row: `.text-col` (50%) with `.tags` pills, one-line `<ul>` bullets opening with `<b>` terms, an italic `.example` line, and a `.key-point` callout; `.viz-col` (50%) holds one 720×300 canvas. (This page has no 3-column sections.)
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem, `li b` in `#1a5276`. Canvas CSS `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = bg `rgba(39,174,96,0.15)` / `#27ae60`, red = bg `rgba(231,76,60,0.12)` / `#e74c3c`, orange = bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvases:** intrinsic 720×300 `width`/`height` attributes (the `setup(id)` helper defaults to 720×300 if attributes are absent); backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`) with `ctx.scale` back to logical coordinates. All data hardcoded (no `Math.random()`); the tier chart carries an "illustrative" label in its title.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions (this page has none).
