# The Confusion Matrix

**Page type:** detail page (tutorial card-sections: h2 per section; two-column table.layout 45/55, with one 3-column row 38/31/31 holding two canvases)
**HTML title tag:** The Confusion Matrix

**Subtitle:** Four boxes that hold everything a yes/no model did — every metric you'll ever quote is just arithmetic on these four counts

## Sorting 1,000 Transactions Into Four Boxes

**Tags:** `core idea` (blue), `running example` (green)

- **Two questions per row** — was it really fraud, and did the model flag it?
- **True positive (TP)** — fraud, and flagged: 40
- **False negative (FN)** — fraud, but let through: 10
- **False positive (FP)** — legit, but flagged anyway: 60
- **True negative (TN)** — legit, and passed: 890
- **Check** — 40 + 10 + 60 + 890 = 1,000; every transaction lands in exactly one box

*Example:* The two "false" boxes are the model's mistakes: 60 false alarms and 10 missed frauds.

**Key point:** The matrix is the complete record of what the model did — any single metric is a summary that throws boxes away.

### Visualization (canvas `c1`, 720×300)

A 2×2 confusion matrix (shared `drawMatrix` helper) with side annotations and a sum check.

- **Title (bold 15px, `#1a5276`, top center):** "All 1,000 Transactions, Each in Exactly One Box".
- **Matrix (at x=200, y=62; cells 170×88):** rows = actual, columns = model said. Cells:
  - TP = 40, sub-label "caught", fill `rgba(0,131,0,0.75)` (green), row 0 col 0
  - FN = 10, sub-label "missed fraud", fill `rgba(213,81,129,0.80)` (magenta), row 0 col 1
  - FP = 60, sub-label "false alarm", fill `rgba(217,89,38,0.80)` (orange), row 1 col 0
  - TN = 890, sub-label "passed", fill `rgba(42,120,214,0.35)` (light blue, text in `#1a5276`), row 1 col 1
  - Cell text white bold 15px "LABEL = N" plus 12px sub-label; white 2px cell borders; gray `#6b7280` outer border.
- **Axis labels (bold 12px `#1a5276`):** column headers above — "model says \"fraud\"", "model says \"legit\""; rotated row labels on the left — "fraud (50)", "legit (950)".
- **Side annotations (left-aligned at x=560):** "rows: the truth" and "columns: the model" (12px `#2c3e50`); "diagonal = correct" (bold 12px green `#008300`); "off-diagonal = mistakes" (bold 12px red `#e74c3c`).
- **Bottom annotation (orange `#d95926` bold 14px, centered, y=274):** "40 + 10 + 60 + 890 = 1,000 — nothing missing, nothing counted twice".

## Reading Metrics Off the Boxes

**Tags:** `worked example` (green), `core idea` (blue)

- **Accuracy** — (TP + TN) / all = (40 + 890) / 1,000 = 93%
- **Precision** — TP / (TP + FP) = 40 / 100 = 40% — read down the "flagged" column
- **Recall** — TP / (TP + FN) = 40 / 50 = 80% — read across the "fraud" row
- **False alarm rate** — FP / (FP + TN) = 60 / 950 ≈ 6.3% of legit customers flagged
- **The twist** — a "never fraud" model scores 95% accuracy; this one 93%, yet catches 40 frauds

*Example:* Every metric above uses only the four counts — no new information, just different recipes.

**Key point:** Precision is a column story, recall is a row story — both start from the same TP box.

This row uses the 3-column layout: text 38%, then two viz columns of 31% each.

### Visualization (canvas `c2a`, 420×340)

The same 2×2 matrix with the "flagged" (left) column highlighted for precision.

- **Title (bold 15px, `#1a5276`, top center):** "Precision: the Flagged Column".
- **Matrix (at x=90, y=70; cells 130×82):** same cells/colors/labels as c1, but cells NOT in column 0 are dimmed to 18% opacity with gray `#6b7280` text.
- **Bracket:** violet `#4a3aa7` 3px rectangle around the left column (TP over FP).
- **Bottom annotations (centered):** violet bold 14px: "40 / (40 + 60) = 40%"; gray 12px: "of the 100 flagged, how many were fraud?"; violet bold 12px: "read DOWN one column".

### Visualization (canvas `c2b`, 400×340)

The same 2×2 matrix with the "fraud" (top) row highlighted for recall.

- **Title (bold 15px, `#1a5276`, top center):** "Recall: the Fraud Row".
- **Matrix (at x=80, y=70; cells 130×82):** same cells as c1, but cells NOT in row 0 are dimmed to 18% opacity with gray text.
- **Bracket:** aqua `#199e70` 3px rectangle around the top row (TP and FN).
- **Bottom annotations (centered):** aqua bold 14px: "40 / (40 + 10) = 80%"; gray 12px: "of the 50 frauds, how many got flagged?"; aqua bold 12px: "read ACROSS one row".

## Why Keep All Four Numbers

**Tags:** `where it's used` (blue), `costs` (orange)

- **Errors differ** — a false positive annoys a customer; a false negative loses the stolen money
- **Price them** — 10 missed frauds × $500 = $5,000 lost; 60 false alarms × $5 review = $300
- **One number hides this** — "93% accurate" says nothing about the $5,000 vs $300 split
- **Threshold tuning** — moving the flagging bar shifts counts between boxes; the matrix shows where
- **Debugging** — a box that grows after a release points straight at what changed

*Example:* Two models can tie on accuracy while one loses 17x more money to missed fraud.

**Key point:** Business decisions need the four counts and their costs — not one blended score.

### Visualization (canvas `c3`, 720×300)

Two-bar dollar-cost comparison of the mistake boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Same Matrix, Priced: the Small Box Costs 17x More".
- **Axes:** padding top 56, bottom 62, left 100, right 40; y in dollars 0–5,500 with tick labels "$0", "$2,500", "$5,000" (gray 12px) and light gridlines `#e5e9ef`; L-shaped `#999` axes.
- **Bars (160px wide):**
  - Magenta `#d55181`: $5,000 — label "10 missed frauds (FN)" (12px), sub-label "10 × $500 stolen" (gray 12px), value "$5,000" bold 13px above.
  - Orange `#d95926`: $300 — label "60 false alarms (FP)", sub-label "60 × $5 review time", value "$300".
- **Annotations (centered):** magenta bold 13px near top of plot: "accuracy 93% blends both mistakes — the dollars do not"; gray 12px at bottom: "illustrative costs: $500 per missed fraud, $5 per alert reviewed".

## The Confusion: Which "False" Is Which

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **Decode the name backwards** — the second word is what the MODEL said
- **False positive** — model said "fraud" (positive), and that was false: a false alarm
- **False negative** — model said "legit" (negative), and that was false: a missed fraud
- **Axis flips** — libraries disagree on row/column order; read the labels, never assume
- **Older names** — statisticians call FP a Type I error and FN a Type II error

*Example:* Swapping FP and FN turns "60 annoyed customers" into "60 missed frauds" — a very different meeting.

**Common mistake:** Reading "positive/negative" as the truth — it is the model's answer; "true/false" says whether that answer was right.

### Visualization (canvas `c4`, 720×300)

Two word-decoding diagrams side by side (FALSE POSITIVE at cx=200, FALSE NEGATIVE at cx=540), split by a vertical dashed divider.

- **Title (bold 15px, `#1a5276`, top center):** "Decode the Name: Second Word = the Model, First Word = Was It Right".
- **Each diagram:** two outlined word boxes at y=78 (90px and 110px wide, 36px tall, fill `#f8f9fa`, 2px colored border, bold 16px colored word text) — "FALSE" + "POSITIVE" in orange `#d95926`, "FALSE" + "NEGATIVE" in magenta `#d55181`. Gray arrows point down from each box to two-line 12px gray explanations: under FALSE — "the answer" / "was wrong"; under the second word — "model said" / "\"fraud\"" (POSITIVE) or "\"legit\"" (NEGATIVE).
- **Meaning lines (centered under each diagram):** bold 13px colored meaning + 12px `#2c3e50` count — left: "a false alarm" / "60 legit customers flagged"; right: "a missed fraud" / "10 frauds let through".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at x=370 from y=50 to y=240.
- **Takeaway (red `#e74c3c` bold 13px, centered, y=274):** "\"positive\" is what the model claimed — not what was true".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials/ style). h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left text `<td>` holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` paragraph, and a `.key-point` callout; right cell(s) holding canvases. Sections 1, 3, 4 use `.text-col` 50% / `.viz-col` 50%; section 2 uses the 3-column variant `.text-col3` 38% / two `.viz-col3` 31% cells (canvases c2a 420×340 and c2b 400×340).
- **Shared JS:** a `drawMatrix(ctx, x, y, cellW, cellH, opts)` helper draws the 2×2 matrix (cells TP/FN/FP/TN with counts 40/10/60/890, colors as specified in c1) and supports an `opts.highlight(cell)` predicate that dims non-matching cells to 18% alpha.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; `li b` in `#1a5276`; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; canvases `width:100%`, `1px solid #e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic width/height attributes as given per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; all data hardcoded literal values (no Math.random). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions. No nav bar, no back/home links.
