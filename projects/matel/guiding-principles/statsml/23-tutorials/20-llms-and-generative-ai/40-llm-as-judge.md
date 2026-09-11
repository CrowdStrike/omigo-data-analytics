# LLM as Judge

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** LLM as Judge

**Subtitle:** Use a strong model to grade thousands of outputs against your rubric — then spot-check the judge itself with humans

## 1,000 Summaries to Grade, One Weekend to Do It

**Tags:** `core idea` (blue), `running example` (green)

- **The pile** — 1,000 generated ticket summaries need a pass/fail grade against a rubric
- **Humans** — 3 minutes each × 1,000 = 50 hours; at $30/hour that is $1,500
- **The judge** — a strong LLM gets the rubric plus each ticket-and-summary pair
- **Its job** — answer the same yes/no questions a human grader would, at scale
- **The bill** — about $8 in API calls and roughly 35 minutes, illustrative but typical

*Example (italic):* The same rubric from the golden set — "accurate? complete? concise?" — becomes the judge's prompt.

**Key point:** An LLM judge is a grading machine: it applies your rubric at a scale and speed humans cannot — the question is whether you can trust its grades.

### Visualization (canvas `c1`, 720×300)

Two side-by-side panels of horizontal bars comparing humans vs judge on time and cost, split by a vertical dashed divider at mid-width.

- **Title (bold 15px, `#1a5276`, top center):** "Grading 1,000 Outputs: Humans vs LLM Judge (illustrative)".
- **Divider:** vertical dashed line (`#bdc3c7`, dash 4/3) at w/2 from y=40 to h-15.
- **Left panel "Time to grade":** two horizontal bars (30px tall, rows at y=90 and y=160), scale max 3000 minutes — "humans" 3000 (rendered label "50 hours", blue `#2a78d6`), "judge" 35 (label "35 min", aqua `#199e70`); row names right-aligned 12px left of the bars, bold 13px value labels right of each bar; minimum bar length 4px.
- **Right panel "Cost to grade":** same layout, scale max $1500 — "humans" $1500 (blue), "judge" $8 (aqua); labels "$1500", "$8".
- **Panel titles:** bold 13px `#1a5276` centered at y=58.
- **Takeaway (bold 13px aqua `#199e70`, bottom center):** "~85x faster, ~190x cheaper — worthless unless the grades can be trusted".

## Spot-Checking the Judge on 50 Outputs

**Tags:** `worked example` (green), `core idea` (blue)

- **The check** — humans re-grade 50 of the 1,000; compare their calls to the judge's
- **Both say pass** — 32 outputs; **both say fail** — 9 outputs
- **Disagreements** — judge passes 6 that humans fail; fails 3 that humans pass
- **Agreement** — (32 + 9) / 50 = 41/50 = 82%
- **Read the 9** — each disagreement is either a judge error or a vague rubric line

*Example (italic):* Five of the 6 wrongly-passed outputs missed a fact — the judge wasn't checking against the ticket closely.

**Key point:** Never report a judge's scores without an agreement number. "The judge says 76% pass" means little until you add "and it agrees with humans 82% of the time".

### Visualization (canvas `c2`, 720×300)

2×2 agreement (confusion) matrix of judge vs human pass/fail calls on 50 outputs, with side annotations.

- **Title (bold 15px, `#1a5276`, top center):** "Judge vs Human on the Same 50 Outputs".
- **Matrix:** 92px square cells, 8px gap, origin at x=240, y=78. Rows = human (pass, fail), columns = judge (pass, fail). Counts: [[32, 3], [6, 9]].
- **Cell colors:** agree cells (32 "both pass", 9 "both fail") translucent green `rgba(0,131,0,0.55)`; human-pass/judge-fail (3) `rgba(217,89,38,0.65)`; human-fail/judge-pass (6) `rgba(231,76,60,0.8)`.
- **Cell text:** count in bold 26px white centered; 11px white sub-label "agree" on diagonal cells, "disagree" off-diagonal.
- **Axis labels (bold 12px `#1a5276`):** "judge: pass" and "judge: fail" centered above the columns; "human: pass" and "human: fail" right-aligned left of the rows.
- **Annotations (right of matrix):** bold 14px green `#008300`: "agreement: (32 + 9) / 50 = 82%"; bold 12px red `#e74c3c`, two lines: "6 false passes are the risky kind:" / "bad outputs the judge waves through".
- **Caption (12px mute `#6b7280`, bottom center):** "read every disagreement: each is a judge error or a vague rubric line".

## The Judge Has Tastes: Long and Familiar Wins

**Tags:** `bias` (orange), `common mistake` (red)

- **Length bias** — judges tend to reward longer answers even when content is equal
- **Our numbers** — human pass rates by length: 72%, 74%, 73% — essentially flat
- **Judge's version** — same bins: 60%, 74%, 86% — a 26-point tilt toward long answers
- **Self-preference** — a judge scores text written in its own style more kindly
- **Position bias** — in "compare A vs B" setups, the first answer shown wins more often

*Example (italic):* Swap the order of A and B in the comparison prompt and re-run — if the winner changes, that's position bias.

**Common mistake:** Optimizing outputs to please the judge. Reward its length bias and your summaries grow — while the humans they're written for stop reading them.

### Visualization (canvas `c3`, 720×300)

Two-series line chart: pass rate by answer-length bin, human graders flat vs LLM judge tilted.

- **Title (bold 15px, `#1a5276`, top center):** "Pass Rate by Answer Length (illustrative)".
- **Data:** x bins short / medium / long; human graders `[72, 74, 73]` (blue `#2a78d6`); LLM judge `[60, 74, 86]` (orange `#d95926`).
- **Axes:** y 0–100% with ticks 0%, 50%, 100% and gridlines `#e5e9ef`; x bin labels 12px below baseline; x-axis title "answer length bin" (12px mute, bottom center); padding top 56, bottom 50, left 60, right 170; points at 18%, 50%, 82% of chart width.
- **Series:** 3px lines with 5px-radius dots; bold 12px value labels at each point ("72%" etc., human labels offset left/below, judge labels offset right/above), each in its series color.
- **Legend (right side, x = w-155):** blue swatch "human graders", orange swatch "LLM judge".
- **Annotation (bold 12px orange, below legend, three lines):** "humans see flat quality;" / "the judge pays +26 pts" / "for length alone".

## The Working Loop: Judge at Scale, Humans as Anchor

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Judge everything** — all 1,000 outputs get a grade; no human could keep up
- **Humans anchor** — a 50-output spot-check per batch keeps the judge honest
- **Threshold** — agreement at or above ~80%: trust the batch; below: stop and fix
- **Fix means** — tighten vague rubric lines, or swap the judge model, then re-check
- **Re-anchor on change** — new rubric, new judge, or new task: the old agreement is void

*Example (italic):* The team's fix was one rubric line: "compare each fact to the ticket" — agreement rose from 82% to 90%.

**Rule of thumb:** The judge replaces human volume, never human judgment. Scale with the machine, calibrate with people.

### Visualization (canvas `c4`, 720×300)

Flow diagram (boxes and arrows) of the calibration loop with a yes/no decision branch.

- **Title (bold 15px, `#1a5276`, top center):** "The Calibration Loop".
- **Boxes (filled rect + 2px stroke, bold 12px centered text):**
  - "1,000 outputs" at (30, 90), 140×56, fill `#eaf2fb`, stroke blue `#2a78d6`.
  - "judge grades all" / "(rubric prompt)" at (220, 90), 150×56, fill `#eaf2fb`, stroke blue.
  - "humans re-grade" / "the 50" at (420, 90), 150×56, fill `#fdf2e9`, stroke orange `#d95926`.
  - "agreement" / ">= 80% ?" at (420, 190), 150×52, fill `#f8f9fa`, stroke mute `#6b7280`.
  - "trust the batch" at (600, 250), 110×40, fill `rgba(0,131,0,0.12)`, stroke and text green `#008300`.
  - "tighten rubric /" / "change judge" at (90, 190), 150×52, fill `rgba(231,76,60,0.1)`, stroke and text red `#e74c3c`.
- **Arrows (2px, filled arrowheads):** outputs → judge (mute); judge → humans (mute, labeled "sample 50"); humans ↓ agreement box (mute); agreement → right in green with label "yes: 82%", then down into "trust the batch"; agreement → left in red toward the fix box, with red bold label "no: fix rubric or swap judge, re-check"; fix box → back up to the judge box (red).
- **Caption (12px mute, bottom center):** "re-anchor whenever the rubric, the judge model, or the task changes".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
