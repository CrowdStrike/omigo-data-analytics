# Knowledge Distillation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Knowledge Distillation

**Subtitle:** A small student model trains to copy a big teacher model's full answer spread — most of the skill survives at a fraction of the size

## The Teacher's Full Answer, Not Just the Top Pick

**Tags:** `core idea` (blue), `teacher-student` (green)

- **The setup** — a big, expensive teacher model already does the task well; we want a small, cheap one
- **The trick** — the student doesn't train on right answers; it trains on the teacher's answers
- **The full spread** — for "the movie was ___" the teacher says: great 45%, good 30%, fine 15%, bad 7%, terrible 3%
- **Dark knowledge** — the spread reveals that "good" is almost right and "terrible" is very wrong
- **Soft labels** — those probability spreads are the student's training targets, class by class

*Example (italic):* A plain right/wrong label says only "great"; the teacher's spread also teaches the student how the wrong answers rank.

**Key point:** Distillation trains the student to copy the teacher's probabilities — every answer carries far more signal than a lone correct label.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow: a large teacher box emitting a soft-label spread that feeds a small student box.

- **Title (bold 15px, `#1a5276`, top center):** "The Teacher Writes the Student's Training Targets".
- **Teacher box:** rounded rect x=30, y=80, 170×130, fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; bold 13px `#4a3aa7` centered header "teacher model" at (115, 104); 12px `#2c3e50` centered lines at y=128/146/164: "huge — slow —", "expensive to run", "already skilled"; caption 11px `#6b7280` "(sizes illustrative)" at (115, 196).
- **Spread panel (middle):** rounded rect x=250, y=64, 220×162, fill `rgba(201,133,0,0.07)`, 2px `#c98500` border; bold 12px `#c98500` centered header "one soft label" at (360, 84); five horizontal bars for classes `["great","good","fine","bad","terrible"]` with values `[45, 30, 15, 7, 3]` (%): each row at y = 100 + i*24, label 12px `#2c3e50` right-aligned at x=322, bar from x=330 width = value*2.4 px, height 14, fill `#c98500`; value label 11px `#6b7280` after each bar ("45%", "30%", "15%", "7%", "3%").
- **Student box:** rounded rect x=520, y=105, 170×80, fill `rgba(0,131,0,0.10)`, 2px `#008300` border; bold 13px `#008300` centered header "student model" at (605, 129); 12px `#2c3e50` centered lines at y=150/168: "small — fast — cheap", "learns to copy the spread".
- **Arrows:** 2px `#6b7280` with arrowheads, from (200,145) to (244,145) and from (470,145) to (514,145); 11px `#6b7280` label "answers" centered at (222,132) on the first arrow, "targets" at (492,132) on the second.
- **Annotation (bold 12px orange `#d95926`, centered at y=262):** "the teacher's full spread — not the right answer — is what the student trains on".
- **Caption (11px `#444`, bottom right, y=290):** "probabilities illustrative".

## One Review, Three Classes, By Hand

**Tags:** `worked example` (blue), `soft labels` (orange)

- **The input** — one movie review: "the plot was fine but slow" — classes: positive / neutral / negative
- **Hard label** — a human tagged it neutral, so the plain target is neutral 100%, others 0%
- **Teacher's soft label** — the teacher says: neutral 60%, negative 30%, positive 10%
- **Student's current guess** — neutral 40%, negative 20%, positive 40% — too sure it's positive
- **The nudge** — per class, push toward the teacher: neutral +20, negative +10, positive −30

*Example (italic):* The hard label would only say "more neutral"; the teacher's spread also says "much less positive, a bit more negative" — three corrections from one example.

**Key point:** The student's error is the gap to the teacher's 60/30/10 spread — redo the three subtractions yourself and you've done a distillation step.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: for each of the three classes, three bars — hard label, teacher's soft label, student's current guess — with the per-class nudge written above.

- **Title (bold 15px, `#1a5276`, top center):** "One Training Example: Push the Student Toward the Teacher".
- **Legend (12px, top left, swatch + label):** gray `#6b7280` swatch "hard label" at (30,52); yellow `#c98500` swatch "teacher (target)" at (130,52); blue `#2a78d6` swatch "student now" at (280,52).
- **Axes:** baseline y=240, plot top y=80; y = probability 0–100% with light `#e5e9ef` gridlines at 25/50/75/100 and 12px `#444` right-aligned tick labels at x=64 ("25%", "50%", "75%", "100%"); axis lines 1px `#999` from (70,80) to (70,240) to (660,240).
- **Groups (centers x = `[180, 390, 600]`):** classes "neutral", "negative", "positive"; per group three bars 40px wide at center−62, center−14, center+34 (left edges), heights scaled 1.6px per % point:
  - neutral: hard 100 (gray), teacher 60 (yellow), student 40 (blue)
  - negative: hard 0 (gray, draw a 2px stub), teacher 30 (yellow), student 20 (blue)
  - positive: hard 0 (gray, 2px stub), teacher 10 (yellow), student 40 (blue)
  - 11px value labels above each bar in the bar's color ("100", "60", "40", "0", "30", "20", "0", "10", "40").
- **Nudge labels (bold 12px `#008300` for +, `#e74c3c` for −, centered above each group at y=95):** "nudge +20" (neutral), "nudge +10" (negative), "nudge −30" (positive).
- **X labels:** bold 12px `#444` class names centered at y=260.
- **Annotation (bold 12px orange `#d95926`, centered at y=283):** "the error signal is the gap to the yellow bars — the gray bar alone teaches far less".
- **Caption (11px `#444`, bottom right, y=296):** "one worked example, numbers exact".

## Why Small Models Punch Above Their Weight

**Tags:** `where it's used` (blue), `model compression` (green)

- **The pressure** — flagship models are too slow and costly for phones, laptops, and high-volume APIs
- **The route** — small "mini" and "flash" model tiers are commonly distilled from a larger sibling
- **Reasoning too** — a student can train on a teacher's written-out step-by-step solutions, not just labels
- **The payoff** — a student a tenth the size can keep most of the teacher's quality on the target tasks
- **The catch** — shrink too far and the rare, hard cases are the first skills to disappear

*Example (italic):* A support-ticket classifier distilled from a big general model runs on one cheap GPU and answers in milliseconds instead of seconds.

**Key point:** Distillation is the standard route from a flagship-quality demo to a model cheap enough to run everywhere.

### Visualization (canvas `c3`, 720×300)

Bar chart: quality retained vs model size — the teacher at full size, then three students at shrinking fractions of its size.

- **Title (bold 15px, `#1a5276`, top center):** "Quality Kept as the Student Shrinks (illustrative)".
- **Axes:** baseline y=235, plot top y=70; y = quality retained 0–100% with `#e5e9ef` gridlines at 25/50/75/100 and 12px `#444` tick labels right-aligned at x=64; axis lines 1px `#999` from (70,70) to (70,235) to (660,235).
- **Bars (90px wide, centered x = `[155, 305, 455, 605]`):** heights scaled 1.65px per % point:
  - "teacher" 100% quality, fill `#4a3aa7`
  - "student ¼ size" 97% quality, fill `#008300`
  - "student 1/10 size" 93% quality, fill `#2a78d6`
  - "student 1/30 size" 82% quality, fill `#c98500`
  - bold 13px value labels above each bar in the bar's color: "100%", "97%", "93%", "82%".
- **X labels:** 12px `#444` main label centered at y=254 ("teacher", "¼ size", "1/10 size", "1/30 size") and 11px `#6b7280` descriptor at y=270 ("full cost", "cheaper", "phone-friendly", "tiny").
- **Annotation (bold 12px orange `#d95926`, centered at (390, 92)):** "a tenth the size keeps most of the skill — the last drop is the rare, hard cases".
- **Caption (11px `#444`, bottom right, y=292):** "quality percentages illustrative".

## Not the Same as Squeezing the Model

**Tags:** `common mistake` (red), `compression vs distillation` (orange)

- **Quantization** — the same model, each knob stored with fewer digits; nothing is retrained
- **Pruning** — the same model with the least useful knobs deleted; the wiring stays
- **Distillation** — a brand-new smaller model, trained from the teacher's outputs
- **Vs fine-tuning** — fine-tuning uses human-made data; in distillation the teacher writes the dataset
- **They stack** — a real deployment often distills first, then quantizes the student

*Example (italic):* A "small" phone assistant is typically a distilled student that was then quantized — two different shrinks layered on each other.

**Common mistake:** Calling every small model "distilled" — distillation names the training recipe (teacher-written targets), not the size of the result.

### Visualization (canvas `c4`, 720×300)

Three side-by-side panels contrasting quantization, pruning, and distillation.

- **Title (bold 15px, `#1a5276`, top center):** "Three Ways to Shrink — Only One Trains a New Model".
- **Panels:** three rounded rects 210×180 at x = `[30, 255, 480]`, y=52; each with a bold 13px colored header centered at y=76 and content below.
  - **Panel 1 (blue `#2a78d6`, fill `rgba(42,120,214,0.07)`):** header "quantization"; inner rect 130×56 centered (x=70, y=96), fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 12px `#1a5276` centered two-line label "same model," / "fewer digits"; 12px `#2c3e50` centered lines at y=182/200: "3.14159 → 3.1", "no retraining".
  - **Panel 2 (magenta `#d55181`, fill `rgba(213,81,129,0.07)`):** header "pruning"; inner rect 130×56 centered (x=295, y=96), fill `rgba(213,81,129,0.12)`, 1.5px `#d55181` border, 12px `#1a5276` centered two-line label "same model," / "knobs deleted"; three small white 10×10 squares with 1px `#d55181` border punched inside the rect at (310,104), (350,124), (390,110); 12px `#2c3e50` centered lines at y=182/200: "weakest knobs cut", "wiring kept".
  - **Panel 3 (green `#008300`, fill `rgba(0,131,0,0.07)`):** header "distillation"; big outlined rect 92×40 at (500,92), 1.5px `#4a3aa7` border, 11px `#4a3aa7` centered label "teacher"; arrow 1.5px `#6b7280` from (546,132) to (546,150) with arrowhead; small filled rect 66×30 at (513,152), fill `rgba(0,131,0,0.15)`, 1.5px `#008300` border, 11px `#008300` centered label "student"; 12px `#2c3e50` centered lines at y=200/218 (panel center x=585): "new smaller model,", "trained on teacher output".
- **Annotation (bold 12px orange `#d95926`, centered at y=262):** "quantize and prune squeeze the same model — distillation teaches a new one".
- **Caption (11px `#444`, bottom right, y=290):** "diagrams simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values are the hardcoded arrays above (no randomness); the c1 spread must read 45/30/15/7/3, the c2 groups must read exactly hard 100/0/0, teacher 60/30/10, student 40/20/40 with nudges +20/+10/−30 to match the text, and c3 must read 100/97/93/82.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
