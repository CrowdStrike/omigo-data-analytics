# Surveys, Crowdsourcing & Annotation

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Surveys, Crowdsourcing & Annotation — Paying People to Produce Data

**Subtitle:** Every other acquisition channel finds data that someone already generated; this one manufactures it on demand. When the data does not exist, you ask humans to produce it — answers, opinions, and labels are all purchasable, from free web forms to million-dollar annotation contracts.

**Intro callout (blue-left-border box):** Data produced by paid or volunteered human effort has a property no log file has: the humans respond to incentives, instructions, and fatigue. What you get back is shaped less by the people answering than by how carefully you designed the ask — the sampling frame, the pay, the instruction document, and the quality gates. Every section below is a version of that one lesson.

## 1. Running surveys — the frame matters more than the sample size

Form tools made asking questions nearly free, but the hard part was never building the form — it is deciding who gets asked.

- **Form tools:** Google Forms, SurveyMonkey, Qualtrics — asking is free
- **Setup:** branching logic, validation, spreadsheet export in minutes
- **Sampling frame:** who could possibly see the survey defines the estimate
- **Convenience sample:** your followers measure your followers, at any n
- **Nonresponse bias:** strong opinions answer first, even in a good frame

Key point: More responses do not repair a bad frame. A convenient audience produces confident, wrong estimates about everyone else: the confidence interval shrinks with sample size, but it only measures noise — the bias from asking the wrong people never appears in it.

### Visualization (canvas `c1`, 720×380)

Sampling-frame diagram: a large population circle with a small skewed convenience-sample circle inside it, plus a two-bar comparison on the right showing the bias that sample size cannot close.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The frame decides what the estimate is about"
- **Population circle:** center (230, 205), radius 130, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered at (230, 92): "TARGET POPULATION"; 11px `#666` at (230, 108): "everyone you want to describe".
- **Population dots:** ~40 dots (radius 2.5, `#bbb`) scattered deterministically inside the population circle (angle/radius from a simple seeded loop), skipping the convenience-circle region.
- **Convenience circle:** center (295, 262), radius 52, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border. ~18 dots (radius 2.5, `#e74c3c`) clustered inside. Bold 11px `#e74c3c` centered at (295, 335): "CONVENIENCE SAMPLE"; 10px `#666` at (295, 350): "your followers, customers, mailing list".
- **Right panel (x=440..690):** bold 12px `#2c3e50` left-aligned at (440, 130): "Same question, two answers". Two labeled horizontal bars (height 20, from x=440):
  - y=160: 11px `#666` label above: "true value in the population"; bar width 130, fill `rgba(26,82,118,0.35)`, 1.5px `#1a5276` border.
  - y=225: 11px `#666` label above: "estimate from the convenience sample"; bar width 215, fill `rgba(231,76,60,0.12)`, 1.5px `#e74c3c` border.
  - Dashed (5/4) 1.5px `#e74c3c` bracket between the two bar ends with bold 11px `#e74c3c` label at (612, 200): "bias"; 10px `#999` at (440, 275), two lines: "n = 5,000 gives a tight interval —" / "tightly wrapped around the wrong answer".
- **Caption (12px `#999`, centered, y = h−14):** "Sample size shrinks the error bars; it never moves them onto the right answer"

## 2. Recruiting respondents — you pay for who answers

When you need strangers rather than your own audience, respondent marketplaces sell access to people — and the price tracks how defensible the sampling frame is.

- **Mechanical Turk:** anonymous crowd, cents per task, fast and high-volume
- **Quality tradeoff:** workers rationally optimize for throughput
- **Prolific:** research-oriented pool with prescreening and targeting
- **Probability panels:** Pew ATP, YouGov — recruited to mirror the population
- **What you buy:** representativeness itself, not just responses
- **Working strategy:** pilot cheap on the crowd, measure where the frame fits

Key point: You are not just paying for answers — you are paying for who answers. The price gap between a crowd platform and a probability panel is the price of a defensible sampling frame, and skipping it silently converts "the population thinks X" into "people who do online tasks for money think X".

### Visualization (canvas `c2`, 720×340)

Recruiting-spectrum diagram: cost axis along the bottom, representativeness axis up the left, three platform boxes climbing a dashed trade-off diagonal.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The recruiting spectrum — cost buys the frame"
- **Cost axis:** 2px `#999` horizontal line at y=270 from x=60 to x=680 with a filled right-arrowhead; 11px `#999` right-aligned at (680, 292): "cost per response →"; 10px `#999` at x=70: "cents" and at x=600: "dollars per respondent".
- **Representativeness axis:** 2px `#999` vertical line at x=60 from y=270 up to y=50 with a filled up-arrowhead; rotated (−90°) 11px `#999` label centered along it: "representativeness →".
- **Trade-off diagonal:** dashed (6/5) 1.5px `#ccc` line from (150, 225) through (600, 95).
- **Platform boxes (each 190×72, white fill, 2px border in box color; bold 12px label in box color, two 11px `#666` sublines, all left-aligned with 12px inset):**
  - (80, 185) `#e67e22` "Mechanical Turk" — "anonymous crowd: fast, cheap," / "highly variable quality"
  - (270, 125) `#1a5276` "Prolific" — "screened research pool," / "demographic targeting"
  - (475, 62) `#27ae60` "Probability panels" — "Pew ATP, YouGov panels:" / "recruited to mirror the population"
- **Caption (12px `#999`, centered, y = h−14):** "Cheap crowds answer fast; probability panels answer for everyone"

## 3. Annotation at scale — labels are manufactured

Supervised learning consumes labels, and an entire industry exists to manufacture them from raw examples.

- **Labeling vendors:** Scale AI, Appen, Labelbox, Toloka run annotator pools
- **The product:** raw images, text, and audio turned into training data
- **Instruction doc:** the written guide is the real label specification
- **Ambiguity cost:** unclear rules in the doc become noise in the dataset
- **Agreement metrics:** multiple annotators per item, agreement measured
- **RLHF turn:** preference rankings became fuel for aligning language models

Key point: An annotation pipeline is a measurement instrument. The instruction document, the annotator pool, and the adjudication rules all leave fingerprints on the labels — and therefore on every model trained from them.

### Visualization (canvas `c3`, 720×360)

Pipeline diagram: five stage boxes left to right with arrows, a dashed rework loop from the agreement check back to the instructions, and an RLHF note box below.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The label factory — from raw examples to a training set"
- **Stage boxes (each 120×66 at y=70, white fill, 2px border in stage color; bold 11px label in stage color, two 10px `#666` sublines, all centered in the box):**
  - x=30 `#999` "RAW EXAMPLES" — "unlabeled images," / "text, audio"
  - x=165 `#1a5276` "INSTRUCTION DOC" — "definitions and" / "edge-case rules"
  - x=300 `#e67e22` "ANNOTATOR POOL" — "trained workers," / "3 labels per item"
  - x=435 `#8e44ad` "AGREEMENT CHECK" — "do the labels" / "agree?"
  - x=570 `#27ae60` "TRAINING SET" — "labeled and" / "adjudicated"
- **Arrows:** 1.5px `#bbb` connector between consecutive boxes at mid-height (y=103) with small filled arrowheads.
- **Rework loop:** dashed (6/5) 1.5px `#e74c3c` path from the bottom of the agreement-check box (495, 136) down to y=175, left to x=225, up into the bottom of the instruction-doc box, ending in a filled `#e74c3c` arrowhead; 10px `#e74c3c` centered at (360, 190), two lines: "low agreement → rewrite the instructions," / "retrain the annotators, relabel".
- **RLHF note box:** 560×64 centered at x=360, top y=230, fill `rgba(142,68,173,0.12)`, 2px `#8e44ad` border. Bold 12px `#8e44ad` centered: "RLHF made preference labels a strategic commodity"; two 11px `#666` centered lines: "annotators rank model outputs; the rankings become the reward signal" / "that aligns language models".
- **Caption (12px `#999`, centered, y = h−14):** "Ambiguity in the instruction doc comes out the other end as noise in the model"

## 4. Quality control — gates, agreement, and fair pay

Paid data production invites careless and automated responses, so every serious pipeline runs explicit quality gates — and the best lever is often on the requester's side.

- **Attention checks:** planted items with known answers catch careless work
- **Gold questions:** failing them drops the whole submission
- **Bot detection:** timing, device, and duplicate signals protect the pool
- **Agreement check:** heavy disagreement usually means an ill-defined task
- **Fix direction:** low agreement sends you back to the instructions
- **Fair pay:** measurably improves quality — rushed work is underpaid work

Key point: Quality control is not an accusation against workers. Most quality failures trace back to the requester — unclear instructions, impossible time budgets, and pay that rewards speed over care — and the gates exist to surface those design errors early.

### Visualization (canvas `c4`, 720×380)

Gate-sequence diagram: submissions flow left to right through three quality gates, rejected items drop down to a reject zone at each gate, and survivors reach an accepted box.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Quality gates — careless work drops out before it reaches the dataset"
- **Incoming box:** 118×56 at (20, 92), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 11px `#1a5276` centered: "SUBMISSIONS"; 10px `#666`: "raw, unchecked".
- **Gates (each a 96×112 rounded-feel rectangle at y=64, white fill, 2px border in gate color; bold 10px two-line label in gate color near the top, 9.5px `#666` two-line subline below):**
  - x=185 `#1a5276` "ATTENTION" / "CHECKS" — "gold questions" / "with known answers"
  - x=340 `#e67e22` "BOT +" / "DUPLICATE" — "timing, device," / "repeat detection"
  - x=495 `#8e44ad` "AGREEMENT" — "compare against" / "other annotators"
- **Flow arrows:** 2px `#999` horizontal arrows at y=120 connecting incoming box → gate 1 → gate 2 → gate 3 → accepted box, each with a filled arrowhead.
- **Accepted box:** 90×56 at (620, 92), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 11px `#27ae60` centered: "ACCEPTED"; 10px `#666`: "enters dataset".
- **Reject drops:** from the bottom-center of each gate, a dashed (5/4) 1.5px `#e74c3c` vertical arrow down to y=246; below each, bold 10px `#e74c3c` label (y=266) and 10px `#999` subline (y=280): gate 1 "careless" / "failed gold items", gate 2 "automated" / "bots, duplicates", gate 3 "outliers" / "systematic disagreement".
- **Reject zone:** dashed (5/4) 1.5px `#e74c3c` rectangle 560×40 at (80, 250) enclosing the drop labels; below it, bold 11px `#e74c3c` centered at y=308: "REJECTED — never reaches the dataset"; 10px `#999` centered at y=324: "each rejection is also feedback on the task design".
- **Fair-pay note (11px `#27ae60`, centered, y=346):** "fair pay raises the pass rate at every gate — rushed work is exactly what the gates catch"
- **Caption (12px `#999`, centered, y = h−14):** "Most rejections trace back to the requester: unclear instructions and pay that rewards speed"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; `li strong` colored `#1a5276`; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Bullets:** each bullet is a bold `#1a5276` label plus a short phrase that fits on one line — no wrapping; split dense content into more bullets rather than longer ones.
- **Canvases:** intrinsic width 720, heights 380/340/360/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (multiply backing store, `ctx.scale(dpr,dpr)`). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(39,174,96,0.10)`, `rgba(142,68,173,0.12)`, `rgba(231,76,60,0.12)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.

