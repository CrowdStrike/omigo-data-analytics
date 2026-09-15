# Descriptive, Predictive, Prescriptive

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Descriptive, Predictive, Prescriptive

**Subtitle:** Analytics climbs a three-rung ladder — what happened, what will happen, what to do about it — and every rung stands on the one below

## Three Questions About the Same Churn

**Tags:** `core idea` (blue), `analytics ladder` (green), `churn` (orange)

- **The business** — a subscription service with 20,000 subscribers watches customers cancel every month
- **Descriptive** — what happened: 1,200 canceled last month, a 6% churn rate, worst on the Basic plan
- **Predictive** — what will happen: a model gives each current customer a churn-risk score for next month
- **Prescriptive** — what to do: choose who gets a $10 retention offer, weighing cost against saved revenue
- **The ladder** — each rung uses the one below: prescription usually leans on prediction, prediction on description

*Example (italic):* At the monthly review, "churn was 6%" (descriptive) becomes "these 2,000 look likely to leave" (predictive) becomes "send those 2,000 a discount" (prescriptive).

**Key point:** The three rungs answer three different questions about the same data — what happened, what will happen, and what to do — and each rung is built directly on the output of the one below.

### Visualization (canvas `c1`, 720×300)

Three-rung ladder diagram: stacked rounded boxes (descriptive at the bottom, prescriptive at the top) with upward arrows between them and the churn example stated beside each rung.

- **Title (bold 15px, `#1a5276`, top center):** "The Analytics Ladder: One Churn Problem, Three Questions".
- **Boxes (left column, x=70, width 260, height 46, radius 8):** bottom box top-left at y=228 labeled "DESCRIPTIVE — what happened" (fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border); middle box at y=148 labeled "PREDICTIVE — what will happen" (fill `rgba(25,158,112,0.15)`, 2px `#199e70` border); top box at y=68 labeled "PRESCRIPTIVE — what to do" (fill `rgba(217,89,38,0.15)`, 2px `#d95926` border); box labels bold 13px `#2c3e50`, centered.
- **Arrows:** 3px `#6b7280` vertical arrows with solid heads from box top to next box bottom, at x=200, spanning y=228→194 and y=148→114.
- **Side notes (12px `#444`, left-aligned at x=360, vertically centered on each box):** bottom "churn was 6% last month; Basic plan worst"; middle "each customer scored for next-month churn risk"; top "top-risk 2,000 customers get the $10 offer".
- **Annotation (bold 13px violet `#4a3aa7`, at x=360, y=40):** "each rung consumes the rung below it".
- **Caption (12px `#444`, bottom right):** "churn numbers illustrative".

## From 6% Churn to a Ranked Offer List

**Tags:** `worked example` (blue), `hand-check` (green)

- **The base rate** — 1,200 of 20,000 subscribers canceled last month: churn = 6.0%
- **The slice** — by plan: Basic 9%, Standard 5%, Premium 2% — the description already localizes the pain
- **The scores** — a model ranks customers into ten risk deciles; the top decile averages 32% risk
- **The economics** — an offer costs $10, a saved customer keeps $90 of revenue, offers save 40% of takers
- **Hand-check** — break-even risk = $10 / (0.40 × $90) = 27.8%; only the top decile clears it

*Example (italic):* Offering the top decile (2,000 customers) expects 0.32 × 0.40 × $90 − $10 = $1.52 gained per offer; decile 9 at 8% risk would lose $7.12 each.

**Key point:** The prescriptive answer is a threshold, not a score — send the offer only where predicted risk beats the 27.8% break-even, a cutoff the descriptive 6% base rate alone could never give you.

### Visualization (canvas `c2`, 720×300)

Bar chart of average churn risk by model decile with a dashed break-even line; only the top decile crosses it, so only that decile gets the offer.

- **Title (bold 15px, `#1a5276`, top center):** "Predicted Risk by Decile vs the 27.8% Break-Even Line".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = deciles 1–10, one 12px `#444` tick label centered under each bar; y = churn risk 0% to 35%, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Bars:** 10 bars, 44px wide, evenly spaced across the plot; decile risks (%) `[0.5, 1, 1.5, 2, 2.5, 3, 4, 5.5, 8, 32]`; deciles 1–9 fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; decile 10 fill `rgba(0,131,0,0.30)` with 2px `#008300` border and bold 12px `#008300` value label "32%" above it.
- **Break-even line:** horizontal dashed `#d95926` (dash 6/4, 2px) at 27.8%, bold 12px `#d95926` label "break-even 27.8%" above the line at the left end.
- **Annotation (bold 13px green `#008300`, near decile 8, y=70):** "only decile 10 clears break-even → 2,000 offers".
- **Caption (12px `#444`, bottom right):** "decile risks illustrative; break-even 27.8% exact from $10 / (0.40 × $90)".

## Most of the Value Is on the Bottom Rung

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **The workhorse** — most business questions ("is churn up? where? since when?") live on the descriptive rung
- **The slice pays** — Basic at 9% vs Premium at 2% already says where to look, with no model at all
- **Stacked assumptions** — prediction adds "the past pattern holds"; prescription adds cost and response guesses
- **More ways wrong** — a bad dashboard misreads the past; a bad offer policy spends real money on it
- **Inheritance** — a model trained on miscounted churn events is confidently wrong at scale

*Example (italic):* If the assumed 40% save rate is really 20%, break-even doubles to 55.6% risk and the top-decile offer flips from +$1.52 to −$4.24 per customer.

**Key point:** Each rung up multiplies leverage and fragility together — the higher rungs are worth climbing only after the rung below is solid, and descriptive done well already answers most of the questions.

### Visualization (canvas `c3`, 720×300)

Horizontal stacked bar chart: assumptions carried by each rung, split into assumptions inherited from below (blue) and new assumptions the rung adds (orange).

- **Title (bold 15px, `#1a5276`, top center):** "Every Rung Inherits the Assumptions Below and Adds Its Own".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 44px per assumption; x-axis 12px `#444` tick labels "0" to "7" under the plot at y=268.
- **Rows (bar top edges at y = 70, 135, 200, bars 26px tall), each with a left-aligned 12px `#444` two-line label at x=20:**
  - "Descriptive": orange `rgba(217,89,38,0.55)` added-segment width 88 (2 added: "events counted once", "one churn definition"), no inherited segment
  - "Predictive": blue `rgba(42,120,214,0.35)` inherited segment width 88, then orange added segment width 88 (2 added: "past pattern holds", "features known at score time")
  - "Prescriptive": blue inherited segment width 176, then orange added segment width 132 (3 added: "save rate 40%", "worth $90", "no discount-hunting taught")
- **Segment labels:** 11px `#2c3e50` count labels inside segments ("2", "2 + 2", "4 + 3"); the added assumptions listed as 11px `#6b7280` text right of each bar end.
- **Legend (12px, top right):** blue swatch "inherited from below", orange swatch "added by this rung".
- **Annotation (bold 13px magenta `#d55181`, bottom center near y=250):** "7 ways to be wrong by the top rung — each one silent".
- **Caption (12px `#444`, bottom right):** "assumption counts schematic".

## Predicting on Top of Broken Definitions

**Tags:** `common mistake` (red), `data quality` (orange)

- **The jump** — a team asks for a churn model before the company agrees on what "churned" means
- **Two definitions** — billing calls churn "canceled"; product calls it "30 days inactive" — the labels disagree
- **Dirty events** — cancel events double-fire from a retry bug, so even the training label is miscounted
- **The symptom** — the model looks accurate in backtest and useless in every production argument
- **The fix** — one written churn definition and a deduplicated event stream before any modeling starts

*Example (italic):* On the same 20,000 customers the two definitions produce 1,200 vs 1,750 "churners" — the model can hit either target, but no single model can hit both.

**Common mistake:** Jumping to prediction before descriptive basics exist. A score is precision stacked on ambiguity when nobody has cleanly defined the quantity being predicted — fix the events and the definition first.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: building the model straight on raw events (labels disagree, model distrusted) vs doing the descriptive rung first (one label, model trusted).

- **Title (bold 15px, `#1a5276`, top center):** "Skip the Descriptive Rung and the Model Inherits the Mess".
- **Row 1 (boxes vertically centered at y=105), label 12px `#444` at x=20:** "skip the rung"; blue `#2a78d6` rounded box at x=150 labeled "raw events: dupes, 2 churn definitions" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "model fits ambiguity" with bold 12px red "✗ 1,200 vs 1,750 — teams argue" beneath it.
- **Row 2 (boxes vertically centered at y=215), label:** "descriptive first"; blue box at x=150 labeled "dedupe + one written definition", 3px arrow to a green `#008300` box at x=360 labeled "one label: 1,200 churners", then arrow to a green box at x=555 labeled "model teams trust" with bold 12px green "✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, 2px borders in the box color.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "the model was never the blocker — the definition was".
- **Caption (12px `#444`, bottom right):** "churner counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); subscriber counts, plan-level churn rates, decile risks `[0.5, 1, 1.5, 2, 2.5, 3, 4, 5.5, 8, 32]`, and the 1,200 vs 1,750 label counts are invented and labeled illustrative; the break-even 27.8% = $10 / (0.40 × $90), the per-offer values +$1.52 / −$7.12 / −$4.24, and the 55.6% break-even under a 20% save rate are exact arithmetic from those illustrative inputs.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
