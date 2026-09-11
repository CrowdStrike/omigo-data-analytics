# Latent Variables

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Latent Variables

**Subtitle:** A latent variable is the thing your model needs but no column ever records — you never observe it directly, you only infer it from the traces it leaves in the data

## One Histogram, Two Invisible Crowds

**Tags:** `core idea` (blue), `hidden variable` (orange), `observed vs latent` (green)

- **The shop** — a coffee shop logs prep time for 200 morning drinks, and nothing else
- **The shape** — the histogram has two humps: one near 85 seconds, one near 230 seconds
- **The hidden column** — commuters vs lingerers would explain the humps, but no column says which
- **Latent variable** — a variable the model needs that this dataset never recorded and cannot recover
- **Observed vs latent** — prep time is observed; "customer type" is latent: inferred, not read

*Example (italic):* The overall mean is 143 seconds — a prep time almost no real drink has, because 143 sits in the valley between the two hidden crowds.

**Key point:** A latent variable is the unrecorded cause behind a pattern in recorded data. You model it, estimate it, and reason about it — but you never see it.

### Visualization (canvas `c1`, 720×300)

Single histogram of the 200 prep times showing a clear two-hump shape, with the misleading overall mean marked in the valley between them.

- **Title (bold 15px, `#1a5276`, top center):** "Prep Time for 200 Morning Drinks: Two Humps, No Column Explaining Them".
- **Data:** ten 30-second bins from 30s to 330s; counts `[18, 50, 41, 10, 4, 20, 30, 20, 5, 2]` (sums to 200); bin edge labels "30s", "60s", "90s", "120s", "150s", "180s", "210s", "240s", "270s", "300s", "330s" (11px `#444` below each edge).
- **Axes:** origin x=60, plot width 600, baseline y=245, chart height 185, y scale 0–60; 2px ink `#1a5276` axis lines; y ticks at 0/20/40/60 (12px `#6b7280`).
- **Bars:** fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke.
- **Hump labels:** blue `#2a78d6` bold 13px "hump A ≈ 85s" above the 60–90s bar; magenta `#d55181` bold 13px "hump B ≈ 230s" above the 210–240s bar.
- **Mean marker:** vertical dashed orange `#d95926` line (dash 5/4) at 143s from baseline to y=55; orange bold 12px two-line annotation beside it: "mean = 143s" / "in the valley — describes no one".
- **Caption (12px `#444`, bottom right):** "illustrative data — one observed column, prep time only".

## Splitting the Humps Without Seeing the Groups

**Tags:** `worked example` (blue), `mixture model` (green), `soft labels` (orange)

- **Guess a structure** — assume two hidden groups, each with its own typical time and spread
- **Fit** — best fit: 60% "quick" around 85s (spread 25s), 40% "slow" around 230s (spread 30s)
- **Soft labels** — every drink gets a probability of each group: a 70s drink is 99% quick
- **The valley** — a 155s drink splits about 50/50; the model admits it cannot tell
- **Check the sum** — 0.60 × 85 + 0.40 × 230 = 143, matching the overall mean exactly

*Example (italic):* Drink #37 took 240 seconds; the fitted model assigns it 99% "slow" — a label no barista ever wrote down, produced entirely by the fit.

**Key point:** You never observe the group, so you estimate a probability of it for every row. Mixture models and EM repeat exactly this guess-fit-relabel loop.

### Visualization (canvas `c2`, 720×300)

Dual panel split by a vertical dashed divider at x=360: fitted mixture components over the histogram (left) and the soft-label probability curve (right).

- **Title (bold 15px, `#1a5276`, top center):** "Fitting Two Hidden Groups: Components (left), Soft Labels (right)".
- **Left panel (mixture fit):** axis origin x=55, width 280, baseline y=245, chart height 175, x range 30–330s, y scale 0–60; the c1 histogram counts `[18, 50, 41, 10, 4, 20, 30, 20, 5, 2]` redrawn as bars fill `rgba(107,114,128,0.25)` with 1px `#9aa2ad` stroke; blue `#2a78d6` 3px smooth curve for the quick component (bell peaking at 85s, height 57 in count units, spread 25s — plot 57 × exp(−(x−85)²/(2·25²)) at x = 40,50,...,320); magenta `#d55181` 3px bell for the slow component (peak 230s, height 32, spread 30s — plot 32 × exp(−(x−230)²/(2·30²)) at the same x steps); ink `#1a5276` dashed 2px curve (dash 4/3) for their sum; blue bold 12px label "60% quick, 85s" near the left peak; magenta bold 12px label "40% slow, 230s" near the right peak; caption 12px `#444` "gray bars = observed; curves = inferred".
- **Right panel (soft labels):** axis origin x=400, width 280, baseline y=245, chart height 175, x range 30–330s, y from 0 to 1 labeled "P(slow group)" (12px `#6b7280`, rotated or above axis); green `#008300` 3px S-curve rising from 0.01 at 70s through 0.50 at 155s to 0.99 at 240s (logistic shape, midpoint 155s, plot at x = 40,50,...,320); dashed `#bdc3c7` horizontal guide at 0.5; three 5px dots with bold 12px labels: blue dot at (70s, 0.01) "70s → 99% quick", orange `#d95926` dot at (155s, 0.50) "155s → 50/50", magenta dot at (240s, 0.99) "240s → 99% slow".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## One Hidden Cause, Many Visible Traces

**Tags:** `where it's used` (blue), `shared cause` (orange)

- **This shop** — customer type drives drink choice, prep time, and arrival hour all at once
- **Test scores** — "math ability" is latent; the 20 question marks are its observed traces
- **Recommenders** — a user's taste is latent; clicks and ratings are the symptoms you see
- **Topic models** — a document's topic is latent; the words on the page are what's observed
- **Hidden states** — a machine's wear level is latent; sensor readings point back at it

*Example (italic):* In the coffee shop's log, drip drinks, short prep times, and 7–9am arrivals all move together — three columns correlating because of one unrecorded cause.

**Key point:** When several observed columns correlate with no direct link between them, suspect one shared latent cause — that suspicion is the starting point of factor models.

### Visualization (canvas `c3`, 720×300)

Cause-and-traces diagram: one dashed latent node on the left with arrows to three solid observed boxes on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Latent Cause and Its Observed Traces".
- **Latent node:** dashed violet `#4a3aa7` circle (2px, dash 6/4), center (170, 155), radius 60, fill `rgba(74,58,167,0.08)`; inside, bold 13px violet text on two lines: "customer type" / "(latent — never recorded)".
- **Observed boxes:** three rounded rectangles at x=420, width 250, height 48, corner radius 6, tops at y=52, y=131, y=210; fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border; each holds a bold 13px `#1a5276` first line and 11px `#444` second line: "drink choice" / "drip vs latte"; "prep time" / "85s vs 230s typical"; "arrival hour" / "7–9am vs 10am+".
- **Arrows:** three 2.5px violet `#4a3aa7` lines from the circle's right edge (x=230, y=155) to the left edge of each box (x=420, box vertical centers y=76, y=155, y=234), each ending in a small filled triangle arrowhead.
- **Annotation (bold 12px violet, under the circle at y=240):** "one unmeasured cause".
- **Caption (12px `#444`, bottom center):** "the arrows are why the three observed columns correlate with each other".

## Latent Is Not Missing

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Missing** — a value that exists but wasn't recorded; the barista could recheck the receipt
- **Latent** — a value with no ground truth in the data; no receipt ever held "customer type"
- **Estimates, not facts** — fitted group labels are model output; never report them as counts
- **Wrong-count risk** — a two-group fit will happily split data that really has three groups
- **Validate sideways** — judge a latent model by what it predicts, not by labels you can't check

*Example (italic):* An analyst shipped "60% of customers are commuters" as if it were a survey result — it was a fitted mixture weight resting on the assumption of exactly two groups.

**Common mistake:** Treating inferred latent labels as observed data. A blank cell can be filled by looking harder; a latent variable cannot — every "value" of it is an estimate that inherits every assumption of the model that produced it.

### Visualization (canvas `c4`, 720×300)

Two mini order tables split by a vertical dashed divider at x=360: a missing cell that could be recovered (left) vs a latent column that never existed (right).

- **Title (bold 15px, `#1a5276`, top center):** "Missing (Recoverable) vs Latent (No Ground Truth)".
- **Shared table data (both panels):** four rows — `#101, latte, 212s`; `#102, drip, 78s`; `#103, drip, ?`; `#104, latte, 241s` — with the left panel's `?` only in the prep column of #103.
- **Left panel (missing):** heading bold 13px `#008300` "missing value" at (60, 55); table from x=50, columns "order" (55px), "drink" (90px), "prep" (70px); header row 12px bold `#1a5276` at y=80, four data rows 12px `#444` at y=105/130/155/180, row separators 1px `#e5e9ef`; the #103 prep cell drawn as a yellow `rgba(201,133,0,0.20)` box with bold `#c98500` "?" centered; green bold 12px annotation below the table at y=215: "the receipt exists — recheck it and fill the cell"; below that 11px `#444` "left panel prep column: 212s, 78s, ?, 241s".
- **Right panel (latent):** heading bold 13px `#d55181` "latent variable" at (400, 55); same four rows from x=390 with prep filled in (212s, 78s, 96s, 241s) plus a fourth column "type" (70px) drawn with a dashed 2px magenta `#d55181` border around the whole column; all four type cells show bold magenta "?"; magenta bold 12px annotation below the table at y=215: "no record has ever existed — you can only estimate it".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px `#d95926`, centered at y=285):** "missing = look harder; latent = model harder — and label the output as an estimate".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays — no `Math.random()`; bell and logistic curves are computed deterministically from the stated parameters at fixed x steps. In regenerated HTML, any card links would use `.html` extensions (this page has no links).
