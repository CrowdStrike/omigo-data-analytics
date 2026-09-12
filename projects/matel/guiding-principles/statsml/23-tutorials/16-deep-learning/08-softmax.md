# Softmax

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Softmax

**Subtitle:** Softmax turns a list of raw scores into a probability distribution — every output positive, all summing to 1, the favorite still the favorite

## A Pet Photo, Three Raw Scores

**Tags:** `core idea` (blue), `probability distribution` (green), `raw scores` (orange)

- **The app** — a photo app scores one pet photo: cat 2.0, dog 1.0, rabbit 0.1 — raw, unitless numbers
- **Not probabilities** — the scores sum to 3.1, not 1, and could even be negative; "2.0" means nothing alone
- **Softmax** — turns the three scores into 0.659, 0.242, 0.099 — all positive and summing to exactly 1
- **Order kept** — the highest score stays the favorite; softmax rescales the scores, never reorders them
- **Gaps matter** — cat beats dog by 1.0 score points, and that gap becomes a 2.7× probability ratio

*Example (italic):* The app can now tell the user "66% cat" instead of "cat scored 2.0", which no one can interpret.

**Key point:** Softmax converts raw model scores into a probability distribution you can read, compare, and act on — same ranking, but now the numbers mean something.

### Visualization (canvas `c1`, 720×300)

Dual-panel bar chart: the three raw scores (left) and their softmax probabilities (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Pet Photo: Raw Scores vs Softmax Probabilities".
- **Data:** classes `["cat", "dog", "rabbit"]`; raw scores `[2.0, 1.0, 0.1]`; softmax probabilities `[0.659, 0.242, 0.099]`.
- **Left panel (raw scores):** axis origin x=55, width 280, baseline y=245, chart height 185, y scale 0–2.2; three bars 60px wide, gap 34px, fill `rgba(42,120,214,0.45)`, 2px `#2a78d6` border; bold 12px `#2a78d6` value labels "2.0", "1.0", "0.1" above bars; class names 12px `#444` below baseline; magenta `#d55181` bold 12px annotation, two lines: "no units, no sum rule —" / "sums to 3.1"; caption 12px `#444` "raw scores (logits)".
- **Right panel (softmax):** axis origin x=400, width 280, same baseline/height, y scale 0–0.75; same bar geometry, fill `rgba(0,131,0,0.4)`, 2px `#008300` border; bold 12px `#008300` labels "65.9%", "24.2%", "9.9%" above bars; same class names below; green bold 13px annotation "sums to exactly 1.000"; caption "softmax probabilities".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Exponentiate, Add, Divide

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Step 1: exp** — raise e to each score: e^2.0 = 7.39, e^1.0 = 2.72, e^0.1 = 1.11
- **Step 2: add** — total the exponentials: 7.39 + 2.72 + 1.11 = 11.21
- **Step 3: divide** — each takes its share: 7.39/11.21 = 0.659, 2.72/11.21 = 0.242, 1.11/11.21 = 0.099
- **Why exp** — e^x is always positive, so even a negative score turns into a legal probability
- **Check** — 0.659 + 0.242 + 0.099 = 1.000, so the outputs form a genuine distribution

*Example (italic):* Rabbit's tiny 0.1 survives the trip: e^0.1 = 1.11, and 1.11/11.21 gives rabbit its 9.9% share.

**Key point:** softmax(xᵢ) = e^xᵢ / Σ e^xⱼ — three arithmetic steps you can redo by hand on any calculator.

### Visualization (canvas `c2`, 720×300)

Three-column flow diagram: score boxes on the left, exponential boxes in the middle (with a sum box below), probability boxes on the right, connected by labeled arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Three Steps: Score → e^x → Share of the Total".
- **Data (one row per class):** cat `2.0 → 7.39 → 0.659`; dog `1.0 → 2.72 → 0.242`; rabbit `0.1 → 1.11 → 0.099`; sum `11.21`.
- **Columns:** box centers at x=130, x=360, x=590; rows at y=85 (cat), y=150 (dog), y=215 (rabbit); boxes 120×40, 6px radius, 1.5px borders, 13px bold centered values with 11px `#6b7280` class name above each box.
- **Column styling:** score boxes border `#2a78d6`, fill `rgba(42,120,214,0.10)`; exp boxes border `#c98500`, fill `rgba(201,133,0,0.10)`; probability boxes border `#008300`, fill `rgba(0,131,0,0.10)`.
- **Column headings (bold 12px `#1a5276`, above each column at y=48):** "raw score x", "e^x", "e^x ÷ 11.21".
- **Arrows:** 2px `#6b7280` horizontal arrows between columns per row; label above the first arrow set "exponentiate" (bold 11px `#c98500`, once, at y=70 between columns 1–2) and "divide by sum" (bold 11px `#008300`, between columns 2–3).
- **Sum box:** 140×32 box centered at (360, 268), border `#d95926`, fill `rgba(217,89,38,0.10)`, bold 12px `#d95926` text "sum = 11.21"; thin dashed `#d95926` connectors from the three exp boxes down to it.

## The Last Layer of Every Classifier

**Tags:** `where it's used` (blue), `temperature` (orange), `shift invariance` (green)

- **Where** — softmax is the output layer of nearly every neural classifier, from digits to language models
- **Training** — cross-entropy loss needs probabilities, so softmax is what makes the loss computable
- **Shift-proof** — add 10 to every score: 12, 11, 10.1 still give the same 0.659, 0.242, 0.099
- **Sharpen** — multiply scores by 3: 6, 3, 0.3 give 0.950, 0.047, 0.003 — an almost-certain cat
- **Soften** — divide scores by 2: 1.0, 0.5, 0.05 give 0.502, 0.304, 0.194 — a much closer race

*Example (italic):* An LLM picks its next word by softmaxing thousands of scores; raising temperature divides them first, flattening the bets.

**Key point:** Only the gaps between scores matter, and stretching the gaps is the temperature dial — bigger gaps in, sharper distribution out.

### Visualization (canvas `c3`, 720×300)

Triple-panel bar chart: softmax of the same three scores after dividing by 2 (left), unchanged (middle), and multiplying by 3 (right), all on a shared 0–1 scale.

- **Title (bold 15px, `#1a5276`, top center):** "Same Ranking, Three Temperatures: Stretch the Gaps, Sharpen the Bet".
- **Data:** left scores `[1.0, 0.5, 0.05]` → probabilities `[0.502, 0.304, 0.194]`; middle scores `[2.0, 1.0, 0.1]` → `[0.659, 0.242, 0.099]`; right scores `[6.0, 3.0, 0.3]` → `[0.950, 0.047, 0.003]`.
- **Panels:** axis origins x=50, x=285, x=520, each width 185, baseline y=235, chart height 155, shared y scale 0–1 with a light `#e5e9ef` gridline at 0.5.
- **Panel headings (bold 12px `#1a5276`, centered above each panel at y=55):** "scores ÷ 2 (T = 2)", "scores as-is (T = 1)", "scores × 3 (T = ⅓)".
- **Bars:** three per panel, 42px wide, gap 22px; left panel fill `rgba(42,120,214,0.45)` border `#2a78d6`; middle fill `rgba(0,131,0,0.4)` border `#008300`; right fill `rgba(217,89,38,0.45)` border `#d95926`; bold 11px value labels ".50 .30 .19", ".66 .24 .10", ".95 .05 .00" above the bars in the panel's border color; class initials "c d r" 11px `#444` below baseline.
- **Annotations:** blue bold 12px "close race" over the left panel bars; orange `#d95926` bold 12px "almost certain" over the right panel bars.
- **Caption (12px `#444`, bottom center, y=288):** "cat stays the favorite in all three — temperature changes confidence, never the ranking".

## Why Not Just Divide by the Sum?

**Tags:** `common mistake` (red), `worked example` (blue)

- **The shortcut** — dividing raw scores by their sum looks fine here: 2.0/3.1 = 0.645, close to 0.659
- **It breaks** — score the photo cat 2.0, dog 1.0, rabbit −1.0: the sum is 2.0 and rabbit gets −0.5
- **Softmax copes** — e^−1.0 = 0.37, so the same scores give 0.705, 0.260, 0.035 — all legal
- **Not calibrated** — "0.705" is the model's bet, not a measured accuracy; overconfidence is common
- **Not max** — softmax is a soft argmax: it favors the winner but keeps every option alive

*Example (italic):* A naive normalizer fed the scores 2.0, 1.0, −1.0 reports "rabbit: −50%", which is not a probability at all.

**Common mistake:** Normalizing scores by their plain sum. It produces negative "probabilities" on negative scores and blows up when the sum is near zero — exponentiating first is exactly what makes softmax always valid.

### Visualization (canvas `c4`, 720×300)

Dual-panel bar chart on the scores 2.0, 1.0, −1.0: naive divide-by-sum with a bar going below zero (left) vs softmax (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Scores 2.0, 1.0, −1.0: Naive Divide-by-Sum vs Softmax".
- **Data:** naive outputs `[1.0, 0.5, -0.5]`; softmax outputs `[0.705, 0.260, 0.035]` (from e^2.0 = 7.39, e^1.0 = 2.72, e^−1.0 = 0.37, sum 10.48).
- **Left panel (naive):** axis origin x=55, width 280; zero line at y=185 (2px `#999`), y scale −0.6 to 1.1 over chart top y=60 to bottom y=250; three bars 60px wide, gap 34px; cat and dog bars up from the zero line, fill `rgba(42,120,214,0.45)` border `#2a78d6`; rabbit bar drawn downward, fill `rgba(231,76,60,0.35)`, 2px `#e74c3c` border; bold 12px value labels "1.0", "0.5" above and red "−0.5" below its bar; class names 12px `#444` along the zero line; red `#e74c3c` bold 12px annotation "a negative probability!" with a short arrow to the rabbit bar; caption 12px `#444` "score ÷ sum of scores".
- **Right panel (softmax):** axis origin x=400, width 280, baseline y=250, chart height 190, y scale 0–0.8; same bar geometry, fill `rgba(0,131,0,0.4)`, 2px `#008300` border; bold 12px `#008300` labels "70.5%", "26.0%", "3.5%" above bars; class names below baseline; green bold 13px annotation "exp first: always positive, sums to 1"; caption "e^score ÷ sum of e^scores".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays (no `Math.random()`); every number in the charts matches the text bullets exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
