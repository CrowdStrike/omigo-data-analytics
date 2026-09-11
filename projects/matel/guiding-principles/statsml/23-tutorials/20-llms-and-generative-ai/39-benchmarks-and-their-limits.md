# Benchmarks & Their Limits

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Benchmarks & Their Limits

**Subtitle:** A benchmark is an exam; your workload is the job — a model can ace one and fumble the other

## 92% on the Exam, 60% on Your Tickets

**Tags:** `core idea` (blue), `running example` (green)

- **The pick** — a team chooses Model A because it scores 92% on a public benchmark
- **The reality** — on 50 of their own support tickets, A gets only 30 right: 60%
- **The runner-up** — Model B scored a "worse" 85% on the exam, but 37/50 = 74% on the job
- **Ranking flip** — the benchmark says A > B; your own data says B > A
- **Why** — the benchmark tests trivia and puzzles, not refund policies and order jargon

*Example (italic):* The leaderboard never saw a ticket that says "chargeback on the BOGO promo, split shipment".

**Key point:** A benchmark score tells you how a model did on someone else's questions. It is a hint, not a verdict, about yours.

### Visualization (canvas `c1`, 720×300)

Grouped vertical bar chart: two groups (benchmark vs your tickets), two series (Model A, Model B), showing the ranking flip.

- **Title (bold 15px, `#1a5276`, top center):** "Public Benchmark vs Your 50 Tickets: the Ranking Flips".
- **Data:** group "public benchmark" — A: 92, B: 85; group "your 50 tickets" — A: 60, B: 74.
- **Axes:** y from 0 to 100% with tick labels at 0%, 50%, 100% and light gridlines `#e5e9ef`; L-shaped axis in `#999`; padding top 56, bottom 48, left 60, right 160.
- **Bars:** 90px wide, 24px in-group gap; Model A bars `#2a78d6` (blue), Model B bars `#d95926` (orange); bold 13px value labels ("92%", "85%", "60%", "74%") above each bar in `#2c3e50`; group labels 12px below the baseline.
- **Legend (right side, x = w-145):** blue swatch "Model A", orange swatch "Model B".
- **Annotation (bold 13px red `#e74c3c`, below legend, two lines):** "exam winner," / "job loser".
- **Caption (12px mute `#6b7280`, bottom-right):** "illustrative data".

## Where the 50 Tickets Came Apart

**Tags:** `worked example` (green), `core idea` (blue)

- **The test set** — 50 real tickets: 20 billing, 18 shipping, 12 refunds
- **Model A** — billing 16/20 (80%), shipping 10/18 (56%), refunds 4/12 (33%)
- **Model B** — billing 15/20 (75%), shipping 13/18 (72%), refunds 9/12 (75%)
- **Totals check** — A: 16+10+4 = 30/50 = 60%; B: 15+13+9 = 37/50 = 74%
- **The tell** — A collapses on refunds, the ticket type the public exam never covered

*Example (italic):* Fifty graded tickets took one afternoon — and reversed a decision the leaderboard had "settled".

**Key point:** Break your test set down by case type. An overall score can hide a category where the model fails 2 times out of 3.

### Visualization (canvas `c2`, 720×300)

Grouped vertical bar chart: three ticket-type groups, two series (Model A, Model B), with count labels on each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Accuracy by Ticket Type (counts on each bar)".
- **Data:** "billing (20)" — A: 80% ("16/20"), B: 75% ("15/20"); "shipping (18)" — A: 56% ("10/18"), B: 72% ("13/18"); "refunds (12)" — A: 33% ("4/12"), B: 75% ("9/12").
- **Axes:** y 0–100% with ticks 0%, 50%, 100%, gridlines `#e5e9ef`; padding top 56, bottom 48, left 60, right 160.
- **Bars:** 62px wide, 14px in-group gap; Model A bars `#2a78d6`, except the refunds group where A's bar is red `#e74c3c`; Model B bars `#d95926`; bold 12px count labels ("16/20" etc.) above bars; group labels 12px below baseline.
- **Legend (right side, x = w-145):** blue swatch "Model A", orange swatch "Model B".
- **Annotation (bold 12px red `#e74c3c`, below legend, two lines):** "A fails 2 of 3 refunds —" / "the exam had none".
- **Caption (12px mute `#6b7280`, bottom-right):** "illustrative data".

## Teaching to the Test and Contamination

**Tags:** `why scores inflate` (orange), `core idea` (blue)

- **Public = leaked** — benchmark questions sit on the open web, where training data comes from
- **Contamination** — a model that saw the answers during training is reciting, not reasoning
- **Teaching to the test** — vendors tune on popular benchmarks because buyers quote them
- **The probe** — reword the same 50 exam questions; memorization can't follow the rewording
- **Our example** — Model A: 92% → 78% on reworded questions; Model B: 85% → 82%

*Example (italic):* A student who memorized last year's answer key drops hardest when the wording changes.

**Key point:** A big drop on reworded questions is the fingerprint of memorization. The stable score, not the higher score, is the trustworthy one.

### Visualization (canvas `c3`, 720×300)

Grouped vertical bar chart: two model groups, two series (original vs reworded questions), with point-drop labels above each group.

- **Title (bold 15px, `#1a5276`, top center):** "Same Questions, New Wording: Who Really Learned?".
- **Data:** "Model A" — original 92, reworded 78; "Model B" — original 85, reworded 82.
- **Axes:** y 0–100% with ticks 0%, 50%, 100%, gridlines `#e5e9ef`; padding top 56, bottom 48, left 60, right 175.
- **Bars:** 90px wide, 24px in-group gap; original-questions bars violet `#4a3aa7`, reworded bars aqua `#199e70`; bold 13px value labels ("92%", "78%", "85%", "82%") above bars; group labels 12px below baseline.
- **Drop labels (bold 13px, centered above each group at pad.top+14):** "-14 pts" in red `#e74c3c` over Model A; "-3 pts" in green `#008300` over Model B.
- **Legend (right side, x = w-160):** violet swatch "original questions", aqua swatch "reworded (illustrative)".
- **Annotation (bold 12px red `#e74c3c`, below legend, two lines):** "a big drop means" / "memorized answers".

## The Confusion: Higher Benchmark Means Better for Us

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The mistake** — reading a leaderboard as a shopping ranking for your task
- **Five models** — exam scores 92, 88, 85, 80, 76; your-ticket scores 60, 66, 74, 70, 58
- **Weak link** — the exam winner is your worst-but-one; your winner sat 3rd on the exam
- **Use benchmarks to shortlist** — screen out clearly weak models, nothing more
- **Decide on your data** — a private 50-example graded set beats any public number

*Example (italic):* The team now keeps their 50 tickets secret from prompts and fine-tuning — a private exam stays honest.

**Rule of thumb:** Shortlist by benchmark, choose by your own test set — and never publish that test set, or it becomes the next contaminated benchmark.

### Visualization (canvas `c4`, 720×300)

Scatter plot: five models, benchmark score (x) vs your-task score (y), showing weak correlation.

- **Title (bold 15px, `#1a5276`, top center):** "Five Models: Exam Score Barely Predicts Your-Task Score".
- **Data (name, benchmark, your-tickets, dot color):** A (92, 60, blue `#2a78d6`); C (88, 66, mute `#6b7280`); B (85, 74, orange `#d95926`); D (80, 70, mute `#6b7280`); E (76, 58, mute `#6b7280`).
- **Axes:** x = "public benchmark score" from 70 to 95 with tick labels 70%, 80%, 90%; y = "score on your 50 tickets" from 50 to 80 with tick labels 50%, 60%, 70%, 80% and gridlines `#e5e9ef`; axis labels 12px mute `#6b7280`, y-axis title rotated vertical; padding top 56, bottom 52, left 70, right 40.
- **Points:** 8px-radius filled circles, bold 12px model letter above each point in `#2c3e50`.
- **Annotations (bold 13px):** in orange near model B: "your best (B) was 3rd on the exam"; in blue near model A: "exam best (A) is nearly your worst".
- **Caption (12px mute `#6b7280`, bottom-right):** "illustrative data".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of pill spans, then `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic width/height attributes as given (all 720×300); shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
