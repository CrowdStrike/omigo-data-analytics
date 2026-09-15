# Beam

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Beam

**Subtitle:** Apache Beam is one programming model for batch and streaming — write the pipeline once, then run the same code on Dataflow, Flink, or Spark

## One Word-Count, Two Kinds of Input

**Tags:** `core idea` (blue), `unified model` (green), `Apache Beam` (orange)

- **The pipeline** — read text, split into words, count each word, write the totals: four steps
- **Bounded run** — pointed at yesterday's 10,000-line server log, it finishes and prints final counts
- **Unbounded run** — pointed at the live message feed, the same four steps just keep running
- **The unification** — Beam's claim: batch is just a bounded stream, so one model covers both
- **The names** — data flowing between steps is a PCollection; each step is a PTransform

*Example (italic):* The same word-count code counts "error" 480 times in yesterday's log and keeps counting on the live feed — nothing in the code says batch or streaming (counts illustrative).

**Key point:** Beam gives one programming model for batch and streaming — the pipeline says what to compute; whether the input ever ends is a property of the source, not of the code.

### Visualization (canvas `c1`, 720×300)

Flow diagram: two source boxes (bounded file, unbounded stream) feeding one shared four-step pipeline chain.

- **Title (bold 15px, `#1a5276`, top center, y=28):** "One Pipeline, Two Sources: Bounded File and Unbounded Stream".
- **Source boxes (160×40, 8px radius, 12px `#2c3e50` text):** blue `rgba(42,120,214,0.15)` box at (30, 75) labeled "log file — bounded"; green `rgba(0,131,0,0.12)` box at (30, 185) labeled "live feed — unbounded".
- **Converging arrows:** 3px `#6b7280` lines with arrowheads from (190, 95) and (190, 205) meeting at (240, 150).
- **Pipeline chain (four boxes 95×44 at y=128, fills `rgba(26,82,118,0.10)`, 1.5px `#1a5276` borders, 12px labels):** "Read" at x=250, "Split words" at x=360, "Count" at x=470, "Write" at x=580; 3px `#1a5276` arrows between consecutive boxes.
- **Band label (12px `#6b7280`, centered above the chain at y=115):** "the same four PTransforms".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=265):** "the source decides bounded or unbounded — the pipeline code never changes".

## Windows Turn the Stream Into Countable Chunks

**Tags:** `worked example` (blue), `windows & triggers` (green)

- **The problem** — an unbounded count can never say "done", so Beam counts per window instead
- **Fixed windows** — the pipeline groups messages into 1-minute windows by event time
- **The counts** — "error" hits per window from 2:00pm: 7, 12, 9, 5 — four results instead of one
- **Hand-check** — 7 + 12 + 9 + 5 = 33, matching a batch run over the same four minutes of log
- **Triggers** — a trigger decides when each window emits: at the watermark, then again on late data

*Example (italic):* The 2:01pm window closes with 12 "error" hits; a message that arrives late can still revise that count if the trigger allows a late firing.

**Key point:** Windows chop an unbounded stream into finite pieces the same Count transform can finish — batch is the special case of one global window over a bounded source.

### Visualization (canvas `c2`, 720×300)

Bar chart: "error" count per 1-minute streaming window, plus one batch-total bar over the same span, showing the sums agree.

- **Title (bold 15px, `#1a5276`, top center):** "Counting a Stream: 'error' Hits per 1-Minute Window".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = count 0 to 35, gridlines `#e5e9ef` at 10/20/30 with 12px `#444` labels.
- **Window bars:** four blue `rgba(42,120,214,0.55)` bars, 70px wide, centered at x = 120, 215, 310, 405; heights for values `[7, 12, 9, 5]`; 12px `#444` x-labels "2:00", "2:01", "2:02", "2:03"; bold 13px `#2a78d6` value labels atop each bar.
- **Batch bar:** one green `rgba(0,131,0,0.45)` bar, 70px wide, centered at x=560, value `33`, x-label "batch 2:00–2:04", bold 13px `#008300` value label "33"; vertical dashed `#6b7280` (dash 4/3) separator at x=485.
- **Annotation (bold 13px violet `#4a3aa7`, near x=200, y=70):** "7 + 12 + 9 + 5 = 33 — windowed stream matches the batch total".
- **Caption (12px `#444`, bottom right):** "counts illustrative; the sum is exact".

## Change the Runner, Not the Code

**Tags:** `where it's used` (blue), `portability` (green)

- **The runner** — a Beam pipeline does not execute itself; a runner translates it for an engine
- **The menu** — the same code runs on Google Cloud Dataflow, Apache Flink, or Apache Spark
- **The switch** — moving engines means changing the `--runner` flag, not rewriting the job
- **The origin** — Beam grew out of Google's Dataflow model paper (2015) and joined Apache in 2016
- **Adoption reality** — Beam is strongest inside the Google Cloud orbit, with Dataflow as the runner

*Example (italic):* A team migrating a hand-written Spark job to Flink rewrites ~1,800 lines; the Beam team changes one flag and resubmits (line counts illustrative).

**Key point:** Portability is the payoff of the abstraction — the pipeline is a description of work, so the execution engine becomes a swappable parameter instead of a rewrite.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: lines of code changed to move the same word-count job from Spark to Flink, hand-written vs Beam.

- **Title (bold 15px, `#1a5276`, top center):** "Moving the Job From Spark to Flink: Lines Changed".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; widths schematic, not a linear scale.
- **Row 1 (y=110), left-aligned 12px `#444` label at x=20:** "hand-written Spark job — ~1,800 lines rewritten"; red `#e74c3c` bar fill `rgba(231,76,60,0.45)`, 22px tall, width 420, 11px `#e74c3c` label "≈1,800" at the bar end.
- **Row 2 (y=190), label:** "Beam pipeline — 1 line (--runner=FlinkRunner)"; green `#008300` solid bar, 22px tall, width 4, bold 12px `#008300` label "1 line" beside it.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "the pipeline is data; the engine is a parameter".
- **Caption (12px `#444`, bottom right):** "line counts illustrative; bar widths schematic".

## Beam Is a Model, Not an Engine

**Tags:** `common mistake` (red), `capability matrix` (orange)

- **The confusion** — Beam looks like an engine, but it ships no cluster; it builds a pipeline graph
- **Who runs it** — the runner hands that graph to Dataflow, Flink, or Spark; the engine does the work
- **Not identical** — runners differ in what they support; Beam publishes a capability matrix
- **The local trap** — the DirectRunner exists for testing; it is slow and not a production engine
- **The mistake** — benchmarking on DirectRunner, or assuming every trigger works on every runner

*Example (italic):* A pipeline using an exotic trigger runs fine on Dataflow but is rejected by another runner — same code, different row of the capability matrix.

**Common mistake:** Reading "write once, run anywhere" as "runs identically everywhere". Beam standardizes the model; the capability matrix tells you what each runner actually implements.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the assumption (Beam executes the pipeline — wrong) vs the reality (a runner translates the graph and an engine executes it).

- **Title (bold 15px, `#1a5276`, top center):** "Who Actually Executes the Pipeline".
- **Row 1 (y=95), left-aligned 12px `#444` label at x=20:** "the assumption"; blue `rgba(42,120,214,0.15)` rounded box at x=150 labeled "Beam pipeline" (12px), 3px arrow to a red `rgba(231,76,60,0.12)` box at x=400 labeled "Beam executes it" with bold 12px red `#e74c3c` "✗ Beam ships no engine" beneath.
- **Row 2 (y=205), label:** "the reality"; blue box at x=150 "Beam pipeline", 3px arrow to a green `rgba(0,131,0,0.12)` box at x=340 labeled "runner translates graph", then arrow to a green box at x=545 labeled "engine executes — Dataflow / Flink / Spark" with bold 12px green `#008300` "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, 12px `#2c3e50` text, 1.5px borders matching each fill's hue.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "check the capability matrix before assuming a feature travels with the code".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the per-window counts `[7, 12, 9, 5]`, the batch total 33, the 480 log hits, and the ~1,800 rewritten lines are invented and labeled illustrative; the sum 7+12+9+5=33 is exact; the Dataflow paper (2015), the Apache donation (2016), the runner names, and the capability matrix are publicly documented Beam facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
