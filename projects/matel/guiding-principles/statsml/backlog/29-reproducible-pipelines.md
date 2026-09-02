# Reproducible Data Pipelines & Deterministic Output

**Page type:** detail page (backlog-style 2-column layout: `.card-section` per topic, each with a text column ~45% left and a canvas column ~55% right)
**HTML title tag:** Reproducible Data Pipelines & Deterministic Output

**Subtitle:** A pipeline should produce identical output given identical input — regardless of when it runs, how many times it retries, or which environment executes it. This is the design philosophy that contains query determinism but reaches further: ingestion through model output, plus the audit trail that lets you replay any past decision.

## 1. What Breaks Reproducibility

Non-determinism enters from two directions — the world outside the process, and the way the process itself executes.

**Temporal & environmental**

- **Wall-clock dependencies** — `NOW()`, `CURRENT_DATE`, time-based partitions that shift under you
- **Floating-point drift** — x86 vs ARM, different GPU generations, different rounding
- **Hash randomization** — `PYTHONHASHSEED` changing set/dict iteration order
- **External API calls** — exchange rates, model endpoints, third-party services that move

**Execution & concurrency**

- **Parallel execution order** — reducer order, Spark shuffles, thread scheduling
- **Unfixed random seeds** — sampling, train/test splits, weight initialization
- **Training non-determinism** — GPU scheduling, non-deterministic cuDNN kernels, gradient accumulation order
- **Dependency drift** — unpinned libraries, OS differences, transitive upgrades

Key-point callout (red accent): **The insidious ones:** floating-point drift and parallel ordering produce results that *look* correct — just slightly different each run. Eyeball checks never catch them; only bit-for-bit output comparison does.

### Visualization (canvas `c1`, 720×300)

Stacked layer diagram of the reproducibility stack, pinned vs floating layers.

- **Title (bold 14px `#1a5276`, top center):** "The Reproducibility Stack: Pinned vs Floating Layers".
- **Layers:** five horizontal boxes (x=150, width = canvas width − 150 − 96, height 30px, row pitch 42px starting y=46). Layer name in 11px `#2c3e50` right-aligned left of the box; description in 10px `#555` inside the box; status word in bold 10px right of the box. Pinned layers: fill `rgba(39,174,96,0.10)`, solid stroke `#27ae60`, label "PINNED". Floating layers: fill `rgba(231,76,60,0.10)`, dashed stroke (4/3) `#e74c3c`, label "FLOATS". Light `#eee` separator line between rows.
  1. "Code version" — "git SHA recorded in run manifest" — PINNED.
  2. "Data snapshot" — "as-of timestamp, immutable partition" — PINNED.
  3. "Config / params" — "hashed and stored with the output" — PINNED.
  4. "Environment" — "dependency range resolves at build time" — FLOATS.
  5. "Random seed" — "unset — split and init vary per run" — FLOATS.
- **Bracket:** orange `#e67e22` 2px square bracket on the far left spanning the two floating rows (rows 4–5).
- **Caption (11px gray `#888`, bottom center):** "Every layer must be pinned — one floating layer is enough to change the output".

## 2. Design Principles

**Referential transparency** — the contract is that `f(x)` returning `y` today means `f(x)` returns `y` tomorrow.

- **Version everything** — code, data, config, dependencies, environment. If any of these change, it is a different pipeline.
- **Snapshots over live queries** — read a frozen snapshot, never a table that can change mid-execution.
- **Explicit side effects** — every write, external call, and non-deterministic operation is declared and isolated.

**Content-addressed storage** — the hash of the input determines where the output lives.

- **Memoization** — unchanged input hash means the stage is skipped, not recomputed.
- **Deduplication** — the same bytes written twice land in the same place.
- **Audit trail** — any output traces back to the exact inputs and code that produced it.

Key-point callout (red accent): **Where staleness enters:** a cache keyed on a hash that omits one input — a config flag, a library version — will happily serve a stale artifact that looks fresh. The key must cover everything the stage reads.

### Visualization (canvas `c2`, 720×300)

Five-node horizontal DAG showing cache hits vs recomputes in a content-addressed pipeline.

- **Title (bold 14px `#1a5276`, top center):** "Content Addressing: Cache Hit vs Recompute".
- **Nodes:** five boxes in a row (equal cells across the width starting x=22, box width = cell − 22, y=104, height 62px), connected by gray `#999` arrows. Each box shows the stage name (bold 12px `#1a5276`), state text (10px in state color), and "key <hash>" (9px `#888`):
  1. "Ingest" — "cache hit" — key a91f — green.
  2. "Clean" — "cache hit" — key 4c07 — green.
  3. "Features" — "recompute" — key bd52 — orange.
  4. "Train" — "recompute" — key 7e10 — orange.
  5. "Evaluate" — "recompute" — key 0fa8 — orange.
- **Node styling:** cache hits — fill `rgba(39,174,96,0.10)`, stroke `#27ae60`; recomputes — fill `rgba(230,126,34,0.10)`, stroke `#e67e22` (1.5px strokes).
- **Change marker:** dashed orange `#e67e22` (4/3) vertical line dropping to the "Features" node with 10px orange centered label above (y=50): "config flag changed → new input hash".
- **Artifact markers:** small 32×12 rectangle under each node (y = box bottom + 16), fill `rgba(26,82,118,0.35)` for hits labeled "reused", `rgba(230,126,34,0.35)` for recomputes labeled "written" (9px `#555`); left-aligned 10px `#2c3e50` label lower down: "materialized artifact store".
- **Caption (11px gray `#888`, bottom center):** "Staleness enters when the key omits an input the stage actually reads".

## 3. Practical Techniques

**Environment & dependencies**

- Pinned versions — `pip freeze`, lockfiles, no floating ranges
- Containerized execution with pinned base images
- Fixed seeds at every non-deterministic call site
- Deterministic serialization — sorted JSON keys, stable Parquet row order
- Explicitly sorted outputs — never rely on implicit database or hash-map ordering

**Data versioning**

- **DVC** — git-style tracking for large files
- **LakeFS** — git-like branching over a data lake
- **Delta Lake time travel** — query any historical version of a table
- **Immutable partitions** — write-once, append-only, never overwrite
- **Input manifests** — exact paths, sizes, and checksums consumed by each run

Key-point callout (red accent): **Point-in-time correctness:** a feature computed against a mutable table answers "what is true now", not "what was true at label time". The same query, run twice, silently trains on future information.

Example line (italic): Example: `lifetime_orders` read live is larger than the value that existed when the churn label was set — the model learns from orders that had not happened yet.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart comparing as-of snapshot values vs live-table values per user.

- **Title (bold 14px `#1a5276`, top center):** "Point-in-Time Correctness: As-Of Snapshot vs Live Table".
- **Data (`lifetime_orders`, as-of vs live):** user A 6 vs 9; user B 12 vs 14; user C 3 vs 11; user D 8 vs 8; user E 5 vs 13.
- **Axes:** plot area x=70, y=62, width = canvas − 130, height 172; gray `#ccc` L-axes; horizontal `#f0f0f0` gridlines at quarters; y ticks 0/4/8/12/16 in 9px `#999` (max 16); rotated 11px `#666` y-axis label "lifetime_orders"; user labels in 10px `#2c3e50` under each group.
- **Bars:** paired bars per user (bar width 26% of group). As-of bar fill `rgba(26,82,118,0.35)` with value label in `#1a5276` above; live bar fill `rgba(231,76,60,0.45)` with value label in `#e74c3c` above.
- **Legend (top right):** blue swatch "as-of label date (correct)"; red swatch "live table today (leaked)".
- **Caption (11px gray `#888`, bottom center):** "The gap is future information — same query, different answer, silent leakage".

## 4. The Determinism Spectrum

Not every stage *can* be deterministic. Classify each one honestly instead of claiming a guarantee you cannot hold.

- **Fully deterministic** — SQL transforms, feature engineering, aggregations, cleaning: pure functions over snapshots.
- **Deterministic with effort** — model training and sampling: fix all seeds, force deterministic kernels, accept the performance cost.
- **Inherently non-deterministic** — live API calls, user interaction, real-time sensor feeds.

**Cost on both sides**

- **Of reproducibility** — storage for snapshots, added complexity, slower iteration.
- **Of non-reproducibility** — cannot debug production, cannot audit a decision, cannot compare experiments, cannot answer a regulator.
- **Pragmatic split** — full reproducibility for training and evaluation; best-effort for exploratory work.

Key-point callout (red accent): **Strategy:** isolate the non-deterministic stages, checkpoint immediately around them, and accept that the checkpoint — not the stage — becomes the reproducible artifact.

### Visualization (canvas `c4`, 720×300)

Three-column zone diagram of the determinism spectrum.

- **Title (bold 14px `#1a5276`, top center):** "Determinism Spectrum: What Each Stage Can Promise".
- **Zones:** three equal-width columns (16px padding, from y=44 to ~42px above the bottom), each a tinted box with 2px colored border, bold 11px centered header in the zone color, 11px `#2c3e50` bullet items, and a 10px `#555` centered cost line at the bottom:
  1. "FULLY DETERMINISTIC" — green `#27ae60`, fill `rgba(39,174,96,0.08)` — items: SQL transforms, Feature engineering, Aggregations, Data cleaning — "cost to enforce: low".
  2. "DETERMINISTIC W/ EFFORT" — orange `#e67e22`, fill `rgba(230,126,34,0.08)` — items: Model training, Sampling & splits, Embedding generation, Hyperparam search — "cost to enforce: high".
  3. "INHERENTLY NOT" — red `#e74c3c`, fill `rgba(231,76,60,0.08)` — items: Live API calls, User interaction, Real-time sensors, Third-party scores — "checkpoint instead".
- **Top line:** thin gray `#999` horizontal line across the top of the zones.
- **Caption (11px gray `#888`, bottom center):** "Isolate the right-hand stages and checkpoint them — the checkpoint is the reproducible artifact".

## 5. Relationship to Testing

Testing is downstream of reproducibility: if output moves between runs, there is nothing stable to assert on.

- **Unit tests** assert exact output for known input — requires determinism.
- **Integration tests** verify end-to-end behavior — requires the whole chain to be reproducible.
- **Regression tests** catch unintended change — requires that "unchanged" genuinely means "same output".

Key-point callout (red accent): **Critical insight:** a non-reproducible pipeline is an untestable pipeline. If a test passes most of the time, the pipeline is non-deterministic — the test is not flaky.

Example line (italic): Example: an unseeded split spreads evaluation AUC across a band; some runs land under the assertion threshold. Fixing the seed collapses the band to one value, and the assertion becomes meaningful.

### Visualization (canvas `c5`, 720×300)

Histogram of eval AUC across unseeded runs vs a single seeded value.

- **Title (bold 14px `#1a5276`, top center):** "Unseeded Runs vs One Seeded Run: Eval AUC".
- **Histogram bins (bin width 0.004, lower edge → count):** 0.806 → 2, 0.810 → 5, 0.814 → 9, 0.818 → 12, 0.822 → 8, 0.826 → 4, 0.830 → 2. Count scale max 13. X range 0.804–0.836.
- **Axes:** plot area x=58, y=58, width = canvas − 108, height 176; gray `#ccc` L-axes; `#f0f0f0` quarter gridlines; x tick labels 0.806, 0.812, 0.818, 0.824, 0.830, 0.836 in 9px `#999`; 11px `#666` axis labels "eval AUC" (below, centered) and rotated "runs" (left).
- **Bar colors:** bins entirely at or below the threshold 0.815 (i.e. bin upper edge ≤ 0.815) fill `rgba(231,76,60,0.45)` stroke `#e74c3c`; the rest fill `rgba(26,82,118,0.35)` stroke `#1a5276`.
- **Assertion threshold:** vertical dashed red `#e74c3c` (dash 5/4, width 2) line at AUC 0.815, labeled in 10px red to the right: "assert AUC > 0.815" / "runs left of the line fail".
- **Seeded value:** solid green `#27ae60` vertical line (width 2.5) at AUC 0.8207, labeled above in bold 10px green: "seeded: 0.8207 every run".
- **Caption (11px gray `#888`, bottom center):** "A spread this wide makes any exact assertion meaningless — the pipeline is the flake".

## 6. Streaming Systems

Streaming input is unbounded, so "same input" has to be defined before determinism can even be discussed.

- **Windowing** creates bounded sub-problems — but window boundaries can land differently across runs.
- **Watermarks are heuristic** — how long you wait decides whether late data is counted.
- **Event-time processing** helps, but does not remove non-deterministic arrival order.

Questions callout (orange accent): **Open question:** the lambda-architecture tension — streaming for freshness, batch replay for correctness. Which number is the official one when the replay disagrees with the live stream?

Example line (italic): Example: an event arriving after the watermark closes is included in a later replay but was missing from the live run — the same window reports two different totals, both defensible.

### Visualization (canvas `c6`, 720×300)

Two stacked event timelines (live run vs batch replay) showing a late event changing the window total.

- **Title (bold 14px `#1a5276`, top center):** "Same Window, Two Runs: Late Event Changes the Total".
- **Layout:** timeline axis x=74, width = canvas − 180; two runs drawn at y=96 ("Live run") and y=196 ("Batch replay"), labels in 11px `#2c3e50` right-aligned left of each axis. Bottom axis label (10px `#999`, centered at y=248): "event time →".
- **Window band:** light blue `rgba(26,82,118,0.08)` band from fraction 0 to 0.78 of the axis (44px tall, centered on each timeline).
- **In-window events:** six 8×24 bars at axis fractions `[0.08, 0.20, 0.34, 0.47, 0.60, 0.71]`, fill `rgba(26,82,118,0.35)`, stroke `#1a5276`.
- **Late event at fraction 0.88:** on the live run — fill `rgba(231,76,60,0.20)` with dashed (3/2) red `#e74c3c` outline (excluded); on the batch replay — fill `rgba(39,174,96,0.45)` with solid green `#27ae60` outline (included).
- **Watermark:** vertical dashed orange `#e67e22` (dash 5/4, width 2) line at fraction 0.78 on both timelines, labeled "watermark closes window" in 10px orange (y=58); "late event" labeled in 10px `#888` above the late-event position.
- **Results (bold 12px, right of each timeline):** live run "sum = 41" in red `#e74c3c`; batch replay "sum = 47" in green `#27ae60`.
- **Caption (11px gray `#888`, bottom center):** "Both totals are defensible — the pipeline must declare which one is official".

## 7. Audit Replay & Open Decisions

The practical test of reproducibility is an audit question: can you reconstruct the exact input state as it was at the moment a decision was made?

- Where on the spectrum from "fully deterministic" to "best effort" should each stage sit?
- Should reproducibility be a hard gate — pipeline fails when non-determinism is detected — or a soft metric that is tracked and reviewed?
- Does the manifest retention window cover the audit horizon, or does it expire first?

Questions callout (orange accent): **The trap:** snapshots and manifests are usually retained on a storage-cost schedule, while audits are requested on a legal schedule. When the second is longer than the first, the decision is unreplayable no matter how deterministic the code was.

Example line (italic): Example: a decision from six months ago is technically deterministic, but its input snapshot expired at ninety days — the code replays, the data does not.

### Visualization (canvas `c7`, 720×300)

Timeline diagram of retention bands vs audit horizon over the last 180 days.

- **Title (bold 14px `#1a5276`, top center):** "Audit Replay: Reconstructing State at Decision Time".
- **Axis:** days −180 to 0 mapped from x=60 to x=canvas−60, gray `#ccc` axis at y=190 with ticks and 9px `#999` labels at −180d, −135d, −90d, −45d, "today".
- **Retention bands (y=100, height 44):**
  - −180d to −90d: fill `rgba(231,76,60,0.12)`, dashed (4/3) red `#e74c3c` outline; bold 11px red label "snapshots expired" with 10px `#555` subtext "code replays, inputs are gone".
  - −90d to 0d: fill `rgba(39,174,96,0.12)`, solid green `#27ae60` outline; bold 11px green label "replayable window" with 10px `#555` subtext "manifest + snapshot + seed retained".
- **Manifest boundary:** vertical dashed orange `#e67e22` (dash 5/4, width 2) line at −90d from y=96 to the axis, labeled above in 10px orange: "manifest retention: 90d".
- **Decision marker:** blue `#1a5276` filled dot (radius 5) on the axis at −150d with a 2px stem below and bold 10px blue label "decision under audit".
- **Audit horizon:** thin gray `#999` horizontal line at y=64 spanning −180d to 0d, labeled above in 10px `#888`: "audit horizon: 180d".
- **Caption (11px gray `#888`, bottom center):** "Deterministic code is not enough when the inputs expire before the audit arrives".

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Page: h1, `.subtitle` paragraph, then one `.card-section` per numbered topic. Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one `<tr>`: left `td.text-col` (45%) with paragraphs, `<ul>` bullets, `.key-point`/`.questions` callouts and `.example` lines; right `td.viz-col` (55%) with the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `strong` in `#1a5276`; lists 0.92rem.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.questions` — same but left border `3px solid #e67e22`. `.example` — italic, `#555`, 0.9rem.
- **Inline code:** background `#f8f9fa`, border `1px solid #e0e0e0`, padding 1px 5px, radius 3px, 0.85em, color `#1a5276`. (This page also defines a `table.compare` style — 0.9rem, `th` background `#f8f9fa` color `#1a5276`, all cells `1px solid #e0e0e0` — though no compare table appears in the body.)
- **Canvas:** intrinsic 720×300 per chart, CSS `width: 100%`, border `1px solid #e0e0e0` radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, grays `#666`/`#888`/`#999`/`#ccc`.
- Detail pages have no nav bar and no back/home links; any card links in regenerated HTML grids use `.html` extensions.
