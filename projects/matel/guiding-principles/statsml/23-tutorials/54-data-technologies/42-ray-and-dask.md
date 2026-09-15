# Ray & Dask

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Ray & Dask

**Subtitle:** When one machine runs out of memory or cores, Dask and Ray split the same Python work into pieces that run across many — often with barely any code change

## The 40 GB File That Killed a 16 GB Laptop

**Tags:** `core idea` (blue), `Dask` (green), `partitions` (orange)

- **The job** — a year of order logs sits in one 40 GB CSV; the analyst's laptop has 16 GB of RAM
- **The crash** — `pd.read_csv` tries to load all 40 GB at once and the process dies out of memory
- **The switch** — `dd.read_csv` in Dask reads the file as 320 partitions with blocksize set to 128 MB
- **Lazy graphs** — Dask builds a task graph of per-partition steps and runs nothing until `.compute()`
- **The payoff** — only a few partitions are in RAM at any moment, so 40 GB flows through 16 GB

*Example (italic):* The same `df.groupby("store").amount.sum()` line runs unchanged on Dask — it just executes as 320 small groupbys plus one merge instead of one giant one.

**Key point:** Dask mirrors the pandas and NumPy APIs but cuts the data into partitions and executes a lazy task graph across them — the code stays the same while the data no longer has to fit in memory.

### Visualization (canvas `c1`, 720×300)

Line chart of RAM used over the run: pandas climbing to an out-of-memory crash vs Dask cycling through partitions under a flat 16 GB limit line.

- **Title (bold 15px, `#1a5276`, top center):** "Same Groupby, 40 GB File: Pandas Hits the RAM Wall, Dask Streams Past It".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes into the run 0 to 10, 12px `#444` tick labels every 2 minutes; y = RAM used in GB 0 to 20, gridlines `#e5e9ef` at 5/10/15.
- **Limit line:** horizontal dashed `#6b7280` (dash 4/3) line at 16 GB with 12px `#6b7280` label "16 GB RAM limit" at its right end.
- **Pandas line:** red `#e74c3c` 3px line through minutes `[0, 1, 2, 3, 4, 4.5]`, GB `[1, 5, 9, 13, 15.8, 16]`, ending in a bold 16px red "✗ OOM" marker at (4.5, 16).
- **Dask line:** green `#008300` 2px sawtooth through minutes `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, GB `[1, 2.4, 1.6, 2.5, 1.7, 2.4, 1.6, 2.5, 1.7, 2.3, 1.8]` — partitions loaded and released.
- **Annotation (bold 13px green `#008300`, near minute 6, y=110):** "320 partitions, a few in RAM at a time".
- **Caption (12px `#444`, bottom right):** "memory profile illustrative; 40 GB / 128 MB = 320 partitions exact".

## Fanning Out 48 Trials with @ray.remote

**Tags:** `worked example` (blue), `Ray tasks` (green)

- **The sweep** — the analyst tunes a model over 48 hyperparameter configs; one trial takes 10 minutes
- **Serial cost** — run in a plain loop, 48 trials × 10 min = 480 minutes, an entire working day
- **One decorator** — `@ray.remote` on the train function turns each call into a task Ray schedules
- **On the laptop** — 8 cores run 8 trials at once: 48 / 8 = 6 waves × 10 min = 60 minutes
- **On a cluster** — 4 workers × 8 cores = 32 slots: 48 / 32 rounds up to 2 waves = 20 minutes
- **Actors too** — `@ray.remote` on a class makes an actor: a stateful worker holding data between calls

*Example (italic):* The loop body barely changes — `train(cfg)` becomes `train.remote(cfg)` and one `ray.get()` collects all 48 results.

**Key point:** Ray turns ordinary functions into distributed tasks and classes into actors with one decorator — the sweep's shape stays a plain Python loop while the work fans out across every core it can find.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of wall-clock time for the same 48-trial sweep at three levels of parallelism.

- **Title (bold 15px, `#1a5276`, top center):** "48 Trials × 10 Minutes Each: Same Loop, Three Speeds".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; linear scale where 480 min = 440 px.
- **Rows (bars 26px tall, top edges at y = 70, 140, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "serial loop — 1 core": red `#e74c3c` fill `rgba(231,76,60,0.30)` bar width 440, 12px `#444` end label "480 min"
  - "Ray on laptop — 8 cores": blue `#2a78d6` fill `rgba(42,120,214,0.30)` bar width 55, end label "60 min (6 waves)"
  - "Ray cluster — 32 cores": green `#008300` fill `rgba(0,131,0,0.30)` bar width 18, end label "20 min (2 waves)"
- **Bar edges:** 2px solid stroke in each bar's base color.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=260):** "24× fewer minutes from one decorator".
- **Caption (12px `#444`, bottom right):** "10-min trial time illustrative; wave counts 48/8=6 and ⌈48/32⌉=2 exact".

## The Layer Under the PyData and ML Stack

**Tags:** `where it's used` (blue), `ML infrastructure` (green)

- **Dask's lane** — parallel pandas, NumPy and scikit-learn workflows: dataframes, arrays, delayed graphs
- **Ray's lane** — general distributed Python from Berkeley's RISELab, grown into ML infrastructure
- **The libraries** — Ray Tune sweeps hyperparameters, Ray Train scales training, Ray Serve deploys models
- **LLM era** — Ray is the scheduling layer under many large-model training and serving stacks
- **Same laptop start** — both run locally first, so the cluster is an upgrade, not a rewrite

*Example (italic):* A team prototypes a sweep with Ray Tune on one desktop, then points the same script at a 32-node cluster by changing only the `ray.init` address.

**Key point:** Dask scales the analysis stack you already write (pandas/NumPy-shaped code); Ray scales arbitrary Python and carries the ML libraries — together they are how Python work outgrows one machine.

### Visualization (canvas `c3`, 720×300)

Two side-by-side stack diagrams: the Dask stack over the PyData APIs it mirrors, and the Ray stack with its ML libraries over Ray core.

- **Title (bold 15px, `#1a5276`, top center):** "Two Stacks, One Idea: Familiar Code on Top, a Cluster Underneath".
- **Left stack (Dask), boxes 250px wide centered at x=190:** top box at y=60 labeled "dask.dataframe / dask.array" (blue `#2a78d6`, fill `rgba(42,120,214,0.15)`), 12px `#6b7280` note "mirrors pandas / NumPy APIs" just below it; middle box at y=125 labeled "lazy task graph" (violet `#4a3aa7`, fill `rgba(74,58,167,0.12)`); bottom box at y=190 labeled "cores or cluster" (green `#008300`, fill `rgba(0,131,0,0.12)`).
- **Right stack (Ray), boxes 250px wide centered at x=530:** top box at y=60 labeled "Ray Tune / Train / Serve" (blue, same fill); middle box at y=125 labeled "Ray core: tasks + actors" (violet, same fill), 12px `#6b7280` note "@ray.remote" at its right edge; bottom box at y=190 labeled "cores or cluster" (green, same fill).
- **Box style:** 40px tall, 8px radius, 2px stroke in each box's base color, bold 13px `#2c3e50` centered text; 2px `#6b7280` downward arrows between stacked boxes.
- **Column headers (bold 13px `#1a5276`):** "Dask — scale the PyData stack" centered at x=190, y=40; "Ray — scale any Python" centered at x=530, y=40.
- **Annotation (bold 13px magenta `#d55181`, centered near y=265):** "the bottom layer changes; the code on top mostly doesn't".

## Distributing a Job That Fit All Along

**Tags:** `common mistake` (red), `overhead` (orange)

- **The temptation** — the cluster exists, so every job gets sent to it, even a 200 MB dataframe
- **The overhead** — scheduling, serializing and shipping partitions has a fixed cost per task
- **The flip** — on small data that fixed cost dominates: the distributed run is slower than pandas
- **The check** — if the data fits in RAM and pandas finishes in seconds, distribution buys nothing
- **The rule** — reach for Dask or Ray when memory or hours run out, not by default

*Example (italic):* A 200 MB groupby takes 0.8 s in pandas and 4 s on the cluster — the cluster spends more time moving the work than doing it.

**Common mistake:** Distributing work that fits comfortably on one machine. Task-graph and network overhead is roughly constant, so below a few GB it eats the speedup — both projects' own docs say to stay local when local works.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart of runtime for the same groupby at four data sizes: pandas on one machine vs Dask on a cluster, with the crossover marked.

- **Title (bold 15px, `#1a5276`, top center):** "Small Data on a Cluster: Overhead Eats the Speedup".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four groups labeled "200 MB", "1 GB", "10 GB", "40 GB" (12px `#444`, centered under each group at x = 135, 285, 435, 585); y = runtime in seconds, hardcoded bar heights (log-feel, no real log axis).
- **Bars per group:** pandas bar (blue `#2a78d6`, fill `rgba(42,120,214,0.30)`) left, Dask-cluster bar (orange `#d95926`, fill `rgba(217,89,38,0.25)`) right; each 40px wide, 6px apart.
- **Bar heights (px above baseline) and 11px value labels at bar tops:** pandas `[18, 45, 110, 0]` labeled `["0.8s", "6s", "70s", "OOM"]` — the 40 GB pandas slot is no bar, just a bold 13px red `#e74c3c` "✗ OOM" at the baseline; Dask `[65, 80, 95, 130]` labeled `["4s", "9s", "30s", "95s"]`.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line between the 1 GB and 10 GB groups at x=360, 12px `#6b7280` label "crossover" at its top.
- **Annotation (bold 13px orange `#d95926`, above the 200 MB group, y=70):** "5× slower on the cluster".
- **Caption (12px `#444`, bottom right):** "all runtimes illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); memory profiles, trial minutes and runtimes are invented and labeled illustrative; the arithmetic facts are exact — 40 GB / 128 MB = 320 partitions, 48/8 = 6 waves, ⌈48/32⌉ = 2 waves, 48 × 10 min = 480 min.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
