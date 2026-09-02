# How Things Happened Together

**Page type:** other (single-page long doc: h1, subtitle, intro callout, four full-width sections each with a kind badge, h2, thesis paragraph, one timeline canvas, a reading note, bullets, and optionally a caveat callout)
**HTML title tag:** Evolution — How Things Happened Together

**Subtitle:** Four ways strands of the industry overlap — each section puts three or four of them on one timeline and lets the pattern between them show without drawing it.

## Intro callout

**Reading every chart on this page:** a technology is written at the year it began gaining mainstream adoption, so the name is the mark. A **filled dot** means it arrived quickly, a **hollow dot** that it climbed gradually. The thin line trailing right, fading out at the present, shows it continuing; a **vertical tick** marks where it stopped being mainstream practice, and a **dashed line** means it survives in niches only. Rows are collected into labelled groups down the left. **No arrows are drawn between rows** — where one group precedes another, the gap between them is the point. Dates are approximate.

## 1. Hardware Capability Arrived First, Software Followed

**Kind badge:** Overlap type · lag between capability and use

**Thesis:** The same sequence repeats across eighty years: a hardware capability becomes cheap and widely available, and the software that depends on it becomes mainstream several years later. The gap is rarely shorter than three years and has been as long as sixteen.

**Reading note (below canvas):** Hardware rows are grouped above, the software shifts that depended on them below. Rows are ordered by role rather than by date, so the horizontal distance between a capability and the software beneath it is the lag.

- **Transistors, then integrated circuits, made memory and cycles affordable enough to spend on abstraction.** Compiled high-level languages were viable years before they were normal, because early machines could not spare the overhead a compiler imposed.
- The microprocessor in 1971 put a whole processor on one part, and the software industry that assumed a machine per person — shrink-wrapped applications, the office suite — follows about a decade later.
- Falling memory prices are what made **garbage collection acceptable**. Java and the managed runtimes traded memory for programmer time, a trade that was simply unaffordable earlier.
- Consumer graphics hardware from 1996 became generally programmable with CUDA in 2007, and the neural network results that rely on it begin in 2012 — **a gap of about sixteen years** between the silicon being available and the software using it this way.
- Two cases run the other direction, where hardware stopped improving and software had to absorb it: **frequency scaling hit a power wall** around 2004, after which concurrency moved into languages and runtimes rather than arriving free with the next processor.
- Flash storage removed the seek penalty that decades of database and file system design had been built to avoid, which is what made key-value stores and log-structured designs practical rather than merely interesting.

### Visualization (canvas `data-story="0"`, responsive width min 720px × computed height)

Grouped timeline chart (shared renderer, see Regeneration instructions). Year range 1945–2026, gridline step 10 years.

Rows are `[label, start, end, status, adoption]` where status is alive / niche / gone and adoption `fast` means filled dot (else hollow); end 2026 means still current.

- **Group "Hardware capability"** (colour `#d35400`):
  - Transistors replace tubes, 1955–2026, alive, hollow
  - Integrated circuits, cheap memory, 1965–2026, alive, hollow
  - Microprocessor — a CPU on one part, 1971–2026, alive, fast
  - Frequency scaling, 1975–2004, gone, hollow
  - Consumer GPUs, 1996–2026, alive, fast
  - Multicore — frequency stops rising, 2005–2026, alive, hollow
  - CUDA — GPUs generally programmable, 2007–2026, alive, fast
  - Flash / SSD — no seek penalty, 2009–2026, alive, fast
  - Accelerators, TPUs, 2016–2026, alive, hollow
- **Group "What software then did"** (colour `#1a5276`):
  - Compiled high-level languages, 1957–2026, alive, hollow
  - Interactive, then personal computing, 1976–2026, alive, fast
  - Managed runtimes, garbage collection, 1995–2026, alive, fast
  - Virtualisation — one machine as many, 2001–2026, alive, fast
  - Concurrency moves into the language, 2007–2026, alive, hollow
  - Key-value and log-structured stores, 2009–2026, alive, fast
  - Deep learning on GPUs, 2012–2026, alive, fast
  - Transformers, then large models, 2017–2026, alive, fast
- **Group "And the data grew to match"** (colour `#16a085`):
  - Relational databases on disk, 1974–2026, alive, fast
  - Warehouses at terabyte scale, 1995–2026, alive, hollow
  - Object storage, 2006–2026, alive, fast
  - Web-scale training corpora, 2019–2026, alive, hollow

## 2. Query Languages and Formats Accumulated Rather Than Replaced

**Kind badge:** Overlap type · accumulation, not succession

**Thesis:** Each new grammar or format arrived to serve a scenario the existing ones did not cover. The earlier ones stayed in mainstream use, so what looks like a sequence of replacements is mostly addition.

**Reading note (below canvas):** Each group is a reason a new grammar or format was needed. Count the vertical ticks: almost nothing here ended, and the few that did are clustered in one group.

- **CSV from 1972 and SQL from 1974 are both still in mainstream use.** Fifty years of successors were added alongside them rather than in place of them.
- Each new shape of data — nested, binary, columnar, one record per line — added a format without retiring one. A current pipeline commonly uses five of these together.
- The only group with substantial casualties is the one driven by **scale**: MapReduce and Pig Latin ended as mainstream practice, and that line of work concluded by returning to SQL rather than by being superseded by something new.
- New kinds of question received new grammars — log pipelines, then vector similarity, then plain language — and these remain confined to their domains. **A language persists by holding a well-defined boundary.**

**Caveat callout:** **The counter-case:** if workloads determined grammar directly, SQL would have remained displaced after MapReduce. Instead Hive, Pig, Spark SQL and Trino were built over the following decade to return to it. A new workload reliably produces a new grammar; whether that grammar persists depends on whether its abstraction boundary is better than the one it replaced.

### Visualization (canvas `data-story="1"`, responsive width min 720px × computed height)

Grouped timeline chart. Year range 1970–2026, gridline step 10 years.

- **Group "Still here from the start"** (colour `#1a5276`):
  - CSV / TSV, 1972–2026, alive, hollow
  - SQL, 1974–2026, alive, fast
  - ANSI SQL standard, 1986–2026, alive, hollow
- **Group "Added for a new shape"** (colour `#16a085`):
  - XML, 1998–2026, niche (dashed line), fast
  - JSON, 2001–2026, alive, fast
  - Protobuf / Thrift, 2001–2026, alive, hollow
  - Parquet / ORC — columnar, 2013–2026, alive, fast
  - JSONL / NDJSON, 2015–2026, alive, fast
  - Arrow — columnar in memory, 2016–2026, alive, hollow
- **Group "Added for a new scale"** (colour `#c0392b`):
  - MapReduce, hand-planned, 2004–2012, gone (vertical end tick), fast
  - Pig Latin, 2006–2015, gone, hollow
  - HiveQL, 2008–2026, alive, hollow
  - SQL on new engines, 2010–2026, alive, fast
  - Spark SQL, 2015–2026, alive, fast
- **Group "Added for a new question"** (colour `#7e22ce`):
  - Splunk SPL — log pipelines, 2003–2026, alive, hollow
  - Kusto KQL, 2018–2026, alive, hollow
  - Vector similarity, 2021–2026, alive, fast
  - Plain language → SQL, 2023–2026, alive, fast

## 3. How Languages Evolved With the Work They Served

**Kind badge:** Overlap type · the job arrives before the tool

**Thesis:** Each dominant kind of work is followed within a few years by a paradigm, a language and a storage format suited to it. Calculation had Fortran and C; systems that outgrew one author had objects; pipelines had map-and-reduce; networked services had Go and containers; model work had Python.

**Reading note (below canvas):** The top group is the kind of work that dominated; the groups below are what showed up to serve it. **The top group is editorial** — those spans are a reading of the history rather than dated releases, unlike every other row on this page.

- Calculation-heavy programs were served by Fortran and C. Programs that grew past what one person could hold were served by **objects and encapsulation**, which addressed team size as much as problem structure.
- Log-scale pipelines brought map-and-reduce into general use. The lasting effect was not the adoption of functional languages but **functional constructs being added to imperative ones** — lambdas and immutable collections in Java and Python.
- Services communicating over networks made startup time and deployment size matter, which is the practical basis for Go's adoption.
- Python entered as **plumbing rather than as a general programming language** — a simple way to wire together numerical code already written in C and Fortran. That role is what brought it into scientific work.
- It then became the language the work was written in. With enough people already using it for wiring, the models, libraries and teaching followed, and the effect **compounds**: simple syntax, so it is taught widely, so more libraries exist, so there is more reason to teach it.

### Visualization (canvas `data-story="2"`, responsive width min 720px × computed height)

Grouped timeline chart. Year range 1955–2026, gridline step 10 years.

- **Group "The dominant job"** (colour `#7f8c8d`, editorial: drawn grey with 1/3 dashed lines):
  - Calculation and reporting, 1957–1985, gone, hollow
  - Systems too large for one person, 1980–2026, alive, hollow
  - Pipelines over logs at web scale, 2004–2026, alive, fast
  - Services over networks, 2012–2026, alive, fast
  - Training and serving models, 2015–2026, alive, fast
- **Group "Organised as"** (colour `#1a5276`):
  - Procedural, 1960–2026, alive, hollow
  - Object-oriented, 1980–2026, alive, fast
  - Functional style, retrofitted, 2007–2026, alive, hollow
  - Microservices, 2012–2026, alive, fast
  - Serverless, 2015–2026, alive, fast
- **Group "Written in"** (colour `#2980b9`):
  - Fortran, COBOL, Lisp, 1957–1980, niche (dashed line), hollow
  - C, 1972–2026, alive, hollow
  - C++, 1983–2026, alive, hollow
  - Java, JavaScript, 1995–2026, alive, fast
  - Python as plumbing over C, 1998–2026, alive, hollow
  - Go, Rust, TypeScript, 2010–2026, alive, hollow
  - Python as the language itself, 2015–2026, alive, fast
- **Group "Data handled as"** (colour `#16a085`):
  - Relational, schema on write, 1974–2026, alive, fast
  - Warehouse and cubes, 1995–2026, alive, hollow
  - Schema on read, JSON, 2004–2026, alive, fast
  - Columnar on object storage, 2013–2026, alive, fast
  - Embeddings and vector indexes, 2021–2026, alive, fast

## 4. Every Strand Turned Over Between 2004 and 2011

**Kind badge:** Overlap type · simultaneous, independent movement

**Thesis:** Silicon, storage, build practice and consumer products all changed within the same seven years. Multicore, CUDA, ARM, object storage, IaaS, Git, GitHub, social networking and mobile-first products all reached mainstream adoption in this window.

**Reading note (below canvas):** A narrow window, drawn year by year — the strip above the chart shows where these years sit in the full 1945–2026 range. Unlike the other sections there is no cause-and-effect ordering here — the claim is only that these were contemporaneous.

- Several of these were forced rather than chosen. Multicore happened because **frequency scaling hit a power wall**, and everything about concurrency downstream follows from a physics limit.
- Cheap object storage and rented infrastructure landed within months of each other, which is what made "keep everything and decide later" a viable default instead of a luxury.
- Git and then GitHub arriving here matters more than it looks: **reviewable, branchable history is the precondition** for the tooling that came fifteen years later.
- Set against this, the current AI period is **narrow** — the movement is concentrated in models and data while languages, networking and architecture are comparatively quiet. It feels larger partly because we are inside it.

**Caveat callout:** **How much to trust "everything at once":** the clustering is measured from this project's own list of phases, so it reflects which phases were recorded and how they were dated. The concentration in these years is real and shows up regardless of reasonable re-dating; the exact count of strands is a property of the segmentation, not a discovery about history.

### Visualization (canvas `data-story="3"`, responsive width min 720px × computed height)

Grouped timeline chart. Year range 2003–2012, gridline step 2 years (spans extending past 2012 are clamped to the right edge). A context strip above the plot (`context: [1945, 2026]`) shows the zoomed range inside the full 1945–2026 span: a thin `#d5dde3` baseline across the plot width with `1945`/`2026` labels (11px `#666`) above its ends, a shaded band over the zoom range (fill `rgba(41,128,185,0.22)`, stroke `rgba(41,128,185,0.55)`, 10px tall), and two lens lines from the band's bottom corners down to the zoomed plot's top corners; adds 34px to the canvas height.

- **Group "Silicon"** (colour `#d35400`):
  - Multicore — the power wall, 2005–2026, alive, hollow
  - CUDA / GPGPU, 2007–2026, alive, fast
  - Mobile SoC, ARM, 2008–2026, alive, fast
  - Flash / SSD, 2009–2026, alive, fast
- **Group "Where it ran"** (colour `#16a085`):
  - Object storage, 2006–2026, alive, fast
  - Cloud IaaS, 2006–2026, alive, fast
  - SaaS subscriptions, 2006–2026, alive, fast
  - 10–100 Gbps data centres, 2007–2026, alive, hollow
- **Group "How it was built"** (colour `#1a5276`):
  - MapReduce, hand-planned, 2004–2012, gone, fast
  - Git, 2005–2026, alive, fast
  - Rails, Django, 2005–2026, alive, fast
  - Functional style returns, 2007–2026, alive, hollow
  - GitHub and pull requests, 2008–2026, alive, fast
- **Group "What people did with it"** (colour `#27ae60`):
  - Social networking, 2004–2026, alive, fast
  - Video platforms, 2005–2026, alive, fast
  - Subscription streaming, 2007–2026, alive, hollow
  - Mobile-first products, 2008–2026, alive, fast
  - Mobile photo and messaging, 2010–2026, alive, fast

## Regeneration instructions

- **Layout:** single long page inside a full-width `.wrap` div: h1, `.subtitle`, `.intro` callout, then four `.sec` sections separated by `border-top: 2px solid #2980b9` and 46px bottom margin / 26px top padding. Each section: `.kind` badge, h2, `.thesis` paragraph, `.viz-wrap` (overflow-x auto) containing one `<canvas data-story="N">`, a `.reads` note, a `<ul>` of bullets, and optionally a `.caveat` callout at the end.
- **Callout styles:** `.intro` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 14px 18px, 0.95em. `.caveat` — background `#fffaf0`, left border `4px solid #e67e22`, padding 11px 16px, 0.9em, `strong` inside in `#a04000`.
- **Kind badge:** `.kind` — inline-block, 0.72em, weight 700, uppercase, letter-spacing 0.7px, colour `#1f618d`, background `#eaf2f9`, border `1px solid #cfe0f0`, radius 3px, padding 2px 8px.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6, base font 15px; h1 1.9em `#1a5276`; h2 1.32em `#1a5276`; `strong` in `#1a5276`; subtitle `#555` 1.05em; `.thesis` 1.0em `#333`; `.reads` 0.88em `#666`; bullets 0.95em `#333`. No nav bar, no back/home links.
- **Canvas / shared timeline renderer:** all four charts are drawn by one function reading a `STORIES` array of `{range, step, groups}` (plus optional `context: [min, max]` — draws the context strip described under section 4 and adds 34px to the height) where each group has label, colour, optional `editorial: true`, and rows `[label, start, end, status, adoption]`. Canvases are `width: 100%; min-width: 720px; display: block`; height computed as `TOP(34) + contextStrip(34, if present) + nRows*ROW(25) + (nGroups-1)*GRP_GAP(12) + BOT(34)`. Backing store scaled by `window.devicePixelRatio` (`setTransform(dpr,0,0,dpr,0,0)`); redraw on window resize (debounced 140ms). Left gutter 116px holds the wrapped uppercase group label (10.5px bold, group colour) plus a 2px vertical accent line at x=2 in the group colour at 55% alpha; alternate groups get a `#fafcfe` band behind their rows. Year gridlines every `step` years in `#eef3f7` with year labels in `#666` 11px below the plot. Per row: a 1px horizontal line from start to end in the group colour at 45% alpha (32% and dashed 3/3 for `niche`; dashed 1/3 for editorial groups, drawn grey `#7f8c8d`); rows still current at the range max fade to transparent over the line's final 60px (linear gradient) instead of stopping hard at the edge; if the span ends before the range max, a 1.6px vertical tick (±4.5px) at the end in 75% alpha marks it as gone-from-mainstream; a 3.6px-radius dot at the start (filled with the group colour for `fast` adoption, else white-filled with 1.4px coloured stroke); the row label in 13px (600 weight if fast) in the group colour, placed right of the dot with a 7px gap on a white text-background rect, flipped to the left of the dot if it would overflow the right padding (16px).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; additional group colours used here: `#d35400`, `#16a085`, `#c0392b`, `#7e22ce`, `#2980b9`, `#7f8c8d` (editorial grey).
