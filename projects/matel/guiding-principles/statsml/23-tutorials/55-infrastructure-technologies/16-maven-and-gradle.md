# Maven & Gradle

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Maven & Gradle

**Subtitle:** The JVM's build systems compile, test, and package your code — but their real job is fetching every library your libraries need, and picking a version when two of them disagree

## One Line in pom.xml, Thirteen Jars in the Build

**Tags:** `core idea` (blue), `dependency resolution` (green), `Maven` (orange)

- **The job** — a build system compiles, runs tests, packages a jar, and above all resolves dependencies
- **The line** — an orders service adds one declaration to its `pom.xml`: `web-client 4.2`
- **The fan-out** — `web-client` itself needs 4 libraries, and those need 8 more: 12 transitive jars arrive
- **Convention** — Maven (2004) fixed the layout (`src/main/java`, `src/test/java`) so builds need no setup
- **The warehouse** — every jar is fetched from Maven Central, the artifact repository the whole JVM world shares

*Example (italic):* The team wrote one `<dependency>` line; the build downloads 13 jars (1 declared + 12 transitive) and puts all of them on the classpath.

**Key point:** You declare what you use; the build system computes the full closure of what your dependencies use — most of your classpath is jars you never asked for by name.

### Visualization (canvas `c1`, 720×300)

Left-to-right fan-out diagram: one declared jar expanding into 4 direct dependencies, which expand into 8 more — 13 boxes total in three columns.

- **Title (bold 15px, `#1a5276`, top center):** "One Declared Jar Pulls In 12 More".
- **Column headers (bold 12px `#6b7280`):** "you declare (1)" at x=90, "it needs (4)" at x=330, "they need (8)" at x=570 — all at y=55.
- **Declared box:** blue `#2a78d6` rounded box (130×34, 8px radius, fill `rgba(42,120,214,0.15)`) centered at (90, 160), 12px `#2c3e50` label "web-client 4.2".
- **Direct boxes (column 2, x=330):** four green-edged boxes (120×28, fill `rgba(0,131,0,0.10)`, 1.5px `#008300` border) at y = 90, 140, 190, 240, labels 11px: "http-core 4.2", "json-mapper 2.6", "logging-api 1.7", "codec-util 1.15".
- **Transitive boxes (column 3, x=570):** eight gray boxes (120×22, fill `rgba(107,114,128,0.10)`, 1px `#6b7280` border) at y = 75, 102, 129, 156, 183, 210, 237, 264, labels 11px `#444`: "conn-pool 2.0", "uri-tools 1.3", "mapper-core 2.6", "annotations 2.6", "stream-io 2.6", "logging-impl 1.7", "commons-lang 3.12", "charset-x 1.15".
- **Edges:** 1.5px `#c8d0da` lines from the declared box to each direct box, and from direct boxes to their transitive boxes (http-core → rows 1–2, json-mapper → rows 3–5, logging-api → row 6, codec-util → rows 7–8).
- **Annotation (bold 13px green `#008300`, near x=330, y=278):** "13 jars on the classpath from 1 line of pom.xml".
- **Caption (12px `#444`, bottom right):** "library names and counts illustrative".

## Two Paths, Two Versions: Nearest Wins

**Tags:** `worked example` (blue), `version mediation` (green)

- **The clash** — `json-mapper` shows up twice in the tree: once at version 2.6, once at version 2.9
- **Path A** — root → `web-client 4.2` → `json-mapper 2.6`; the conflict node sits at depth 2
- **Path B** — root → `metrics-kit 1.1` → `stats-core 3.0` → `json-mapper 2.9`; depth 3
- **The rule** — Maven's mediation is nearest-wins: the version on the shortest path from the root ships
- **Hand-check** — depth 2 beats depth 3, so 2.6 wins — even though 2.9 is the newer version

*Example (italic):* One classpath slot, two candidates: `json-mapper 2.6` at depth 2 beats `json-mapper 2.9` at depth 3, so the build ships 2.6 and silently drops 2.9.

**Key point:** Only one version of a jar can be on the classpath, so the build must mediate — Maven picks by tree distance, not by version number, and never asks you.

### Visualization (canvas `c2`, 720×300)

Dependency tree diagram: root at the left, two paths reaching `json-mapper` at different depths, with the depth-2 winner highlighted and the depth-3 loser struck through.

- **Title (bold 15px, `#1a5276`, top center):** "Nearest Wins: Depth 2 Beats Depth 3 — Even When 2.9 Is Newer".
- **Root box:** ink-edged box (120×32, fill `rgba(26,82,118,0.12)`, 1.5px `#1a5276` border) centered at (85, 155), 12px label "orders-service".
- **Path A (upper):** box "web-client 4.2" at (280, 105), then winner box "json-mapper 2.6" at (490, 105) — winner drawn 130×34 with 2.5px `#008300` border, fill `rgba(0,131,0,0.12)`, bold 12px green tick "✓ ships" at (585, 92).
- **Path B (lower):** box "metrics-kit 1.1" at (250, 210), box "stats-core 3.0" at (420, 210), then loser box "json-mapper 2.9" at (600, 210) — loser drawn with 1.5px `#e74c3c` border, fill `rgba(231,76,60,0.10)`, a 2px red strike-through line across it, and bold 12px red "✗ dropped" at (600, 245).
- **Middle boxes:** 120×28, fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border, 11px `#2c3e50` labels.
- **Edges:** 1.5px `#c8d0da` lines root→A1→winner and root→B1→B2→loser; depth labels 11px `#6b7280` under each hop: "depth 1", "depth 2", "depth 3".
- **Annotation (bold 13px violet `#4a3aa7`, near x=300, y=272):** "shortest path wins — version number is not the tiebreaker".
- **Caption (12px `#444`, bottom right):** "tree illustrative; nearest-wins rule exact (Maven)".

## Why Android and Big Repos Went to Gradle

**Tags:** `where it's used` (blue), `incremental builds` (green), `Gradle` (orange)

- **Scripts, not XML** — Gradle build files are Groovy or Kotlin code, so builds can compute and branch
- **The task graph** — every step (compile, test, jar) is a node; Gradle runs only what changed inputs touch
- **Incremental** — edit one file and Gradle recompiles that module, not the whole project
- **Build cache** — task outputs are keyed by input hashes, so unchanged work is fetched, never redone
- **The badge** — Gradle is Android's official build system; large multi-module repos are its home turf

*Example (italic):* On a 40-module project, a clean build takes 210s under either tool, but after a one-file edit Gradle's incremental build finishes in 14s, and a cache hit in 6s (illustrative).

**Key point:** Maven re-runs its fixed lifecycle every time; Gradle's task graph skips everything whose inputs did not change — on large projects that difference dominates the workday.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: wall-clock time for the same one-file change under four build strategies, bars extending right from a common baseline.

- **Title (bold 15px, `#1a5276`, top center):** "Same One-File Edit, Four Build Times (40-module project)".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430 = 210s scale (x pixels ≈ 250 + seconds × 2.05).
- **Rows (bar centers at y = 80, 130, 180, 230), each with a right-aligned 12px `#444` label ending at x=240:**
  - "Maven clean build — 210s": blue `#2a78d6` bar width 430
  - "Gradle clean build — 210s": blue `#2a78d6` bar width 430
  - "Gradle incremental — 14s": green `#008300` bar width 29
  - "Gradle cache hit — 6s": aqua `#199e70` bar width 12
- **Bar style:** 22px tall, fills at 0.85 alpha, 12px bold value labels ("210s", "210s", "14s", "6s") 8px past each bar end in the bar's color.
- **Annotation (bold 13px green `#008300`, near x=430, y=205):** "15× faster: only changed tasks re-run".
- **Caption (12px `#444`, bottom right):** "timings illustrative; clean builds tie by construction".

## Shipping Whichever Version Won

**Tags:** `common mistake` (red), `dependency hell` (orange)

- **The trap** — two libraries need incompatible versions of the same jar; mediation still picks exactly one
- **Silent** — the build succeeds with `json-mapper 2.6` on the classpath; nothing warns that 2.9 lost
- **Tool-specific** — nearest-wins is Maven; Gradle defaults to highest — a different silent winner
- **The blowup** — `stats-core 3.0` calls a method added in 2.9; at runtime: `NoSuchMethodError`
- **Worse** — it explodes only on the code path that touches `stats-core`, so tests can pass and prod can die
- **The fix** — inspect the tree (`mvn dependency:tree`, `gradle dependencyInsight`) and pin one version explicitly

*Example (italic):* The build is green, the demo works — then the first metrics flush in production throws `NoSuchMethodError: JsonMapper.streamWrite`, a method that exists in 2.9 but not in the 2.6 that shipped.

**Common mistake:** Trusting a green build to mean the dependency graph is consistent. Mediation guarantees one version ships — not that every library can live with it; the loser's missing methods surface at runtime, not at compile time.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: silent mediation ending in a runtime crash vs an explicit version pin ending in a working service.

- **Title (bold 15px, `#1a5276`, top center):** "The Silent Winner: Green Build, Runtime Crash".
- **Row 1 (boxes centered on y=100), label 12px `#444` at x=20:** "silent default"; blue `#2a78d6` rounded box at x=170 labeled "2.6 vs 2.9 conflict" (12px), 3px arrow to a blue box at x=370 labeled "nearest wins: 2.6 · build green", 3px arrow to a red `#e74c3c` box at x=590 labeled "stats-core calls 2.9 API" with bold 12px red "✗ NoSuchMethodError in prod" beneath at y=138.
- **Row 2 (boxes centered on y=215), label:** "explicit pin"; blue box at x=170 "2.6 vs 2.9 conflict", 3px arrow to a green `#008300` box at x=370 labeled "pin json-mapper 2.9", arrow to a green box at x=590 labeled "both libs run on 2.9" with bold 12px green "✓" beneath at y=253.
- **Box style:** 150–175px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, borders 1.5px in the box color.
- **Annotation (bold 13px orange `#d95926`, centered near y=282):** "mediation resolves the build, not the compatibility — check the tree, then pin".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, labels, and bar widths are the hardcoded values above (no randomness); library names, jar counts (1 + 12 = 13), and build timings (210 / 210 / 14 / 6 s) are invented and labeled illustrative; the nearest-wins mediation rule, the standard Maven layout, and Gradle being Android's official build system are exact publicly documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
