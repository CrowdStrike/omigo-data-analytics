# IntelliJ & JetBrains

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** IntelliJ & JetBrains

**Subtitle:** JetBrains bet on depth over breadth — the IDE reads and understands your entire project, so renaming a method with 47 callers becomes a safe one-click operation

## The Editor That Read Every File First

**Tags:** `core idea` (blue), `semantic model` (green), `since 2001` (orange)

- **The bet** — IntelliJ IDEA (2001) parses the whole project up front and keeps a live model of it
- **The model** — it knows this `process` is a method on `OrderService`, not just an 8-letter string
- **The payoff** — ask "who calls this?" and get the exact 47 call sites, never a lookalike match
- **Versus text** — a text tool sees characters; `process` in a comment looks identical to a call
- **Always fresh** — every keystroke re-parses and re-resolves, so the model tracks the code live

*Example (italic):* In an 1,850-file Java project, "find usages of OrderService.process" returns exactly 47 results in one click — none of them a comment or a lookalike.

**Key point:** JetBrains' founding bet is the full semantic model: the IDE continuously parses and resolves the entire project, so its answers come from understanding the code, not from matching strings.

### Visualization (canvas `c1`, 720×300)

Two-panel diagram: the same word `process` as a text tool sees it (four identical strings) vs as the semantic model sees it (one resolved method node with its relationships).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Eight Letters: a String vs a Resolved Method".
- **Left panel:** rounded rect x=25, y=55, width=320, height=215, 2px border `#6b7280`, 8px radius; header bold 13px `#6b7280` "what a text tool sees" centered at y=75.
- **Left contents:** four "code lines" at y = 105, 145, 185, 225 — each a 3px-tall `#d1d5db` bar from x=45 to x=325, with the word "process" in bold 12px orange `#d95926` drawn on the bar at x=140; beneath each, an 11px `#6b7280` sub-label at x=45: "a real call", "a comment", "a string literal", "an unrelated method" — visually indistinguishable above, only the sub-labels differ.
- **Right panel:** rounded rect x=375, y=55, width=320, height=215, 2px border `#1a5276`, 8px radius; header bold 13px `#1a5276` "what the semantic model sees" centered at y=75.
- **Right contents:** center node — blue `#2a78d6` rounded box (190×36, fill `rgba(42,120,214,0.15)`) at x=440, y=100, 12px `#2c3e50` text "OrderService.process(Order)"; three child boxes at y=190 (each 90×36, 11px text) at x=390 / x=490 / x=590: green `#008300` "47 call sites", violet `#4a3aa7` "103 lookalikes / ruled out" (two lines), mute `#6b7280` "comments: not code"; 2px `#6b7280` connector lines from the center node down to each child.
- **Annotation (bold 13px green `#008300`, centered near y=290):** "the model knows which process this is".

## Renaming process() Without Breaking Anything

**Tags:** `worked example` (blue), `refactoring` (green)

- **The task** — rename `OrderService.process` to `submitOrder` across the 1,850-file project
- **Text search** — grep for `process` returns 212 matches; only 47 are calls to this method
- **The noise** — 103 other identifiers named process, 38 comment mentions, 24 string literals
- **Hand-check** — 47 + 103 + 38 + 24 = 212, so 165 of the 212 grep hits must NOT be touched
- **Semantic rename** — the IDE edits the declaration plus the 47 true call sites: 48 edits, 0 misses
- **The breakthrough** — safe one-click rename/move/extract made Fowler-style refactoring routine

*Example (italic):* The rename lands as one atomic change — 48 edits across 31 files — while the 24 string literals containing "process" are left untouched.

**Key point:** Trustworthy automated refactoring — every real reference updated, nothing else touched — was the historical breakthrough that made refactoring an everyday operation, and it only works because the IDE resolves references instead of matching strings.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart breaking the 212 grep matches for "process" into what they actually are; only the green bar is safe to rename.

- **Title (bold 15px, `#1a5276`, top center):** "One grep, 212 Matches — Only 47 Are the Method".
- **Axis:** vertical 2px `#999` baseline at x=230; bars extend right, 3px of width per match; bars 16px tall with 11px `#444` count labels just past each bar end.
- **Rows (top to bottom at y = 80, 125, 170, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "calls to OrderService.process — 47": solid green `#008300` bar width 141
  - "other identifiers named process — 103": blue fill `rgba(42,120,214,0.30)` bar width 309, 2px `#2a78d6` edge
  - "comment mentions — 38": solid mute `#6b7280` bar width 114
  - "string literals — 24": solid orange `#d95926` bar width 72
- **Annotation (bold 13px red `#e74c3c`, right side near y=255):** "a text rename would touch 165 wrong places".
- **Caption (12px `#444`, bottom right):** "counts illustrative; 3px per match".

## One Engine, Many IDEs — and Its Own Language

**Tags:** `where it's used` (blue), `product strategy` (green), `Kotlin` (orange)

- **One platform** — parser, indexes, and refactoring engine are shared by every JetBrains IDE
- **Tuned distributions** — PyCharm, WebStorm, GoLand, DataGrip: the same core, tuned per stack
- **Kotlin** — JetBrains built a language (2011) to showcase the platform and secure its future
- **The payoff** — Google made Kotlin an official (2017), then preferred (2019), Android language
- **Survey reality** — VS Code leads public surveys (~74%); IntelliJ ~27%, strongest on the JVM
- **The business** — paid products sustained by depth, while a free competitor owns breadth

*Example (italic):* A Python data scientist in PyCharm and a Go engineer in GoLand are running the same indexing and refactoring engine underneath two different products.

**Key point:** The strategy is depth sold per language: one semantic engine shipped as many specialized paid IDEs, with Kotlin as a language-sized moat that the Android adoption turned into a durable payoff.

### Visualization (canvas `c3`, 720×300)

Stack diagram: five product boxes standing on one shared platform box, a Kotlin box beside them, and a small survey-share inset grounding the breadth-vs-depth numbers.

- **Title (bold 15px, `#1a5276`, top center):** "One Platform, Many IDEs — Kotlin Guards the Moat".
- **Platform box:** x=40, y=190, width=480, height=48, fill `rgba(26,82,118,0.12)`, 2px border `#1a5276`, 8px radius; bold 13px `#1a5276` centered text "IntelliJ Platform — parser · indexes · refactorings · inspections".
- **Product boxes (row at y=95, each 88×44, 8px radius, 11px `#2c3e50` two-line text, 2px `#6b7280` connector line down to the platform box):** x=40 "IntelliJ IDEA (2001)" border `#2a78d6` fill `rgba(42,120,214,0.15)`; x=138 "PyCharm (Python)" border `#008300` fill `rgba(0,131,0,0.12)`; x=236 "WebStorm (JS/TS)" border `#199e70` fill `rgba(25,158,112,0.12)`; x=334 "GoLand (Go)" border `#4a3aa7` fill `rgba(74,58,167,0.12)`; x=432 "DataGrip (SQL)" border `#d95926` fill `rgba(217,89,38,0.12)`.
- **Kotlin box:** x=550, y=95, width=150, height=64, 2px border magenta `#d55181`, fill `rgba(213,81,129,0.10)`, 11px text "Kotlin (2011) — Android official 2017, preferred 2019".
- **Survey inset (bottom right, x=550..710, y=205..280):** 11px `#444` heading "editor survey share"; two mini bars 12px tall at 1.6px per point — "VS Code ~74%" blue `#2a78d6` width 118 at y=230, "IntelliJ ~27%" magenta `#d55181` width 43 at y=255; 11px labels left of each bar.
- **Caption (11px `#444`, under the inset):** "public survey, multi-select, approximate".

## What Depth Costs

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Indexing** — the semantic model must be built: big projects show "indexing..." before full smarts
- **Memory** — holding a resolved model of 1,850 files in RAM costs gigabytes, not megabytes
- **Per language** — the deep model must be rebuilt per language, hence one IDE per stack
- **The mistake** — judging the IDE in its first minute: until indexing ends it is a slow text editor
- **LSP contrast** — language servers amortize the per-language cost, at a lower quality bar

*Example (italic):* First open of the 1,850-file project: ~75 seconds of indexing and ~2.6 GB of RAM before full features; a lightweight editor was typing-ready in 4 seconds on 0.4 GB.

**Common mistake:** Treating the "indexing..." wait as waste. Those minutes are the semantic model being built — the same investment that later makes every rename, usage search, and inspection trustworthy. Deep understanding is a durable moat, but it is expensive per language — exactly the cost LSP amortizes away at a lower quality bar.

### Visualization (canvas `c4`, 720×300)

Two-panel grouped bar chart: time-to-full-features and memory footprint for a lightweight text editor vs IntelliJ on the same big project.

- **Title (bold 15px, `#1a5276`, top center):** "Depth Has a Bill: First-Open Time and Memory".
- **Shared baseline:** 2px `#999` horizontal line at y=245; bars 70px wide grow upward; bold 12px value labels above each bar top; 12px `#444` product names below the baseline.
- **Left panel — "time to full features" (bold 13px `#1a5276` label centered at x=180, y=60), 2.2px per second:** bar at x=90 "lightweight editor", green `#008300`, value "4s", height 9; bar at x=200 "IntelliJ first open", blue `#2a78d6`, value "75s", height 165.
- **Right panel — "memory footprint" (bold 13px `#1a5276` label centered at x=520, y=60), 65px per GB:** bar at x=430 "lightweight editor", green `#008300`, value "0.4 GB", height 26; bar at x=540 "IntelliJ", blue `#2a78d6`, value "2.6 GB", height 169.
- **Divider:** 1px `#e5e9ef` vertical line at x=350 from y=70 to y=245.
- **Annotation (bold 12px violet `#4a3aa7`, centered near x=360, y=95):** "the wait and the RAM are the model being built".
- **Caption (12px `#444`, bottom right):** "numbers illustrative; big-project scale".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the project size (1,850 files), grep breakdown (47/103/38/24 = 212), indexing time (75s vs 4s), and memory (2.6 GB vs 0.4 GB) are invented and labeled illustrative; the dates (IDEA 2001, Kotlin 2011, Android official 2017 / preferred 2019) are documented history; survey shares (~74% / ~27%) are approximate public-survey figures and labeled as such.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
