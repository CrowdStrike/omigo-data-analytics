# LangChain, LlamaIndex & Orchestration

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** LangChain, LlamaIndex & Orchestration

**Subtitle:** LangChain and LlamaIndex are the glue between your documents, your prompts, and a model API — hugely convenient plumbing, with an open debate about how long the glue layer lasts

## One Shopper Question, Five Jobs

**Tags:** `core idea` (blue), `glue layer` (green), `pipelines` (orange)

- **The bot** — a bookstore wants a help bot that answers "can I return a gift without a receipt?"
- **Five jobs** — load the policy PDF, split it into chunks, index them, fetch the best chunk, ask the model
- **The glue** — only the last job is the model; the other four are plumbing that feeds it the right text
- **The frameworks** — LangChain and LlamaIndex ship a prebuilt part for each job; you snap them together
- **Orchestration** — the word for sequencing those parts: which job runs, in what order, on whose output

*Example (italic):* When a shopper types "do you buy used textbooks?", the bot never "knows" the answer — the glue fetches the buyback paragraph and hands it to the model next to the question.

**Key point:** An orchestration framework is prewritten plumbing between your data, your prompts, and a model API — glue, not intelligence.

### Visualization (canvas `c1`, 720×300)

Horizontal pipeline diagram: five rounded boxes from left to right showing the bot's jobs, the first four styled as glue and the last as the model, with phase labels above and the sample question/answer below.

- **Title (bold 15px, `#1a5276`, top center):** "One Shopper Question, Five Jobs — Four of Them Are Glue".
- **Boxes:** five rounded rects (6px radius), each 112 wide × 56 tall, top edge y=118, left edges x = `[30, 168, 306, 444, 582]`; boxes 1–4 fill `rgba(42,120,214,0.12)` with 2px `#2a78d6` border, box 5 fill `rgba(0,131,0,0.12)` with 2px `#008300` border.
- **Box labels (bold 12px, centered, two lines):** "Load the / policy PDF", "Split into / chunks", "Index the / chunks", "Fetch best / chunk", "Ask the / model" — boxes 1–4 in `#1a5276`, box 5 in `#008300`.
- **Arrows:** 2px `#6b7280` horizontal arrows with small arrowheads in the 26px gaps between boxes, at y=146.
- **Phase labels (12px `#6b7280`):** "done once, ahead of time" centered above boxes 1–3 at y=100; "runs on every question" centered above boxes 4–5 at y=100.
- **Question/answer line (12px `#6b7280`, y=230):** left-aligned at x=30: "in: \"can I return a gift without a receipt?\""; right-aligned at x=690: "out: \"yes — 30 days, store credit\"".
- **Annotation (bold 12px orange `#d95926`, centered at y=268):** "only the last box thinks — the other four are glue".
- **Caption (11px `#444`, bottom right, y=292):** "illustrative pipeline for the bookstore bot".

## Counting the Plumbing Lines

**Tags:** `worked example` (blue), `plumbing saved` (green)

- **By hand** — writing the five jobs yourself costs about 30 + 25 + 35 + 20 + 18 = 128 lines of code
- **With a framework** — the same five jobs shrink to 4 + 3 + 6 + 5 + 4 = 22 lines calling prebuilt parts
- **Check it** — add either column yourself: 128 by hand, 22 with the framework, roughly a 6× saving
- **Same brain** — both versions send the same final prompt to the same model API; only plumbing differs
- **The trade** — those 22 lines hide the other layers; when a chunk comes back empty, you debug code you never wrote

*Example (italic):* The bookstore's whole bot fits on one screen with the framework — but the day retrieval returned nothing, the fix was three layers down in someone else's defaults.

**Key point:** 128 lines by hand vs 22 with the framework — the glue saves ~6× the plumbing, paid for in layers you no longer see.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: five job groups on the x axis, each with a "by hand" bar and a "with framework" bar, showing the plumbing shrink job by job.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Bot: 128 Lines by Hand vs 22 with a Framework".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = lines of code 0 to 40 with light `#e5e9ef` gridlines at 10, 20, 30, 40 and 12px `#444` tick labels; x = five groups centered at x = `[130, 248, 366, 484, 602]` with 12px `#444` labels below the baseline: "load", "split", "index", "retrieve", "ask + parse".
- **Bars:** each group has two 34px-wide bars, 6px apart, centered on the group x; left bar blue `#2a78d6`, values `[30, 25, 35, 20, 18]`; right bar green `#008300`, values `[4, 3, 6, 5, 4]`; bold 12px value label above each bar in the bar's color.
- **Legend (12px, top left inside plot at x=80, y=75):** blue swatch "by hand", green swatch "with framework" stacked on two lines.
- **Annotation (bold 13px orange `#d95926`, near x=420, y=85):** "totals: 128 vs 22 lines — ~6× less plumbing".
- **Caption (12px `#444`, bottom right):** "line counts illustrative".

## The Glue Layer in the Stack

**Tags:** `where it's used` (blue), `two frameworks` (green), `standard interfaces` (orange)

- **Where you meet it** — most LLM app demos, tutorials, and job postings since 2023 name one of the two
- **LangChain** — general-purpose glue: chains of steps, agents, memory, and hundreds of connectors
- **LlamaIndex** — data-first glue: loading, indexing, and retrieving your own documents for RAG
- **Not the glue** — Claude Code, Cursor, Codex, Kimi Code are apps in the top band, not glue
- **Swap freedom** — the glue standardizes interfaces, so changing the model or vector store is one line
- **Without it** — every team rebuilds the same five jobs slightly differently, with in-house bugs and lock-in

*Example (italic):* When the bookstore switched model providers, the bot code changed on one line — the glue's standard interface absorbed everything else.

**Key point:** The glue layer's real product is standard interfaces — the same bot code keeps working when the providers underneath change.

### Visualization (canvas `c3`, 720×300)

Three-layer stack diagram: your app on top, the orchestration glue with its five parts in the middle, and the providers at the bottom, with arrows through the glue.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Glue Sits: Your App Above, Providers Below".
- **Top band:** rounded rect x=50, y=52, width 620, height 42; fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; centered bold 13px `#4a3aa7` label: "Your app — the bookstore bot (what shoppers see)".
- **Middle band:** rounded rect x=50, y=114, width 620, height 84; fill `rgba(42,120,214,0.08)`, 2px `#2a78d6` border; bold 13px `#2a78d6` label at top center (y=131): "Orchestration glue — LangChain / LlamaIndex"; inside, five small rounded boxes 108 wide × 32 tall at y=152, left edges x = `[62, 184, 306, 428, 550]`, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, centered 12px `#1a5276` labels: "loaders", "splitters", "indexes", "prompts", "parsers".
- **Bottom band:** rounded rect x=50, y=218, width 620, height 42; fill `rgba(0,131,0,0.10)`, 2px `#008300` border; centered bold 13px `#008300` label: "Providers — model API · vector store · your PDFs".
- **Arrows:** two vertical 2px `#6b7280` arrows with arrowheads at x=250 and x=470, one in each gap between bands (y=94–114 and y=198–218), double-headed to show calls both ways.
- **Annotation (bold 12px orange `#d95926`, centered at y=280):** "swap a provider without touching the app — that is the glue's promise".
- **Caption (11px `#444`, bottom right, y=296):** "layers illustrative".

## Will the Glue Last? The Durability Debate

**Tags:** `common mistake` (red), `durability debate` (orange)

- **The debate** — is glue a lasting layer like web frameworks, or a patch over missing model features?
- **Absorption** — model APIs keep absorbing glue jobs: structured output, tool calling, file search moved in
- **The count** — of six classic glue jobs from 2023, roughly 3 now ship inside the major model APIs
- **What remains** — multi-step workflows, your private data plumbing, and switching between providers
- **The mistake** — treating the framework as the skill; the durable skill is knowing the five jobs themselves

*Example (italic):* The bookstore's JSON-parsing glue became a single API flag a year later — that code was deleted, but knowing why the bot needs retrieval never expired.

**Common mistake:** Learning a framework's syntax instead of the jobs it wires together — the syntax churns every year; the jobs (load, split, index, retrieve, ask) do not.

### Visualization (canvas `c4`, 720×300)

Two-column ledger: six classic glue jobs shown as pills, three still living in the framework on the left and three already absorbed into model APIs on the right, split by a dashed divider.

- **Title (bold 15px, `#1a5276`, top center):** "The Durability Ledger: 3 of 6 Glue Jobs Absorbed So Far".
- **Column headers (bold 13px, y=70):** "still glue (framework)" in `#2a78d6` centered at x=200; "absorbed into model APIs" in `#d95926` centered at x=520.
- **Divider:** vertical dashed (dash 4/3) 1.5px `#6b7280` line at x=360 from y=58 to y=232.
- **Left pills (rounded rects 240 wide × 34 tall, left edge x=80, tops y = `[92, 138, 184]`):** fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, centered 12px `#1a5276` labels: "prompt templates & memory", "multi-step workflows", "provider switching".
- **Right pills (same size, left edge x=400, tops y = `[92, 138, 184]`):** fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, centered 12px `#d95926` labels: "structured JSON output", "tool-calling loop", "file search / retrieval".
- **Migration arrows:** three short 2px `#d95926` arrows with arrowheads crossing the divider left-to-right, from x=344 to x=396, at y = `[109, 155, 201]` (aligned to the right pills' centers), showing the absorbed jobs having moved across.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=258):** "the debate: do the last three follow, or is glue permanent?".
- **Caption (12px `#444`, bottom right, y=290):** "job ledger illustrative — status as of the page's writing".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box positions, bar values, and pill lists are the hardcoded arrays above (no randomness); the line-count bars must sum to 128 and 22 to match the text, and the ledger must show exactly 3 pills per column to match "3 of 6".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
