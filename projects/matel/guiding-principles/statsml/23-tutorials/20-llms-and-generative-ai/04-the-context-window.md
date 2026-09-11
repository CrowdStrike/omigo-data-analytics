# The Context Window

**Page type:** detail page (tutorial layout: `.card-section` blocks, each a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** The Context Window

**Subtitle:** A model can only look at a fixed amount of text at once — paste more and the oldest part silently falls out; the window is working memory, not knowledge

## A 400-Page Contract Meets a 200-Page Window

**Tags:** `core idea` (blue), `running example` (green)

- **The paste** — you drop a 400-page contract into the chat and start asking about it
- **The window** — this model can hold about 200 pages of text at once, no more
- **What falls out** — pages 1–200 are dropped; the model never sees them at all
- **No warning** — in chat apps nothing errors; the model answers from the half it kept
- **Working memory** — the window is what it is looking at now, not what it knows

*Example:* Ask about the payment terms on page 12 and the model answers from pages 201–400 — page 12 was never in front of it.

**Key point:** The context window is the model's working memory — everything it can look at for this one answer. Text past the limit is not skimmed or summarized; it is simply gone.

### Visualization (canvas `c1`, 720×300)

Horizontal page-strip diagram: the 400-page contract as one bar, with the context window bracketing the last 200 pages.

- **Title (bold 15px `#1a5276`, top center):** "400 Pages In, 200 Pages Seen".
- **Strip:** from x=60 to x=660, y=110, height 70; midpoint = page-200 boundary.
- **Dropped half (pages 1–200, left):** fill `#eceff3`, dashed mute `#6b7280` outline (dash 5/4); inside labels in mute — bold 13px "pages 1–200: dropped", 12px "the model never sees them".
- **Kept half (pages 201–400, right):** fill `rgba(42,120,214,0.25)`, solid blue `#2a78d6` 2px outline; inside labels in blue — bold 13px "pages 201–400: in the window", 12px "the only text the model reads".
- **Page ticks (12px `#444`, below strip):** "page 1", "page 200", "page 400".
- **Window bracket:** green `#008300` bracket above the kept half with bold 13px green label "context window ≈ 200 pages".
- **Page-12 marker:** red `#e74c3c` 2px tick dropping below the strip at the 12/400 position, with bold red 12px label "page 12: the payment terms you asked about".
- **Callout (bold 13px red, centered at y=50):** "the first half falls out silently — no error, no warning".

## Counting the Overflow by Hand

**Tags:** `worked example` (green), `rule of thumb` (blue)

- **Tokens** — models measure text in tokens, word-pieces of roughly 3/4 of a word each
- **One page** — about 500 tokens, so the contract is 400 × 500 = 200,000 tokens
- **The window** — this model holds 100,000 tokens, which is about 200 pages
- **The overflow** — 200,000 − 100,000 = 100,000 tokens, a full 200 pages, fall out
- **The answer counts too** — the reply shares the same window, so room is even tighter

*Example:* Your question (~100 tokens) plus a 2,000-token reply leave room for about 195 pages of contract, not the full 200.

**Key point:** Budget the window like a suitcase — instructions + document + question + answer must all fit inside the same 100,000 tokens.

### Visualization (canvas `c2`, 720×300)

Split panel: token arithmetic written out on the left, pasted-vs-capacity stacked bars on the right, dashed divider at x=360.

- **Title (bold 15px `#1a5276`, top center):** "200,000 Tokens Pasted, 100,000 Tokens of Room".
- **Left panel (text lines, left-aligned at x=48):** "1 page ≈ 500 tokens" (13px `#2c3e50`); "400 pages × 500 = 200,000 tokens" (bold 15px blue `#2a78d6`); "window = 100,000 tokens (≈ 200 pages)" (bold 15px green `#008300`); thin `#e5e9ef` rule; "overflow = 200,000 − 100,000" / "= 100,000 tokens = 200 pages lost" (bold 15px red `#e74c3c`, two lines); mute 12px footnote: "and the question (~100 tokens) plus the" / "reply (~2,000 tokens) fit in the same budget".
- **Right panel bars:** baseline y=250, chart height 175, y-scale max 210,000; bars 100px wide at x=420 and x=580.
  - "what you pasted / 200,000 tokens": stacked bar — bottom 100,000 in `rgba(42,120,214,0.55)` labeled "100,000 fit" (bold white), top 100,000 in `rgba(231,76,60,0.55)` labeled "100,000 fall out" (bold red); blue outline.
  - "window capacity / 100,000 tokens": 100,000 bar in `rgba(0,131,0,0.35)` with green 2px outline; small bold green "capacity" label at its top-right.
- **Capacity line:** green dashed line (dash 6/4) across both bars at the 100,000 level; thin gray `#999` baseline.
- **Callout (bold 13px red at (548, 60)):** "exactly half the contract cannot fit".

## Where This Bites in Real Work

**Tags:** `where it's used` (blue), `watch out` (orange)

- **Long inputs** — contracts, server logs, and call transcripts routinely exceed the window
- **Chat history** — a long conversation is context too; early turns fall out the same way
- **Silent wrong answers** — questions about the dropped half get confident guesses
- **The test** — 40 questions, 10 per quarter: kept quarters score 9/10, dropped ones 2/10
- **The fix** — retrieval: search the document first, paste only the relevant pages

*Example:* All 40 answers read equally fluent and confident — nothing in the wording marks the 2/10 quarters as guesses.

**Key point:** A model never says "that part fell out of my window" — accuracy quietly collapses for text it never saw, so always check that the source actually fit.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: answer accuracy by contract quarter, with dropped/kept brackets.

- **Title (bold 15px `#1a5276`, top center):** "Accuracy by Where the Answer Lives (10 questions per quarter)".
- **Data:** quarters `['pages 1–100', 'pages 101–200', 'pages 201–300', 'pages 301–400']` with correct counts `[2, 2, 9, 9]` out of 10.
- **Bars:** first two red `#e74c3c`, last two green `#008300`, alpha 0.65; plot from x=90 width 540, baseline y=232, chart height 160, y max 10, bar width 96; bold colored "N/10" labels above bars, 12px quarter labels below; thin gray `#999` baseline.
- **Brackets below labels:** red bracket under bars 1-2 with bold red 12px caption "dropped half: confident guessing"; green bracket under bars 3-4 with bold green caption "inside the window: real reading".
- **Callout (bold 13px red, centered at y=54):** "every answer sounds equally confident — the wording never flags a guess".
- **Note (11px mute, right-aligned):** "illustrative counts".

## The Mix-Up: a Desk Is Not a Library

**Tags:** `common mistake` (red), `core idea` (blue)

- **Training knowledge** — facts learned during training: permanent, shared by every chat
- **The window** — text pasted now: temporary, gone when the conversation ends
- **New chat, empty desk** — yesterday's contract is not remembered today
- **Bigger ≠ smarter** — a larger window fits more text; it adds no new knowledge
- **Not an upload** — pasting the contract once does not "teach" the model the contract

*Example:* The model knows what a contract is from training, but it only sees your contract while it sits in the window.

**Key point (labeled "Common mistake:"):** treating the window as long-term memory. It is a desk, not a library — whatever the answer needs must be on the desk right now.

### Visualization (canvas `c4`, 720×300)

Two labeled comparison boxes (library vs desk) joined by an arrow.

- **Title (bold 15px `#1a5276`, top center):** "Two Memories: the Library and the Desk".
- **Left box (x=50, y=62, 290×168, violet `#4a3aa7` outline), title "LIBRARY — training knowledge", lines (12px `#333`):** "baked in during training" / "permanent, same for every chat" / "general facts: what a contract is" / "cannot be changed by pasting text".
- **Right box (x=390, y=62, 290×168, blue `#2a78d6` outline), title "DESK — context window", lines:** "whatever you paste right now" / "temporary: emptied per conversation" / "your facts: this contract’s page 12" / "limited: ≈ 200 pages, then it drops".
- **Boxes:** faint `rgba(0,0,0,0.02)` fill, 2px colored outline, bold 14px colored title.
- **Arrow between boxes:** orange `#d95926` horizontal arrow at y=146 with bold 11px labels "the answer" (above) and "needs both" (below).
- **Captions (bottom center):** bold 13px red `#e74c3c` "a new chat starts with a full library and a completely empty desk" at y=262; 12px mute "a bigger window is a bigger desk — the library does not grow" at y=284.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`, social-graph reference skeleton). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` with `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` row of colored pill spans first, then a `<ul>` of one-line bullets each opening with `<b>` (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, `<strong>` label).
- **Tag pill colors:** blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`. Pills: 0.72rem, weight 600, padding 2px 10px, radius 10px.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links, no cross-page links.
- **Canvas:** all charts 720×300 logical, scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (fixed 720×300 rect, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates). Hardcoded data arrays only — no `Math.random()`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
