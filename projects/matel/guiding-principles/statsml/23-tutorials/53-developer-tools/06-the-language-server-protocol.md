# The Language Server Protocol

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Language Server Protocol

**Subtitle:** One server per language, any editor — Microsoft's open protocol (published 2016) lets every editor share the same language brains instead of rebuilding them

## Ten Editors, Twenty Languages, Two Hundred Plugins

**Tags:** `core idea` (blue), `N×M problem` (green), `Microsoft 2016` (orange)

- **The matrix** — 10 editors × 20 languages means 200 separate language-support plugins to build
- **Each cell bespoke** — VS Code's Python plugin shares nothing with vim's Python plugin or Emacs's
- **Most cells empty or bad** — small editors get a few languages; new languages get a few editors
- **The 2016 move** — Microsoft published LSP: pull language smarts out into a standalone server
- **One per language** — the server owns parsing, completion, diagnostics, go-to-definition, rename

*Example (italic):* In the pre-LSP world, a Go plugin for vim, another for Emacs, another for Sublime — three teams re-implementing the same parser, and seventeen editors with no Go support at all.

**Key point:** LSP splits the work in two: one LANGUAGE SERVER per language holds all the intelligence, and any editor acts as a CLIENT speaking a standard JSON-RPC protocol to it.

### Visualization (canvas `c1`, 720×300)

Matrix grid of editors (rows) × languages (columns), each cell one bespoke plugin: a few good, a few poor, most empty.

- **Title (bold 15px, `#1a5276`, top center):** "10 Editors × 20 Languages = 200 Cells, Each Built by Hand".
- **Grid geometry:** origin x=110, y=62; 20 columns at 26px pitch (cell 22px wide), 10 rows at 18px pitch (cell 14px tall); grid footprint 520×180.
- **Row labels (12px `#444`, right-aligned at x=104):** `["VS Code","Vim","Emacs","Sublime","Atom","Eclipse","Kate","Geany","Helix","Nova"]`.
- **Column header (12px `#6b7280`, above grid at x=110):** "20 languages →".
- **Cell fills, hardcoded per row (leftmost cells of each row):** good plugins green `rgba(0,131,0,0.55)` with counts `[6, 5, 4, 3, 3, 2, 2, 1, 1, 1]` (28 total); next cells poor plugins orange `rgba(217,89,38,0.55)` with counts `[4, 4, 3, 3, 2, 2, 2, 2, 1, 1]` (24 total); remaining 148 cells empty, 1px `#e5e9ef` outline only.
- **Legend (12px, below grid at y=270):** green swatch "good plugin (28)", orange swatch "poor plugin (24)", outline swatch "no support (148)".
- **Annotation (bold 13px red `#e74c3c`, over the empty right region near x=430, y=150):** "most cells empty or bad".
- **Caption (12px `#444`, bottom right):** "cell placement illustrative, 200-cell total exact".

## What the Editor Actually Says to the Server

**Tags:** `worked example` (blue), `JSON-RPC` (green)

- **You type** — you edit line 12 of `report.py` and pause after typing `data.par`
- **Editor reports** — it sends `textDocument/didChange` with the new contents of line 12
- **Editor asks** — it sends `textDocument/completion` for line 12, column 8
- **Server answers** — a JSON list of items: `parse_csv`, `parse_json`, `parse_xml`
- **Editor stays dumb** — it just draws the popup; it knows nothing about Python at all

*Example (italic):* Open a Go file and the editor sends the exact same two messages — only the server process on the other end of the pipe changes.

**Key point:** The entire interface is a conversation of small JSON-RPC messages — "document changed", "what completes here?", "where is this defined?" — so language knowledge never leaks into the editor.

### Visualization (canvas `c2`, 720×300)

Sequence diagram: editor and Python language server as two lifelines exchanging four JSON-RPC messages.

- **Title (bold 15px, `#1a5276`, top center):** "One Completion Request, Message by Message".
- **Lifelines:** vertical 2px `#999` lines at x=180 (editor) and x=540 (server), from y=70 to y=272; header boxes 150×30 at top, fills `rgba(42,120,214,0.15)` labeled "editor (client)" and `rgba(0,131,0,0.12)` labeled "Python language server", 12px `#2c3e50` bold text.
- **Arrows (3px, arrowheads 8px), labels 12px above each arrow:**
  - y=115, editor→server, blue `#2a78d6`: "textDocument/didChange — line 12 now `data.par`"
  - y=160, editor→server, blue `#2a78d6`: "textDocument/completion — line 12, col 8"
  - y=205, server→editor, green `#008300`: "items: parse_csv, parse_json, parse_xml"
  - y=250, editor→server, blue `#2a78d6`: "textDocument/definition — parse_csv"
- **Annotation (bold 13px violet `#4a3aa7`, left margin near x=30, y=285, left-aligned):** "the editor never parses Python — it only draws answers".
- **Caption (12px `#444`, bottom right):** "message names are the real LSP methods".

## Two Hundred Integrations Become Thirty

**Tags:** `where it's used` (blue), `N+M` (green), `ecosystem` (orange)

- **The arithmetic (exact)** — 10 editors × 20 languages = 200 bespoke plugins; with LSP it is 10 + 20 = 30
- **Each side once** — every editor implements the protocol once; every language ships one server
- **Small editors won** — vim, neovim, and helix gained IDE-grade completion and rename via LSP clients
- **New languages won** — ship a server on day one and the language works in every editor at once
- **The pattern spread** — the Debug Adapter Protocol repeats the same move for debuggers

*Example (italic):* rust-analyzer is one codebase, yet it is the Rust intelligence inside VS Code, vim, Emacs, and helix alike — one M-side cell serving every N.

**Key point:** Defining the INTERFACE, not winning the editor market, is what collapsed the problem — bespoke effort grows as N×M, protocol effort grows as N+M.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: bespoke integration counts vs LSP integration counts at three ecosystem sizes.

- **Title (bold 15px, `#1a5276`, top center):** "Bespoke Grows as N×M, LSP Grows as N+M".
- **Axis:** bars start at x=250, extend right on one linear scale (0.55 px per integration, max width 440); 2px `#999` vertical baseline at x=250.
- **Rows (bar height 16px, left-aligned 12px `#444` labels at x=20), top to bottom:**
  - y=70: "5 editors × 10 langs — bespoke 50": blue `#2a78d6` fill `rgba(42,120,214,0.30)`, width 28
  - y=95: "5 + 10 — LSP 15": solid green `#008300`, width 8
  - y=135: "10 editors × 20 langs — bespoke 200": blue fill, width 110
  - y=160: "10 + 20 — LSP 30": solid green, width 17
  - y=200: "20 editors × 40 langs — bespoke 800": red `#e74c3c` fill `rgba(231,76,60,0.25)`, width 440
  - y=225: "20 + 40 — LSP 60": solid green, width 33
- **Value labels:** 11px `#444` counts at each bar's right end (50, 15, 200, 30, 800, 60).
- **Annotation (bold 13px green `#008300`, near x=340, y=265):** "the green bars barely grow — that is the whole trick".
- **Caption (12px `#444`, bottom right):** "one linear scale — bar widths proportional to counts".

## Not a Full IDE in a Box

**Tags:** `common mistake` (red), `limits` (orange)

- **The confusion** — assuming a language server equals a full IDE's analysis engine
- **Lowest common denominator** — the protocol standardizes what every editor can use, not the maximum
- **Beyond the messages** — deep project-wide refactorings and framework-aware inspections exceed it
- **Why JetBrains stays custom** — its engine keeps whole-project semantic models LSP cannot express
- **Quality varies** — one server per language means that community's server IS your experience everywhere

*Example (italic):* A symbol rename works fine over LSP; "extract this class hierarchy into a new module and update every config that wires it" typically does not.

**Common mistake:** Treating LSP as a ceiling-free standard. It is a shared floor — a deliberately common denominator — and languages with a weak server feel weak in every editor at once.

### Visualization (canvas `c4`, 720×300)

Check/cross table: six capabilities rated for "LSP standard" vs "full IDE engine".

- **Title (bold 15px, `#1a5276`, top center):** "The Shared Floor: What the Protocol Covers".
- **Layout:** feature labels 12px `#2c3e50` left-aligned at x=30; two column headers bold 12px `#1a5276` at x=430 ("LSP standard") and x=590 ("full IDE engine"), y=68; light `#e5e9ef` 1px row separators.
- **Rows (y = 100, 130, 160, 190, 220, 250), marks bold 16px centered on the column x positions — green `#008300` "✓" or red `#e74c3c` "✗":**
  - "completion": ✓ / ✓
  - "diagnostics": ✓ / ✓
  - "go-to-definition": ✓ / ✓
  - "symbol rename": ✓ / ✓
  - "project-wide structural refactor": ✗ / ✓
  - "framework-aware inspections": ✗ / ✓
- **Annotation (bold 13px orange `#d95926`, bottom center near y=285):** "the bottom two rows are why JetBrains keeps its own engine".
- **Caption (12px `#444`, bottom right):** "capability split simplified".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the N×M vs N+M integration counts (50/15, 200/30, 800/60) are exact arithmetic; the c1 good/poor cell counts and their placement are invented and labeled illustrative; the c2 method names (`textDocument/didChange`, `textDocument/completion`, `textDocument/definition`) are real LSP methods.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
