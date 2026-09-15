# VS Code

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** VS Code

**Subtitle:** In 2015 Microsoft launched a free editor into a crowded field — and won not by being the best editor, but by being the best platform for others to build on

## Launching Into a Crowded Field

**Tags:** `core idea` (blue), `platform strategy` (green), `2015` (orange)

- **The field** — in 2015 Sublime Text, Atom, Vim, and full IDEs already owned every developer's desk
- **The launch** — Microsoft ships VS Code free at Build 2015, then open-sources the core that November
- **The bet** — an editor, not an IDE: fast startup, small core, nothing heavyweight built in
- **Electron** — one web-tech codebase runs on Mac, Windows, and Linux; memory overhead is the price
- **Extension-first** — language support, themes, and debuggers live in extensions, never in the core

*Example (italic):* Out of the box VS Code barely knows Python; install one marketplace extension and it gains completion, linting, and debugging — the core never changed.

**Key point:** The core stays deliberately small and everything else is an extension — VS Code is less a product than a platform that tens of thousands of extensions are built on.

### Visualization (canvas `c1`, 720×300)

Diagram: one small core box on the left fanning out to a grid of extension boxes, showing that capability lives in the marketplace, not the core.

- **Title (bold 15px, `#1a5276`, top center):** "A Small Core, a Marketplace of Everything Else".
- **Core box:** rounded box at x=40, y=110, 160×80, fill `rgba(26,82,118,0.15)`, 2px `#1a5276` border, bold 13px `#1a5276` label "VS Code core" over 11px `#444` "editing, UI, extension API".
- **Fan lines:** 2px `#6b7280` lines from the core's right edge (x=200, y=150) to the left edge of each extension box.
- **Extension grid:** 12 rounded boxes 125×42, columns at x = 300 / 440 / 580, rows at y = 42 / 96 / 150 / 204; fill `rgba(42,120,214,0.15)`, 1.5px `#2a78d6` border, 12px `#2c3e50` labels: "Python", "C/C++", "GitLens", "Prettier", "Docker", "Remote-SSH", "themes", "debuggers", "linters", "Jupyter", "ESLint", "spell check".
- **Annotation (bold 13px green `#008300`, centered near y=278):** "plus tens of thousands more in the marketplace".
- **Caption (12px `#444`, bottom left):** "extension names representative".

## The Protocol Masterstroke: N×M Becomes N+M

**Tags:** `worked example` (blue), `LSP` (green), `network effects` (orange)

- **The old cost** — every editor needed its own plugin per language: 10 editors × 20 languages = 200 plugins
- **The protocol** — LSP (2016): a language "server" speaks one JSON protocol; each editor speaks it once
- **The new cost** — 10 editor clients + 20 language servers = 30 pieces of work instead of 200
- **Hand-check** — add an 11th editor: the old way costs 20 new plugins, the LSP way costs exactly 1 client
- **The masterstroke** — a server built for ANY editor (a Rust server aimed at Vim users) helps VS Code too

*Example (italic):* One team writes one Rust language server, and VS Code, Vim, Emacs, and Sublime all gain Rust smarts — nobody wrote four separate plugins.

**Key point:** By publishing the protocol instead of keeping it, Microsoft turned rival editors' communities into VS Code's supply chain — every new language server anywhere makes VS Code better.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: integrations required with 10 editors as the language count grows, per-editor plugins (N×M) vs LSP (N+M).

- **Title (bold 15px, `#1a5276`, top center):** "Language Support for 10 Editors: 200 Integrations Becomes 30".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = four groups labeled "5 langs" / "10 langs" / "15 langs" / "20 langs" (12px `#444`); y = integrations 0 to 200, gridlines `#e5e9ef` at 50/100/150 with 12px labels.
- **Per-editor bars (left of each pair):** orange `#d95926`, fill `rgba(217,89,38,0.35)`, 2px edge, 40px wide, heights for values `[50, 100, 150, 200]`.
- **LSP bars (right of each pair):** green `#008300`, fill `rgba(0,131,0,0.30)`, 2px edge, 40px wide, heights for values `[15, 20, 25, 30]`.
- **Value labels:** bold 12px in each bar's color, centered above each bar top.
- **Legend (12px, top left inside plot):** orange swatch "one plugin per editor per language (N×M)", green swatch "LSP: clients + servers (N+M)".
- **Annotation (bold 13px green `#008300`, right side near y=70):** "every new server helps every editor".
- **Caption (12px `#444`, bottom right):** "counts exact for the N×M vs N+M arithmetic".

## The Flywheel and the Scoreboard

**Tags:** `where it's used` (blue), `survey data` (green), `ecosystem` (orange)

- **The flywheel** — more users attract extension authors, whose extensions attract more users
- **The scoreboard** — Stack Overflow's annual survey has shown VS Code the most-used editor for years
- **The real prize** — Microsoft gave the editor away to win the ecosystem: GitHub, cloud dev, vscode.dev
- **Remote development** — the editor UI runs locally while the workload sits in a container, SSH box, or cloud VM
- **Compounding** — vscode.dev and cloud dev environments put the same editor in a browser, no install at all

*Example (italic):* A developer opens a repo in the browser, gets a full VS Code with the project's recommended extensions, and edits code running in a cloud container.

**Key point:** The strategic reading: the free editor is the front door to a paid ecosystem — in developer tools, the platform beats the product.

### Visualization (canvas `c3`, 720×300)

Line chart of VS Code's share of respondents in Stack Overflow's annual developer survey, from launch-era obscurity to sustained dominance.

- **Title (bold 15px, `#1a5276`, top center):** "Stack Overflow Survey: Share of Developers Using VS Code".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 2016 to 2023 with 12px `#444` tick labels at 2016/2017/2018/2019/2021/2022/2023; y = 0 to 80% with gridlines `#e5e9ef` at 20/40/60 and 12px "%" labels.
- **VS Code line:** blue `#2a78d6` 3px line with 4px radius dots through years `[2016, 2017, 2018, 2019, 2021, 2022, 2023]`, shares `[7, 24, 35, 51, 71, 74, 74]` (2020 omitted — no comparable question that year).
- **Halfway marker:** horizontal dashed `#6b7280` (dash 4/3) line at y = 50%, 12px `#6b7280` label "half of all respondents" at its left.
- **Annotation (bold 13px blue `#2a78d6`, near the 2019 point):** "crosses 50% four years after launch".
- **Caption (12px `#444`, bottom right):** "shares approximate, from published survey results".

## Free Is Not the Same as Flawless — or Fully Open

**Tags:** `common mistake` (red), `trade-offs` (orange)

- **The mistake** — reading "most used" as "best product"; the platform, not the polish, did the winning
- **Electron tax** — a web runtime per window: hundreds of MB where a native editor uses tens
- **Extension roulette** — tens of thousands of extensions means quality varies wildly; no one vets them all
- **Free vs open** — the MIT-licensed Code-OSS source is open; the branded download adds telemetry and proprietary bits
- **The fork tell** — VSCodium exists precisely to ship the open core without the telemetry

*Example (italic):* A small text file that costs a terminal editor 20 MB can cost VS Code with ten extensions 600 MB — the documented, accepted price of one cross-platform codebase (figures illustrative).

**Common mistake:** Treating the criticisms as oversights. The memory cost was a deliberate trade for one codebase, and "open source with a proprietary build" is a documented nuance — judge the strategy by what it bought, not by pretending it was free.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: memory to open one small text file across editors, making the Electron overhead visible.

- **Title (bold 15px, `#1a5276`, top center):** "The Electron Tax: Memory to Open One Small Text File".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 (scale: 600 MB = 440px).
- **Rows (bars 14px tall, top to bottom at y = 80, 130, 180, 230), each with a left-aligned 12px `#444` label at x=20 and an 11px value label at the bar end:**
  - "terminal editor (Vim)": green `#008300` bar width 15 — "20 MB"
  - "native GUI editor": aqua `#199e70` bar width 44 — "60 MB"
  - "VS Code, no extensions": orange `#d95926` bar width 220 — "300 MB"
  - "VS Code + 10 extensions": red `#e74c3c` bar width 440 — "600 MB"
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=262):** "the accepted price of one web-tech codebase".
- **Caption (12px `#444`, bottom right):** "memory figures illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the N×M vs N+M integration counts are exact arithmetic for 10 editors; survey shares are approximate values from Stack Overflow's published annual results (labeled as such); the memory figures are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
