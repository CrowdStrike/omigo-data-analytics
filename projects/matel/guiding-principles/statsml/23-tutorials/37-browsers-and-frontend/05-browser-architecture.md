# Browser Architecture

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Browser Architecture

**Subtitle:** A modern browser is not one program but a team of processes — one per tab — so a crashing page takes out its own tab and nothing else

## One Tab Crashes, Nine Keep Playing

**Tags:** `core idea` (blue), `crash isolation` (green), `one process per tab` (orange)

- **The setup** — you have ten tabs open: mail, music, a shared doc, and seven news pages
- **The crash** — tab 7 runs a buggy page whose script eats memory until its process dies
- **The old way** — in a single-process browser, one bad page took the whole window down
- **The new way** — each tab gets its own renderer process with its own private memory
- **The result** — tab 7 shows a sad-tab error page; the song playing in tab 2 never skips

*Example (italic):* A runaway script kills tab 7 at 2:14pm; the other nine tabs, music still playing in tab 2, run on as if nothing happened.

**Key point:** A modern browser is many cooperating processes — when one renderer crashes, it takes down its own tab and nothing else.

### Visualization (canvas `c1`, 720×300)

Tab-strip diagram: ten rounded tab boxes in two rows of five, each labeled with its process; tab 7 crashed (red), the other nine alive (green).

- **Title (bold 15px, `#1a5276`, top center):** "Ten Tabs, Ten Renderer Processes: One Dies, Nine Live".
- **Layout:** two rows of five boxes; row 1 tops at y=70, row 2 tops at y=160; box x positions `[30, 168, 306, 444, 582]`, each box 110px wide, 52px tall, 8px radius.
- **Alive boxes (tabs 1–6, 8–10):** fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px `#2c3e50` two-line label: "tab N" over "process N"; tab 2 additionally tagged with a bold 11px aqua `#199e70` "♪ playing" under its label.
- **Crashed box (tab 7, row 2, second position):** fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, bold 12px `#e74c3c` label "tab 7" over "process died ✗".
- **Annotation (bold 13px green `#008300`, centered at y=255):** "1 crash, 9 tabs untouched — each tab is its own process".
- **Caption (12px `#444`, bottom right):** "tab lineup illustrative".

## Who Does What: The Four Kinds of Processes

**Tags:** `worked example` (blue), `process roles` (green)

- **Browser process** — the boss: owns the address bar, tab strip, bookmarks, and disk — 180 MB
- **Renderer process** — one per tab here: parses HTML, runs JavaScript, lays out pages — 90 MB each
- **GPU process** — one for the whole browser: turns every tab's finished layout into pixels — 140 MB
- **Network service** — fetches every URL for everyone, so no renderer touches the network — 60 MB
- **The bill** — 180 + 140 + 60 + 10 × 90 = 1,280 MB with ten tabs open

*Example (italic):* Closing three news tabs frees 3 × 90 = 270 MB, dropping the total from 1,280 MB to 1,010 MB — the shared processes stay put.

**Key point:** Each process owns one job; renderers scale with tabs, so open tabs — not the browser core — set the memory bill.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of memory by process type for the ten-tab session: browser, GPU, network service, and the ten renderers combined.

- **Title (bold 15px, `#1a5276`, top center):** "Where 1,280 MB Goes with Ten Tabs Open".
- **Axis:** vertical 2px `#999` baseline at x=210, bars extend right, max bar width 440 (scale: 900 MB → 440px, ~0.489 px/MB).
- **Rows (bar tops at y = 60, 105, 150, 195), each with a right-aligned 12px `#444` label ending at x=200:**
  - "browser process": blue `#2a78d6` bar width 88, 12px `#444` value label "180 MB" at bar end
  - "GPU process": violet `#4a3aa7` bar width 68, label "140 MB"
  - "network service": aqua `#199e70` bar width 29, label "60 MB"
  - "10 renderers (10 × 90)": orange `#d95926` bar width 440, label "900 MB"
- **Bar style:** 26px tall, fills at 0.85 alpha of the row color, 1px solid border in the row color.
- **Annotation (bold 13px orange `#d95926`, near x=300, y=250):** "renderers are 900 of 1,280 MB — tabs set the bill".
- **Caption (12px `#444`, bottom right):** "memory sizes illustrative".

## Why Browsers Eat RAM — and What the RAM Buys

**Tags:** `where it's used` (blue), `sandboxing` (green), `security` (red)

- **The sandbox** — each renderer runs locked in a box: no direct access to files, disk, or camera
- **The broker** — anything sensitive must go through the browser process, which checks first
- **Site isolation** — since the 2018 Spectre attacks, browsers split different sites into different processes
- **The trade** — a copy of the page engine in every process is why ten tabs cost over a gigabyte
- **The payoff** — a hacked page in one process still cannot read your bank tab's memory

*Example (italic):* A malicious ad that breaks out of JavaScript still lands inside a sandboxed renderer that holds nothing but its own page.

**Key point:** The RAM cost buys two guarantees: a crash stays inside its tab, and a compromised page stays inside its cage.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: a renderer trying to reach the disk directly (blocked at the sandbox wall) vs going through the browser-process broker (checked, then allowed).

- **Title (bold 15px, `#1a5276`, top center):** "The Sandbox: Renderers Ask, the Browser Process Decides".
- **Sandbox wall:** vertical dashed `#6b7280` (dash 5/4) 3px line at x=390 from y=55 to y=250, 12px `#6b7280` label "sandbox wall" rotated or placed at its top.
- **Row 1 (boxes at y=80), label 12px `#444` at x=20:** "direct grab"; orange `#d95926` rounded box at x=140 labeled "renderer: read passwords file" (12px), 3px `#e74c3c` arrow stopping AT the wall with a bold 14px red `#e74c3c` "✗ blocked" at x=400.
- **Row 2 (boxes at y=185), label:** "via broker"; blue `#2a78d6` box at x=140 labeled "renderer: save download?", 3px `#2c3e50` arrow to a green `#008300` box at x=420 labeled "browser process checks", then arrow to a green box at x=600 labeled "disk ✓".
- **Box style:** 150–170px wide, 42px tall, 8px radius, fills `rgba(217,89,38,0.12)` / `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "hacked renderer, empty cage — nothing sensitive lives in the tab".

## The Unit Is the Site, Not Exactly the Tab

**Tags:** `common mistake` (red), `process counting` (orange)

- **The slogan** — "one process per tab" is the mental model; the real boundary is the site
- **Sharing** — tabs a page opened onto the same site can share a single renderer process
- **Splitting** — one tab with a cross-site iframe uses two renderer processes, not one
- **Task-manager shock** — 10 tabs can show 14 processes: 11 renderers + browser + GPU + network
- **Threads are not this** — one multi-threaded process would still crash all its tabs at once

*Example (italic):* One bank tab embedding a cross-site ad frame runs two renderers: the bank's pages in one process, the ad in another.

**Common mistake:** Judging bloat by process count. Browsers split by site for security and may share within a site to save memory — so process count ≠ tab count by design.

### Visualization (canvas `c4`, 720×300)

Two-row mapping diagram: three same-site tabs collapsing into one shared renderer vs one tab with a cross-site iframe splitting into two renderers.

- **Title (bold 15px, `#1a5276`, top center):** "Process Boundaries Follow Sites, Not Tabs".
- **Row 1 (y=75), label 12px `#444` at x=20:** "3 linked tabs, same site"; three blue `#2a78d6` rounded boxes at x = 150, 260, 370 (100px wide, 40px tall) labeled "docs tab 1/2/3" (12px), three thin 2px `#6b7280` arrows converging to one green `#008300` box at x=540 (150px wide) labeled "1 shared renderer".
- **Row 2 (y=185), label:** "1 tab, cross-site iframe"; one blue box at x=150 (130px wide) labeled "bank tab + ad frame", two 2px `#6b7280` arrows fanning out to a green box at x=420 labeled "bank renderer" and a magenta `#d55181` box at x=580 labeled "ad renderer".
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(213,81,129,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, centered near y=262):** "this window: 10 tabs, 14 processes — and that is by design".
- **Caption (12px `#444`, bottom right):** "tab/process counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all numbers are the hardcoded literals above (no randomness); memory sizes (180 / 140 / 60 / 90 per renderer, total 1,280 MB) and tab/process counts (10 tabs, 14 processes) are invented and labeled illustrative; the multi-process split (browser / renderer / GPU / network service), sandboxing via a browser-process broker, and post-2018-Spectre site isolation are documented browser-architecture facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
