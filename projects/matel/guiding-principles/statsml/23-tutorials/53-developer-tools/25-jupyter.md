# Jupyter

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Jupyter

**Subtitle:** A Jupyter notebook interleaves code, its output, and prose in one live document — the kernel remembers every variable you create, which is both its power and its famous trap

## One Document, Three Kinds of Cells

**Tags:** `core idea` (blue), `notebook model` (green), `kernel` (orange)

- **The document** — a notebook is a single file made of cells stacked top to bottom
- **Code cells** — each holds a snippet you run on its own; the result renders right beneath it
- **Markdown cells** — prose, headings, and notes live between the code, in the same file
- **The kernel** — a live process behind the page holds every variable from every cell you have run
- **The counter** — each run stamps the cell `In [n]`, recording the order things actually executed

*Example (italic):* An analyst opens `sales.csv` in a notebook: a markdown heading, one cell that loads the file, and the row count printed directly under it — code, output, and prose in one scrollable page.

**Key point:** A notebook is code, rendered output, and narrative in one document, backed by a kernel that keeps all state alive between cell runs.

### Visualization (canvas `c1`, 720×300)

Schematic of a notebook page beside its kernel: three stacked cells on the left, a kernel memory box on the right, arrows showing runs feeding variables into the kernel.

- **Title (bold 15px, `#1a5276`, top center):** "A Notebook Is Cells on a Page; the Kernel Holds the State".
- **Cell stack (left):** three rounded boxes at x=30, width 380, height 52, at y = 60, 128, 196; 8px radius, 1px `#e0e0e0` border.
  - Box 1 fill `rgba(201,133,0,0.10)`, 12px `#2c3e50` text "Markdown:  # Sales exploration — June review", small 11px `#c98500` corner label "prose".
  - Box 2 fill `rgba(42,120,214,0.12)`, 12px monospace text "In [1]:  df = pd.read_csv('sales.csv')", beneath it 12px `#008300` output line "Out: 1,200 rows x 5 cols".
  - Box 3 fill `rgba(42,120,214,0.12)`, 12px monospace text "In [2]:  df.amount.mean()", output line "Out: 42.50".
- **Kernel box (right):** rounded box at x=490, y=95, width 200, height 130, fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; bold 13px `#4a3aa7` header "kernel memory"; 12px monospace `#2c3e50` lines "df -> 1,200 rows" and "mean -> 42.50".
- **Arrows:** 2px `#6b7280` arrows from box 2 and box 3 right edges to the kernel box left edge.
- **Annotation (bold 12px violet `#4a3aa7`, below kernel box at y=255):** "close the tab, the page stays; kill the kernel, the variables vanish".
- **Caption (12px `#444`, bottom right):** "row counts and mean illustrative".

## Exploring a Sales CSV Cell by Cell

**Tags:** `worked example` (blue), `exploratory analysis` (green)

- **Load** — cell 1 reads `sales.csv`: 1,200 order rows, columns id, month, amount, status
- **First look** — cell 2 prints the mean order value: $42.50 across all 1,200 rows
- **Clean** — cell 3 drops 30 refund rows, leaving 1,170 orders; the mean rises to $43.10
- **Hand-check** — 1,170 orders x $43.10 = $50,427 of revenue, matching the six monthly bars
- **Chart** — cell 4 plots revenue by month right in the page; June peaks at $9,577

*Example (italic):* Four cells, run top to bottom: load 1,200 rows, see mean $42.50, drop 30 refunds to get 1,170 rows at $43.10, then plot the monthly climb from $7,050 in January to $9,577 in June.

**Key point:** The notebook records the whole exploration — every step, its output, and the notes in between — so a colleague can reread the analysis exactly as it happened.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart of monthly revenue from the cleaned sales data, the same chart the analyst's cell 4 would render.

- **Title (bold 15px, `#1a5276`, top center):** "Cell 4 Output: Revenue by Month, 1,170 Cleaned Orders".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = revenue $0 to $10,000 with gridlines `#e5e9ef` at 2,500 / 5,000 / 7,500 and 12px `#444` labels "$2.5k" / "$5k" / "$7.5k"; x = months Jan–Jun, 12px `#444` labels centered under bars.
- **Bars:** six bars, width 62, gap 38, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` top edge; heights from revenue `[7050, 7820, 8140, 8610, 9230, 9577]` dollars.
- **Value labels:** 11px `#2c3e50` above each bar: "7,050", "7,820", "8,140", "8,610", "9,230", "9,577".
- **June highlight:** June bar fill `rgba(0,131,0,0.30)` with 2px `#008300` edge.
- **Annotation (bold 13px green `#008300`, upper left near x=90, y=70):** "six months sum to $50,427 = 1,170 x $43.10 (exact)".
- **Caption (12px `#444`, bottom right):** "order counts and amounts illustrative; the sum is exact arithmetic".

## Why Notebooks Won Exploratory Analysis

**Tags:** `where it's used` (blue), `history` (green), `IPython` (orange)

- **The roots** — Jupyter grew out of IPython, an enhanced interactive Python shell started in 2001
- **The notebook** — the IPython Notebook added the browser document interface in 2011
- **The split** — in 2014 the language-agnostic parts became Project Jupyter (Julia, Python, R)
- **The fit** — exploration is try, look, adjust; a notebook keeps each attempt and its output visible
- **The reach** — the standard medium for data analysis, teaching, and sharing results with prose attached

*Example (italic):* An analyst hands a reviewer one `.ipynb` file: the reviewer scrolls through the loading step, the cleaning decision, and the final chart without running anything.

**Key point:** Notebooks dominate exploratory data analysis because the artifact of the work — code plus outputs plus reasoning — is the report itself.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline of the publicly documented milestones from IPython shell to Project Jupyter.

- **Title (bold 15px, `#1a5276`, top center):** "From a Better Python Shell to a Language-Agnostic Notebook".
- **Timeline spine:** 3px `#1a5276` horizontal line from x=70 to x=650 at y=160.
- **Milestones (circles radius 7 on the spine, label above or below alternating):**
  - x=110, fill `#2a78d6`: bold 13px `#2a78d6` "2001" above, 12px `#444` "IPython — enhanced interactive shell" below.
  - x=290, fill `#c98500`: bold 13px `#c98500` "2011" below, 12px `#444` "IPython Notebook — cells in the browser" above.
  - x=470, fill `#008300`: bold 13px `#008300` "2014" above, 12px `#444` "Project Jupyter — kernels for many languages" below.
  - x=620, fill `#4a3aa7`: bold 13px `#4a3aa7` "today" below, 12px `#444` "default tool for exploratory analysis" above.
- **Name note (12px `#6b7280`, centered at y=40):** "Jupyter nods to Julia, Python, and R — the first three kernel languages".
- **Annotation (bold 12px green `#008300`, near x=470, y=250):** "the notebook idea outgrew Python".
- **Caption (12px `#444`, bottom right):** "milestone years exact (publicly documented)".

## The Notebook That Lies: Hidden Kernel State

**Tags:** `common mistake` (red), `hidden state` (orange)

- **The freedom** — cells can run in any order, and edited cells re-run without re-running the rest
- **The residue** — the kernel keeps variables from runs whose code was since edited or deleted
- **The lie** — the page reads top to bottom, but `In [7]` above `In [6]` says it did not run that way
- **The trap** — the analyst deletes the cell that made `clean`, yet later cells still work — for now
- **The test** — Restart & Run All wipes the kernel and replays top to bottom; the truth comes out

*Example (italic):* The sales notebook shows every cell green, but on Restart & Run All, the mean cell dies with `NameError: name 'clean' is not defined` — the cleaning cell was deleted last Tuesday and only the kernel remembered.

**Common mistake:** Trusting a notebook because every cell shows output. Out-of-order execution leaves hidden state, so a notebook can "work" today and fail on a fresh run — Restart & Run All before sharing is the honesty check.

### Visualization (canvas `c4`, 720×300)

Two-column comparison: the same three cells in the live session (all green, counters out of order) vs after Restart & Run All (third cell fails).

- **Title (bold 15px, `#1a5276`, top center):** "Same Notebook, Two Truths: Live Session vs Restart & Run All".
- **Column headers (bold 13px at y=55):** `#2c3e50` "live session — looks fine" centered at x=185; `#e74c3c` "restart & run all — fails" centered at x=535.
- **Left column (x=40, width 290):** three rounded boxes height 48 at y = 75, 135, 195, fill `rgba(0,131,0,0.10)`, 1px `#008300` border, 12px monospace `#2c3e50` text:
  - "In [5]:  df = read_csv(...)  ✓"
  - "In [7]:  clean.amount.mean()  ✓" with 11px `#6b7280` note "counter 7 above 6 — ran out of order"
  - "In [6]:  plot(clean)  ✓"
- **Right column (x=390, width 290):** same three positions; boxes 1–2 fill `rgba(42,120,214,0.12)` with text "In [1]:  df = read_csv(...)  ✓" and "In [2]:  clean.amount.mean()"; box 2 and box 3 replaced state: box 2 fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, text "NameError: 'clean' not defined", bold 12px `#e74c3c` "✗" at its right edge; box 3 fill `rgba(107,114,128,0.10)` with 12px `#6b7280` text "never runs".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=360 from y=60 to y=250.
- **Annotation (bold 13px red `#e74c3c`, centered at y=275):** "the cell that defined 'clean' was deleted — only the kernel remembered it".
- **Caption (12px `#444`, bottom right):** "cell contents illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the sales example (1,200 rows, 30 refunds, means $42.50 / $43.10, monthly revenue `[7050, 7820, 8140, 8610, 9230, 9577]`) is invented and labeled illustrative; 1,170 × $43.10 = $50,427 and the bar sum are exact arithmetic; timeline years 2001 / 2011 / 2014 are publicly documented milestones (IPython, IPython Notebook, Project Jupyter split).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
