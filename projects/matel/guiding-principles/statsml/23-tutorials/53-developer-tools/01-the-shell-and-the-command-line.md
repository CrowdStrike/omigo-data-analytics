# The Shell & the Command Line

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Shell & the Command Line

**Subtitle:** Text composed by pipes — a 50-year-old interface still wins because small tools that speak plain text can be snapped together into programs no one had to write

## Four Tiny Tools and a Coffee Shop's Orders

**Tags:** `core idea` (blue), `Unix philosophy` (green), `pipes` (orange)

- **The file** — `orders.txt` lists one drink per line: 12 orders from a coffee shop's morning rush
- **The question** — which drink sells most? No program exists for this, and none is needed
- **The pipe** — `sort orders.txt | uniq -c | sort -rn | head` chains four tools; each output feeds the next
- **The philosophy** — McIlroy at Bell Labs: small programs, one job each, joined by text streams
- **The plumbing** — every tool reads stdin, writes stdout, sends errors to stderr; `|` and `>` connect them
- **Still winning** — the same pipe idea runs today's servers, CI systems, and laptops unchanged

*Example (italic):* One line of shell answers "top seller?" without opening an editor, writing a loop, or naming a single variable.

**Key point:** The pipe is the deep idea — because every tool speaks plain text, any tool can feed any other, and composition replaces programming for one-off work.

### Visualization (canvas `c1`, 720×300)

Left-to-right pipeline flow diagram: the orders file passing through four tool boxes, with the line count and a data snippet shown at every stage.

- **Title (bold 15px, `#1a5276`, top center):** "One Pipe, Four Tools: Text Flows Left to Right".
- **Boxes (rounded 8px radius, 42px tall, top edge y=100):** "orders.txt" fill `rgba(107,114,128,0.15)` border 2px `#6b7280` at x=15 w=105; "sort" fill `rgba(42,120,214,0.15)` border `#2a78d6` at x=165 w=95; "uniq -c" fill `rgba(0,131,0,0.12)` border `#008300` at x=305 w=95; "sort -rn" fill `rgba(74,58,167,0.12)` border `#4a3aa7` at x=445 w=95; "head" fill `rgba(217,89,38,0.12)` border `#d95926` at x=585 w=95. Box labels bold 13px monospace `#2c3e50`, centered.
- **Arrows:** 3px `#6b7280` horizontal arrows between consecutive boxes at y=121, each with an 11px `#6b7280` label above (y=90): "12 raw", "12 sorted", "4 counted", "4 ranked".
- **Snippets (11px monospace `#444`, centered under each box, two lines starting y=165):** under orders.txt "latte / espresso / latte ..."; under sort "cappuccino / espresso / espresso ..."; under uniq -c "1 cappuccino / 3 espresso ..."; under sort -rn "6 latte / 3 espresso ..."; under head "6 latte (top line)".
- **Annotation (bold 13px `#1a5276`, centered near y=240):** "no tool knows the others exist — plain text is the only contract".
- **Caption (12px `#444`, bottom right):** "line counts exact for the 12-order file".

## Walking the Pipeline by Hand

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **The input** — 12 lines in arrival order: latte ×6, espresso ×3, mocha ×2, cappuccino ×1
- **Step 1, sort** — groups duplicates: cappuccino, then espresso ×3, then latte ×6, then mocha ×2
- **Step 2, uniq -c** — collapses each run into a count: `1 cappuccino`, `3 espresso`, `6 latte`, `2 mocha`
- **Step 3, sort -rn** — biggest count first: `6 latte`, `3 espresso`, `2 mocha`, `1 cappuccino`
- **Step 4, head** — keeps the top 10 lines; with only 4 lines here, all pass through
- **Hand-check** — 6 + 3 + 2 + 1 = 12, matching the input (`wc -l orders.txt` → 12)

*Example (italic):* The final screen reads "6 latte / 3 espresso / 2 mocha / 1 cappuccino" — latte is half the morning's sales.

**Key point:** `uniq -c` only collapses adjacent duplicates, which is exactly why `sort` must run first — the order of tools in a pipe is part of the logic.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the pipeline's final output: one bar per drink, widths proportional to the counts, labeled with the literal output lines.

- **Title (bold 15px, `#1a5276`, top center):** "Final Output: 6 latte, 3 espresso, 2 mocha, 1 cappuccino".
- **Layout:** bars start at x=180, 26px tall, scale 60px per order; rows at y = 80, 130, 180, 230; row labels 13px monospace `#2c3e50` right-aligned at x=170 showing the literal output lines "6 latte", "3 espresso", "2 mocha", "1 cappuccino".
- **Bars:** counts `[6, 3, 2, 1]` → pixel widths `[360, 180, 120, 60]`; fills blue `rgba(42,120,214,0.35)` with 2px `#2a78d6` border for latte, and `rgba(107,114,128,0.25)` with 2px `#6b7280` borders for the other three; 12px `#444` count labels 8px past each bar end.
- **Gridlines:** vertical `#e5e9ef` 1px at x = 180+120, 180+240, 180+360 (counts 2, 4, 6), 11px `#999` tick labels "2", "4", "6" below y=260.
- **Annotation (bold 13px `#2a78d6`, near x=400, y=60):** "latte = 6 of 12 orders — half the rush".
- **Caption (12px `#444`, bottom right):** "counts exact — re-derivable by hand from the 12-line file".

## Text Commands Don't Rot

**Tags:** `where it's used` (blue), `repeatability` (green)

- **A command is text** — copy it into a runbook, commit it to git, paste it over SSH onto a server
- **A GUI procedure is screenshots** — the click path goes stale at the next redesign; text replays exactly
- **Rerunnable** — yesterday's pipeline on today's file is the same command with a new filename
- **The glue layer** — CI pipelines, container entrypoints, and cron jobs all bottom out in shell scripts
- **First look at big data** — head, wc, cut, grep, sort, uniq profile a 10 GB CSV before anything can load it

*Example (italic):* `head -5 big.csv` shows a 10 GB file's columns in milliseconds; a spreadsheet hits its row limit and refuses to open it at all.

**Key point:** The shell wins for repeatable work because a command is text — the only artifact that is simultaneously runnable, versionable, and pasteable into a message.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time to a first answer ("how many rows, what do the columns look like") on a 10 GB CSV, by tool.

- **Title (bold 15px, `#1a5276`, top center):** "First Look at a 10 GB CSV: Time to an Answer".
- **Layout:** baseline 2px `#999` vertical line at x=240, bars extend right, max width 430; rows at y = 65, 110, 155, 200, 245; row labels 12px `#444` left-aligned at x=20.
- **Time scale:** linear, 1.7 px per second; vertical `#e5e9ef` 1px gridlines at 1/2/3/4 min (x = 240 + 102/204/306/408) with 11px `#999` tick labels "1 min".."4 min" at y=272.
- **Rows (label / bar):** "head -5 — instant": green `#008300` bar width 4; "wc -l — ~25 s": blue `#2a78d6` bar width 43; "cut | sort | uniq -c — ~90 s": blue bar width 153; "pandas read_csv — ~4 min": orange `#d95926` bar width 408; "spreadsheet — won't open": red `#e74c3c` bar width 430 (off-scale) with bold 12px red label "row limit — fails" at the bar's right end.
- **Bar style:** 16px tall, fills at 0.35 alpha with solid 2px borders in the row color; 11px `#444` time labels 8px past each bar end (except the spreadsheet row, which carries the red fail label).
- **Annotation (bold 13px `#008300`, near x=300, y=40):** "the pipeline answers before pandas finishes loading".
- **Caption (12px `#444`, bottom right):** "timings illustrative — typical laptop, 10 GB file".

## The Write-Only One-Liner

**Tags:** `common mistake` (red), `footguns` (orange)

- **Write-only code** — a 200-character pipeline works today and is unreadable next month, even by its author
- **Quoting footgun** — an unquoted `$file` holding `q2 report.txt` becomes two arguments, not one
- **Whitespace trap** — filenames with spaces break naive loops; `rm $tmp` can hit the wrong files
- **Decades of accretion** — flags like `-rn` and `-c` are cryptic because fifty years added, never redesigned
- **The discipline** — once a pipeline works, save it as a commented script with a name, not a memory

*Example (italic):* `sort orders.txt | uniq -c | sort -rn` becomes `top_sellers.sh` with three comment lines — the same power, now reviewable.

**Common mistake:** Treating the shell as a scratchpad only. The one-liner that answered the question is the start of a script, not the end of the work — an unsaved pipeline is an analysis no one can repeat.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same delete command with and without quotes, showing how the shell splits an unquoted variable on whitespace.

- **Title (bold 15px, `#1a5276`, top center):** "Quotes Decide Where Words End: file=\"q2 report.txt\"".
- **Row 1 (boxes' top edge y=80), 12px `#444` label "unquoted" at x=20:** blue `#2a78d6` rounded box at x=110 w=150 labeled `rm $file` (13px monospace); 3px `#6b7280` arrow to a red `#e74c3c` box at x=340 w=310 labeled "two args: \"q2\" + \"report.txt\"" with bold 12px red "✗ wrong files targeted" 10px below the box.
- **Row 2 (boxes' top edge y=190), label "quoted":** blue box at x=110 w=150 labeled `rm "$file"` (13px monospace); 3px arrow to a green `#008300` box at x=340 w=310 labeled "one arg: \"q2 report.txt\"" with bold 12px green "✓ exactly one file removed" 10px below the box.
- **Box style:** 42px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, text 12–13px `#2c3e50` centered.
- **Annotation (bold 13px orange `#d95926`, centered near y=280):** "whitespace is the shell's silent splitter — quote every variable".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Inline code:** commands and output fragments in bullets use `<code>` at 0.85em monospace, `#2c3e50` on `#f4f6f8`, 1px 4px padding, 3px radius.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the orders pipeline counts (6 latte, 3 espresso, 2 mocha, 1 cappuccino out of 12 lines) are exact and hand-verified end to end; the 10 GB CSV timings in c3 are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
