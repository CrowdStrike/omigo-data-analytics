# Valid Parentheses

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Valid Parentheses

**Subtitle:** Brackets nest like boxes inside boxes — you must close the one you opened most recently, and the data structure that remembers "most recently" is a stack

## Packing Boxes Inside Boxes

**Tags:** `core idea` (blue), `nesting` (green), `last in, first out` (orange)

- **The move** — someone packing for a move opens a crate, then opens a shoebox inside it
- **The rule** — the shoebox must be taped shut before the crate; you close the newest open box first
- **As symbols** — write open as `(` or `[` and close as `)` or `]`: the packing day reads `( [ ] )`
- **The smell** — "handle the most recent thing first" is exactly what a stack of plates does
- **The check** — a bracket string is valid when every closer matches the newest still-open opener

*Example (italic):* Taping the crate while the shoebox inside is still open — `( [ ) ]` — is the packing mistake that "invalid parentheses" describes.

**Key point:** Nesting means last opened, first closed — so checking brackets is a job for a stack, the last-in-first-out pile.

### Visualization (canvas `c1`, 720×300)

Single-panel matching-arcs diagram: the four symbols of `( [ ] )` laid on a baseline with one arc per box connecting its opener to its closer, inner arc lower than outer.

- **Title (bold 15px, `#1a5276`, top center):** "Every Closer Matches the Newest Opener".
- **Baseline:** horizontal 2px `#999` line at y=210 from x=120 to x=600; the four symbols `(`, `[`, `]`, `)` in bold 24px monospace `#2c3e50` centered at x = `[180, 300, 420, 540]`, sitting just above the line; 12px `#6b7280` step labels "1", "2", "3", "4" below the line under each symbol.
- **Crate arc:** blue `#2a78d6` 3px arc from x=180 to x=540 with apex at y=80; bold 13px blue label "crate: opened 1st, closed last" centered above the apex.
- **Shoebox arc:** green `#008300` 3px arc from x=300 to x=420 with apex at y=145; bold 13px green label "shoebox: opened 2nd, closed 1st" just above its apex.
- **Annotation (bold 13px orange `#d95926`, centered near x=360, y=255):** "last opened, first closed — arcs never cross".
- **Caption (12px `#444`, bottom right):** "illustrative — one packing day written in brackets".

## Reading ( [ ( ) ] ) One Symbol at a Time

**Tags:** `worked example` (blue), `stack trace` (green)

- **The string** — check `( [ ( ) ] )`, six symbols, by walking left to right with an empty stack
- **Opener** — push it: after symbols 1, 2, 3 the stack reads `( [ (` and is three deep
- **Closer** — pop the top and compare: symbol 4 is `)`, top is `(` — a match, so pop it off
- **Keep going** — symbol 5 `]` pops `[`, symbol 6 `)` pops `(`; the stack empties exactly at the end
- **Three failures** — wrong closer on top, a closer with an empty stack, or leftovers when the string ends
- **Verdict** — depth went 1, 2, 3, 2, 1, 0 with every pop a match, so `( [ ( ) ] )` is valid

*Example (italic):* On the broken string `( [ ) ]` the walk dies at symbol 3: the closer `)` arrives while `[` sits on top of the stack.

**Key point:** Push openers, pop-and-match closers, finish empty — one pass over the six symbols settles it, no backtracking.

### Visualization (canvas `c2`, 720×300)

Single-panel staircase chart: stack depth after each of the six symbols of `( [ ( ) ] )`, with the stack's contents printed at each step.

- **Title (bold 15px, `#1a5276`, top center):** "Stack Depth While Reading ( [ ( ) ] )".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = steps 1 to 6 at x = `[110, 210, 310, 410, 510, 610]`, each with its symbol in bold 15px monospace `#2c3e50` below the baseline (`(`, `[`, `(`, `)`, `]`, `)`) and an 11px `#6b7280` step number beneath; y = depth 0 to 3 with 12px `#444` labels "0"–"3" and light `#e5e9ef` gridlines at 1, 2, 3.
- **Depth steps:** blue `#2a78d6` 3px staircase line through depths `[1, 2, 3, 2, 1, 0]` (horizontal run then vertical rise at each step), with a 7px blue dot at each of the six step levels.
- **Stack labels:** 12px `#444` monospace text above each dot showing the stack at that moment: "(", "([", "([(", "([", "(", "empty".
- **Push/pop coloring:** small bold 11px labels next to each dot — "push" in green `#008300` for steps 1–3, "pop" in orange `#d95926` for steps 4–6.
- **End marker:** green `#008300` dashed (dash 4/3) circle of radius 12 around the final dot at depth 0; bold 13px green annotation to its upper left: "ends empty, every pop matched — valid".

## Where the One-Pass Stack Check Shows Up

**Tags:** `where it's used` (blue), `one pass` (green), `interview signal` (orange)

- **Editors** — a spreadsheet or code editor flags `((8+2)*(5-3)` the instant you type the last symbol
- **Data files** — JSON and XML readers run this exact check on `{ } [ ]` pairs before parsing anything
- **Depth for free** — the stack's height is the nesting depth; the profile below peaks at depth 4
- **Cost** — one pass, one push or pop per symbol: 24 symbols means 24 cheap steps, never a re-read
- **Interview signal** — saying "nesting smells like a stack" out loud is the answer's whole skeleton

*Example (italic):* A config file 24 symbols long is checked in 24 steps, and its deepest moment — depth 4 — is just the tallest the stack ever got.

**Key point:** The same walk that validates the brackets also hands you nesting depth for free — one pass, no second look at any symbol.

### Visualization (canvas `c3`, 720×300)

Single-panel area chart: nesting depth across the 24 symbols of an illustrative config file, a skyline that rises and falls and returns to zero.

- **Title (bold 15px, `#1a5276`, top center):** "One Pass Over a Config File: Depth Rises and Falls Like a Skyline".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = symbol position 1 to 24 with 12px `#444` tick labels at 1, 6, 12, 18, 24; y = depth 0 to 4 with 12px `#444` labels "0"–"4" and light `#e5e9ef` gridlines at 1, 2, 3, 4.
- **Depth profile:** blue `#2a78d6` 2px staircase line through the 24 hardcoded depths `[1, 2, 3, 2, 3, 2, 3, 2, 3, 4, 3, 2, 1, 2, 3, 2, 3, 2, 1, 2, 1, 2, 1, 0]`; fill under the staircase `rgba(42,120,214,0.15)`.
- **Peak marker:** vertical dashed orange `#d95926` (dash 4/3) line at position 10 from the baseline to the depth-4 level; bold 13px orange label above it: "deepest nesting: 4 — the stack's tallest moment".
- **End marker:** 7px green `#008300` dot on the baseline at position 24; 12px green label "back to 0 — balanced" to its upper left.
- **Annotation (bold 12px `#1a5276`, near x=150, y=90):** two lines: "24 symbols, 24 steps —" / "each one a single push or pop".
- **Caption (12px `#444`, bottom right):** "illustrative — depth profile of a small config file".

## Why Counting Isn't Enough

**Tags:** `common mistake` (red), `order matters` (orange)

- **The shortcut** — many first attempts just count: same number of openers and closers means valid, right?
- **Fooled once** — `) (` has one closer and one opener, counts equal, yet it closes a box never opened
- **Fooled twice** — `( [ ) ]` has one of each symbol, all counts equal, yet the closers come in the wrong order
- **What counts miss** — a counter forgets which opener is newest; only the stack remembers the order
- **Where they die** — the stack rejects `) (` at symbol 1 and `( [ ) ]` at symbol 3, mid-walk, with a reason

*Example (italic):* Two analysts check `( [ ) ]`: the counter reports "2 openers, 2 closers, fine" while the stack stops at symbol 3 because `)` arrived with `[` on top.

**Common mistake:** Validating brackets by counting alone. Equal counts are necessary but not sufficient — order is the whole point, and order is what a stack stores.

### Visualization (canvas `c4`, 720×300)

Two-row verdict board: the strings `) (` and `( [ ) ]` each judged twice — by a naive counter and by the stack — with the counter wrongly passing both.

- **Title (bold 15px, `#1a5276`, top center):** "Two Strings That Fool a Counter".
- **Column headers (bold 13px, y=75):** "just counting" in `#6b7280` centered at x=390, "using a stack" in `#1a5276` centered at x=590.
- **Row 1 (y=125):** string `) (` in bold 20px monospace `#2c3e50` at x=60; counter verdict pill centered at x=390 — rounded rect filled `rgba(230,126,34,0.15)` with bold 12px orange `#d95926` text "counts 1–1: passes"; stack verdict pill centered at x=590 — rounded rect filled `rgba(231,76,60,0.12)` with bold 12px red `#e74c3c` text "FAIL at symbol 1".
- **Row 2 (y=195):** string `( [ ) ]` in bold 20px monospace `#2c3e50` at x=60; counter pill at x=390, same style, text "counts 2–2: passes"; stack pill at x=590, same style, text "FAIL at symbol 3".
- **Reason notes (11px `#6b7280`, under each stack pill):** row 1 "closer with nothing open", row 2 "top is [ but saw )".
- **Pill style:** 170×30 rounded rects (6px radius), 1px border matching each pill's text color.
- **Annotation (bold 13px magenta `#d55181`, centered near x=360, y=265):** "equal counts, wrong order — only the stack catches it".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all depth arrays, symbol positions, and pill texts are the hardcoded literals above (no randomness); bracket symbols render in a monospace font so `(`, `[`, `)`, `]` align; the c3 profile is invented and keeps its "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
