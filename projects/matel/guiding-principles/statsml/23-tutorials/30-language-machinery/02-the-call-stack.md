# The Call Stack

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Call Stack

**Subtitle:** When one job pauses to do a smaller job, the computer keeps a pile of bookmarks saying where to go back — that pile is the call stack, and a stack trace is a photo of it

## A Cook, Three Recipe Cards, and a Pile of Sticky Notes

**Tags:** `core idea` (blue), `frames` (green), `return address` (orange)

- **The cook** — a cook starts the PANCAKES card; step 3 says "make the batter — see the BATTER card"
- **The pause** — the cook sticks a note on PANCAKES ("resume at step 4") and lays BATTER on top of it
- **Deeper still** — BATTER's step 2 says "melt the butter", so a MELT BUTTER card goes on top of the pile
- **A frame** — each card in the pile is a frame: one job in progress plus its own notes and ingredients
- **Return address** — the sticky note is the return address: exactly where to pick up when the card ends
- **The rule** — only the top card gets worked on; finishing it peels the pile back down, one card at a time

*Example (italic):* Mid-melt, the pile reads MELT BUTTER on top, BATTER under it, PANCAKES at the bottom — three paused-or-active jobs, each with a note saying where its parent resumes.

**Key point:** The call stack is a pile of frames, one per unfinished job, and each frame carries a return address — the exact spot its caller continues from.

### Visualization (canvas `c1`, 720×300)

Diagram of the cook's pile at its deepest moment: three stacked recipe cards, the bottom two carrying yellow sticky notes with their resume points, the top card marked as the one being worked on.

- **Title (bold 15px, `#1a5276`, top center):** "The Cook's Pile: Top Card Is the Current Job".
- **Cards:** three rounded rectangles, width 300, height 62, left edge x=80 — bottom at y=210 labeled bold 13px "PANCAKES" with 12px `#444` sub-line "paused at step 3: make the batter"; middle at y=140 labeled "BATTER" with sub-line "paused at step 2: melt the butter"; top at y=70 labeled "MELT BUTTER" with sub-line "working now". Bottom and middle: fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; top: fill `rgba(0,131,0,0.12)`, 3px `#008300` border.
- **Sticky notes:** small rects (110×34, fill `rgba(201,133,0,0.18)`, 1px `#c98500` border) overlapping the right edge of the bottom two cards; 11px `#c98500` text — on PANCAKES: "resume at step 4"; on BATTER: "resume at step 3". No note on the top card.
- **Growth arrow:** vertical 2px `#6b7280` arrow at x=45 pointing up from y=270 to y=60; 12px `#6b7280` label near its top: "newer cards go on top".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=110):** two lines: "finish the top card," / "then peel back down".
- **Caption (12px `#444`, bottom right):** "illustrative — a pancake recipe pausing into sub-recipes".

## Tracing a Breakfast Program, Step by Step

**Tags:** `worked example` (blue), `push & pop` (green)

- **The program** — `main` calls `make_breakfast`, which calls `brew_coffee`, which calls `grind_beans`
- **Push** — every call lays a new frame on the pile: depth goes 1, 2, 3, and hits 4 at `grind_beans`
- **Pop** — `grind_beans` returns at step 5 (depth 3), `brew_coffee` returns at step 6 (depth 2)
- **Reuse** — at step 7 `make_breakfast` calls `toast_bread`, so depth climbs back to 3, then falls again
- **The end** — by step 10 every frame has popped and depth is 0: breakfast is done, the pile is empty

*Example (italic):* Written as one list, the depth over steps 1–10 is 1, 2, 3, 4, 3, 2, 3, 2, 1, 0 — you can replay it by pushing a coin per call and removing one per return.

**Key point:** Depth goes up exactly one on every call and down exactly one on every return — the peak of 4 frames happens the moment `grind_beans` is running.

### Visualization (canvas `c2`, 720×300)

Staircase step-chart of stack depth across the ten steps of one breakfast run, with the calls and returns that cause each rise and fall labeled along the line.

- **Title (bold 15px, `#1a5276`, top center):** "Stack Depth While Breakfast Runs: Push on Call, Pop on Return".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = step 1 to 10 with 12px `#444` tick labels "1"–"10"; y = depth 0 to 4, light `#e5e9ef` gridlines at 1, 2, 3, 4 with 12px `#444` labels.
- **Depth staircase:** blue `#2a78d6` 3px line through hardcoded depths `[1, 2, 3, 4, 3, 2, 3, 2, 1, 0]` at steps `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, drawn as horizontal treads with vertical risers; 5px blue dot at each step.
- **Event labels (11px `#444`, staggered above/below the treads to avoid overlap):** step 2 "call make_breakfast", step 4 "call grind_beans", step 5 "grind_beans returns", step 7 "call toast_bread".
- **Peak marker:** bold 13px green `#008300` label just above the step-4 tread: "deepest pile: 4 frames".
- **Annotation (bold 12px orange `#d95926`, near steps 5–6 at y=150):** "every return peels one frame off".
- **Caption (12px `#444`, bottom right):** "illustrative — one run of the breakfast program".

## The Crash Report Is a Photo of the Pile

**Tags:** `where it's used` (blue), `stack trace` (green), `debugging` (orange)

- **The crash** — suppose `grind_beans` fails at line 58 because the bean jar is empty
- **The trace** — the error printout lists one line per open frame: main, make_breakfast, brew_coffee, grind_beans
- **A photo** — that printout is nothing but the frame pile at the instant of the crash, written as text
- **Read it** — the last frame line is where it broke; the lines above it are the callers still waiting
- **Line numbers** — each caller's line (12, 25, 41) is its return address: where it would have resumed

*Example (italic):* Four open frames become four trace lines — `main` at line 12, `make_breakfast` at line 25, `brew_coffee` at line 41, and `grind_beans` at line 58, the crash site.

**Key point:** A stack trace is the call stack printed at the moment of failure — one line per frame, and the return addresses are the line numbers you see.

### Visualization (canvas `c3`, 720×300)

Side-by-side mapping: the four-frame pile at the crash on the left, the printed traceback text on the right, with an arrow tying each frame to its trace line.

- **Title (bold 15px, `#1a5276`, top center):** "A Stack Trace Is the Frame Pile Printed as Text".
- **Left pile:** four rounded frame boxes, width 240, height 38, left edge x=60, stacked at y=230, 185, 140, 95 (bottom to top) — 12px labels: "main — waiting at line 12", "make_breakfast — waiting at line 25", "brew_coffee — waiting at line 41", "grind_beans — crashed at line 58"; bottom three fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border, top frame fill `rgba(231,76,60,0.10)` with 2px `#e74c3c` border.
- **Right traceback (12px monospace, starting x=390, y=95, line spacing 26px):** "Traceback (most recent call last):" in `#444`; then '  kitchen.py, line 12, in main', '  kitchen.py, line 25, in make_breakfast', '  kitchen.py, line 41, in brew_coffee', '  kitchen.py, line 58, in grind_beans' in `#2c3e50`; final line bold red `#e74c3c`: "Error: no beans left".
- **Mapping arrows:** four 1.5px `#6b7280` arrows from each frame box's right edge to its matching text line (bottom frame to the "in main" line, top frame to the "in grind_beans" line).
- **Annotation (bold 12px red `#e74c3c`, under the traceback near y=270):** "read the last frame line first — that's where it broke".
- **Caption (12px `#444`, bottom left):** "illustrative crash — file and line numbers invented".

## The Trace Shows the Pile, Not the Day

**Tags:** `common mistake` (red), `snapshot vs history` (orange)

- **New crash** — same program, but this time the failure happens later, inside `toast_bread` at step 7
- **What ran** — by then 5 functions have run: main, make_breakfast, brew_coffee, grind_beans, toast_bread
- **What shows** — the trace lists only 3: main, make_breakfast, toast_bread — the frames still open
- **The gap** — `brew_coffee` and `grind_beans` already returned, so their frames popped and left no trace
- **The mistake** — reading a trace as "everything that executed" and hunting bugs in functions it never names

*Example (italic):* A reader sees no `brew_coffee` in the toast-crash trace and concludes coffee never happened — it did, at steps 3–6, but its frames were gone before the photo was taken.

**Common mistake:** Treating a stack trace as a diary of the whole run. It is a snapshot of the frames open at the crash — 5 functions ran here, and only 3 appear.

### Visualization (canvas `c4`, 720×300)

Two-column comparison for the toast-time crash: every function that ran on the left, the three frames the trace actually shows on the right, with the already-returned pair grayed out.

- **Title (bold 15px, `#1a5276`, top center):** "Crash During Toast: 5 Functions Ran, Only 3 Are in the Trace".
- **Left column:** header bold 13px `#1a5276` at x=185 (centered), y=68: "everything that ran"; five rounded rows, width 250, height 30, left edge x=60, at y=82, 121, 160, 199, 238 — labels 12px: "main", "make_breakfast", "brew_coffee", "grind_beans", "toast_bread". Rows main / make_breakfast / toast_bread: fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; rows brew_coffee / grind_beans: fill `rgba(107,114,128,0.12)`, 1px dashed `#6b7280` border with 11px `#6b7280` right-aligned note "already returned".
- **Right column:** header bold 13px `#1a5276` at x=535 (centered), y=68: "what the trace shows"; three rounded rows, width 250, height 30, left edge x=410, at y=95, 150, 205 — "main" and "make_breakfast" fill `rgba(42,120,214,0.15)` with 2px `#2a78d6` border; "toast_bread" fill `rgba(231,76,60,0.10)` with 2px `#e74c3c` border and 11px `#e74c3c` note "crashed here".
- **Mapping arrows:** three 1.5px `#6b7280` arrows from the left blue rows (main, make_breakfast, toast_bread) to their right-column counterparts.
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=360, y=283):** "a trace is a photo of the open pile, not a diary of the whole run".
- **Caption (12px `#444`, bottom right):** "illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness) — the c2 depth array `[1, 2, 3, 4, 3, 2, 3, 2, 1, 0]` over steps 1–10, the line numbers 12 / 25 / 41 / 58 in c3, and the 5-ran / 3-shown split in c4 must match the text exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
