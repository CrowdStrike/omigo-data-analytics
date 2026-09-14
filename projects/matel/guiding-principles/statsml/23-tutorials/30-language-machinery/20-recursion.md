# Recursion

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Recursion

**Subtitle:** A recursive solution answers a big question by handing a slightly smaller copy of the same question to the next step — plus one case small enough to answer outright

## Counting the Line by Asking the Person Behind You

**Tags:** `core idea` (blue), `smaller copy` (green), `base case` (orange)

- **The line** — five people wait at a coffee cart, and the person at the front wonders how long it is
- **One small step** — instead of counting everyone, she asks the person behind her: "how many from you back?"
- **The same question** — that person cannot see the whole line either, so he passes the exact question backward
- **The last person** — the one at the back sees nobody behind and answers without asking anyone: "just me, 1"
- **The pattern** — a question answered by a smaller copy of itself, plus one tiny step, is recursion

*Example (italic):* The front person never counts the line herself — she does one addition and lets four smaller versions of the same question do all the rest.

**Key point:** Recursion solves a big problem by handing a smaller copy of it backward, with one case — the back of the line — small enough to answer directly.

### Visualization (canvas `c1`, 720×300)

Single-panel diagram: five people in a line drawn as circles, the question passed backward along arrows above them, the answers 1–4 returning along arrows below, and the front person's final "4 + 1 = 5".

- **Title (bold 15px, `#1a5276`, top center):** "The Coffee Line: One Question Passed Backward, Answers Added Coming Forward".
- **People:** five circles, radius 20, centers at y=160, x = `[110, 240, 370, 500, 630]`; fill `rgba(42,120,214,0.15)`, stroke blue `#2a78d6` 2px; bold 13px `#1a5276` labels below at y=200: "P1 (front)", "P2", "P3", "P4", "P5 (back)". P5's circle gets a green `#008300` 3px stroke instead (it is the base case).
- **Ask arrows:** four blue `#2a78d6` 2px arrows at y=115 pointing right (P1→P2, P2→P3, P3→P4, P4→P5), small arrowheads; one shared bold 12px blue label centered at y=95: "'how many from you back?'".
- **Answer arrows:** four green `#008300` 2px arrows at y=222 pointing left (P5→P4, P4→P3, P3→P2, P2→P1); bold 13px green labels under each arrow midpoint at y=240: "1", "2", "3", "4".
- **Base-case annotation (bold 12px orange `#d95926`, right-aligned near x=630, y=60):** two lines: "sees nobody behind —" / "answers '1' (base case)".
- **Final answer (bold 13px `#1a5276`, above P1 at x=110, y=60):** "4 + 1 = 5".
- **Caption (12px `#444`, bottom right):** "illustrative — a five-person line".

## Tracing Five People: Down the Line and Back

**Tags:** `worked example` (blue), `two phases` (green)

- **Going back** — the question travels through all 5 people; each one just forwards it and waits to add 1
- **The turnaround** — person 5 sees empty pavement behind and answers 1; the chain starts returning
- **Coming forward** — person 4 hears 1 and says 2, person 3 says 3, person 2 says 4, person 1 says 5
- **Two phases** — no one answers on the way back; all five "+1"s happen on the way forward, in reverse order
- **Check by hand** — nine steps total: four asks going back, one base-case answer, four additions returning

*Example (italic):* Four "+1"s hang in the air unpaid while the question travels backward; the answers 1, 2, 3, 4, 5 settle them one by one on the way forward.

**Key point:** Every recursive run has the same shape — a winding-back phase that only forwards the question, a turnaround at the base case, and an unwinding phase where the real work happens.

### Visualization (canvas `c2`, 720×300)

Single-panel staircase chart: step number on the x axis, how far back the question has traveled on the y axis — depth climbs 0 to 4 as the question passes back, then descends as the answers 1–5 return.

- **Title (bold 15px, `#1a5276`, top center):** "Nine Steps: the Question Winds Back, the Answers Unwind Forward".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = step 0 to 8 with 12px `#444` tick labels "0"–"8"; y = depth 0 to 4 (0 at the baseline) with 12px `#444` labels "0"–"4" and light `#e5e9ef` gridlines at 1, 2, 3, 4; 12px `#6b7280` y-axis caption rotated at x=25: "people back from the front".
- **Path points:** steps = `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, depth = `[0, 1, 2, 3, 4, 3, 2, 1, 0]`.
- **Winding leg (steps 0–4):** blue `#2a78d6` 3px line with 6px blue dots; 12px blue label "asks travel back" beside the rising leg near step 1.5.
- **Unwinding leg (steps 4–8):** green `#008300` 3px line with 6px green dots; bold 13px green answer labels above the last five dots: "1" at step 4, "2" at step 5, "3" at step 6, "4" at step 7, "5" at step 8.
- **Turnaround annotation (bold 12px orange `#d95926`, near the peak at step 4, y offset above the dot):** "base case: 'just me, 1'".
- **Final annotation (bold 13px `#008300`, near step 7.2 at depth 0.8):** "front person: 5".
- **Caption (12px `#444`, bottom right):** "illustrative — five people, nine steps".

## Folders Inside Folders: Where Recursion Shows Up

**Tags:** `where it's used` (blue), `nested data` (green), `one rule` (orange)

- **Nested data** — folders inside folders, JSON inside JSON, replies to replies: real data contains itself
- **One rule everywhere** — "my count = my own photos + each subfolder's report" works at every level
- **The example** — photos holds 2, trips 3, beach 4, family 3: beach reports 4, trips reports 7, total 12
- **No depth planning** — the same one-line rule handles 2 levels or 20; you never write code per level
- **Where it appears** — file crawlers, decision trees, org charts, sorting, parsers — data shaped like itself

*Example (italic):* A photo counter written for one folder counts a whole 20-level archive unchanged, because every subfolder is just a smaller version of the same job.

**Key point:** Whenever data contains smaller copies of itself, one recursive rule replaces separate code for every level of nesting.

### Visualization (canvas `c3`, 720×300)

Single-panel folder-tree diagram: four folder boxes connected top-down, each showing its own photo count, with green "reports" bubbling up the tree to the total of 12 at the root.

- **Title (bold 15px, `#1a5276`, top center):** "One Rule, Every Level: my count = my photos + subfolder reports".
- **Boxes:** rounded rectangles 170×36, fill `rgba(42,120,214,0.10)`, stroke `#1a5276` 2px, bold 13px `#1a5276` centered text:
  - "photos — 2 own" centered at (360, 78)
  - "trips — 3 own" centered at (210, 160)
  - "family — 3 own" centered at (510, 160)
  - "beach — 4 own" centered at (210, 242)
- **Connectors:** 2px `#6b7280` lines from photos to trips, photos to family, trips to beach (box edge to box edge).
- **Bubble-up reports (bold 12px green `#008300`, to the right of each box):** "reports 4" beside beach, "reports 3 + 4 = 7" beside trips, "reports 3" beside family; bold 13px green beside photos: "2 + 7 + 3 = 12"; small green 2px up-arrows next to each report label.
- **Base-case annotation (bold 12px orange `#d95926`, near x=600, y=242):** two lines: "no subfolders —" / "just count (base case)"; thin orange 1px dashed (dash 4/3) pointers to beach and family.
- **Caption (12px `#444`, bottom right):** "illustrative — 12 photos across 4 folders".

## Forgetting the Last Person: the Missing Base Case

**Tags:** `common mistake` (red), `base case` (orange)

- **The base case** — the one input answered directly with no further asking: the back of the line
- **Forgetting it** — remove person 5's "just me, 1" and everyone waits on someone behind, forever
- **The crash** — each unanswered ask is held in memory; the pile deepens until it overflows the stack
- **Not shrinking** — asking someone ahead of you (the same or a bigger problem) also never finishes
- **The check** — every step must move toward the base case: the green run turns at depth 4, red never turns

*Example (italic):* One missing line — the direct answer — turns a nine-step count into a program that asks forever and crashes with a stack overflow.

**Common mistake:** Writing the "smaller copy" step and forgetting the direct answer. Without a base case, recursion is an infinite chain of waiting, not a calculation.

### Visualization (canvas `c4`, 720×300)

Single-panel line chart comparing two runs on shared axes: the healthy run (green) climbs to depth 4, turns at the base case, and finishes; the run with no base case (red, dashed) climbs past depth 12 and crashes.

- **Title (bold 15px, `#1a5276`, top center):** "The Base Case Is the Difference Between an Answer and a Crash".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = step 0 to 12 with 12px `#444` tick labels "0"–"12"; y = depth (unanswered asks) 0 to 13 with 12px `#444` labels at 0, 4, 8, 12 and light `#e5e9ef` gridlines; 12px `#6b7280` y-axis caption rotated at x=25: "asks waiting in memory".
- **Healthy run:** green `#008300` 3px line with 5px dots, steps = `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, depth = `[0, 1, 2, 3, 4, 3, 2, 1, 0]`; bold 12px green label near step 5.5, depth 3.2: "turns at the base case, finishes".
- **Broken run:** red `#e74c3c` 3px dashed (dash 6/4) line, steps = `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`, depth = `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]`; bold 14px red "×" marker at the last point (step 12, depth 12).
- **Annotation (bold 13px red `#e74c3c`, near step 8, depth 11, right-aligned to the plot edge):** two lines: "no base case —" / "grows until stack overflow".
- **Caption (12px `#444`, bottom right):** "illustrative — real stacks crash after thousands of frames, not 12".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red appears only in c4's broken run (a genuine error state).
- **Data:** all positions, step/depth arrays, and folder counts are the hardcoded literals above (no randomness); the line has exactly 5 people, the trace has 9 steps with answers 1–5, the folder tree totals 2 + 7 + 3 = 12, and the broken run climbs to depth 12 — text and charts must keep these numbers identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
