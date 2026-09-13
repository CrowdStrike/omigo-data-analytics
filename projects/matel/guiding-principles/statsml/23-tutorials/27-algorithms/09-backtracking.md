# Backtracking

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Backtracking

**Subtitle:** Build a solution one guess at a time, and the moment a guess breaks a rule, undo it and try the next option — try, fail, undo, try next

## Seating Four Friends Without a Feud

**Tags:** `core idea` (blue), `try-fail-undo` (green), `guesses are pencil` (orange)

- **The party** — four friends (Ava, Ben, Cleo, Dan) need chairs 1–4 in a row, but three pairs are feuding
- **The rules** — Ava–Ben, Cleo–Dan, and Ben–Dan must not sit side by side
- **The move** — seat one friend per chair, left to right, checking the rules the moment each one sits
- **The fail** — Ben in chair 2 lands next to Ava, so that try is refused and Ben never actually sits
- **The undo** — when every friend fails a chair, un-seat the previous friend and try their next option
- **The name** — this try, fail, undo, try-next loop is what programmers call backtracking

*Example (italic):* Ava takes chair 1, Ben is refused chair 2 for the feud, so Cleo takes chair 2 instead — one tiny fail, no restart.

**Key point:** Backtracking builds the answer one guess at a time and treats every guess as reversible — pencil, never ink.

### Visualization (canvas `c1`, 720×300)

Three-panel strip showing the loop on the first three tries: a row of four chairs where Ava sits, Ben is refused, and Cleo sits instead.

- **Title (bold 15px, `#1a5276`, top center):** "Try, Fail, Undo, Try Next — the Whole Trick in Three Frames".
- **Panels:** three panel groups centered at x = 130, 365, 600; each panel is a row of four 38px chair squares (6px gaps, 2px `#e5e9ef` border, chair tops at y=150) with 11px `#6b7280` chair numbers "1"–"4" below each square at y=205.
- **Panel captions (bold 13px `#1a5276`, centered above each panel at y=120):** "1 · try Ben", "2 · fail — feud", "3 · try next: Cleo".
- **Panel 1:** Ava's square filled `rgba(42,120,214,0.15)` with 12px `#2a78d6` label "Ava" inside chair 1; a 2px `#2a78d6` arrow from a 12px "Ben" label at y=135 pointing down into chair 2.
- **Panel 2:** Ava seated as before; "Ben" shown inside chair 2 in red `#e74c3c` with a bold 14px red "✗" over the square; 11px red label "Ava ↔ Ben feud" centered below the chairs at y=222.
- **Panel 3:** Ava seated; chair 2 filled `rgba(0,131,0,0.15)` with 12px `#008300` label "Cleo"; chair 2 outlined 2px green `#008300`.
- **Flow arrows:** 2px `#6b7280` arrows between panels at y=170 (from panel 1 to 2, and 2 to 3).
- **Annotation (bold 12px orange `#d95926`, centered near x=365, y=262):** "a fail costs one try — never a restart".
- **Caption (12px `#444`, bottom right):** "feud rules: Ava–Ben, Cleo–Dan, Ben–Dan apart".

## The Full Trace: 14 Tries, 6 Fails, 4 Undos

**Tags:** `worked example` (blue), `search tree` (green)

- **The trace** — seating friends alphabetically, chair by chair, takes exactly 14 tries to fill the table
- **Six fails** — 6 tries break a feud rule on the spot, and the friend simply is not seated there
- **Four undos** — 4 times every friend fails a chair, so the previously seated friend is un-seated
- **The big undo** — even Ava at chair 1 gets undone; the very first guess was wrong all along
- **The answer** — the first seating that works is Ben, Cleo, Ava, Dan
- **By hand** — redo it yourself: the only check is whether the new friend feuds with their left neighbor

*Example (italic):* Ava, Cleo, and Ben all seat fine, then Dan fails chair 4 — undo Ben, and the retreat begins.

**Key point:** Every fail is cheap because only the newest guess is thrown away — 14 tries and 4 undos land on Ben, Cleo, Ava, Dan.

### Visualization (canvas `c2`, 720×300)

Search tree with one level per chair: all 14 tries drawn as name pills, numbered in order, with rule-breaking tries in red, the winning path in green, and the four undos as dashed arrows curving back up.

- **Title (bold 15px, `#1a5276`, top center):** "One Search, 14 Tries: Fails in Red, Undos Dashed".
- **Levels:** 12px `#6b7280` labels at x=14 — "chair 1" y=78, "chair 2" y=133, "chair 3" y=188, "chair 4" y=243; nodes on each level share that y.
- **Node style:** rounded pill 58×20 centered on its coordinate, name centered 12px; try number bold 11px `#6b7280` just above the pill's left edge. OK try: 2px `#2a78d6` border, `#2a78d6` text, white fill. Fail: 2px `#e74c3c` border, `#e74c3c` text, a 12px red "✗" after the name. Winning-path nodes: fill `rgba(0,131,0,0.15)`, 2px `#008300` border, `#008300` text.
- **Nodes (try number, name, center x/y):** 1 Ava (185, 78); 2 Ben ✗ (95, 133); 3 Cleo (185, 133); 4 Ben (150, 188); 5 Dan ✗ (150, 243); 6 Dan ✗ (230, 188); 7 Dan (285, 133); 8 Ben ✗ (310, 188); 9 Cleo ✗ (385, 188); 10 Ben (520, 78); 11 Ava ✗ (445, 133); 12 Cleo (530, 133); 13 Ava (530, 188); 14 Dan (530, 243). Nodes 10, 12, 13, 14 are the winning path (node 14 also gets a bold 12px green "✓" after the name).
- **Tree edges:** 2px `#e5e9ef` lines from each parent pill bottom to child pill top (Ava→tries 2/3/7; Cleo(3)→4/6; Ben(4)→5; Dan(7)→8/9; Ben(10)→11/12; Cleo(12)→13; Ava(13)→14); the three winning-path edges 10→12→13→14 drawn 3px `#008300` instead.
- **Undo arrows:** dashed (dash 5/4) 2px orange `#d95926` curved arrows with arrowheads, each labeled 11px orange: "undo 1" from node 5 up to node 4, "undo 2" from node 6 up to node 3, "undo 3" from node 9 up to node 7, "undo 4" from node 7 up to node 1.
- **Legend (11px, top right near x=600, y=40):** blue pill "seated ok", red "✗ rule broken", green "final path".
- **Annotation (bold 12px orange `#d95926`, near x=70, y=282):** "4 undos — even first-pick Ava gets un-seated".

## Why Solvers Live and Die by the Undo

**Tags:** `where it's used` (blue), `pruning` (green), `rule of thumb` (orange)

- **Brute force** — listing all 24 complete seatings and checking each also works, but wastes effort
- **Pruning** — one fail at chair 2 kills every seating that starts that way, sight unseen
- **The blow-up** — 8 friends means 40,320 orderings; backtracking needs only ~400 tries (illustrative)
- **Where you meet it** — Sudoku solvers, exam timetables, regex engines, and constraint solvers all backtrack
- **The habit** — any puzzle shaped like "place items under rules" is a backtracking problem in disguise

*Example (italic):* A Sudoku solver pencils in a 7, hits a contradiction three cells later, erases back to it, and pencils in an 8.

**Key point:** Backtracking wins by refusing to finish a seating that is already broken — the earlier the fail, the bigger the prune.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart on a log scale comparing tries needed: checking every complete seating versus backtracking, for the 4-friend table and an 8-friend table.

- **Title (bold 15px, `#1a5276`, top center):** "Tries Needed: Check Every Seating vs Backtrack".
- **Axis:** log-scale x from 1 to 100,000 mapped to x=60..680 (124px per decade); 2px `#999` baseline at y=252; 11px `#444` tick labels "1", "10", "100", "1k", "10k", "100k" at x = 60, 184, 308, 432, 556, 680; light `#e5e9ef` vertical gridlines at each tick.
- **Group labels (bold 12px `#2c3e50`, left-aligned at x=60):** "4 friends (this page's table)" at y=92, "8 friends" at y=182.
- **Bars (18px tall, starting at x=60, widths from the log mapping):** brute force 24 → blue `#2a78d6` bar y=100, width 171; backtracking 14 → green `#008300` bar y=124, width 142; brute force 40,320 → blue bar y=190, width 571; backtracking ~400 → green bar y=214, width 323.
- **Value labels (bold 12px, just right of each bar end in the bar color; a label that would clip past the canvas edge is drawn inside the bar, right-aligned, in ink `#1a5276`):** "24 seatings", "14 tries", "40,320 seatings" (inside its bar), "~400 tries".
- **Legend (11px, top right near x=560, y=45):** blue swatch "check every seating", green swatch "backtracking".
- **Annotation (bold 12px green `#008300`, near x=400, y=160):** "the gap explodes as the party grows".
- **Caption (12px `#444`, bottom right):** "log scale; 8-friend counts illustrative".

## The Trap of Keeping a Valid Step

**Tags:** `common mistake` (red), `greedy vs backtracking` (orange)

- **The trap** — "the step passed the check, so keep it": valid-so-far does not mean part of the answer
- **Greedy version** — never undo: Ava, Cleo, Ben all seat fine, Dan fails both ways, search stuck
- **Wrong verdict** — the stuck greedy search declares the table impossible when a seating exists
- **The fix** — treat every placement as a pencil mark; undoing is a normal move, not a defeat
- **Not random** — backtracking retries in strict order, so it never repeats a try and never misses one

*Example (italic):* The greedy planner gives up after 5 tries and cancels the dinner; the backtracker un-seats Ben and eats at try 14.

**Common mistake:** Stopping at the first dead end. A dead end only disproves the current partial seating — undo one step and the search continues.

### Visualization (canvas `c4`, 720×300)

Two-lane comparison on the same puzzle: the never-undo searcher's path ending stuck at chair 4, above the backtracker's path reaching the full table.

- **Title (bold 15px, `#1a5276`, top center):** "Same Rules, Two Searchers: Never-Undo Gets Stuck".
- **Lane labels (12px `#6b7280`, left-aligned at x=20):** "never undo" at y=100, "backtracking" at y=205.
- **Lane 1 (pills centered at y=105):** rounded pills 70×24 at x = 170, 260, 350 labeled "Ava", "Cleo", "Ben" (2px `#2a78d6` border, blue text); at x=440 a dashed 2px `#e74c3c` empty pill labeled 12px red "no one fits"; bold 12px `#e74c3c` label at x=440, y=70: "stuck after 5 tries — 'impossible' (wrong)".
- **Lane 2 (pills centered at y=210):** pills 70×24 at x = 170, 260, 350, 440 labeled "Ben", "Cleo", "Ava", "Dan", fill `rgba(0,131,0,0.15)`, 2px `#008300` border, green text; small dashed 2px orange `#d95926` circular loop icon with arrowhead at x=95, y=210 labeled 11px orange "4 undos"; bold 12px `#008300` label at x=440, y=250: "full table at try 14".
- **Connectors:** 2px `#e5e9ef` arrows between consecutive pills in each lane.
- **Divider:** 1px `#e5e9ef` horizontal line across the canvas at y=158.
- **Annotation (bold 13px magenta `#d55181`, centered near x=360, y=285):** "a dead end disproves the guess, not the puzzle".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** no randomness — the c2 tree nodes, try numbers, and undo arrows are the exact hand trace above (rules: Ava–Ben, Cleo–Dan, Ben–Dan not adjacent; chairs filled left to right, friends tried alphabetically; 14 tries, 6 fails, 4 undos, solution Ben, Cleo, Ava, Dan); c3 bar values are the hardcoded numbers 24, 14, 40,320, ~400 with the 8-friend pair labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
