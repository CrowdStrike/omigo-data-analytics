# Refactoring

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Refactoring

**Subtitle:** Changing a program's internal structure without changing what it does — proven by tests that pass before and after every small step

## The 118-Line Invoice Function

**Tags:** `core idea` (blue), `behavior preserved` (green), `Fowler` (orange)

- **The mess** — a billing codebase has one 118-line `computeInvoice` function nobody wants to touch
- **The output** — for order #4471 it returns $86.40; every caller depends on exactly that answer
- **The move** — the team splits it into subtotal, discount, and tax pieces; order #4471 still returns $86.40
- **The proof** — a test asserting $86.40 passes before the change and after; that pass IS the definition
- **The book** — Martin Fowler's *Refactoring* (1999) named the practice and catalogued the moves
- **No tests?** — then you are not refactoring, just changing things and hoping

*Example (italic):* After the split, order #4471 still bills $86.40 to the cent — the outside world cannot tell anything happened.

**Key point:** Refactoring means improving internal structure while preserving external behavior, verified by tests that pass before and after — Fowler's strict definition, which people routinely dilute.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same input flowing to the same output through a monolith (before) and through three named pieces (after).

- **Title (bold 15px, `#1a5276`, top center):** "Same Input, Same Output — Only the Insides Change".
- **Row 1 (boxes centered at y=100), label 12px `#444` at x=20:** "before"; blue `#2a78d6` rounded box at x=130 labeled "order #4471" (12px), 3px arrow to a wide orange `#d95926` box at x=300 (200px wide) labeled "computeInvoice — 118 lines", 3px arrow to a green `#008300` box at x=580 labeled "$86.40".
- **Row 2 (boxes centered at y=210), label:** "after"; identical blue "order #4471" box at x=130, arrows fanning to three stacked violet `#4a3aa7` boxes at x=300 (each 130px wide, 26px tall, at y=180/210/240) labeled "subtotal()", "discountFor()", "taxFor()", arrows converging to an identical green "$86.40" box at x=580.
- **Box style:** 40px tall (26px for the three small ones), 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, right side near y=155):** "output identical: $86.40 both rows".
- **Caption (12px `#444`, bottom right):** "order and amount illustrative".

## Six Commits, Green After Every One

**Tags:** `worked example` (blue), `small steps` (green)

- **The moves** — extract function, rename, inline, move: small, named, reversible transformations
- **The sequence** — extract `taxFor` (118→96 lines), extract `discountFor` (96→74), rename, move, extract `subtotal` (74→52), inline a temp (52→41)
- **The rhythm** — run tests after each move and commit at every green state, six commits in one sitting
- **The misstep** — the first try at the move breaks one caller; tests go red, `git reset` undoes just that step
- **The payoff** — a mistake costs one small commit, not one afternoon of untangling a big-bang change

*Example (italic):* The longest function shrinks 118 → 96 → 74 → 74 → 74 → 52 → 41 lines across six commits, and the one red step is undone in thirty seconds.

**Key point:** Refactoring is composed of tiny named transformations with a test run between each — committing at every green state means any misstep rolls back one step, never one afternoon.

### Visualization (canvas `c2`, 720×300)

Step line chart: longest-function line count across commits 0–6, green dots at each passing commit, one red X for the reset misstep.

- **Title (bold 15px, `#1a5276`, top center):** "Six Small Commits: Tests Green at Every Step".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = commit 0 to 6, 12px `#444` tick labels "start", "1", "2", "3", "4", "5", "6"; y = lines in longest function 0 to 120, gridlines `#e5e9ef` at 30/60/90.
- **Main line:** blue `#2a78d6` 3px step line through commits `[0, 1, 2, 3, 4, 5, 6]`, lines `[118, 96, 74, 74, 74, 52, 41]`.
- **Green dots:** 6px filled `#008300` circles at every point of the main line; 11px `#008300` label "✓ tests pass" under the dot at commit 1.
- **Misstep marker:** red `#e74c3c` bold 16px "✗" floated at (commit 3.5, y mapped to 74) with a short red dashed (4/3) loop-back arrow to commit 3, 12px red label "first try of the move broke a caller — reset to commit 3".
- **Step labels (11px `#6b7280`, above each drop):** "extract taxFor", "extract discountFor", "rename", "move", "extract subtotal", "inline temp" at commits 1, 2, 3, 4, 5, 6.
- **Annotation (bold 13px green `#008300`, near commit 5, y=100):** "118 → 41 lines, behavior untouched".
- **Caption (12px `#444`, bottom right):** "line counts illustrative".

## Pay Principal Where the Edits Are

**Tags:** `where it's used` (blue), `economics` (green), `code smells` (orange)

- **The triggers** — code smells name what to fix: duplication, long functions, feature envy
- **The debt** — messy structure taxes every future change; each edit in the mess costs ~25 extra minutes
- **The math** — checkout gets 34 edits a quarter: 34 × 25 = 850 min of tax; legacy-export gets 1 edit: 25 min
- **The rule** — a ~300-minute cleanup pays for itself in checkout within weeks; in legacy-export, never
- **Boy-scout rule** — leave code you touch slightly better than found; save dedicated efforts for hot spots
- **Not aesthetics** — refactor the code you change weekly, leave the ugly-but-stable corner alone

*Example (italic):* Cleaning checkout recovers 850 minutes of tax per quarter for a one-time 300-minute cost; the same cleanup on legacy-export would take 12 quarters to break even.

**Key point:** Refactoring is an economic activity, not aesthetics — the debt metaphor says pay principal where change is frequent, because interest is only charged on code you actually edit.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: quarterly "mess tax" per module (edits × 25 min each) against the flat one-time cleanup cost, showing where refactoring pays.

- **Title (bold 15px, `#1a5276`, top center):** "Mess Tax per Quarter vs a 300-Minute Cleanup".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; x scale 0 to 880 minutes, 11px `#6b7280` tick labels at 0/300/600 min.
- **Rows (bar centers at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "checkout — 34 edits/qtr": blue `#2a78d6` bar width 425 (850 min tax), 11px label "850 min"
  - "pricing — 21 edits/qtr": blue bar width 262 (525 min), label "525 min"
  - "reports — 6 edits/qtr": blue bar width 75 (150 min), label "150 min"
  - "legacy-export — 1 edit/qtr": blue bar width 12 (25 min), label "25 min"
- **Cleanup line:** vertical dashed `#d95926` (dash 4/3) line at x = 230 + 150 (the 300-min mark), bold 12px `#d95926` label "one-time cleanup: 300 min" at its top.
- **Verdicts (bold 12px at each bar end):** green `#008300` "refactor" on checkout and pricing rows; mute `#6b7280` "leave alone" on reports and legacy-export rows.
- **Bar style:** 16px tall, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge.
- **Annotation (bold 13px magenta `#d55181`, bottom center near y=270):** "interest is only charged on code you actually edit".
- **Caption (12px `#444`, bottom right):** "edit counts and 25 min/edit tax illustrative".

## The Refactor That Wasn't

**Tags:** `common mistake` (red), `behavior drift` (orange)

- **Silent drift** — a "refactor" changes rounding and $86.40 quietly becomes $86.00; only a test catches it
- **The euphemism** — calling a rewrite a "refactor" hides its scope; a rewrite replaces behavior, on purpose
- **Dead code** — refactoring a module scheduled for deletion polishes something about to be thrown away
- **Speculation** — cleaning untouched code pays principal on a loan nobody is paying interest on
- **The tell** — if you cannot say which test proves behavior held, the word refactoring does not apply

*Example (italic):* The no-test "refactor" ships the $86.00 bug and it surfaces as customer complaints three weeks later; with the test it dies red on commit 2, thirty seconds in.

**Common mistake:** Using "refactoring" for any code change. Without tests passing before and after it is unverified change; when scope secretly grows it is a rewrite; and ahead of a deletion or on untouched code it is effort with no payback.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same rounding misstep with no tests (bug ships) vs with tests (caught at commit 2), shown as boxes flowing left to right.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Misstep, With and Without a Safety Net".
- **Row 1 (boxes centered at y=95), label 12px `#444` at x=20:** "no tests"; blue `#2a78d6` rounded box at x=150 labeled "change rounding" (12px), 3px arrow to an orange `#d95926` box at x=340 labeled "$86.40 → $86.00, unnoticed", 3px arrow to a red `#e74c3c` box at x=560 labeled "complaints, week 3" with bold 12px red "✗ bug shipped".
- **Row 2 (boxes centered at y=205), label:** "with tests"; identical blue "change rounding" box at x=150, arrow to a red-edged box at x=340 labeled "test expects 86.40, gets 86.00" with bold 12px red "red in 30s", arrow to a green `#008300` box at x=560 labeled "revert 1 commit" with bold 12px green "✓".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the tests are what make it refactoring instead of hoping".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the $86.40 invoice, the line-count sequence 118/96/74/74/74/52/41, and the edit counts 34/21/6/1 at 25 min tax per edit (850/525/150/25 min against a 300-min cleanup) are invented and labeled illustrative; text numbers must match chart numbers exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
