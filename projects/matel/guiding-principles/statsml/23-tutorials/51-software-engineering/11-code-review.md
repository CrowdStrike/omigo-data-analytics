# Code Review

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Code Review

**Subtitle:** A second pair of eyes is excellent at teaching and shaping code, and surprisingly mediocre at finding bugs — knowing which is which changes how a team uses it

## What a Second Pair of Eyes Actually Buys

**Tags:** `core idea` (blue), `knowledge transfer` (green), `what it catches` (orange)

- **The setup** — Priya adds discount logic to the payments service; Sam is assigned the review
- **Knowledge moves** — after the review Sam can debug this code at 2am; Priya learns the team's money idioms
- **Readability pressure** — knowing Sam will read it, Priya renames things and splits functions before pushing
- **Design and gaps** — Sam asks "what if the discount exceeds the price?" — humans are good at spotting what's missing
- **Weak at bugs** — reviewers approve plausible-looking code; subtle-bug catch rates are modest (~15% here, illustrative)
- **Machines first** — style, formatting, and lint go to bots, so the human hour is spent on substance

*Example (italic):* Sam's review renames three functions and reworks the retry design, but the off-by-one in the rounding sails through — a unit test catches it a week later.

**Key point:** Review's proven value is knowledge transfer and maintainability pressure; treating it mainly as a bug filter overrates it — tests and types catch what eyes miss.

### Visualization (canvas `c1`, 720×300)

Grouped horizontal bar chart: catch rate by defect type, human review (blue) vs automated tests (green), showing review winning on design/readability and losing on subtle bugs.

- **Title (bold 15px, `#1a5276`, top center):** "What Review Catches vs What Tests Catch".
- **Legend (12px, below title):** blue swatch "human review", green swatch "automated tests".
- **Layout:** bars start at x=230, max width 440 (= 100%); row labels left-aligned 12px `#444` at x=20; rows at y = 70, 115, 160, 205; per row the review bar sits at y and the tests bar at y+15, each 12px tall.
- **Rows (label: review%, tests% → pixel widths at 4.4 px per point):**
  - "naming & readability": review 70% (width 308, `#2a78d6`), tests 0% (width 0, print "0%" label only)
  - "design-level flaws": review 60% (width 264), tests 10% (width 44, `#008300`)
  - "missing edge cases": review 40% (width 176), tests 60% (width 264)
  - "subtle logic bugs": review 15% (width 66), tests 75% (width 330)
- **Bar style:** review bars solid `#2a78d6`, tests bars solid `#008300`, 11px `#444` percent labels just past each bar end.
- **Annotation (bold 13px red `#e74c3c`, right side near y=250):** "the bug filter is the tests, not the eyes".
- **Caption (12px `#444`, bottom right):** "catch rates illustrative".

## The 1,600-Line PR That Got Three Comments

**Tags:** `worked example` (blue), `diff size` (orange), `rule of thumb` (green)

- **The monster** — Priya ships the whole discount feature as one 1,600-line PR
- **The result** — Sam scrolls for four minutes, leaves three comments, and approves: "LGTM"
- **The rerun** — the same code split into four 400-line PRs draws about 6 comments each, ~26 in total
- **Hand-check** — the curve gives 0.2 comments per 100 lines at 1,600 lines: 0.2 × 16 ≈ 3 comments
- **Hand-check** — at 400 lines it gives 1.6 per 100: 1.6 × 4 ≈ 6 per PR, × 4 PRs ≈ 26 comments
- **The rule** — keep diffs to a few hundred lines; past that, extra lines get skimmed, not read

*Example (italic):* The 1,600-line PR gets three comments and an approval in four minutes; split into four 400-line PRs, the same code draws 26 comments and two caught bugs.

**Key point:** Review attention is roughly fixed per sitting, not per line — so scrutiny per line collapses as the diff grows, and the biggest changes get the least real review.

### Visualization (canvas `c2`, 720×300)

Line chart: reviewer comments per 100 lines against PR size, falling steeply as the diff doubles.

- **Title (bold 15px, `#1a5276`, top center):** "Scrutiny per Line Collapses as the Diff Grows".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = comments per 100 lines, 0 to 4, gridlines `#e5e9ef` at 1/2/3 with 12px `#444` labels; x = PR size with five points evenly spaced at pixel x `[110, 240, 370, 500, 630]`, 12px `#444` tick labels "100", "200", "400", "800", "1,600 lines" (doublings evenly spaced — spacing schematic, not linear).
- **Data line:** blue `#2a78d6` 3px line with 4px filled dots through comments-per-100-lines values `[3.5, 2.8, 1.6, 0.7, 0.2]` at those five x positions.
- **Annotation (bold 13px green `#008300`, near the first point, y=70):** "small diffs get real scrutiny".
- **Annotation (bold 13px red `#e74c3c`, near the last point, y=200):** "0.2 per 100 lines ≈ a shrug and LGTM".
- **Caption (12px `#444`, bottom right):** "comment rates illustrative; the falling shape matches published review-effectiveness studies".

## The Waiting Is the Expensive Part

**Tags:** `why it matters` (blue), `latency` (orange), `team velocity` (green)

- **The round trip** — each review cycle costs roughly a day of calendar time just waiting for a response
- **The math** — Priya's change: 4h coding + 2h addressing comments, but 22h + 18h = 40h sitting in queues
- **The switch tax** — while waiting she starts a second task; now two half-finished changes are in flight
- **The pile-up** — slow reviews push authors to batch work into bigger PRs, which review even worse
- **The fix** — small PRs plus a team norm like "first review response within 4 working hours"

*Example (italic):* A change that took 4 hours to write merges 46 hours after its first keystroke — 87% of its life was spent waiting, not being worked on.

**Key point:** Review latency is a velocity tax paid on every merge; cutting queue time with small PRs and fast first responses buys more than any review checklist.

### Visualization (canvas `c3`, 720×300)

Single horizontal stacked timeline bar: the 46-hour life of one PR, work segments in blue and waiting segments in grey.

- **Title (bold 15px, `#1a5276`, top center):** "Life of a 4-Hour Change: 46 Hours from First Keystroke to Merge".
- **Bar:** one horizontal bar at y=130, height 36, starting x=60, total width 600 for 46 hours (≈13.0 px per hour); segments left to right:
  - "code — 4h": blue `#2a78d6`, width 52
  - "waiting for review — 22h": grey `rgba(107,114,128,0.25)` with 1px `#6b7280` border, width 287
  - "fix comments — 2h": blue `#2a78d6`, width 26
  - "waiting for re-review — 18h": grey `rgba(107,114,128,0.25)`, width 235
- **Segment labels:** 12px `#444` above the bar for the two waits, below the bar for the two work segments; hour counts included in the labels.
- **Day marker:** vertical dashed `#6b7280` (dash 4/3) line at the 24-hour mark (x≈373), 12px `#6b7280` label "day 2 starts" at its top.
- **Annotation (bold 13px red `#e74c3c`, centered near y=220):** "6 of 46 hours are work — 87% is queue".
- **Caption (12px `#444`, bottom right):** "hours illustrative".

## When Review Becomes a Ritual

**Tags:** `common mistake` (red), `review culture` (orange)

- **The ritual** — ownership rules ("payments code needs a payments approver") decay into checkbox clicking
- **Rubber stamp** — the reviewer approves plausible-looking code, and plausible is exactly what subtle bugs look like
- **On the code** — "this function re-reads the file on every call" lands; "you wrote slow code" doesn't
- **Questions first** — "what happens when the list is empty?" teaches more than "handle the empty list"
- **Nits don't block** — approve-with-nits keeps trivia from costing another day-long round trip

*Example (italic):* The payments owner approves Priya's 1,600-line PR in four minutes to unblock her — the required-reviewer rule was satisfied, and nobody actually read the diff.

**Common mistake:** Measuring review health by approvals granted. An approval on a diff nobody could realistically read is a signature, not scrutiny — the fix is smaller diffs and reviewers who ask questions, not stricter sign-off rules.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a ritual approval that lets a bug ship vs a small-PR review that asks a question and catches it.

- **Title (bold 15px, `#1a5276`, top center):** "Ritual Approval vs Review with Teeth".
- **Row 1 (y=95), label 12px `#444` at x=20:** "ritual"; blue `#2a78d6` rounded box at x=110 labeled "1,600-line PR" (12px), 3px arrow to a grey box at x=300 labeled "owner approves — 4 min", 3px arrow to a red `#e74c3c` box at x=520 labeled "rounding bug ships" with bold 12px red "✗" beside it.
- **Row 2 (y=205), label:** "with teeth"; blue box at x=110 "400-line PR", 3px arrow to a green `#008300` box at x=300 labeled "Q: what if qty = 0?", 3px arrow to a green box at x=520 labeled "test added, merged" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(107,114,128,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "an approval is a signature, not a guarantee of scrutiny".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); catch rates, comment rates, and hour breakdowns are invented and labeled illustrative; the arithmetic in the text (0.2×16≈3 comments, 1.6×4×4≈26 comments, 6 of 46 hours = 87% waiting) must match the chart values exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
