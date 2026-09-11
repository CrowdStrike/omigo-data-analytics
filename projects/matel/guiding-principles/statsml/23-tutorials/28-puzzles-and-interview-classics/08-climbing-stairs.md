# Climbing Stairs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Climbing Stairs

**Subtitle:** Count the ways to climb a flight when every move is one step or two — the counts are Fibonacci in disguise, and spotting that recurrence is the whole trick

## One Step or Two on the Way Home

**Tags:** `core idea` (blue), `recurrence` (green), `counting paths` (orange)

- **The flight** — a walk-up apartment building, 10 steps per flight; the climber takes one step or two per move
- **The question** — how many different move sequences reach the top? 1+2+1 and 2+1+1 count as different ways
- **Tiny cases** — 1 step: 1 way; 2 steps: 2 ways (1+1 or 2); 3 steps: 3 ways (1+1+1, 1+2, 2+1)
- **The last move** — every climb of 4 steps ends with a 1-step from step 3 or a 2-step from step 2
- **The recurrence** — so ways(4) = ways(3) + ways(2) = 3 + 2 = 5; every higher step obeys the same rule

*Example (italic):* To land on step 4 you must have just left step 3 or step 2 — so the 5 ways up 4 steps are exactly the 3 ways up 3 steps plus the 2 ways up 2 steps.

**Key point:** Ask "what was the last move?" — when the answer splits today's count into smaller counts you already know, you have found a recurrence.

### Visualization (canvas `c1`, 720×300)

Row diagram enumerating all 5 ways up a 4-step flight as horizontal block sequences, grouped by the last move to make the recurrence ways(4) = ways(3) + ways(2) visible.

- **Title (bold 15px, `#1a5276`, top center):** "All 5 Ways Up 4 Steps — Grouped by the Last Move".
- **Rows:** five rows at y = 78, 112, 146, 196, 230 (extra gap between y=146 and y=196 separates the two groups); each row's track runs x=180 to x=480 (75px per stair unit); left-aligned 12px `#444` sequence label at x=60: "1+1+1+1", "2+1+1", "1+2+1", then "1+1+2", "2+2".
- **Blocks:** each move drawn as a rounded 20px-tall block centered on its row — a 1-step move is 71px wide, a 2-step move 146px (4px gaps); 1-step blocks fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border, 2-step blocks fill `rgba(217,89,38,0.30)` with 2px `#d95926` border; bold 12px `#1a5276` "1" or "2" centered in each block.
- **Group labels (right of the tracks, x=500):** rows 1–3 get bold 12px blue `#2a78d6` two-line label "last move: 1-step" / "= the 3 ways up 3 steps"; rows 4–5 get bold 12px orange `#d95926` two-line label "last move: 2-step" / "= the 2 ways up 2 steps".
- **Annotation (bold 13px green `#008300`, bottom center near y=278):** "ways(4) = ways(3) + ways(2) = 3 + 2 = 5".
- **Caption (11px `#444`, bottom right):** "exact enumeration — every sequence listed".

## Building the Table to Step 10

**Tags:** `worked example` (blue), `fibonacci` (green)

- **Two seeds** — start the table with ways(1) = 1 and ways(2) = 2; everything else follows from them
- **The rule** — each new entry is the sum of the previous two: ways(3) = 2 + 1 = 3, ways(4) = 3 + 2 = 5
- **Filling forward** — 1, 2, 3, 5, 8, 13, 21, 34, 55, 89: ten rows of plain addition and the table is done
- **The answer** — a 10-step flight can be climbed in exactly 89 different ways
- **The disguise** — those are the Fibonacci numbers; the stair puzzle never says so out loud

*Example (italic):* ways(10) = ways(9) + ways(8) = 55 + 34 = 89 — checkable with nothing but addition.

**Key point:** The whole solution is a ten-row addition table — recognizing the recurrence replaced brute-force listing of 89 separate paths.

### Visualization (canvas `c2`, 720×300)

Bar chart of ways(n) for flights of 1 to 10 steps, with the last bar highlighted and arrows showing it is the sum of the two bars before it.

- **Title (bold 15px, `#1a5276`, top center):** "Ways to Climb n Steps: 1, 2, 3, 5, 8, 13, 21, 34, 55, 89".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = steps 1 to 10 with 12px `#444` tick labels "1"–"10" (bar centers at x = 90, 150, 210, 270, 330, 390, 450, 510, 570, 630); 12px `#444` axis title "steps in the flight" centered below at y=278; y = 0 to 90 with light `#e5e9ef` gridlines at 20, 40, 60, 80 and 11px `#6b7280` labels.
- **Bars:** width 36px, heights from hardcoded values `[1, 2, 3, 5, 8, 13, 21, 34, 55, 89]`; bars 1–9 fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; bar 10 fill `rgba(0,131,0,0.25)` with 2px `#008300` border; bold 12px value label above each bar (ink `#1a5276`, bar 10's "89" in green `#008300`).
- **Arrows:** two thin 2px green `#008300` arrows from the tops of bar 8 (34) and bar 9 (55) to the top of bar 10, with small arrowheads.
- **Annotation (bold 13px green `#008300`, near x=150, y=80):** "each bar = sum of the two before it: 89 = 55 + 34".

## Why Spotting the Recurrence Pays

**Tags:** `where it's used` (blue), `dynamic programming` (green), `interviews` (orange)

- **The interview** — this puzzle appears constantly because it tests one skill: seeing the recurrence
- **Naive recursion** — computing ways(10) by re-branching on every call takes 109 calls, not 10
- **Repeated work** — the naive call tree recomputes ways(6) five times and ways(4) thirteen times
- **The fix** — fill the table bottom-up instead: 10 fills, one per step (dynamic programming)
- **It scales** — at 30 steps the naive tree needs about 1.7 million calls; the table still needs 30

*Example (italic):* The same last-move trick counts paths through grids, ways to make change, and ways to tile a hallway with 1-foot and 2-foot boards.

**Key point:** A recurrence is only half the win — storing each answer once turns an exploding call tree into a straight line of additions.

### Visualization (canvas `c3`, 720×300)

Two-line chart comparing the work to compute ways(n): calls made by naive recursion versus entries filled by a bottom-up table, for n = 1 to 10.

- **Title (bold 15px, `#1a5276`, top center):** "Same Answer, Different Work: Naive Calls vs Table Fills".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = steps 1 to 10, points at x = 120, 180, 240, 300, 360, 420, 480, 540, 600, 660, 12px `#444` tick labels "1"–"10"; y = 0 to 110 with light `#e5e9ef` gridlines at 25, 50, 75, 100 and 11px `#6b7280` labels.
- **Naive line:** orange `#d95926` 3px line with 5px dots through hardcoded call counts `[1, 1, 3, 5, 9, 15, 25, 41, 67, 109]`; bold 12px orange label "naive recursion" above the line near n=8; bold 13px orange label "109 calls" just above the last point.
- **Table line:** green `#008300` 3px line with 5px dots through `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`; bold 12px green label "bottom-up table" below the line near n=8; bold 13px green label "10 fills" to the right of the last point.
- **Annotation (bold 12px violet `#4a3aa7`, near x=180, y=95):** two lines: "same recurrence, same answer —" / "wildly different work".
- **Caption (12px `#444`, bottom right):** "call counts exact for the 1-or-2-step recurrence".

## Mixes Are Not Ways

**Tags:** `common mistake` (red), `order matters` (orange)

- **The trap** — "up 4 steps I can use zero, one, or two double-steps, so 3 ways" — which is wrong
- **Order counts** — one double-step can come first, middle, or last: 2+1+1, 1+2+1, 1+1+2 are 3 ways
- **The tally** — zero doubles: 1 ordering; one double: 3 orderings; two doubles: 1 — total 5, not 3
- **Off-by-one** — ways(n) equals Fibonacci(n+1), so quoting Fib(n) silently shifts every answer down
- **Check tiny cases** — ways(1) = 1 and ways(2) = 2 catch both mistakes before they reach step 10

*Example (italic):* Two climbers both "solved" the 4-step flight and reported 3 and 5 — the first counted step-mixes, the second counted ordered move sequences.

**Common mistake:** Counting which moves were used instead of the order they were made in — the recurrence counts ordered sequences, and only tiny hand-checked cases expose the difference.

### Visualization (canvas `c4`, 720×300)

Three-column chip chart for the 4-step flight: sequences grouped by how many 2-steps they use, showing 3 mixes but 5 ordered ways.

- **Title (bold 15px, `#1a5276`, top center):** "4 Steps: 3 Step-Mixes, 5 Ordered Ways".
- **Baseline:** horizontal 2px `#999` line at y=245 from x=80 to x=680; column footer labels 12px `#444` at y=270, centered under each column: "zero 2-steps", "one 2-step", "two 2-steps".
- **Columns (centers at x = 170, 390, 610):** each sequence drawn as a rounded 120×24 chip, fill `rgba(42,120,214,0.12)`, 1px `#2a78d6` border, bold 12px `#1a5276` text centered — column 1: one chip at y=210 ("1+1+1+1"); column 2: three chips stacked at y = 210, 178, 146 ("2+1+1", "1+2+1", "1+1+2"); column 3: one chip at y=210 ("2+2").
- **Count labels (bold 13px `#1a5276`, centered above each stack):** "1 way" at y=190 over column 1, "3 ways" at y=126 over column 2, "1 way" at y=190 over column 3.
- **Annotation (bold 13px magenta `#d55181`, centered near y=70):** "3 step-mixes, but 5 ways — the order of the moves counts".
- **Caption (11px `#444`, bottom right):** "exact enumeration for the 4-step flight".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all sequences, bar heights, and line points are the hardcoded arrays above (no randomness); ways counts `[1, 2, 3, 5, 8, 13, 21, 34, 55, 89]` and naive call counts `[1, 1, 3, 5, 9, 15, 25, 41, 67, 109]` are exact values of the recurrences ways(n) = ways(n-1) + ways(n-2) and calls(n) = calls(n-1) + calls(n-2) + 1; text numbers must match chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
