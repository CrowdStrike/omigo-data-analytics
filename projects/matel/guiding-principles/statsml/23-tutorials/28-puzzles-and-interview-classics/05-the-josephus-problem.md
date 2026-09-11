# The Josephus Problem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Josephus Problem

**Subtitle:** Kids in a circle, every second one tapped out — and the winning seat pops out of one binary move: write the head count in binary and shift the leading 1 to the end

## Ten Kids, One Winner

**Tags:** `core idea` (blue), `elimination circle` (green), `binary trick` (orange)

- **The game** — ten kids stand in a circle at a party; going around, every second kid is tapped out
- **The prize** — the last kid standing wins, and everyone wants to know the winning seat in advance
- **The surprise** — for 10 kids the winner is seat 5: not seat 1, not seat 10, and it never varies
- **The trick** — write 10 in binary, 1010; move the front 1 to the back: 0101, which is 5
- **Works for any count** — 6 kids: 110 → 101 = seat 5; 100 kids: 1100100 → 1001001 = seat 73

*Example (italic):* At a party with 10 kids the kid in seat 5 wins every single time — and 10 in binary (1010), shifted once (0101), says exactly that.

**Key point:** The winning seat comes from one binary move: write the number of kids in binary and rotate the leading 1 to the end.

### Visualization (canvas `c1`, 720×300)

Single-panel circle diagram: ten seats around a ring, eliminated seats grayed with small red order badges, the surviving seat 5 in bold green, and the binary-shift answer stated beside the ring.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Kids in a Circle, Every Second One Out — Seat 5 Wins".
- **Ring:** light `#e5e9ef` 2px circle, center (280, 165), radius 100.
- **Seat dots (10px), seat labels bold 12px `#2c3e50` placed 20px outside the ring, at hardcoded coordinates for seats 1–10:** `[[280,65],[339,84],[375,134],[375,196],[339,246],[280,265],[221,246],[185,196],[185,134],[221,84]]`.
- **Eliminated seats (2, 4, 6, 8, 10, 3, 7, 1, 9):** gray `#6b7280` hollow dots; small red `#e74c3c` 11px badge just inside the ring giving the tap order "1"–"9" (seat 2 tapped 1st ... seat 9 tapped 9th).
- **Survivor seat 5:** green `#008300` filled 13px dot with a 2px green halo ring; bold 13px green label "seat 5 wins" beside it.
- **Annotation (bold 13px orange `#d95926`, right side, three lines starting near x=490, y=115):** "10 = 1010" / "shift front 1 to back" / "0101 = 5".
- **Legend (11px `#6b7280`, bottom left):** "red number = tap order".
- **Caption (12px `#444`, bottom right):** "exact — the same seat wins every replay".

## Crossing Off Seats, Pass by Pass

**Tags:** `worked example` (blue), `do it by hand` (green)

- **Seats 1–10** — the kids sit in seats 1 to 10; counting starts at seat 1, so seat 2 is the first out
- **First lap** — the evens fall in order: 2, 4, 6, 8, 10 are tapped; five remain: 1, 3, 5, 7, 9
- **Second lap** — counting continues past 10: skip 1, tap 3; skip 5, tap 7; three left: 1, 5, 9
- **Final laps** — skip 9, tap 1; then skip 5, tap 9 — seat 5 is the last kid standing
- **Formula check** — 10 = 8 + 2, so the leftover is 2 and the winner is 2×2 + 1 = 5; binary agrees

*Example (italic):* Redo it with 6 kids: out go 2, 4, 6, then 3, then 1 — seat 5 wins, matching 110 → 101 = 5.

**Key point:** Nine taps, all checkable on fingers, land on seat 5 — exactly the seat the binary shift predicted before the game started.

### Visualization (canvas `c2`, 720×300)

Grid-of-passes chart: five rows of ten seat markers on shared columns, one row per stage of the game, showing the circle thinning from ten kids down to the lone seat 5.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Kids Thin Out to One: Seat 5".
- **Columns:** seats 1–10 at x = `[140, 192, 244, 296, 348, 400, 452, 504, 556, 608]`; bold 12px `#2c3e50` seat numbers as a header row at y=62.
- **Rows (y = 95, 135, 175, 215, 255), each with a left-aligned 12px `#444` label at x=8:**
  - "start — all ten in": alive `[1,2,3,4,5,6,7,8,9,10]`
  - "lap 1 taps 2,4,6,8,10": alive `[1,3,5,7,9]`, just-tapped `[2,4,6,8,10]`
  - "lap 2 taps 3,7": alive `[1,5,9]`, just-tapped `[3,7]`
  - "lap 3 taps 1": alive `[5,9]`, just-tapped `[1]`
  - "lap 4 taps 9 — 5 wins": alive `[5]`, just-tapped `[9]`
- **Marker style:** alive = blue `#2a78d6` filled 9px dot; just-tapped this row = red `#e74c3c` X (2px strokes, 10px arms); already out = light gray `#d0d5dc` hollow 7px dot; final surviving seat 5 = green `#008300` filled 12px dot.
- **Annotation (bold 13px green `#008300`, near x=430, y=282):** "last one standing: seat 5 = 2×2+1".
- **Caption (11px `#6b7280`, bottom left):** "exact tap-by-tap record — redo it on paper".

## Powers of Two Reset the Game

**Tags:** `where it's used` (blue), `pattern` (green), `sawtooth` (orange)

- **The reset** — with exactly 2, 4, 8, or 16 kids, seat 1 always wins; a full power-of-two lap lands back at the start
- **Between resets** — from each power of two, the winner climbs by 2 per extra kid: 1, 3, 5, 7, ...
- **The formula** — write the count as (biggest power of two) + leftover L; the winner is seat 2L + 1
- **Binary view** — dropping the leading 1 subtracts the power of two; appending it computes 2L + 1
- **Where it shows up** — a classic interview puzzle, and a model case for recurrences and thinking in binary

*Example (italic):* With 100 kids, 100 = 64 + 36, so seat 2×36 + 1 = 73 wins — one subtraction, one doubling, no acting it out.

**Key point:** The winner's pattern is a sawtooth — reset to seat 1 at every power of two, then climb by 2 — and the binary shift encodes that whole sawtooth in one move.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: winning seat versus number of kids for 1 to 16 players, a sawtooth that crashes to 1 at every power of two, with the page's n=10 point highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Winning Seat vs Number of Kids: a Sawtooth That Resets at 2, 4, 8, 16".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 185; x = number of kids 1 to 16 with 12px `#444` tick labels "1"–"16" under every point; y = winning seat 0 to 16 with light `#e5e9ef` gridlines and 12px `#444` labels at 1, 5, 9, 13; axis captions 12px `#6b7280`: "kids in the circle" (bottom center), "winning seat" (rotated, left).
- **Series (hardcoded, exact):** kids = `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]`, winner = `[1, 1, 3, 1, 3, 5, 7, 1, 3, 5, 7, 9, 11, 13, 15, 1]`; blue `#2a78d6` 2px connecting line with 6px blue dots.
- **Power-of-two resets:** the points at kids = 2, 4, 8, 16 drawn as orange `#d95926` 7px dots; vertical dashed `#6b7280` (dash 4/3) guide lines at those x positions from baseline to y=70.
- **Highlight n=10:** green `#008300` 8px dot at (10, 5) with bold 13px green label "10 kids → seat 5" above it.
- **Annotation (bold 12px orange `#d95926`, near x=13.5 in data coords, y=90):** two lines: "crashes to seat 1 at" / "every power of two".
- **Caption (12px `#444`, bottom right):** "exact values — no simulation needed".

## Who Gets Tapped First?

**Tags:** `common mistake` (red), `off-by-one` (orange)

- **The ambiguity** — "every second kid" hides a choice: does the count start by skipping seat 1 or tapping it?
- **One word, new winner** — with 6 kids, skip-then-tap crowns seat 5; tap-then-skip crowns seat 4
- **The standard rule** — the classic Josephus setup skips seat 1 first, so seat 2 is the first one out
- **Bigger steps break it** — tapping every third kid has no clean shift trick; the binary answer is for step 2 only
- **Check tiny cases** — before trusting any formula, act out 2 and 3 kids by hand under your exact rule

*Example (italic):* Two friends played the same 6-kid circle, disagreed on who counts first, and crowned seat 5 and seat 4 — both did the arithmetic right.

**Common mistake:** Applying the binary-shift answer to a variant it doesn't cover — a different first tap or a step other than 2 changes the winner, so pin the rule down before shifting bits.

### Visualization (canvas `c4`, 720×300)

Two-row seat chart for 6 kids on shared columns: the same circle played under the two counting rules, tap-order numbers under every seat, showing the winner move from seat 5 to seat 4.

- **Title (bold 15px, `#1a5276`, top center):** "Same 6 Kids, Two Readings of 'Every Second' — Two Different Winners".
- **Columns:** seats 1–6 at x = `[260, 332, 404, 476, 548, 620]`; bold 12px `#2c3e50` seat numbers as a header row at y=72.
- **Row 1 (y=125), label 12px `#444` at x=12:** "skip first, then tap (classic)"; tap order under seats 1–6 = `[5, 1, 4, 2, "-", 3]` (11px red `#e74c3c`, "-" for the survivor); seats 1, 2, 3, 4, 6 = gray `#6b7280` hollow 9px dots; seat 5 = green `#008300` filled 12px dot with bold 13px green label "seat 5 wins" above.
- **Row 2 (y=215), label:** "tap first, then skip"; tap order under seats 1–6 = `[1, 4, 2, "-", 3, 5]`; seats 1, 2, 3, 5, 6 = gray hollow dots; seat 4 = orange `#d95926` filled 12px dot with bold 13px orange label "seat 4 wins" above.
- **Divider:** light `#e5e9ef` 1px horizontal line at y=170 between the rows.
- **Annotation (bold 13px magenta `#d55181`, centered near y=282):** "one word in the rule moved the crown — 110 → 101 = 5 assumes the classic rule".
- **Caption (11px `#6b7280`, bottom right):** "exact tap orders for both rules".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all seat coordinates, tap orders, and the winner-vs-kids series are the hardcoded literal arrays above (no randomness); every number is exact (survivor of every-second-person elimination), so captions say "exact" rather than "illustrative".
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
