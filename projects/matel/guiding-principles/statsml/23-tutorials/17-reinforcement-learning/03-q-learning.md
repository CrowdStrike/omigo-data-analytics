# Q-Learning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Q-Learning

**Subtitle:** A robot keeps a scorecard of how good each move is in each room — and by nudging one score after every single step, the scorecard alone teaches it the way to the cheese

## A Robot, Four Rooms, and a Scorecard

**Tags:** `core idea` (blue), `Q-table` (green), `trial and error` (orange)

- **The maze** — a toy robot sits in room 1 of a four-room hallway; the cheese waits in room 4
- **No map** — nobody tells the robot where the cheese is; it only feels a reward when it arrives
- **The rewards** — stepping into room 4 pays +10; every other step pays exactly 0
- **The Q-table** — a scorecard with one score per (room, move): rooms 1–3, moves left or right
- **All zeros** — the scorecard starts blank; every score is 0, so at first every move looks equal
- **The promise** — with enough wandering, each score becomes the cheese ahead, shrunk a bit per step

*Example (italic):* The robot in room 2 checks two scores — Q(room 2, left) and Q(room 2, right) — and walks toward the bigger one; that lookup is the whole decision.

**Key point:** Q-learning stores one number per (situation, action) pair — the scorecard IS the strategy: in any room, just take the move with the higher score.

### Visualization (canvas `c1`, 720×300)

Two-part diagram: the four-room hallway drawn as boxes on top, and the starting Q-table drawn as a small grid of zeros below it.

- **Title (bold 15px, `#1a5276`, top center):** "The Maze and the Blank Scorecard".
- **Hallway (top band, y=55–130):** four 130×75 rounded boxes at x = 80, 230, 380, 530; 2px `#1a5276` borders, fill `#f8f9fa` except room 4 fill `rgba(0,131,0,0.12)`; bold 13px `#1a5276` labels "room 1"–"room 4" centered in each; a 20px robot dot (blue `#2a78d6` circle with two 11px white eye dots) in room 1; bold 13px green `#008300` text "CHEESE +10" centered in room 4.
- **Reward labels:** 12px `#6b7280` text "reward 0" under rooms 1–3 gaps, bold 12px green "reward +10" under the arrow into room 4; thin `#999` arrows between adjacent boxes pointing both ways.
- **Q-table (bottom band, y=185–265):** 3 rows × 2 columns of 90×26 cells starting at x=250; column headers bold 12px `#1a5276` "move left" / "move right"; row labels 12px `#444` "room 1", "room 2", "room 3" at x=175; every cell shows "0.0" in 13px `#6b7280`, cell borders 1px `#e5e9ef`.
- **Annotation (bold 13px orange `#d95926`, right of the table near x=520, y=215):** two lines: "one score per (room, move) —" / "all zeros before the first trip".
- **Caption (12px `#444`, bottom right):** "illustrative — a 4-room maze keeps the whole table visible".

## Two Trips, Checked By Hand

**Tags:** `worked example` (blue), `the update rule` (green)

- **The rule** — new score = old score + 0.5 × (reward + 0.9 × best score in the next room − old score)
- **Two dials** — 0.5 is the learning rate (move halfway to the news); 0.9 discounts rewards one step away
- **Trip 1, room 3 → cheese** — Q(3, right) = 0 + 0.5 × (10 + 0 − 0) = 5.0; the win is written down
- **Trip 1, rooms 1 and 2** — reward 0 and next-room scores still 0, so their updates stay at 0.0
- **Trip 2, room 2 → room 3** — Q(2, right) = 0 + 0.5 × (0 + 0.9 × 5.0 − 0) = 2.25; credit flows backward
- **Trip 2, room 3 again** — Q(3, right) = 5.0 + 0.5 × (10 − 5.0) = 7.5; the score creeps toward 10

*Example (italic):* Room 2 never sees cheese directly — its 2.25 score exists only because room 3's score became 5.0 first; good news travels one room per trip.

**Key point:** One update touches one cell using only (reward now) + (0.9 × best score next door) — no map, no model of the maze, just repeated local bookkeeping.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: the scores Q(1, right), Q(2, right), Q(3, right) shown at three moments — before any trip, after trip 1, after trip 2 — so the backward flow of credit is visible.

- **Title (bold 15px, `#1a5276`, top center):** "Two Trips of Updates: the Cheese News Walks Backward".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; y = score 0 to 10 with light `#e5e9ef` gridlines at 2.5, 5, 7.5, 10 and 12px `#444` tick labels "0", "2.5", "5", "7.5", "10"; x = three groups centered at x=170, 370, 570 labeled bold 13px `#444` "Q(room 1, right)", "Q(room 2, right)", "Q(room 3, right)".
- **Bars:** three 38px-wide bars per group (start / after trip 1 / after trip 2), values `[0, 0, 0]`, `[0, 0, 2.25]`, `[0, 5.0, 7.5]`; fills mute `rgba(107,114,128,0.35)`, blue `rgba(42,120,214,0.55)`, green `rgba(0,131,0,0.55)` with matching 2px borders; zero-height bars drawn as a 3px stub so all bars are visible.
- **Value labels:** bold 12px above each nonzero bar in the bar's border color: "2.25", "5.0", "7.5"; 11px `#6b7280` "0.0" above zero stubs.
- **Legend (top right, 12px):** three swatches — mute "before any trip", blue "after trip 1", green "after trip 2".
- **Arrow:** 2.5px dashed orange `#d95926` arrow from the top of the 5.0 bar (group 3) to the top of the 2.25 bar (group 2), with bold 12px orange annotation above its midpoint: "0.5 × 0.9 × 5.0 = 2.25".
- **Caption (12px `#444`, bottom right):** "learning rate 0.5, discount 0.9 — illustrative maze".

## Why a Scorecard Beats a Map

**Tags:** `where it's used` (blue), `model-free` (green), `convergence` (orange)

- **Keep repeating** — run the same trip again and again; each score climbs toward its true value
- **The true values** — Q(3, right) → 10, Q(2, right) → 0.9 × 10 = 9, Q(1, right) → 0.9 × 9 = 8.1
- **By trip 8** — the scores read 9.96, 8.68, 6.93: already ranking the rooms in the right order
- **No model needed** — the robot never learns where walls or cheese are, only which move pays
- **Same recipe elsewhere** — which ad to show, which warehouse route, which move in a board game
- **The famous heir** — swap the lookup table for a neural network and this becomes deep Q-learning

*Example (italic):* A support team routes tickets the same way — each (ticket type, queue) pair gets a running score from outcomes, and no one ever writes down a model of customers.

**Key point:** With enough visits to every (room, move) pair and a decaying learning rate, the table provably converges to the true long-run scores — trial and error alone is enough.

### Visualization (canvas `c3`, 720×300)

Line chart: the three right-move scores over trips 0 through 8, each climbing an S-ish curve toward its dashed true-value line, later rooms lagging behind earlier ones.

- **Title (bold 15px, `#1a5276`, top center):** "Every Score Climbs to Its True Value — Farther Rooms Learn Later".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; x = trip number 0–8, 12px `#444` tick labels "0"–"8"; y = score 0 to 10, light `#e5e9ef` gridlines at 2, 4, 6, 8, 10 with 12px `#444` labels.
- **Shared x grid:** trips `[0, 1, 2, 3, 4, 5, 6, 7, 8]`.
- **Q(3, right):** green `#008300` 3px line through `[0, 5, 7.5, 8.75, 9.38, 9.69, 9.84, 9.92, 9.96]`; 12px green label "room 3" near trip 2.
- **Q(2, right):** blue `#2a78d6` 3px line through `[0, 0, 2.25, 4.5, 6.19, 7.31, 8.02, 8.44, 8.68]`; 12px blue label "room 2" near trip 4.
- **Q(1, right):** orange `#d95926` 3px line through `[0, 0, 0, 1.01, 2.53, 4.05, 5.32, 6.27, 6.93]`; 12px orange label "room 1" near trip 6.
- **True-value guides:** horizontal dashed (dash 4/3) 1.5px lines at 10 (green), 9 (blue), 8.1 (orange), each with an 11px right-end label in its color: "true 10", "true 9", "true 8.1".
- **Annotation (bold 12px violet `#4a3aa7`, near trip 2.5, y=70):** two lines: "credit moves one room per trip —" / "room 1 flatlines until trip 3".
- **Caption (12px `#444`, bottom right):** "learning rate 0.5, discount 0.9 — illustrative".

## The Score Is Not the Reward

**Tags:** `common mistake` (red), `reward vs value` (orange)

- **The trap** — reading Q as "what I get paid here"; rooms 1–3 pay 0, yet their scores are far from 0
- **Reward** — the immediate payment for one step: 0, 0, 0, then +10 at the cheese door
- **Q-value** — the whole future folded in: cheese two rooms away is worth 0.9 × 0.9 × 10 = 8.1 today
- **The comparison** — room 1 pays reward 0 but carries Q ≈ 8.1; the gap IS the discounted future
- **Why it bites** — judging moves by immediate reward makes the robot wander; all early moves pay 0

*Example (italic):* A free app pays 0 on install day; its "Q-value" is the subscriptions it leads to later — pricing it by day-one revenue is the same mistake.

**Common mistake:** Treating the Q-value as the reward. Reward is one step's payment; Q is the promise of everything that follows, shrunk by 0.9 per step of distance.

### Visualization (canvas `c4`, 720×300)

Paired bar chart over the three rooms: for each room, one bar for the immediate reward of moving right and one for the learned Q-value, showing rewards near zero while Q-values stay high.

- **Title (bold 15px, `#1a5276`, top center):** "Reward Now vs Score of the Future (move right, true values)".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; y = 0 to 10, light `#e5e9ef` gridlines at 2, 4, 6, 8, 10 with 12px `#444` labels; x = three groups centered at x=190, 380, 570 labeled bold 13px `#444` "room 1", "room 2", "room 3".
- **Reward bars:** 46px wide, mute fill `rgba(107,114,128,0.35)` with 2px `#6b7280` border, values `[0, 0, 10]`; zero bars drawn as 3px stubs; bold 12px `#6b7280` labels "0", "0", "10" above.
- **Q-value bars:** 46px wide, blue fill `rgba(42,120,214,0.55)` with 2px `#2a78d6` border, values `[8.1, 9, 10]`; bold 12px blue labels "8.1", "9", "10" above.
- **Legend (top left, 12px):** mute swatch "reward for the step", blue swatch "Q-value (future folded in)".
- **Annotation (bold 13px magenta `#d55181`, over room 1 near y=85):** two lines: "pays nothing today," / "worth 8.1 = 0.9 × 0.9 × 10".
- **Caption (12px `#444`, bottom right):** "true long-run values after learning finishes — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights and line points are the hardcoded arrays above (no randomness); the trip-by-trip curves in `c3` are exact Q-learning iterates for learning rate 0.5, discount 0.9, reward +10 at the terminal room (values rounded to 2 decimals); true values 10 / 9 / 8.1 are 10 × 0.9^distance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
