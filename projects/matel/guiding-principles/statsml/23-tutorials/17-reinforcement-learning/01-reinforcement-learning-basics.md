# Reinforcement Learning Basics

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Reinforcement Learning Basics

**Subtitle:** A learner tries a move, the world hands back a score — the state is where it stands, the action is what it tries, and the reward is the points that come back

## A Robot Vacuum in a 12-Tile Apartment

**Tags:** `core idea` (blue), `state / action / reward` (green), `gridworld` (orange)

- **The vacuum** — a robot vacuum wakes up in an apartment of 12 floor tiles, 4 wide and 3 tall
- **The state** — the tile it is standing on right now; that is all the vacuum knows about the world
- **The action** — from any tile it can try one of four moves: up, down, left, or right
- **The reward** — after each move the world hands back points: −1 per move, −5 puddle, +10 dock
- **Nobody explains** — no one tells the vacuum where the dock is; it sees only its tile and points

*Example (italic):* Standing on the start tile, the vacuum tries "right", lands in the puddle, and gets −1 for the move plus −5 for the wet tile — that −6 is its only feedback.

**Key point:** Reinforcement learning has three moving parts — state (where you are), action (what you try), reward (the score that comes back) — and the learner improves using scores alone.

### Visualization (canvas `c1`, 720×300)

Gridworld map: the 4×3 tile apartment drawn as a grid with the start tile, the puddle, and the dock marked, plus the two possible first moves drawn as arrows, and a small state/action/reward legend on the left.

- **Title (bold 15px, `#1a5276`, top center):** "The Vacuum's World: 12 Tiles, One Puddle, One Dock".
- **Grid:** 4 columns × 3 rows of 80×80 px cells; grid top-left corner at x=220, y=42 (grid spans x 220–540, y 42–282); cell borders 1px `#cbd5e1`, plain-floor fill `#f8f9fa`.
- **Start tile (bottom-left cell, x 220–300, y 202–282):** fill `rgba(42,120,214,0.15)`, bold 13px `#2a78d6` centered label "START".
- **Puddle tile (bottom row, 2nd column, x 300–380, y 202–282):** fill `rgba(217,89,38,0.18)`, bold 12px `#d95926` centered two-line label "PUDDLE" / "−5".
- **Dock tile (top-right cell, x 460–540, y 42–122):** fill `rgba(0,131,0,0.15)`, bold 12px `#008300` centered two-line label "DOCK" / "+10".
- **Action arrows from the start tile:** two 3px `#2a78d6` arrows with arrowheads — one up (from the start cell's top edge to the cell above's center) labeled "up" and one right (into the puddle cell) labeled "right", labels 12px `#2a78d6`.
- **Legend (left side, x=20, 13px, one line each at y = 100 / 130 / 160):** bold `#1a5276` term then `#444` text — "state — which tile", "action — up / down / left / right", "reward — points after each move".
- **Move-cost note (12px `#444`, centered under the grid at y=296):** "every move costs −1 battery point".
- **Annotation (bold 12px violet `#4a3aa7`, x=20, y=215, two lines):** "the vacuum only sees" / "its tile and its points".
- **Caption (12px `#444`, bottom right):** "illustrative — a made-up 12-tile apartment".

## Scoring Two Routes to the Dock

**Tags:** `worked example` (blue), `adding up rewards` (green)

- **Route A** — right through the puddle, then along the wall: 5 moves, hits the puddle on move 1
- **Route A's score** — move 1 earns −1 −5 = −6, moves 2–4 earn −1 each, move 5 docks: −10 +10 = 0
- **Route B** — up first, then along the middle row: also 5 moves, but the puddle is never touched
- **Route B's score** — five moves at −1 each is −5, plus +10 at the dock: total +5
- **A long detour** — a 7-move route around everything scores −7 +10 = +3: safe, but worse than Route B
- **Same length, 5 apart** — Routes A and B take identical effort; the puddle alone splits 0 from +5

*Example (italic):* Add it yourself: Route A is −6 −1 −1 −1 +(−1+10) = 0, Route B is −1 −1 −1 −1 +(−1+10) = +5.

**Key point:** The vacuum judges a whole route by the sum of its rewards — Route B's +5 beats Route A's 0, so avoiding the puddle is worth exactly 5 points.

### Visualization (canvas `c2`, 720×300)

Single-panel line chart: the running point total after each move for both routes, showing Route A dive to −6 at the puddle and both routes jump at the dock.

- **Title (bold 15px, `#1a5276`, top center):** "Two Routes, Same Length — 5 Points Apart".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = move number 0 to 5 with 12px `#444` tick labels "0"–"5"; y = running total −12 to +6, light `#e5e9ef` gridlines at −10, −5 and +5, and a 2px `#999` zero line at total 0 with an 11px `#6b7280` "0" label.
- **Route A (through puddle):** orange `#d95926` 3px line with 5px dots through hardcoded points at moves `[0, 1, 2, 3, 4, 5]`, running totals `[0, -6, -7, -8, -9, 0]`; bold 12px orange label "Route A ends at 0" right of its last point.
- **Route B (around puddle):** green `#008300` 3px line with 5px dots at moves `[0, 1, 2, 3, 4, 5]`, running totals `[0, -1, -2, -3, -4, 5]`; bold 12px green label "Route B ends at +5" right of its last point.
- **Puddle marker:** vertical dashed `#d95926` (dash 4/3) line at move 1 from the baseline to y=70; 12px `#d95926` label "puddle: −5 extra" beside it.
- **Annotation (bold 12px `#1a5276`, near move 3.2, y=100):** "the +10 dock lifts both — the puddle decides the winner".
- **Caption (12px `#444`, bottom right):** "illustrative — totals from the worked example".

## Why the Score Beats the Rulebook

**Tags:** `where it's used` (blue), `learning loop` (green)

- **The loop** — the agent reads its state, picks an action, collects a reward, and lands in a new state, forever
- **No rulebook** — nobody can write "if tile B2 then go up" for every situation; the score teaches it instead
- **Trial and error** — repeat the loop thousands of times and moves that lead to points get picked more often
- **The same loop everywhere** — game bots, thermostats, ad choosers, and warehouse robots all live in this loop
- **What changes** — only the meaning of state, action, and reward changes; the loop itself stays identical

*Example (italic):* A thermostat's state is the room temperature, its action is heat on or off, and its reward is comfort minus the energy bill — the exact same loop as the vacuum's.

**Key point:** Reinforcement learning replaces a hand-written rulebook with one repeated loop — state, action, reward, new state — and lets the scores do the teaching.

### Visualization (canvas `c3`, 720×300)

Schematic loop diagram: an agent box and an environment box joined by two labeled arrows, showing one turn of the loop with the vacuum's actual numbers.

- **Title (bold 15px, `#1a5276`, top center):** "The Loop the Vacuum Lives In".
- **Agent box:** rounded rectangle at x=80, y=110, width 180, height 80; 3px `#2a78d6` border, fill `rgba(42,120,214,0.10)`; bold 14px `#2a78d6` centered two-line label "VACUUM" / "(agent)".
- **Environment box:** rounded rectangle at x=460, y=110, width 200, height 80; 3px `#008300` border, fill `rgba(0,131,0,0.10)`; bold 14px `#008300` centered two-line label "APARTMENT" / "(environment)".
- **Action arrow (top):** 3px `#1a5276` arrow left-to-right at y=85 from x=260 to x=460 with arrowhead; bold 12px `#1a5276` label above it: "action: move right".
- **Feedback arrow (bottom):** 3px `#1a5276` arrow right-to-left at y=225 from x=460 to x=260 with arrowhead; 12px `#444` two-line label below it: "new state: the puddle tile" / "reward: −6".
- **Repeat note (12px `#6b7280`, centered at y=60):** "one turn — then the loop runs again".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "no answer key — just points, thousands of turns in a row".
- **Caption (12px `#444`, bottom right):** "illustrative — one turn from the worked example".

## Points, Not Answers

**Tags:** `common mistake` (red), `RL vs supervised` (orange)

- **The mix-up** — newcomers assume the vacuum is told the right move after each try, like a graded quiz
- **Supervised learning** — a teacher shows the correct answer for every example: "you said dog, it was cat"
- **Reinforcement learning** — the world only hands back a score; −6 never says which move would have scored better
- **Why it is harder** — the vacuum must try the other moves itself to discover that "up" beat "right"
- **Delayed credit** — the +10 arrives at the dock, five moves after the choices that actually earned it

*Example (italic):* After the −6 puddle step, no teacher whispers "you should have gone up" — the vacuum only learns that when it later tries "up" and finishes with +5 instead of 0.

**Common mistake:** Treating the reward as the correct answer. A score tells you how the move went, never which move was right — that gap is exactly what makes reinforcement learning its own field.

### Visualization (canvas `c4`, 720×300)

Two-panel side-by-side schematic: supervised learning's teacher giving the correct label on the left, reinforcement learning's bare score on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Teacher vs Scoreboard".
- **Divider:** vertical 1px `#e5e9ef` line at x=365 from y=50 to y=270.
- **Left panel header (bold 13px `#2a78d6`, centered over x 40–350 at y=65):** "supervised learning".
- **Left panel flow (three rounded 12px-text boxes stacked at x=70, width 250, y = 90 / 150 / 210, joined by 2px `#6b7280` down-arrows):** "input: photo of a cat" (border `#cbd5e1`), "guess: 'dog'" (border `#cbd5e1`), "teacher: 'wrong — it was cat'" (border 2px `#008300`, bold 12px `#008300` text).
- **Right panel header (bold 13px `#008300`, centered over x 380–690 at y=65):** "reinforcement learning".
- **Right panel flow (three rounded 12px-text boxes stacked at x=410, width 250, y = 90 / 150 / 210, joined by 2px `#6b7280` down-arrows):** "state: the start tile" (border `#cbd5e1`), "action: move right" (border `#cbd5e1`), "score: −6 ... and nothing else" (border 2px `#d95926`, bold 12px `#d95926` text).
- **Annotation (bold 13px magenta `#d55181`, centered at y=290):** "RL gets a score, never the correct answer".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all grid positions, route points, and running totals are the hardcoded arrays above (no randomness); rewards are −1 per move, −5 puddle, +10 dock throughout, and every number in a chart matches the worked example's text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
