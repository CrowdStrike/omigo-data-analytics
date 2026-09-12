# Elo Ratings

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Elo Ratings

**Subtitle:** An Elo rating is a running guess of skill — each match compares the result to what was expected, and the size of the surprise decides how far the number moves

## The Office Ping-Pong Ladder

**Tags:** `core idea` (blue), `update by surprise` (green), `zero-sum` (orange)

- **The ladder** — an office ping-pong club gives every new player 1,200 points to start
- **Win = take points** — the winner takes rating points from the loser, so the total never changes
- **Surprise sets the size** — beating a stronger player moves ratings a lot; an easy win barely does
- **Maya vs Sam** — Maya (1,400) beating Sam (1,200) is worth ±8; Sam beating Maya is worth ±24
- **Moving estimate** — a rating is not a grade; it is a running guess that every match nudges

*Example (italic):* After the upset, Sam jumps 1,200 → 1,224 and Maya drops 1,400 → 1,376 — one game, 24 points each way.

**Key point:** Elo updates skill by surprise: the more unexpected the result, the bigger the rating move. Expected results barely change anything.

### Visualization (canvas `c1`, 720×300)

Dual-panel signed bar chart around a shared zero baseline: rating changes for the two possible outcomes of the Maya (1,400) vs Sam (1,200) match.

- **Title (bold 15px, `#1a5276`, top center):** "Maya (1,400) vs Sam (1,200): Rating Moves for Each Outcome".
- **Data:** left panel (Maya wins): Maya `+8`, Sam `−8`; right panel (Sam wins, the upset): Sam `+24`, Maya `−24`.
- **Zero baseline:** 2px `#999` horizontal line at y=160, from x=55 to x=345 (left panel) and x=395 to x=685 (right panel); scale 4px per rating point (so +24 draws 96px tall, +8 draws 32px).
- **Left panel:** heading bold 12px `#444` "expected result: Maya wins" at y=55; bar for Maya up from baseline, fill `rgba(42,120,214,0.5)`, 60px wide, centered x=140, bold 13px blue `#2a78d6` label "+8" above; bar for Sam down from baseline, fill `rgba(213,81,129,0.5)`, centered x=250, bold 13px magenta `#d55181` label "−8" below; player names 12px `#444` under each bar's label.
- **Right panel:** heading "the upset: Sam wins"; bar for Sam up, fill `rgba(0,131,0,0.45)`, centered x=480, bold 13px green `#008300` label "+24" above; bar for Maya down, fill `rgba(217,89,38,0.5)`, centered x=590, bold 13px orange `#d95926` label "−24" below.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=370 from y=40 to h-12.
- **Annotation (bold 13px green `#008300`, right panel, two lines centered at x=597, y=95/112, in the clear space right of the +24 bar):** "the upset moves" / "3× more points".
- **Caption (12px `#444`, bottom center):** "K = 32; points won always equal points lost (illustrative match)".

## Doing One Update by Hand

**Tags:** `worked example` (blue), `expected score` (green), `K-factor` (orange)

- **Expected score** — before the match, compute E = 1 / (1 + 10^((opponent − you) / 400))
- **Sam's number** — Sam is 200 below Maya, so E = 1 / (1 + 10^0.5) = 0.24; Maya's is 0.76
- **The update** — new rating = old + K × (actual − expected); actual is 1 for a win, 0 for a loss
- **K = 32** — the step size; the upset win pays Sam 32 × (1 − 0.24) = 24.3 ≈ +24 points
- **The favorite** — if Maya wins instead she gets 32 × (1 − 0.76) = 7.7 ≈ +8, and Sam drops 8
- **Odds ×10** — every 400 points of gap multiplies the win odds by 10, by construction

*Example (italic):* Every rating on the club wall is two lines of arithmetic: E from the rating gap, then K × (result − E).

**Key point:** The whole algorithm is one curve and one correction: an expected score from the rating gap, then a move proportional to the surprise (result − expected).

### Visualization (canvas `c2`, 720×300)

Single S-curve chart: expected score E as a function of rating difference (you − opponent), with Sam's and Maya's positions marked for the 200-point gap.

- **Title (bold 15px, `#1a5276`, top center):** "Expected Score vs Rating Difference: E = 1 / (1 + 10^(−diff/400))".
- **Data (hardcoded points):** diffs `[-400, -300, -200, -100, -50, 0, 50, 100, 200, 300, 400]`, E `[0.09, 0.15, 0.24, 0.36, 0.43, 0.50, 0.57, 0.64, 0.76, 0.85, 0.91]`.
- **Axes:** origin x=70, baseline y=250, plot width 560 (to x=630), plot height 190 (top y=60); x maps −400 → +400 with ticks at −400, −200, 0, +200, +400 (12px `#444`); y maps 0 → 1 with ticks 0, 0.25, 0.5, 0.75, 1 (12px `#444`) and light gridlines `#e5e9ef`.
- **Curve:** blue `#2a78d6` 3px polyline through the 11 hardcoded points.
- **Sam's point:** magenta `#d55181` 6px dot at (−200, 0.24); dashed `#bdc3c7` (dash 4/3) guides down to the x-axis and left to the y-axis; bold 13px magenta label "Sam: 200 below → E = 0.24" left of the dot.
- **Maya's point:** blue `#2a78d6` 6px dot at (+200, 0.76); same dashed guides; bold 13px blue label "Maya: 200 above → E = 0.76" right of the dot.
- **Midline marker:** ink `#1a5276` 5px dot at (0, 0.50) with 12px `#444` label "even match: 0.50".
- **Caption (12px `#444`, bottom center):** "x = your rating − opponent's; the two expected scores always sum to 1".

## A Season on the Ladder

**Tags:** `where it's used` (blue), `moving estimate` (green), `online learning` (orange)

- **Sam's season** — Sam starts at 1,200 but plays like a ~1,340 player, so wins keep beating E
- **Early jumps** — the first three wins pay +16, +20, +17 because the rating is still far too low
- **Cheap losses** — losing to a 1,400 player in match 4 costs only −10; that loss was half expected
- **Late wobbles** — by match 12 Sam hovers near 1,327; wins and losses now roughly cancel out
- **Always current** — no season-end recompute; if Sam improves again, the same rule keeps chasing

*Example (italic):* Twelve matches take Sam from 1,200 to 1,327 — most of the climb happens in the first six games, while the estimate is furthest from the truth.

**Key point:** Elo is an online estimator: the rating converges toward true skill with big early corrections, then settles into small wobbles that track current form.

### Visualization (canvas `c3`, 720×300)

Line chart of Sam's rating after each of 12 matches, with win/loss dots and a dashed line at Sam's illustrative true skill.

- **Title (bold 15px, `#1a5276`, top center):** "Sam's Rating Over a 12-Match Season (illustrative)".
- **Data:** ratings (index 0 = start) `[1200, 1216, 1236, 1253, 1243, 1262, 1282, 1298, 1287, 1306, 1325, 1309, 1327]`; results `["W","W","W","L","W","W","W","L","W","W","L","W"]`; opponent ratings `[1200, 1300, 1250, 1400, 1300, 1350, 1280, 1400, 1350, 1380, 1320, 1360]`; per-match changes `[+16, +20, +17, -10, +19, +20, +16, -11, +19, +19, -16, +18]` (each = 32 × (result − E), rounded).
- **Axes:** origin x=70, baseline y=250, plot width 560, plot height 190; y maps 1,180 → 1,380 with ticks 1,200 / 1,250 / 1,300 / 1,350 (12px `#444`) and gridlines `#e5e9ef`; x ticks 0–12 (match number, 12px `#444`), x-axis label "matches played".
- **Line:** blue `#2a78d6` 3px through all 13 points; start point ink `#1a5276` 5px dot; win matches green `#008300` 5px dots; loss matches orange `#d95926` 5px dots.
- **Change labels:** bold 11px above points 1–4 only: "+16", "+20", "+17" in green and "−10" in orange (the rest stay unlabeled to avoid crowding).
- **True-skill line:** dashed aqua `#199e70` (dash 5/4) horizontal line at rating 1,340 across the plot, bold 12px aqua label "true skill ≈ 1,340 (illustrative)" above it at the right edge.
- **Annotations:** bold 12px green `#008300` near matches 1–3: "big jumps while underrated"; bold 12px orange `#d95926` near match 11: "small wobbles once caught up".

## What the Number Does — and Doesn't — Say

**Tags:** `common mistake` (red), `probability not verdict` (blue), `rule of thumb` (green)

- **Odds, not certainty** — a 200-point gap means the favorite wins about 76 matches in 100, not all
- **Upsets are priced in** — even at a 400-point gap the underdog still wins about 9 in 100
- **Relative scale** — 1,400 in the office club and 1,400 in a city league are different skills
- **Streaks mislead** — three lucky wins inflate the number; later results pull it back down
- **K trade-off** — a big K chases form fast but jitters; a small K is smooth but slow to react

*Example (italic):* A manager saw "1,400 vs 1,200" and called the match already decided — Sam still had a 24-in-100 chance.

**Common mistake:** Reading a rating gap as a verdict. Elo outputs a win probability, not a ranking of certainty — underdogs cash in that probability all the time, exactly as often as the curve says.

### Visualization (canvas `c4`, 720×300)

Bar chart of how many matches out of 100 the underdog wins, at five rating gaps, with the Maya-vs-Sam gap highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Underdog Wins per 100 Matches, by Rating Gap".
- **Data:** gaps `[0, 100, 200, 300, 400]` with underdog wins `[50, 36, 24, 15, 9]` (= 100 × (1 − E) from the section-2 curve, rounded).
- **Axes:** origin x=70, baseline y=245, plot width 560, plot height 180; y maps 0 → 60 with ticks 0, 25, 50 (12px `#444`) and gridlines `#e5e9ef`; gap labels "0", "100", "200", "300", "400" 12px `#444` below each bar, x-axis label "rating gap (favorite − underdog)".
- **Bars:** 70px wide, evenly spaced; fill `rgba(42,120,214,0.45)` except the 200-gap bar in magenta `rgba(213,81,129,0.5)`; value labels bold 12px above each bar in the bar's color (`#2a78d6` / `#d55181`).
- **Highlight label:** bold 12px magenta `#d55181` "Maya vs Sam" above the 200-gap bar's value label.
- **Annotation (bold 13px green `#008300`, upper right):** "even 400 points ahead, the favorite loses 9 in 100".
- **Caption (12px `#444`, bottom center):** "underdog win rate = 1 − E, straight from the expected-score formula".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All numbers are hardcoded literal arrays — no `Math.random()`; season trajectory, per-match changes, expected-score points, and underdog win counts are the fixed illustrative values listed above (each derived exactly from E = 1/(1+10^(−diff/400)) with K = 32 and rounded to integers).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
