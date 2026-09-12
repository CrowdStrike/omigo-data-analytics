# Games & Self-Play

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Games & Self-Play

**Subtitle:** A game player can be its own teacher: play both sides, see who wins, nudge the moves that led to the win — that simple loop, run millions of times, is how AlphaGo Zero taught itself Go

## Learning Tic-Tac-Toe With No Teacher

**Tags:** `core idea` (blue), `self-play` (green), `learning from wins` (orange)

- **The kid** — a kid with no opponent plays tic-tac-toe alone, taking the X turn and then the O turn
- **The rule** — after each game she writes +1 next to every winner's move and −1 next to every loser's
- **Self-play** — both sides read the same scribbled scores, so every game trains both players at once
- **Fair sparring** — her opponent is never too strong or too weak, because her opponent is literally her
- **RL in one line** — no answer key, only a win or loss at the end, and moves get nudged toward wins

*Example (italic):* After game one (X wins), every X move on the page gains +1 and every O move loses 1 — that single bookkeeping step is the entire lesson.

**Key point:** Self-play is reinforcement learning where you are your own opponent: play a game, score the outcome, nudge the moves that were used, repeat.

### Visualization (canvas `c1`, 720×300)

Four-box cycle diagram of the self-play loop, arrows running clockwise, showing that the output of each game is nothing but a small score update that both sides share.

- **Title (bold 15px, `#1a5276`, top center):** "The Self-Play Loop: One Player, Both Sides".
- **Boxes:** four rounded rects (160×52, 8px radius, 2px border, 12px `#2c3e50` text centered on two lines) at centers (170, 95), (550, 95), (550, 225), (170, 225):
  - Box A (blue `#2a78d6` border, fill `rgba(42,120,214,0.12)`): "play one game" / "against yourself"
  - Box B (violet `#4a3aa7` border, fill `rgba(74,58,167,0.10)`): "note who won" / "(the only feedback)"
  - Box C (green `#008300` border, fill `rgba(0,131,0,0.10)`): "+1 to the winner's moves," / "−1 to the loser's"
  - Box D (aqua `#199e70` border, fill `rgba(25,158,112,0.10)`): "both sides start the" / "next game a little smarter"
- **Arrows:** 3px `#6b7280` lines with solid triangular arrowheads, clockwise A→B (along y=95), B→C (along x=550), C→D (along y=225), D→A (along x=170).
- **Annotation (bold 13px orange `#d95926`, centered at (360, 160)):** two lines: "the opponent is never too strong or too weak —" / "it is you".
- **Caption (12px `#444`, bottom right):** "illustrative — the loop itself, not measured data".

## Ten Games, Three Openings, One Scoreboard

**Tags:** `worked example` (blue), `move scores` (green)

- **The question** — which first move is best: center, corner, or edge? She lets the scoreboard decide
- **The tally** — over 10 games she opened center 6 times (5–1), corner 3 times (1–2), edge once (0–1)
- **The scores** — center: 5 − 1 = +4; corner: 1 − 2 = −1; edge: 0 − 1 = −1
- **The policy** — next round she picks the highest score more often, so center gets played ever more
- **Redo it by hand** — the whole "training run" is ten wins and losses anyone can re-add in a minute

*Example (italic):* Ten games of small additions turn "no idea where to start" into "open in the center" — with no book, no coach, and no second player.

**Key point:** Center +4, corner −1, edge −1 — the scoreboard alone rediscovers the classic center opening from just ten self-played games.

### Visualization (canvas `c2`, 720×300)

Single-panel bar chart: the three opening moves and their scores after ten self-play games, with the win–loss tally written under each bar so the reader can re-add the numbers.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Self-Play Games: the Opening Scoreboard".
- **Axes:** zero line 2px `#999` at y=180 from x=110 to x=680; y scale 26px per point, range −2 to +5; light `#e5e9ef` gridlines at +1, +2, +3, +4 (y=154, 128, 102, 76) and −1, −2 (y=206, 232); 12px `#444` y labels at x=95, right-aligned.
- **Bars (width 110, centered at x = 220, 400, 580):**
  - "center": green `#008300` fill `rgba(0,131,0,0.30)` with 2px green border, from y=180 up to y=76; bold 14px green label "+4" above the top
  - "corner": orange `#d95926` fill `rgba(217,89,38,0.25)` with 2px orange border, from y=180 down to y=206; bold 14px orange label "−1" below
  - "edge": same orange style, y=180 down to y=206; bold 14px orange label "−1" below
- **Labels under the axis (12px `#444`, two lines, centered under each bar):** "center" / "5 wins, 1 loss"; "corner" / "1 win, 2 losses"; "edge" / "0 wins, 1 loss".
- **Annotation (bold 12px green `#008300`, near (360, 60)):** "wins pull a move up, losses pull it down — no teacher needed".
- **Caption (12px `#444`, bottom right):** "illustrative — one kid's ten-game tally".

## From a Paper Scoreboard to AlphaGo

**Tags:** `where it's used` (blue), `AlphaGo` (green), `scale` (orange)

- **Same loop** — AlphaGo added the kid's loop on top of human game data: play itself, score, nudge
- **Scale** — millions of self-played games and a neural network instead of ten games and a pencil
- **2016** — AlphaGo beat Lee Sedol, one of the world's best Go players, 4 games to 1
- **Zero human data** — AlphaGo Zero then learned from self-play alone and beat the 2016 version 100–0
- **Endless data** — self-play manufactures its own training data, so it never runs out of games
- **Rising rival** — after ten practice generations the kid beats her game-1 self 97 times out of 100

*Example (italic):* The famous "move 37" against Lee Sedol was one AlphaGo reckoned a human would play with 1-in-10,000 odds — self-play, not imitation, found it.

**Key point:** The recipe that finds "open in the center" in ten games finds superhuman Go in millions — the loop scales; the idea never changes.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: the kid's win rate against a frozen copy of her game-1 self, rising across ten self-play generations from an even 50% to 97%.

- **Title (bold 15px, `#1a5276`, top center):** "Self-Play Generations: Beating Your Old Self More Every Round".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = generation 0 to 10, 12px `#444` tick labels "0"–"10" every 1; y = win rate 40% to 100%, light `#e5e9ef` gridlines at 50, 60, 70, 80, 90, 100 with 12px `#444` labels.
- **Even-match line:** horizontal dashed `#6b7280` (dash 4/3) line at 50%; 12px `#6b7280` label at its left end: "even match with game-1 self".
- **Curve:** blue `#2a78d6` 3px line with 4px blue dots through generations `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, win rate `[50, 61, 70, 77, 83, 87, 90, 93, 95, 96, 97]`.
- **End marker:** bold 13px green `#008300` label "97%" just above the last point.
- **Annotation (bold 12px orange `#d95926`, near (300, 95)):** "each generation trains against an equal — itself".
- **Caption (12px `#444`, bottom right):** "illustrative — win rate vs her game-1 self".

## The Reward Comes Only at the End

**Tags:** `common mistake` (red), `credit assignment` (orange)

- **One signal** — nobody grades individual moves; the only feedback is who won, given at game's end
- **Blanket credit** — after a win, every move in that game gets +1, including a genuine blunder
- **The fix** — average over many games: good moves keep company with wins, blunders with losses
- **The numbers** — one win credits all five moves +1; over 100 games move 3 averages out to −0.4
- **The mistake** — judging a move by one game's result; self-play works because it plays thousands

*Example (italic):* Move 3 was a blunder, but its game was won anyway, so it earned +1 that day — 100 games later its average sits at −0.4 and the blunder is exposed.

**Common mistake:** Crediting a move from a single outcome. The end-of-game reward is noisy praise for everything on the winning side; only the average over many self-played games separates skill from luck.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: five moves from one won game, each with a pair of bars — the +1 credit from that single win versus the average credit after 100 games, where the blunder finally goes negative.

- **Title (bold 15px, `#1a5276`, top center):** "One Won Game Praises Every Move — Averages Find the Blunder".
- **Axes:** zero line 2px `#999` at y=170 from x=110 to x=690; y scale 90px per 1.0, range −0.6 to +1.2; light `#e5e9ef` gridlines at +1.0 (y=80), +0.5 (y=125), −0.5 (y=215) with 12px `#444` labels at x=95, right-aligned.
- **Groups (centers at x = 170, 290, 410, 530, 650; two bars per group, each 40px wide, 8px gap):**
  - Left bar (credit from this one win): blue `#2a78d6` fill `rgba(42,120,214,0.30)`, 2px blue border, all five at +1 (top y=80)
  - Right bar (average over 100 games): green `#008300` fill `rgba(0,131,0,0.30)`, 2px green border, values `[+0.6, +0.5, −0.4, +0.7, +0.5]` — except move 3's bar, drawn in orange `#d95926` fill `rgba(217,89,38,0.25)` extending below the zero line to y=206
- **x labels (12px `#444`, centered under each group):** "move 1" … "move 5".
- **Legend (12px `#444`, top left inside plot):** blue swatch "after this one win", green swatch "average of 100 games".
- **Blunder label (bold 13px orange `#d95926`, below move 3's orange bar):** "−0.4 — the blunder".
- **Annotation (bold 12px orange `#d95926`, near (450, 250)):** "one game can't tell a good move from a lucky one — many games can".
- **Caption (12px `#444`, bottom right):** "illustrative — per-move credit".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, curve points, and per-move credits are the hardcoded literal arrays above (no randomness); the ten-game tally, the +4/−1/−1 scores, the 97% end point, and the −0.4 blunder average in the text match the chart values exactly; invented numbers carry "illustrative" captions. The AlphaGo facts (4–1 vs Lee Sedol, AlphaGo Zero's 100–0, move 37) are documented history, not invented data.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
