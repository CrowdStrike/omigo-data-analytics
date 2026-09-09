# Game Theory / Adversarial Domain Pitfalls

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one table per h2 section)
**HTML title tag:** Game Theory / Adversarial Domain Pitfalls

**Subtitle:** Common failure modes when applying game-theoretic models to real-world adversarial scenarios

## Nash Equilibrium Assumes Rationality

**Nash Equilibrium Assumes Rationality**

- **Humans aren't rational:** They are emotional, biased, and routine-driven.
- **Catastrophic failure:** Equilibrium strategies are computed against an opponent assumed to be fully rational.
- **Dominated strategies:** They fail against one who plays them out of spite, habit, or misunderstanding.
- **Harder to model:** Irrational behavior has no clean mathematical structure to exploit.
- **The catch:** Your "optimal" counter-strategy only works if the opponent is also optimizing.

### Visualization (canvas `canvas1`, 720×200 drawn; HTML attribute 720×300)

Grouped bar chart: expected (Nash) vs actual payoff against four opponent types.

- **Title (bold 17px `#1a5276`, centered):** "Expected vs Actual Payoff Against Opponents".
- **Bars:** 4 groups at x = 100, 255, 410, 565 (155px spacing), bar width 80, baseline y=175, height = value × 1.2px.
  - Expected: dashed (5/3) `#bbb` outline rectangles, all at value 90.
  - Actual: filled bars at 75% opacity — Rational Opponent 88% `#27ae60`; Emotional 42% `#e74c3c`; Biased 55% `#e67e22`; Routine-Driven 35% `#8e44ad`.
- **Labels:** category names (13px `#333`) under each group ("Rational" / "Opponent" on two lines, "Emotional", "Biased", "Routine-Driven"); bold 14px value labels ("88%", "42%", "55%", "35%") in the bar's color above each filled bar.
- **Legend (upper right, 12px `#666`):** dashed gray line sample + "Expected (Nash)"; green filled swatch + "Actual payoff".

## Opponent Modeling from Sparse Data

**Opponent Modeling from Sparse Data**

- **Sample sizes:** Poker needs 200+ hands of play to reliably infer an opponent's strategy profile.
- **Geopolitics is worse:** A high-stakes standoff offers perhaps 5 comparable historical decisions.
- **Always insufficient:** You are fitting a complex behavioral model to a handful of noisy observations.
- **False certainty:** Confidence intervals on opponent strategy estimates are enormous, never narrow.
- **Forced to act:** Decisions must still be made as if you know exactly what the opponent will do.

### Visualization (canvas `canvas2`, 720×200 drawn; HTML attribute 720×300)

Curve with confidence band: model confidence vs sample size, with domain markers.

- **Title (bold 17px `#1a5276`, centered):** "Confidence in Opponent Model vs Sample Size".
- **Axes:** origin (80, 170), width 580, height 130, blue `#2980b9` 1.5px L-axes; x-axis label "Sample Size (observations)" centered below; rotated y-axis label "Model Confidence" (13px `#555`).
- **Curve:** red `#e74c3c` 2px line, confidence = 1 − 1/√n for n = 1 to 501 (n = 1 + i×5, i = 0…100), mapped over the axis width/height.
- **Band:** shaded region `rgba(231,76,60,0.12)` around the curve with half-width 0.8/√n.
- **Domain markers (5px dots with 13px `#333` labels to the right):** Geopolitics (5) `#8e44ad`; Cybersecurity (50) `#e67e22`; Poker (200) `#27ae60` — each placed at its n on the confidence curve.

## Strategy Space Explosion

**Strategy Space Explosion**

- **The scale:** Chess has approximately 10^120 possible games, far beyond what any search enumerates.
- **Worse in practice:** Real-world adversarial scenarios have continuous action spaces with infinite possibilities.
- **Forced approximation:** You cannot enumerate all strategies, so every model leans on heuristics.
- **Blind spots:** Those heuristics inevitably miss the creative, unconventional moves an opponent finds.
- **The winner:** The opponent who finds the move outside your search space wins.
- **Pruning cost:** Cutting the strategy tree means accepting blindness to entire categories of plays.

### Visualization (canvas `canvas3`, 720×200 drawn; HTML attribute 720×300)

Branching tree diagram plus a strategy-space size table and a coverage bar.

- **Title (bold 17px `#1a5276`, centered):** "Strategy Space Growth (Log Scale)".
- **Tree (left half):** recursive branching tree in `#2980b9` 1px strokes starting at (60, 100), angle 0, depth 5, initial branch length 55, 3 branches per node with ±0.5 rad spread, branch length ×0.6 per level, opacity fading with depth (alpha = 1 − depth×0.15).
- **Table (right side at x=460, y=45):** bold 14px `#1a5276` headers "Game" and "Strategy Space" (second column offset +150px); rows in 14px, game names `#333`, values `#2980b9` — Tic-Tac-Toe 10^3; Checkers 10^31; Chess 10^120; Go 10^360; Real World ∞ (final value bold 16px `#e74c3c`).
- **Coverage bar (below table):** light-red (`#e74c3c` at 20% alpha) track 240×16 with a small green `#27ae60` segment 24px wide at the left; caption 11px `#555`: "Heuristic coverage of real strategy space".

## Mixed Strategy Unobservability

**Mixed Strategy Unobservability**

- **Optimal play randomizes:** Mixed strategies play each action with a specific probability.
- **Can't verify:** You cannot observe whether an opponent is genuinely playing "random" move to move.
- **Or a hidden plan:** They may be executing a deterministic plan you simply haven't detected yet.
- **Hidden timescales:** A sequence that looks random might be a pattern at a timescale you haven't considered.
- **Fundamental:** The ambiguity is unresolvable from observation alone.

### Visualization (canvas `canvas4`, 720×200 drawn; HTML attribute 720×300)

Diagram: one observed L/R sequence with two competing interpretations.

- **Title (bold 17px `#1a5276`, centered):** "Same Observations, Two Interpretations".
- **Sequence row (y=55):** label "Observed sequence:" (15px monospace `#333`) followed by 20 letters from the binary sequence `[1,0,1,1,0,0,1,0,1,1,0,1,0,0,1,1,0,1,0,1]` rendered as "L" (`#2980b9`) for 1 and "R" (`#e74c3c`) for 0, 24px apart.
- **Interpretation A (left, y=100):** bold 14px `#27ae60` heading "Interpretation A: Mixed Strategy (p=0.55)"; below it a green `#27ae60` bell-shaped density curve (Gaussian centered at 0.55, drawn over a 250×40 region at x=60) with `rgba(39,174,96,0.15)` fill.
- **Interpretation B (right, x=400, y=100):** bold 14px `#e74c3c` heading "Interpretation B: Deterministic Pattern"; a red `#e74c3c` cycle diagram — circle radius 30 with four 8px state nodes labeled "S1"–"S4" (white 10px text) around it, plus a red arc arrow with arrowhead indicating rotation.
- **Center bottom:** large bold 40px "?" in `#8e44ad` at 50% alpha (centered, y=175); caption 13px `#666`: "Indistinguishable from finite observations" (centered, y=195).

## Deploying Your Model Changes the Game

**Deploying Your Model Changes the Game**

- **Adaptation:** Competitors observe your deployed bidding strategy and adapt, shifting the equilibrium.
- **Optimality expires:** Your "optimal" strategy is no longer optimal because the game changed when you entered it.
- **Observer effect:** The act of using intelligence changes the strategic landscape.
- **Signal to adversaries:** Every deployed model is a signal, and they will exploit it.

### Visualization (canvas `canvas5`, 720×200 drawn; HTML attribute 720×300)

Dual line chart: strategy effectiveness decays after deployment while competitor adaptation rises.

- **Title (bold 17px `#1a5276`, centered):** "Strategy Effectiveness Over Time After Deployment".
- **Axes:** origin (80, 170), width 570, height 120, blue `#2980b9` L-axes; x-axis label "Time" (12px `#555`).
- **Deploy marker:** vertical dashed (5/4) red `#e74c3c` 2px line at 25% of the x-range, labeled "Deploy" (12px red) above it.
- **Effectiveness curve (solid `#2980b9`, 2.5px):** flat at 0.9 before deploy, then exponential decay 0.9·e^(−3·progress) + 0.15 after.
- **Adaptation curve (dashed 4/3, `#e67e22`, 2px):** flat at 0.1 before deploy, then growth 0.1 + 0.75·(1 − e^(−3·progress)) after.
- **Legend (upper right, 12px `#333`):** solid blue sample + "Your strategy effectiveness"; dashed orange sample + "Competitor adaptation".

## Multi-Agent Emergent Behavior

**Multi-Agent Emergent Behavior**

- **Simple rules, chaos:** Each agent follows simple rules, yet collective behavior is unpredictable.
- **Examples:** Flash crashes, bank runs, and traffic jams all emerge from individual optimization.
- **Nobody's plan:** No single agent intends the outcome, yet the collective reliably produces it.
- **Irreducible:** You cannot model the system merely by modeling each of its individual agents.
- **New dynamics:** Interactions create dynamics that do not exist at the individual level at all.
- **Qualitatively different:** The whole is not merely more than the sum of parts.

### Visualization (canvas `canvas6`, 720×200 drawn; HTML attribute 720×300)

Two-panel diagram: individual agents (left) → emergent chaotic system behavior (right).

- **Title (bold 17px `#1a5276`, centered):** "Individual Rules → Emergent Collective Chaos".
- **Left panel:** light gray `#ccc` box (50, 60, 200×120) labeled above "Individual Agents" and below "(simple rules)" (12px `#555`); six 6px agent dots with short direction arrows at (90,90), (140,80), (110,130), (170,110), (80,150), (200,140), colored `#2980b9`, `#27ae60`, `#e67e22`, `#8e44ad`, `#e74c3c`, `#16a085`.
- **Connector:** dark blue `#1a5276` arrow from (270, 120) to (320, 120) with the word "Interaction" (11px `#555`) above.
- **Right panel:** gray box (340, 60, 340×120) labeled "Emergent System Behavior"; inside, a red `#e74c3c` 2px "flash crash" price line generated from a seeded LCG random walk (seed 42): drift ±1 for steps 0–29, sharp drop (−up to 8/step) for steps 30–39, partial recovery (+up to 6/step) for steps 40–49, then small noise; normalized to fit the panel.
- **Caption (italic 11px `#8e44ad`, centered under right panel):** "Unpredictable from individual models".

## Regeneration instructions

- **Layout:** standard detail-page structure — h1, `.subtitle` paragraph, then per pitfall an `<h2>` section heading followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` div (same text as the h2) + `<ul>` of labeled bullets, right `<td>` (60%, centered) with a `<canvas>` (HTML attributes `width="720" height="300"`).
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, even rows background `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` callout class defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** shared `setupCanvas(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), default font `17px -apple-system, BlinkMacSystemFont, sans-serif`; returns `{ctx, w, h}`. Note the drawn size (720×200) overrides the 720×300 HTML attribute.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#555`/`#333`/`#666`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
