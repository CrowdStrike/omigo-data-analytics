# Variance of a Random Outcome

**Page type:** detail page (tutorial page: 4 `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Variance of a Random Outcome

**Subtitle:** Two games can pay $10 on average while one pays $9-$11 and the other $0 or $100 — variance is the number that tells them apart

## Two Games, Same $10 Average, Nothing Alike

**Tags:** `core idea` (blue), `running example` (orange)

- **Game A** — pays $9 or $11, each half the time; average = (9 + 11) / 2 = $10
- **Game B** — pays $0 nine times in ten, $100 once in ten; average = 0.1 × 100 = $10
- **Same mean** — both games are "worth $10 a play" on average
- **Different feel** — A is a steady trickle; B is long droughts and rare jackpots
- **Definition (after the example)** — variance measures how far outcomes sit from their average

*Example:* Ten plays of A pay roughly $100 every time; ten plays of B might pay $0 — or $300.

**Key point:** The average alone cannot tell these games apart — you need a second number for the spread.

### Visualization (canvas `c1`, 720×300)

Two-panel bar chart: the payout distribution of each game with a shared $10 mean marker.

- **Title (bold 15px ink `#1a5276`, centered):** "Payout Distributions: Both Average $10".
- **Divider:** vertical dashed line `#bdc3c7` (dash 4/3) at x=360 from y=38 to y=285.
- **Panels:** each panel has an L-shaped axis in `#999` (baseline y=222, chart height 140px), a bold 13px colored panel title at y=54, 40px-wide probability bars (y scale 0–100%), bold 12px percent labels above bars, 12px `#2c3e50` payout labels below, and a dashed red mean line `#e74c3c` (dash 5/4, width 2) labeled "mean $10" in bold 12px red above it.
  - **Left panel (x=40, width 290), blue `#2a78d6`:** title "Game A: $9 or $11, 50/50"; bars at 35% and 65% of panel width labeled "$9" and "$11", both 50%; mean line at panel center (frac 0.5).
  - **Right panel (x=395, width 290), orange `#d95926`:** title "Game B: $0 (90%) or $100 (10%)"; bars at 15% and 85% of panel width labeled "$0" (90%) and "$100" (10%); mean line near the left (frac 0.22).
- **Bottom line (bold 13px violet `#4a3aa7`, centered):** "Same balance point, completely different shapes — the mean hides this".

## Computing the Spread by Hand

**Tags:** `worked example` (green), `by hand` (blue)

- **Step 1** — measure each outcome's distance from the $10 mean
- **Step 2** — square the distances so below-average and above-average both count
- **Game A** — (−1)² and (+1)², each half the time: variance = 1
- **Game B** — 0.9 × (−10)² + 0.1 × (+90)² = 90 + 810 = 900
- **Back to dollars** — take the square root: spread of $1 for A, $30 for B

*Example:* Game B's rare +$90 outlier contributes 810 of its 900 variance — squaring makes big misses dominate.

**Key point:** Variance = average squared distance from the mean; its square root (standard deviation) is the spread in ordinary units.

### Visualization (canvas `c2`, 720×300)

Four-bar chart of each outcome's probability-weighted squared-distance contribution to variance.

- **Title (bold 15px ink, centered):** "Variance = Probability × (Distance From $10)²".
- **Axes:** padding top 55, bottom 84, left 75, right 40; L-shaped axis `#999`. Y from 0 to 900 with labels every 300 (12px mute `#6b7280`), gridlines `#e5e9ef`.
- **Bars (110px wide, 4 evenly spaced; minimum drawn height 3px):**
  | Value | Label | Sub-label | Color |
  |---|---|---|---|
  | 0.5 | A: $9 | 0.5 × (−1)² | blue `#2a78d6` |
  | 0.5 | A: $11 | 0.5 × (+1)² | blue `#2a78d6` |
  | 90 | B: $0 | 0.9 × (−10)² | orange `#d95926` |
  | 810 | B: $100 | 0.1 × (+90)² | orange `#d95926` |
  Value in bold 13px above each bar; label bold 12px `#2c3e50` and sub-label 12px mute below the baseline.
- **In-plot annotations (bold 13px):** blue at ~25% width near the top: "Game A: 0.5 + 0.5 = 1 → sd $1"; orange at ~72% width: "Game B: 90 + 810 = 900 → sd $30".
- **Bottom line (bold 13px violet, centered):** "One rare jackpot term carries 810 of Game B’s 900 — squaring amplifies big misses".

## Twenty Plays of Each: Same Total, Different Ride

**Tags:** `where it's used` (blue), `risk` (orange)

- **The experiment** — play each game 20 times and track the running total
- **Game A** — alternating $11 and $9 payouts climb smoothly to $200
- **Game B** — 18 zeros plus $100 wins at plays 7 and 16: also exactly $200
- **Same destination** — both totals hit $200; the paths could not differ more
- **Where it bites** — revenue per user, A/B test metrics, and latency all hide risk in the mean

*Example:* Two ad campaigns with equal average revenue can be a steady earner and a lottery — budget them differently.

**Key point:** High-variance quantities need bigger samples and bigger safety buffers — the mean says nothing about the ride.

### Visualization (canvas `c3`, 720×300)

Two-line chart: running totals of 20 plays of each game, both ending at $200.

- **Title (bold 15px ink, centered):** "Running Total Over 20 Plays of Each Game".
- **Axes:** padding top 50, bottom 55, left 70, right 150 (legend space); axis lines `#999`. Y scale max $220 with labels every $50 from $0 to $200 (12px mute), gridlines `#e5e9ef`. X spans 20 plays.
- **Data (cumulative sums plotted from $0 at play 0):**
  - Game A payouts: `[11, 9, 11, 9, 11, 9, 11, 9, 11, 9, 11, 9, 11, 9, 11, 9, 11, 9, 11, 9]` — blue `#2a78d6`, width 2.5.
  - Game B payouts: `[0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0]` — orange `#d95926`, width 2.5.
- **Annotations:** bold 12px orange "$100 win" above the jumps at plays 7 and 16.
- **Legend (right side, 12px, color swatch squares):** blue "Game A ($9/$11)"; orange "Game B ($0/$100)". Below it, bold 13px violet, two lines: "both end at $200" / "after 20 plays".
- **X-axis caption (12px mute, centered):** "plays (one illustrative 20-play run of each game)".

## The Common Confusion: Variance vs Standard Deviation

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Odd units** — Game B's variance is 900 "squared dollars", a unit nobody can picture
- **The fix** — the square root, $30, is the standard deviation: back in real dollars
- **Read it as** — "a typical play lands about $30 away from the $10 average"
- **The mix-up** — quoting variance where sd is meant overstates spread wildly (900 vs 30)
- **Rule of thumb** — compute with variance, report and reason with standard deviation

*Example:* "Spread of 900" and "spread of $30" describe the same Game B — only the second is readable.

**Key point:** Variance and standard deviation carry the same information — sd is the one in units your audience understands.

### Visualization (canvas `c4`, 720×300)

Two-panel bar chart: variance vs standard deviation for both games, same information in two units.

- **Title (bold 15px ink, centered):** "Same Information, Two Units".
- **Divider:** vertical dashed line `#bdc3c7` (dash 4/3) at x=360 from y=38 to y=285.
- **Panels:** each has an L-shaped `#999` axis (baseline y=216, chart height 132px), a bold 13px ink title at y=56, a 12px mute unit line at y=74, and two bars (90px wide, minimum height 3px) — Game A in blue `#2a78d6`, Game B in orange `#d95926` — with bold 13px value labels above and 12px `#2c3e50` game labels below.
  - **Left panel (x=40, width 290):** title "Variance", unit line: in "squared dollars" — unreadable; values 1 and 900 (scale max 900), labels "1" and "900".
  - **Right panel (x=395, width 290):** title "Standard deviation = √variance", unit line: back in plain dollars; values $1 and $30 (scale max 35), labels "$1" and "$30".
- **Bottom line (bold 13px violet, centered):** "Report the right-hand panel: \"a typical Game B play lands ~$30 from its $10 mean\"".

## Regeneration instructions

- **Template:** tutorials topic-page layout. `<h1>` concept name (no index number), `.subtitle` line, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pill row, a 5-bullet `<ul>` (each `<li>` opens with a `<b>` term in `#1a5276`), one italic `.example` paragraph, one `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; table cells padded 12px, no borders; canvas `width:100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px solid `#e74c3c` left border, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px. Colors — blue: bg `rgba(26,82,118,0.12)` / `#1a5276`; green: bg `rgba(39,174,96,0.15)` / `#27ae60`; red: bg `rgba(231,76,60,0.12)` / `#e74c3c`; orange: bg `rgba(230,126,34,0.15)` / `#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** all canvases 720×300 logical; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles bold 15px, labels 12–13px. Hardcoded literal data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
