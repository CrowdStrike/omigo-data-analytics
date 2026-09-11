# Rank Correlation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table — text left 50%, canvas right 50%; one section uses the 3-column variant 38/31/31 with two canvases)
**HTML title tag:** Rank Correlation

**Subtitle:** Spearman's ρ ignores the raw scores and asks one question: do the two judges put things in the same ORDER?

## Two Judges, Two Very Different Scales

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — 8 desserts scored by two judges at a baking contest
- **Judge A spreads out** — scores run from 2.1 all the way to 9.5
- **Judge B compresses** — every score sits between 7.1 and 8.9
- **Raw scores disagree** — A gives the worst cake 2.1; B gives it 7.1
- **Order agrees perfectly** — both rank the 8 desserts identically, so ρ = 1.0

*Example:* The winning tart gets 9.5 from A and 8.9 from B — different numbers, same verdict: first place.

**Core idea:** convert each judge's scores to ranks (1st, 2nd, 3rd, ...) and correlate the ranks — scale habits and generosity wash out completely.

### Visualization (canvas `c1`, 720×300)

Slope chart connecting 8 desserts across two vertical judge scales; no lines cross.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Desserts on Two Scales — No Lines Cross".
- **Data:** judge A scores = `[9.5, 8.7, 7.9, 6.5, 5.8, 4.9, 3.5, 2.1]`; judge B scores = `[8.9, 8.8, 8.5, 8.2, 8.0, 7.8, 7.4, 7.1]` (same rank order).
- **Layout:** two vertical gray `#999` axes at x=200 (A, value scale 1–10) and x=520 (B, value scale 6.8–9.2); axis captions bold 13px `#444` at bottom: "Judge A (uses 1–10)" and "Judge B (uses 7–9)".
- **Marks:** for each dessert, a translucent blue `rgba(42,120,214,0.5)` connecting line (width 2); 5px dots — blue `#2a78d6` on the A axis, aqua `#199e70` on the B axis; score labels 12px next to each dot (A values right-aligned left of the axis in blue, B values left-aligned right of the axis in aqua).
- **Rank column:** muted "dessert rank →" header and rank numbers "1."–"8." (12px, muted `#6b7280`) to the left of the A axis at each A dot's height.
- **Annotations (green `#008300`, bold 14px, centered between the axes):** "no crossings = identical order" near the top; "ρ = 1.0 (Pearson says 0.998 — perfect order, not perfectly linear)" at the bottom.

## Compute Spearman on Five Cakes

**Tags:** `worked example` (green), `by hand` (blue)

- **Rank each judge** — judge A's order: 1, 2, 3, 4, 5; judge B's: 2, 1, 3, 4, 5
- **Rank differences d** — per cake: −1, 1, 0, 0, 0 (they swap the top two)
- **Square and add** — d² = 1, 1, 0, 0, 0, so Σd² = 2
- **The formula** — ρ = 1 − 6Σd² / (n(n²−1)) = 1 − 12/120
- **The answer** — ρ = 0.90: near-perfect agreement, one small swap

*Example:* One swapped pair among five cakes costs exactly 0.10 of agreement.

**The trick:** identical orders give Σd² = 0 and ρ = +1; a fully reversed order gives ρ = −1; every disagreement in between just piles up d².

### Visualization (canvas `c2`, 720×300)

Two rank columns connected by lines, a d² column, and a formula panel.

- **Title (bold 15px, `#1a5276`):** "Five Cakes: Ranks, Differences, and the Formula".
- **Data:** cakes = carrot, lemon, mocha, plum, spice; judge A ranks = `[1, 2, 3, 4, 5]`; judge B ranks = `[2, 1, 3, 4, 5]`; d² per cake = 1, 1, 0, 0, 0.
- **Columns:** headers bold 13px `#444` "Judge A rank" (x=150), "Judge B rank" (x=330), "d²" (x=415). Five 124×28 boxes per judge column (background `#f8f9fa`, `#e5e9ef` border), rows starting y=68 with 40px spacing. A column entries blue `#2a78d6` bold: "1. carrot" … "5. spice"; B column entries aqua `#199e70`: "1. lemon", "2. carrot", "3. mocha", "4. plum", "5. spice".
- **Connecting lines:** from each A row to its B row position — the swapped pair (carrot, lemon) in orange `#d95926` width 2.5, unmoved cakes translucent blue `rgba(42,120,214,0.45)` width 1.5.
- **d² column:** values with d²>0 in bold orange 14px, zeros in muted 13px; orange bold total below: "Σd² = 2".
- **Formula panel (left-aligned at x=480):** ink bold 14px "ρ = 1 − 6Σd² / (n(n²−1))"; `#444` 14px "= 1 − (6 × 2) / (5 × 24)" and "= 1 − 12 / 120"; green `#008300` bold 16px "ρ = 0.90"; orange bold 12px "one swapped pair costs 0.10".

## One Typo, Two Very Different Answers

**Tags:** `outliers` (red), `worked example` (green)

This section uses the 3-column layout: text 38%, two canvases 31% each.

- **Ten desserts** — the judges agree on the order all the way down
- **One typo** — judge A's 8.1 gets entered as 81
- **Pearson panics** — the regular r collapses from 0.98 to 0.36
- **Spearman shrugs** — 81 is still just "A's highest": rank 1
- **Small rank shuffle** — ρ only dips from 1.00 to 0.96

*Example:* To ranks, 81 and 9.5 are the same thing: whatever is biggest gets rank 1.

**Why it's robust:** ranks cap how far any single point can move — the wildest outlier can only shift its own rank, never stretch the whole scale.

### Visualization (canvas `c3a`, 350×300)

Scatter of judge A vs judge B scores with the typo point highlighted.

- **Title (bold 14px, `#1a5276`):** "Judge A vs Judge B, with typo".
- **Data:** A = `[9.4, 8.8, 81, 7.3, 6.6, 5.8, 5.1, 4.3, 3.6, 2.8]`; B = `[8.9, 8.6, 8.3, 8.0, 7.8, 7.6, 7.4, 7.2, 7.1, 7.0]`.
- **Axes:** x 0–90 (labels 0, 30, 60, 90), y scale 6.6–9.4 (labels 7, 8, 9); L-shaped gray axes; padding top 44, bottom 46, left 46, right 16; x-axis title "judge A score".
- **Points:** blue `#2a78d6` circles radius 5; the typo point (81, 8.3) red `#e74c3c` radius 7.
- **Annotations (bold 12px):** red right-aligned "8.1 typed as 81" near the typo point; blue left-aligned "the real cluster" near the main cluster.

### Visualization (canvas `c3b`, 350×300)

Grouped bar chart: Pearson r and Spearman ρ, clean vs with the typo.

- **Title (bold 14px, `#1a5276`):** "Same data, two answers".
- **Axes:** y 0–1.00 (labels every 0.25); L-shaped gray axes; padding top 44, bottom 60, left 46, right 16.
- **Groups (bars 44px wide, 14px gap; "clean" bar at 50% alpha, "typo" bar at 85%):**
  - "Pearson r": clean 0.98 in muted gray `#6b7280`, typo 0.36 in red `#e74c3c`.
  - "Spearman ρ": clean 1.00 in muted gray, typo 0.96 in green `#008300`.
- **Labels:** values bold 12px `#222` above bars; "clean"/"typo" 11px muted below bars; group names bold 12px `#444` beneath.
- **Caption (red bold 12px, bottom center):** "one typo: r loses 0.62, ρ loses 0.04".

## Where Rank Correlation Saves You

**Tags:** `where it's used` (orange), `rule of thumb` (orange)

- **Mismatched scales** — survey answers, reviewer scores, school grades from different graders
- **Skewed data** — customer spend or city sizes, where a few huge values dominate Pearson
- **Curved growth** — on doubling data (2, 4, 8, ..., 1024), Pearson reads 0.80, Spearman 1.00
- **Same recipe** — Spearman IS Pearson, just computed on the ranks
- **Not magic** — a U-shape (down then up) still fools both; plot the data first

*Example:* "Does more X go with more Y, whatever the shape?" is a Spearman question, not a Pearson one.

**Rule of thumb:** when the relationship should be "always in the same direction" but not necessarily a straight line — or the data has outliers — reach for Spearman.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart comparing Pearson vs Spearman across three data scenarios.

- **Title (bold 15px, `#1a5276`):** "Pearson vs Spearman on Three Kinds of Data".
- **Groups (two bars each — Pearson violet `#4a3aa7`, Spearman green `#008300`; bars 64px wide, 16px gap, 80% alpha):**
  - "clean scores (10 desserts)": Pearson 0.98, Spearman 1.00.
  - "doubling growth (2, 4, ..., 1024)": Pearson 0.80, Spearman 1.00.
  - "one typo (8.1 → 81)": Pearson 0.36, Spearman 0.96.
- **Axes:** y 0–1.00 with labels every 0.25 and light `#e5e9ef` horizontal gridlines; L-shaped gray axes; padding top 54, bottom 66, left 62, right 24; value labels bold 13px `#222` above bars; group labels 12px `#444` beneath.
- **Legend (upper-left, 12px, color squares):** violet "Pearson r", green "Spearman ρ".
- **Caption (green bold 13px, bottom center):** "same order → Spearman stays near 1; Pearson needs a straight, typo-free line".

## Regeneration instructions

- **Template:** tutorials topic-page layout. Page: `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, bottom border 2px solid `#2980b9`) + `table.layout`. Sections 1, 2, 4 use one `<tr>` with `td.text-col` (50%) + `td.viz-col` (50%, one 720×300 canvas). Section 3 uses the 3-column variant: `td.text-col-3` (38%) + two `td.viz-col-3` (31% each), each holding one 350×300 canvas.
- **Text column structure per section:** `.tags` row of colored pill spans (0.72rem, 600 weight, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (`li b` in `#1a5276`); one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) starting with a `<strong>` label.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes as given (720×300 or 350×300); the shared `setup(id, width, height)` helper takes optional width/height (defaults 720×300) and scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Hardcode all data arrays (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (tutorials `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette accents: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange (red `#e74c3c` used for typo/error highlights).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
