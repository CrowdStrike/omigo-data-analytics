# Why Randomization Works

**Page type:** detail page (tutorial layout: h2 card-sections, two-column table 50% text / 50% viz throughout; one section holds both canvases side by side in a `.viz-pair` flex row)
**HTML title tag:** Why Randomization Works

**Subtitle:** A coin flip knows nothing about the user — so every trait, measured or not, splits evenly between the groups

## Flip a Coin 1,000 Times

Tags: `core idea` (blue), `running example` (orange)

- **The setup** — 1,000 app users, each sent to group A or group B by a fair coin
- **The result** — 503 landed in A, 497 in B: near-even is what coins do at scale
- **No favoritism** — the coin can't prefer iPhone owners, night owls, or power users
- **Independence** — the assignment has nothing to do with any property of the user
- **The mechanism** — balanced groups are a side effect of the coin's ignorance

*Example (italic):* The coin never sees a profile — it splits iPhone owners 50/50 for the same reason it splits everyone 50/50.

**Key point:** The coin's ignorance is the feature — it cannot build biased groups, on purpose or by habit.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a dot-grid crowd flows through a coin into two group boxes.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "1,000 Users, One Fair Coin".
- **Crowd:** 10×10 grid of violet (`#4a3aa7`) dots (radius 5, cell 17px) starting at (55, 60); bold violet 12px caption below: "1,000 users (1 dot = 10)".
- **Coin:** circle at (330, 145), radius 32, fill `#fdf3e0`, stroke yellow `#c98500` 2.5px; bold yellow 12px labels inside: "fair" / "coin".
- **Arrows (filled heads):** mute gray `#6b7280` from crowd to coin (238,145)→(294,145); blue `#2a78d6` from coin up-right to Group A box (360,125)→(425,90); aqua `#199e70` from coin down-right to Group B box (360,165)→(425,200).
- **Group A box:** (435, 55, 220×70), fill `#eef4fd`, stroke blue 2px; bold blue 15px "Group A: 503 users"; 12px `#2c3e50` "50.3% — not exactly half".
- **Group B box:** (435, 165, 220×70), fill `#e9f7f1`, stroke aqua 2px; bold aqua 15px "Group B: 497 users"; 12px "49.7% — close is normal".
- **Bottom annotation (bold magenta `#d55181` 13px, centered):** "the coin reads no profiles — assignment is blind to every trait".

## Check the Numbers: Every Trait Splits Evenly

Tags: `worked example` (green)

Standard two-column row; the viz cell holds both canvases side by side in a `.viz-pair` flex row.

- **Age** — under-25 share: 30.4% in A vs 29.6% in B — a 0.8-point gap
- **Age again** — 25–40: 44.9% vs 45.3%; over-40: 24.7% vs 25.1%
- **Device** — iOS: 41.2% vs 40.6%; Android: 58.8% vs 59.4%
- **Country** — top-country share: 52.1% vs 51.7%
- **Hand check** — every gap is under one point, and nobody balanced anything

*Example (italic):* No one sorted by age or device — the coin never saw either column, and both still match.

**Key point:** Balance you didn't engineer, on every trait at once — that is what randomization buys.

### Visualization (canvas `c2a`, 310×300)

Paired bar chart: age-band shares for groups A and B.

- **Title (bold 15px, `#1a5276`, centered, y=24):** "Age Mix: Group A vs B".
- **Paired bars** (baseline y=235, chart height 155, y-scale max 55, bar width 30, group span 84, first group at x=42; A bars blue `#2a78d6`, B bars aqua `#199e70`; bold 12px value labels above each bar; 12px trait labels below baseline; thin `#999` baseline):
  - "under 25": A 30.4%, B 29.6%
  - "25–40": A 44.9%, B 45.3%
  - "over 40": A 24.7%, B 25.1%
- **Legend (top left):** blue swatch "group A", aqua swatch "group B" (12px).
- **Bottom annotation (bold green `#008300` 13px, centered):** "every age band within 0.8 points".

### Visualization (canvas `c2b`, 310×300)

Paired bar chart: device and country shares for groups A and B.

- **Title (bold 15px, `#1a5276`, centered, y=24):** "Device & Country: A vs B".
- **Paired bars** (same geometry as `c2a` but y-scale max 70):
  - "iOS": A 41.2%, B 40.6%
  - "Android": A 58.8%, B 59.4%
  - "top country": A 52.1%, B 51.7%
- **Legend:** blue "group A", aqua "group B".
- **Bottom annotation (bold green 13px, centered):** "nobody sorted these — the coin did".

## It Also Balances What You Never Measured

Tags: `core idea` (blue), `where it's used` (green)

- **The magic** — motivation, patience, income, free time: balanced too, unmeasured
- **Why** — the coin is independent of everything, including traits with no column in your table
- **Contrast** — matching and adjusting can only fix the traits you thought to record
- **Hidden trait** — if 30% of users are "keen", about 30% of each group is keen
- **Consequence** — a later outcome gap can't be blamed on the users being different

*Example (italic):* You never logged "keenness", yet roughly 150 of each group's ~500 users are keen — the coin didn't know either.

**Key point:** Randomization is the only tool that balances the traits you don't know exist.

### Visualization (canvas `c3`, 720×300)

Paired bar chart: two measured traits (colored) and two unmeasured traits (gray), all balanced across A and B.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "Traits You Logged and Traits You Didn't — Both Balanced".
- **Paired bars** (baseline y=225, chart height 145, y-scale max 65, bar width 42, group span 158, first group at x=70; measured traits: A blue `#2a78d6`, B aqua `#199e70`; unmeasured traits drawn gray: A `#9aa5b1`, B `#c2cad3`; sub-labels in mute gray below trait labels):
  - "iOS" (sub "measured"): A 41.2%, B 40.6%
  - "under 25" (sub "measured"): A 30.4%, B 29.6%
  - ""keen"" (sub "never measured", gray bars): A 30.1%, B 29.8%
  - ""patient"" (sub "never measured", gray bars): A 54.6%, B 55.1%
- **Divider:** dashed `#bdc3c7` (dash 4/3, 1px) vertical line at x=378 from y=45 to y=250 separating measured from unmeasured.
- **Legend (top left):** blue "group A", aqua "group B"; right-aligned mute 12px note at (680, 52): "gray = trait with no column in your data (illustrative)".
- **Bottom annotation (bold magenta `#d55181` 13px, centered):** "balanced without ever being measured — no adjustment method can promise that".

## The Catch: Small Samples Wobble

Tags: `common mistake` (red), `rule of thumb` (green)

- **Not magic at 20** — flip 20 coins and one group can easily end up mostly keen
- **Luck of the draw** — with 20 users, a 15-point trait gap between groups is ordinary
- **Grows reliable** — the typical gap shrinks to ~7 points at 100 users, ~2 at 1,000
- **Rule of thumb** — randomization is fair on average; sample size makes it fair in your one trial
- **Sanity check** — after splitting, compare a few known traits; big gaps mean use more users

*Example (italic):* A 10-user pilot put 4 of the 5 power users in one group — the coin was fair, the sample was tiny.

**Key point:** Balance is a large-numbers effect — the coin guarantees the method, not any single small split.

### Visualization (canvas `c4`, 720×300)

Line chart: typical trait gap between groups shrinking as sample size grows.

- **Title (bold 16px, `#1a5276`, centered, y=26):** "Typical Keen-Share Gap Between Groups vs Test Size".
- **Data:** x values (users in test): `[20, 100, 200, 500, 1000]`; y values (typical gap, points): `[15, 7, 5, 3, 2]`; y-scale max 18.
- **Axes:** L-shaped `#999` axes, padding left 75 / right 40 / top 55 / bottom 60; x labels are the n values under each point; x-axis title (12px centered): "users in the test (split into two groups)"; y-axis title rotated −90° at x=24: "typical gap in "keen" share, points".
- **Series:** blue (`#2a78d6`) 3px connected line; dots radius 6 — first point orange `#d95926`, the rest blue; bold 13px value labels "15 pts", "7 pts", "5 pts", "3 pts", "2 pts" above points.
- **Annotations:** bold orange 13px left-aligned next to first point: "20 users: lopsided splits are ordinary luck"; bold green (`#008300`) 13px right-aligned near last point: "1,000 users: groups all but identical"; mute 12px right-aligned at top: "illustrative — gap shrinks roughly with √n".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md`). Page: `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle`, four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border). All four sections use `table.layout` (text-col 50% / viz-col 50%); section 2 places canvases `c2a`/`c2b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Text column structure:** `.tags` pill row (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22; 0.72rem bold pills, 10px radius), `<ul>` (0.92rem) of one-line bullets opening with `<b>` (`#1a5276`), italic `.example` (`#555`, 0.9rem), `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; canvases `width:100%`, border `1px solid #e0e0e0`, radius 4px.
- **Canvas palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic `width`/`height` attributes per chart; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; a shared `pairedBars(ctx, traits, opts)` helper draws the A/B paired bar charts (c2a, c2b, c3). All data arrays hardcoded — no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
