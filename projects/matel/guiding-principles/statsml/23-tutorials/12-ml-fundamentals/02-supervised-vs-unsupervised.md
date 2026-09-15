# Supervised vs Unsupervised

**Page type:** detail page (tutorial: 4 card-sections; each two-column table.layout 50/50 — text left, canvas right)
**HTML title tag:** Supervised vs Unsupervised

**Subtitle:** The same customer table, twice: keep the churn column and you predict it; delete the column and you look for natural groups — the label is the whole difference

## One Customer Table, Two Questions

Tags: `core idea` (blue), `running example` (green)

- **The table** — 8 customers, each with monthly visits and monthly spend
- **With the label** — a "churned?" column exists: learn to predict it for new customers
- **Without the label** — no churn column: ask "do these customers form natural groups?"
- **Supervised** — learning WITH an answer column to imitate
- **Unsupervised** — learning WITHOUT one, using only the shape of the inputs

*Example (italic):* Same spreadsheet twice: keep the churn column and you predict; delete it and you group.

**Key point:** The only difference between supervised and unsupervised learning is one column — the label.

### Visualization (canvas `c1`, 720×300)

Side-by-side rendering of the same 8-row data table, with and without the label column.

- **Title (bold 15px, `#1a5276`, top center):** "Same Table — the Only Difference Is One Column"
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 4/3) at x=360.
- **Shared data:** IDs A–H; visits `[2, 3, 4, 3, 12, 15, 11, 14]`; spend `[$10, $15, $12, $8, $60, $75, $55, $70]`; churned `[Yes, Yes, Yes, Yes, No, No, No, No]`.
- **Left table** (x0=55, with label): columns ID / visits / spend / churned? (header "churned?" in green `#008300`, others in ink `#1a5276`); zebra rows `#f4f7fa`/white, 21px row height; churned? cells tinted `rgba(217,89,38,0.15)` for Yes (text bold orange `#d95926`) and `rgba(0,131,0,0.12)` for No (text bold green `#008300`); thin border `#c9d4de`.
- **Right table** (x0=415, without label): same three data columns; the label column slot is an empty dashed gray rectangle with a large bold gray "?" centered in it.
- **Footers (bold 13px, centered):** left in green `#008300`: "SUPERVISED: predict the label" with 12px `#444` line "learn \"churned?\" for new customers"; right in violet `#4a3aa7`: "UNSUPERVISED: find the groups" with "no answer column — only structure".

## Eight Customers, Done Both Ways

Tags: `worked example` (green)

- **The data** — visits 2, 3, 4, 3 for A-D and 12, 15, 11, 14 for E-H
- **Spend follows** — $8-$15 a month for A-D, $55-$75 for E-H
- **Supervised** — A-D churned, E-H stayed; "visits under 8 → churn" gets all 8 right
- **Unsupervised** — hide the labels; the scatter still shows two clumps on its own
- **Same clumps** — here the groups match the churn split exactly: luck, not a law

*Example (italic):* Cover the churn column with your thumb — you can still see two clouds of points.

**Key point:** Supervised learning draws a boundary that matches the labels; unsupervised learning just circles the clumps it finds.

### Visualization (canvas `c2a`, 310×340)

Labeled scatter plot with a learned decision boundary.

- **Title (bold 15px, `#1a5276`, top center):** "With labels: draw the boundary"
- **Axes:** x = monthly visits (0–17), y = monthly spend (0–85); L-shaped gray axis; padding top 46 / bottom 58 / left 46 / right 12; axis caption (12px `#444`, bottom center): "monthly visits (x), monthly spend $ (y)".
- **Points:** the 8 customers at (visits, spend); 7.5px dots labeled with white bold 12px letters A–H; churned (A–D) in orange `#d95926`, stayed (E–H) in blue `#2a78d6`.
- **Boundary:** vertical dashed violet line (`#4a3aa7`, dash 6/4, width 2) at visits=8, labeled above in bold violet: "visits < 8 → churn".
- **In-plot labels (bold 12px):** orange "churned (A-D)" upper-left; blue "stayed (E-H)" centered over the right cluster.
- **Caption (bold 12px green `#008300`, centered below axis):** "the rule gets 8 of 8 right"

### Visualization (canvas `c2b`, 310×340)

Unlabeled scatter plot with two discovered cluster ellipses.

- **Title (bold 15px, `#1a5276`, top center):** "No labels: find the clumps"
- **Axes:** same scales (x 0–17, y 0–85); padding top 46 / bottom 58 / left 46 / right 12; caption (12px `#444`): "same 8 customers, churn column hidden".
- **Points:** all 8 customers in neutral gray `#8a97a5`, 7.5px dots with white bold 12px letter labels.
- **Clusters:** two dashed aqua ellipses (`#199e70`, dash 6/4, width 2.5) — one around (3, 11) radii 37×26, one around (13, 65) radii 45×45; bold aqua labels "Group 1" and "Group 2" above each.
- **Caption (bold 12px magenta `#d55181`, centered below axis):** "two clumps appear with no labels at all"

## The Label Decides Your Toolbox

Tags: `where it's used` (blue), `rule of thumb` (blue)

- **Labels cost money** — someone had to record who actually churned, often months later
- **Supervised scoring** — you can grade it: predicted vs actual churn, right or wrong
- **Unsupervised scoring** — no answer key; a human judges whether the groups mean anything
- **Typical split** — predict a known outcome: supervised; explore unknown structure: unsupervised
- **In practice** — teams often cluster first to understand customers, then label and predict

*Example (italic):* Churn, fraud, and price models are supervised; customer segments and anomaly hunts are unsupervised.

**Key point:** Ask one question — "does a truth column exist?" — and it routes you to the right family of methods.

### Visualization (canvas `c3`, 720×300)

Decision flowchart: one question box branching to two method-family boxes.

- **Title (bold 15px, `#1a5276`, top center):** "One Question Routes the Whole Project"
- **Question box:** 320×40 centered at top (y=48), violet border `#4a3aa7` with fill `rgba(74,58,167,0.10)`, bold violet text: "Does a truth column exist in the table?"
- **YES branch** (left box 280×96 at x=55, y=140, green `#008300`, arrow labeled "YES"): bold title "SUPERVISED — imitate the answers"; middle line (12px `#333`): "churn, fraud, price, diagnosis"; bold bottom line: "graded against the truth column".
- **NO branch** (right box at x=385, aqua `#199e70`, arrow labeled "NO"): "UNSUPERVISED — describe the shape"; "segments, groupings, anomalies"; "graded by human judgment".
- **Bottom caption (bold 13px orange `#d95926`, centered, y=270):** "labels are expensive: someone recorded every churn, often months after signup"

## Clusters Don't Come With Names

Tags: `common mistake` (red)

- **The output** — the algorithm returns "Group 1: A, B, C, D" and "Group 2: E, F, G, H" — nothing more
- **No meaning attached** — nothing says Group 1 is "churners"; a human must inspect and name it
- **Different features, different groups** — cluster on signup month instead and the groups reshuffle
- **Verify before acting** — check a sample from each group against reality before targeting it

*Example (italic):* Group 1 became "low-visit, low-spend customers" only after an analyst read the accounts in it.

**Common mistake (key-point callout):** Treating a cluster as an answer — unsupervised learning finds structure, not meaning; the naming is on you.

### Visualization (canvas `c4`, 720×300)

Before/after board: raw algorithm output flowing through an analyst to named segments.

- **Title (bold 15px, `#1a5276`, top center):** "What the Algorithm Returns vs What the Business Needs"
- **Left column** (two 240×74 aqua-bordered boxes `#199e70` at x=50, y=60 and y=170): "Group 1" / "A, B, C, D" / "name: ?" and "Group 2" / "E, F, G, H" / "name: ?" (names in muted gray `#6b7280`).
- **Arrow:** horizontal violet arrow (`#4a3aa7`, width 2.5) from x=305 to x=400 at y=150, labeled bold 12px "analyst inspects" (above) / "the accounts" (below).
- **Right column** (two 250×74 boxes at x=420): orange `#d95926` box "Group 1" / "visits 2-4, spend $8-$15" / "\"low-visit, low-spend — at risk\""; green `#008300` box "Group 2" / "visits 11-15, spend $55-$75" / "\"frequent, high-spend — healthy\"".
- **Bottom caption (bold 13px magenta `#d55181`, centered, y=278):** "the names come from a human, not from the algorithm"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (most-powerful-signals compact style). Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout`; every row uses `.text-col` (50%) / `.viz-col` (50%). One section places canvases `c2a`/`c2b` (310×340 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`).
- **Left column per section:** `.tags` pill row first (0.72rem bold, 10px radius pills — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), then a `<ul>` of one-line bullets each opening with `<b>` term in `#1a5276`, then an italic `.example` line (`#555`, 0.9rem), then a `.key-point` callout (background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes as given per chart (720×300, 310×340), CSS `width:100%`, 1px border `#e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper reading width/height attributes (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Data:** shared literal arrays IDS A–H, VISITS `[2,3,4,3,12,15,11,14]`, SPEND `[10,15,12,8,60,75,55,70]`, CHURN `[Yes×4, No×4]` used by all four charts; no `Math.random()`.
- In regenerated HTML, any card links use `.html` extensions.
