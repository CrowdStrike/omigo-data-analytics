# Choosing the Right Test

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Choosing the Right Test

**Subtitle:** Three questions about your data — what type is the outcome, how many groups, are they paired — pick the test for you; every classic test owns exactly one cell of the same grid

## One Coffee Shop, Four Questions

**Tags:** `core idea` (blue), `where it's used` (orange)

- **The shop** — Beanline Coffee's analyst faces four different questions in a single month
- **Q1 layouts** — average sale under layout A vs layout B: numeric outcome, two separate crowds
- **Q2 loyalty** — each regular's spend before vs after the card: the same people, measured twice
- **Q3 blends** — 0–100 tasting scores for three roast blends: numeric, but three groups at once
- **Q4 iced** — hot vs iced choice across three locations: no numbers at all, just categories
- **The point** — the question's shape (outcome type, groups, pairing) picks the test, not habit

*Example (italic):* Q1 and Q2 both compare two averages, yet they need different tests — Q2's before/after values share a customer.

**Key point:** A test is chosen by three facts about the data — outcome type, number of groups, paired or not — never by which test you happen to know best.

### Visualization (canvas `c1`, 720×300)

Canvas-drawn table: the four Beanline questions in rows, with columns for outcome type, group setup, and the matching test shown as a colored pill.

- **Title (bold 15px, `#1a5276`, top center):** "Four Questions, Four Tests: One Coffee Shop".
- **Columns:** header row bold 12px `#1a5276` at y=52: "Question" (x=15, width 335), "Outcome" (x=360, width 105), "Groups" (x=470, width 110), "Test" (x=585, width 120); 1px `#e5e9ef` horizontal rule under the header at y=60.
- **Rows:** four rows, top edges y=70, 125, 180, 235, each 50px tall, separated by 1px `#e5e9ef` rules; question text 12px `#2c3e50`, outcome/groups 12px `#6b7280`.
- **Row 1:** "Q1 — Do layouts A and B differ in average sale?" | "numeric" | "2, independent" | pill "Two-sample t-test".
- **Row 2:** "Q2 — Did loyalty cards lift each regular's spend?" | "numeric" | "2, paired" | pill "Paired t-test".
- **Row 3:** "Q3 — Do three roast blends score differently?" | "numeric" | "3, independent" | pill "ANOVA".
- **Row 4:** "Q4 — Is iced-vs-hot choice linked to location?" | "categorical" | "2 × 3 table" | pill "Chi-squared".
- **Test pills:** rounded rects (radius 8, padding 4px 8px), bold 12px text; row 1 blue `#2a78d6` on `rgba(42,120,214,0.12)`; row 2 green `#008300` on `rgba(0,131,0,0.10)`; row 3 violet `#4a3aa7` on `rgba(74,58,167,0.10)`; row 4 orange `#d95926` on `rgba(217,89,38,0.10)`.
- **Caption (12px `#444`, bottom center, y=292):** "same shop, same month — four question shapes, four different tests".

## Walking the Decision Tree

**Tags:** `worked example` (blue), `decision tree` (green)

- **Start** — ask "what is the outcome?": a number (dollars, scores) or a category (hot vs iced)
- **Numeric, 2 crowds** — different people in each group → two-sample t-test (layouts, Q1)
- **Numeric, paired** — the same people measured twice → paired t-test (loyalty card, Q2)
- **Numeric, 3+ groups** — compare all the means at once with ANOVA (roast blends, Q3)
- **Skewed or ranks** — heavy outliers or 1–5 stars → swap to a rank test like Mann-Whitney
- **Categorical** — counts in a table → chi-squared (Q4); paired yes/no answers → McNemar

*Example (italic):* Q2 walks the tree in three hops: numeric outcome → two measures → same people → paired t-test.

**Key point:** Every hop in the tree is a fact about your data, not a preference — two analysts holding the same data should land on the same leaf.

### Visualization (canvas `c2`, 720×300)

Left-to-right decision tree: one root question, two outcome branches, six condition boxes, six test leaves, connected by elbow lines.

- **Title (bold 15px, `#1a5276`, top center, y=22):** "The Test-Picker Decision Tree".
- **Boxes:** all rounded rects (radius 6), white fill, 2px colored border, bold 12px centered text in the border color; condition boxes use 1.5px `#6b7280` border with 12px `#2c3e50` text.
- **Root (ink `#1a5276` border):** "What is the / outcome?" (two lines) at x=10, y=169, width 118, height 32.
- **Level 2:** "Numeric" (blue `#2a78d6` border) at x=155, y=105, width 115, height 32; "Categorical" (orange `#d95926` border) at x=155, y=233, width 115, height 32.
- **Level 3 condition boxes:** x=300, width 185, height 32, at y=45 "2 groups, different people"; y=85 "2 measures, same people"; y=125 "3+ groups"; y=165 "skewed data or ranks"; y=213 "counts in a table"; y=253 "paired yes/no".
- **Leaves:** x=525, width 185, height 32, same y as their condition box: "Two-sample t-test" (blue `#2a78d6`); "Paired t-test" (green `#008300`); "ANOVA" (violet `#4a3aa7`); "Mann-Whitney / Kruskal-Wallis" (aqua `#199e70`, 11px text); "Chi-squared test" (orange `#d95926`); "McNemar's test" (magenta `#d55181`).
- **Connectors:** 1.5px `#6b7280` elbow lines (horizontal-vertical-horizontal): root right edge to each level-2 box; "Numeric" right edge fans to the top four condition boxes; "Categorical" fans to the bottom two; each condition box connects straight across to its leaf.
- **Annotation:** green bold 12px "Q2's path" beside the Numeric → "2 measures, same people" → "Paired t-test" route; redraw that route's connectors in 3px green `#008300`.

## Every Test Owns One Cell

**Tags:** `rule of thumb` (green), `where it's used` (orange)

- **Rows** — outcome type: numeric and roughly normal, numeric but skewed/ranked, or categories
- **Columns** — group setup: 2 independent groups, 2 paired measures, or 3+ groups
- **One cell each** — every classic test owns exactly one cell; none of them ever compete
- **Drop a row** — skewed dollars or star ratings? step down one row to the rank-test twin
- **Q2's cell** — loyalty spend is numeric and paired: row 1, column 2 — the paired t-test

*Example (italic):* The blends question (Q3) lands in row 1, column 3: ANOVA — its skewed twin one row down is Kruskal-Wallis.

**Key point:** Find your row and your column first and the test name falls out — memorize one 3×3 grid instead of nine separate recipes.

### Visualization (canvas `c3`, 720×300)

Canvas-drawn 3×3 grid of tests: outcome type as rows, group setup as columns, one test name per cell, with Q2's cell highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "The 3×3 Test Grid: Outcome Type × Group Setup".
- **Column headers (bold 12px `#1a5276`, centered, y=58):** "2 independent groups" over x=175–345, "2 paired measures" over x=355–525, "3+ groups" over x=535–705.
- **Row labels (bold 12px `#1a5276`, x=15, width 150, vertically centered per row, two lines each):** "Numeric, / roughly normal"; "Numeric, / skewed or ranks"; "Yes-no / / categories".
- **Cells:** three columns at x=175, 355, 535 (width 170 each); three rows at y=70, 145, 220 (height 65 each, 10px gaps); rounded rects (radius 6) with tinted fills and bold 13px centered test names in the row color.
- **Row 1 (blue `#2a78d6` text, fill `rgba(42,120,214,0.10)`):** "Two-sample t-test" | "Paired t-test" | "ANOVA".
- **Row 2 (aqua `#199e70` text, fill `rgba(25,158,112,0.10)`):** "Mann-Whitney U" | "Wilcoxon signed-rank" | "Kruskal-Wallis".
- **Row 3 (orange `#d95926` text, fill `rgba(217,89,38,0.10)`):** "Chi-squared (2×2)" | "McNemar" | "Chi-squared".
- **Highlight:** the "Paired t-test" cell gets a 3px green `#008300` border and a green bold 12px tag "Q2 lives here" just above its top-right corner.
- **Caption (12px `#444`, bottom center, y=295):** "step down a row when the numbers are skewed — each test has a rank-based twin".

## The Pairing Trap

**Tags:** `common mistake` (red), `worked example` (blue)

- **The trap** — Q2's data fed to a two-sample t-test: 12 before spends against 12 after spends
- **The numbers** — before averages $50.5 (range $38–$63); after averages $55.2 (range $44–$68)
- **Unpaired view** — the two clouds overlap heavily, so the test shrugs: p ≈ 0.17 (illustrative)
- **Paired view** — all 12 regulars rose, lifts of $3 to $6, average +$4.7: p < 0.001
- **Why** — pairing subtracts each person's own baseline, wiping out spender-to-spender noise

*Example (italic):* The same 24 numbers say "no effect" unpaired and "clear effect" paired — the test choice was the finding.

**Common mistake:** Matching a test to the data type alone. Pairing is part of the study design — ignore it and the noise between people buries the steady effect within each person.

### Visualization (canvas `c4`, 720×300)

Dual-panel view of the same loyalty data: unpaired dot clouds (left) vs paired before→after slope lines (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same 24 Numbers: Unpaired Clouds vs Paired Lines (illustrative)".
- **Data:** 12 regulars; before `[42, 55, 38, 61, 47, 52, 44, 58, 49, 63, 41, 56]`, after `[47, 59, 44, 66, 50, 57, 48, 64, 53, 68, 45, 61]` (per-person lifts `[5, 4, 6, 5, 3, 5, 4, 6, 4, 5, 4, 5]`; means 50.5 and 55.2).
- **Y scale (both panels):** $35 at y=255 up to $70 at y=60; y = 255 − (v − 35) × (195 / 35).
- **Left panel (unpaired):** column centers x=150 ("before") and x=265 ("after"), labels 12px `#444` below y=270; dots 5px, before blue `#2a78d6`, after orange `#d95926`, horizontal jitter offsets `[-14, -6, 2, 10, -10, -2, 6, 14, -12, -4, 4, 12]` px applied in order; 24px-wide dashed mean ticks at $50.5 (blue) and $55.2 (orange) with 11px labels "mean $50.5" / "mean $55.2"; magenta `#d55181` bold 12px annotation, two lines: "clouds overlap:" / "unpaired p ≈ 0.17".
- **Right panel (paired):** column centers x=440 ("before") and x=635 ("after"), same y scale and labels; for each regular a 2px green `#008300` line from (440, before) to (635, after) with 3.5px dots at both ends; green bold 13px annotation, two lines: "all 12 lines rise:" / "paired p < 0.001".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h−12.
- **Caption (12px `#444`, bottom center, y=295):** "pairing compares each regular to themselves — the between-person spread cancels out".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- Decision-tree and table canvases (`c1`, `c2`, `c3`) draw rounded rects and elbow connectors by hand; all node labels, positions, and connections are specified above — no layout library.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
