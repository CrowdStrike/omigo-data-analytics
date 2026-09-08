# Trust Halo / Authority Bias in Organizations

**Page type:** detail page (h2 section heading per pitfall, each followed by a one-row two-column obj-table: text left ~40%, canvas right ~60%)
**HTML title tag:** 131. Trust Halo / Authority Bias in Organizations

**Subtitle:** Trust in seniority, brand, or pedigree replaces verification — so the work of trusted sources goes unchecked exactly when it fails.

## "She's Always Right" Trap

- Stop verifying senior analyst's numbers
- Trust replaces verification processes
- The ONE time she's wrong goes uncaught

**Example:** Senior analyst's quarterly report has transposed digits. Nobody checks. $3M allocation based on bad data. Discovered 4 months later.

### Visualization (canvas `c1`, 720×200)

Bar chart: verification frequency falls with seniority.

- **Title (17px `#1a5276`, at 20,25):** "Verification Frequency by Analyst Seniority".
- **Data:** labels `["Junior", "Mid", "Senior", "Principal"]`, verification rates `[95, 70, 30, 5]` (%).
- **Bars:** 80px wide at x = 100 + i·155, height = value·1.4, baseline y=175; first two bars green `#27ae60`, last two red `#e74c3c`.
- **Labels (14px `#333`):** category names at y=192; percentage values above each bar.
- **Subtitle note (13px `#666`):** "% of outputs independently verified" at (250,25).

## Brand Halo Effect

- premium brand product assumed high-quality before anyone uses it
- Reviews: 4.5 stars before week 1
- Data shows "satisfaction" but measures BRAND not PRODUCT

**Example:** New product launch: prestigious brand gets 4.6 avg rating day 1. Unknown brand's identical product gets 3.8. Same hardware, different logo.

### Visualization (canvas `c2`, 720×200)

Two horizontal bars comparing day-1 ratings of an identical product.

- **Title (17px `#1a5276`, at 20,25):** "Day-1 Ratings: Prestigious Brand vs Unknown (Same Product)".
- **Bars:** blue `#2980b9` rect at (120,65) size 200×55 with white 16px label "Premium Brand: 4.6 stars"; orange `#e67e22` rect at (120,135) size 145×40 with `#333` label "Unknown Brand: 3.8 stars".
- **Annotations (14px `#666`):** "Identical hardware, different logo" at (400,98); "Delta = pure brand halo (+0.8)" at (400,125).

## Code Review Rubber-Stamping

- Principal engineer's PR: "LGTM" in 30 seconds
- Junior's PR: scrutinized 2 hours
- Bugs from senior reach production uncaught

**Example:** Analysis of post-mortems shows 40% of critical prod bugs came from senior PRs that received less than 2 min review time.

### Visualization (canvas `c3`, 720×200)

Grouped bars: review time vs production bugs for senior vs junior PRs.

- **Title (17px `#1a5276`, at 20,25):** "Code Review: Time Spent vs Bugs Reaching Production".
- **Review-time bars (blue `#2980b9`):** senior PR small rect (100,100) 30×60; junior PR large rect (320,50) 160×60.
- **Prod-bug bars (red `#e74c3c`):** senior PR long rect (100,165) 110×18; junior PR short rect (320,165) 20×18.
- **Labels (14px `#333`):** "Senior PR" at (85,195); "Junior PR" at (360,195). Legend: blue "Review time" at (560,65); red "Prod bugs" at (560,85). Small gray 12px annotations: "30 sec" at (100,95), "2 hours" at (370,45).

## Domain Transfer Fallacy

- Past success assumed to transfer to new domain
- VP built great reco system, now runs security team
- Everyone assumes success because of halo from different domain

**Example:** ML VP moves to security team. Team defers to "brilliant" leader. Security posture degrades for 8 months before board notices.

### Visualization (canvas `c4`, 720×200)

Two diverging lines: actual competence drops at domain switch while perceived competence stays high.

- **Title (17px `#1a5276`, at 20,25):** "Actual vs Perceived Competence After Domain Switch".
- **Actual line:** solid green `#27ae60`, width 2 — values `[150, 145, 80, 70, 65, 60, 70, 80]` at x = 80 + i·80, y = 190 − value (sharp drop at index 2 = the switch).
- **Perceived line:** dashed red `#e74c3c` (dash 5/5) — values `[150, 148, 145, 140, 135, 130, 120, 100]`, same mapping.
- **Legend (14px):** green "Actual" at (570,130); red "Perceived (halo)" at (530,55).
- **Annotation (12px `#666`):** "← Domain switch here" at (180,195).

## Hierarchy as Correctness Proxy

- Director says "churn is 5%." Analyst calculates 12%.
- Politically: director is "right." Mathematically: analyst is right.
- Analyst won't push back

**Example:** Company plans for 5% churn. Actual 12% churn devastates Q4. "Nobody could have predicted this." (Analyst did predict it.)

### Visualization (canvas `c5`, 720×200)

Two boxes contrasting the politically accepted number with the data.

- **Title (17px `#1a5276`, at 20,25):** "Churn Rate: Political Truth vs Mathematical Truth".
- **Boxes:** blue `#2980b9` rect at (120,95) size 100×70 with white 22px "5%"; red `#e74c3c` rect at (400,55) size 180×110 with white 22px "12%".
- **Labels (14px `#333`):** "Director says" at (125,185); "Data shows" at (440,185).
- **Annotation:** red `#e74c3c` "Analyst won't push back → wrong plan adopted" at (150,195).

## Pedigree Over Performance

- Prestigious university/company background
- Work assumed higher quality regardless of actual output
- The PERSON's pedigree evaluates the WORK

**Example:** search engine alum's mediocre model (AUC 0.72) deployed over unknown's excellent model (AUC 0.89). "He's from search engine, he knows what he's doing."

### Visualization (canvas `c6`, 720×200)

Two boxes comparing model AUC, with the worse (pedigree) model circled as deployed.

- **Title (17px `#1a5276`, at 20,25):** "Model AUC: Pedigree Hire vs Unknown (Deployed Choice Circled)".
- **Boxes:** orange `#e67e22` rect at (130,80) size 140×80 with 18px `#333` "AUC: 0.72"; green `#27ae60` rect at (430,50) size 180×110 with "AUC: 0.89".
- **Labels (14px):** "search engine Alum" at (155,178); "Unknown Dev" at (475,178).
- **Circle:** red `#e74c3c` stroke (width 3), radius 75 centered at (200,120) around the orange box, with red 13px "DEPLOYED" at (170,195).

## First-Mover Narrative Ownership

- First person to present analysis OWNS that narrative
- Subsequent contradicting analysis must overcome inertia
- Data quality of FIRST analysis rarely questioned

**Example:** First deck says "market is $5B." Later analysis shows $800M. Response: "but we already know it's $5B" — anchoring from first-mover.

### Visualization (canvas `c7`, 720×200)

Bar chart of belief persistence across successive corrections.

- **Title (17px `#1a5276`, at 20,25):** "Narrative Stickiness: First Analysis vs Corrections".
- **Data:** labels `["1st Pres.", "Correct #1", "Correct #2", "Correct #3"]`, belief `[95, 80, 65, 55]` (%).
- **Bars:** blue `#2980b9`, 90px wide at x = 100 + i·155, height = value·1.5, baseline y=180; 12px `#333` labels below and "95% believe" etc. above each bar.
- **Annotation (13px red `#e74c3c`):** "Original claim: \"$5B market\" — actual: $800M" at (280,50).

## Trust Decay Lag

- Person's quality declines (burnout, wrong domain, outdated skills)
- Trust persists 6-12 months after quality drops
- Org adjusts MUCH slower than reality changes

**Example:** Star engineer burned out in month 3. Code quality dropped. Team didn't notice until month 10 when accumulated bugs exploded.

### Visualization (canvas `c8`, 720×200)

Two lines with a shaded gap: trust decays much slower than actual quality.

- **Title (17px `#1a5276`, at 20,25):** "Trust Level vs Actual Output Quality (Burnout Onset Month 3)".
- **Trust line:** blue `#2980b9`, width 2 — values `[140, 140, 138, 135, 133, 130, 125, 115, 100, 80]` at x = 80 + i·64, y = 190 − value.
- **Quality line:** red `#e74c3c` — values `[140, 135, 130, 90, 70, 60, 55, 50, 48, 45]`, same mapping (sharp drop at index 3).
- **Gap shading:** translucent red `rgba(231,76,60,0.08)` rect at (240,50) size 420×95 between the lines.
- **Legend (14px):** blue "Trust" at (650,95); red "Quality" at (640,148).
- **Caption (12px `#666`):** "6-12 month gap = organizational risk zone" at (290,195).

## Regeneration instructions

- **Layout:** for each of the 8 pitfalls, an `<h2>` section heading (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table`: full-width table, left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + an `<p><strong>Example:</strong> ...</p>` paragraph, right `<td>` (60%, centered) holds the canvas. Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`. Unused `.philosophy` class: background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
- **Canvas:** HTML attributes declare `width="720" height="300"` but a shared init loop overrides every canvas to a 720×200 logical size — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), CSS size fixed at 720×200 px, `ctx.scale` back to logical coordinates. All chart coordinates above are in the 720×200 space. Chart titles 17px, labels 12–16px, `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- Card links elsewhere referencing this page use the `.html` extension in regenerated HTML.
