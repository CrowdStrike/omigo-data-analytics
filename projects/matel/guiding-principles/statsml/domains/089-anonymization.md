# Data Anonymization & De-identification

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: bullets left ~40%, canvas right ~60%)
**HTML title tag:** Data Anonymization & De-identification - Domain Pitfalls

**Subtitle:** Anonymization techniques that look robust in theory — k-anonymity, aggregation thresholds, name removal — crumble against adversaries armed with auxiliary datasets and behavioral fingerprints.

## k-Anonymity Fails With Auxiliary Data

- **The trap:** k-anonymity makes each record indistinguishable from k-1 others on the quasi-identifiers.
- **Hidden assumption:** It presumes the adversary holds only those attributes and no outside data.
- **Real-world failure:** A hospital released records at k=5, treating that guarantee as sufficient.
- **The linkage:** An attacker cross-referenced pharmacy loyalty card data, identifying 23% of patients.
- **Why it persists:** Audits check a fixed quasi-identifier list, not what an adversary could know.
- **Moving target:** Adversary knowledge grows as more outside datasets become linkable.
- **Scale of damage:** With 15 demographic attributes, resisting modern linkage attacks needs k>10,000.
- **Utility collapse:** At that group size the released data is too coarse to answer any question.

### Visualization (canvas `canvas1`, 720×200)

Line chart of re-identification rate rising with number of auxiliary attributes.

- **Title (bold 17px `#1a5276`):** "Re-identification Rate vs Auxiliary Attributes (k=5)".
- **Data:** x = attribute counts `[1, 3, 5, 8, 10, 15, 20, 25, 30]` (evenly spaced), y = rates `[0.02, 0.12, 0.28, 0.51, 0.64, 0.82, 0.91, 0.96, 0.99]`.
- **Axes:** origin (60, 170), plot 620 wide × 130 tall; L-shaped `#ccc` axes; y gridlines (`#eee`) and labels at 0%, 25%, 50%, 75%, 100% in `#666` 12px; x tick labels are the attribute counts in `#666` 11px below the axis.
- **Series:** red line `#e74c3c` width 3 with 4px red dots at each point.
- **Danger band:** top 30% of the plot area filled `rgba(231,76,60,0.1)`, labeled near its top-right in bold 11px red: "DANGER: >70% re-identification".

## Differential Privacy Budget Trade-offs

- **The trap:** The epsilon budget is finite, and every query against the dataset consumes part of it.
- **Hard stop:** Once exhausted, further analysis is only possible by degrading the stated guarantee.
- **Composition problem:** 100 queries at ε=0.1 each total ε=10 under basic composition.
- **Weaker than advertised:** That total is far less private than the per-query epsilon suggests.
- **Real-world failure:** US Census 2020 used ε≈19.61 for redistricting data releases.
- **The critique:** Researchers argued it gave minimal protection while distorting small-area statistics.
- **Funding stakes:** Those distorted small-area counts feed formulas that allocate federal money.
- **Utility death spiral:** ε=1 across 50 queries gives ε=0.02 each — noise swamps subgroups under ~50,000.

### Visualization (canvas `canvas2`, 720×200)

Two crossing line curves showing the privacy/utility trade-off as epsilon grows.

- **Title (bold 17px `#1a5276`):** "Privacy vs Utility Trade-off (Differential Privacy)".
- **Data:** x = epsilon values `[0.01, 0.1, 0.5, 1, 2, 5, 10, 20]` (evenly spaced, x tick labels "ε=0.01" … "ε=20" in `#666` 11px); utility = `[0.05, 0.25, 0.55, 0.72, 0.85, 0.93, 0.97, 0.99]` (green `#27ae60`, width 3); privacy protection = `[0.99, 0.95, 0.78, 0.62, 0.45, 0.22, 0.10, 0.03]` (red `#e74c3c`, width 3).
- **Axes:** origin (60, 170), plot 600 wide × 130 tall, L-shaped `#ccc` axes.
- **Legend (top-left inside plot):** green swatch + "Utility"; red swatch + "Privacy Protection" (labels `#333` 12px).
- **Annotation:** vertical dashed `#1a5276` line (dash 4/4) at the ε=1 position, labeled in bold 11px `#1a5276`: "← Sweet spot (ε≈1)".

## Quasi-Identifier Re-identification

- **The trap:** Combinations of individually innocent attributes add up to a unique fingerprint.
- **The classic result:** Latanya Sweeney showed zip+DOB+gender uniquely identifies 87% of the US.
- **Real-world failure:** Massachusetts GIC released hospital data it described as "anonymized."
- **The $20 attack:** Sweeney purchased voter rolls and re-identified Governor Weld's medical records.
- **Modern expansion:** Quasi-identifiers now include browser fingerprints and typing patterns.
- **Outside the PII list:** Device characteristics like these were never traditionally treated as PII.
- **Combinatorial explosion:** 30 binary attributes yield 2^30 (>1 billion) distinct combinations.
- **Guaranteed uniqueness:** With more cells than people, most records land in a cell of one.

### Visualization (canvas `canvas3`, 720×200)

Bar chart of population uniqueness by quasi-identifier combination.

- **Title (bold 17px `#1a5276`):** "Population Uniqueness by Quasi-Identifier Combination".
- **Data:** categories `["Zip alone", "Zip+Gender", "Zip+DOB", "Zip+DOB+Gender", "+ Ethnicity"]` with values `[1%, 4%, 69%, 87%, 97%]`.
- **Layout:** baseline y=175, plot 550 wide × 130 tall starting at x=120; bars 70px wide, evenly spaced.
- **Bar colors:** vertical gradient by value — >70%: `#e74c3c` → `#c0392b` (red); >40%: `#f39c12` → `#d68910` (orange); else `#27ae60` → `#1e8449` (green).
- **Labels:** percentage value in bold 13px `#1a5276` above each bar; category text split on "+" into stacked 10px `#666` lines below each bar.

## Synthetic Data Mode Collapse

- **The trap:** GANs and VAEs generating "privacy-safe" synthetic data collapse toward modal values.
- **What is lost:** The rare but critical subpopulations at the tails get erased in the process.
- **Real-world failure:** A health insurer's synthetic claims data collapsed rare disease codes.
- **The mechanism:** ICD-10 codes appearing <50 times were folded into common nearby codes.
- **Research impact:** Rare disease work is impossible on data where those codes no longer exist.
- **Privacy paradox:** The rare records mode collapse eliminates are exactly the most re-identifiable ones.
- **Not real protection:** "Privacy" here comes from destroying sensitive data, not from protecting it.
- **Detection difficulty:** Mean, variance, and correlation look fine; only subgroup analysis reveals it.

### Visualization (canvas `canvas4`, 720×200)

Overlaid density curves comparing real vs synthetic data distributions.

- **Title (bold 17px `#1a5276`):** "Real vs Synthetic Data Distribution (Mode Collapse)".
- **Curves:** drawn point-by-point across a 620-wide × 110-tall plot with origin (60, 160), t in [0,1]:
  - Real data (solid green `#27ae60`, width 2): `y = 0.6·exp(-((t-0.3)·8)²) + 0.4·exp(-((t-0.6)·10)²) + 0.15·exp(-((t-0.85)·12)²)` — three modes including a small right-tail mode.
  - Synthetic (dashed red `#e74c3c`, dash 6/4, width 2): `y = 0.8·exp(-((t-0.35)·6)²) + 0.5·exp(-((t-0.58)·8)²) + 0.02·exp(-((t-0.85)·12)²)` — right-tail mode nearly erased.
- **Legend (top):** green swatch + "Real data (rare diseases present)"; red swatch + "Synthetic (rare diseases erased)" (labels `#333` 12px).
- **Annotation:** bold 11px red text "← Rare conditions lost" near t=0.85, with a `rgba(231,76,60,0.15)` highlight rectangle (120px wide, full plot height) over the lost right-tail region.

## Temporal De-anonymization

- **The trap:** With identifiers stripped, event timing still forms a unique behavioral signature.
- **As good as prints:** Those timing signatures identify a person about as reliably as fingerprints.
- **Real-world failure:** Researchers matched credit card streams to social media check-ins by timing.
- **The result:** 4 spatiotemporal points identified 90% of 1.1M individuals in that dataset.
- **Compounding risk:** A Tuesday 8am login plus a Thursday 6pm purchase narrows the field fast.
- **Weeks, not years:** Add a Saturday 10am browse and the trace is near-unique within weeks.
- **Failed countermeasure:** Time jitter of ±hours still preserves the ordinal event structure.
- **Why it fails:** Sequence-matching attacks exploit that surviving order at >60% accuracy.

### Visualization (canvas `canvas5`, 720×200)

Filled area/line chart of re-identification success vs number of temporal data points.

- **Title (bold 17px `#1a5276`):** "Re-identification Success vs Temporal Data Points".
- **Data:** x = points `[1, 2, 3, 4, 5, 6, 8, 10]` (evenly spaced, tick labels "1 pts" … "10 pts" in `#666` 11px), y = rates `[0.12, 0.45, 0.72, 0.90, 0.95, 0.97, 0.99, 0.995]`.
- **Axes:** origin (60, 170), plot 620 wide × 130 tall; `#ccc` axes; y labels 0%, 25%, 50%, 75%, 100% in `#666` 11px with `#eee` gridlines.
- **Series:** red `#e74c3c` line width 3 with 5px red dots; area under the curve filled `rgba(231,76,60,0.15)`.
- **Annotation:** bold 11px `#1a5276` text near the 4-point position at the top: "4 points → 90% identified".

## Zip + DOB + Gender = 87% Unique

- **The trap:** Three fields present in nearly every demographic dataset act as a unique identifier.
- **Not a theory:** This is a demonstrated mathematical certainty, not a speculative attack scenario.
- **Real-world failure:** Health releases include age, gender, and region, believing them "too broad."
- **The measured rate:** Year-level age + 5-digit zip + gender uniquely identifies 87.1% of the US.
- **Granularity illusion:** Coarsening to a 3-digit zip prefix and 5-year age bands is not enough.
- **Still failing k=5:** Groups of ≤5 remain for 18% of records, which is millions of people.
- **International variation:** In smaller countries the same combination is even more identifying.
- **Why:** Netherlands and Singapore use finer geographic units, so each cell holds fewer people.

### Visualization (canvas `canvas6`, 720×200)

Sorted bar strip of uniqueness rates across 50 simulated US counties.

- **Title (bold 17px `#1a5276`):** "Uniqueness Rate Across US Counties (Zip+DOB+Gender)".
- **Data:** 50 bars with random values uniform in [0.65, 0.99], sorted ascending (regenerated each load).
- **Layout:** origin (50, 170), plot 640 wide × 130 tall; bar width = column width − 2px.
- **Bar colors:** value > 0.87 red `#e74c3c`; > 0.75 orange `#f39c12`; else green `#27ae60`.
- **Reference line:** horizontal dashed red (`#e74c3c`, dash 5/5, width 2) at 0.87, labeled bold 12px red at right: "87% national average".
- **X-axis label:** "US Counties (sorted)" centered below in `#666` 11px.

## anonymization prize dataset Re-identification

- **The trap:** A streaming service released 100M ratings from 480K subscribers with names removed.
- **The belief:** Organizers assumed movie ratings alone could not point back to a named person.
- **Real-world failure:** Narayanan & Shmatikov (2008) cross-referenced the set with public IMDb reviews.
- **The result:** 8 ratings sufficed to identify 99% of users, even under ±14-day and ±1 star noise.
- **Why it matters:** Movie preferences correlate with political views, sexual orientation, and health.
- **Concrete harm:** One plaintiff was a closeted mother whose viewing history would have outed her.
- **Legacy impact:** The streaming service cancelled its planned second prize competition.
- **The lesson:** This became the canonical proof that "removing names" is not anonymization.

### Visualization (canvas `canvas7`, 720×200)

Two-line chart of re-identification accuracy vs number of ratings known.

- **Title (bold 17px `#1a5276`):** "streaming service Re-identification: Ratings Needed vs Accuracy".
- **Data:** x = ratings `[2, 4, 6, 8, 10, 15, 20]` (evenly spaced, tick labels "2 ratings" … "20 ratings" in `#666` 11px); exact-dates accuracy = `[0.68, 0.84, 0.92, 0.99, 0.996, 0.999, 1]` (solid red `#e74c3c`, width 3); noisy accuracy = `[0.45, 0.65, 0.79, 0.88, 0.93, 0.96, 0.98]` (dashed orange `#f39c12`, dash 6/4, width 3).
- **Axes:** origin (60, 170), plot 600 wide × 130 tall, `#ccc` axes.
- **Legend (top-left):** red swatch + "Exact dates"; orange swatch + "±14 days + ±1 star noise" (labels `#333` 12px).
- **Annotation:** bold 11px `#1a5276` text near the 8-ratings position at the top: "8 ratings = 99% identified".

## Location Traces Re-identification Without Names

- **The trap:** "Anonymous" GPS and cell-tower data is almost never actually anonymous.
- **Two clusters suffice:** Home at night plus work by day together identify nearly everyone.
- **Real-world failure:** De Montjoye et al. (2013) studied mobility traces for 1.5M individuals.
- **The result:** 4 spatiotemporal points identify 95%, and coarse resolution keeps it above 50%.
- **Aggregation doesn't help:** Origin-destination pairs in 1-hour bins are unique commute fingerprints.
- **Your own bin:** A specific departure time on a specific route is often just one person.
- **Commercial reality:** NYT journalists traced an "anonymous" phone in a data broker dataset.
- **Who it was:** Home-to-work commute patterns pointed to a Pentagon official.

### Visualization (canvas `canvas8`, 720×200)

Heatmap of location uniqueness by spatial resolution (rows) and number of points (columns).

- **Title (bold 17px `#1a5276`):** "Location Uniqueness by Spatial Resolution & Points".
- **Rows (labeled left, `#666` 11px):** "GPS (10m)", "Cell (200m)", "Zip code", "County". **Columns (labeled below, `#666` 11px):** "2 pts", "4 pts", "6 pts", "8 pts", "10 pts".
- **Data (uniqueness):**
  - GPS (10m): 75%, 95%, 99%, 100%, 100%
  - Cell (200m): 42%, 78%, 91%, 96%, 99%
  - Zip code: 15%, 45%, 68%, 82%, 90%
  - County: 5%, 18%, 35%, 52%, 65%
- **Layout:** grid starts at x=80, baseline y=165, 580 wide × 120 tall; cells inset 2px.
- **Cell color:** linear interpolation between green `rgb(39,174,96)` (v=0) and red `rgb(231,76,60)` (v=1) at alpha 0.8; percentage printed in each cell, bold 12px, white text when v>0.6 else `#333`.
- **Annotation (top-right, bold 11px `#1a5276`):** "Darker = higher re-identification risk".

## Consent ≠ Anonymous (Behavioral Fingerprints)

- **The trap:** Users consenting to "anonymized" collection believe that label leaves them protected.
- **What survives:** Mouse movements and typing cadence form biometric signatures anonymization misses.
- **Real-world failure:** Mouse movement on a single webpage identifies return visitors at 95% accuracy.
- **Nothing needed:** That works with no cookies, no login, and no IP address; keystrokes are more unique.
- **Consent gap:** Users agree to "anonymous analytics" not knowing behavioral patterns ARE identifying.
- **What they signed:** In effect, identified tracking presented to them under an anonymous label.
- **Cross-session linking:** Behavioral biometrics re-link "anonymous" sessions across days, browsers, devices.
- **Deletion doesn't help:** Linking holds at >80% accuracy even after every identifier is deleted.

### Visualization (canvas `canvas9`, 720×200)

Bar chart of identification accuracy by behavioral biometric method.

- **Title (bold 17px `#1a5276`):** "Behavioral Biometric Identification Accuracy".
- **Data:** methods (two-line labels) `["Mouse move", "Scroll pattern", "Keystroke dynamics", "Touch gesture", "All combined"]` with accuracy `[78%, 65%, 95%, 82%, 99%]`.
- **Layout:** baseline y=170, plot 500 wide starting at x=140, height scale 130px; bars 60px wide with equal gaps.
- **Bar colors:** vertical gradient by value — >90%: `#c0392b` → `#e74c3c`; >75%: `#e74c3c` → `#f39c12`; else `#f39c12` → `#f1c40f`.
- **Labels:** percentage in bold 13px `#1a5276` above each bar; method name in stacked 10px `#666` lines below.
- **Annotation (top, bold 11px red):** "All achieved WITHOUT cookies, login, or IP address".

## Aggregation Threshold Gaming

- **The trap:** "We only report when a group has ≥5" is the standard rule in census and health data.
- **How it breaks:** Adversaries isolate individuals by subtracting overlapping group query results.
- **Real-world failure:** "Diabetics in zip 02138 aged 45-50" returns 8 and is released as safe.
- **The subtraction:** Adding "...male" returns 3 and is suppressed, so 8 − 3 reveals 5 females.
- **Differencing attacks:** Many aggregate queries let attackers solve a system of equations for cells.
- **Census exposure:** Tables published at several nested geographic levels are especially vulnerable.
- **Threshold inconsistency:** Agencies apply thresholds of 3, 5, 10, or 20 to the very same data.
- **Leakage across releases:** A cell suppressed under one threshold is visible under another release.

### Visualization (canvas `canvas10`, 720×200)

Flow diagram of a differencing attack using three stacked labeled boxes.

- **Title (bold 17px `#1a5276`):** "Differencing Attack: Extracting Suppressed Values".
- **Boxes (200×35px, starting at x=50, white bold 12px text inside; result text to the right in 12px):**
  - Blue `#3498db` box at y=45: "Query A: \"Zip 02138, Age 45-50\"" — right label `#333`: "= 8 people (RELEASED)".
  - Green `#27ae60` box at y=90: "Query B: \"...+ Male\"" — right label `#333`: "= 3 people (SUPPRESSED, <5)".
  - Red `#e74c3c` box at y=135: "Query C: A - B" — right label bold red: "= 5 females (DERIVED! Threshold bypassed)".
- **Arrow:** red `#e74c3c` 2px elbow connector from the Query A result row down to the Query C result row, with a small filled red arrowhead, labeled bold 11px red: "ATTACK".

## Regeneration instructions

- **Layout:** one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by its own single-row `.obj-table`: full-width table, left `<td>` (40%) with `.obj-title` + `<ul>` bullets, right `<td>` (60%, centered) with the canvas. Even table rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px. A `.philosophy` callout style (background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em) is defined but unused. No nav bar, no back/home links.
- **Canvas:** HTML attributes `width="720" height="300"`, but a shared `initCanvas(id)` helper re-sizes the backing store to 720×200 × `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates; CSS fixes canvases at 720×200px. Effective drawing area is 720×200.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`/`#e67e22`, blue accent `#3498db`, gray text `#666`/`#333`.
- **Links:** in regenerated HTML, any card links use `.html` extensions (this page has none).
