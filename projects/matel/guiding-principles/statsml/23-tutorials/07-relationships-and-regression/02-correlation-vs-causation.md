# Correlation vs Causation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table — text left 50%, canvas right 50%)
**HTML title tag:** Correlation vs Causation

**Subtitle:** Two things moving together does not mean one is driving the other — often a third thing is driving both

## Ice Cream and Drownings Rise Together

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — a seaside town tracks monthly ice cream sales and drowning deaths
- **They match eerily** — both bottom out in January and peak in July
- **The correlation is real** — across the 12 months, r = 0.99, almost perfect
- **The trap** — it is tempting to conclude ice cream somehow causes drownings
- **Nothing here says why** — correlation reports co-movement, not mechanism

*Example:* July: 62,000 cones sold and 14 drownings; January: 12,000 cones and 2 drownings.

**Core idea:** a correlation, even a near-perfect one, is only a description of two columns of numbers — it carries no arrow saying which causes which.

### Visualization (canvas `c1`, 720×300)

Dual-axis line chart: monthly cones sold and drownings over one year.

- **Title (bold 15px, `#1a5276`, top center):** "Monthly Ice Cream Sales and Drownings — One Year".
- **Shared data (used across charts):** months J F M A M J J A S O N D; TEMP = `[3, 4, 9, 14, 19, 24, 27, 26, 21, 15, 9, 4]` °C; CONES (thousands) = `[12, 13, 20, 28, 40, 55, 62, 60, 45, 30, 18, 13]`; DROWN = `[2, 2, 3, 5, 8, 12, 14, 13, 8, 5, 3, 2]`.
- **Axes:** x = 12 month initials; left y for cones 0–70 (labels "0k" to "70k" every 20, in blue `#2a78d6`); right y for drownings 0–16 (labels every 4, in orange `#d95926`); full rectangular frame in gray `#999`; padding top 46, bottom 46, left 58, right 62.
- **Series:** cones as blue `#2a78d6` line (width 3) with 4px dots; drownings as orange `#d95926` line (width 3) with 4px dots.
- **In-plot legend (bold 12px, upper-left):** blue "cones sold (thousands)", orange "drownings".
- **Annotation (magenta `#d55181`, bold 13px, centered near top over July):** "r = 0.99 — but neither causes the other".

## Four Stories That Fit the Same Correlation

**Tags:** `core idea` (blue), `checklist` (orange)

- **A causes B** — eating ice cream somehow makes people drown (implausible here)
- **B causes A** — drownings drive ice cream sales (even less plausible)
- **C causes both** — hot weather sends people to buy cones AND to swim
- **Pure luck** — with little data, two unrelated things can line up by chance
- **Walk all four** — every correlation you meet fits at least one of these stories

*Example:* Summer is the hidden "C": temperature correlates 0.99 with cones and 0.97 with drownings.

**Checklist:** before believing A causes B, ask which of the four stories fits — the hidden-third-thing story (a "confounder") is the usual culprit.

### Visualization (canvas `c2`, 720×300)

Box-and-arrow causal diagram of the four candidate explanations.

- **Title (bold 15px, `#1a5276`):** "Four Stories Behind \"Ice Cream and Drownings Correlate\"".
- **Left column — three plain boxes (150×50, background `#f8f9fa`, muted `#6b7280` 1.5px border), stacked at y=60/130/200:**
  - "Story 1: A → B" / "cones cause drownings?"
  - "Story 2: B → A" / "drownings sell cones?"
  - "Story 4: luck" / "lined up by chance?"
  - Muted caption below them (12px, centered at x=95): "stories the data" / "cannot rule out alone".
- **Right diagram — Story 3, centered at cx=445:**
  - Top box (150×50 at y=62), highlighted: green `#008300` 2.5px border, fill `rgba(0,131,0,0.10)`, bold title "HOT WEATHER (C)" / "the hidden driver".
  - Lower-left box (160×50 at y=190): blue `#2a78d6` border, "ice cream sales (A)" / "people want cones".
  - Lower-right box (160×50 at y=190): orange `#d95926` border, "drownings (B)" / "people go swimming".
  - Two green arrows (width 2, filled triangular heads) from the C box down to each lower box.
  - Dashed magenta `#d55181` horizontal link (dash 6/5, width 2) between A and B at y=215, labeled above in bold magenta 12px: "correlated, no arrow".
- **Bottom annotation (green bold 13px, centered at cx):** "Story 3 fits: one cause upstream of both".
- Box first lines are bold 12–13px in the border color; second lines 12px `#444`.

## Hold Temperature Fixed and the Link Vanishes

**Tags:** `worked example` (green), `by hand` (blue)

- **The test** — compare pairs of months that had nearly the same temperature
- **March vs November** — both 9°C: 20k vs 18k cones, drownings 3 vs 3
- **April vs October** — 14° and 15°: 28k vs 30k cones, drownings 5 vs 5
- **May vs September** — 19° and 21°: 40k vs 45k cones, drownings 8 vs 8
- **The verdict** — at equal temperature, more cones no longer means more drownings

*Example:* Once you know it was 9°C, the extra 2,000 cones in March predict nothing about drownings.

**The move:** "controlling for" a suspect cause means comparing cases where it is equal — if the correlation disappears, the suspect was driving it.

### Visualization (canvas `c3`, 720×300)

Scatter of cones vs drownings for the 12 months, colored by temperature band, with a matched pair circled and a legend.

- **Title (bold 15px, `#1a5276`):** "Cones vs Drownings, Coloured by Temperature".
- **Data:** the 12 (CONES, DROWN) pairs from the shared arrays; point color by temperature band: cold < 10°C blue `#2a78d6`, mild 10–21°C aqua `#199e70`, hot > 21°C orange `#d95926`; dots radius 6.
- **Axes:** x 0–70 (labels "0k"–"70k" every 10k), y 0–16 (labels every 4); L-shaped gray axes; padding top 46, bottom 46, left 58, right 180 (legend room). Axis titles: "monthly cones sold" (bottom), rotated "drownings" (left).
- **Matched-pair highlight:** dashed violet `#4a3aa7` ellipse (26×15, dash 4/3, width 2) around the Mar & Nov points at ~(19k, 3); violet bold 12px two-line label to its upper right: "Mar & Nov: both 9°C —" / "cones differ, drownings equal".
- **Legend (right side, 12px, color swatch squares):** blue "cold (< 10°C)", aqua "mild (10–21°C)", orange "hot (> 21°C)".
- **Legend-column annotation (green `#008300`, bold 13px, three lines):** "temperature walks" / "up the line —" / "the cones don't".

## Why a Data Scientist Cares

**Tags:** `where it's used` (orange), `common mistake` (red)

- **Wrong lever** — banning ice cream in July would leave drownings at 14
- **Right lever** — acting on the real cause (swimming safety) is what moves the number
- **Same trap at work** — "users of feature X churn less" rarely means X prevents churn
- **Engaged users differ** — the kind of user who adopts X was already going to stay
- **The fix** — run an experiment (A/B test): randomly give X, then compare churn

*Example:* A team once emailed everyone to adopt a feature "linked to retention" — retention did not move.

**Common mistake:** spending money to move a correlated metric instead of the cause — only randomized experiments (or careful controls) tell you where the lever is.

### Visualization (canvas `c4`, 720×300)

Three-bar chart: July drownings under two interventions vs doing nothing.

- **Title (bold 15px, `#1a5276`):** "July Drownings Under Two Interventions (illustrative)".
- **Bars (130px wide, 75% alpha fills, evenly spaced):**
  - "do nothing" — value 14, muted gray `#6b7280`.
  - "ban ice cream" — value 14, magenta `#d55181`.
  - "lifeguards + lessons" — value 8, green `#008300`.
- **Axes:** y 0–16 (labels every 4); L-shaped gray axes; padding top 56, bottom 64, left 70, right 30; rotated y-axis title "drownings in July"; value labels bold 13px `#222` above each bar; bar labels 12px below.
- **Annotations (bold 13px, near top):** magenta "acting on the correlation changes nothing" (centered over the middle bar); green "acting on the cause moves the number" (offset 40px right, one line below).
- **Caption (muted 12px, bottom center):** "numbers are illustrative — the shape is the point".

## Regeneration instructions

- **Template:** tutorials topic-page layout. Page: `<h1>` + `.subtitle`, then 4 `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, bottom border 2px solid `#2980b9`) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pill spans (0.72rem, 600 weight, padding 2px 10px, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (`li b` in `#1a5276`); one italic `.example` paragraph (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem) starting with a `<strong>` label.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each declared 720×300 intrinsic; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Shared global arrays MONTHS/TEMP/CONES/DROWN are hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (tutorials `P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette accents: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
