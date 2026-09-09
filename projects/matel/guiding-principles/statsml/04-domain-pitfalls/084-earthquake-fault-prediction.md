# Earthquake - Domain-Specific Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Earthquake - Domain-Specific Pitfalls

**Subtitle:** Seismic data combines tiny sample sizes, incomplete historical catalogs, and fundamentally unpredictable events — a domain where standard statistical intuition fails badly.

## Prediction Is Fundamentally Impossible

**50+ Years of Precursor Research — No Reliable Signal Found**

- **Failed precursors:** Radon emissions, animal behavior, and electromagnetic anomalies all fail as signals.
- **Seismic gaps too:** Gaps and quiescence have been studied for decades — none reliably precedes an earthquake.
- **Chaotic trigger:** Rupture initiation depends on stress details at scales no instrument can observe.
- **Valid hazard:** "30% chance of M7+ in 30 years" is probabilistic hazard — a defensible statement.
- **Invalid prediction:** "M7 next Tuesday" is prediction of an individual event, and it stays unsolved.
- **Model implication:** Pipelines predicting individual earthquakes fit noise; the honest target is long-run rates.

### Visualization (canvas `c1`, 720×300)

Horizontal bar scoreboard rating precursor candidates by reliability.

- **Title (bold 17px `#1a5276`, centered, y=22):** "50+ Years of Precursor Candidates: Reliability Scoreboard".
- **Rows** (name at x=40, bar starting at x=280, bar width = rel×4 px, bar height 18, rows start y=44 spaced 32px apart):
  - Radon emissions — rel 8, bar `#e74c3c`
  - Animal behavior — rel 4, bar `#e74c3c`
  - Electromagnetic signals — rel 6, bar `#e74c3c`
  - Seismic gaps / quiescence — rel 12, bar `#e74c3c`
  - Foreshock patterns — rel 18, bar `#e67e22`
  - Long-run hazard rates — rel 80, bar `#27ae60`
- **Bar-end label** (`#333`, 17px, right of bar): "unreliable" when rel<20, otherwise "works (but ≠ prediction)".
- **Footer line 1 (bold red `#e74c3c`, centered, y=262):** "No signal predicts individual earthquakes. Only long-run rates are estimable."
- **Footer line 2 (gray `#555`, centered, y=285):** ""30% chance of M7+ in 30 years" is science. "M7 next Tuesday" is not."

## Recurrence Interval Estimated From n=3-5 Events

**Events in 1700, 1857, 1906 → "Average Interval 100 Years" Is Nearly Meaningless**

- **The math:** Three events give two intervals, so "mean recurrence" rests on n=2 gaps.
- **Enormous CI:** The true interval could plausibly be 60 years or 200 years — a huge confidence interval.
- **False precision:** The ~100-year point estimate hides that spread behind one confident-looking number.
- **Wrong model:** The Poisson (memoryless) assumption may not hold — faults can cluster or quasi-cycle.
- **Undecidable:** With n=3 events there is no way to distinguish those regimes from each other.
- **Downstream damage:** Building codes and insurance pricing inherit the unquantified uncertainty.

### Visualization (canvas `c2`, 720×300)

Timeline chart with three known events and a huge confidence band for the next one.

- **Title (bold 17px `#1a5276`, centered, y=22):** "3 Events, 2 Intervals → "Next One Due ~2006" ± A Century".
- **Timeline axis:** horizontal line at y=140 spanning years 1650–2150 mapped from x=50 to x=670 (`#333`, width 1.5); tick marks and gray (`#666`) year labels at 1700, 1800, 1900, 2000, 2100.
- **CI band:** semi-transparent red rectangle `rgba(231,76,60,0.15)` from year 1966 to 2106, y=60 down to the axis; red labels centered at year 2036: "CI for "next event"" (y=78) and "(1966 – 2106+)" (y=98).
- **Known events:** vertical blue `#2980b9` markers (width 3, 55px tall above axis) at 1700, 1857, 1906, each with a bold blue year label above.
- **Interval annotations** (gray `#555`, 17px, at y=axis−20): "157 yr" centered at year 1778, "49 yr" centered at year 1881.
- **Footer line 1 (bold red, centered, y=215):** "Mean "≈100 yr" comes from two wildly different gaps (157, 49)."
- **Footer line 2 (gray `#555`, y=240):** "n=3 events cannot tell periodic from Poisson from clustered."
- **Footer line 3 (gray `#555`, y=262):** "Point estimate drives codes and insurance; the uncertainty band does not."

## Catalog Incompleteness — Detection, Not Occurrence

**"Earthquakes Are Increasing!" — No, Seismometers Are**

- **The artifact:** Pre-1970 catalogs contain only large earthquakes — small events went unrecorded.
- **Network growth:** As seismometer networks expanded, those small events started entering the catalog.
- **The trap:** Counting events per decade shows a steep rise that looks like rising seismicity.
- **Real cause:** That rise is an instrumentation improvement, not any change in actual earthquake rate.
- **Invalid comparison:** Trends must restrict to magnitudes above the oldest period's completeness threshold.
- **General lesson:** When the measurement system changes, raw counts confound detection with the true rate.

### Visualization (canvas `c3`, 720×300)

Bar chart of recorded earthquakes per decade with a falling detection-threshold overlay.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Recorded Earthquakes Per Decade: Detection, Not Occurrence".
- **Bars:** decades 1900, 1920, 1940, 1960, 1980, 2000, 2020 with counts `[12, 18, 25, 45, 110, 180, 240]`; bar height = count×0.6 px above baseline y=200; bars start at x=70, width = (720−140)/7 minus 10px gap. First three decades filled `rgba(41,128,185,0.3)`, later decades solid `#2980b9`. Count value labeled above each bar and decade labeled below (`#333`, 17px).
- **Detection threshold line:** dashed red `#e74c3c` (width 2, dash 6/4) descending across the chart from upper-left (y≈70) to lower-right (y≈175); red left-aligned label near top: "detection threshold (M6+ → M2+)".
- **Footer line 1 (bold red, centered, y=248):** "20x more events recorded — because the network detects smaller quakes."
- **Footer line 2 (gray `#555`, y=272):** "Valid trend analysis: restrict to magnitudes complete in ALL periods."

## Aftershock or Foreshock? Classification Is Uncertain for Days

**M7.0 Then M5.5 Two Days Later — Decaying Aftershock or Foreshock of a Coming M8?**

- **The ambiguity:** A M5.5 following a M7.0 is usually an aftershock of that main shock.
- **The exception:** Occasionally the whole sequence turns out to be a foreshock series to something larger.
- **Statistics work:** Omori's law describes aggregate aftershock decay well across many sequences.
- **Instances don't:** Classifying any single event as aftershock or foreshock stays uncertain for days.
- **Why it matters:** Evacuation, emergency response, and public messaging hinge on that unresolvable distinction.
- **Label problem:** "Aftershock" vs "foreshock" labels are assigned retrospectively, unavailable at decision time.

### Visualization (canvas `c4`, 720×300)

Synthetic seismogram trace over 48 hours showing a main shock, decaying aftershocks, and one ambiguous highlighted event.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Seismogram: Main Shock, Omori Decay — And One Ambiguous Event".
- **Plot area:** margins left 60, right 30, top 40, bottom 40; black L-shaped axes; dashed light-gray (`#ccc`) zero line at vertical center; x axis labeled "Time (hours)" with ticks every 8 hours from 0 to 48.
- **Trace:** 960 points of a black (`#000`, width 1) waveform built from seeded pseudo-random noise (amplitude 3% of max) plus damped sinusoid bursts: main shock at hour 4 (amplitude 0.95×max, ~0.8h duration), aftershocks at hours 5.5, 7, 9, 11, 14, 17, 24, 30, 36, 42 with relative amplitudes 0.35, 0.28, 0.22, 0.18, 0.14, 0.11, 0.08, 0.06, 0.05, 0.04, and an ambiguous event at hour 20 (amplitude 0.45×max, ~0.6h duration).
- **Highlights:** main-shock segment re-stroked in red `#e74c3c` (width 1.8); ambiguous-event segment re-stroked in orange `#e67e22` (width 1.8).
- **Labels (17px, near top):** red "Main shock (M7.0)" over the main shock; orange "Aftershock or foreshock?" over the hour-20 event.
- **Omori decay envelope:** dashed gray `#999` curve from the main shock following envelope = 0.95×max / (3·Δt+1)^0.8; gray label "Omori decay" at right side above center.

## Induced Seismicity — Human Activity Breaks the Historical Baseline

**Fracking, Wastewater Injection, Reservoirs → Earthquakes Where the Record Says "Stable"**

- **The mechanism:** Wastewater injection, fracking, and reservoir filling change subsurface stress.
- **The result:** That stress change triggers earthquakes in regions that were previously quiet.
- **Not in the record:** These events have no historical precedent anywhere in the seismic catalog.
- **Near-zero risk:** So catalog-based hazard models assign the affected area almost no risk at all.
- **Systematic underestimate:** Maps calibrated on "natural" seismicity understate risk wherever injection began.
- **Non-stationarity:** The data-generating process changed — the pre-injection era is a different distribution.

### Visualization (canvas `c5`, 720×300)

Bar chart of earthquakes per year before vs after wastewater injection begins.

- **Title (bold 17px `#1a5276`, centered, y=22):** ""Stable" Region: Earthquakes Per Year Before vs After Injection".
- **Bars:** years 2004, 2006, 2008, 2010, 2012, 2014, 2016 with counts `[2, 1, 3, 25, 60, 110, 90]`; bar height = count×1.2 px above baseline y=200; bars start at x=70, width = (720−140)/7 minus 12px gap. First three years green `#27ae60`, remaining years red `#e74c3c`. Count above each bar, year below.
- **Injection marker:** dashed orange `#e67e22` vertical line (width 2, dash 6/4) between the 2008 and 2010 bars, from y=45 to baseline, with bold orange left-aligned label "wastewater injection begins".
- **Hazard model level:** dashed blue `#2980b9` horizontal line (dash 3/3) just above the baseline across the chart, with blue label "hazard model: "~2/yr"".
- **Footer line 1 (bold red, centered, y=248):** "The historical catalog says "stable." The process changed underneath it."
- **Footer line 2 (gray `#555`, y=272):** "Models trained on natural seismicity underestimate induced risk by 10-50x."

## Building Vulnerability Is Unknown Until It Fails

**Damage = Ground Motion × Building Response — And the Second Factor Is Unobserved**

- **The equation:** Predicting damage needs both ground motion and building response as inputs.
- **Split observability:** Ground motion is measurable; building response is unknown without physical inspection.
- **Code ≠ construction:** A building permitted under modern code may not actually have been built to it.
- **Paper vs concrete:** Compliance in the permit file is not compliance in the poured structure.
- **Post-facto discovery:** Soft-story and other vulnerable configurations are often identified only after collapse.
- **Model gap:** Loss models plugging in code-assumed fragility curves understate damage for non-compliant stock.

### Visualization (canvas `c6`, 720×300)

Equation-box diagram plus horizontal bars comparing assumed vs actual collapse probability.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Damage = Ground Motion × Building Response".
- **Equation boxes** (60px tall at y=50, each with a bold first line and regular second line, both centered):
  - x=40, width 190: fill `rgba(39,174,96,0.12)`, stroke `#27ae60`, text `#27ae60`: "Ground motion" / "measured".
  - x=280, width 190: fill `rgba(231,76,60,0.12)`, stroke `#e74c3c`, text `#e74c3c`: "Building response" / "UNKNOWN".
  - x=520, width 160: fill `rgba(41,128,185,0.3)`, stroke `#2980b9`, text `#1a5276`: "Damage" / "???".
  - Bold "×" between boxes 1 and 2 (x=255) and "=" between boxes 2 and 3 (x=497), at y=86.
- **Bar section header (bold `#333`, left-aligned at x=40, y=150):** "Collapse probability at design-level shaking:".
- **Bars** (name at x=40, bar starting x=330, width = value×6 px, height 18, rows from y=165 spaced 30px; percent value labeled right of bar):
  - Code-assumed fragility — 5%, `#27ae60`
  - Actual (as-built) stock — 18%, `#e67e22`
  - Soft-story (found post-collapse) — 45%, `#e74c3c`
- **Footer (bold red, centered, y=282):** "Code compliance on paper ≠ construction in concrete. Vulnerability is revealed by failure."

## Regeneration instructions

- **Layout:** standard detail-page structure — h1, `.subtitle`, then one `<h2>` per pitfall, each followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` + `<ul>` of labeled bullets, right `<td>` (60%, centered) holds one canvas. Even table rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvases:** all six declared 720×300; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper that multiplies the backing store, and calls `ctx.scale` so drawing stays in logical coordinates. Chart text is 17px -apple-system (bold for titles/emphasis).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#333`/`#555`/`#666`/`#999`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
