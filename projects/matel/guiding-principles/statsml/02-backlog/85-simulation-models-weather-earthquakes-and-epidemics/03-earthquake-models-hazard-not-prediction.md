# Earthquake Models — Hazard, Not Prediction

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Earthquake Models — Why Prediction Fails and Hazard Forecasting Works

**Subtitle:** After a century of failed prediction schemes, seismology settled on a different product — not "when", but "how likely, how strong, and how many seconds of warning".

**Intro callout (blue-left-border box):** No method reliably predicts the time, place, and magnitude of an individual earthquake — decades of candidate precursors have failed every rigorous out-of-sample test. So the working models answer probabilistic questions instead: hazard maps that set building codes, aftershock forecasts published hours after a mainshock, and early-warning systems that buy seconds during one. Each product is honest about what the physics allows.

## 1. Why prediction keeps failing

Every few decades a new precursor is announced — and every one has failed when tested against quakes it was not fitted to.

- **Proposed precursors:** animal behavior, radon gas, and foreshock patterns have all been tried.
- **The test they fail:** none survives evaluation on earthquakes outside the data it was tuned on.
- **Criticality:** faults sit near a critical state, so identical stress conditions can end in tiny or huge ruptures.
- **No size signal:** the start of a rupture carries no readable signature of how large it will grow.
- **Foreshock hindsight:** a foreshock is only labeled a foreshock after the mainshock has happened.
- **Irregular clocks:** recurrence intervals on a single fault scatter far too widely to extrapolate a date.
- **The honest deliverable:** seismology ships probabilities per time window, never a date.

Key point: Prediction fails not from missing data but from the physics: a near-critical system makes rupture size unknowable at rupture start. The defensible product is a probability, not an appointment.

### Visualization (canvas `c1`, 720×340)

Timeline of large quakes on one fault over four centuries — irregular gaps annotated with their intervals — contrasted with a dashed uniform ruler below showing what a periodic fault would look like.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Same fault, irregular clock — the average interval hides wild scatter"
- **Observed record label:** bold 12px `#1a5276` left-aligned at (60, 72): "OBSERVED RECORD — one fault, four centuries".
- **Observed timeline:** 1.5px `#1a5276` horizontal line at y=130 from x=60 to x=660. Quake marks: 2.5px `#e74c3c` vertical stems from y=108 to y=152 at the six event years 1600, 1685, 1745, 1857, 1890, 1985 mapped by x = 60 + (year − 1580) × 600 / 440 (x ≈ 87, 203, 285, 438, 483, 612). Year labels 10px `#999` centered under each stem at y=168.
- **Interval annotations:** 10px `#666` centered between consecutive stems at y=100: "85 yr", "60 yr", "112 yr", "33 yr", "95 yr".
- **Scatter note:** bold 10px `#e67e22` right-aligned at (660, 88): "shortest gap 33 yr, longest 112 yr".
- **Periodic ruler label:** bold 12px `#666` left-aligned at (60, 212): "IF QUAKES WERE PERIODIC — same average rate".
- **Periodic ruler:** dashed (5/4) 1.5px `#999` horizontal line at y=262 from x=60 to x=660; six 1.5px `#999` vertical stems from y=246 to y=278 evenly spaced at x = 60, 180, 300, 420, 540, 660; 10px `#999` centered "77 yr" between the first two stems at y=238 and 10px `#999` centered "…every 77 years, forever" at (480, 238).
- **Caption (12px `#999`, centered, y = h−14):** "The average interval exists — the date of the next event does not follow from it"

## 2. Probabilistic seismic hazard analysis (PSHA)

The workhorse model combines three ingredients into one curve: how likely each shaking level is at a given site over a fixed window.

- **Ingredient 1 — sources:** a catalog of faults and area sources, each with an event rate.
- **Ingredient 2 — Gutenberg–Richter:** each magnitude step up is roughly 10x rarer than the last.
- **Ingredient 3 — attenuation:** ground-motion models describe how shaking decays with distance.
- **The output:** a statement like "10% chance of exceeding this shaking level in 50 years".
- **Who consumes it:** building codes and insurance pricing are set directly from the hazard curve.
- **The map form:** hazard curves computed on a grid become the national seismic hazard maps.
- **The criticism:** a 50-year exceedance probability is hard to validate on human timescales.

Key point: PSHA never says when — it integrates rates, magnitudes, and attenuation into the probability that shaking exceeds a level. That one number is what concrete columns and insurance premiums are actually sized against.

### Visualization (canvas `c2`, 720×360)

Flow diagram: three stacked input boxes (sources, magnitude-frequency, attenuation) with arrows converging into a hazard-curve chart of exceedance probability falling against shaking intensity.

- **Title (bold 14px `#1a5276`, centered, y=22):** "PSHA: three inputs combine into one exceedance curve"
- **Input boxes:** three 200×64 boxes at x=40, tops y=64, 154, 244. Box 1: fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border; left-aligned at x+12: bold 11px `#1a5276` "FAULT SOURCES" (y+22), 10px `#555` "which faults exist and" (y+40), "how often they rupture" (y+54). Box 2: white fill, 2px `#e67e22` border; bold 11px `#e67e22` "GUTENBERG–RICHTER" (y+22), 10px `#555` "each magnitude step up" (y+40), "is ~10x rarer" (y+54). Box 3: white fill, 2px `#27ae60` border; bold 11px `#27ae60` "ATTENUATION" (y+22), 10px `#555` "shaking decays with" (y+40), "distance from the fault" (y+54).
- **Converging arrows:** 2px `#999` lines with filled arrowheads from each box's right edge midpoint (x=240; y=96, 186, 276) to a junction at (320, 186); then one 2px `#999` arrow from (320, 186) to (368, 186).
- **Hazard chart:** axes 1.5px `#999` — vertical from (400, 60) to (400, 300), horizontal from (400, 300) to (680, 300). Axis labels 10px `#999`: "P(exceed in 50 yr)" left-aligned at (404, 54); "peak ground shaking →" centered at (540, 318).
- **Hazard curve:** 2.5px `#e74c3c` polyline over x=405..675 with y = 300 − 210 × exp(−(x − 405) / 90) (steep fall flattening into a long tail).
- **Design-level guides:** dashed (4/4) 1px `#1a5276` horizontal line at y=232 from x=400 to x=452, meeting a dashed vertical line at x=452 from y=232 down to y=300; bold 10px `#1a5276` left-aligned at (460, 226): "10% in 50 yr" and 10px `#666` left-aligned at (460, 240): "→ building-code design level".
- **Caption (12px `#999`, centered, y = h−14):** "The output is a probability of a shaking level — never a date on a calendar"

## 3. Aftershocks: the forecastable part

The one place earthquake statistics work beautifully is after the mainshock: aftershock sequences obey stable empirical laws.

- **Omori's law (1894):** the aftershock rate decays roughly as 1/time since the mainshock.
- **Still Gutenberg–Richter:** aftershock magnitudes follow the same 10x-per-step frequency law.
- **ETAS models:** every quake is treated as a potential parent that triggers its own offspring quakes.
- **Cascading structure:** the sequence is a self-exciting point process, aftershocks of aftershocks.
- **Operational forecasts:** agencies publish aftershock probabilities within hours of a mainshock.
- **What they say:** the chance of a magnitude-5+ aftershock in the next week, updated daily.
- **Why it works:** the mainshock resets the clock, and the decay from that reset is lawful.

Key point: The same statistics that cannot date the mainshock describe its aftermath with textbook regularity — Omori decay in time, Gutenberg–Richter in size. Conditional on the big one, the sequence is forecastable.

### Visualization (canvas `c3`, 720×360)

Aftershock rate decay curve over the days following a mainshock spike, with magnitude stem-lines under the curve thinning out over time.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Omori's law — the aftershock rate falls off as roughly 1 / time"
- **Axes:** 1.5px `#999` — vertical from (60, 56) to (60, 290), horizontal from (60, 290) to (660, 290). Labels 10px `#999`: "events / day" left-aligned at (66, 52); "days since mainshock →" centered at (360, 322).
- **Mainshock spike:** 3px `#e74c3c` vertical stem from (85, 290) up to (85, 74); bold 11px `#e74c3c` left-aligned at (94, 78): "M 7.1 MAINSHOCK".
- **Rate curve:** 2.5px `#1a5276` polyline over x=95..655 with y = 290 − 175 / (1 + (x − 85) / 55) (hyperbolic Omori decay from the spike).
- **Aftershock stems:** 1.5px `rgba(26,82,118,0.35)` vertical stems from y=290 up by their heights, dense early and sparse late, at (x, height) pairs: (100, 96), (108, 62), (116, 118), (125, 54), (134, 84), (145, 44), (158, 70), (172, 38), (188, 56), (208, 30), (232, 46), (262, 26), (300, 38), (348, 20), (406, 28), (478, 16), (566, 22), (640, 12).
- **Decay annotation:** bold 11px `#e67e22` right-aligned at (648, 130): "rate halves, then halves again — Omori, 1894", with a thin 1px `#e67e22` connector line from (560, 136) down to the rate curve near (500, 172).
- **Caption (12px `#999`, centered, y = h−14):** "The clock nobody can read before the mainshock reads cleanly after it"

## 4. Early warning: seconds, not days

Early-warning systems do not predict anything — they detect a quake already underway and outrun its damaging waves electronically.

- **Two wave types:** P-waves travel fast but shake weakly; damaging S-waves arrive later.
- **The trick:** sensors near the epicenter detect the P-wave and alert at the speed of light.
- **The race:** the alert races the S-wave, and the alert always travels faster.
- **Warning grows with distance:** a city 200 km out can get tens of seconds of notice.
- **The blind zone:** near the epicenter the S-wave arrives before any alert can beat it.
- **What seconds buy:** trains brake, firehouse doors open, surgeons pause, people drop-cover-hold.
- **Deployed systems:** Japan, Mexico, and the U.S. West Coast run public alerts today.

Key point: Early warning replaces the impossible question "when will it strike" with an answerable one — "it has struck; how many seconds until the shaking reaches you". The product is a head start, not a prophecy.

### Visualization (canvas `c4`, 720×380)

Map-style diagram: concentric P and S wavefronts expanding from an epicenter, a shaded blind zone around it, a sensor catching the P-wave, and a distant city annotated with its seconds of warning.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Racing the S-wave — warning time grows with distance"
- **Epicenter:** filled `#e74c3c` circle radius 6 at (180, 215); bold 11px `#e74c3c` centered at (180, 344): "epicenter".
- **Blind zone:** filled `rgba(231,76,60,0.12)` circle radius 48 centered at (180, 215); bold 10px `#e74c3c` centered at (180, 322): "blind zone — alert arrives too late", with a thin 1px `#e74c3c` connector from (180, 312) up to (180, 262).
- **S wavefronts:** 2px `#e74c3c` circles centered at (180, 215) with radii 48, 82, 116; bold 10px `#e74c3c` left-aligned at (250, 118): "S-wave — slow, damaging".
- **P wavefronts:** 1.5px dashed (5/4) `#2980b9` circles centered at (180, 215) with radii 150, 200, 250; bold 10px `#2980b9` left-aligned at (352, 78): "P-wave — fast, weak".
- **Sensor:** filled `#27ae60` triangle with vertices (298, 148), (290, 164), (306, 164); bold 10px `#27ae60` left-aligned at (312, 160): "sensor detects P-wave, alerts instantly".
- **City:** three filled `#555` rectangles as a skyline at (556, 190) 16×50, (576, 175) 18×65, (598, 200) 14×40 (all bottoms at y=240); bold 11px `#1a5276` centered at (585, 258): "city, 200 km away"; bold 11px `#27ae60` centered at (585, 276): "~25 seconds of warning".
- **Alert path:** dashed (3/4) 1.5px `#27ae60` line from (306, 156) to (556, 200) with a filled `#27ae60` arrowhead at the city end; 10px `#666` centered at (430, 156): "alert travels at light speed".
- **Bottom note:** 11px `#666` centered at (360, 306): "seconds are enough to slow trains, open firehouse doors, drop-cover-hold".
- **Caption (12px `#999`, centered, y = h−14):** "Not a prediction — a race between electrons and shear waves"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above. The h1 carries no index number.
- **Bullet style:** each bullet is a bold label + one short complete sentence that fits on one line at normal page width — split dense content into more bullets rather than longer ones. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 340/360/360/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`. Every canvas has a bold 14px `#1a5276` centered title at y=22 and a 12px `#999` centered caption at y = h−14.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(231,76,60,0.12)`.
- No nav bar, no back/home links, no cross-references to other pages, no item counts.
