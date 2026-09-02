# Hurricane Forecasting

**Page type:** detail page (four titled sections, each a two-column row: text left 50%, canvas right 50%)
**HTML title tag:** Hurricane Forecasting — Track Cones, Spaghetti Models, and the Intensity Problem

**Subtitle:** A hurricane forecast is an ensemble product end to end — many models vote on the track, historical error defines the cone, and intensity remains the stubborn unsolved half.

**Intro callout (blue-left-border box):** Hurricane forecasting runs the full simulation stack at once: global weather models supply the steering flow that moves the storm, regional high-resolution models resolve its inner core, statistical baselines keep everyone honest, and downstream hydrodynamic models turn the wind field into surge and flood predictions. Every public product — the spaghetti plot, the cone, the surge map — is a different way of summarizing that stack's agreement and disagreement.

## 1. Spaghetti models: many trajectories, one consensus

The famous spaghetti plot is an ensemble across institutions: every line is a different model's answer to the same question — where does this storm go?

- **One line, one model:** Each strand on the plot is a separate model's forecast track for the same storm.
- **Global models:** Worldwide models capture the large-scale steering flow that carries the storm along.
- **Regional models:** High-resolution hurricane models resolve the inner core the global grids smear out.
- **Statistical baselines:** Simple climatology-and-persistence tracks set the floor any physics model must beat.
- **Consensus wins:** The average of the credible models beats nearly every individual model over a season.
- **Divergence is signal:** When the strands fan apart, the forecast is genuinely uncertain days in advance.
- **Agreement is signal too:** When independent models converge on one track, confidence is justified.

Key point: The consensus track is the ensemble lesson in miniature: averaging independent, competent forecasts cancels their independent errors, which is why the blend beats nearly every model inside it.

### Visualization (canvas `c1`, 720×360)

Stylized coastline map with about ten colored model tracks fanning out from one storm position, the consensus track drawn bold.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Every line is a model — the consensus is the forecast"
- **Land:** polygon filled `#f8f9fa` with a 2px `#999` coastline edge — points (40, 40), (300, 40), (280, 90), (230, 140), (200, 200), (150, 250), (60, 290), (40, 290), closed back to (40, 40); 11px `#999` label "land" left-aligned at (70, 80).
- **Storm:** filled `#e74c3c` circle radius 7 at (600, 290); 2px `#e74c3c` ring radius 12 around it; bold 11px `#e74c3c` centered label "storm now" at (600, 320).
- **Model tracks:** ten 1.5px quadratic curves from (600, 290), control point (430, 210), to endpoints (150 + i×22, 75 + i×14) for i = 0..9; colors in order: `#27ae60`, `#e67e22`, `#8e44ad`, `#2980b9`, `#bbb`, `#999`, `#27ae60`, `#e67e22`, `#8e44ad`, `#2980b9`.
- **Consensus track:** 3.5px `#1a5276` quadratic curve from (600, 290), control (430, 210), to (249, 138) — the middle of the endpoint fan.
- **Legend (left-aligned block starting (400, 60), 16px line spacing):** 22px 1.5px `#999` line swatch + 11px `#555` label "individual model tracks"; 22px 3.5px `#1a5276` swatch + 11px `#555` label "consensus of models".
- **Fan annotation:** bold 11px `#e67e22` left-aligned at (400, 110): "spread between models ="; second line at (400, 126): "early warning of uncertainty".
- **Caption (12px `#999`, centered, y = h−14):** "The blend beats nearly every model in it — divergence flags uncertainty days early"

## 2. The cone of uncertainty — what it does and doesn't say

The cone is not drawn from this storm's physics at all — it is a purely statistical object built from the forecast errors of past seasons.

- **Historical error:** The cone radius at each lead time is set by past official track errors at that lead time.
- **Two-thirds rule:** The cone is sized to enclose roughly two-thirds of historical center errors.
- **Center path only:** The cone bounds where the storm's center will probably go — nothing else.
- **Not the storm's size:** Hurricane-force winds routinely extend far outside the cone's edges.
- **One in three:** By construction, the center escapes the cone about a third of the time.
- **The deadly misread:** "We're outside the cone, so we're safe" is the most dangerous sentence in a landfall.
- **Same width all season:** The cone does not narrow for a well-behaved storm or widen for an erratic one.

Key point: The cone answers exactly one question — where has the official forecast's center error historically landed — and the public reliably reads it as an impact map, which it has never been.

### Visualization (canvas `c2`, 720×340)

Cone widening from the storm position across a coastline, with a dashed storm-size circle far wider than the cone at one forecast point.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The cone bounds the center's path — not the storm, not the impacts"
- **Land:** polygon filled `#f8f9fa` with a 2px `#999` coastline edge — points (40, 50), (250, 50), (215, 120), (170, 190), (110, 260), (40, 290), closed back to (40, 50); 11px `#999` label "land" left-aligned at (60, 84).
- **Center track points:** (620, 280), (520, 240), (420, 205), (320, 175), (220, 150) with cone half-widths 0, 18, 36, 56, 78 at each point.
- **Cone fill:** polygon `rgba(26,82,118,0.12)` from the upper boundary points (track y − half-width) out and back along the lower boundary (track y + half-width); 1.5px `#1a5276` outline along both boundaries.
- **Center track:** 2px dashed (5/4) `#1a5276` polyline through the five track points; filled `#1a5276` dots radius 4 at each point.
- **Storm marker:** filled `#e74c3c` circle radius 7 at (620, 280); bold 11px `#e74c3c` centered "storm now" at (620, 312).
- **Storm-size circle:** 1.5px dashed (5/4) `#e74c3c` circle radius 85 centered at (420, 205) — far wider than the cone's 36px half-width there.
- **Annotation:** bold 11px `#e74c3c` left-aligned at (420, 60): "wind field is much wider than the cone —"; second line at (420, 76): "impacts extend beyond the cone".
- **Cone note:** bold 11px `#1a5276` left-aligned at (150, 100): "cone = ~2/3 of historical center error"; 11px `#666` at (150, 118): "the center leaves it about 1 time in 3".
- **Caption (12px `#999`, centered, y = h−14):** "Outside the cone is not safe — the cone was never an impact map"

## 3. Intensity: the hard half

For decades the track forecast improved on schedule while the intensity forecast barely moved — the same storm, two very different prediction problems.

- **Track fell steadily:** Average track error has dropped decade after decade as models and data improved.
- **Intensity stalled:** Intensity error stayed nearly flat for much longer before finally bending down.
- **Why the split:** Track follows the large-scale steering flow that global models resolve well.
- **Below the grid:** Intensity lives in inner-core convection at scales smaller than the model cells.
- **Rapid intensification:** A large wind-speed jump within 24 hours is the deadliest forecast surprise.
- **Hardest call:** Rapid intensification near landfall leaves no time to evacuate if it is missed.
- **Recent gains:** High-resolution coupled ocean-atmosphere models have finally started cutting intensity error.

Key point: Track and intensity diverged because they live at different scales — the steering flow is resolvable physics, while the inner core sits below grid resolution, exactly where models must fall back on parameterization.

### Visualization (canvas `c3`, 720×340)

Two error-trend lines over three decades: track error falling steeply, intensity error nearly flat with a slight late decline.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Track error fell for decades — intensity error barely moved"
- **Axes:** 1.5px `#999` lines — horizontal at y=280 from x=60 to x=660, vertical at x=60 from y=50 to y=280; 10px `#999` year labels centered at y=296: "1990" (x=80), "2000" (x=245), "2010" (x=410), "2020" (x=574), "2024" (x=640); 10px `#999` label "48-h forecast error" left-aligned at (64, 46).
- **Track error:** 2.5px `#27ae60` polyline through (80, 81), (245, 129), (410, 179), (574, 217), (640, 228); bold 11px `#27ae60` label "track error" left-aligned at (300, 130).
- **Intensity error:** 2.5px `#e74c3c` polyline through (80, 150), (245, 154), (410, 158), (574, 175), (640, 188); bold 11px `#e74c3c` label "intensity error" left-aligned at (300, 190).
- **Annotation:** bold 11px `#e67e22` left-aligned at (410, 240): "rapid intensification remains"; second line at (410, 256): "the hardest, deadliest call".
- **Caption (12px `#999`, centered, y = h−14):** "Steering flow is resolvable physics; the inner core lives below the grid"

## 4. From wind to water: surge and rain models

The forecast chain does not end with the wind — water is what kills, and separate simulation models turn the wind field into surge and flood predictions.

- **Water, not wind:** Most hurricane deaths come from storm surge and flooding rather than wind itself.
- **Galveston 1900:** The deadliest US natural disaster was overwhelmingly a storm-surge event.
- **Surge models:** Coastal hydrodynamic grid models are driven by the forecast wind field to predict surge.
- **Track sensitivity:** A small shift in landfall angle can move the worst surge to a different bay entirely.
- **Tide timing:** The same surge arriving at high tide instead of low tide adds the full tidal range.
- **Coastline shape:** Shallow, funnel-shaped coasts amplify surge far beyond the open-ocean value.
- **Inland flooding:** Stalled storms dump rain modeled separately with rainfall-runoff hydrology.

Key point: Surge is a compound event — wind field, track angle, tide phase, and coastline shape multiply together — so small track errors upstream become large water errors downstream, and the wind category alone badly understates the risk.

### Visualization (canvas `c4`, 720×360)

Coastal cross-section showing normal tide, storm surge stacked on the tide, and waves on top, with labeled heights.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Surge stacks on tide, waves stack on surge — the water arrives in layers"
- **Seabed and land:** filled `#ccc` polygon — (40, 320), (420, 320), (560, 230), (680, 195), (680, 330), (40, 330), closed; 10px `#999` label "seabed → shore → land" centered at (360, 344).
- **Normal sea:** filled `rgba(26,82,118,0.12)` polygon from x=40 to the shoreline where y=260 meets the ground slope (about x=513): (40, 260), (513, 260), then back along the ground — (420, 320), (40, 320), closed; 1.5px dashed (5/4) `#999` line along y=260 from x=40 to x=513; 10px `#999` label "normal high tide" left-aligned at (48, 254).
- **Surge water:** filled `rgba(26,82,118,0.35)` polygon from x=40 to the shoreline where y=195 meets the ground slope (about x=680): (40, 195), (680, 195), then back along the ground — (560, 230), (420, 320), (40, 320), closed, drawn over the normal-sea fill; 2px `#1a5276` line along y=195 from x=40 to x=680.
- **Waves:** 2px `#2980b9` polyline over x=40..400: `y = 183 − 12·sin((x−40)/22)` — crests riding on top of the surge line.
- **House:** simple glyph at the shore — filled `#555` rect 26×20 at (600, 205) with a `#555` triangle roof from (596, 205) to (613, 190) to (630, 205); the surge line at y=195 passes through it.
- **Height brackets (left side):** 1.5px `#27ae60` vertical bracket from (86, 320) to (86, 260) with 4px end ticks, bold 10px `#27ae60` label "normal depth" left-aligned at (94, 294); 1.5px `#e74c3c` vertical bracket from (150, 260) to (150, 195) with end ticks, bold 10px `#e74c3c` label "storm surge +15 ft" left-aligned at (158, 232); 1.5px `#e67e22` vertical bracket from (214, 195) to (214, 171) with end ticks, bold 10px `#e67e22` label "waves on top" left-aligned at (222, 187).
- **Flood annotation:** bold 11px `#e74c3c` centered at (600, 168): "coastal homes flood".
- **Caption (12px `#999`, centered, y = h−14):** "Track angle, tide phase, and coastline shape multiply — wind category alone understates the water"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (50%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Bullet style:** each bullet is a bold colored label + one short complete sentence that fits on one line at normal page width; split dense content into more bullets rather than longer sentences. Bullet `<strong>` labels are colored `#1a5276` (via `li strong { color: #1a5276; }`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 360/340/340/360 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, accent `#2980b9`; grays `#555`/`#666`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links, no cross-references to other pages, no index number in the h1.
