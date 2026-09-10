# Sport Wearables: Cycling & Rowing Power

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section; picture-story canvases)
**HTML title tag:** Sport Wearables: Cycling & Rowing Power

**Subtitle:** A strain gauge in the crank or the oarlock measures force mechanically — the rare sports sensor where the number of interest is measured rather than inferred. The catch is where it measures, and what it doubles.

Sport hue: yellow `#c98500`. Measured annotations green `#008300`; derived annotations orange `#d95926`.

## What is it?

Lede: A strain gauge where the force enters the machine.

- **Fitted:** inside the crank arm, the pedal spindle, or the oarlock
- **Measures:** the flex of the metal under force, plus rotation rate
- **Derived:** power in watts — from two measured quantities, not a body model

Key-point callout: **Measured, but at one point:** many meters sit in one crank arm only, so the other leg is never measured — the reading is one leg, doubled.

### Visualization (canvas `c1`, 720×360)

Picture story: side-view bicycle with rider, the strain gauge marked on the crank arm, the pedal force arrow, and a rowing-oarlock inset.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "Cycling & rowing — the flex of the metal is the measurement".
- **Ground line:** 1px `#e5e9ef` at y=316, x 60→660.
- **Bicycle (side view, facing right):** wheels — circles r56 at (250,260) and (470,260), ink 2px stroke, hub dots r5 fill `#e5e9ef` with ink stroke; frame in ink 2px lines — bottom bracket (360,260) to seat-post top (325,148); (360,260)→(250,260) chainstay; (325,148)→(250,260) seat stay; (325,148)→(452,150) top tube; (452,155)→(360,260) down tube; (452,150)→(470,260) fork; saddle line (312,144)→(340,144) 3px; handlebar (452,150) short line to (466,142).
- **Rider (schematic, ink 2px):** hip at (330,150); torso (330,150)→(392,106); head circle (404,98) r11, fill `tint(ink,0.06)`; arm (388,110)→(456,146); leg (332,152)→(358,208)→(378,268) to the front pedal; second leg hinted (332,152)→(340,215)→(342,252) at lower alpha (0.35 ink stroke).
- **Crank:** small circle r5 at (360,260) ink stroke; crank arm 2.5px ink line (360,260)→(382,272); pedal short 2px line (376,274)→(390,274).
- **Sensor marker** — yellow: 6px dot at (372,267) on the crank arm; dashed yellow 1.5px leader (dash 3/3) to (560,220); bold 13.5px yellow label left-aligned (566,216) "strain gauge" / 11.5px mute "inside the crank arm".
- **Force arrow** — green: 2px arrow from the foot (384,280) straight down to (384,306) with arrowhead; bold 13px green label left-aligned (398,296) "force — measured" / 12px green "as metal flex".
- **Derived annotation** — orange: bold 13px left-aligned (66,110) "derived: power = torque × cadence" / 12px orange "both measured — almost nothing assumed".
- **Oarlock inset (top right, x 580–700):** oar drawn as a 3px ink line (596,74)→(692,138); oarlock U — two short ink strokes (640,104)→(640,120) and (656,100)→(656,116) with a base line between their bottoms; yellow 6px dot at (648,110); 11.5px mute label centered (645,158) two lines "rowing: same gauge," / "in the oarlock".
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px; 14px `#2c3e50` centered "The exception among sports sensors: the quantity the athlete cares about is measured — at one crank arm."

## What does it collect?

- **Per-revolution force/torque samples** and cadence
- **Derived:** power, total work, and a left/right balance figure
- **Keyed to a rider account**, often synced onward to training platforms
- **Precision:** vendor-quoted at around a percent or two of the reading — but a single-sided meter's doubling is a systematic bias that can exceed that margin for any rider whose legs differ

Key-point callout: **The balance figure can be fabricated:** on a single-sided meter "50/50" is not a finding — it is the arithmetic of copying one leg onto the other.

### Visualization (canvas `c2`, 720×320)

Two-panel picture: the true left/right split beside what a single-sided meter reports.

- **Header strip:** tinted band, 28px; bold 15px ink centered title "One leg, doubled — the single-sided shortcut".
- **Panels:** two boxes (30,58)–(340,252) and (380,58)–(690,252), fill `rgba(42,120,214,0.04)`, stroke `#e5e9ef`; panel titles bold 13px ink centered at y=76: "what the legs actually did" / "what a single-sided meter reports"; 11px mute "(illustrative)" under the left title.
- **Left panel bars:** baseline y=232, bar width 64; left-leg bar centered x=130, height 122 (47%), fill `rgba(42,120,214,0.35)`, stroke blue `#2a78d6`, bold 13px blue value "47%" above, 12px `#2c3e50` "left leg" below; right-leg bar centered x=240, height 138 (53%), violet `#4a3aa7` (0.35 fill), "53%" above, "right leg" below.
- **Right panel bars:** left-leg bar centered x=480, height 122, blue as before, "47% measured"; right bar centered x=590, height 122, fill `rgba(217,89,38,0.25)`, dashed orange `#d95926` stroke, bold 12px orange two lines above "47% —" / "copy of the left"; below the bars, bold 14px orange centered (535,286→ inside panel at y≈246? place at (535,246)): "reported balance: 50 / 50".
- **Footer band:** 34px; 14px `#2c3e50` centered "For any rider whose legs differ, the doubling error is systematic — and larger than the quoted precision."

### Sample payload (right column, under canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. Vendor sync schemas are not public;
// the measured/derived split is the point.
{
  "rider_id":  "u_5521…",
  "ride_id":   "rd_0817",
  "mount":     "left_crank",     // single-sided

  // ── measured by the strain gauge ──
  "torque_nm":   [18.2, 22.4, 20.1, 24.8, 21.5],
  "cadence_rpm": 88,
  "temp_c":      31.5,           // gauges drift with temperature
  "zero_offset": "at last calibration — rider-initiated",

  // ── derived ──
  "power_w":     212,            // left torque × cadence × 2
  "kj_total":    684,
  "lr_balance":  "50/50"         // single-sided: right = copied from left
}
```

## Why is it collected?

Label pill: STATED PURPOSE

- **Training by power zones** — power responds instantly where heart rate lags
- **Pacing and testing** on repeatable numbers

Label pill: ADDITIONAL CONSEQUENCE

- **A multi-year record of physical capacity** on a vendor account, often visible on social training platforms
- **Power files as evidence** — online race verification, and reading a rival's fitness from shared files, on platforms where files are public

Key-point callout: **Calibration is a ritual the data does not record:** a meter zeroed in a warm room can read differently on a cold morning, and the file carries no flag that the ritual was skipped.

### Visualization (canvas `c3`, 720×320)

Line chart as picture story: two meters on one bike, the same ride reported a few percent apart.

- **Header strip:** tinted band, 28px; bold 15px ink centered title "Two meters, one ride — a few percent apart".
- **Plot area:** x from 70 to 520, baseline y=240, top y=70; y gridlines `#e5e9ef` at 160/200/240 W mapped over that range (value range 140–260); 11px mute y-labels at the left; 12px mute x-label centered (295,262) "one ride, in order".
- **Data (illustrative, 24 points each):** crank meter (blue `#2a78d6`, 2px line + 3px dots) `[180,195,210,230,238,225,210,205,215,232,246,240,228,214,206,199,208,220,236,242,235,222,210,200]`; pedal meter (orange `#d95926`) = each crank value ×1.035 rounded, drawn the same way.
- **Right-hand key (x=540, left-aligned):** the two series averages are computed from the arrays at draw time and printed — bold 13px blue "crank meter: avg ≈ N W" at y=140, bold 13px orange "pedal meter: avg ≈ N W" at y=170; italic 11.5px mute at y=200, two lines: "a season's 'fitness gain'" / "can fit inside this gap".
- **Footer band:** 34px; 14px `#2c3e50` centered "Neither stream is flagged as wrong — the disagreement never appears as a field."

## Regeneration instructions

- **Layout:** tracking detail page — `<table class="obj-table">`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, `.lede`, bullets with bold lead terms (`li b` in `#1a5276`), `.key-point` callouts, `.lbl` label pills (last row); right `<td>` (55%, text-align center) holds the canvas, and for the "What does it collect?" row also the `.payload-note` caption plus `<pre class="payload">` block (left-aligned).
- **Page CSS:** identical to the sibling sport pages (see `02-soccer.html`): body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; td borders `1px solid #2980b9`, padding 16px; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`; `.lbl-purpose` `#eaf2fb`/`#1a5276`, `.lbl-effect` `#fdf0e6`/`#a8501c`; `.payload` monospace block with ink left border. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes (c1 720×360, c2/c3 720×320); shared `setupCanvas(id)` sizes the backing store to rendered width × `window.devicePixelRatio` and `ctx.scale`s to logical coordinates. Helpers: `tint(hex,a)`, `band()` header/footer strips, `arrowHead()`.
- **Palette:** `P = { blue: #2a78d6, green: #008300, magenta: #d55181, yellow: #c98500, aqua: #199e70, orange: #d95926, violet: #4a3aa7, ink: #1a5276, text: #2c3e50, mute: #6b7280, grid: #e5e9ef }`; sport hue yellow; red unused. All chart data hardcoded literal arrays labeled illustrative; c3 averages computed from the arrays at draw time so labels cannot drift.
- In regenerated HTML, any card links elsewhere use `.html` extensions (this page has none).
