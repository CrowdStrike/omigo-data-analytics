# Sport Wearables: Swimming

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Sport Wearables: Swimming

**Subtitle:** Water blocks both radio and satellite signal, so one wrist unit dead-reckons the whole swim — stroke, count and splits are all inferences between wall touches.

## What is it?

Lede: One sealed wrist unit is the only sensor that goes in the water.

- **Worn:** a watch-style inertial unit on the wrist — water blocks both radio and satellite signal
- **Measures:** wrist acceleration and rotation, and the jolt of each wall touch
- **Derived:** stroke type (a classifier), stroke count, and lap splits

**Structural nulls under water:** there is no position fix between walls, so the swim is dead-reckoned from one wrist — and a soft wall touch silently merges two laps into one.

### Visualization (canvas `c1`, 720×360)

Annotated pool scene, side view: a swimmer mid-stroke under the waterline, the wrist unit hue-coded aqua on the recovery hand, a satellite whose signal dies at the water surface, and the pool wall as the only ground truth. Hue-coded sensor dots, dashed leader lines (dash 3/3), labels in the same hue.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)`, 28px tall; bold 15px ink `#1a5276` centered title "Swimming — no signal under water; one wrist reconstructs the whole lap".
- **Water:** rect (40,170)–(680,330), fill `rgba(25,158,112,0.10)` (aqua tint); waterline 2px `rgba(25,158,112,0.6)` at y=170; two small wave arcs on the line near x=200 and x=430.
- **Pool wall:** ink rect (640,150)–(664,330), fill 0.15-alpha ink tint, 1.5px ink stroke; green `#008300` 4px vertical stripe on its left face y=200→260 = touch pad; bold 12px green two-line label right-aligned ending at x=630 (y=128 and y=142): "wall touch —" / "the only ground truth".
- **Swimmer** (side view, swimming right, body under the line): head circle center (470,192) r11, 0.12-alpha ink fill + 2px stroke in 0.75-alpha ink; body 2.5px 0.75-alpha ink line (459,196)→(360,208); kick lines (360,208)→(330,222) and (360,208)→(336,200); recovery arm above water in full ink 2.5px: polyline (452,190)→(430,158)→(400,150) with hand circle r5 stroked at (398,152); underwater arm 0.65-alpha ink (452,196)→(430,225)→(408,232).
- **Stroke arc:** dashed aqua `#199e70` 1.5px arc above the swimmer, center (430,190) r46, from −160° to −20° — the recovery path.
- **Wrist unit** — aqua: 6px-radius dot on the recovery hand (396,150); dashed aqua leader (dash 3/3) to (300,96); bold 13.5px aqua right-aligned label at (294,92) "inertial unit on the wrist", 11.5px mute second line at (294,107) "the only sensor in the water".
- **Satellite** — blue `#2a78d6`: icon at (120,52) (body rect + two panels); dashed blue 1.5px arrow down from (120,66) ending at the waterline (140,168); bold 14px `#e74c3c` "✕" at the end point (the page's single red — a genuine dead-signal state); 12px mute label left-aligned at (60,90), two lines "satellite signal" / "stops at the water" (second line y=105).
- **Measured annotation** — green `#008300`: bold 13px left-aligned (60,240) "measured: wrist acceleration"; 12px green (60,256) "+ rotation, + wall-touch jolt".
- **Derived annotation** — orange `#d95926`: bold 13px left-aligned (60,286) "derived: stroke type (classifier),"; 12px orange (60,302) "stroke count, lap splits".
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall; 14px `#2c3e50` centered text "Between walls the watch is dead-reckoning. A missed touch does not error — it quietly merges two laps."

## What does it collect?

- **Wrist inertial series:** continuous acceleration and rotation — the raw material for everything else on the page
- **Heart rate:** optical, on some units, where the sensor keeps skin contact in the water
- **Derived per length:** stroke type, stroke count, lap count and splits, an efficiency score
- **Keyed to an account:** the swim syncs to a vendor account, not just the watch face
- **Precision:** per-sample noise is tiny, but dead-reckoning integrates it with no fix to reset against between walls — splits are only as precise as wall-touch detection

**Derived is most of the record:** only wrist motion is measured; stroke, count and splits are classifier output, presented with the same confidence as the clock.

### Visualization (canvas `c2`, 720×320)

The lap-merge story: a timeline of wall-touch jolt spikes for six lengths where one touch is too soft to clear the detection threshold. Top strip = the true six lengths; bottom strip = the recorded five laps, one spanning the missed wall. Hardcoded illustrative arrays.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "One soft wall touch — six lengths recorded as five laps".
- **Top strip** — aqua: 12px mute label left-aligned (60,50) "what happened — six lengths (schematic)"; six segments x=70→650 (step ≈96.7), y=58 h=20, fill 0.15-alpha aqua tint, 1px aqua stroke; 11px aqua numbers "1"–"6" centered at y=72.
- **Jolt spikes** — green (measured): baseline 1px `#e5e9ef` at y=204, x 60→660; 11px mute label left-aligned (60,110) "wall-touch jolt from the wrist accelerometer (schematic)"; six 3px green vertical spikes at the segment boundaries x = 70 + i·96.7 (i=1…6), heights `[56, 50, 60, 16, 54, 58]` rising from the baseline, 3px-radius green dot at each tip.
- **Threshold** — orange: dashed (4/4) 1.5px line at y=174, x 60→660; 12px orange label left-aligned at (368,167) "detection threshold".
- **Missed touch:** italic 11.5px mute label centered (457,124) "too soft — below threshold"; dashed mute leader (3/3) from (457,130) down to (457,182) pointing at the short 4th spike.
- **Bottom strip** — orange (derived): 12px mute label left-aligned (60,232) "what the record shows — five laps"; five segments y=240 h=20, fill 0.12-alpha orange tint, 1px orange stroke — laps 1–3 match lengths 1–3, lap 4 spans lengths 4+5 (x≈360→553), lap 5 = length 6; 11px orange labels centered at y=254: "1", "2", "3", "4 — two lengths, one lap", "5".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "The merged lap enters the account as one slow length; no later signal contradicts it."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. Field names are generic; no vendor
// schema is asserted.
{
  "swim_id":    "sw_0619",
  "account_id": "user_88x2",
  // ── measured by the wrist unit ──
  "imu":  { "accel_hz": 50, "gyro_hz": 50,
            "duration_s": 1122 },          // raw series, summarized
  "wall_touches": [
    { "t_s":  34.8, "jolt_g": 2.1 },
    { "t_s":  71.2, "jolt_g": 1.9 },
    { "t_s": 108.5, "jolt_g": 0.4 }        // soft touch — see chart
  ],
  // ── derived / classified ──
  "stroke":       "freestyle",             // classifier output
  "lengths":      38,
  "splits_s":     [34.8, 36.4, 73.9, 35.1],// one split spans two lengths
  "swolf":        41                       // vendor efficiency formula
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Lap counting and pace feedback** — mid-swim, the swimmer cannot count laps or read a clock themselves
- **Stroke-technique trends** across sessions

**Additional consequence** (label pill `.lbl-effect`)

- **A longitudinal training record** synced to a vendor account under a consumer agreement
- **Errors persist silently** — misclassified strokes and merged laps enter the historical record, and downstream trend charts render them with full confidence

**The record self-corrects nothing:** an error at capture time has no later signal to contradict it — unlike a GPS run, where the map betrays a bad trace.

### Visualization (canvas `c3`, 720×320)

Two trend lines over illustrative weeks: what the swimmer did vs what the account shows, drifting apart as small capture errors accumulate. Hardcoded illustrative arrays.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "Capture errors accumulate — and nothing later corrects them".
- **Plot area:** padL=70, plot right x=500 (key sits right of it), top y=56, baseline y=240.
- **Axes:** y from 1800 to 2600, gridlines `#e5e9ef` with 11px right-aligned labels at 1800 / 2200 / 2600; rotated 12px mute y-title "metres per week (illustrative)" at x=22; 11px mute x ticks "wk 1" / "wk 6" / "wk 12" at y=258 under weeks 1, 6, 12.
- **Data (illustrative, 12 weeks):** swimmer aqua `[2000, 2100, 2050, 2200, 2150, 2250, 2300, 2250, 2400, 2350, 2450, 2500]`; account orange `[2000, 2050, 2000, 2100, 2000, 2100, 2100, 2000, 2150, 2050, 2100, 2150]`.
- **Lines:** gap between the two lines filled 0.08-alpha orange tint first; then swimmer line solid aqua 2.5px, account line solid orange 2px.
- **Right-hand key** (x=512): bold 13px aqua "what the swimmer did" beside the aqua line's end; bold 13px orange "what the account shows" beside the orange line's end; italic 11.5px mute two lines below "gap = merged laps and misread" / "strokes, never revisited".
- **Caption:** italic 11px mute centered (360,274) "illustrative weeks — the shape, not measured training".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "A GPS run leaves a map that can betray a bad trace; a swim record has no second witness."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares its intrinsic size in `width`/`height` attributes (c1 720×360, c2 and c3 720×320); a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect, `arrowHead()` for the satellite arrow. Every canvas carries a tinted ink header band (28px, `rgba(26,82,118,0.07)`, bold 15px `#1a5276` centered title) and footer band (34px, `rgba(26,82,118,0.06)`, 14px `#2c3e50` centered text). Charts hardcode literal data arrays (no Math.random).
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Sport hue aqua; measured annotations green, derived annotations orange, satellite blue. Red `#e74c3c` appears exactly once on this page — the dead-signal ✕ where the satellite arrow meets the waterline (a genuine dead-signal state).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
