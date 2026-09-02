# Sport Wearables: Soccer

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Sport Wearables: Soccer

**Subtitle:** A GNSS pod between the shoulder blades logs a trail of fixes; everything a coach reads — distance, sprints, "load" — is arithmetic and thresholds on that trail.

**Sport hue:** blue `#2a78d6`. Measured annotations green `#008300`; derived annotations orange `#d95926`. Red is reserved for genuine alarm states and is unused on this page.

## What is it?

Lede: A satellite-positioning pod on the upper back, logging a trail of fixes across the session.

- **Worn:** a GNSS-plus-inertial pod in a vest pocket between the shoulder blades — GNSS is satellite positioning generally, of which GPS is one system; pods listen to several at once
- **Measures:** position fixes about ten times a second, plus triaxial acceleration and rotation
- **Derived:** distance, top speed, sprint count, direction changes and a composite "load"

**A sprint is a configuration:** it is any moment above a chosen speed cutoff, so retuning the cutoff changes a player's sprint count with no change in their running.

### Visualization (canvas `c1`, 720×360)

Top-down pitch with one hardcoded run trail whose fixes change hue where they cross a speed cutoff, plus a receive-only satellite and a back-view vest inset showing where the pod sits.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "Soccer — a trail of fixes, colored by a speed cutoff".
- **Pitch (top-down):** rect (50,70)–(540,320), fill `tint(green, 0.06)`, stroke 1.5px `tint(green, 0.45)`; halfway line at x=295 (y 70→320); center circle r36 at (295,195); penalty box rect (50,133)–(122,257). Same green stroke throughout.
- **Run trail** (hardcoded fixes, dots + 2px connecting line): [(90,290),(120,282),(150,270),(178,262),(205,250),(228,232),(248,210),(262,186),(272,160),(292,150),(318,146),(345,142),(372,136),(400,128),(428,120),(452,114),(470,110)]. First 9 points blue `#2a78d6` with r3 dots (cruising); the rest orange `#d95926` with r4 dots (above the cutoff); the connecting polyline is blue through point 9 and orange from there on.
- **Labels on the trail:** bold 12px orange centered at (382,108) "above the cutoff → 'sprint'"; 12px blue centered at (150,306) "below it → invisible in the count".
- **Satellite** — blue: icon at (600,60) — body rounded rect 20×12 centered plus two panel rects either side; 12px mute label centered (600,44) "positioning satellites"; dashed blue 1.5px **one-way** arrow (600,72)→(614,150), drawn before the vest inset so the head occludes it.
- **Vest inset** (right of pitch, back view): head circle (614,132) r9 ink stroke; shoulder line (588,144)→(640,144); torso trapezoid (590,144)(638,144)(632,196)(596,196), fill `tint(ink, 0.06)`, ink stroke; blue 5px pod dot at (614,158); 12px blue label centered (614,214)/(614,229), two lines "GNSS pod," / "vest pocket"; thin dashed blue 1px leader (dash 3/3) from the pod dot to the trail's last fix (470,110).
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall; 14px `#2c3e50` centered text, wrapped to two lines so it fits: "The vest only receives — nothing on the pitch measures the player." / "A 'sprint' is any fix above a cutoff someone configured."

## What does it collect?

- **Position fixes** typically 10 per second (10 Hz), each with an accuracy estimate
- **Triaxial acceleration** and rotation from the inertial unit, usually sampled around 100 times a second
- **Derived per session:** distance, top speed, sprint count, accel/decel and change-of-direction counts (banded by turn angle), a composite "load"
- **Keyed to a named player** and session — this is a person's record, not a device's
- **Precision:** fixes are typically good to a few meters in open sky and worse near stands — each fix carries the pod's own accuracy estimate, a field that rarely survives into the session report
- **Turn angles are derived** — heading comes from successive fixes plus the gyro, so slow, tight cuts are where the angle estimate is weakest, and the angle bands are vendor-chosen

**"Load" is a private formula:** the composite score is a vendor-weighted sum of the counts above, so two systems' load numbers are not the same quantity even on the same run.

### Visualization (canvas `c2`, 720×320)

The same hardcoded speed trace drawn twice, once per panel, with two different sprint cutoffs — same run, two different sprint counts. Both counts are computed at draw time from the literal array so the printed numbers always match the highlighted episodes.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "One run, two cutoffs — two different sprint counts".
- **Speed trace (illustrative, schematic units 0–10, 38 samples):** [2.0, 3.0, 5.0, 9.0, 9.5, 6.0, 3.0, 4.0, 6.0, 6.2, 4.0, 2.0, 5.0, 8.5, 8.8, 5.0, 3.0, 4.5, 6.2, 6.4, 4.0, 2.5, 5.0, 8.0, 8.2, 5.0, 3.0, 4.0, 6.4, 6.1, 3.5, 2.0, 4.0, 8.8, 9.0, 5.5, 3.0, 2.0].
- **Panels:** plot x 60→545 in both; panel A y 50→146 ("same run — cutoff set high", cutoff 7.0), panel B y 172→268 ("same run — cutoff set low", cutoff 5.5). Panel titles 12px `#2c3e50` left-aligned above each plot; baseline gridline `#e5e9ef` at each base; rotated 11px mute "speed" at the left of each panel; "time →" mute 11px under panel B.
- **Trace rendering:** blue 2px polyline for the whole trace; the region above the cutoff re-stroked orange 2.5px via a clip rect above the cutoff line.
- **Cutoff line:** dashed orange 1.5px (dash 5/4) across the plot at the cutoff height, 11px orange label "speed cutoff" above its left end.
- **Right key per panel** (x=560): bold 14px orange "4 sprints" / "7 sprints" (printed from the computed count), then two 11px mute lines "episodes above" / "the line".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "Same trace, two cutoffs, two sprint counts. Illustrative trace — schematic units."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. Field names are generic; vendor
// schemas and the load formula are not public.
{
  // ── measured / sampled by the pod ──
  "player_id":  "p_23",          // a named squad member
  "session_id": "s_0142",
  "fixes": [
    { "t": "…T10:04:00.0Z", "lat": 51.0000, "lon": -0.1000,
      "acc_m": 1.8, "accel_g": [0.2, -0.1, 1.0] },
    { "t": "…T10:04:00.5Z", "lat": 51.0001, "lon": -0.0999,
      "acc_m": 2.1, "accel_g": [0.9, 0.0, 1.1] }
  ],
  // ── derived downstream ──
  "distance_m":    9412,
  "top_speed_kph": 31.4,
  "sprint_count":  7,            // crossings of a chosen threshold
  "accel_events":  38,           // cutoff-defined, like sprints
  "cod_events":    21,           // direction changes, banded by turn angle
  "load_score":    412           // vendor weighted sum, formula private
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Training-load management** — pace a squad's week with everyone on the same units and settings
- **Return from injury** — ramp a recovering player back against their own baseline

**Additional consequence** (label pill `.lbl-effect`)

- **A performance record of a named employee** — the same sessions feed selection and contract judgements
- **Cutoffs are tuned per squad** — "sprints per match" from two clubs still renders as one chart, but the counts are not comparable

**The chart hides the config:** a derived number carries its thresholds invisibly, so a cross-club comparison looks precise while comparing different definitions.

### Visualization (canvas `c3`, 720×320)

Picture story: one session record feeding two readers — a coach's short-lived session view and a selection/contract view that accumulates for years.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "One trail, two readers".
- **Session record card** — blue: rounded rect (55,105) 160×115 r8, fill `tint(blue, 0.06)`, blue 1.5px stroke; inside, a mini trail polyline with r2.5 dots [(75,195),(95,185),(115,170),(130,150),(150,140),(175,132),(195,128)] in blue 2px; below the card, 12px blue centered (135,236) "one session's fixes", 11px mute centered (135,251) "keyed to a named player".
- **Coach box** — aqua `#199e70`: rounded rect (420,52) 240×92 r8, fill `tint(aqua, 0.07)`, aqua stroke; bold 13px aqua title (440,74) "Coach's session view"; 11px `#2c3e50` (440,92) "this week, this squad"; four aqua bars (illustrative weekly load) base y=136, x 445/473/501/529, width 18, heights [20,28,16,32]; below the box, 11px mute centered (540,158) "decision: adjust tomorrow's session".
- **Front-office box** — violet `#4a3aa7`: rounded rect (420,190) 240×92 r8, fill `tint(violet, 0.06)`, violet stroke; bold 13px violet title (440,212) "Selection & contract view"; 11px `#2c3e50` (440,230) "seasons of sessions, one player"; violet 2px trend line with r2.5 dots [(440,262),(475,256),(510,250),(545,244),(580,240),(615,232)].
- **Arrows** (solid 2px, arrowhead at the box end): aqua from (215,150) to (415,100), 11px aqua label centered (310,112) "today"; violet from (215,175) to (415,232), 11px violet label centered (315,196) "kept for seasons".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "Same numbers, two decisions: one expires tomorrow, one follows a named employee for years."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276` (no index number); subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links, no links to other pages.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares its intrinsic size in `width`/`height` attributes (c1 720×360, c2 and c3 720×320); a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect, `arrowHead()` for arrow tips. Every canvas carries the tinted ink header band (28px, bold 15px ink centered title) and footer band (34px, 14px `#2c3e50` centered text). Charts hardcode literal data arrays (no Math.random); c2's two sprint counts are computed at draw time from the hardcoded speed array.
- **Palette (declared once as tokens):** blue `#2a78d6` (sport hue), green `#008300` (measured), magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926` (derived), violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and is not used on this page.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
