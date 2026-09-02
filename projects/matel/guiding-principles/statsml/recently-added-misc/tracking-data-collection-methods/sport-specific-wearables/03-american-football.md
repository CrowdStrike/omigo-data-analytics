# Sport Wearables: American Football

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows: What is it? / What does it collect? / Why is it collected?)
**HTML title tag:** Sport Wearables: American Football

**Subtitle:** The shoulder-pad tag is a beacon, not a recorder — the measurement happens in the stadium's antennas, and so does the custody of the data.

## What is it?

Lede: A radio tag inside each shoulder pad, pinging many times a second.

- **Worn:** a radio tag inside each shoulder pad
- **Measures:** nothing — the tag transmits a ping many times a second
- **Derived:** position, speed and route shape, computed from ping arrival times at antennas around the stadium

**The beacon inversion:** unlike a GNSS vest, this wearable holds no data. The record is born in the stadium's infrastructure, keyed to a roster number, and held by whoever runs the antennas.

### Visualization (canvas `c1`, 720×360)

Annotated overhead scene: a green playing field, one player at midfield carrying two orange shoulder-pad tags, and four yellow antenna masts at the corners with dashed sight-lines converging on the tags. Hue code: orange = the tag, yellow = antenna infrastructure, green = the field, mute = derived quantities.

- **Header strip:** tinted ink band `rgba(26,82,118,0.07)` full width, 28px tall; bold 15px ink `#1a5276` centered title "American football — the wearable is a beacon; the stadium does the measuring".
- **Field:** rect (110,80)–(610,300), fill `rgba(0,131,0,0.06)`, stroke 1.5px `rgba(0,131,0,0.45)`; vertical yard lines 1px `rgba(0,131,0,0.35)` every 50px from x=160 to x=560, y 80→300.
- **Player:** ink circle r10 at (350,190) (helmet, fill 0.15-alpha ink tint, ink stroke); two orange `#d95926` 5px dots at (342,184) and (358,184) — one tag per shoulder pad; two static orange ping arcs (r16 and r24, 1.5px, 0.5 alpha) centered (350,184).
- **Tag label** orange, bold 13px centered (350,140): "two radio tags — transmit only"; 11.5px mute `#6b7280` sub-line centered (350,156): "many pings a second".
- **Four antennas** yellow `#c98500`: small masts (vertical 2.5px stem 12px tall below the tip, v-fork at the top, 4px yellow dot at the tip) with tips at (130,66), (590,66), (130,314), (590,314); dashed yellow 1.5px lines (dash 3/3, 0.6 alpha) from each tip converging on (350,186).
- **Antenna label** yellow, bold 13px right-aligned (560,50): "antennas timestamp each ping"; 12px yellow right-aligned (560,66): "position = solved from arrival times". (Moved from the seed's bottom-right position — two lines did not fit between the field edge and the footer band.)
- **Derived annotation** 12px mute centered (350,244): "speed, separation, route shape — all computed downstream".
- **Footer band:** tinted ink band `rgba(26,82,118,0.06)`, 34px tall at the bottom; 14px `#2c3e50` centered text "The player carries a transmitter, not a recorder. The data is born — and stays — in the stadium's hands."

## What does it collect?

- **Tag identifier and ping timestamps** at each antenna — the only things actually measured
- **Solved positions** many times a second, one x/y on the field per tag
- **Derived:** speed, acceleration, separation from other tagged players — and the ball, where tagged
- **Roster identity** joined on afterward — the tag broadcasts an ID, not a name
- **Precision:** radio ranging is vendor-quoted at sub-meter — tighter than GNSS — but it locates the tag, and a player's body extends farther than the quoted error

**Position is a solution, not a reading:** no antenna measures where the player is. Each antenna measures when a ping arrived; position is computed from the tiny differences between those arrival times.

### Visualization (canvas `c2`, 720×320)

Time-difference-of-arrival picture: one ping leaving the tag reaches three antennas at three slightly different times. Dashed orange wavefront arcs of different radii show the ping the moment it reaches each antenna; the arrivals land as yellow ticks on a shared time ruler; the tag position carries a green ring — the geometric solution.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "One ping, three arrival times — position is a solution, not a reading".
- **Tag** — orange: 6px dot at (250,150); bold 12.5px orange label centered (250,120) "one ping from the tag".
- **Solved position** — green `#008300`: 11px-radius green ring (1.5px stroke) around the tag dot; green 12px label centered, two lines (250,185) "solved position — the one spot" / (250,200) "consistent with all three times".
- **Three antennas** — yellow masts (same style as c1: tip dot, 12px stem, v-fork) with tips at A1 (120,75), A2 (415,60), A3 (330,225); bold 12px yellow tick names beside each: "t₁" at (138,80), "t₂" at (433,66), "t₃" at (348,230).
- **Wavefront arcs** — dashed orange (dash 3/3, 1.5px, 0.55 alpha), centered on the tag (250,150), one per antenna, radius = tag→antenna distance, spanning a short arc aimed at that antenna: A1 radius ≈150 over angles −2.92→−2.32 rad; A2 radius ≈188 over −0.70→−0.30 rad; A3 radius ≈110 over 0.45→1.05 rad.
- **Time ruler** (bottom right): 11.5px mute centered (570,212) "one ping, three arrival times"; italic 10.5px mute centered (570,227) "(differences exaggerated — schematic)"; 1px mute line (450,245)→(690,245) with 4px end ticks; yellow 2px arrival ticks y 238→252 at x=479 (t₃), x=560 (t₁), x=636 (t₂) — tick order matches antenna distance order; bold 12px yellow labels "t₃" "t₁" "t₂" centered under each tick at y=266.
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "No antenna measures position. Each measures a time; the position is computed from the differences."

### Payload (below canvas `c2`)

Caption (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
// Reconstruction. Field names are generic; the league
// player-tracking schema is not public.
{
  // ── measured by the antennas ──
  "tag_id": "TAG-3F82",
  "arrivals": [
    { "antenna": "NE-04", "t_arrival": "…19.184203710" },
    { "antenna": "SW-11", "t_arrival": "…19.184203892" },
    { "antenna": "NW-02", "t_arrival": "…19.184203845" }
  ],

  // ── solved / derived downstream ──
  "x_yd": 34.6, "y_yd": 21.2,     // position on the field
  "speed_yd_s": 7.1,
  "route": { "depth_yd": 12, "break_angle_deg": 87 },
  "player": { "roster_no": 11, "name": "…" }
              // joined from a roster table, not sensed
}
```

## Why is it collected?

**Stated purpose** (label pill `.lbl-purpose`)

- **Broadcast graphics and game analytics** — live speed and separation figures on screen
- **Officiating support** on some plays, in some leagues

**Additional consequence** (label pill `.lbl-effect`)

- **A per-play movement record of a named employee**, held by the league and clubs rather than the player
- **It feeds scouting, contract and roster decisions** — and outlives the player's career

**No opt-out from the instrument of evaluation:** wearing the tag is typically league-mandated — a condition of playing — so the player cannot decline the data that helps price their next contract.

### Visualization (canvas `c3`, 720×320)

Custody flow picture: the record moves one way, from the tag on the player's shoulder through the stadium antennas to the league backend and on to clubs and broadcast. The player figure at the far left holds nothing; every node is labeled with who holds what.

- **Header strip:** tinted ink band, 28px; bold 15px ink centered title "Custody flow — the record moves away from the player, one way".
- **Player** (far left) — ink stick figure: head circle r9 at (72,120), body (72,129)→(72,178), arms (72,140)→(56,160) and (72,140)→(88,160), legs (72,178)→(58,205) and (72,178)→(86,205); two orange 4px shoulder dots at (66,136) and (78,136) with one orange ping arc r14 (0.5 alpha) centered (72,136); mute 11.5px label centered, two lines (72,224) "the player" / (72,239) "holds nothing".
- **Node A — stadium antennas** (yellow): rounded rect (150,128) 128×46, 1.5px yellow stroke, 0.07-alpha yellow fill; bold 12.5px yellow centered (214,148) "stadium antennas"; 11px text centered (214,164) "raw pings + timestamps"; 11px yellow centered (214,190) "held by the tracking operator".
- **Node B — league backend** (ink): rounded rect (330,128) 130×46, ink stroke, 0.06-alpha ink fill; bold 12.5px ink centered (395,148) "league backend"; 11px text centered (395,164) "positions + identities"; 11px ink centered (395,190) "holds the full movement record".
- **Node C — clubs** (blue `#2a78d6`): rounded rect (560,62) 120×42, blue stroke, 0.07-alpha blue fill; bold 12.5px blue centered (620,80) "clubs"; 11px text centered (620,96) "scouting, contracts".
- **Node D — broadcast** (aqua `#199e70`): rounded rect (560,196) 120×42, aqua stroke, 0.07-alpha aqua fill; bold 12.5px aqua centered (620,214) "broadcast"; 11px text centered (620,230) "on-air graphics".
- **Arrows, all one-way** (arrowhead at the destination end only): dashed orange 1.5px (dash 3/3) from the shoulder (86,136) to node A (150,146), 11px orange label centered (114,118) "pings"; solid yellow 2px from (278,151) to (330,151); solid blue 2px from (460,143) to (560,86); solid aqua 2px from (460,160) to (560,214).
- **Note:** italic 11px mute centered (395,262) "no arrow returns to the player".
- **Footer band:** tinted ink band, 34px; 14px `#2c3e50` centered text "One-way flow. The record outlives the play, the season and the career — none of it in the player's custody."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width bordered table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts + `.lbl` pills, right `<td>` (55%, centered) holds the canvas; the "What does it collect?" row also carries the `.payload-note` caption and `.payload` pre block under its canvas (both left-aligned).
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li b` `#1a5276` weight 600, li 0.93em. No nav bar, no back/home links, no links to other pages.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** each declares its intrinsic size in `width`/`height` attributes (c1 720×360, c2 and c3 720×320); a shared `setupCanvas(id)` reads the element's attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills from palette tokens, `rr()` rounded-rect, `arrowHead()` for one-way arrows, `mast()` for the small yellow antenna icon shared by c1 and c2. Every canvas carries the tinted ink header band (28px) and footer band (34px). Charts hardcode literal coordinate arrays (no Math.random); the ruler tick positions in c2 are hardcoded to match the drawn antenna distances.
- **Hue roles on this page:** orange `#d95926` = the tag (sport hue); yellow `#c98500` = antennas / infrastructure; green `#008300` = the field and measured-solution annotations; blue `#2a78d6` and aqua `#199e70` = downstream recipients in c3; ink `#1a5276` headings, bands and figure outlines only. Red is reserved for genuine alarm states and is not used on this page.
- **Naming rule:** the league's tracking product and the tag vendor are never named — generic terms only ("league player-tracking system", "radio tags"). No unsourced specifics: ping rate is "many times a second", officiating support is hedged "on some plays, in some leagues", the mandate is "typically league-mandated". The c2 ruler is labeled "(differences exaggerated — schematic)".
- **Palette (declared once as tokens):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
