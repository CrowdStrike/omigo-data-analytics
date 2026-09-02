# Tracking Data: WiFi Probe Requests

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: WiFi Probe Requests

**Subtitle:** A phone with WiFi on transmits scan frames whether or not it connects — and any receiver in range can log them.

## Section 1: What is it?

Lede: Short frames a phone sends to ask whether any access point is in range.

- **When:** WiFi on but not connected — the phone probes periodically
- **Wildcard probes** leave the network-name field empty; most modern phones send these
- **Directed probes** name specific saved networks one by one — older behaviour, still seen
- **Unencrypted and unaddressed**, so any receiver in range can log the frame without participating

Key point callout: **Connecting is not required to be counted:** having the radio on and scanning is enough, which is why footfall deployments use this signal rather than the guest network.

### Visualization (canvas `c1`, 720×320)

Radial broadcast diagram: a phone at center emitting probe frames outward, with directed-probe network names and one wildcard placed around it. Page-wide hue scheme: blue = what a receiver observes on the air, green = the device / ground truth, violet = receiving infrastructure and the wildcard (no-name) case, orange = what the sensor derives.

- **Legend (top left, bold 13px):** blue `#2a78d6` swatch at (150, 22) — "directed probe — names one saved network"; violet `#4a3aa7` swatch at (150, 42) — "wildcard probe — the name field is left empty".
- **Phone (center, at (w/2, h/2+12)):** green `#008300` rounded-rect outline 36×70 (radius 5) with a `#e5e9ef` inner screen 28×50 and a green three-arc WiFi icon.
- **Broadcast waves:** three full blue circles at radii 50, 75, 100, stroke width 1.5, 0.3 alpha.
- **Probe labels (13px monospace, positioned on an ellipse at radius 130×85 around the phone by angle):** blue directed names `"home-network"` (angle −0.8), `"employer-guest"` (−0.2), `"hotel-lobby"` (0.4), `"cafe-chain"` (1.0), `"airport-free"` (1.8); violet wildcard `(empty — wildcard)` (2.5).
- **Caption (gray `#6b7280` 13px, centered at (cx, h−10)):** "Most current phones send the wildcard form; either way the frame is readable by any receiver in range".

## Section 2: What does it collect?

- **Source MAC address** — usually randomised, not the hardware ID
- **Network name** requested, when the probe is a directed one
- **Signal strength**, from which a range is estimated
- **Timestamp** of each probe
- **Capability fields**, which hint at device class
- **Probe frequency** and how long probes keep arriving

Key point callout: **`locally_administered` decides whether the dataset means anything:** when set, the address is randomised — not a device identifier, and it changes while the same phone is still in the room. Counting distinct `src_addr` values overcounts devices, and overcounts most for the longest visits, so the bias tracks dwell time and cannot be divided out.

Key point callout: **`visitor_key` is a guess:** the sensor clustering on capability fields to undo randomisation. The record admits it by leaving `oui_vendor` null.

Key point callout: **Where names appear, the set beats any one name:** a home network plus an employer's plus a hotel's narrows a device far more than a common coffee-shop SSID. But note the direction — that distinguishes a device within the observed population, which is not identifying anyone. With wildcard probes there is no name at all.

### Visualization (canvas `c2`, 720×320)

Comparison of two devices' saved-network sets — distinguishing within the observed set, not identifying anyone. Names are placeholders.

- **Device A (blue `#2a78d6`):** filled circle r=15 at (150, 50), bold 13px label "Device A" at (150, 80); four 140×18 chips at x=80 starting y=92 (24px steps), fill `#f8f9fa`, blue stroke, 12px monospace blue text: `home-a`, `employer-a`, `airport-a`, `hotel-a`.
- **Device B (green `#008300`):** filled circle r=15 at (400, 50), bold 13px label "Device B" at (400, 80); matching chips at x=330 in green: `home-b`, `employer-b`, `airport-b`, `hotel-b`.
- **Difference marker:** bold 24px orange `#d95926` "≠" at (275, 130) — the inference drawn from the two sets, so the derived hue.
- **Wildcard note (violet `#4a3aa7`, swatch at (484, 91), 13px text left-aligned from (502, 100)):** "With wildcard probes there is" / "no name to compare, so this" / "comparison is unavailable".
- **Notes (13px, centered at bottom):** orange "The combination distinguishes; any single common name does not" at (w/2, h−30); gray "Distinguishing a device within the observed set is not identifying a person" at (w/2, h−12).

Payload note (right column, under the canvas): *Sample payload — illustrative structure, not real captured data.*

Payload block (monospace `.payload`):

```
{
  // ── present in the raw 802.11 frame ──
  "frame_type":   "probe_request",
  "src_addr":     "5e:8a:41:…",
  "ssid":         "",            // wildcard probe, no name given
  "ie_supported_rates": [1, 2, 5.5, 11, 6, 9, 12, 18],
  "ie_ht_capabilities": "0x016e",
  "channel":      6,
  "rssi_dbm":     -68,           // radiotap header
  "ts":           "2026-08-22T13:04:51.207Z",

  // ── inferred / plausible, added by the sensor ──
  "locally_administered": true,  // bit 1 of octet 0 is set
  "oui_vendor":   null,          // no vendor: address is randomised
  "est_range_m":  7.5,           // RSSI + path-loss assumption
  "rotation_suspected": true,
  "visitor_key":  "vk_c19d…"     // clustered, not observed
}
```

## Section 3: Why is it collected?

Label (`.lbl-purpose`): STATED PURPOSE

- **Counting** — footfall, dwell and path, which a venue cannot get from till receipts
- **Passive** — an estimate without a turnstile or asking anyone to do anything

Label (`.lbl-effect`): ADDITIONAL CONSEQUENCE

- **A count needs distinctness, and distinctness is the same operation as recognition** — a sensor that can decide two observations are one device can decide two visits are
- A **captive-portal login** supplies an identifier the radio layer does not

Key point callout: **The unit is a radio interface, not a person:** a visitor with a phone, watch and earbuds contributes several; a visitor with WiFi off contributes none. Both errors are systematic, so the measured population is not the walking one — and staff standing near a sensor all day are its longest-dwell observations.

### Visualization (canvas `c3`, 720×320)

Floor-plan schematic of what a probe deployment actually has (discrete sightings at fixed receivers) versus what it reports (a path and a dwell time), with a footer bar row of the same dwell values. Blue = observed, orange = derived, violet = receiving infrastructure, grey = the venue (not data).

- **Legend strip (top, bold 13px):** blue `#2a78d6` swatch at (50, 10) — "observed: a probe reached a receiver"; orange `#d95926` swatch at (330, 10) — "derived: the path and the dwell between sightings".
- **Floor plan:** grid-gray `#e5e9ef` outer rectangle at (50, 34) 620×150; six sections filled `#f8f9fa` with grid-gray borders and gray `#6b7280` 12px centered labels: Electronics (60, 42, 140×66), Clothing (60, 112, 140×64), Grocery (220, 42, 140×134), Home (380, 42, 140×66), Sports (380, 112, 140×64), Checkout (540, 42, 120×134).
- **Receivers:** violet `#4a3aa7` filled triangles (12px wide) at (50, 34), (360, 34), (670, 34), (50, 184), (360, 184), (670, 184).
- **Sightings and path:** dashed (5/3) orange polyline width 2.5 through nine points `(60,120), (130,75), (200,90), (290,110), (350,100), (420,75), (500,130), (580,100), (620,90)` — the line is interpolation, not observation. At each point: a blue filled dot r=3 (observed) inside an orange ring of radius dwell+2 where dwell values are `[3, 8, 3, 12, 3, 6, 4, 3, 3]` (derived).
- **Footer band:** the same dwell values as a bar row — baseline at y=268 in grid gray from x=52; nine bars 26px wide at 62px steps from x=60, height (d/12)×46, orange at 0.55 alpha, each sitting on a 3px blue tick, numbered 1–9 in gray 11px below.
- **Footer labels:** bold 13px orange "derived dwell per sighting, in order" at (60, 210); 13px blue "each blue tick is one sighting — the height above it is interpolated between sightings" at (60, 226); right-aligned 13px violet "▲ = receiver" at (670, 210); right-aligned gray "Schematic — illustrative dwell units" at (670, 226).

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title`, optional `.lede`, bullets, and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li b` `#1a5276` weight 600; list items 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Labels:** `.lbl` uppercase pill 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em italic `#666` immediately above.
- **Canvas:** intrinsic size 720×320 per chart; `setupCanvas(id)` reads the element's own `width`/`height` attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helper: `rr()` rounded-rect.
- **Hue scheme (held constant across all canvases on this page):** blue = what a receiver actually observes on the air; green = the device, and the ground truth about it; violet = the receiving infrastructure, and the wildcard (no-name) case; orange = what the sensor derives on top of the observations. Where a canvas contrasts plain categories instead (C2's two devices), those take SERIES order.
- **Palette (tracking-page chart tokens, declared once as `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation — reserved for genuine alarm states. Project-level palette anchors: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
