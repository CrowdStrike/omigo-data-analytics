# Tracking Data: Connected Appliance Telemetry

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Connected Appliance Telemetry

**Subtitle:** A washing machine, a fridge, an air conditioner, a microwave, a ceiling fan, a light. Each holds a small, steady connection open to the maker's cloud. Most of what it sends is not a measurement — it is a statement that it is still there. Those statements are a time series, and the gaps in it are the hard part.

## Section 1: What is it?

One long-lived connection carrying very short messages.

- **Why not the web:** little memory, little spare power, an unreliable home network — so no fresh session per drum-speed change
- **MQTT** is a published open standard: publish a small payload to a named topic, the broker forwards it to subscribers
- **CoAP** is the request/response counterpart — shaped like HTTP but small enough for a light switch
- **Both are public specs**, so the envelope fields below are spec concepts, not anyone's internals
- **Keepalive:** say *something* inside a fixed window, or the broker treats the session as dead
- **Periodic status** goes out whether anything changed or not — an idle dishwasher still reports idle

**Key point (callout):** **The heartbeat alone is informative:** a fridge checking in every N seconds emits a presence-and-power series — up, up, up, then nothing. A set of appliances usually reveals a home's power and connectivity pattern before it reveals anything any of them was built to measure.

**Key point (callout):** **Even with no reading at all:** a smart plug reporting no wattage still says when the mains was live and the router reachable.

### Visualization (canvas `c1`, 720×320)

Timeline schematic: a keepalive tick series with gaps, and the presence state derived from it. Hue encodes provenance: blue = arrived as a message, violet = derived from the messages, orange = the gap, where nothing was observed.

- **Title (bold 16px `#1a5276`, top center):** "A heartbeat with no reading in it is still a time series".
- **Timeline span:** x from 156 to 700.
- **Gaps:** two orange bands over the tick lane at fractional spans [0.30, 0.44] and [0.66, 0.72] — fill `#d95926` at alpha 0.10, dashed orange border (dash 3/3).
- **Tick lane (y=78):** light gray baseline `#e5e9ef`; row label right-aligned in bold 14px blue `#2a78d6`: "Fridge keepalive". Regular vertical blue ticks (width 2, ±9px) every 0.0205 of the span starting at 0.012, skipped inside the gaps.
- **Derived lane (y=152):** row label bold 14px violet `#4a3aa7`: "Derived state". Five contiguous blocks (height 24): "up" spans [0.00–0.30], [0.44–0.66], [0.72–1.00] in violet tint alpha 0.20 with violet stroke; "silent" spans [0.30–0.44], [0.66–0.72] in orange tint alpha 0.14 with orange stroke.
- **Gap labels:** bold 14px orange "gap" centered under each gap band; below, 13px muted `#6b7280`: "mains, router, low battery, or the fridge was unplugged to clean behind it".
- **Footer band (y=214, height 74):** violet-tinted panel (alpha 0.08) with 4px violet left bar; three bold 13px legend rows with 8×8 color swatches:
  - blue: "blue ticks — arrived as a message"
  - violet: "violet blocks — derived from the ticks, not sent by the fridge"
  - orange: "orange bands — nothing arrived; the cause was not observed"
  - closing line 13px `#2c3e50`: "The power and connectivity pattern arrives before any measurement does."
- **Caption (13px `#6b7280`, bottom center):** "Schematic".

## Section 2: What does it collect?

- **Sensor samples** with a unit — watts, temperature, water inlet, fan speed, dimmer level
- **State and cycle stage** — idle, fill, heat, wash, rinse, spin, door open, defrost, compressor on
- **Report reason** — scheduled, or triggered by a change
- **Firmware version** and each update check's outcome
- **Connectivity** — signal strength, disconnect/reconnect counts, session resumes
- **Battery or supply voltage**, on anything not mains-powered
- **Appliance identifier** written at manufacture, plus a message counter
- **Hub or gateway** the message arrived through

**Key point (callout):** **The interval bounds every conclusion:** a fridge reporting once a minute cannot resolve a door opened and shut inside that minute, and no smoothing or model recovers it. A claim about short events from long-interval data is a claim about the interval.

**Key point (callout):** **Duty cycles make fixed intervals worse:** sampling a roughly periodic compressor on/off process at a fixed interval is the textbook setup for aliasing. If the sampling period lands near the duty-cycle period, the sampled fraction of "on" states sits far from the true one and stays there — the error looks like a stable reading, not noise.

**Key point (callout):** **So averaging wattage to estimate energy needs a condition:** a short interval relative to the cycle, or samples uniform in phase. A fixed interval is neither, by construction.

**Key point (callout):** **Change-reporting breaks even spacing:** lag operators, autocorrelation, differencing and most seasonal decompositions assume even spacing, so they are mis-specified unless the series is resampled onto a grid first.

**Key point (callout):** **Averaging unweighted overweights brief states:** a two-second drain burst and a forty-minute wash each contribute one row. The correct summary is duration-weighted, and on a wash cycle the difference is large.

### Visualization (canvas `c2`, 720×320)

Step-trace plus two sampling lanes: one wash cycle's power draw, sampled on a fixed interval vs reported on change. Hue encodes the cycle stage (six stages, six series hues); the blind fixed grid gets the one remaining hue (yellow) so it reads as a separate thing.

- **Title (bold 16px `#1a5276`, top center):** "One wash cycle: fixed-interval samples vs reports on change".
- **Chart area:** x from 158 to 700, trace between top=44 and base=126; power scale 0–2000 W.
- **Data (piecewise-constant wash cycle, [slots, watts], 60 slots total):** `[[12, 3], [4, 160], [9, 1900], [18, 120], [2, 480], [15, 130]]` with stage labels `['idle', 'fill', 'heat', 'wash', 'drain', 'rinse']` and stage hues `[#2a78d6, #008300, #4a3aa7, #d95926, #199e70, #d55181]`; fixed-grid hue yellow `#c98500`.
- **True trace:** each stage drawn as a horizontal step segment (width 2) in its own hue, with a riser to the next level in the incoming stage's hue. Left label 13px muted: "true draw (W)".
- **Fixed-interval samples:** yellow dots (radius 3.5) at every 8th slot (slots 0, 8, 16, 24, 32, 40, 48, 56), centered in the slot, at the trace value.
- **Drain-burst annotation:** dashed vertical line (dash 3/3) in the drain hue `#199e70` rising from the 480 W burst, with right-aligned bold 13px label "brief drain burst".
- **Lane A (y=154):** label bold 14px yellow "fixed interval"; gray baseline; yellow vertical ticks (width 2, ±8px) at the fixed-grid slots.
- **Lane B (y=186):** label bold 14px `#2c3e50` "reports on change"; gray baseline; one tick per stage start, each in its stage hue, with the stage name in bold 12px to the right of its tick.
- **Footer band (y=222, height 74, fill `#e5e9ef`):** six stage swatches (9×9) followed by bold 13px `#2c3e50`: "one row per stage, averaged unweighted: 466 W"; second line bold 13px `#1a5276`: "the same rows, weighted by how long each stage held: 356 W"; third row yellow swatch plus 13px: "the fixed grid (yellow) samples the drain burst not at all — it falls between two ticks". (Means computed from the literal arrays: unweighted (3+160+1900+120+480+130)/6 ≈ 466; duration-weighted 21316/60 ≈ 355.)
- **Caption (13px `#6b7280`, bottom center):** "Schematic".

### Example payload (below canvas `c2`, right column)

Visible caption above the block (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── documented concept: MQTT-style envelope ──
  "topic": "appliance/8f2c…/telemetry",
  "client_id": "8f2c…",          // written at manufacture
  "qos": 1,
  "keepalive_s": 60,

  // ── documented concept: standard appliance/sensor fields ──
  "ts": "2026-08-22T07:14:03Z",  // stamped on arrival, see below
  "kind": "washer",
  "report_reason": "change",      // "change" | "interval"
  "state": "spin",
  "power_w": 480,
  "drum_temp_c": null,            // read failed — this is not 0
  "rssi_dbm": -84,
  "fw": "2.4.1",
  "seq": 40817,                   // last seen was 40792: 24 absent

  // ── inferred / plausible: operator enrichment ──
  "gap_prior_s": 5412,            // silence before this message
  "gap_label": "offline",         // cause was not observed
  "hub_id": "h_2213",             // hub it arrived through
  "occupancy_guess": "away"       // derived from the gap, not measured
}
```

## Section 3: Why is it collected?

**Label (`.lbl-purpose`):** Stated purpose

- **Fleet reliability** — which firmware is on which unit, what fault it raised, whether it is powered and reachable
- **Remote control** — starting the AC from a phone requires the channel to stay open

**Label (`.lbl-effect`):** Additional consequence

- The same rows form a **long, regular, per-unit behavioural series** — only the query differs
- It hangs off an **identifier written at manufacture**, more durable than an account

**Key point (callout):** **The key outlives the household:** an account can be deleted or handed on, but a factory identifier usually survives resale and a factory reset — a reset clears configuration, not silicon. So a fridge sold secondhand arrives carrying the previous household's history, and a model keyed on the appliance attributes it to whoever lives there now. Nothing on this channel marks the handover.

### Visualization (canvas `c3`, 720×320)

Timeline schematic: the appliance identifier outlives the account it was registered to. Hue encodes which household is behind the identifier — the same household keeps its hue across a reset; a different one gets a different hue.

- **Title (bold 16px `#1a5276`, top center):** "The appliance identifier is durable; the household behind it is not".
- **Timeline span:** x from 158 to 700.
- **Account lane (y=54, blocks 28px tall):** label bold 14px `#2c3e50` "Account". Three outlined tinted blocks (tint alpha 0.16, stroke width 1.5, centered bold 13px name in hue):
  - [0.00–0.42] "household A" in green `#008300`
  - [0.46–0.62] "household A again" in green `#008300`
  - [0.66–1.00] "household B" in magenta `#d55181`
- **Transition events:** dashed orange (`#d95926`, dash 4/3, width 1.5) vertical lines dropping from above the account lane to y=158, at fractions 0.44 and 0.64, each with a bold 13px orange label above: "factory reset" and "sold secondhand". Neither emits an event on the telemetry channel.
- **Identifier lane (y=126, 30px tall, spanning full width):** label bold 14px blue "Washing machine id"; one continuous blue block (tint alpha 0.20, blue stroke) with centered bold 14px blue text: "8f2c… — written at manufacture, unchanged throughout".
- **Cycle-report lane (y=178):** gray baseline; label bold 14px `#2c3e50` "Cycle reports"; dense vertical ticks (width 2, ±7px) every 0.028 of the span starting at 0.015, each colored by the household block it falls inside (green then green then magenta; no ticks in the unowned transition gaps).
- **Footer band (y=208, height 82):** magenta-tinted panel (alpha 0.10) with 4px magenta left bar; legend rows with 9×9 swatches, bold 13px:
  - blue swatch: "the identifier (blue) is one continuous key from manufacture onward"
  - green + magenta swatches: "two different households sit behind it, in green and magenta"
  - orange swatch: "neither transition (orange) emits an event on this channel"
  - closing line 13px `#2c3e50`: "A wash history joined on the identifier is attributed to whoever owns it now."
- **Caption (13px `#6b7280`, bottom center):** "Schematic".

## Regeneration instructions

- **Layout:** tracking detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets, `.lbl` labels and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600; `.lede` 0.95em.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`.
- **Labels:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** intrinsic 720×320 each; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`, a `tint(hex, alpha)` helper for translucent fills, and a rounded-rect path helper. Red is reserved for alarm states and not used here. All chart data is hardcoded literal arrays (no Math.random); the c2 means are computed in-script from those arrays.
- **Project palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
