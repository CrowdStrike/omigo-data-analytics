# Bluetooth Fitness Machines

**Page type:** detail page (two-column obj-table layout: text left 45%, payload + canvas right 55%, single Overview row, followed by an API-references list)
**HTML title tag:** Bluetooth Fitness Machines — Platform APIs

**Subtitle:** Lets any app read live speed, cadence, power and heart rate straight off exercise equipment over Bluetooth — no vendor account or cloud involved.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get** (section label, left column)

- Live speed, cadence, power, resistance, distance and heart rate while connected
- Control of the machine: set target resistance or power (the mechanism behind ERG mode)
- Works with any brand that implements the open Bluetooth standard
- No sign-up, no API keys, no rate limits, no terms of service — the bytes go from the machine straight into your app

**Key-point callout (red left border):**

**Nothing is stored, anywhere.** There is no vendor cloud, so there is also no history, no account and no backfill. If your app was not connected and recording at the moment the exercise happened, that data never existed and can never be recovered. Capture is entirely your responsibility.

**Watch out for** (section label, left column)

- Only one app can connect at a time — if the vendor's own app is talking to the machine, yours is locked out
- Packets carry no timestamps and can be silently lost; your receiving clock is the timestamp
- "Power" may be truly measured or just estimated from speed and resistance — same field, different instruments, no way to tell from the data
- Packet layout shifts with a flags byte, so a parser tested on one machine can misread another and still produce plausible-looking numbers

**Payload note (right column, inline-styled 0.85em `#555`):** **One 11-byte packet from an indoor bike, decoded.** The flags decide which fields are present — and therefore where every byte lands.

Code block (`pre`), verbatim:

```
raw (11 bytes):  44 0A A8 0C B0 00 D6 00 8E F4 04

flags = 0x0A44 -> which fields follow,
                  and at which byte offsets

A8 0C -> 3240 / 100 = 32.40 km/h
B0 00 ->  176 /   2 = 88 rpm
D6 00 ->  214       = 214 W
8E    ->  142       = 142 bpm
F4 04 -> 1268       = 1268 s elapsed
```

**Chart caption (above canvas):** **The same minute of "power", delivered four ways.** The two local Bluetooth streams exist only if someone was connected and recording at the time.

### Visualization (canvas `samplingLanesChart`, responsive width × 380)

Four horizontal event-lane strips over a 60-second time axis: each lane shows arrival times (tick marks) of "power" values from one delivery mechanism; the bottom lane is a single solid bar (one summary value).

- **Lanes (top to bottom), each with a bold label, a smaller sub-label, and a vendor-in-path note:**
  1. "BLE Cycling Power notify" / "(~1-4 Hz, device-decided, lossy)" — ticks in `#1a5276`; note "no vendor in path" in `#27ae60`. Tick times: deterministic irregular sequence starting t=0.35 s with period `0.30 + 0.22*(1+sin(0.9i))/2 + 0.10*((i mod 7)/7)` s, and two simulated dropout windows where ticks skip from 21.5→23.4 s and 44.0→45.2 s.
  2. "FTMS Indoor Bike Data" / "(~1 Hz notify)" — ticks in `#27ae60`; note "no vendor in path" in `#27ae60`. Tick times: `0.6 + i*1.0 + 0.06*sin(1.3i)` s for i = 0..59 (< 60 s).
  3. "Vendor cloud API" / "(1-second samples, post-hoc after upload)" — ticks in `#e67e22`; note "vendor server in path" in `#e74c3c`. Tick times: `0.5 + i*1.0` s, perfectly regular.
  4. "Daily summary" / "(one average value for the whole ride)" — a solid `#8e44ad` bar spanning the full lane with centered bold white 10px text "one value for the entire session"; note "vendor server in path" in `#e74c3c`.
- **Layout:** height 380; margins left 236 (room for lane labels), right 20, top 60, bottom 78. x maps 0–60 s; each lane strip is 30px tall on a `#f8f9fa` background with a `#e8e8e8` separator line below.
- **Title (top left):** bold 13px `#1a5276` "Arrival times of \"power\" for the same 60 seconds of riding"; 11px `#555` sub-line "Each mark is one value delivered to the consumer. Same effort, four sampling processes."
- **Gridlines:** `#e8e8e8` vertical every 10 s across the lane area.
- **Lane labels (right-aligned to the left of the plot):** bold 11.5px `#2c3e50` main label; 10.5px `#555` sub-label; 9.5px vendor note colored `#e74c3c` ("vendor server in path") or `#27ae60` ("no vendor in path").
- **Tick marks:** vertical line segments, width 1.8, in the lane's color, spanning the strip height minus 4px at each end.
- **Dropout annotation (lane 1):** dashed (`[3,3]`) red `#e74c3c` vertical boundary lines at t=21.5 and t=23.4; italic 9.5px red label to the right: "lost notifications — no gap marker in the data".
- **Cloud annotation (lane 3):** italic 9.5px orange `#e67e22` label above the strip: "regular by construction: resampled server-side after the ride".
- **X axis:** `#2c3e50` line width 1.2 at the bottom of the lanes; ticks every 10 s labeled "0 s" … "60 s" in `#555` 11px; `#888` centered axis title "elapsed time within one 60-second effort".
- **Caption (bottom, inside canvas, italic 10.5px):** first line in red `#e74c3c`: "All four series are described as “power”. The bottom two require a vendor server, an account and an upload;" second line in `#555`: "the top two exist locally and only if a client was connected and recording at the time."
- Redraws on window resize.

## Official API References

- [Bluetooth SIG Specifications](https://www.bluetooth.com/specifications/specs/) — published GATT service specs including Fitness Machine (FTMS), Cycling Power, CSC and Heart Rate
- [Bluetooth Assigned Numbers](https://www.bluetooth.com/specifications/assigned-numbers/) — the 16-bit service and characteristic UUID registry (0x1826, 0x2AD2, etc.)

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" with a single-row `.obj-table` (left `<td>` 45%: section labels + bullet lists + one `.key-point` callout; right `<td>` 55%: payload note + `<pre>` decoded packet + chart note + `<canvas>`), then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline badge — background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.section-label` bold `#1a5276` block. Payload/chart notes are inline-styled 0.85em `#555` paragraphs. `li`/`p` 0.93em; links `#1a5276`; `code` background `#f4f4f4`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="samplingLanesChart" height="380">`, CSS `display:block; width:100%`; drawing code reads `getBoundingClientRect().width`, sets backing store to `rect.width * dpr` / `380 * dpr` using `window.devicePixelRatio`, fixes CSS height to 380px, `ctx.scale` back to logical coordinates, and re-renders on `resize`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad` (summary bar); strip background `#f8f9fa`; grid `#e8e8e8`; text `#555`/`#2c3e50`/`#888`.
- In regenerated HTML, any card/page links use `.html` extensions.
