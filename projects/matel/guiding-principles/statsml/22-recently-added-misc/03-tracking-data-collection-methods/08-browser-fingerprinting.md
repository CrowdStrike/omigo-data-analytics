# Tracking Data: Browser Fingerprinting

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Browser Fingerprinting

**Subtitle:** A script reads configuration values the browser exposes to any page — screen, timezone, fonts, rendering behaviour — and hashes them into one identifier. Nothing is stored on the device.

## What is it?

Configuration values, concatenated and hashed into one value.

- **Declared attributes:** *screen.width*, *navigator.language*, timezone offset, logical CPU cores, which fonts resolve
- **Exposed by design** so pages can adapt themselves
- **A few dozen read**, then hashed together
- **Behavioural inputs:** canvas drawing and audio buffer readback differ by GPU, driver and font rasteriser
- **So machines reporting identical specs** can still hash differently

**An entropy argument, not an identification one:** no single attribute names anyone — millions share a timezone and a screen size.

**Sparsity in the joint distribution:** across thirty-odd attributes most observed combinations occur once or a handful of times in the population being measured.

### Visualization (canvas `c1`, 720×320)

Funnel/flow diagram: many input signals converging through a hash into one fingerprint.

- **Title (bold 16px, centered, blue `#2a78d6`, y=20):** "Many small details → one unique fingerprint".
- **Input pills (left):** six rounded rects (170×22, radius 4) at x=30, starting y=45, spaced 30px, fill `rgba(42,120,214,0.35)`, blue 14px centered text: "Screen: 2560x1440", "Fonts: 247 installed", "GPU: NVIDIA RTX", "Zone: America/Chicago", "Lang: en-US", "Plugins: 3 active". A thin blue line from each pill's right edge (x=200) converges to (280, h/2).
- **Funnel:** orange `#d95926` filled polygon from (280,60) → (350, mid−15) → (350, mid+15) → (280, h−50), with white bold 15px label "HASH" centered at (315, mid+4).
- **Arrow out:** magenta `#d55181` line (width 3) from (350, mid) to (420, mid) with filled magenta arrowhead.
- **Result box:** magenta rounded rect (240×70, radius 8) at x=440 centered vertically; white text: bold 16px "Your Fingerprint", 13px monospace "a7f2c9e1b4d8...", 13px "(unique among 300,000+ browsers)".

## What does it collect?

- **User agent string** — browser and OS name and version
- **Screen** resolution, colour depth, device pixel ratio
- **Timezone offset** and language list
- **Logical CPU count** and, on some browsers, memory tier
- **Fonts present**, probed by text-width measurement
- **GPU vendor and renderer** strings from WebGL
- **Canvas and audio hash** — rendering behaviour, not a setting
- **Precision:** the unit is a browser installation, not a person — shared machines merge, and a browser update typically shifts the hash

**Measured versus estimated:** attribute values come from APIs. *entropy_bits* and *stability_days* are estimates against whatever corpus the collector has.

**Inherited selection bias:** a fingerprint calibrated on visitors to one kind of site will overstate uniqueness on a different population.

### Visualization (canvas `c2`, 720×320)

Radar (spider) chart: relative contribution of each signal to uniqueness.

- **Title (bold 16px, centered, blue `#2a78d6`, y=20):** "How much each signal contributes to uniqueness".
- **Geometry:** center at (360, 135), max radius 85, six axes starting at top, clockwise.
- **Data (axis label → value on 0–1 scale):** Canvas hash 0.95, Fonts 0.85, Screen 0.60, Timezone 0.40, Plugins 0.55, GPU 0.80.
- **Grid:** concentric hexagonal rings at levels 0.25/0.50/0.75/1.00 plus radial axis lines, stroked light gray `#e5e9ef`.
- **Axis labels:** blue `#2a78d6` 14px, placed 20px beyond ring edge.
- **Data polygon:** fill magenta at 20% opacity (`rgba(213,81,129,0.2)`), stroke magenta `#d55181` width 2, with 4px-radius magenta dots at each vertex.

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, preformatted, verbatim):**

```
{
  // ── documented browser APIs ──
  "user_agent":     "Mozilla/5.0 (Macintosh; Intel Mac OS X
                     10_15_7) … Chrome/128.0 …",
  "languages":      ["en-US", "en"],   // navigator.languages
  "tz_offset":      -300,              // Date.getTimezoneOffset
  "screen":         "2560x1440",       // screen.width/height
  "color_depth":    24,                // screen.colorDepth
  "pixel_ratio":    2,                 // devicePixelRatio
  "cpu_cores":      10,                // hardwareConcurrency
  "device_memory":  8,                 // deviceMemory, coarse
  "touch_points":   0,                 // maxTouchPoints
  "webgl_vendor":   "Apple",           // getParameter(VENDOR)
  "webgl_renderer": "Apple M2 Pro",    // getParameter(RENDERER)
  "fonts_present":  ["Helvetica Neue", "Menlo", "…"],
  "canvas_hash":    "9c4e…",  // toDataURL of a drawn sample
  "audio_hash":     "b71f…",  // OfflineAudioContext readback

  // ── inferred / plausible — computed by the collector ──
  "fp_id":          "a7f2c9e1b4d8…",   // hash of the above
  "entropy_bits":   17.4,   // est. from a reference corpus
  "stability_days": 34,     // est. until an update shifts it
  "match":          "rejoin_candidate"
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **Fraud and abuse detection** — flagging one machine presenting as hundreds of accounts
- **Nothing stored** on the device that could simply be discarded

**Additional consequence** (label pill, orange)

- The same property gives **continuity across cookie clearing**, private windows and separate accounts
- The signal **does not distinguish** a fraud check from a measurement rejoin — the difference is what the operator does with the match

**Hard to consent-gate:** no header to set, no store to clear — the values are the ones a page legitimately reads to render itself. Countermeasures reduce precision rather than block access: rounding screen dimensions, freezing the user agent, adding noise to canvas readback.

**A wrong match costs differently on each side:** an abuse team's decision can be reviewed and reversed. The same threshold used as an identity join writes the wrong match into a visitor history that nothing later revisits.

### Visualization (canvas `c3`, 720×320)

Bar chart: probability that a rejoined visitor history contains at least one wrong match, growing with the number of rejoins. Values computed as 1−(1−p)^n with illustrative p=0.005.

- **Title (bold 14px, ink `#1a5276`, centered, y=24):** "Chance a rejoined history holds at least one wrong match".
- **Subtitle (12px, muted `#6b7280`, centered, y=42):** "one wrong match in 200 per join, applied again at every rejoin".
- **Axes/scale:** y from 0% to 100%, horizontal gridlines at 0/25/50/75/100% in `#e5e9ef` with right-aligned muted 11px percent labels; baseline at y=218, top at y=74; left padding 120, right padding 40.
- **Bars (width 52), x categories and computed heights:** 1 join → 0.5%; 10 joins → 4.9%; 50 joins → 22%; 200 joins → 63%; 600 joins → 95%. First three bars blue `#2a78d6` (fill at 32% opacity, 1px stroke); last two bars orange `#d95926` (same treatment). Bold 13px value label in the bar hue above each bar; 12px category label ("1 join", "10 joins", …) below the baseline in `#2c3e50`.
- **X-axis caption (muted 12px, centered below labels):** "rejoins in one visitor history".
- **Contrast note (left-aligned, below):** bold 12px aqua `#199e70` "An abuse check spends the same rate once, on a decision" followed by 12px `#2c3e50` "somebody can review and reverse."
- **Footnote (italic 11px, muted, bottom center):** "Illustrative rate — the bars are arithmetic from it, not a measured error rate."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, plus (row 2 only) the `.payload-note` caption and `.payload` pre block, both left-aligned.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with bold lead terms `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`; `.lbl` uppercase pill labels 0.7em bold — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` monospace 0.78em, background `#f8f9fa`, left border `3px solid #1a5276`, `white-space: pre`; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×320); a shared `setupCanvas(id)` helper reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills, `rr(ctx, x, y, w, h, r)` rounded-rect path. Chart 3 computes bar heights from the literal formula 1−(1−0.005)^n over joins [1, 10, 50, 200, 600] — no random data.
- **Palette:** declared once as tokens `P` — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately excluded from the series rotation (reserved for genuine alarm states). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
