# Tracking Data: Mobile App SDKs

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Mobile App SDKs

**Subtitle:** Prebuilt code libraries inside phone apps that report app activity to analytics and advertising companies.

## Section 1: What is it?

A prebuilt library inside the app, reporting to its vendor.

- **Developers rarely write their own analytics** — they add a vendor's SDK
- **The developer gets** a dashboard without building one
- **The vendor gets** a copy of the event stream
- **More than one** — where an app bundles several, each reports separately

**Declared, but not field-by-field:** the store listing declares data categories. The developer usually cannot state the field-level content of every request a bundled library makes — it is a binary they integrated, not code they wrote.

### Visualization (canvas `c1`, 720×320)

Layered app architecture diagram: a phone with stacked layers colored by who wrote them, fanning out to four vendor endpoints. Hue encodes provenance: blue `#2a78d6` = first-party (developer's own screens and code), orange `#d95926` = third-party (bundled vendor libraries), violet `#4a3aa7` = platform (OS and network).

- **Phone outline:** rounded rect (radius 14) at x=50, y=30, 170×200, ink `#1a5276` stroke 2px, inner screen fill `#f8f9fa`.
- **Layers inside phone** (140px wide at x=65):
  1. y=58, 34px tall, fill blue tint alpha 0.18, bold 15px blue text "Screens you see"
  2. y=97, 34px tall, fill blue tint alpha 0.34, blue text "Code the developer" / "wrote"
  3. y=136, 34px tall, solid orange fill, white bold text "Bundled vendor" / "libraries"
  4. y=175, 30px tall, solid violet fill, white 14px text "Phone OS / Network"
- **Arrow:** dashed (4/3) orange 2px horizontal line from the vendor-library layer (205,153) to (330,153) with a solid orange arrowhead at x=340; thin translucent lines (alpha 0.5 of each endpoint hue) fan from (340,153) to each endpoint box.
- **Vendor endpoints** (solid-filled rounded rects 130×30, radius 5, at x=350, starting y=52, 46px pitch, white 14px centered labels): "Ad network" green `#008300`, "Attribution" aqua `#199e70`, "Product analytics" magenta `#d55181`, "Crash reporting" yellow `#c98500`. Categories, not named products.
- **Labels:** bold 15px ink "One phone, one app" centered under the phone (x=135, y=248). Muted 14px three-line note at x=590 (y=112/128/144): "Each library sends its own" / "request from the same tap," / "with its own field names".
- **Footer band** (full-width, y=262, 58px tall, ink tint alpha 0.05): legend row 1 with 9px color squares — "first-party" (blue, x=30), "third-party library" (orange, x=120), "platform" (violet, x=250); legend row 2 — bold "destinations:" then lowercase endpoint names with their hue squares at x=125/250/375/530.

## Section 2: What does it collect?

- **Screen views** — which screens opened, in what order
- **Tap events** the developer chose to instrument
- **Time spent** per screen
- **Phone model**, OS version, and carrier
- **Coarse location** — usually derived from the request's IP address, no location permission involved
- **Advertising ID**, when the OS prompt was accepted
- **Crash reports** and performance traces
- **Purchases** and subscription state
- **Precision:** events are usually batched on-device and uploaded later, stamped by the device clock — timing is approximate and counts arrive late

**One tap, several records:** each SDK builds its own event, with its own field names and session boundary. No two agree on the count.

**Opaque to the developer too:** they see their own dashboard, not what each bundled library transmits.

### Visualization (canvas `c2`, 720×320)

Seven-row provenance table chart: which fields come from the OS vs the SDK's own instrumentation. Three provenance hues: blue `#2a78d6` = platform (supplied by the OS), orange `#d95926` = instrumented (developer had to instrument it), violet `#4a3aa7` = gated (exists only behind an accepted OS prompt).

- **Header strip** (full-width, 30px tall, ink tint alpha 0.06): bold 16px ink centered title "Where each field comes from".
- **Rows** (starting y=44, 30px pitch; each row washed in its provenance hue at alpha 0.07 with a 3px solid hue tab on the left; field label right-aligned at x=188 in 15px `#2c3e50`; a 5px-radius dot in the hue at x=200; explanation text in the hue at x=214 in 14px):
  1. "Device + OS version" — blue — "supplied by the OS — always available"
  2. "App version" — blue — "supplied by the OS — always available"
  3. "Screen views" — orange — "only if the developer instrumented it"
  4. "Taps" — orange — "only if the developer instrumented it"
  5. "Time on screen" — orange — "only if the developer instrumented it"
  6. "Purchases" — orange — "only if the developer instrumented it"
  7. "Advertising ID" — violet — "only if the OS prompt was accepted"
- **Footer band** (y=262, 58px, ink tint alpha 0.05): legend squares "platform" (blue, x=30), "instrumented" (orange, x=190), "gated" (violet, x=400); centered italic 14px muted line at y=309: "Absence of a field means it was never instrumented, not that the action did not happen."

### Payload (below canvas c2)

Payload note (italic, above the block): "Sample payload — illustrative structure, not real captured data."

```
// One tap, as one bundled SDK reports it. Each SDK in
// the app builds and sends its own record from the same tap.
{
  // ── platform values, documented by the OS ──
  "bundle_id":     "com.example.retail",
  "app_version":   "8.4.1",
  "os_version":    "iOS 18.2",
  "device_model":  "iPhone15,3",
  "advertising_id": null,           // zeroed: prompt was declined
  "tracking_auth": "denied",        // OS prompt result
  "vendor_install_id": "a7f3…",     // per-app, survives the denial

  // ── inferred / plausible SDK fields ──
  "event":         "add_to_cart",
  "screen":        "product_detail",
  "ts_device":     "2026-08-22T19:04:11.482Z",
  "session_id":    "b7c1…",
  "params":        { "item_id": "SKU-4471", "value": 89.00 },
  "batch_seq":     37               // queued offline, sent later
}
```

## Section 3: Why is it collected?

**Stated purpose** (label pill)

- **Product analytics** and **crash diagnostics**
- **Attribution** — which install came from which ad; without device-side reporting there is no path from a paid click to what followed

**Additional consequence** (label pill)

- The same library sits in **many unrelated apps** — a view no single developer has
- With a **shared identifier**, records from separate apps join; without one, they stay disconnected per-app streams

**The join is the fragile part:** the cross-app identifier sits behind a prompt, so joining falls back to a login, a per-app install ID, or an email hash. Each has different coverage, so a cross-app figure describes the subset where the join succeeded — and an app carrying no SDK from that vendor is absent rather than quiet.

### Visualization (canvas `c3`, 720×320)

Horizontal bar chart: coverage of each join key, and the subset where a cross-app join is actually possible. Illustrative shares — the shape, not measured rates.

- **Title (bold 14px, ink, centered, y=24):** "On how many users a cross-app join is even possible". **Subtitle (12px, muted, y=42):** "each key covers a different slice; the install ID never crosses an app".
- **Bars** (x0=320, max width 300px = 100%, starting y=64, 40px pitch, 22px tall; fill is the hue at alpha 0.38, stroke solid hue 1px; label right-aligned at x=308 in 12px `#2c3e50`; percentage bold 12px in the hue right of the bar):
  1. "advertising ID (behind an OS prompt)" — 26% — blue `#2a78d6`
  2. "signed-in account in both apps" — 34% — aqua `#199e70`
  3. "email hash the developer supplied" — 19% — violet `#4a3aa7`
  4. "per-app install ID only" — 100% — yellow `#c98500`
- **Note under bars (11px, muted, centered):** "install ID: present for everyone, useful across apps for no one".
- **Joinable-subset bar** (y=236, x=320, 26px tall): 48% wide (union of the first three, allowing for overlap), fill `rgba(217,89,38,0.38)`, stroke orange `#d95926` 2px; a dashed (4/3) grid-gray outline spans the full 100% width. Left label bold 13px orange "at least one key works"; "48%" right of the bar; below, 11px muted "dashed outline = all users of both apps".
- **Captions (centered):** italic 12px `#2c3e50` at h−26: "A cross-app figure describes the shaded slice and is reported as if it described all of it." Italic 11px muted at h−9: "Illustrative shares — the shape, not measured coverage."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Include a rounded-rect path helper and a `tint(hex, alpha)` helper for translucent fills.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Project-wide palette reference: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
