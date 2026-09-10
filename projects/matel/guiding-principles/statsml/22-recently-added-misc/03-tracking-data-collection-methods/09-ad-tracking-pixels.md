# Tracking Data: Ad Tracking Pixels

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Ad Tracking Pixels

**Subtitle:** A page embeds a 1x1 image, or an equivalent script call, hosted by an ad platform. Fetching it is an HTTP request to that platform, and the request itself carries the report.

## What is it?

An ordinary resource fetch, used as a report.

- **Mechanism:** page references an image or script on the ad platform's domain
- **The report rides in the request** — URL query parameters plus that domain's cookies
- **Response is irrelevant:** a transparent 1x1 GIF nobody sees. Nothing needs to come back
- **Now moving server-side:** cookie restrictions push advertisers to post conversions from their own backend, removing the browser from the path

**Why linkage works:** not a shared database — the *same domain* being fetched from many unrelated pages, with its own cookies attached each time.

### Visualization (canvas `c1`, 720×320)

Schematic diagram: a magnified hidden pixel on a web page, with request arrows to two separate platforms. One hue per actor: page blue `#2a78d6`, pixel magenta `#d55181`, platform A violet `#4a3aa7`, platform B aqua `#199e70`.

- **Web page:** blue-stroked rounded rect (300×200, radius 6) at (30,20); inner content area filled `#f0f4f8` with four placeholder blocks in translucent blue `rgba(42,120,214,0.18)` (a title bar, a body block, two side-by-side blocks).
- **Pixel:** a literal 1×1 magenta rect at (308,200) — barely visible.
- **Magnifying glass:** magenta circle (radius 60) centered at (430,100) with a thick handle line to (510,180); inside it a zoomed 30×30 magenta square labeled "1x1" in white bold 14px. A dotted magenta line (dash 3/3) connects the tiny pixel to the magnifier.
- **Platform boxes:** violet rounded rect (100×40, radius 6) at (590,40) labeled in white "Facebook" (bold 15px) / "servers" (13px), with a violet arrow from the magnifier; aqua rounded rect (100×40) at (590,120) labeled "Google" / "servers", with an aqua arrow.
- **Caption (muted 14px, bottom center):** "The image is invisible; fetching it is the report."

## What does it collect?

- **Page URL** the pixel fired on, plus the referring page
- **Click ID** — a platform token (`gclid`, `fbc`) carried in from the ad's landing URL, tying the visit to the specific ad click
- **Event name** — page view, add to cart, purchase
- **Order value** and currency, where the advertiser sends them
- **Hashed contact details** supplied by the advertiser
- **IP and user agent** — inherent to any request
- **Platform cookies** for its own domain

**Hashing is not anonymising:** SHA-256 here is deterministic and unkeyed, so one email gives the *same* digest at every advertiser. It removes the readable address, not the identity — pseudonymisation, and the join key across companies with no prior relationship.

**Why `event_id` exists:** the pixel and the server call describe one purchase and must be collapsed — which means the two paths are expected to disagree.

### Visualization (canvas `c2`, 720×320)

Hub-and-spoke diagram: one ad network domain fetched from twelve unrelated site contexts, color-coded by context family.

- **Title (bold 16px, ink `#1a5276`, centered, y=20):** "One ad network is fetched from unrelated contexts".
- **Hub:** magenta `#d55181` circle (radius 35) at (360,128), white labels "Ad Network" (bold 15px) / "(one domain)" (13px).
- **Spokes:** thin lines in translucent magenta (`tint(magenta, 0.3)`) from each site to the hub — the request path carries the hub's hue.
- **Sites:** 12 dots (radius 18) on a 95px-radius ring at angles 0 to 5.5 rad in 0.5 steps, each filled with its family hue at 28% opacity and stroked/labeled (13px) in the family hue: News Site (media), Shopping (commerce), Health Info (media), Dating App (personal), Bank (personal), Travel (commerce), Recipe Blog (media), Forum (media), Video (media), Email (personal), Weather (media), Sports (media).
- **Family hues:** Media and content — blue `#2a78d6`; Commerce and travel — violet `#4a3aa7`; Personal accounts — aqua `#199e70`.
- **Legend (y=266, starting x=90, 180px apart):** tinted swatch + label in each family's hue: "Media and content", "Commerce and travel", "Personal accounts".
- **Caption (muted 15px, bottom center):** "Each site carrying the pixel reports its own visit".

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, preformatted, verbatim):**

```
// 1. Browser-side fetch. Reporting rides in the
//    query string; the reply is a 1x1 GIF.
// ── inferred / plausible (observable, not a
//    published parameter contract) ──
GET https://www.facebook.com/tr/?id=8841…
      &ev=Purchase&dl=https%3A%2F%2Fshop.example
      %2Fthanks&rl=…&cd[value]=128.40
      &cd[currency]=USD&ts=1787840587
Cookie: _fbp=fb.1.1755820000.9214…

// 2. Resulting conversion record, sent server to
//    server by the advertiser's backend.
// ── documented in the Meta Conversions API
//    reference ──
{
  "event_name": "Purchase",
  "event_time": 1787840587,
  "event_id": "T-90214",        // deduplication key
  "action_source": "website",
  "event_source_url": "https://shop.example/thanks",
  "user_data": {
    "em": ["e3b0c44298fc1c149afb…"],   // SHA-256
    "ph": ["8d969eef6ecad3c29a3a…"],   // SHA-256
    "client_ip_address": "203.0.113.42",
    "client_user_agent": "Mozilla/5.0 …",
    "fbp": "fb.1.1755820000.9214…"
  },
  "custom_data": { "value": 128.40, "currency": "USD" }
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **Attribution** — without a conversion signal, an advertiser knows what it spent, not what it bought
- **Optimisation** — bidding models need an outcome to train against

**Additional consequence** (label pill, orange)

- One identifier accumulates across **many unrelated advertisers**, supporting audience targeting and lookalike expansion
- Linkage exists **with or without an account** on the platform

**Conflict of interest in the instrument:** the attribution number is produced by the platform being evaluated. It picks the lookback window and the credit rule, and both choices move its own measured contribution — independent of anyone's good faith.

**A tag fires where a tag was installed:** so a category read assembled this way is a sample of one platform's commercial footprint. Sites with no tag are missing, not zero, and the read measures tag adoption alongside demand.

### Visualization (canvas `c3`, 720×320)

Bar chart: the same 300 orders credited four different ways depending on the platform's own attribution setting. Illustrative counts against a fixed 300 orders.

- **Title (bold 14px, ink `#1a5276`, centered, y=26):** "Orders credited to the platform, under four of its own settings".
- **Subtitle (12px, muted `#6b7280`, centered, y=44):** "the same week of orders in every bar".
- **Scale:** y maps 0–300 orders onto baseline y=216 to top y=70; left padding 96, right padding 108; bar width 62; light gray baseline.
- **Bars (label → credited orders):** "1-day click" → 48; "7-day click" → 96; "28-day click" → 141; "28-day click +" / "1-day view" (two-line label) → 197. First three bars blue `#2a78d6` (32%-opacity fill, 1px stroke), last bar orange `#d95926`. Bold 13px value labels above bars in the bar hue; 12px category labels below in `#2c3e50`.
- **Reference line:** dashed aqua `#199e70` horizontal line (dash 6/4, width 2) at the 300-order level, labeled in bold 12px aqua: "300 orders placed — unchanged throughout".
- **Y-axis label (rotated, muted 11px):** "orders credited".
- **Caption (italic 12px, `#2c3e50`, centered):** "The platform picks the window, and the window sets its own measured contribution."
- **Footnote (italic 11px, muted, bottom center):** "Illustrative counts — the window names are standard; the numbers are not measured."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, plus (row 2 only) the `.payload-note` caption and `.payload` pre block, both left-aligned.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with bold lead terms `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`; `.lbl` uppercase pill labels 0.7em bold — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` monospace 0.78em, background `#f8f9fa`, left border `3px solid #1a5276`, `white-space: pre`; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×320); a shared `setupCanvas(id)` helper reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills, `rr(ctx, x, y, w, h, r)` rounded-rect path. All chart data is hardcoded literal arrays — no random values.
- **Palette:** declared once as tokens `P` — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately excluded from the series rotation (reserved for genuine alarm states). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
