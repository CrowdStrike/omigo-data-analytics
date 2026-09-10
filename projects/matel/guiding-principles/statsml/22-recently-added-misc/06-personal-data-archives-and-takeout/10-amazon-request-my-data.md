# Amazon Request My Data

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload sample + canvas right 55%)
**HTML title tag:** Amazon Request My Data

**Subtitle:** Amazon provides a bulk export of personal data through account settings. The archive covers purchase, browsing, voice, and media activity across all Amazon services.

**Verified line:** Last verified: August 2026

## How to Request

Navigate to Account Settings and select "Request My Personal Information." Amazon packages your data as a set of CSV and JSON files delivered via a secure download link sent to your registered email.

## Delivery Timeline

Amazon states up to 30 days. In practice, most exports arrive within 5 days. Larger accounts with extensive Alexa history or Kindle libraries may take longer.

## What's Included

- **Order history** — complete purchase records across all categories
- **Browsing history** — product pages viewed, search queries entered
- **Search history** — timestamped queries on the platform
- **Alexa voice recordings** — actual audio files with transcripts
- **Kindle highlights and notes** — annotations with book metadata
- **Ad click history** — ads interacted with on and off Amazon
- **Prime Video watch history** — titles, timestamps, completion
- **Whole Foods purchases** — itemized grocery receipts

**Key-point callout:** Alexa recordings are the most revealing category. The export includes the raw audio files of voice commands — not just transcripts. This captures ambient sound, other voices in the room, and commands issued by anyone near the device.

## What's Missing

**Missing callout (red-bordered):**

- Product recommendation model weights or inputs
- Purchase prediction scores assigned to your profile
- Dynamic pricing inputs — what price you were shown vs. others
- Internal segmentation labels (e.g., price sensitivity tier)

## Right column: payload sample

**Payload note (italic):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace `<pre>`, verbatim):**

```
// ── Order record ──
{
  // ── documented in export schema ──
  "order_id": "112-748…",
  "order_date": "2026-03-14T09:22:00Z",
  "items": [
    {
      "asin": "B09V3K…",
      "title": "Running Shoes Size 10",
      "quantity": 1,
      "price": 89.99,
      "currency": "USD"
    }
  ],
  "shipping_address_city": "Austin",
  "payment_method": "Visa ending 4821",

  // ── inferred / plausible ──
  "session_id": "amzn1.sess.a7f2…",
  "device_type": "mobile_app",
  "time_to_purchase_mins": 12
}

// ── Alexa voice record ──
{
  // ── documented in export schema ──
  "recording_id": "A3S5…-20260815-071403",
  "timestamp": "2026-08-15T07:14:03Z",
  "transcript": "what's the weather today",
  "audio_file": "Alexa/Audio/2026/08/A3S5…-071403.wav",
  "device_name": "Kitchen Echo",
  "wake_word_used": "Alexa",

  // ── inferred / plausible ──
  "confidence_score": 0.94,
  "preroll_duration_ms": 1500,
  "ambient_noise_level": "low"
}
```

### Visualization (canvas `chart`, 100% width × 380px)

Stacked area chart: monthly data volume by category over 12 months, four stacked layers.

- **Title (bold 14px, `#1a5276`, top center):** "Data Volume by Category (records per month, illustrative)".
- **Data (record counts in hundreds, months Jan–Dec):**
  - Orders (bottom layer, relatively flat, moderate volume): `[42, 38, 45, 40, 43, 47, 44, 41, 50, 55, 68, 72]` — stroke `#1a5276`, fill `rgba(26, 82, 118, 0.6)`
  - Browsing (highest volume, variable): `[180, 165, 195, 210, 175, 190, 220, 200, 185, 230, 260, 280]` — stroke `#27ae60`, fill `rgba(39, 174, 96, 0.6)`
  - Alexa (growing over time): `[30, 35, 40, 48, 55, 62, 70, 78, 85, 92, 100, 110]` — stroke `#e67e22`, fill `rgba(230, 126, 34, 0.6)`
  - Media/Video (top layer, moderate, spiky): `[60, 55, 50, 45, 52, 70, 85, 80, 58, 55, 65, 90]` — stroke `#e74c3c`, fill `rgba(231, 76, 60, 0.6)`
- **Stacking order (bottom to top):** Orders, Browsing, Alexa, Media/Video; layer boundary lines stroked at width 1.5 in each category color.
- **Axes:** y from 0 to max stacked total rounded up to nearest 100 (data max 552 → yMax 600), gridlines every 100 in `#e0e0e0` with `#888` 11px value labels; rotated y-axis label "Records" (`#888`); x labels Jan–Dec centered (`#2c3e50` 11px); padding left 50, right 20, top 50, bottom 50.
- **Legend (bottom center):** four 12×12 color squares with labels "Orders" (`#1a5276`), "Browsing" (`#27ae60`), "Alexa" (`#e67e22`), "Media/Video" (`#e74c3c`), text `#2c3e50` 11px, spaced 95px apart.

## Regeneration instructions

- **Layout:** single `.obj-table` (full width, collapsed borders) with one `<tr>`: left `<td>` (45%) holds `.obj-title` headings with paragraphs/bullet lists, a `.key-point` callout, and a `.missing` callout; right `<td>` (55%, text-align center) holds a `.payload-note` + `<pre class="payload">` block and the canvas.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.85em `#888`; `.obj-title` bold `#1a5276` 1.1em; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px. `.missing` — background `#fdf2f2`, left border `3px solid #e74c3c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace (ui-monospace/Menlo) 0.78em, white-space pre, left-aligned; `.payload-note` 0.82em `#666` italic left-aligned.
- **Canvas:** styled `width:100%; height:380px`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; stacked fills at 0.6 alpha of each. No nav bar, no back/home links; in regenerated HTML any card links use `.html` extensions.
