# Apple Privacy — Data Export

**Page type:** detail page (two-column obj-table layout per section: bullets left 45%, JSON payloads or canvas right 55%)
**HTML title tag:** Apple Privacy — Data Export

**Meta line (`.last-verified`, gray):** Last verified: August 2026

## What's Included

- iCloud data (contacts, calendars, bookmarks, notes, reminders)
- Apple Pay transaction history (merchant, amount, date)
- Siri request logs (transcriptions of what you asked — retained for limited period)
- App Store and iTunes purchase/download history
- Device analytics and diagnostics (crash logs, usage stats)
- AppleCare support interactions
- Apple Music listening history
- iCloud Photos metadata (not the photos themselves — those are in iCloud directly)
- Sign in with Apple usage (which apps you authenticated to)
- Maps request history (searches, but not full route tracking)

Right column: JSON code block (`pre > code`):

```
{
  "apple_pay_transactions": [
    {
      "merchant": "Coffee Shop LLC",
      "amount": "$4.85",
      "date": "2025-08-20",
      "card_suffix": "4242",
      "device": "iPhone 15 Pro"
    }
  ],
  "siri_requests": [
    {
      "timestamp": "2025-08-20T07:30:00Z",
      "transcript": "What's the weather today",
      "response_type": "weather_card",
      "audio_retained": false
    }
  ]
}
```

## How to Request & Delivery

- Go to **privacy.apple.com** and sign in with Apple ID
- Select "Request a copy of your data"
- Choose categories or request everything
- Select maximum file size for splits (1GB, 2GB, 5GB, 10GB, 25GB)
- Delivery: up to 7 days (Apple quotes this; often faster)
- Data available for download for 14 days after ready
- Can request again after current request completes
- Notably smaller archive than Google/Facebook equivalents

### Visualization (canvas `c1`, 720×420)

Paired horizontal bar chart comparing, per data category, how much appears in the export vs how much is processed on-device only.

- **Title (bold 13px, `#1a5276`, top center):** "Data Processing: Export vs On-Device".
- **Legend (below title):** green swatch `#27ae60` — "Included in Export"; muted red swatch `rgba(231,76,60,0.4)` — "Processed On-Device Only"; labels 11px `#2c3e50`.
- **Categories and data pairs `[Included in Export, Processed On-Device Only]` on a relative 0–100 scale:**
  - Photos ML Tags: [15, 90]
  - Siri Voice Processing: [30, 85]
  - Face/Object Recognition: [10, 95]
  - Keyboard Predictions: [5, 92]
  - Health Analytics: [8, 88]
  - Screen Time Analysis: [12, 80]
  - Location Clustering: [10, 85]
  - App Suggestions: [5, 90]
  - Crash Detection ML: [35, 75]
- **Layout:** margins — left 170 (right-aligned 11px `#2c3e50` category labels), right 30, top 50, bottom 50. Each category row holds two bars (30% of the row height each, 4px gap): green export bar above, muted red on-device bar below; bar width = value/100 of chart width.
- **Axes:** vertical axis line at x=170 in `#bdc3c7`; bottom tick marks at 0%, 25%, 50%, 75%, 100% with 10px `#999` labels.

## What's Missing

- On-device ML model outputs (photo classification, face grouping, scene detection)
- Keyboard prediction model and learned vocabulary
- Health app analytics and trend detection (stays on device)
- Screen Time behavioral patterns
- App suggestion model (Siri Suggestions for which app you'll open)
- Location clustering (Significant Locations) — visible in Settings but not exported
- Safari anti-tracking intelligence (what was blocked)
- Neural Engine processing outputs
- Find My network contribution data

Right column: JSON code block (`pre > code`):

```
{
  "philosophy_comparison": {
    "google_approach": {
      "photo_tags": "Server-side ML, included in export",
      "search_history": "Full history retained server-side",
      "location": "Continuous, server-stored"
    },
    "apple_approach": {
      "photo_tags": "On-device ML, never leaves phone",
      "siri_data": "Processed locally, random ID if sent",
      "location": "Significant Locations on-device only"
    }
  }
}
```

## Key point (callout)

Apple's export is notably thinner than Google's or Facebook's — not because Apple collects less total intelligence about you, but because their architecture processes most behavioral signals on-device. The ML models that predict your behavior exist on your phone, not Apple's servers. Whether this is genuinely more private or just less auditable is an open question.

## Regeneration instructions

- **Layout:** detail page. h1, then `.last-verified` line. Three `h2` sections ("What's Included", "How to Request & Delivery", "What's Missing"), each followed by a one-row `table.obj-table` — left `<td>` (45%) with bullets, right `<td>` (55%) with a `<pre><code>` JSON block or the canvas. Ends with a `.key-point` callout div. In regenerated HTML, any links use .html extensions.
- **Page CSS:** body system sans-serif, `line-height: 1.6`, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; h2 1.3em `#1a5276`, `border-bottom: 2px solid #2980b9`, padding-bottom 6px, margin-top 32px. `table.obj-table` full width, collapsed borders, margin 16px 0; cells padding 16px, vertical-align top, **no cell borders** on this page. `li` 0.93em, 6px bottom margin.
- **Blocks:** `pre` — background `#f4f6f7`, border `1px solid #dce1e4`, radius 4px, padding 14px, 0.82em; `code` monospace (SF Mono/Consolas/Menlo). `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.8em, margin-left 12px.
- **Canvas:** declared with intrinsic `width="720" height="420"` attributes, `display: block; margin: 0 auto; width: 100%`; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red (muted as `rgba(231,76,60,0.4)`), `#e67e22` orange; text `#2c3e50`, axis grays `#bdc3c7`/`#999`. No nav bar, no back/home links.
