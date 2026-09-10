# Google Takeout — Your Data Export

**Page type:** detail page (two-column obj-table layout: bullets left 45%, code sample or canvas right 55%, one h2 + table per section; last-verified line under the h1)
**HTML title tag:** Google Takeout — Your Data Export

**Last verified:** Last verified: August 2026

## Section 1: What's Included

- 80+ products selectable (Search, Maps, YouTube, Gmail, Photos, Drive, Chrome, Fit, Play, etc.)
- Location History as JSON with lat/lng/timestamps/confidence
- YouTube watch history with video IDs and timestamps
- Search history (queries + clicks)
- Ad personalization profile (topics, demographics)
- Gmail (MBOX format), Drive files, Photos (original resolution)

**Right column — code block (pre/code, verbatim):**

```
{
  "locations": [
    {
      "timestampMs": "1692700000000",
      "latitudeE7": 374219999,
      "longitudeE7": -1220849999,
      "accuracy": 18,
      "activity": [
        { "type": "WALKING", "confidence": 72 }
      ]
    }
  ]
}
```

## Section 2: How to Request & Delivery

- Go to takeout.google.com
- Select products (all or individual)
- Choose export format: .zip or .tgz
- Choose delivery: email link, Drive, Dropbox, OneDrive, Box
- Delivery time: hours for small exports, 2-3 days for large (50GB+)
- Can schedule recurring exports: every 2 months for 1 year (6 total)
- No limit on how often you request

### Visualization (canvas `treemap`, dynamic width min 720 × 380)

Treemap of relative data volume by Google product, laid out by recursive half-split (split vertically when the region is wider than tall, otherwise horizontally), 10px outer padding, 3px gap between tiles.

- **Data (label, share, tile color):**
  - Photos, 40%, `#1a5276`
  - Drive, 25%, `#27ae60`
  - Gmail, 15%, `#e67e22`
  - YouTube, 10%, `#e74c3c`
  - Maps/Location, 5%, `rgba(26,82,118,0.65)`
  - Other, 5%, `rgba(26,82,118,0.35)`
- **Tile labels:** white centered text — bold product name (font size scales with tile width, clamped 11-18px) above a smaller "~N%" percentage line (clamped 10-14px).
- **Title (top left, bold 13px, `#2c3e50`):** "Relative Data Volume by Google Product".
- Canvas width is computed from the rendered element (minimum 720px), height fixed at 380; scaled by `window.devicePixelRatio`.

## Section 3: What's Conspicuously Missing

- Internal quality scores (page quality, spam scores)
- Spam model features used to classify your emails
- Ad auction bid data (what advertisers paid to reach you)
- Search ranking personalization weights
- YouTube recommendation model scores
- Gmail Smart Reply/Compose model predictions
- Google Maps traffic contribution data

**Right column — code block (pre/code, verbatim):**

```
{
  "adPersonalization": {
    "topics": ["Technology", "Travel", "Cooking"],
    "inferredAge": "25-34",
    "inferredGender": "Male",
    "inferredHouseholdIncome": "Top 30%",
    "inferredParentalStatus": "Not a parent"
  }
}
```

## Callout (key-point box)

The ad personalization profile is the closest Google gives you to seeing how they model you — but it's a curated summary. The actual targeting graph used in real-time ad auctions has hundreds more dimensions.

## Regeneration instructions

- **Layout:** h1, `.last-verified` line, then one `<h2>` + full-width `.obj-table` per section (one `<tr>` each): left `<td>` (45%) holds a `<ul>` of bullets, right `<td>` (55%) holds a `<pre><code>` JSON sample (sections 1 and 3) or the treemap canvas (section 2). The `.key-point` callout follows the last table.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; h2 1.3em `#1a5276` with bottom border `2px solid #2980b9`, padding-bottom 6px; table cells `vertical-align: top`, padding 12px, no borders; list items 0.93em with 5px bottom margin.
- **Code blocks:** `pre` background `#f4f6f7`, border `1px solid #dce1e4`, radius 4px, padding 12px 14px, 0.82em, monospace ('SF Mono', 'Fira Code', 'Fira Mono', Menlo).
- **Callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em.
- - **Canvas:** `display: block; margin: 0 auto; width: 100%`; backing store sized to `max(720, rendered width) × 380` multiplied by `window.devicePixelRatio`, CSS size fixed, `ctx.scale` back to logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar/tile tints `rgba(26,82,118,0.65)` and `rgba(26,82,118,0.35)`. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
