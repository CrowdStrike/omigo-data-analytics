# Instagram — Data Download

**Page type:** detail page (two-column obj-table layout per section: bullets left 45%, JSON payload or canvas right 55%)
**HTML title tag:** Instagram — Data Download

**Meta line (`.verified`, gray):** Last verified: August 2026

## What's Included

- Posts (photos/videos with captions, timestamps, location tags)
- Stories archive (preserved even after 24hr expiry window)
- Reels (with view counts and interaction data)
- Direct Messages (full conversation threads including shared posts)
- Ad interests and topics (similar to Facebook's model)
- Content interactions: likes, saves, shares, comments given
- Search history (accounts, hashtags, places searched)
- Shopping activity, checkout history, saved products
- Login activity with IP addresses and device info
- Followers/following lists with timestamps

Right column: JSON payload block (`.payload`, monospace, blue left border):

```
{
  "media": [
    {
      "uri": "media/posts/202508/image_001.jpg",
      "creation_timestamp": 1692700000,
      "title": "Summer vibes",
      "media_metadata": {
        "photo_metadata": {
          "latitude": 37.7749,
          "longitude": -122.4194
        }
      }
    }
  ],
  "stories": [
    {
      "uri": "media/stories/202508/story_001.mp4",
      "creation_timestamp": 1692600000,
      "is_highlight": false
    }
  ]
}
```

## How to Request & Delivery

- Settings → Your Activity → Download Your Information
- Also accessible via Meta Accounts Center (linked FB/IG)
- Can select specific categories or request all data
- Format: JSON or HTML
- Date range selection available
- Delivery: typically a few hours to 2 days
- File sent as downloadable link via email or in-app notification
- Can request multiple times

### Visualization (canvas `stackedChart`, responsive width (fallback 720)×380)

Stacked vertical bar chart of monthly content production by type.

- **Title (bold 14px, `#1a5276`, top center):** "Content Production by Type (Jan–Aug 2025)".
- **X categories:** Jan, Feb, Mar, Apr, May, Jun, Jul, Aug.
- **Data series (stacked bottom→top):**
  - Posts (blue `#1a5276`): `[4, 3, 5, 4, 3, 4, 5, 4]`
  - Stories (orange `#e67e22`): `[22, 25, 28, 24, 26, 23, 21, 20]`
  - Reels (green `#27ae60`): `[2, 3, 4, 6, 8, 10, 13, 16]`
- **Y axis:** 0 to max stacked total rounded up to nearest 10 (= 40), 5 ticks with horizontal gridlines `#ccc`, tick labels 11px `#666` right-aligned; solid x- and y-axis lines in `#2c3e50`. Padding: left 50, right 20, top 50, bottom 50.
- **Bars:** width = 60% of each month slot, centered; month labels 11px `#666` below the baseline.
- **Legend (bottom center):** color swatches (14×14) with labels "Posts" `#1a5276`, "Stories" `#e67e22`, "Reels" `#27ae60`, spaced 85px apart.
- **Y-axis label:** rotated vertical "Count" in 11px `#666` at the left edge.

## What's Conspicuously Missing

- Explore algorithm weights and ranking signals
- Shadow-recommendation or reach-reduction signals
- Engagement prediction scores per post
- Hashtag distribution/suppression flags
- Content classification labels (what category IG puts your content in)
- Reels boost/de-boost signals
- Account quality/trust scores
- Creator fund revenue calculations and audience overlap metrics

Right column: JSON payload block (`.payload`):

```
{
  "ads_interests": [
    "Photography",
    "Sustainable fashion",
    "Coffee shops",
    "Travel destinations",
    "Fitness & yoga"
  ],
  "content_interactions": {
    "likes_given": 4892,
    "comments_given": 312,
    "posts_saved": 847,
    "stories_replied_to": 156,
    "reels_shared": 203
  }
}
```

## Key point (callout)

Stories are permanently preserved in your archive even though they disappear from the app after 24 hours. Instagram keeps every story you ever posted — the ephemeral nature is a UI feature, not a data deletion policy.

## Regeneration instructions

- **Layout:** detail page. h1, then `.verified` line. Three `h2` sections ("What's Included", "How to Request & Delivery", "What's Conspicuously Missing"), each followed by a one-row `.obj-table` — left `<td>` (45%) with bullets, right `<td>` (55%) with a `<pre class="payload">` JSON block or the canvas. Ends with a `.key-point` callout div. In regenerated HTML, any links use .html extensions.
- **Page CSS:** body system sans-serif, `line-height: 1.6`, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; h2 1.3em `#1a5276`, `border-bottom: 2px solid #2980b9`, padding-bottom 6px. `.obj-table` full width, collapsed borders; cells `border: 1px solid #e0e0e0`, padding 16px, vertical-align top. `li` 0.93em, 6px bottom margin.
- **Blocks:** `.payload` — background `#f8f9fa`, `border-left: 3px solid #1a5276`, padding 10px, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`. `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.8em.
- **Canvas:** `display: block; margin: 0 auto; width: 100%`, height 380px; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; text `#2c3e50`, muted `#666`; accent border `#2980b9`. No nav bar, no back/home links.
