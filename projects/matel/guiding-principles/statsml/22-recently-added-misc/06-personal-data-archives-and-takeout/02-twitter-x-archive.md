# Twitter/X Archive — Your Data Export

**Page type:** detail page (two-column obj-table layout per section: bullets left 45%, JSON payload or canvas right 55%)
**HTML title tag:** Twitter/X Archive — Your Data Export

**Meta line (`.last-verified`, gray):** Last verified: August 2026

## What's Included

- All tweets with full engagement counts (retweets, likes, replies, quote tweets)
- Direct Messages (full conversation threads)
- Ad impressions log (every ad you saw with advertiser + targeting criteria)
- Inferred interest topics (Twitter's topic model of you — often 300+ topics)
- IP login history with timestamps and user agents
- Connected apps and their permission scopes
- Account creation data, email/phone changes, age verification
- Periscope data (if used), Spaces participation, Communities

Right column: JSON payload block (`.payload`, monospace, blue left border):

```
{
  "tweet": {
    "id": "1692700000000000000",
    "full_text": "Example tweet content here",
    "created_at": "2025-08-22T14:30:00.000Z",
    "retweet_count": 12,
    "favorite_count": 89,
    "reply_count": 3,
    "entities": {
      "hashtags": ["datascience"],
      "user_mentions": ["@example"]
    }
  }
}
```

## How to Request & Delivery

- Settings → Your Account → Download an archive of your data
- Requires identity verification (re-enter password, may need 2FA)
- Delivery: 24-48 hours typical, emailed as a download link
- Archive is a .zip containing HTML viewer + raw JSON/JS files
- Can request approximately once every 30 days
- Archive includes an interactive HTML page to browse locally

### Visualization (canvas `topicChart`, responsive width (min 720)×400)

Horizontal bar chart of inferred interest topics with confidence-based coloring.

- **Title (bold 14px, `#1a5276`, top center):** "Inferred Interest Topics — Confidence Scores".
- **Data (label: value %):** Technology: 92, Sports: 78, Politics: 71, Entertainment: 65, Business: 58, Science: 52, Gaming: 45, Food: 38, Travel: 31, Fashion: 24, Fitness: 19, Music: 15.
- **Layout:** bars 24px tall, 6px gap, start y=42; right-aligned topic labels in a 100px label column (`#2c3e50`, 13px); bar width = value/100 of available width (width − 100 − 90).
- **Bar colors by confidence:** value ≥ 65 → green `#27ae60`; 40–64 → orange `#e67e22`; < 40 → red `#e74c3c`. Bars filled at 0.75 alpha with a 1px solid border in the same color.
- **Value labels:** bold 12px `#2c3e50`, e.g. "92%", placed 6px right of each bar end.
- **Legend (below bars):** three swatches (14×14, 0.75 alpha fill) with labels "High (65%+)" `#27ae60`, "Medium (40-64%)" `#e67e22`, "Low (<40%)" `#e74c3c`, spaced 120px apart starting at the label-column x.

## What's Conspicuously Missing

- Internal trust/quality scores
- Shadow-ban or visibility reduction signals
- Recommendation algorithm weights (For You feed ranking)
- Content moderation flags and review history
- Engagement prediction scores per tweet
- Network cluster assignment (what "community" Twitter puts you in)
- Advertiser-specific bid amounts targeting you

Right column: JSON payload block (`.payload`):

```
{
  "ad": {
    "impressionTime": "2025-08-20T09:15:00Z",
    "advertiserName": "TechCo Inc",
    "targetingCriteria": [
      "Age: 25-49",
      "Interest: Software Development",
      "Follower look-alike: @techleader",
      "Location: United States",
      "Platform: iOS"
    ],
    "engagementType": "impression_only"
  }
}
```

## Key point (callout)

The inferred interest topics file is the most revealing — it shows Twitter's complete topic model of you, often containing 300+ weighted categories you never explicitly chose. This is the signal that drives your For You feed and ad targeting.

## Regeneration instructions

- **Layout:** detail page. h1, then `.last-verified` line. Three `h2` sections ("What's Included", "How to Request & Delivery", "What's Conspicuously Missing"), each followed by a one-row `.obj-table` — left `<td>` (45%) with a `<ul>` of bullets, right `<td>` (55%) with either a `<pre class="payload">` JSON block or a canvas. Ends with a `.key-point` callout div. In regenerated HTML there are no card links; any links would use .html extensions.
- **Page CSS:** body system sans-serif, `line-height: 1.6`, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`, padding-bottom 6px. `.obj-table` full width, collapsed borders; cells `border: 1px solid #e0e0e0`, padding 16px, vertical-align top. `li` 0.93em, 6px bottom margin.
- **Blocks:** `.payload` — background `#f8f9fa`, `border-left: 3px solid #1a5276`, padding 10px, monospace (ui-monospace/Menlo) 0.78em, `white-space: pre`. `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.8em.
- **Canvas:** `display: block; margin: 0 auto; width: 100%`; width computed as max(720, parent width − 24), height 400; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; text `#2c3e50`; accent border `#2980b9`. No nav bar, no back/home links.
