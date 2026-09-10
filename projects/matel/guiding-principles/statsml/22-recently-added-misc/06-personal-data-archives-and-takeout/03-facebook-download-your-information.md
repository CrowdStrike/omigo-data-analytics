# Facebook — Download Your Information

**Page type:** detail page (two-column obj-table layout per section: bullets left 45%, JSON payload or canvas right 55%)
**HTML title tag:** Facebook — Download Your Information

**Meta line (`.last-verified`, gray):** Last verified: August 2026

## What's Included

- Posts, photos, videos (with metadata: location, people tagged, timestamps)
- Messages (all Messenger conversations in full)
- Ad interest categories (what FB thinks you care about)
- Off-Facebook Activity (websites/apps that sent your data to FB via pixel/SDK)
- Face recognition templates (if feature was enabled before deprecation)
- Marketplace activity, payments, event RSVPs
- Friend list with add dates, poke history, search history
- Comments, reactions, groups, pages followed

Right column: JSON code block (`pre > code`):

```
{
  "off_facebook_activity_v2": [
    {
      "name": "Amazon",
      "events": [
        {
          "type": "PURCHASE",
          "timestamp": 1692700000
        },
        {
          "type": "PAGE_VIEW",
          "timestamp": 1692690000
        }
      ]
    },
    {
      "name": "NYTimes.com",
      "events": [
        { "type": "PAGE_VIEW", "timestamp": 1692680000 }
      ]
    }
  ]
}
```

## How to Request & Delivery

- Settings → Your Information → Download Your Information (or via Accounts Center for linked accounts)
- Can choose date range (all time or specific period)
- Can select specific categories or request everything
- Format: JSON (for processing) or HTML (human-readable)
- Media quality: High, Medium, Low
- Delivery: usually 1–3 days, up to a week for large archives
- Can request multiple times — no hard limit but throttled

### Visualization (canvas `sankey`, responsive width×420)

Sankey-style flow diagram: data sources (left) → FB Pixel / SDK (center) → usage (right), drawn with bezier flow bands.

- **Column titles (bold 11px `#2c3e50`, centered above each column):** "Data Sources", "Collection", "Usage".
- **Left nodes (sources, x=10, 120px wide, heights proportional to pct of usable height, 6px padding between):** Retail Sites 35% `#1a5276`; News Sites 20% `#2980b9`; Travel 12% `#27ae60`; Finance 10% `#e67e22`; Health 8% `#e74c3c`; Social/Dating 7% `#8e44ad`; Other 8% `#7f8c8d`. Each node shows its label and "N%" in white 11px text (only if the node is >14px tall).
- **Center node:** single rectangle 100px wide at horizontal center (x = W/2 − 50), fill `rgba(26,82,118,0.35)`, with bold 12px `#1a5276` two-line centered label "FB Pixel" / "/ SDK".
- **Right nodes (targets, x = W−130, 120px wide):** Ad Targeting 50% `#1a5276`; News Feed Ranking 30% `#27ae60`; Content Suggestions 20% `#e67e22`. White 11px labels inside.
- **Flow bands:** bezier-curved bands from each source's right edge to the corresponding vertical span of the center node, and from the center node's right edge to each target; each band filled with the node's color at ~27% alpha (hex color + `44` suffix).
- **Vertical layout:** 30px top margin, 20px bottom; center node inset 20px top/bottom relative to the usable height; targets start 30px below top margin.

## What's Conspicuously Missing

- EdgeRank / feed ranking scores for your posts
- Content moderation flags and review history
- Integrity signals (misinformation markers, account quality)
- Shadow distribution reduction metrics
- Advertiser bid amounts for reaching you
- Cross-platform identity graph (FB + IG + WhatsApp linking)
- Predicted lifetime value scores
- Political leaning classification (removed from ad tools but likely still computed)

Right column: JSON code block (`pre > code`):

```
{
  "ad_interests": {
    "topics": [
      "Machine learning",
      "Organic food",
      "National parks",
      "Electric vehicles",
      "Home automation"
    ],
    "advertisers_who_uploaded_your_contact": [
      "BigRetailer Inc",
      "LocalGym",
      "InsuranceCo",
      "PoliticalPAC_2024"
    ]
  }
}
```

## Key point (callout)

Off-Facebook Activity is the most revealing section. It shows every website and app that reported your visits back to Facebook via their tracking pixel or SDK — often hundreds of sites you never expected to be sharing data with Facebook. You can clear this history but cannot stop future collection without disconnecting from the web.

## Regeneration instructions

- **Layout:** detail page. h1, then `.last-verified` line. Three `h2` sections ("What's Included", "How to Request & Delivery", "What's Conspicuously Missing"), each followed by a one-row `table.obj-table` — left `<td>` (45%) with bullets, right `<td>` (55%) with a `<pre><code>` JSON block or the canvas. Ends with a `.key-point` callout div. In regenerated HTML, any links use .html extensions.
- **Page CSS:** body system sans-serif, `line-height: 1.6`, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; h2 1.3em `#1a5276`, `border-bottom: 2px solid #2980b9`, padding-bottom 6px, margin-top 32px. `table.obj-table` full width, collapsed borders, margin 16px 0; cells padding 16px, vertical-align top, **no cell borders** on this page. `li` 0.93em, 6px bottom margin.
- **Blocks:** `pre` — background `#f4f6f8`, border `1px solid #dce1e6`, radius 4px, padding 12px 14px, 0.82em; `code` monospace (SF Mono/Consolas/Menlo). `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.8em, margin-left 12px.
- **Canvas:** `display: block; margin: 0 auto; width: 100%`, height 420px; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#2980b9` secondary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, `#7f8c8d` gray; center-node fill `rgba(26,82,118,0.35)`. No nav bar, no back/home links.
