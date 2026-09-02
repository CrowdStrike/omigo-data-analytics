# Meta Graph & Instagram

**Page type:** detail page (two-column obj-table layout: text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** Meta Graph & Instagram — Platform APIs

**Subtitle:** Lets you pull posts, comments and performance stats for Facebook Pages and Instagram business accounts you manage — not a window into other people's profiles.

**Verified badge:** Last verified: August 2026

## What you can get

- Posts, comments and reactions on Pages and Instagram business accounts you administer
- Performance stats per post: reach, impressions, saves, video views
- Follower demographics as bucketed totals (age band, gender, country) — never a list of individual followers
- Push notifications when new comments or mentions arrive

### Key-point callout

**This API only works for accounts you own or manage, and the research-facing surface has shrunk repeatedly** — friend lists were removed years ago, and the CrowdTangle monitoring tool was shut down in 2024. Any long-running metric series that crosses one of these policy changes contains a break caused by the platform, not by users. Treat those dates as known changepoints, not findings.

## Watch out for

- Real permissions require Meta's App Review with a written use case — prototypes work, production launches stall here
- No friends data or social-graph access of any kind — those endpoints (including the last stragglers like taggable_friends) were removed years ago
- API versions expire on a schedule, and metric definitions have been revised mid-stream
- Deleted and hidden posts or comments vanish from the API, quietly biasing what you measure

## Post performance in one call

Code block (pre, monospace, left border #1a5276):

```
GET /vXX.0/{ig-media-id}/insights
      ?metric=reach,saved

{
  "data": [
    { "name": "reach", "period": "lifetime",
      "values": [ { "value": 28104 } ] },
    { "name": "saved", "period": "lifetime",
      "values": [ { "value": 611 } ] }
  ]
}

/* Audience data comes only as bucketed totals.
   There is no followers?fields=age,gender,city
   at any access tier. */
```

## Research-accessible surface over time

### Visualization (canvas `metaAccessChart`, 100% width × 380px CSS height)

Qualitative stepped area chart: research-accessible API surface stepping down at successive policy events, 2014–2026.6 on the x-axis.

- **Title (bold 13px, `#1a5276`, top left):** "Research-accessible surface contracted at successive policy events".
- **Qualitative banner (italic 10px, `#e67e22`, below title):** "QUALITATIVE — the y-axis encodes ordering only. No quantitative magnitude is claimed or implied."
- **Steps (from-year, to-year, relative level):** 2014.0–2015.3 at 1.00; 2015.3–2018.2 at 0.74 (v2.x friend-list deprecation); 2018.2–2024.0 at 0.44 (post-CA permission tightening); 2024.0–2024.7 at 0.30 (Basic Display deprecation); 2024.7–2026.6 at 0.16 (CrowdTangle shutdown).
- **Rendering:** filled band under the stepped level in `rgba(26,82,118,0.35)` down to y=0, stepped outline in `#1a5276` width 2; faint unlabelled horizontal guides at 0.25/0.5/0.75 in `#eee`; padding top 58, right 18, bottom 88, left 58.
- **Event markers:** vertical dashed red lines (`#e74c3c`, dash 4/4, width 1.2) with a red 3.5px dot at the post-event level and staggered two-line red labels (9.5px, alternate rows, right-aligned when past 72% width):
  - 2015.3: "v2.x: general friend" / "lists removed"
  - 2018.2: "permission tightening," / "App Review expanded"
  - 2024.0: "IG Basic Display" / "deprecated"
  - 2024.7: "CrowdTangle" / "shut down"
- **X axis:** gray (`#999`) baseline with ticks and 10px `#666` labels at 2014, 2016, 2018, 2020, 2022, 2024, 2026.
- **Y axis:** rotated bold 10px `#1a5276` label "relative breadth of research-accessible surface (qualitative)"; italic 9px `#999` anchors "broader" (top) and "narrower" (bottom) instead of numbers.
- **Captions (bottom left):** bold 10px red: "A metric time series crossing any dashed line has a break caused by access policy, not by users." Then italic 9.5px `#666`, two lines: "Metric-definition revisions (e.g. reach / impressions methodology) shift levels the same way, with no behaviour change behind them." / "Declare these dates as known changepoints before analysis. Rediscovering them as \"findings\" is a bookkeeping failure, not a result."

## Official API References

- [Graph API Documentation](https://developers.facebook.com/docs/graph-api/) — core node/edge/field reference and versioning
- [Instagram Graph API](https://developers.facebook.com/docs/instagram-api/) — Business/Creator account media, comments, Insights

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, one `.obj-table` (full-width, border-collapse, one `<tr>`): left `<td>` 45% with `.section-head` headings ("What you can get", "Watch out for"), bullet lists and a `.key-point` callout; right `<td>` 55% with a `.section-head`, a `<pre>` request/response payload and the canvas (`height="380"` attribute). Below the table: an `h2` "Official API References" with a two-link list. Links in HTML are external URLs as given.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, 2px 10px padding, 4px radius, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; table cells `1px solid #ddd` border, 16px padding, top-aligned; `.section-head` bold `#1a5276` 0.95em; li 0.93em; links `#1a5276`.
- **Pre style:** background `#f8f9fa`, left border `3px solid #1a5276`, ui-monospace 0.78em, 12px padding, 4px radius.
- **Key-point style:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em.
- **Canvas:** `display: block; width: 100%; margin: 16px auto 0`; fixed 380px CSS height set in JS; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar/area fill `rgba(26,82,118,0.35)`; grays `#666`/`#999`/`#eee`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
