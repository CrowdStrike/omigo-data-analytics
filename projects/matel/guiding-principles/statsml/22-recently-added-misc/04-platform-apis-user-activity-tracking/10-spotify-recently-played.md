# Spotify Recently Played API

**Page type:** detail page (two-column obj-table layout: descriptive text left 45%, payload + canvas right 55%, one row)
**HTML title tag:** Spotify Recently Played API

**Subtitle:** Last 50 tracks, top artists/tracks over time windows, audio features (deprecated for new apps since Nov 2024) — listening history with relative ranking but no play counts.

## What It Provides

Recently played tracks (last 50 with timestamps), top artists (short/medium/long term), top tracks (short/medium/long term), audio features per track (danceability, energy, valence, tempo, etc. — deprecated for new apps since Nov 2024, see Restrictions), saved library, playlists.

## Authentication

OAuth 2.0 with scopes. Key scopes: `user-read-recently-played`, `user-top-read`, `user-read-playback-state`, `user-library-read`.

## Granularity

Recently played gives individual track plays with millisecond timestamps. Top items are ranked (position 0-49) but without play counts — only relative ordering. Time ranges: `short_term` (~4 weeks), `medium_term` (~6 months), `long_term` (calculated from roughly the last year of data, not all time).

## Key Limitation

**Key-point callout:** Spotify deliberately does not expose play counts. You get ranked lists but cannot know if your #1 artist has 500 plays or 50. The "recently played" endpoint caps at 50 tracks with no pagination backward.

## Rate Limit

**Key-point callout:** Not officially published per-endpoint. Community consensus is approximately 180 requests/minute per app. Returns 429 with Retry-After header when exceeded.

## Business Scenarios

Mood-based recommendations, productivity playlist generation, concert recommendation (top artists + location), social music sharing apps, music taste clustering for dating apps.

## Restrictions

**Key-point callout:** Cannot access other users' listening history (only authenticated user's own). Audio Features, Audio Analysis, Recommendations, and Related Artists endpoints were deprecated for new apps on Nov 27, 2024 (existing apps grandfathered). Streaming history beyond 50 tracks requires GDPR data export (not API).

## Payload Example

Monospace `.payload` block (right column), verbatim:

```
// ── illustrative payload; field names from Spotify Web API docs, values are not real ──
// GET /v1/me/top/artists?time_range=medium_term&limit=5
{
  "items": [
    {
      "name": "Radiohead",
      "id": "4Z8W4fKeB5YxbusRsdQVPb",
      "genres": ["alternative rock", "art rock"],
      "popularity": 78,
      "followers": { "total": 7234521 }
    },
    {
      "name": "Khruangbin",
      "id": "2mVVjNmdjXZZDvhgQWiakk",
      "genres": ["psychedelic soul", "funk"],
      "popularity": 71,
      "followers": { "total": 2145893 }
    }
  ],
  "total": 50,
  "limit": 5,
  "offset": 0,
  "href": "https://api.spotify.com/v1/me/top/artists?..."
}
// Note: no play_count field exists anywhere
```

## Top Artists — Relative Ranking Without Counts

### Visualization (canvas `chartTopArtists`, 720×360)

Horizontal bar chart: 8 top artists ranked by relative affinity score with no absolute values.

- **Layout:** padding left 80, right 40, top 40, bottom 50. Bar height = 70% of the per-artist slot, gap = 30%.
- **Data:** artists `['Artist A', 'Artist B', 'Artist C', 'Artist D', 'Artist E', 'Artist F', 'Artist G', 'Artist H']`; relative affinity scores (bar width fractions of chart width, decreasing by rank, no absolute meaning) `[1.0, 0.88, 0.76, 0.65, 0.55, 0.44, 0.34, 0.22]`.
- **Title (bold 13px, `#1a5276`, centered at y=20):** "Rank position only — no numeric play counts exposed".
- **Bars:** fill `rgba(26,82,118,0.35)`, border stroke `#1a5276` width 1.5.
- **Labels:** artist name right-aligned left of each bar (12px `#2c3e50`); rank label "#1"…"#8" inside each bar at its left edge (bold 11px `#1a5276`).
- **Annotation (bottom center, bold 12px, `#e74c3c`):** "No play counts available — rank only".

**Caption (`.canvas-label`, centered, 0.82em, `#666`):** Illustrative: Top artists — relative ranking without counts

## Official API References

- [Spotify Web API Documentation](https://developer.spotify.com/documentation/web-api) — official reference covering recently played, top items, and player endpoints

## Regeneration instructions

- **Layout:** platform-API detail page. h1 + `.subtitle`, then a single `.obj-table` with one `<tr>`: left `<td>` (45%) with `.obj-title` headings ("What It Provides", "Authentication", "Granularity", "Key Limitation", "Rate Limit", "Business Scenarios", "Restrictions") as `<p class="obj-title">` with inline `style="margin-top: 18px;"` after the first, paragraphs and `.key-point` callouts; right `<td>` (55%) with "Payload Example" title, `.payload` block, "Top Artists — Relative Ranking Without Counts" title, canvas, and `.canvas-label` caption. After the table, `<h2>Official API References</h2>` with a `<ul>` of links.
- **Page style:** `* { box-sizing: border-box; margin: 0; padding: 0; }`; body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`, margin-bottom 4px; `.subtitle` `#666` 1.05em, margin-bottom 24px. No nav bar, no back/home links.
- **Table style:** `.obj-table` full width, border-collapse, margin-bottom 24px; td vertical-align top, padding 16px, border `1px solid #2980b9`; `.obj-title` bold `#1a5276` 1.1em, margin-bottom 8px.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace/Menlo 0.78em, `white-space: pre`, `overflow-x: auto`, line-height 1.45, margin 12px 0.
- **Callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, margin 12px 0, 0.93em.
- **Caption:** `.canvas-label` — centered, 0.82em, `#666`, margin-top 6px.
- **Links:** `a { color: #1a5276; }`. In regenerated HTML, any card/page links use `.html` extensions.
- **Canvas:** `display: block; margin: 0 auto`; intrinsic 720×360; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
