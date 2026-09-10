# Spotify Playlists API

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, one Overview row, plus reference list)
**HTML title tag:** Spotify Playlists API — Platform APIs

**Subtitle:** Lets an app read and edit a signed-in user's playlists, saved tracks and saved albums.

**Verified badge:** Last verified: August 2026

## Overview

Left column:

**What you can get**

- A user's own playlists and their full track lists (with the user's consent)
- The ability to create and edit playlists on the user's behalf
- The user's saved tracks and saved albums
- Catalog details for tracks, albums and artists

Key-point callout (red left border): **In November 2024 Spotify shut off Audio Features, Audio Analysis, Recommendations and Related Artists for new apps** — new integrations get access-denied errors. Any plan built on "danceability", "energy" or mood scores no longer works unless the app was approved before the cutoff.

**Watch out for**

- Algorithmic mixes like Discover Weekly are invisible to the API, even though they appear in the user's library
- Rate limits are not published — heavy use gets throttled unpredictably
- Most older tutorials and datasets rely on the now-deprecated audio-features endpoints

Right column:

Code-sample caption (0.85em, #555): **Audio features response** — deprecated for new apps since Nov 2024

Code block (`pre`):

```
{
  "danceability": 0.735,
  "energy": 0.578,
  "valence": 0.624,
  "tempo": 98.002,
  "acousticness": 0.514,
  "instrumentalness": 0.0902,
  "speechiness": 0.0461,
  "liveness": 0.159,
  "id": "06AKEBrKUckW0KREUWRnvT",
  "duration_ms": 255349
}
```

Chart caption (0.85em, #555): **Energy vs. Valence Scatter — the mood map audio features used to enable**

### Visualization (canvas `scatterCanvas`, responsive width × 380)

Scatter plot: a "mood map" of tracks on valence (x) vs energy (y), split into four quadrants.

- **Canvas sizing:** `height="380"` attribute; width fills the column (`width: 100%`), backing store sized from `getBoundingClientRect().width` × devicePixelRatio, redrawn on window resize. Padding: top 30, right 30, bottom 50, left 60. White background.
- **Grid:** 10×10 light-gray grid lines `#e8e8e8`, width 1.
- **Quadrant dividers:** dashed lines (`#ccc`, width 1.5, dash 5/4) at valence=0.5 (vertical) and energy=0.5 (horizontal).
- **Quadrant labels** (11px, `#bbb`, centered): "Angry / Turbulent" (top-left), "Happy / Energetic" (top-right), "Sad / Depressed" (bottom-left), "Calm / Peaceful" (bottom-right).
- **Axes:** L-shaped border in `#2c3e50`, width 1.5. Tick labels (11px, `#555`) at 0.0, 0.2, 0.4, 0.6, 0.8, 1.0 on both axes.
- **Axis titles** (bold 12px, `#1a5276`): x = "Valence (positivity)" bottom center; y = "Energy (intensity)" rotated -90°, left side.
- **Data points** as `[energy, valence]` pairs: `[0.82, 0.15], [0.91, 0.22], [0.76, 0.08], [0.88, 0.31], [0.85, 0.72], [0.79, 0.81], [0.92, 0.65], [0.73, 0.88], [0.68, 0.77], [0.22, 0.18], [0.31, 0.25], [0.15, 0.12], [0.28, 0.35], [0.18, 0.72], [0.25, 0.85], [0.35, 0.78], [0.12, 0.62], [0.55, 0.48], [0.62, 0.55], [0.45, 0.60]`. Plotted at x = valence, y = energy.
- **Point style:** radius 6 circles, fill `rgba(26, 82, 118, 0.35)`, stroke `#1a5276` width 1.5.

## Official API References

- [Spotify Web API](https://developer.spotify.com/documentation/web-api) — main documentation root for all Web API endpoints
- [Developer Policy](https://developer.spotify.com/policy) — usage restrictions on Spotify data and content

## Regeneration instructions

- **Layout:** single-page platform-API detail doc. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" with a `table.obj-table` (one `<tr>`: left `<td>` 45% text, right `<td>` 55% with pre-formatted JSON sample and canvas), then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, color `#2c3e50`, padding 30px 40px, white background. h1 `#1a5276` 1.8rem; `.subtitle` `#666` 1.05em; `.verified` inline-block badge — background `#eaf2f8`, border `1px solid #e0e0e0`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 `#1a5276` 1.3em with `border-bottom: 2px solid #2980b9`. `table.obj-table` full width, collapsed borders, td padding 16px (no cell borders on this page), first td 45%, last td 55%. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em, left-aligned. `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em. Links `#1a5276`. Canvas `display: block; margin: 16px auto 0; width: 100%`.
- **Palette:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange, bar/point fill rgba(26,82,118,0.35).
- **Canvas scaling:** uses `window.devicePixelRatio` — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, redraw on window resize.
- No nav bar, no back/home links. In regenerated HTML, any card/page links use `.html` extensions.
