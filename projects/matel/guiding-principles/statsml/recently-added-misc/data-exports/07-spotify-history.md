# Spotify Extended Streaming History

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload sample + canvas right 55%)
**HTML title tag:** Spotify Extended Streaming History

**Subtitle:** Full lifetime listening data via GDPR/privacy export

**Verified line:** Last verified: August 2026

## How to Request

**Key-point callout:** Privacy Settings → "Request your data" → select Extended streaming history. Two tiers available:

- **Basic download (5 days):** Last 12 months of listening history, limited fields
- **Extended download (30 days):** Lifetime history with full detail including platform, IP, skip/shuffle state

## What's Included

- Track name, artist name, album name
- `ms_played` — milliseconds played per stream
- `ts` — UTC timestamp of stream end
- Platform (iOS, Android, web_player, Windows, macOS)
- Shuffle state (true/false)
- Repeat state (off, context, track)
- Skip flag — whether user skipped before track ended
- Offline flag — whether played from cache
- IP address at time of play
- Spotify track URI and episode URI (for podcasts)
- Reason start / reason end (e.g., trackdone, fwdbtn, clickrow)

## What's Missing

**Missing callout (red-bordered):**

- Recommendation engine weights (why this track was suggested)
- Taste profile vectors (internal genre/mood embeddings)
- Skip prediction model scores
- Playlist ranking signals
- Social listening context (who shared what)

## Delivery

**Key-point callout:** Delivered as JSON files split into chunks (~5MB each). Extended history can span 10+ files for heavy listeners. Email notification when ready.

## Right column: payload sample

**Payload note (italic):** Sample record from extended streaming history JSON export:

**Payload block (monospace, verbatim):**

```
{
  "ts": "2024-03-15T22:41:03Z",
  "username": "user_abc123",
  "platform": "iOS 17.3 (iPhone15,2)",
  "ms_played": 214520,
  "conn_country": "US",
  "ip_addr_decrypted": "73.162.xx.xx",
  "user_agent_decrypted": null,
  "master_metadata_track_name": "Bohemian Rhapsody",
  "master_metadata_album_artist_name": "Queen",
  "master_metadata_album_album_name": "A Night at the Opera",
  "spotify_track_uri": "spotify:track:4u7EnebtmKWzUH433cf5Qv",
  "episode_name": null,
  "episode_show_name": null,
  "spotify_episode_uri": null,
  "reason_start": "clickrow",
  "reason_end": "trackdone",
  "shuffle": false,
  "skipped": false,
  "offline": false,
  "offline_timestamp": 0,
  "incognito_mode": false

  // --- NOT included in export (internal only) ---
  // "recommendation_score": 0.87,
  // "taste_vector": [0.23, -0.41, 0.67, ...],
  // "skip_probability": 0.12,
  // "playlist_rank_signal": 3,
  // "discovery_source": "release_radar_model_v4"
}
```

### Visualization (canvas `heatmap`, 100% width × 340px CSS height)

Heatmap: listening intensity by day of week (rows) × hour of day (columns).

- **Title (bold 13px, `#1a5276`, top center):** "Listening Heatmap: Day of Week × Hour of Day".
- **Grid:** 7 rows (Mon–Sun) × 24 columns (hours 0–23), cell rectangles with 1px gap; margins top 30, right 20, bottom 35, left 45.
- **Data (intensity 0–10 scale, rows Mon–Sun, cols hours 0–23):**
  - Mon: `[1, 0, 0, 0, 0, 0, 1, 2, 3, 2, 2, 2, 3, 2, 2, 2, 3, 4, 5, 6, 7, 6, 4, 2]`
  - Tue: `[1, 0, 0, 0, 0, 0, 1, 2, 3, 2, 2, 2, 2, 2, 2, 2, 3, 4, 5, 7, 7, 5, 4, 2]`
  - Wed: `[1, 0, 0, 0, 0, 0, 1, 2, 3, 2, 2, 2, 3, 2, 2, 3, 3, 5, 6, 7, 8, 6, 4, 2]`
  - Thu: `[1, 0, 0, 0, 0, 0, 1, 2, 3, 2, 2, 2, 2, 2, 2, 2, 3, 4, 5, 6, 7, 6, 5, 3]`
  - Fri: `[2, 1, 0, 0, 0, 0, 1, 2, 3, 2, 2, 2, 2, 2, 2, 3, 4, 5, 6, 7, 8, 8, 7, 5]`
  - Sat: `[3, 2, 1, 0, 0, 0, 0, 1, 2, 3, 4, 5, 5, 4, 4, 5, 6, 7, 8, 9, 10, 9, 7, 5]`
  - Sun: `[2, 1, 1, 0, 0, 0, 0, 1, 2, 3, 4, 5, 5, 5, 5, 5, 6, 7, 8, 8, 9, 8, 6, 3]`
- **Color scale:** linear interpolation from light `#e8f4fd` (rgb 232,244,253 at intensity 0) to dark `#1a5276` (rgb 26,82,118 at intensity 10).
- **Y-axis labels:** day abbreviations Mon–Sun, right-aligned in `#2c3e50` 11px, left of grid.
- **X-axis labels:** hours every 3 ("0:00", "3:00", … "21:00"), centered below grid.
- **Legend:** 120×8px horizontal gradient bar (same light-to-dark ramp) at bottom left, labels "Low" (left) and "High" (right) in `#666` 9px.
- **Caption (below canvas, 0.8em `#666`):** "Listening intensity: hours of day vs. day of week"

## Regeneration instructions

- **Layout:** single `.obj-table` (full width, collapsed borders) with one `<tr>`: left `<td>` (45%) holds `.obj-title` headings, bullet lists, `.key-point` and `.missing` callouts; right `<td>` (55%, text-align center) holds `.payload-note` + `.payload` code block, the canvas, and its caption paragraph.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.85em `#888`; `.obj-title` bold `#1a5276` 1.1em; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px. `.missing` — background `#fdf2f2`, left border `3px solid #e74c3c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace (ui-monospace/Menlo) 0.78em, white-space pre, left-aligned; `.payload-note` 0.82em `#666` italic left-aligned.
- **Canvas:** styled `width:100%; height:340px`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; heatmap ramp `#e8f4fd` → `#1a5276`. No nav bar, no back/home links; in regenerated HTML any card links use `.html` extensions.
