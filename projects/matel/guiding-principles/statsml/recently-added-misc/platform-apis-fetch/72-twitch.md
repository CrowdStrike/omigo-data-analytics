# Twitch

**Page type:** detail page (h1 + subtitle + verified badge, then h2 sections; one two-column obj-table row: text left 45%, code sample + canvas right 55%)
**HTML title tag:** Twitch — Platform APIs

**Subtitle:** Lets you see what is live on Twitch right now and receive events (follows, subs, chat messages) as they happen — with no way to look back in time.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What you can get**

- Which channels are live right now, with current viewer counts
- Videos (VODs) and clips — while they still exist
- Real-time pushed events via EventSub: stream start/stop, follows, subs, channel-point redemptions and chat messages
- Detailed stats for your own channel, with the broadcaster's login

**Key point callout:** **There is no history.** The API returns snapshots of the present moment — no past viewer counts, no chat archive. Every "Twitch stats over time" chart you have seen was self-collected by someone polling and storing, so third-party stats sites are reconstructions that can legitimately disagree with each other.

**Watch out for**

- VODs expire after a retention period, and chat that was not recorded live is gone forever
- Most interesting data (followers, subscribers, redemptions) requires each broadcaster's permission — consent is per channel
- `viewer_count` is Twitch's own metric with an undisclosed definition — it is not a count of humans
- The old IRC chat interface is legacy; new chat integrations should use EventSub chat-message events

### Right column

**Get Streams — abridged response** (code block, `pre`):

```
GET /helix/streams?game_id=509658&first=1

{ "data": [ {
    "user_name": "ExampleCaster",
    "game_name": "Just Chatting",
    "type": "live",
    "title": "reviewing viewer submissions",
    "viewer_count": 8241,
    "started_at": "2026-08-22T14:05:31Z"
} ] }

// viewer_count is a single instantaneous value —
// nothing in the API carries viewership history
```

**Queryable on demand vs. must be captured live**

### Visualization (canvas `twitchAvailMatrix`, responsive width × 380)

Availability matrix: 12 data-class rows × 3 columns, with a small colored pill drawn in each applicable cell.

- **Title (bold 13px `#1a5276`, top left):** "Twitch data classes: what you can fetch vs. what you had to be recording"
- **Subtitle (italic 10px `#666`):** "placement reflects API surface, not measured volumes"
- **Columns (10px `#555`, two-line headers, centered):** "Queryable / on demand", "Live only — / you must persist", "Not available / via API"
- **Rows** (label, then vals array where 2 = pill in column 1, 1 = pill in that column, 0 = nothing drawn; `want: true` rows render the label bold red `#e74c3c`, others `#2c3e50` 11px, right-aligned):
  | Row label | vals | want |
  |---|---|---|
  | Current stream state | [2, 0, 0] | |
  | Current viewer_count | [2, 0, 0] | |
  | Concurrent viewers over time | [0, 1, 0] | yes |
  | VOD list (unexpired) | [2, 0, 0] | |
  | Clip list | [2, 0, 0] | |
  | Follower count | [2, 0, 0] | |
  | Follower list (own channel) | [2, 0, 0] | |
  | Subscriber list (own channel) | [2, 0, 0] | |
  | Channel point redemptions | [2, 1, 0] | |
  | Live chat messages | [0, 1, 0] | |
  | Chat history | [0, 1, 0] | yes |
  | Broadcaster insights export | [0, 0, 1] | |
- **Pills:** rectangles max 100px wide × 20px tall, centered in cell; color by column: col 1 `#27ae60` with white bold label "fetch", col 2 `#e67e22` label "persist", col 3 `#e74c3c` label "no". Alpha 0.85 (0.5 if val 1 in col 1); stroke `rgba(0,0,0,0.12)`.
- **Grid:** outer rect + column separators `#ddd` 1px; alternate rows shaded `rgba(26,82,118,0.04)`. Padding: top 56, right 20, bottom 52, left min(210, 36% of width).
- **Legend (bottom left, 10px):** green swatch "queryable on demand" (`#27ae60`), orange swatch "live only — persist it yourself" (`#e67e22`), red swatch "dashboard / owner only" (`#e74c3c`).
- **Footnote (italic 10px `#e74c3c`, bottom left):** "the two most-wanted series — concurrent viewers over time and chat history — exist only if you were already recording"

## Official API References

- [Twitch API (Helix)](https://dev.twitch.tv/docs/api/) — main documentation for the Helix REST API
- [EventSub](https://dev.twitch.tv/docs/eventsub/) — subscription types and the webhook and WebSocket transports

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, h2 "Overview" with a single-row two-column `table.obj-table` (left td 45% text, right td 55% code sample + canvas), then h2 "Official API References" with a link list.
- **Section heads inside cells:** `.section-head` — `#1a5276`, bold, 0.95em, 16px top margin (0 for first).
- **Page CSS:** body system sans-serif, `#2c3e50` text, white background, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; obj-table cells `1px solid #e0e0e0`, padding 16px; `pre` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 16px, radius 4px; `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `code` — background `#f4f4f4`, padding 2px 5px, radius 3px, 0.88em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `width: 100%` CSS, height attribute 380; redraw on window resize using `getBoundingClientRect().width`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- In regenerated HTML, any card/page links use `.html` extensions.
