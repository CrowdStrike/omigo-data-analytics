# Discord Messages API

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, one Overview row; second h2 section with reference links)
**HTML title tag:** Discord Messages API — Platform APIs

**Subtitle:** Read and send messages in Discord servers your bot has joined — live as they happen, or back through channel history.

**Verified badge:** Last verified: August 2026

## Overview

### Left column

**What you can get**

- Messages, threads, reactions and attachments from servers the bot has joined
- A live stream of new, edited and deleted messages
- Channel history, fetched in pages of up to 100 messages
- Member lists and channel details

**Key-point callout:** **Reading message text is a special permission, not a default.** A bot needs the "message content" privilege to see what people actually wrote, and once it grows past 100 servers Discord must verify and approve it to keep that privilege. Without it, message bodies come back empty — only who posted, where and when.

**Watch out for**

- There is no general way to read a user's private DM history through the REST API
- Bulk delete only works on messages under 2 weeks old, 2-100 at a time
- Ephemeral (temporary) messages are never stored and cannot be fetched later
- Thread messages live in their own channel — fetching the parent channel misses them

### Right column

**Section title:** A message as the API returns it

Code block (pre, JSON):

```
{
  "id": "1234567890123456789",
  "content": "Hello everyone! Meeting at 3pm.",
  "channel_id": "9876543210987654321",
  "author": { "id": "1111...", "username": "data_engineer" },
  "timestamp": "2026-08-20T15:00:00.000000+00:00",
  "edited_timestamp": null,
  "reactions": [
    {"emoji": {"name": "👍"}, "count": 3}
  ]
}
```

**Section title:** Gateway WebSocket Connection Lifecycle

### Visualization (canvas `gatewayCanvas`, responsive width × 420)

Sequence diagram between two lifelines — "Client" (left, x=90) and "Discord Gateway" (right, x=width−90) — showing the gateway connection lifecycle as labeled horizontal arrows.

- **Background:** light `#f8f9fa` fill over the whole canvas.
- **Lifelines:** solid blue `#1a5276` header boxes at top (60px wide "Client", 120px wide "Discord Gateway", white bold 11px labels), with vertical dashed gray `#bbb` lifelines (dash 4/4, width 1.5) down to near the bottom.
- **Sequence steps** (arrows spaced 52px apart starting at y=92; arrowheads 7px; labels centered above each arrow, 11px, in the arrow color):
  1. Client → Gateway, blue `#1a5276`: "1. Connect (WSS)"
  2. Gateway → Client, green `#27ae60`: "2. Hello (heartbeat_interval: 41250ms)"
  3. Client → Gateway, blue `#1a5276`: "3. Identify (token + intents bitmask)"
  4. Gateway → Client, green `#27ae60`: "4. Ready (session_id, guilds[])"
  5. Bidirectional pair (two arrows 10px apart), orange `#e67e22`: "5. Heartbeat / ACK (ongoing)"
  6. Gateway → Client, green `#27ae60`: "6. Dispatch: MESSAGE_CREATE"
  7. Gateway → Client, red `#e74c3c`: "7. Reconnect / Invalid Session"
- **Legend (bottom left, 12×10 swatches, 10px labels in `#2c3e50`):** blue "Client request"; green "Success response"; orange "Heartbeat"; red "Error / reconnect".
- Canvas redraws on window resize.

## Official API References

- [Discord Developer Portal — docs intro](https://discord.com/developers/docs/intro) — top-level entry point for the Discord API documentation
- [Gateway](https://discord.com/developers/docs/topics/gateway) — WebSocket connection lifecycle, intents, heartbeating, sharding

## Regeneration instructions

- **Layout:** single detail page: h1, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview" with a full-width `.obj-table` (one `<tr>`; left `<td>` 45% text, right `<td>` 55% code + canvas), then `h2` "Official API References" with a plain `<ul>` of links. No nav bar, no back/home links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`; obj-table cells padding 16px, top-aligned (no cell borders); `.section-title` bold `#1a5276` 1.05em; li 0.93em; links `#1a5276`.
- **Code block:** `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.82em, left-aligned, horizontal overflow scroll.
- **Callout:** `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em.
- **Canvas:** `display:block; width:100%`, `height` attribute 420; sized from `getBoundingClientRect().width` (fallback 680), scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, light background `#f8f9fa`, gray `#bbb`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
