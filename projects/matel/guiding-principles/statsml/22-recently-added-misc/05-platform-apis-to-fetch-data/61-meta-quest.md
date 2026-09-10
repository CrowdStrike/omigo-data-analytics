# Meta Quest

**Page type:** detail page (single two-column obj-table row: text left 45%, code snippet + canvas right 55%; followed by a references section)
**HTML title tag:** Meta Quest — Platform APIs

**Subtitle:** Lets a Quest VR app use Meta's servers for accounts, purchases, achievements and multiplayer — while everything the headset senses stays on the device.

**Verified badge:** Last verified: August 2026

## Left column

### What you can get

- Verify the signed-in user actually owns your app (anti-piracy check)
- Achievements, leaderboards and in-app purchases
- Cloud saves and multiplayer matchmaking
- Inside the running app only: room layout, hand tracking, eye and face signals
- Aggregate store stats (installs, revenue) for your own title

**Key-point callout:** **Headset sensing never reaches Meta's servers.** Room scans, hand movements and gaze are handed to the running app frame by frame and stored nowhere else. If your app does not save and upload them itself (with consent), they do not exist anywhere you can query.

### Watch out for

- Friend-list access has been repeatedly narrowed — do not build on a readable friend graph
- Eye and face data carry strict use restrictions: no advertising or profiling
- Sensing quality differs by headset model, so pooled data mixes device generations
- You only see your own app's users, never their behaviour elsewhere

## Right column

### The split in one glance

Code block (`pre`, monospace):

```
// Server-backed: check the user owns the app
Entitlements.IsUserEntitledToApplication()
  .OnComplete(msg => msg.IsError ? QuitToStore()
                                 : StartGame());

// On-device sensing has no server endpoint:
//   GET /users/{id}/scene_mesh   <- does not exist
//   GET /users/{id}/hand_joints  <- does not exist
//   GET /users/{id}/gaze         <- does not exist
```

### Server-queryable vs on-device only

### Visualization (canvas `questSplitMatrix`, responsive width × 380)

Capability matrix (grid of colored cells): 12 rows × 2 columns, cell value 0 = no (red), 1 = partial / conditional (orange), 2 = yes (green).

- **Columns (two-line headers, gray `#555`, 10px, centered):** "Server-queryable / (Meta-hosted)", "On-device only / (app process)".
- **Rows (label right-aligned, 11px; platform rows in `#2c3e50`, sensing rows in `#8e44ad`), with `[server, on-device]` values:**
  - Entitlement check — [2, 2] (platform)
  - Achievements — [2, 2] (platform)
  - Leaderboards — [2, 2] (platform)
  - IAP / purchase receipts — [2, 2] (platform)
  - Friends / presence — [1, 1] (platform)
  - Cloud saves — [2, 2] (platform)
  - Store analytics (aggregate) — [1, 0] (platform)
  - Room mesh + scene labels — [0, 2] (sensing)
  - Hand joint skeleton — [0, 2] (sensing)
  - Eye tracking signal — [0, 1] (sensing)
  - Face expression signal — [0, 1] (sensing)
  - Passthrough frames — [0, 1] (sensing)
- **Cell colors:** `#e74c3c` (no), `#e67e22` (cond), `#27ae60` (yes); cells drawn at 0.85 alpha, max 110×20px, stroke `rgba(0,0,0,0.12)`, white bold 10px mark text "no" / "cond" / "yes".
- **Layout:** padding top 56, right 20, bottom 46, left min(200, 38% of width); zebra striping on odd rows `rgba(26,82,118,0.04)`; grid border `#ddd` with a vertical divider between the two columns.
- **Title (bold 13px `#1a5276`, top left):** "Two disjoint surfaces: commerce and social are queryable, perception is not".
- **Platform/sensing boundary:** dashed purple line (`#8e44ad`, dash 5/4, width 1.5) above the first sensing row, with italic 9px purple label just above it: "sensing layer below".
- **Legend (bottom left, 11px swatches):** "available" (`#27ae60`), "scope-gated / aggregate only" (`#e67e22`), "not provided" (`#e74c3c`); labels gray `#666`.
- **Callout (bottom right, italic 10px red `#e74c3c`):** "everything socially or commercially interesting is queryable; everything perceptually interesting is not".
- Redraws on window resize; width taken from `getBoundingClientRect()`.

## Official API References

- [Meta Horizon OS Developer Portal](https://developers.meta.com/horizon/) — top-level developer hub for Quest / Horizon OS
- [Meta Horizon Documentation](https://developers.meta.com/horizon/documentation/) — Platform SDK (entitlements, achievements, leaderboards, IAP) and Mixed Reality / tracking APIs

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then a single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with `.section-head` headings + bullet lists + one `.key-point` callout; right `<td>` 55% with a `.section-head`, a `pre` code block, another `.section-head`, and the canvas. After the table, an `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline-block, background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; table cells `1px solid #ddd`, padding 16px; `.section-head` bold `#1a5276` 0.95em; `pre` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 12px, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `li` 0.93em; links `#1a5276`; canvas `display:block`, `width:100%`, margin `16px auto 0`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`.
- **Canvas:** declared with `height="380"` attribute and `width:100%` CSS; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
