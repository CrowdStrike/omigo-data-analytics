# Apple Vision Pro

**Page type:** detail page (single two-column obj-table row: text left 45%, code snippet + canvas right 55%; followed by a references section)
**HTML title tag:** Apple Vision Pro — Platform APIs

**Subtitle:** Lets a Vision Pro app understand the room around it — walls, furniture, hand positions — entirely on the device; where the user's eyes look is deliberately kept private.

**Verified badge:** Last verified: August 2026

## Left column

### What you can get

- A 3D mesh of the surrounding room, with labeled surfaces (floor, wall, table, ceiling)
- Hand and finger positions for gesture input
- Headset position, plus anchors that stay in place across sessions
- What the user selected — the system turns look-and-pinch into an ordinary tap event

**Key-point callout:** **Apps never see where the user's eyes are looking.** The system resolves gaze plus a pinch into a normal "this was selected" event; the eye path itself never reaches app code. This is deliberate: gaze is a direct readout of attention, so plan any analytics around explicit selections, not eye movements.

### Watch out for

- Consumer App Store apps get no main-camera pixels; since visionOS 2, Apple's Enterprise APIs (managed entitlements) grant main-camera access to business-distributed apps only
- Nothing is stored on Apple's servers — there is no endpoint to query anyone's room or hand data later
- Most sensing works only when the app runs in a full immersive space
- If your app does not record a signal itself, it is gone — there is no backfill

## Right column

### Hand tracking in a few lines — Swift

Code block (`pre`, monospace):

```
let session = ARKitSession()
let hands = HandTrackingProvider()

let auth = await session.requestAuthorization(
    for: [.handTracking])
try await session.run([hands])

for await update in hands.anchorUpdates {
    // per-frame hand joints, on-device only
}

// No gaze API exists:
//   session.gazeProvider   <- does not exist
// Look + pinch arrives as an ordinary tap.
```

### On-device stream vs. queryable API

### Visualization (canvas `matrixChart`, responsive width × 380)

Capability matrix (grid of colored cells): 8 rows × 3 columns, cell value 0 = not available (red), 1 = conditional (orange), 2 = available (green).

- **Columns (two-line headers, gray `#555`, 10px, centered):** "Available / to app", "Needs immersive / space", "Queryable / cloud API".
- **Rows (label right-aligned, 11px, `#2c3e50`; the gaze row in red `#e74c3c`), with `[to app, immersive, cloud]` values:**
  - Scene mesh (room geometry) — [2, 1, 0]
  - Plane detection + class — [2, 1, 0]
  - Hand joint skeleton — [2, 1, 0]
  - Head / device pose — [2, 1, 0]
  - Image anchor pose — [2, 1, 0]
  - Camera pixel frames — [1, 1, 0]
  - Resolved selection event — [2, 0, 0]
  - Raw gaze ray / fixations — [0, 0, 0]
- **Cell colors:** `#e74c3c` (no), `#e67e22` (cond), `#27ae60` (yes); cells at 0.85 alpha, max 96×22px, stroke `rgba(0,0,0,0.12)`, white bold 10px mark text "no" / "cond" / "yes".
- **Layout:** padding top 54, right 20, bottom 46, left min(210, 36% of width); zebra striping on odd rows `rgba(26,82,118,0.04)`; grid border `#ddd` with vertical separators between columns.
- **Title (bold 13px `#1a5276`, top left):** "visionOS spatial data: reachable on-device, absent server-side".
- **Legend (bottom, starting at left grid edge, swatches 11×11):** "available" (`#27ae60`), "conditional / partial" (`#e67e22`), "not provided" (`#e74c3c`); labels gray `#666`.
- **Callout (bottom right, italic 10px red `#e74c3c`):** "entire right column is empty: no server-side spatial dataset exists".
- Redraws on window resize; width taken from `getBoundingClientRect()`.

## Official API References

- [visionOS Developer Overview](https://developer.apple.com/visionos/) — top-level visionOS developer hub
- [ARKit Documentation](https://developer.apple.com/documentation/arkit) — ARKitSession, data providers, anchors, hand tracking

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then a single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with `.section-head` headings + bullet lists + one `.key-point` callout; right `<td>` 55% with a `.section-head`, a `pre` code block, another `.section-head`, and the canvas. After the table, an `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline-block, background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; table cells `1px solid #ddd`, padding 16px; `.section-head` bold `#1a5276` 0.95em; `pre` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 12px, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `li` 0.93em; links `#1a5276`; canvas `display:block`, `width:100%`, margin `16px auto 0`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** declared with `height="380"` attribute and `width:100%` CSS; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
