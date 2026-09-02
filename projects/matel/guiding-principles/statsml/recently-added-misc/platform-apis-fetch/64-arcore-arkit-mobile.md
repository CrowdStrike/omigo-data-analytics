# ARCore & ARKit Mobile

**Page type:** detail page (single two-column obj-table row: text left 45%, code snippet + canvas right 55%; followed by a references section)
**HTML title tag:** ARCore & ARKit Mobile — Platform APIs

**Subtitle:** Lets phone apps understand the world through the camera — surfaces, depth, faces, body pose — computed live on the device and never stored on a server.

**Verified badge:** Last verified: August 2026

## Left column

### What you can get

- Where the phone is and how it is moving through the room
- Flat surfaces (floors, walls, tables) and per-pixel depth, for placing virtual objects convincingly
- A face mesh with ~52 facial-expression signals (iPhone), and a 3D body skeleton
- A room mesh with labeled surfaces on LiDAR-equipped iPhones and iPads
- Precise global position by matching the camera view against Google's Street View imagery (ARCore Geospatial)

**Key-point callout:** **Everything is computed per frame on the device and then discarded.** There is no server to query for anyone's face data, room scans or movement history — the one networked piece is location lookup, and it returns a position, not a history. Whatever your app does not record itself does not exist anywhere.

### Watch out for

- Capability varies wildly by device (LiDAR on only some iPhones; Android depth quality is all over the map) — pooled data is a mixture of device classes
- Tracking drops out in poor light, on blank surfaces and under fast motion, so gaps in the data are not random
- Face and body signals are biometric-adjacent — platform policy limits what you may keep and share

## Right column

### Per-frame, on-device — and no server counterpart

Code block (`pre`, monospace):

```
// iPhone face tracking, once per frame:
let bs = faceAnchor.blendShapes   // ~52 expression values
// gone next frame unless YOUR app records it

// No server counterpart exists on either platform:
//   GET /users/{id}/faceHistory    -> no such API
//   GET /users/{id}/bodySkeleton   -> no such API
//   GET /devices/{id}/sceneMesh    -> no such API

// The only networked call is location lookup,
// and it returns a position, not a history.
```

### Signal availability by device class, and the boxed server column

### Visualization (canvas `arFidelityChart`, responsive width × 380)

Capability matrix (grid of colored cells): 10 rows × 4 device-class columns plus a separately boxed "queryable server API" column. Cell value 0 = absent (red), 1 = degraded / partial (orange), 2 = available (green). Qualitative capability map, not measured accuracy.

- **Device columns (two-line headers, gray `#555`, 10px, centered):** "ARCore / typical", "ARCore / + depth sensor", "ARKit / no LiDAR", "ARKit / LiDAR".
- **Server column header (bold 10px purple `#8e44ad`, two lines):** "queryable" / "server API". The server column is horizontally separated from the device grid by an 18px gap and boxed with a `#8e44ad` 1.5px border.
- **Rows (label right-aligned, 11px, `#2c3e50`; the VPS row label in purple `#8e44ad`), values `[ARCore typical, ARCore + depth, ARKit no LiDAR, ARKit LiDAR]` plus server value:**
  - Device pose (6-DoF) — [2, 2, 2, 2], server 0
  - Plane detection — [2, 2, 2, 2], server 0
  - Feature point cloud — [2, 2, 2, 2], server 0
  - Depth map — [1, 2, 1, 2], server 0
  - Scene mesh (classified) — [0, 0, 0, 2], server 0
  - Face mesh — [2, 2, 2, 2], server 0
  - ~52 face blendshapes — [0, 0, 2, 2], server 0
  - Body joint skeleton — [0, 0, 1, 2], server 0
  - Light estimate (HDR) — [2, 2, 2, 2], server 0
  - Global VPS pose — [2, 2, 0, 0], server 2
- **Cell colors:** `#e74c3c` (no), `#e67e22` (part), `#27ae60` (yes); device cells at 0.85 alpha, max 82×20px, stroke `rgba(0,0,0,0.12)`, white bold 10px mark text "no" / "part" / "yes". Server-column cells max 58×20px, marked "pose" for the VPS row (green) and "no" (red) elsewhere.
- **Layout:** padding top 62, right 14, bottom 56, left min(180, 30% of width); server column width clamped 52–72px (~17% of grid); zebra striping on odd rows `rgba(26,82,118,0.04)` across both the device grid and server column; device grid border `#ddd` with vertical column separators.
- **Title (bold 13px `#1a5276`, top left):** "On-device fidelity varies by hardware. Server-side access does not vary."
- **Qualitative banner (italic 10px orange `#e67e22`, below title):** "QUALITATIVE CAPABILITY MAP — availability classes, not measured accuracy".
- **Legend (bottom left, swatches 11×11):** "available" (`#27ae60`), "degraded / partial" (`#e67e22`), "absent" (`#e74c3c`); labels gray `#666`.
- **Callouts (bottom right, italic 10px):** purple `#8e44ad`: "the boxed column is the point: only VPS / cloud-anchor localization has a server side, and it returns a pose"; below it in red `#e74c3c`: "every other row exists only inside the app process, for one frame".
- Redraws on window resize; width taken from `getBoundingClientRect()`.

## Official API References

- [ARCore Documentation Home](https://developers.google.com/ar) — top-level ARCore developer docs
- [ARKit Documentation](https://developer.apple.com/documentation/arkit) — ARFaceAnchor, ARBodyAnchor, scene reconstruction, ARWorldMap

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then a single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with `.section-head` headings + bullet lists + one `.key-point` callout; right `<td>` 55% with a `.section-head`, a `pre` code block, another `.section-head`, and the canvas. After the table, an `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline-block, background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; table cells `1px solid #ddd`, padding 16px; `.section-head` bold `#1a5276` 0.95em; `pre` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 12px, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `li` 0.93em; links `#1a5276`; canvas `display:block`, `width:100%`, margin `16px auto 0`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`.
- **Canvas:** declared with `height="380"` attribute and `width:100%` CSS; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
