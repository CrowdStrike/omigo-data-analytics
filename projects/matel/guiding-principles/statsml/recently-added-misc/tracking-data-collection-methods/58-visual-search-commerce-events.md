# Tracking Data: Visual Search Commerce Events

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Visual Search Commerce Events

**Subtitle:** On a visual discovery platform the thing being tracked is a picture, and the purchase — if it happens — happens somewhere else, months later.

## What is it?

People browse images, save the ones they like into boards, and sometimes follow a link out to a shop.

- **The unit is an image,** not a product — many images have no item for sale in them at all
- **Related images come from visual similarity,** so a stool, a tap and a dress can sit next to each other if they share a colour and material

**A save is not a purchase signal in the usual sense:** it is a bookmark for a kitchen, a wedding or a holiday that may be months away.

### Visualization (canvas `c1`, 720×320)

Radial neighbour graph: one opened image at the centre, seven visually similar neighbours around it. Distance is fixed (radius 108 from centre at (w/2, 168)); line thickness encodes similarity; one hue per merchandise category, because the point is that near neighbours belong to unrelated categories.

- **Title (bold 14px, ink `#1a5276`, top center):** "Nearest images by look, not by category".
- **Nodes (angle in degrees, similarity, category label — illustrative; hues assigned in series order blue `#2a78d6`, green `#008300`, violet `#4a3aa7`, orange `#d95926`, aqua `#199e70`, magenta `#d55181`, yellow `#c98500`):**
  - 0° — 0.92 — "faucet" (blue)
  - 51° — 0.61 — "mug" (green)
  - 103° — 0.44 — "tote bag" (violet)
  - 154° — 0.83 — "stool" (orange)
  - 206° — 0.71 — "dress" (aqua)
  - 257° — 0.35 — "paint" (magenta)
  - 309° — 0.55 — "lamp" (yellow)
- **Edges:** line from centre to each node in the node's hue; width = 0.8 + similarity × 3.6; alpha = 0.35 + similarity × 0.5.
- **Neighbour nodes:** circles radius 16 filled with the hue at tint 0.18, stroked in the hue (width 1.4), the similarity value (e.g. "0.92") in bold 12px inside; bold 12px category label outside each node at radius +36, clamped inside the canvas.
- **Centre node:** solid ink `#1a5276` circle radius 28 (the anchor, not a series hue), white bold 12px two-line label "opened" / "image".
- **Captions (bottom center):** 13px `#2c3e50` "Line thickness is similarity. The closest match is a tap; a dress ranks above paint."; italic 11px muted "Illustrative similarities."

## What does it collect?

- **Which image was opened,** how long it was held open, and which similar images were shown beside it
- **The board it was saved to** — including the name the user typed
- **The click out to the shop,** and how long ago the save was

**Board names are volunteered labels:** "Kitchen Reno 2027" states a project and a date more plainly than any inferred category. Being free text, they also carry names, places and personal circumstances, so they are not ordinary click-log fields.

**The platform sees the click, not the order:** what happens on the shop's site is visible only if the shop runs its own tag and reports back.

### Visualization (canvas `c2`, 720×320)

Cumulative brick chart: one board accumulating saves month after month, with the purchase marker only at the far end.

- **Title (bold 14px, ink `#1a5276`, top center):** `board: "Kitchen Reno 2027"`.
- **Data (saves added per month, illustrative, hardcoded):** Jan–Aug = `[2, 4, 1, 5, 3, 6, 2, 1]` (cumulative total 24).
- **Layout:** 8 month columns between x=64 and w−48, baseline at y=244 (ink line width 1.4 extending 10px past each side); each column stacks cumulative bricks 48×11 (2px vertical gap), one per save so far. Bricks new that month filled orange tint `rgba(217,89,38,0.85)`; bricks already on the board filled blue tint `rgba(42,120,214,0.4)`. Month labels 12px muted below the baseline.
- **Purchase marker (green `#008300`):** small downward-pointing arrow (stem plus filled triangle) above the August column, with bold 12px label "first purchase".
- **Legend (12px swatches, y≈44):** orange tint 0.85 "saved that month"; blue tint 0.4 "already on the board"; green "the buy, months later".
- **Captions (bottom center):** 13px `#2c3e50` "Every save is a real signal, but none of them is a purchase yet."; italic 11px muted "Illustrative save counts."

### Payload (under canvas `c2`)

Caption (italic, gray): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── the save ──
  "event_name": "pin_save",
  "event_ts": "2026-06-14T19:42:31.902Z",
  "user_id": "u_88231947",
  "pin_id": "p_612348901772",
  // a hash of the pixels: the same picture reposted
  // by someone else resolves to the same value
  "image_signature": "phash:e3f1a9c4d2b08756",
  "board_name": "Kitchen Reno 2027",   // typed by the user
  "board_is_secret": false,
  "dwell_ms": 11840,

  // ── the click out, 43 days later ──
  "event_name": "outbound_click",
  "event_ts": "2026-07-27T08:15:44.210Z",
  "pin_id": "p_612348901772",
  "product_id": "SKU-OAK-84-NAT",      // null for editorial images
  "has_merchant_tag": true,            // else the order is unobservable
  "days_since_save": 43,
  "attribution_window_days": 7         // shorter than the gap above
}
```

## Why is it collected?

**Stated purpose** (label pill)

- **Filling the feed** — showing more images like the ones already saved, and charging shops for placement

**Additional consequence** (label pill)

- Boards describe **plans that have not happened yet** — a move, a renovation, a wedding — often before they are announced anywhere
- Because similarity ignores categories, **an image can be recommended into a context its saver never had in mind**

**The standard measurement fails here:** a 7-day attribution window drops most of the save-to-buy gap, so the save gets credited with nothing and save-heavy surfaces look unproductive. Set the window from the observed gap, or treat saves as their own leading indicator.

### Visualization (canvas `c3`, 720×320)

Histogram of the save-to-click-out gap in days against a 7-day attribution window, with a proportion strip above. Four hues, four distinct things: credited mass (blue), uncredited mass (orange), the window as a chosen parameter (violet), and the single day-43 record shown in the payload (magenta). Red is not used — a save outside the window is a measurement gap, not an error.

- **Title (bold 14px, ink `#1a5276`, top center):** "How long after saving does the click-out happen?".
- **Data (counts per day, index = days between save and click out, 60 values, illustrative):** `[42, 78, 96, 88, 74, 65, 58, 52, 47, 43, 40, 37, 35, 33, 31, 29, 28, 26, 25, 24, 23, 22, 21, 20, 19, 19, 18, 17, 17, 16, 16, 15, 15, 14, 14, 13, 13, 12, 12, 12, 11, 11, 11, 10, 10, 10, 9, 9, 9, 9, 8, 8, 8, 8, 7, 7, 7, 7, 6, 6]`. The split is computed from this array, not asserted: total 1,450; first 7 days sum to 501 → 35% inside the window, 65% beyond.
- **Proportion strip (y=40, height 12, same width as the plot):** left segment (35% of width) solid blue `#2a78d6`, right segment orange tint `rgba(217,89,38,0.8)`. Labels 12px below: blue left-aligned "35% inside a 7-day window"; orange right-aligned "65% beyond it — credited to nothing".
- **Plot area:** x=62, y=96, 600×148; y scale 0–100 with gridlines in `#e5e9ef` and right-aligned muted labels every 25; axes stroked ink `#1a5276` width 1.4.
- **Bars:** one per day (width = 600/60 minus 0.8px gap); days 0–6 filled blue tint 0.85; days 7+ filled orange tint 0.55.
- **Window boundary (violet `#4a3aa7`):** dashed vertical line (dash 4/3, width 1.6) at day 7, with bold 12px label to its right above the plot: "7-day window ends here".
- **Day-43 marker (magenta `#d55181`):** solid vertical line from the baseline up 46px at day 43, bold 12px centered label "day 43" above it.
- **X labels (12px muted):** 0, 15, 30, 45, "60+"; axis caption below: "days between the save and the click out".
- **Caption (bottom center, italic 11px muted):** "Illustrative distribution — the long right tail is the point, not the exact counts."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` paragraph + bullets + `.key-point` callouts, right `<td>` (55%, text-align center) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption and `.payload` `<pre>` block (both left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; bullets 0.93em with bold lead terms (`li b`) in `#1a5276`; inline `code` monospace 0.9em on `#f4f6f7` background, 1px 4px padding, 2px radius. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em bold, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` italic 0.82em `#666` immediately above.
- **Canvas:** each declares intrinsic `width`/`height` attributes (720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own attributes. Include a `tint(hex, a)` helper producing an rgba tint of a palette token, and a `SERIES` array of the seven hues in rotation order.
- **Palette:** page charts use the tracking-set validated categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` for headings/axes, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is reserved for genuine alarm states and does not appear. Site-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
