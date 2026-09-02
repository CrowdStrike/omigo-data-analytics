# Tracking Data: Clipboard Access

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Clipboard Access

**Subtitle:** An app can read the system clipboard when it comes to the foreground. What it receives is the current item and nothing else — no record of who put it there, or when.

## Section 1: What is it?

A shared system buffer, read with an ordinary API call.

- **Shared by design** — copied text sits there so it can be pasted into a different app
- **Not permission-gated** — reading it is a normal API call
- **Foreground trigger:** an app can read whenever it becomes active, not only on a paste gesture
- **iOS 14 banner** notifies when an app reads the clipboard

**The reads were not new, the instrument was:** before the banner there was no observable signal, so the read rate was unmeasured — not zero. Any claim about how it changed afterwards compares a period with no instrument to a period with one: a change in observation, not necessarily in behaviour.

### Visualization (canvas `c1`, 720×330)

Hub-and-spoke diagram: one shared clipboard in the center, four apps reading it.

- **Title (bold 16px, ink `#1a5276`, centered, y=24):** "One shared clipboard — any app can read it on foreground".
- **Center clipboard:** rounded rect 124×116 (radius 10) centered at (w/2, h/2+8), fill `#f4f6f8`, stroke ink `#1a5276` 2.5px, with a small filled ink clip tab (36×15, radius 4) at the top. Inside: muted 15px label "current item"; below it bold 17px monospace `#2c3e50`: "4532 •••• 1234"; a thin grid-gray divider line; then two magenta (`#d55181`) 15px lines: "no source app" and "no copy time".
- **Apps:** four rounded rects 68×48 (radius 10), solid-filled in their own hue with white bold 17px labels — App A blue `#2a78d6` (upper left), App B green `#008300` (lower left), App C violet `#4a3aa7` (upper right), App D orange `#d95926` (lower right), positioned ±235px horizontally and ±62px vertically from center. Under each, muted 15px caption "reads on open".
- **Arrows:** dashed (6/5) 2px quadratic curves in each app's hue from the clipboard toward each app, with solid triangular arrowheads pointing at the app.
- **Caption (17px, muted, centered, h−14):** "The read returns the item, not its provenance".

## Section 2: What does it collect?

- **Whatever string** is currently on the clipboard
- **Credentials**, if a password manager copied one and it has not expired
- **Addresses, phone numbers, card numbers** copied from another app
- **Text fragments** moved between apps, including message and email excerpts
- **URLs** and query strings
- **Images and files** — the clipboard is typed, so on most platforms a copied screenshot or file reference is readable the same way as text
- **Read time** and which app was in the foreground
- **Sampling:** one snapshot per foreground event, not a running log — a value copied and replaced between app opens is never seen

**Two nulls:** the clipboard hands over a string with no provenance. The reading app gets the text, not which app produced it or how long it sat there, so a value copied yesterday is indistinguishable from one copied a second ago. Origin is inference.

**`pattern_guess` is a guess:** a match on shape, so a tracking number of the right length lands in the same bucket as a card number — recorded as though observed.

### Visualization (canvas `c2`, 720×340)

Five-row table chart: pattern match vs what the string actually is.

- **Title (bold 17px, ink `#1a5276`, centered, y=26):** "Pattern match ≠ what the string is".
- **Column headers (bold 16px, muted, left-aligned, y=54, underlined by a grid-gray rule at y=62):** "OBSERVED STRING" (x=34), "STORED LABEL" (x=330), "NOT RULED OUT" (x=470).
- **Rows** (rounded row cards w−60 wide, 38px tall, starting y=74, 46px pitch; alternating fills `#f7f9fc` / `#ffffff`; a 5px hue tab on the left edge; observed string in 16px monospace `#2c3e50` at x=46; dashed hue arrow (4/3) from x=288 to x=324; stored guess in bold 17px monospace in the row hue at x=330; alternative in 16px muted at x=470):
  1. `4532 8821 •••• ••••` → `card_number` — "could also be an order ref" (blue `#2a78d6`)
  2. `1Z999AA10123456784` → `tracking_id` — "could also be a serial" (green `#008300`)
  3. `742 Evergreen Terrace` → `address` — "could also be a venue name" (violet `#4a3aa7`)
  4. `https://…/reset?t=…` → `url` — "may carry a one-time token" (orange `#d95926`)
  5. `+1 (555) 234-5678` → `phone` — "could also be a case number" (aqua `#199e70`)
- **Captions (17px, muted, centered):** at h−26: "Only the left column is measured." At h−8: "The middle column is a guess stored under a confident name."

### Payload (below canvas c2)

Payload note (italic, above the block): "Sample payload — illustrative structure, not real captured data."

```
// Clipboard reads leave no published record. The
// OS-level API returns only the item; everything an
// app stores about it is its own choice. Reconstruction.
{
  // ── inferred / plausible ──
  "app_session":   "sess_3ac9…",
  "read_ts":       "2026-08-22T08:12:44Z",
  "trigger":       "app_foreground",
  "types":         ["text/plain"],
  "length_chars":  19,
  "sample":        "…",          // content, if retained
  "pattern_guess": "card_number", // regex on the string
  "source_app":    null,          // OS does not say who copied it
  "copied_at":     null           // nor when
}
```

## Section 3: Why is it collected?

**Stated purpose** (label pill)

- **Paste assistance** — recognise a URL, coupon code, or tracking number already copied and offer to use it
- **Why on foreground** — the offer has to be ready before the user asks, so the read cannot wait for a paste gesture

**Additional consequence** (label pill)

- The clipboard becomes a **join key** — a string synced across devices is identical on both, so an app seeing it twice has a match with no identifier supplied
- The match is **on the text**, not on the person

**Wrong merges do not fix themselves:** identical clipboard text usually means one account, but a code copied from a group chat lands on several clipboards at once, and a handed-over phone gives the same string for two people. Once two histories are welded together they look like one consistent history.

### Visualization (canvas `c3`, 720×330)

Timeline tick chart: a merge on shared clipboard text — three people's activity lands under one identity. Illustrative session times over one week, by hour-of-week index (0–167); all three copied the same promo code out of one group chat.

- **Title (bold 14px, ink `#1a5276`, centered, y=26):** "One shared code, three people, one profile". **Subtitle (12px, muted, y=45):** "app sessions over one week, joined on identical clipboard text".
- **Layout:** left pad 96, right pad 96, rows start y=66, 34px row gap; x maps hour index 0–167 across the plot width. Each person row: a thin grid-gray (`#e5e9ef`) baseline with 2.5px vertical tick marks (±8px) in the person's hue at each session time; label right-aligned left of the row (bold 12px, hue); session count left-aligned right of the row (12px, hue).
  - **person 1** (blue `#2a78d6`): ticks at `[9, 33, 58, 81, 106, 130]` — "6 sessions"
  - **person 2** (aqua `#199e70`): ticks at `[14, 20, 44, 68, 92, 96, 118, 141, 160]` — "9 sessions"
  - **person 3** (violet `#4a3aa7`): ticks at `[4, 27, 51, 62, 74, 88, 111, 124, 137, 150, 165]` — "11 sessions"
- **Merged row** (22px below the person rows): background band in translucent orange (`#d95926` at alpha 0.10) spanning the full row; all 26 tick times combined and sorted, drawn as orange ticks (±9px); left label bold 12px orange "one profile" with 11px "after the join" beneath; right label bold "26 sessions".
- **Captions (centered):** italic 12px `#2c3e50` at h−26: "The bottom row has no gaps, so it reads as one steady user rather than a fault." Italic 11px muted at h−9: "Illustrative session times — the pattern, not measured activity."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Include a rounded-rect path helper and a `tint(hex, alpha)` helper for translucent fills.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Project-wide palette reference: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
