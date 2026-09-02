# Smart Glasses

**Page type:** detail page (single two-column obj-table row: text left 45%, code snippet + canvas right 55%; followed by a references section)
**HTML title tag:** Smart Glasses — Platform APIs

**Subtitle:** The short answer: consumer smart glasses (Ray-Ban Meta, Snap Spectacles) offer no way for outside developers to fetch what the wearer captures.

**Verified badge:** Last verified: August 2026

## Left column

### What you can (and cannot) get

- No way to list, download or stream a wearer's photos, video or audio — on any consumer brand
- Ray-Ban Meta: captures sync to the wearer's own Meta account; that path is closed to third parties
- Snap Spectacles: your code can run on the device inside Snap's sandbox, but camera frames cannot be exported
- The only general route to the media: the wearer manually shares or exports it

**Key-point callout:** **The closure is deliberate, not an unfinished feature.** Camera glasses record bystanders who never consented, so vendors keep captures locked to the wearer's own account. Do not plan a project on the assumption "the glasses have a camera, so there must be a camera API" — there is not, and expecting one to appear is not a reasonable bet.

### Watch out for

- Meta's Wearable Device Access Toolkit exists but is partner-gated and narrow — not a general data API
- If you need first-person video at scale, you must recruit consenting participants or use your own hardware
- Any dataset assembled that way is a small volunteer sample, not population data

## Right column

### The call you want vs the surface that exists

Code block (`pre`, monospace):

```
# What you want — does not exist on any vendor:
GET /v1/me/glasses/captures          -> no such endpoint
GET /v1/me/glasses/captures/{id}     -> no such endpoint
POST /v1/webhooks {capture.created}  -> no such event

# What exists:
#   device -> companion app -> wearer's own account
#   (third-party position in this chain: none)
#
# The only general route:
#   wearer manually exports -> your consented
#   collection app -> your storage
```

### Capability availability by device

### Visualization (canvas `glassesAvailMatrix`, responsive width × 380)

Capability matrix (grid of colored cells): 6 rows × 4 columns, cell value 0 = not available (red), 1 = gated / partial (orange), 2 = available (green).

- **Columns (two-line headers, 10px, centered; first three in gray `#555`, last in purple `#8e44ad`):** "Ray-Ban / Meta", "Snap / Spectacles", "Audio / / display glasses", "Research-grade / headset (contrast)".
- **Rows (label right-aligned, 11px; API rows in red `#e74c3c`, others `#2c3e50`), with values `[Ray-Ban Meta, Snap Spectacles, Audio/display glasses, Research-grade headset]`:**
  - Enumerate captures via API — [0, 0, 0, 2] (api row)
  - Download media via API — [0, 0, 0, 2] (api row)
  - Live sensor stream to 3rd party — [0, 0, 0, 2] (api row)
  - On-device work in vendor sandbox — [1, 2, 0, 2]
  - Companion sync to vendor account — [2, 2, 1, 0]
  - User-initiated manual export — [2, 2, 1, 2]
- **Cell colors:** `#e74c3c` (no), `#e67e22` (gated), `#27ae60` (yes); cells at 0.85 alpha, max 96×22px, stroke `rgba(0,0,0,0.12)`, white bold 10px mark text "no" / "gated" / "yes".
- **Layout:** padding top 62, right 16, bottom 58, left min(190, 36% of width); zebra striping on odd rows `rgba(26,82,118,0.04)`; grid border `#ddd` with vertical separators between columns.
- **Title (bold 13px `#1a5276`, top left):** "Third-party data surfaces for consumer smart glasses".
- **Third-party API region highlight:** the top 3 rows × first 3 columns are shaded `rgba(231,76,60,0.06)` and bracketed with a dashed red rectangle (`#e74c3c`, dash 5/4, width 1.5); italic 9px red label just above it: "third-party API region — uniformly closed".
- **Legend (bottom left, swatches 11×11):** "available" (`#27ae60`), "gated / partner-only" (`#e67e22`), "not provided" (`#e74c3c`); labels gray `#666`.
- **Conclusion caption (bottom left, italic 10.5px red `#e74c3c`, two lines):** "Conclusion: no consumer vendor supplies capture data to third parties. Egocentric data at scale" / "requires your own hardware or your own consented collection — or a different research question."
- Redraws on window resize; width taken from `getBoundingClientRect()`.

## Official API References

- [Meta Wearables Device Access Toolkit](https://developers.meta.com/wearables/) — the gated developer-preview access path for Ray-Ban Meta glasses
- [Snap Developer Portal](https://developers.snap.com/) — Lens Studio and Spectacles developer documentation

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then a single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with `.section-head` headings + bullet lists + one `.key-point` callout; right `<td>` 55% with a `.section-head`, a `pre` code block, another `.section-head`, and the canvas. After the table, an `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline-block, background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; table cells `1px solid #ddd`, padding 16px; `.section-head` bold `#1a5276` 0.95em; `pre` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, padding 12px, radius 4px; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `li` 0.93em; links `#1a5276`; canvas `display:block`, `width:100%`, margin `16px auto 0`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`.
- **Canvas:** declared with `height="380"` attribute and `width:100%` CSS; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on resize.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
