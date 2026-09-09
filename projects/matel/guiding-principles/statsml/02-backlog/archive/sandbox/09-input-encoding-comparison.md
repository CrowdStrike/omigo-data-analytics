# CNN Input Encoding — Visual Comparison

**Page type:** other (single-page comparison doc: intro + legend callout, four shape rows each with a 4-column grid of paired zoomed/actual canvases, closing analysis table)
**HTML title tag:** CNN Input Encoding — Visual Comparison

Comparing 4 approaches to encoding histogram + band information in a 64×64 image for CNN input. Each shown at actual size (64px) and zoomed 3× for clarity.

## Legend callout

**Shapes shown:** Bell (Normal), Right Skew (Right_skew), Bimodal (Mixture of 2 Normals)
**Band = per-bucket standard deviation** — narrow where values are tightly packed, wide where values are spread out within the bucket.

## Shape row: Bell (Normal Distribution)

Four cells, each with a zoomed canvas (192×192 CSS) above an actual-size canvas (64×64 CSS):

- **Option A: RGB** (canvases `bell-a-z`, `bell-a`) — caption: "R=bars, G=band, B=0"
- **Option B: 2-Channel** (canvases `bell-b-z`, `bell-b`) — caption: "Left half=bars, Right half=std / (shown combined for viz)"
- **Option C: RGB Visual** (canvases `bell-c-z`, `bell-c`) — caption: "Blue bars + Orange band"
- **Your Choice: Grayscale** (canvases `bell-d-z`, `bell-d`) — caption: "Bright=bars, Mid-gray=band, Black=bg"

## Shape row: Right Skew (Right_skew)

Same four cells (canvases `rskew-a-z`/`rskew-a`, `rskew-b-z`/`rskew-b`, `rskew-c-z`/`rskew-c`, `rskew-d-z`/`rskew-d`) with captions:

- **Option A: RGB** — "R=bars, G=band, B=0"
- **Option B: 2-Channel** — "Left half=bars, Right half=std"
- **Option C: RGB Visual** — "Blue bars + Orange band"
- **Your Choice: Grayscale** — "Bright=bars, Mid-gray=band, Black=bg"

## Shape row: Bimodal (Mixture of 2 Normals)

Same four cells (canvases `bimodal-a-z`/`bimodal-a`, `bimodal-b-z`/`bimodal-b`, `bimodal-c-z`/`bimodal-c`, `bimodal-d-z`/`bimodal-d`) with the same four captions as the Right Skew row.

## Shape row: Spike (Point Mass + Tail)

Same four cells (canvases `spike-a-z`/`spike-a`, `spike-b-z`/`spike-b`, `spike-c-z`/`spike-c`, `spike-d-z`/`spike-d`) with the same four captions as the Right Skew row.

### Visualization (all canvases, 64×64 actual / 192×192 zoomed)

All charts are procedurally generated 64×64 pixel histograms rendered from seeded synthetic data, then pixel-doubled 3× into the paired zoomed canvas with `imageSmoothingEnabled = false` (nearest-neighbor). Every canvas has background fill `#fafcfe`, border `1px solid #333`, and CSS `image-rendering: pixelated`.

- **Data generation** (seeded mulberry32 PRNG + Box-Muller normal `randn()`, n=1000 per shape):
  - Bell (seed 42): `randn() * 10 + 50`
  - Right Skew (seed 77): `exp(randn() * 0.7 + 2.5)` (lognormal)
  - Bimodal (seed 123): half `randn() * 5 + 30`, half `randn() * 5 + 65`
  - Spike (seed 55): 70% at `40 + randn() * 0.5`, 30% at `40 + |randn() * 20|`
- **Binning:** 32 bins over [min, max]; per-bin count and per-bin sample standard deviation (0 if fewer than 2 values). Bar height = `round((count/maxCount) * 58)` px from the bottom edge; band height derived from `std/maxStd` (roughly 10–12 px max), drawn at the top of each bar with double thickness (`bandH * 2`).
- **Option A (RGB):** bar drawn in pure red channel `rgb(r,0,0)` where r = 255 × count/maxCount; band drawn above bar top in pure green channel `rgb(0,g,0)` where g = 255 × std/maxStd.
- **Option B (2-Channel, visualized combined):** bars as grayscale columns `rgb(i,i,i)` with i = 255 × count/maxCount; std channel overlaid as translucent cyan `rgba(0, 200, 200, 0.7)` band at bar tops.
- **Option C (RGB Visual):** solid blue bars `rgb(40, 100, 180)`; orange band `rgba(230, 126, 34, 0.8)` at bar tops.
- **Option D (Grayscale, user's choice):** first pass draws band in mid-gray with intensity `80 + 60 × std/maxStd`; second pass draws bars in near-white `rgb(230, 230, 230)`.

## Analysis

| Option | Channels | What CNN Sees | Pros | Cons |
|---|---|---|---|---|
| **A: RGB** | 3 | R=bar intensity, G=band intensity, B=unused | Clean channel separation | Wasted channel, CNN must learn R≠G |
| **B: 2-Channel** | 2 | Ch1=bars grayscale, Ch2=std grayscale | Explicit separation, efficient | Not a natural "image" — harder to visualize/debug |
| **C: RGB Visual** | 3 | Blue bars + orange band (human-readable) | What you'd show a human | CNN must decompose colors to extract signals |
| **D: Grayscale** | 1 | Bright=bars, mid-gray=band, light=bg | Simplest model, intensity = information density | Band and bar overlap — CNN must learn intensity thresholds |

## Regeneration instructions

- **Layout:** single long page. h1 with bottom border, intro paragraph, `.legend` callout, then four `.shape-row` blocks (white card: background `#fff`, border `1px solid #ddd`, radius 8px, padding 20px) each holding an h2 and a `.row` CSS grid `repeat(4, 1fr)` with 20px gap of `.cell` divs (centered text, white background, border `1px solid #ddd`, radius 8px, padding 15px). Each cell: `<h4>` option name, zoomed canvas (class `zoomed`, 192×192 CSS), `<br><br>`, actual canvas (class `actual`, 64×64 CSS), `<p>` caption. Final `.shape-row` holds the Analysis h2 and a full-width collapsed-border table.
- **Page CSS:** body -apple-system sans-serif, margin 40px, text `#2c3e50`, background `#f8f9fa`; h1 `#1a5276` with `3px solid #1a5276` bottom border; h2 `#1a5276`; h3 `#2980b9`; cell h4 0.85em `#1a5276`; cell p 0.75em `#666`; `.label` 0.9em bold `#444`; `.legend` background `#eef`, border `1px solid #aac`, radius 6px, padding 15px, 0.9em; canvas `border: 1px solid #333; image-rendering: pixelated`.
- **Canvas rendering:** these canvases use intrinsic 64×64 (and 192×192) pixel buffers upscaled by CSS — an intentional pixelated exception; they do NOT use the usual devicePixelRatio scaling helper (the standard project convention is `window.devicePixelRatio` scaling, deliberately skipped here to expose raw CNN input pixels). Rebuild data with the seeded mulberry32/Box-Muller generators and per-option renderers described in the visualization spec above.
- **Palette:** project palette #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange; encoding-specific colors as specified per option (pure red/green channels, `rgb(40,100,180)` blue bars, `rgba(230,126,34,0.8)` orange band, grayscale ramps).
- In regenerated HTML, any links use .html extensions (this page has none).
