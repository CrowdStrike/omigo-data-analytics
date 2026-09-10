# Post-Hoc Segmentation

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Post-Hoc Segmentation — A/B Testing Pitfalls

**Subtitle:** Deliberate — Test failed overall? Find the subgroup where p<0.05.

## Section 1: Deliberate — Test failed overall? Find the subgroup where p<0.05.

- Overall: no effect (p=0.45). Disappointed. "Let's look at segments!" Test 50 segments. Male 25-34 iOS California: p=0.03! "Works for young male iOS users!"
- But: 50 segments × α=0.05 = 2.5 expected false positives. You found the noise.
- Narrative machine: once you find a subgroup, a story is ALWAYS available. "Young men are tech-savvy!" "iOS = higher income!"

**Correct approach:** Pre-register segments. If exploratory, treat as hypothesis GENERATION. Require replication on new data.

**The tell:** Was the segment predicted BEFORE? If not — post-hoc noise-fishing.

### Visualization (canvas `c1`, 720×340)

Grid diagram: 50 segment tiles (10 columns × 5 rows), two random "significant" ones highlighted, one circled with a magnifier.

- **Background:** full-canvas fill `#f8f9fa`.
- **Title (bold 17px `#1a5276`, centered):** "Overall: No Effect (p=0.45)" at y=22.
- **Grid:** 10 cols × 5 rows of 52×28 boxes, 4px horizontal / 6px vertical gaps, grid centered horizontally starting at y=40; each box labeled 16px gray `#666` centered "seg 1" … "seg 50".
- **Box colors:** indices 7 and 38 (0-based) are green `#27ae60` at alpha 0.8 (the false positives); all others gray `#bdc3c7` at alpha 0.5.
- **Magnifier annotation on segment index 38:** red circle (`#e74c3c`, width 2.5, radius 22) around the box center plus a short diagonal handle stroke (width 3); bold 16px red label "AHA! Found it!" to the right of the circle.
- **Bottom label (17px `#555`, centered):** "50 segments × 5% = 2.5 expected false positives".

## Section 2: Illustrative Example: The Tablet-Readers "Win"

- A large news website tested a redesigned homepage, and the overall result was flat — readers as a whole behaved no differently on the new design than the old one.
- Unwilling to accept a boring answer, the team sliced the data by device, country and visit frequency until one slice — returning tablet readers — showed a big lift, and they shipped the redesign to everyone on the strength of that story.
- When the site was measured again on fresh traffic, the tablet effect had vanished; if you examine dozens of slices of the same flat data, a couple will look great by pure luck (statisticians call this a multiple-comparisons problem).

### Visualization (canvas `c2`, 720×300)

Bar chart: % lift per slice of a flat test — most bars hover near zero, one random slice shines.

- **Title (bold 17px `#2a2a2a`, centered):** "Same Flat Test, Sliced 14 Ways" at y=26.
- **Data (14 slices, % lift):** `[-3, 2, -1, 4, -2, 1, -4, 3, 12, -2, 2, -3, 1, -1]`; winner is index 8 (+12%, "returning tablet readers").
- **Layout:** zero baseline at y=165 (thin gray `#999` line spanning the group, labeled "0%" 14px gray at left); bars 34px wide with 12px gaps, group centered; scale 7px per % lift; positive bars rise above the baseline, negative bars hang below.
- **Bar colors:** all bars fill `rgba(26,82,118,0.35)`; the winner also fill `rgba(39,174,96,0.35)` with stroke `#27ae60` width 2.
- **Winner annotation:** red circle (`#e74c3c`, width 2.5, radius 24) around the winner bar's top; bold 15px red label to the right: `"tablet readers +12%!" — shipped on this`.
- **Sub-caption (14px gray `#666`, centered, below baseline):** "slices: device × country × visit frequency".
- **Takeaway (15px `#333`, bottom center):** "On fresh traffic the shiny slice showed no lift at all — it was luck, not a discovery".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; bullets 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
