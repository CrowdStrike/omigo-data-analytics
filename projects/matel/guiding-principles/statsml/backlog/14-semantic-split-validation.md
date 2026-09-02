# Semantic Validation of Decision Tree Splits

**Page type:** detail page (backlog kusto-style two-column layout: text left 45%, canvas right 55%, one table row per section)
**HTML title tag:** Semantic Validation of Decision Tree Splits — Discussion Backlog

**Subtitle:** Beyond significance: does a split make domain sense?

**Intro callout:** Beyond statistical significance: can we check if a split makes logical/semantic sense? A statistically valid cut point may still be a meaningless artifact of the data.

## 1. The Problem

- Cholesterol split at 247.3 — medically meaningful or artifact?
- Age split at 37 — corresponds to known transition?
- Income split at $67,432 — why not $65K or $70K?
- Zip code split numerically — nonsense (not ordinal)

**What "semantically makes sense" means:**

- **Domain boundaries:** Aligns with known thresholds (cholesterol 200=borderline, 240=high)
- **Feature type awareness:** Nominal vs ordinal vs continuous
- **Granularity check:** Suspiciously precise boundaries = noise
- **Conditional logic:** Makes sense given parent split
- **Interaction validity:** Combined path produces coherent subpopulation

### Visualization (canvas `c1`, 720×300)

Number line showing a raw algorithmic cholesterol split vs medical boundaries, with a snap arrow.

- **Title (bold 16px, `#1a5276`, top center):** "Cholesterol Split: Raw vs Medical Boundaries"
- **Number line:** horizontal line (`#2c3e50`, width 2) at y=120 from x=60 to x=660, value range 150–300, tick marks with labels every 25 (150, 175, 200, 225, 250, 275, 300).
- **Background zones** (rectangles from y=45 to y=175): 150–200 fill `rgba(39,174,96,0.1)`; 200–240 fill `rgba(230,126,34,0.1)`; 240–300 fill `rgba(231,76,60,0.1)`. Zone labels (13px, near y=58): "Normal" in `#27ae60` at value 175, "Borderline" in `#e67e22` at 220, "High" in `#e74c3c` at 270.
- **Medical boundaries:** solid green (`#27ae60`) vertical lines width 3 at values 200 and 240, spanning y=85 to y=145, each labeled with its value in bold 14px green above the line.
- **Raw split:** dashed red (`#e74c3c`, dash 4/3, width 3) vertical line at value 247.3, labeled below in bold red "247.3" with "(raw split)" in 12px underneath.
- **Snap arrow:** blue (`#1a5276`, width 2) quadratic curve from 247.3 to 240 at y=180, arrowhead at 240, labeled below in 13px blue: "snap to medical boundary".
- **Legend (bottom left):** green square + "Medical boundaries"; red square + "Raw algorithmic split" (text in `#2c3e50`).

## 2. Implementation Approaches

- Snap-to-meaningful: 247.3 → snap to 240 or 250
- LLM semantic check: Ask if split makes domain sense
- Type-aware validation: Reject numeric splits on categoricals
- Stability test: If 247 works, does 240? Does 250?
- Path coherence scoring

**Key Questions:**
(1) How much without domain knowledge?
(2) Snap all to round numbers?
(3) Interaction with range-based approach?
(4) Can semantic metadata (doc 25) help?
(5) Cost of false rejection?

### Visualization (canvas `c2`, 720×300)

Flowchart of a nonsensical decision path vs a valid path.

- **Title (bold 16px, `#1a5276`, top center):** "Nonsensical Decision Path Detection"
- **Top path:** 4 boxes (140×45, starting x=40, y=70, 30px gaps, gray `#95a5a6` arrows between): "age > 65" (valid), "retired = no" (valid), "pregnant = yes" (INVALID), "Result: 92%" (valid). Valid boxes fill `#e8f8e8` stroke `#27ae60`; the invalid box fill `#fde8e8` stroke `#e74c3c`; box text 14px `#2c3e50` centered.
- **Contradiction annotation:** dashed red (`#e74c3c`, dash 3/3) vertical line dropping from the third box, then centered red text: "CONTRADICTION" (bold 13px), "age > 65 AND pregnant = yes" (12px), "(biologically impossible)" (12px).
- **Bottom path:** left-aligned label "Valid path:" (bold 14px `#27ae60`) at y=195, then 4 green boxes (140×35, fill `#e8f8e8` stroke `#27ae60`, green arrows): "age > 55", "cholesterol > 240", "smoker = yes", "Result: 78%".
- **Bottom caption (13px `#27ae60`, left-aligned):** "Coherent medical subpopulation — domain expert would agree"

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem), then one `.lang-section` per numbered section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with a single `<tr>` — left `td.text-col` (45%) holds bullets/key-point, right `td.viz-col` (55%) holds the canvas. No index number in the h1.
- **Key-point callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem with 4px li spacing; canvases `width: 100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, accents `#2980b9`, gray `#95a5a6`.
- **Canvas:** intrinsic width/height attributes as given (720×300 each); scale via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setupCanvas(id)` helper.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
