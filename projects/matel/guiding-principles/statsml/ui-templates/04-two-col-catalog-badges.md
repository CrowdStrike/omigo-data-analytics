# TEMPLATE: Catalog with Domain Badges — bad-examples style

**Page type:** other (UI template file: row catalog with blue-header obj-table, domain pill badge + numbered title + bullets left 50%, wide 720×200 canvas right 50%)
**HTML title tag:** TEMPLATE: Catalog with Domain Badges — bad-examples style

**CSS header comment (verbatim):**
```
═══ TEMPLATE: Row catalog with domain badge pills, large canvas text ═══
Use for: Bad metrics, disaster catalogs, "hall of shame" style docs
Pattern: Blue header table → rows with domain pill + numbered title + bullets | wide canvas
Source: reference/metrics/bad-examples.html

KEY DIFFERENCES from metric-testing style (template 05):
- No CSS reset (* { margin:0; ... })
- Shorter font stack (no Segoe UI, Roboto)
- Domain pill badge (colored inline-block)
- Table has <th> header row with blue background
- Canvas fonts are 17px (larger, bolder)
- Canvas fixed at 720×200 in CSS with margin-top:8px
- Back-nav link at top
- Background: #fafafa (not white)
```

## Document content (in order)

**Back-nav link (`.nav-link`, template-specific — present in this file's markup):** "← Back to Section Index" (href="#"). Note: this is a documented feature of this template style; site pages no longer use back links.

**h1:** Catalog Title — N Items by Domain

**Subtitle:** Each row: one item. Why it fails and what it hides.

### Catalog table (`.obj-table` with thead)

Header row: "The Item & What Went Wrong" | "Visualization".

Three placeholder rows; each left cell has a `.metric-domain` pill, a `.metric-title`, and a `.metric-desc` bullet list; each right cell holds a canvas.

**Row 1** — pill: Domain Name; title: 1. Item Title
- **Why bad:** Core problem in one sentence — the fundamental flaw
- **What it hides:** The reality concealed behind the number
- **Real damage:** Concrete consequence that actually happened
- **Fix:** What to measure instead

**Row 2** — pill: Domain Name; title: 2. Item Title
- **Why bad:** Core problem
- **What it hides:** Hidden reality
- **Real damage:** Concrete consequence
- **Fix:** Better approach

**Row 3** — pill: Domain Name; title: 3. Item Title
- **Why bad:** Core problem
- **What it hides:** Hidden reality
- **Real damage:** Concrete consequence
- **Fix:** Better approach

### Visualization (canvas `canvas1`, 720×200)

Two-line divergence chart (vanity vs reality).

- **Title (bold 17px `#1a5276`, left-anchored at pad.left, y=18):** "Vanity Metric vs. Real Signal".
- **Padding:** top 30, bottom 35, left 60, right 140; L-shaped axes in `#999` 1px.
- **X labels (12px `#666`):** 2019, 2020, 2021, 2022, 2023, 2024.
- **Data (y scale 0–100):** vanity line `[40, 55, 68, 78, 86, 92]` in red `#e74c3c` width 3; reality line `[35, 42, 45, 43, 38, 32]` in green `#27ae60` width 3.
- **Gap annotation:** dashed (4/3) vertical `#c0392b` line at 85% of chart width between the two final values (92 and 32); bold 14px `#c0392b` label "THE GAP" beside it with 12px second line "= the lie".
- **Legend (right side, 13px):** red swatch "Vanity metric", green swatch "Real signal" (label text `#333`).

### Visualization (canvas `canvas2`, 720×200)

Side-by-side bar comparison.

- **Title (bold 17px `#1a5276`):** "Two Approaches: Surface Metric vs Deep Metric".
- **Group 1 (x≈120):** heading bold 14px `#555` "Surface (looks good)"; red `#e74c3c` bar 80px wide at 90% of a 100px max height; 13px `#333` value label "90%" above and caption '"Great!"' below.
- **Group 2 (x≈400):** heading "Deep (actual quality)"; green `#27ae60` bar at 25%; value label "25%", caption "Reality".
- **Verdict (bold 13px `#c0392b`, bottom center):** "The surface metric hides the truth".

### Visualization (canvas `canvas3`, 720×200)

Bimodal histogram with a misleading mean line.

- **Title (bold 17px `#1a5276`):** "Distribution Reveals What the Average Hides".
- **Padding:** top 30, bottom 35, left 60, right 30.
- **Bins (11 bars, y scale max 40):** `[5, 15, 25, 20, 8, 5, 3, 8, 20, 35, 30]`; bars 0–4 red `#e74c3c`, bars 5–6 amber `#f39c12`, bars 7–10 green `#27ae60`; 2px gaps.
- **Mean line:** dashed (5/3) vertical `#1a5276` 2px line at bin position 5.5; bold 12px `#1a5276` label "Mean: \"looks fine\"" and 11px second line "Nobody is actually here!".

## Regeneration instructions

- **Template:** this file IS ui-template 04-two-col-catalog-badges — a self-documenting HTML template with placeholder content. Structure: `.nav-link` back link (a feature of this template style), h1 + `.subtitle`, one `.obj-table` with a `<thead>` header row and one `<tr>` per catalog item (text left | canvas right), then the canvas script. Keep the CSS header comment block and the script's palette/setup comments.
- **Page style:** body font `-apple-system, BlinkMacSystemFont, sans-serif` (no CSS reset), background `#fafafa`, text `#222`, padding 20px 10px; h1 `#1a5276`; `.subtitle` `#555` 1.1em; `.nav-link a` `#2980b9`, weight 500, underline on hover.
- **Table:** full width, collapsed; th background `#1a5276` white text, padding 12px 16px, border `1px solid #2980b9`; td border `1px solid #2980b9`, padding 14px 16px, vertical-align top; even rows background `#f0f8ff`; first td 50%, last td 50%.
- **Row elements:** `.metric-domain` pill — inline-block, background `#2980b9`, white text, padding 2px 8px, radius 3px, 0.8em; `.metric-title` bold `#1a5276` 1.05em; `.metric-desc ul` 0.93em, line-height 1.7, margin `4px 0 0 16px`. Numbered titles use unpadded "N. Title".
- **Canvases:** fixed 720×200 CSS size with margin-top 8px, `display:block`; `setup(id)` helper multiplies the backing store by `window.devicePixelRatio`, fixes CSS size, `ctx.scale(dpr,dpr)`, and sets a default 17px font. Canvas titles bold 17px.
- **Script palette comment (keep):** Primary `#1a5276` (titles, axes); Positive `#27ae60`; Negative `#e74c3c`; Warning `#e67e22` (orange); Accent `#c0392b` (bold callouts); Bar fill `rgba(26,82,118,0.35)`; Lines `#999` (axes), `#e0e0e0` (grid).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, plus `#c0392b` accent and `#f39c12` amber.
- In regenerated HTML, links use `.html` extensions (here the back link is a `#` placeholder).
