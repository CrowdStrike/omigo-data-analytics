# Sensitivity & Specificity

**Page type:** detail page (tutorial layout: h1 + subtitle, 4 `.card-section` blocks each with a two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Sensitivity &amp; Specificity

**Subtitle:** Two questions about one screening test: of the sick, how many did it catch — and of the healthy, how many did it correctly clear

## One Test, Two Report Cards

Tags: `core idea` (blue), `running example` (green)

- **The clinic** — 1,000 patients screened; 50 truly sick, 950 healthy
- **The test flags 83** as positive — 45 truly sick, 38 healthy false alarms
- **Missed** — 5 of the 50 sick walk out with a clean result
- **Sensitivity** — of the 50 sick, how many caught? 45/50 = 90%
- **Specificity** — of the 950 healthy, how many cleared? 912/950 = 96%

*Example:* A sensitive test rarely misses disease; a specific test rarely scares a healthy person.

**Key point:** Sensitivity grades the test on the sick; specificity grades it on the healthy. One test, two separate report cards.

### Visualization (canvas `c1`, 720×300)

Two stacked "piles", each normalized to 100% of its own group, split by a vertical dashed divider at x=360 (`#bdc3c7`, dash 4/3).

- **Title (bold 15px `#1a5276`, top center):** "The Sick Pile and the Healthy Pile".
- **Geometry:** baseline y=232, pile height 170px, column width 120px; each pile has a muted `#6b7280` 1px outline around the full 100% extent.
- **Left pile (x=110), the 50 sick:** header (muted 12px) "the truly sick (pile = 100% of 50)"; bottom segment 45/50 in green `#008300` with white bold 13px inside label "45 caught"; top segment 5/50 in magenta `#d55181` with magenta label to the right "5 missed"; below: bold "50 sick", then bold 14px green "sensitivity = 45/50 = 90%".
- **Right pile (x=450), the 950 healthy:** header "the truly healthy (pile = 100% of 950)"; bottom segment 912/950 in blue `#2a78d6` with white inside label "912 cleared"; top segment 38/950 in orange `#d95926` with orange label to the right "38 false alarms"; below: bold "950 healthy", then bold 14px blue "specificity = 912/950 = 96%".

## The 2×2 Table by Hand

Tags: `worked example` (green), `core idea` (blue)

- **Caught (TP)** — 45 sick patients correctly flagged positive
- **Missed (FN)** — 5 sick patients flagged negative
- **False alarms (FP)** — 38 healthy patients flagged positive
- **Cleared (TN)** — 912 healthy patients flagged negative
- **Sensitivity** — 45 / (45 + 5) = 90%; it reads across the sick row
- **Specificity** — 912 / (912 + 38) = 96%; it reads across the healthy row

*Example:* Each row of the table is one report card: the sick row gives sensitivity, the healthy row gives specificity.

**Key point:** Sensitivity = TP / (TP + FN). Specificity = TN / (TN + FP). Both denominators come from the truth, never from the test.

### Visualization (canvas `c2`, 720×300)

Confusion-matrix 2×2 grid with per-row formula readings.

- **Title (bold 15px `#1a5276`):** "Each Row of the 2×2 Is One Report Card".
- **Grid:** origin (150, 78), cells 115×64px, muted 1px cell outlines. Column headers (bold 12px muted): "test positive", "test negative", with "the test says →" above them. Row labels (bold 13px `#2c3e50`, right-aligned): "sick (50)", "healthy (950)"; rotated vertical muted label at far left: "the truth".
- **Cells (bold 14px white text "LABEL count"):** TP 45 fill `rgba(0,131,0,0.75)`; FN 5 fill `rgba(213,81,129,0.75)`; FP 38 fill `rgba(217,89,38,0.75)`; TN 912 fill `rgba(42,120,214,0.75)`.
- **Row readings (short colored tick line + bold 13px text to the right of the grid):** green `#008300` "sensitivity = 45 / (45 + 5) = 90%" beside the sick row; blue `#2a78d6` "specificity = 912 / (912 + 38) = 96%" beside the healthy row.
- **Bottom annotations (centered):** bold 13px red `#e74c3c` "both denominators are row totals — set by the truth, not by the test"; muted 12px "50 sick + 950 healthy = the same 1,000 patients from the running example".

## Medicine's Names, ML's Names

Tags: `where it's used` (blue), `translation` (orange)

- **Sensitivity = recall** — the exact same fraction, 45/50 = 90%
- **Specificity = TNR** — ML's "true negative rate", 912/950 = 96%
- **1 − specificity = FPR** — 38/950 = 4%; the x-axis of every ROC curve
- **PPV = precision** — of the 83 positives, how many sick? 45/83 = 54%
- **Watch the swap** — precision (54%) and specificity (96%) are often confused

*Example:* A paper says "recall 90%", a doctor says "sensitivity 90%" — same number, same fraction, different field.

**Key point:** Learn the translation once — sensitivity/recall start from the sick, specificity starts from the healthy, precision starts from the flags.

### Visualization (canvas `c3`, 720×300)

Two-column translation diagram: medicine's term boxes connected to ML's term boxes, with the shared fraction on each connector.

- **Title (bold 15px `#1a5276`):** "Two Vocabularies, Same Fractions".
- **Column headers (bold 12px muted, centered):** "medicine says" (left column at x=60, boxes 165×36px) and "ML says" (right column at x=490, boxes 170×36px); rows start y=64, spaced 52px.
- **Rows (box fill `#f8f9fa`, 2px colored outline, bold 13px colored term text; 1.5px colored connector line between the columns; the fraction printed in bold 12px `#2c3e50` on a white patch mid-connector):**
  - "sensitivity" ↔ "recall / TPR", value "45/50 = 90%", green `#008300`
  - "specificity" ↔ "TNR", value "912/950 = 96%", blue `#2a78d6`
  - "PPV" ↔ "precision", value "45/83 = 54%", violet `#4a3aa7`
  - "1 − specificity" ↔ "FPR", value "38/950 = 4%", orange `#d95926`
- **Bottom annotation (bold 13px red `#e74c3c`, centered):** "sensitivity is just recall — translate the names before comparing papers".

## The "99% Accurate" Trap

Tags: `common mistake` (red), `trade-off` (orange)

- **The lazy test** — call all 1,000 patients healthy: 950 right, accuracy 95%
- **But sensitivity 0%** — it catches none of the 50 sick (specificity 100%)
- **The real test** — accuracy 95.7%, barely higher, yet sensitivity 90%
- **Rare disease inflates accuracy** — at 5% sick, "healthy" is right 95% of the time
- **An ad's "99% accurate"** — without the pair, the claim is unverifiable

*Example:* With a 1-in-100 disease, a test that never detects it at all is still "99% accurate".

**Key point — Common mistake:** Accepting one blended number — always ask "accurate on whom?" and demand both sensitivity and specificity.

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: lazy test vs real test across three metrics.

- **Title (bold 15px `#1a5276`):** "Accuracy Ties at ~95% — Sensitivity Exposes the Useless Test".
- **Data (three metric groups, two 52px bars each):** accuracy — lazy 95%, real 95.7%; sensitivity — lazy 0%, real 90%; specificity — lazy 100%, real 96%. Lazy test bars gray `#aab4be` with muted bold value labels; real test bars blue `#2a78d6` with blue bold value labels; metric names below baseline.
- **Axes:** y labeled 0%, 50%, 100% (values scaled against 108 so 100% bars leave headroom), gridlines `#e5e9ef`; padding top 60, bottom 56, left 70, right 190; gray `#999` L axes.
- **Annotation (bold 13px red `#e74c3c`, centered above the plot area, y=48):** "0% sensitivity: every one of the 50 sick is missed".
- **Legend (right column, x = width−178):** gray swatch "lazy test: "all healthy""; blue swatch "real screening test".
- **Caption (muted 12px, bottom center):** "same 1,000 patients: 50 sick, 950 healthy".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, per `tutorials/CLAUDE.md`). Structure: `<h1>` (no index number), `.subtitle` paragraph, then 4 `.card-section` divs each containing `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%) holding one canvas.
- **Text column structure per section:** `.tags` row of colored pills (`.tag.blue` rgba(26,82,118,0.12)/#1a5276, `.tag.green` rgba(39,174,96,0.15)/#27ae60, `.tag.red` rgba(231,76,60,0.12)/#e74c3c, `.tag.orange` rgba(230,126,34,0.15)/#e67e22), then a `<ul>` of one-line bullets each opening with `<b>bold term</b>` (bold in `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem, with `<strong>` lead).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.card-section h2` 1.3rem `#1a5276` with 2px `#2980b9` bottom border; table cells padding 12px, vertical-align top; canvases `width:100%`, 1px solid `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** each 720×300 logical; `setup(id)` helper on this page reads the canvas `width`/`height` attributes (defaults 720×300), sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Hardcoded literal data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; red reserved for error/alarm annotations.
- In regenerated HTML, any card links use `.html` extensions (this page has no outgoing links).
