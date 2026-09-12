# Distance Metrics

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table layout text left 50% / canvas right 50%, one three-column row)
**HTML title tag:** Distance Metrics

**Subtitle:** "Similar" is not one thing — Euclidean asks how far apart two users sit, cosine asks whether their tastes point the same way

## Two Rulers, Two Different "Most Similar" Users

Tags: `core idea` (blue), `running example` (green)

- **The data** — ratings for two movies: Ana (action 4, romance 2), Ben (2, 1), Cara (3, 3)
- **Euclidean asks** — how far apart do the two users sit? the straight-line gap
- **Cosine asks** — do their arrows point the same way? the angle between tastes
- **Euclidean's pick** — Cara is Ana's nearest: gap 1.41 vs Ben's 2.24
- **Cosine's pick** — Ben is a perfect 1.00 match: same taste ratio, harsher scores

*Example (italic):* Ben is exactly "Ana ÷ 2" — half the enthusiasm, identical taste.

**Key point:** **A distance metric is the definition of "similar"** you hand to kNN, k-means, or a recommender. Change the ruler and every neighbor, cluster, and recommendation changes with it.

### Visualization (canvas `c1`, 720×300)

Vector plot: three users drawn as arrows from the origin in rating space, comparing straight-line gaps to angles.

- **Title (bold 15px, `#1a5276`, top center):** "Three Users as Arrows: Gap vs Angle".
- **Axes:** L-shaped gray (`#999`) axes; x = "action-movie rating" with tick labels 0–4; y = "romance rating" with tick labels 0–3 (rotated axis title). Axis labels 12px `#6b7280`. Padding: top 44, bottom 46, left 62, right 200. Scale maxX 4.6, maxY 3.6.
- **Arrows (from origin, width 3, filled arrowheads):** Ana to (4, 2) in blue `#2a78d6`; Ben to (2, 1) in green `#008300`; Cara to (3, 3) in violet `#4a3aa7`.
- **Dashed gap lines (yellow `#c98500`, width 2, dash 5/4):** Ana→Cara and Ana→Ben.
- **Point labels (bold 13px):** "Ana (4, 2)" blue, "Ben (2, 1)" green, "Cara (3, 3)" violet.
- **Gap labels (bold 12px yellow):** "gap 1.41" near the Ana–Cara segment (at data ≈ (3.55, 2.65)); "gap 2.24" near the Ana–Ben segment (at ≈ (2.9, 1.32)).
- **Angle note (bold 12px green, near origin at ≈ (0.3, 0.28)):** "Ben sits exactly on Ana’s line: angle 0°".
- **Right-side legend (x = w−188):** "Euclidean: Cara nearest" (bold 13px `#2c3e50`) / "(shortest dashed gap)" (12px); "Cosine: Ben nearest" (bold 13px green) / "(same direction, cos = 1.00)" (12px).

## Both Numbers by Hand

Tags: `worked example` (green), `arithmetic` (blue)

Three-column row: text 38%, two canvases 31% each.

- **Euclid, Ana–Ben** — √((4−2)² + (2−1)²) = √5 ≈ 2.24
- **Euclid, Ana–Cara** — √((4−3)² + (2−3)²) = √2 ≈ 1.41 — Cara wins
- **Cosine recipe** — dot product ÷ (length × length); the sizes cancel out
- **Ana·Ben** — 4×2 + 2×1 = 10; lengths √20 × √5 = 10 → cosine 1.00 — Ben wins
- **Ana·Cara** — 4×3 + 2×3 = 18; √20 × √18 ≈ 18.97 → cosine 0.95

*Example (italic):* Same three users — and the two rulers rank them in opposite order.

**Key point:** **Neither ruler is wrong:** Euclidean counts enthusiasm as part of similarity; cosine deliberately ignores it and compares the taste pattern alone. Strictly, cosine is a similarity score — bigger means closer.

### Visualization (canvas `c2a`, 420×340)

Two-bar chart of Euclidean distances from Ana.

- **Title (bold 15px `#1a5276`):** "Euclidean Distance to Ana"; subtitle (12px `#6b7280`): "smaller = more similar".
- **Bars:** Ben = 2.24 (green `#008300`, alpha 0.55) and Cara = 1.41 (violet `#4a3aa7`, alpha 0.95 — the winner is more opaque). Bar width 100, gap 80, centered; baseline gray axis.
- **Y-axis:** 0.0 to 2.5 in steps of 0.5 (scale max 2.6), right-aligned 12px gray labels.
- **Value labels (bold 13px `#2c3e50`)** above bars: "2.24", "1.41"; names "Ben", "Cara" below.
- **Bottom annotation (bold 13px violet):** "this ruler picks Cara".

### Visualization (canvas `c2b`, 420×340)

Two-bar chart of cosine similarities to Ana.

- **Title (bold 15px `#1a5276`):** "Cosine Similarity to Ana"; subtitle (12px `#6b7280`): "bigger = more similar (max 1.00)".
- **Bars:** Ben = 1.00 (green `#008300`, alpha 0.95 — winner) and Cara = 0.95 (violet `#4a3aa7`, alpha 0.55). Same geometry as c2a; scale max 1.05.
- **Y-axis:** 0.00 to 1.00 in steps of 0.25, right-aligned 12px gray labels.
- **Value labels (bold 13px):** "1.00", "0.95"; names "Ben", "Cara" below.
- **Bottom annotation (bold 13px green):** "this ruler picks Ben — ranks flipped".

## Why Cosine Wins for Ratings and Text

Tags: `where it's used` (blue), `rule of thumb` (green)

- **Harsh raters** — some users never give 5 stars; cosine cancels that personal scale
- **Documents too** — a 2,000-word article uses its 200-word summary's words ~10× as often
- **Word counts (10, 4) vs (100, 40)** — Euclid gap ≈ 96.9, huge; cosine = 1.00, identical
- **Direction is topic, length is size** — for text, the nuisance is almost always size
- **In practice** — search and recommenders default to cosine, or normalize then use Euclid

*Example (italic):* A tweet and an essay about the same match should count as neighbors — cosine says they are.

**Key point:** **Rule of thumb:** when the overall size of a vector is a nuisance — rating harshness, document length, activity level — cosine removes exactly that nuisance and nothing else.

### Visualization (canvas `c3`, 720×300)

Vector plot: a summary and its full article as word-count vectors sharing one direction.

- **Title (bold 15px `#1a5276`):** "A Summary and Its Full Article: Same Direction, Different Length".
- **Axes:** x = 'count of the word "goal"' with ticks 0–100 step 25 (scale max 110); y = 'count of the word "team"' with ticks 0–40 step 10 (scale max 46, rotated title). Gray `#999` L-axes, 12px `#6b7280` labels. Padding: top 44, bottom 46, left 66, right 220.
- **Arrows (width 3):** article to (100, 40) in blue `#2a78d6`; summary to (10, 4) in aqua `#199e70`.
- **Dashed Euclid gap (yellow `#c98500`, width 2, dash 5/4):** from (10, 4) to (100, 40).
- **Labels (bold 13px):** "summary (10, 4)" aqua; "article (100, 40)" blue.
- **Yellow annotation (bold 12px, at ≈ data (30, 26)):** 'Euclid gap ≈ 96.9: "very different"'.
- **Right-side legend (x = w−208):** green bold 13px: "cosine = 1.00:" / '"same topic exactly"'; then 12px `#2c3e50`: "direction = what it is about," / "length = how long it is".

## Units Quietly Break Euclidean

Tags: `common mistake` (red), `caution` (orange)

- **Mix features** — compare users on (rating 1–5, watch time); the units differ wildly
- **In minutes** — Priya (5★, 300) vs Quinn (1★, 310): √(4² + 10²) ≈ 10.8
- **vs Raj (5★, 260)** — √(0² + 40²) = 40 — so opposite-taste Quinn is "nearer"
- **Same data in hours** — Quinn: √(16 + 0.03) ≈ 4.0; Raj: √(0 + 0.44) ≈ 0.67 — Raj is nearer
- **The fix** — z-score the features first, or use cosine on features of comparable scale

*Example (italic):* Nothing about the people changed — only the unit did, and the nearest neighbor flipped.

**Key point:** **The confusion:** Euclidean feels objective, but on raw mixed units it just echoes whichever column has the biggest numbers. If a unit change flips your answer, the answer was about units, not people.

### Visualization (canvas `c4`, 720×300)

Two side-by-side bar panels showing the nearest-neighbor flip between minutes and hours, split by a dashed vertical divider.

- **Title (bold 15px `#1a5276`):** "Who Is Priya’s Nearest Neighbor? Depends on the Unit".
- **Divider:** vertical dashed light-gray line (`#bdc3c7`, dash 4/3) at mid-width from y=40 to h−14.
- **Left panel (x=48, width 260), title bold 13px `#1a5276`:** "Watch time in MINUTES". Bars for "Quinn (1★)" = 10.8 (magenta `#d55181`, alpha 0.95 — winner) and "Raj (5★)" = 40 (aqua `#199e70`, alpha 0.45); scale max 44; value labels "10.8", "40.0" bold 12px; "nearest" label (bold 12px magenta) under Quinn. Baseline at y=224, bar area height 138, bar width 92, gap 44.
- **Right panel (x=414), title:** "Watch time in HOURS". Bars: Quinn = 4.0 (alpha 0.45) and Raj = 0.67 (alpha 0.95 — winner); scale max 4.4; value labels "4.00", "0.67"; "nearest" label (bold 12px green `#008300`) under Raj.
- **Panel annotations (bold 13px, y=44):** left in magenta at x=178: "opposite taste, but minutes dominate"; right in green at x=544: "same data in hours — the ranking flips".
- **Bottom caption (bold 12px `#2c3e50`, center):** "distance from Priya (5★, 300 min): the unit — not the people — picked the neighbor".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (2rem, `#1a5276`, bottom border 2px solid `#2980b9`), `.subtitle` (`#666`, 0.95rem), then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: `.text-col` (50%) text, `.viz-col` (50%) canvas. Section 2 uses the 3-column variant: `.text-col3` (38%) + two `.viz-col3` (31% each), canvases 420×340.
- **Text cell structure:** `.tags` row of pill spans first, then `<ul>` of one-line bullets each opening with `<b>` term (bold terms colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Tag pills:** 0.72rem, weight 600, padding 2px 10px, radius 10px. blue = `rgba(26,82,118,0.12)`/`#1a5276`; green = `rgba(39,174,96,0.15)`/`#27ae60`; red = `rgba(231,76,60,0.12)`/`#e74c3c`; orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic width/height attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via shared `setup(id)` helper; shared `drawArrow` helper for vector arrows (12px arrowheads). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
