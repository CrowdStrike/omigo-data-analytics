# Weighted Averages

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%)
**HTML title tag:** Weighted Averages

**Subtitle:** Not every number deserves an equal vote — weight each one by how much it represents

## 4.8 Stars from 10 People vs 4.2 from 5,000

**Tags:** `core idea` (blue), `running example` (green)

- **Product A** — 4.8 stars, but from only 10 reviews
- **Product B** — 4.2 stars, from 5,000 reviews
- **Combined rating** — (4.8×10 + 4.2×5,000) ÷ 5,010 = 21,048 ÷ 5,010 ≈ **4.20**
- **The naive version** — (4.8 + 4.2) ÷ 2 = 4.5 — off by 0.3 stars
- **The fix** — let each review vote once, not each product

*Example:* The 10 five-star fans of Product A are 0.2% of the voters — they cannot pull the combined answer far from 4.2.

**Key point:** A weighted average multiplies each value by its weight (here, review count) before dividing — so bigger groups rightly count for more.

### Visualization (canvas `c1`, 720×300)

Rating scale 4.0–5.0 with circle size = review count; naive vs weighted markers.

- **Title (bold 15px, `#1a5276`, top center):** "5,000 Votes Pull the Combined Rating to 4.20".
- **Axis:** horizontal star-rating axis at y=200 from x=70, spanning 4.0 to 5.0 with ticks/labels at 4.0, 4.2, 4.4, 4.6, 4.8, 5.0 (12px `#6b7280`); axis title "star rating" centered below.
- **Product B:** large circle (radius 52) centered at x=4.2, y=130, fill `rgba(42,120,214,0.30)`, stroke blue `#2a78d6` 2px; inside labels bold 13px blue "B: 4.2" and 12px "5,000 reviews".
- **Product A:** small circle (radius 9) at x=4.8, fill `rgba(217,89,38,0.35)`, stroke orange `#d95926` 2px; labels above in orange: bold 13px "A: 4.8", 12px "10 reviews".
- **Naive marker:** vertical dashed gray line (`#6b7280`, width 2, dash 5/4) at x=4.5, labeled above in bold 12px: "naive (4.8+4.2)/2 = 4.5".
- **Weighted marker:** solid green vertical line (`#008300`, width 3) at x=4.20, labeled above in bold 13px green: "weighted = 4.20".
- **Annotation (bold 13px green, two lines, right of x=4.55):** "circle area ~ reviews (not to scale):" / "the combined answer sits with the crowd".

## A Course Grade, Weighted by the Syllabus

**Tags:** `worked example` (green)

- **The syllabus** — homework 20%, midterm 30%, final 50%
- **Your scores** — homework 95, midterm 80, final 70
- **Weighted grade** — 95×0.2 + 80×0.3 + 70×0.5 = 19 + 24 + 35 = **78**
- **Simple average** — (95 + 80 + 70) ÷ 3 = 81.7 — flattering, and wrong per the syllabus
- **Check the weights** — 0.2 + 0.3 + 0.5 = 1, so each score contributes its exact share

*Example:* The final is worth half the grade, so the 70 drags harder than the 95 can lift.

**Key point:** Each contribution is value × weight. The pieces (19, 24, 35) stack up to the answer — you can audit every part by hand.

### Visualization (canvas `c2`, 720×300)

Horizontal stacked bar: grade contributions stack to 78; simple-average marker at 81.7.

- **Title (bold 15px, `#1a5276`, top center):** "Grade = 19 + 24 + 35 = 78 (Not the Simple Average 81.7)".
- **Axis:** horizontal 0–100 axis below the bar (ticks/labels every 20, 12px `#6b7280`); axis title "points toward the final grade (out of 100)".
- **Stacked segments** (bar at y=110, 52px tall; each segment's value in bold 13px white centered inside; segment name in bold 12px segment color above, formula in 12px muted above the bar):
  - aqua `#199e70`: 19 — "homework" / "95 x 0.2 = 19"
  - violet `#4a3aa7`: 24 — "midterm" / "80 x 0.3 = 24"
  - orange `#d95926`: 35 — "final" / "70 x 0.5 = 35"
- **Weighted total marker:** solid green vertical line (`#008300`, width 3) at 78, labeled above bold 13px green: "weighted grade: 78".
- **Simple average marker:** dashed magenta vertical line (`#d55181`, width 2, dash 5/4) at 81.7, labeled bold 12px magenta to the right: "simple avg: 81.7".
- **Annotation (bold 13px orange, bottom center):** "the final (weight 0.5) contributes the biggest block — its 70 caps the grade".

## Where the Average-of-Averages Trap Bites

**Tags:** `where it's used` (blue), `trap` (orange)

- **Any "average of averages"** without weights is suspect: ratings, conversion rates, salaries
- **The classic** — branch A: $50/order on 100 orders; branch B: $100/order on 10 orders
- **Company average** — (50×100 + 100×10) ÷ 110 = 6,000 ÷ 110 ≈ **$54.5**, not (50+100)/2 = $75
- **Group sizes are the weights** — ignoring them is how Simpson-style surprises start
- **Same idea everywhere** — GPA, portfolio returns, blended ad costs, survey corrections

*Example:* "Average order value is $75" overstates reality by more than a third — the 10-order branch got an equal vote.

**Key point:** Before averaging any per-group numbers, ask "how big is each group?" If sizes differ, you need weights.

### Visualization (canvas `c3`, 720×300)

Two-panel chart split by a dashed vertical divider at x=392: block widths = order counts (left), naive vs weighted bars (right).

- **Title (bold 15px, `#1a5276`, top center):** "110 Orders, Two Branch Averages, One Wrong Company Number".
- **Divider:** dashed light-gray vertical line `#bdc3c7` (dash 4/3).
- **Left panel (x=40, width 320):** caption bold 12px `#2c3e50`: "block width = number of orders". Two blocks at y=90, 46px tall, widths proportional to 100 vs 10 orders (out of 110):
  - Branch A block: fill `rgba(42,120,214,0.35)`, stroke blue 2px; inside bold 13px blue "branch A: $50/order" and 12px "100 orders".
  - Branch B block (narrow, 6px to the right): fill `rgba(217,89,38,0.40)`, stroke orange; labels bold 12px orange "B: $100" above and "10 orders" below.
  - Below (12px muted): "total revenue: 50 x 100 + 100 x 10 = $6,000" and "total orders: 110"; then bold 13px green: "true average: 6,000 / 110 = $54.5".
- **Right panel (from x=430):** baseline at y=235, two bars 100px wide, scale max $90 over 155px:
  - magenta `#d55181` bar $75, value label "$75" bold 13px above, caption 12px muted "naive (50+100)/2".
  - green `#008300` bar $54.5, label "$54.5", caption "weighted by orders".
  - Annotation bold 13px magenta near top: "the naive number is 38% too high"; 12px muted below baseline: "10 orders got the same vote as 100".

## The Confusion: Percentages Without Their Bases

**Tags:** `common mistake` (red)

- **Never average percentages** without their denominators — the bases are the weights
- **Example** — 90% satisfaction from 10 customers, 60% from 1,000 customers
- **Naive** — (90% + 60%) ÷ 2 = 75%; **weighted** — (9 + 600) ÷ 1,010 ≈ **60.3%**
- **Equal weighting is a choice too** — the plain mean is just "all weights equal"
- **Big groups dominating is correct** — 5,000 reviews pinning the answer near 4.2 is not bias

*Example:* Calling satisfaction "75%" only works if you ignore 990 of the 1,010 people who answered.

**Common mistake:** Treating every row of a summary table as one vote. Rows are groups; the people inside them are the votes.

### Visualization (canvas `c4`, 720×300)

Two-panel chart (dashed divider at x=392): survey group areas ~ respondents (left), naive vs weighted bars (right).

- **Title (bold 15px, `#1a5276`, top center):** "90% of 10 People + 60% of 1,000 People Is Not 75%".
- **Left panel (from x=45):**
  - Tiny square (22×22) at y=90: fill `rgba(217,89,38,0.40)`, stroke orange 2px; label bold 12px orange to the right: "survey kiosk: 10 people, 90% happy (9 people)".
  - Large rectangle (170×110) at y=130: fill `rgba(42,120,214,0.25)`, stroke blue; left 60% of it filled darker `rgba(42,120,214,0.45)` with bold 13px white "60%" inside; labels bold 12px blue to the right: "email survey: 1,000 people" / "60% happy (600 people)".
  - Footnote 12px muted: "area ~ respondents (not to exact scale)".
- **Right panel (from x=430):** baseline y=235, two bars 100px wide, scale max 100% over 150px:
  - magenta `#d55181` bar 75, label "75%", caption "naive (90+60)/2".
  - green `#008300` bar 60.3, label "60.3%", caption "(9+600)/1,010".
  - Annotation bold 13px green near top: "609 happy people out of 1,010 = 60.3%"; bold 12px magenta below baseline: "\"75%\" ignores 990 of the answers".

## Regeneration instructions

- **Layout:** tutorial detail page. h1 (no index number) with 2px `#2980b9` bottom border, `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; `.example` italic `#555` 0.9rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, 1px `#e0e0e0` border, radius 4px; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
