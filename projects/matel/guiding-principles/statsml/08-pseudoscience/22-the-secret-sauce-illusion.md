# The Secret Sauce Illusion

**Page type:** detail page (two-column obj-table layout: text left 40%, two stacked canvases right 60%, one row)
**HTML title tag:** The Secret Sauce Illusion — Pseudoscience in Data Analysis

**Subtitle:** 99% of Results Come from Boring Fundamentals, the 1% "Secret" Gets All the Credit

## Section 1: 99% of Results Come from Boring Fundamentals — The 1% "Secret" Gets All the Credit

- **Skincare:** Sunscreen + moisturizer + retinol = 95% of results (known for 40 years, costs $15), while the "proprietary peptide complex with marine collagen" adds ~0% yet gets the $120 price tag and the marketing campaign. Strip the peptide, same results; strip the sunscreen, skin ages.
- **ML papers:** Clean data + feature engineering + hyperparameter tuning = 95% of model performance; the "novel architecture" contributes 1-2% at most. The contribution section credits the architecture while 200 hours of data cleaning gets one sentence in "preprocessing" — "we cleaned the data really well" doesn't get published, "novel attention mechanism" does.
- **Cooking:** Salt, fat, acid, heat, fresh ingredients = 95% of why food tastes good; the "secret family recipe" is a pinch of nutmeg or a specific soy sauce brand. "Use good ingredients and season properly" isn't a cookbook — "grandmother's secret spice blend" is.
- **Business consulting:** "Talk to customers, ship faster, cut waste, hire well" = 95% of business success; the $500K "proprietary framework" (a 2×2 matrix with trademarked axis names) adds nothing. "Just do the obvious things" can't justify a $500K engagement — a "proprietary methodology" can.
- **Fitness:** Caloric deficit + progressive overload + adequate sleep + consistency = 99% of results; "optimal meal timing," "secret rep tempo," "anabolic window," and "muscle confusion" = 0-1%. "Eat less and lift more" doesn't sell a $200/month program — a "scientifically optimized periodization protocol" does.

**Why it's pseudoscience:** The 1% gets 99% of the credit because it's the only sellable part — fundamentals are common knowledge requiring patience, not purchase, while the mystifiable 1% can be packaged as a product, paper, course, or engagement. The economic model depends on inverting the contribution ratio: the part that matters least is positioned as the part that matters most.

### Visualization (canvas `c1`, 720×340)

Horizontal stacked-bar comparison of actual contribution vs credit received, plus explanatory text lines.

- **Title (bold 17px, `#1a5276`, centered at w/2, y=20):** "The 1% Gets 99% of Credit Because It's the Only Part You Can SELL".
- **Row labels (bold 17px, `#333`, left-aligned at x=50):** "Actual contribution:" at y=55; "Credit received:" at y=115.
- **Actual contribution bar (y=40, height 25):** green segment `rgba(39,174,96,0.4)` from x=220 width 430; red segment `rgba(231,76,60,0.4)` from x=650 width 25. Green centered label (17px `#27ae60`) at (435,58): "95% boring fundamentals (sunscreen, clean data, caloric deficit)"; red label (16px `#e74c3c`) at (662,56): "5%".
- **Credit received bar (y=100, height 25):** green segment `rgba(39,174,96,0.4)` from x=220 width 25; red segment `rgba(231,76,60,0.4)` from x=245 width 430. Red centered label (17px `#e74c3c`) at (460,118): "95% \"proprietary peptide\" / \"novel architecture\" / \"secret method\"".
- **Why lines (bold 17px `#555`, centered):** at y=160: "Why? Fundamentals are COMMON KNOWLEDGE → unsellable."; at y=185: "The mystifiable 1% → packaged as product, paper, course, consulting engagement."
- **Punchline (bold 17px, red `#e74c3c`, centered at y=220):** "If the truth (\"just do the basics\") got out → nothing to sell."

### Visualization (canvas `c2`, 720×300)

Two vertical stacked bars showing the inversion between actual contribution and revenue/credit attribution, with crossing dashed arrows.

- **Title (bold 16px Arial, `#2c3e50`, centered at w/2, y=18):** "The inversion exists because fundamentals are unsellable."
- **Bar geometry:** bar width 100, bar height 140, top y=35. Left bar at x = w/2−160; right bar at x = w/2+60. Both outlined `#2c3e50` 1px.
- **Left bar (Actual Contribution):** top 95% filled blue `#1a5276` with white bold 18px centered labels "Fundamentals" and "95%" (at 47% height and +15px); bottom 5% filled red `#e74c3c`. Below-bar label (bold 17px `#2c3e50`, two lines): "Actual Contribution" / "to Result".
- **Right bar (Revenue/Credit Attribution):** top 5% blue `#1a5276`; bottom 95% red `#e74c3c` with white bold 18px labels "\"Secret Sauce\"" and "95%" (at 52% height and +15px). Below-bar label: "Revenue/Credit" / "Attribution".
- **Mirror arrows:** two orange `#e67e22` dashed lines (width 2, dash 4/3) crossing between the bars — from (x1+110, y0+20) to (x2−10, y0+120) and from (x1+110, y0+130) to (x2−10, y0+65); bold 17px orange label "INVERTED" centered between them at mid-height.
- **Legend (bottom left, 16px Arial `#2c3e50`):** blue `#1a5276` 12×12 swatch at (50, h−20) with text "Fundamentals (boring, effective)"; red `#e74c3c` swatch at (300, h−20) with text "\"Secret ingredient\" (exciting, minimal)".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with one `<tr>`; left `<td>` (40%) holds `.obj-title` + bullet list + closing paragraph, right `<td>` (60%, centered) holds two stacked canvases (`c1` then `c2`).
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper returning `{ctx, w, h}`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, dark slate `#2c3e50`, gray text `#555`/`#666`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
