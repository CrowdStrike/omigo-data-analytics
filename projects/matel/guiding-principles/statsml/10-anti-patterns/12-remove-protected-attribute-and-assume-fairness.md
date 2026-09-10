# Remove Protected Attribute and Assume Fairness

**Page type:** detail page (two card-sections, each an h2 + two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Remove Protected Attribute and Assume Fairness

**Subtitle:** Drop the race column — zip code, first name, university still encode it. Proxies remain

## The Anti-Pattern

Remove "race" from a lending model and assume fairness is achieved. But proxies remain: zip code encodes race and income (residential segregation, redlining legacy), first name signals ethnicity and gender, university attended tracks class and race. The model reconstructs the protected attribute from correlated features.

**Key point (red left border):** Dropping a column does not drop the information it carried. Correlated features act as proxies and produce the same discriminatory outcomes.

*Domain examples:*

- Hiring — resume screening uses first name as gender proxy
- Credit — zip code encodes race and income
- Insurance — proxy features reproduce redlining
- Policing — location data correlates with ethnicity
- Regulated domains — any feature correlated with protected class

### Visualization (canvas `c1`, 720×300)

Drawn feature table (monospace) showing the removed race column and the remaining proxy features, with a warning bracket.

- **Header row (bold 13px monospace, `#1a5276`, at y=30):** columns "FEATURE" (x=40), "ENCODES" (x=300), "STATUS" (x=520); underlined with a 1.5px `#1a5276` rule across the width.
- **Rows (13px monospace, row height 36, starting y=60):**
  - `race` — removed: row background `rgba(231,76,60,0.08)`; name in red `#e74c3c` with a 2px red strikethrough; ENCODES column bold red "[REMOVED]"; STATUS bold red "✘".
  - `zip_code` — proxy: row background `rgba(230,126,34,0.1)`; name orange `#e67e22`; ENCODES bold orange "race, income"; STATUS bold orange "⚠ PROXY".
  - `first_name` — proxy: same styling; ENCODES "ethnicity, gender"; STATUS "⚠ PROXY".
  - `university` — proxy: same styling; ENCODES "class, race"; STATUS "⚠ PROXY".
  - `experience_yrs` — safe: name gray `#555`; ENCODES green `#27ae60` "—"; STATUS bold green "✔ OK".
  - `debt_ratio` — safe: same styling as above.
- **Bracket:** red `#e74c3c` 2px right-side bracket spanning the three proxy rows (near x = w-55), with a short arrow to a bold 11px red "!".
- **Warning label (bold 15px system-ui, red, bottom center, y = h-25):** "⚠ Proxies still encode race!"

## The Design Pattern

Audit the correlation of EVERY feature with protected attributes. If r > 0.5: it's a proxy — remove it or apply fairness constraints. Test: does model output differ by protected group?

**Key point (red left border):** Fairness requires active auditing, not passive omission. Measure disparate impact on outputs, not just inputs.

*Steps:*

- Compute correlation of each feature with every protected attribute
- Flag any feature with |r| > 0.5 as a potential proxy
- Remove proxies or apply fairness-aware constraints
- Test model outputs for disparate impact across protected groups
- Re-audit after each model update or data refresh

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of feature correlations with the protected attribute, with a vertical threshold line at r = 0.5.

- **Title (bold 13px, top center, `#1a5276`):** "Feature Correlation with Protected Attribute (race) — illustrative"
- **Data (name: r):** `zip_code` 0.78, `first_name` 0.66, `university` 0.55, `debt_ratio` 0.15, `experience` 0.08.
- **Margins:** top 35, right 120, bottom 40, left 110. Bar height 32, even vertical gaps; bar width scaled to r on a 0–1.0 x-scale.
- **Bars:** proxy (r > 0.5) fill `rgba(231,76,60,0.75)` with `#e74c3c` 1.5px border; safe fill `rgba(39,174,96,0.75)` with `#27ae60` border. Feature names 12px monospace `#2c3e50` right-aligned left of bars. Value labels bold 12px monospace "r = 0.78" etc. — white inside the bar when bar width > 60px, otherwise `#2c3e50` just right of the bar.
- **Action labels (bold 12px, right of chart area):** red "✘ REMOVE" on proxy rows, green "✔ KEEP" on safe rows.
- **Threshold line:** vertical dashed orange (`#e67e22`, dash 6/4, width 2.5) at r=0.5, from just above the chart to below the axis; bold 12px orange centered label below: "threshold r = 0.5".
- **X-axis:** thin `#ccc` line under the bars with ticks and 10px `#888` labels at 0.0, 0.2, 0.4, 0.6, 0.8, 1.0.

## Regeneration instructions

- **Layout:** two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each with an `h2` and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (45%) with paragraph + `.key-point` + `.example` + `ul`, `td.viz-col` (55%) with the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px red `#e74c3c` left border, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem. The "r > 0.5" comparisons use the `&gt;` HTML entity in source. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; CSS width 100%. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
