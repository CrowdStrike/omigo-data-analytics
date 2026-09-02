# Log Transformation: Dangers and Interpretability

**Page type:** detail page (backlog kusto-style two-column layout: text left 45%, canvas right 55%, one table row per section)
**HTML title tag:** Log Transformation: Dangers and Interpretability — Discussion Backlog

**Subtitle:** The most common fix for skew changes more than the shape

**Intro callout:** Log transformation is the most commonly applied "fix" for right-skewed data. But it changes data in ways rarely acknowledged — compressing scale, altering what tests compare, and breaking interpretability.

## 1. Core Problems

- **Scale compression:** $50K, $200K, $1M become 10.8, 12.2, 13.8 — nearly indistinguishable
- **Interpretability loss:** "mean difference of 0.3 in log space" means nothing intuitive
- **Zero/negative values:** log(0) undefined, log(negative) undefined. Hacks introduce artifacts
- **Changed test semantics:** t-test on log data tests GEOMETRIC means, not arithmetic
- **Shape doesn't transfer:** "Normal in log space" = log-normal in original space
- **Threshold trap:** "log(cholesterol) > 5.8" means "cholesterol > 330" — non-obvious

*Example (italic):* A 20x difference ($50K vs $1M) collapses to only 1.3x after the transform.

### Visualization (canvas `c1`, 720×300)

Side-by-side horizontal bar comparison: original dollar scale vs log scale, with a `log()` arrow between halves.

- **Title (bold 16px, `#1a5276`, top center):** "Scale Compression: Original vs Log Transform"
- **Left half — "Original Scale"** (label centered in `#1a5276` at top of left half): 7 horizontal bars, one per value, right-aligned labels left of each bar; bar length proportional to frequency (max freq 25). Data (label, value, freq): `$30K`/30/15, `$50K`/50/25, `$70K`/70/20, `$100K`/100/12, `$200K`/200/5, `$500K`/500/2, `$1M`/1000/1. Bars fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 0.5. Layout: marginTop 30, marginBottom 50, each half is (w/2 − 20) wide; bar height = (h − 80)/7 − 3.
- **Center arrow:** red (`#e74c3c`) right-pointing arrow at mid-height with label "log()" above in 12px red.
- **Right half — "Log Scale (compressed!)"** (label centered in `#1a5276`): 7 horizontal bars, bar length scaled linearly from logMin 10 to logMax 14. Data (label = log value, original): 10.3/$30K, 10.8/$50K, 11.2/$70K, 11.5/$100K, 12.2/$200K, 13.1/$500K, 13.8/$1M. Bars for the top 3 log values (12.2, 13.1, 13.8 — indices 4-6) fill `rgba(231,76,60,0.3)` stroke `#e74c3c`; the rest fill `rgba(26,82,118,0.35)` stroke `#1a5276`.
- **Bottom annotation (bold 13px, `#e74c3c`, centered):** "20x difference ($50K vs $1M) becomes only 1.3x in log!"

## 2. What to Do Instead

- Detect log-normal shape via CNN — flag explicitly
- Use non-parametric test (Mann-Whitney) on original scale
- If transform chosen: document exactly what quantity is compared
- Never silently transform

**Key Questions:**
(1) Ever recommend log-transform?
(2) Auto-route to Mann-Whitney?
(3) Box-Cox generalization?

### Visualization (canvas `c2`, 720×300)

Two-box semantics diagram plus a concrete-example callout box.

- **Title (bold 16px, `#1a5276`, top center):** "What t-test on log(x) Actually Tests"
- **Left box** (280×80 at x=40, y=50, fill `#e8f8e8`, stroke `#27ae60` width 2): heading "What you THINK you test:" (bold 14px `#27ae60`), line "H0: mean(A) = mean(B)" (15px `#2c3e50`), line "(arithmetic mean difference)" (14px).
- **Right box** (280×80 at x=w−320, same y, fill `#fde8e8`, stroke `#e74c3c` width 2): heading "What you ACTUALLY test:" (bold 14px `#e74c3c`), line "H0: geomean(A) = geomean(B)" (15px `#2c3e50`), line "(geometric mean RATIO)" (14px).
- **Arrow between boxes:** horizontal red (`#e74c3c`) arrow at box mid-height, labeled "log()" above in 12px red.
- **Example box** (x=40, y=155, width w−80, height 100, fill `#f8f9fa`, stroke `#2980b9` width 1):
  - Centered heading (bold 15px `#1a5276`): "Concrete Example: Income Comparison"
  - Left-aligned lines (14px `#2c3e50`): "Group A incomes: $40K, $50K, $60K, $200K" / "Group B incomes: $45K, $55K, $65K, $70K" / "Arithmetic means: A=$87.5K vs B=$58.75K (A higher by $28.75K)"
  - Final line in red `#e74c3c`: "Log means test geometric: A=$72.5K vs B=$57.9K (much closer! outlier dampened)"

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem), then one `.lang-section` per numbered section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with a single `<tr>` — left `td.text-col` (45%) holds bullets/example/key-point, right `td.viz-col` (55%) holds the canvas. No index number in the h1.
- **Key-point callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem with 4px li spacing; canvases `width: 100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `rgba(26,82,118,0.35)` bar fill, accents `#2980b9`.
- **Canvas:** intrinsic width/height attributes as given (720×300 each); scale via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setupCanvas(id)` helper.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
