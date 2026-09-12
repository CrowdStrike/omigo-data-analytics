# Anchoring, Decoys & Loss Aversion

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Anchoring, Decoys & Loss Aversion

**Subtitle:** The first number you see drags your estimate toward it — menus plant decoy prices and "don't lose it" wording to steer choices the same way

## The $9 Sign That Writes the Answer

**Tags:** `core idea` (blue), `anchoring` (orange), `estimates` (green)

- **The setup** — a smoothie shop shows half its customers a "$3?" sign and half a "$9?" sign
- **The question** — each customer then guesses what the new smoothie is really worth
- **The pull** — the $3 group averages $4.60; the $9 group averages $7.10 — for the same drink
- **Anchoring** — the first number seen drags every later estimate toward it, even a useless one
- **No escape** — knowing the sign is arbitrary barely helps; the pull works on experts too

*Example (italic):* Same smoothie, same day: a sign reading $9 instead of $3 lifted the average guess by $2.50.

**Key point:** The first number in view becomes the starting point, and people adjust away from it too little. Whoever plants the anchor steers the estimate.

### Visualization (canvas `c1`, 720×300)

Dual-panel histogram: value guesses from 20 customers who saw the "$3?" sign (left) vs 20 who saw the "$9?" sign (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Guessed Worth of the Same Smoothie, by Sign Shown (illustrative)".
- **Data:** eight $1-wide bins with edge labels "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9" (12px `#444` below each bar); low-anchor counts `[2, 5, 6, 4, 2, 1, 0, 0]`; high-anchor counts `[0, 0, 1, 3, 5, 6, 4, 1]` (each sums to 20 customers).
- **Left panel (low anchor):** axis origin x=55, width 280, baseline y=240, chart height 170, scale max 7; bars fill `rgba(42,120,214,0.45)`, 1px stroke blue `#2a78d6`; heading bold 12px `#444` "saw the $3? sign"; blue dashed vertical mean line at $4.60 with bold 12px blue label "mean $4.60".
- **Right panel (high anchor):** axis origin x=400, width 280, same baseline/height/scale; bars fill `rgba(217,89,38,0.45)`, 1px stroke orange `#d95926`; heading "saw the $9? sign"; orange dashed mean line at $7.10 with bold 12px orange label "mean $7.10".
- **Annotation (bold 13px magenta `#d55181`, centered near y=282, bottom takeaway):** "same drink — the sign moved the average guess by $2.50".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Adding a Size Nobody Buys

**Tags:** `worked example` (blue), `decoy effect` (orange), `pricing` (green)

- **Menu A** — two sizes: small $4 and large $7; out of 100 customers, 68 pick small, 32 large
- **Menu B** — the shop adds a medium at $6.50 that is barely smaller than the large
- **The flip** — now 25 pick small, 6 pick medium, 69 pick large; large jumps from 32 to 69
- **The decoy** — the medium exists to make the large look like a bargain, not to be bought
- **Anchor cousin** — a decoy is a planted comparison; it warps choices like a sign warps guesses

*Example (italic):* The $6.50 medium sells only 6 cups out of 100, yet its presence more than doubles large sales.

**Key point:** A decoy changes what people choose without changing the options they actually pick. Evaluate a lineup as a whole — one item can exist purely to reprice its neighbor.

### Visualization (canvas `c2`, 720×300)

Dual-panel bar chart: size choices out of 100 customers under Menu A (two sizes, left) vs Menu B (decoy medium added, right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "100 Customers Choose a Size: Before vs After the Decoy (illustrative)".
- **Data:** Menu A choices `[68, 32]` for "small $4" and "large $7"; Menu B choices `[25, 6, 69]` for "small $4", "medium $6.50", "large $7".
- **Left panel (Menu A):** axis origin x=55, width 280, baseline y=240, chart height 170, scale max 75; heading bold 12px `#444` "Menu A: two sizes"; two bars — small fill `rgba(42,120,214,0.5)` (blue), large fill `rgba(0,131,0,0.5)` (green); size + price labels 12px `#444` below each bar, bold 12px count labels above each bar in the bar's color.
- **Right panel (Menu B):** axis origin x=400, width 280, same baseline/height/scale; heading "Menu B: decoy medium added"; three bars — small blue fill as left, medium fill `rgba(201,133,0,0.5)` (yellow `#c98500`), large green fill as left; same label styling; bold 12px yellow label "the decoy" above the medium bar's count.
- **Annotation (bold 13px green `#008300`, upper right area of right panel):** "large: 32 → 69".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Losing $10 Hurts More Than Winning $10 Feels Good

**Tags:** `core idea` (blue), `loss aversion` (red), `framing` (orange)

- **Two coupons** — "get $2 off" vs "you'll lose your $2 off unless you use it today"
- **Redemption** — the gain wording gets 34 of 100 coupons used; the loss wording gets 61
- **The curve** — gaining $10 feels like +8 happy points; losing $10 feels like −17 (illustrative)
- **The ratio** — losses weigh roughly twice as much as equal gains, across many experiments
- **Loss aversion** — people fight harder to keep something than to gain the very same thing

*Example (italic):* Rewording the same $2 coupon from "get $2 off" to "don't lose your $2 off" nearly doubled redemptions.

**Key point:** The felt-value curve is about twice as steep for losses as for gains. Frame the same offer as a loss to avoid and behavior changes, with nothing else different.

### Visualization (canvas `c3`, 720×300)

Dual-panel: the loss-aversion value curve (left) and redemption rates for the two coupon wordings (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Losses Loom Larger: the Value Curve and a Coupon Test (illustrative)".
- **Left panel (value curve):** plot area x=55 to x=335, y=45 to y=250; x axis maps dollars −10 to +10 (zero at x=195), y axis maps felt value −18 to +9 (zero at y≈95); thin 1px `#e5e9ef` zero lines through the origin; axis labels 12px `#6b7280` "lose $10", "$0", "gain $10" below and "felt value" rotated at left.
- **Gain branch:** green `#008300` 3px line through points (dollars, feel) `(0,0), (2,2), (4,3.7), (6,5.2), (8,6.6), (10,8)` with a 4px dot at (10,8); bold 12px green annotation "+$10 feels like +8".
- **Loss branch:** magenta `#d55181` 3px line through `(0,0), (−2,−4.5), (−4,−8.3), (−6,−11.5), (−8,−14.4), (−10,−17)` with a 4px dot at (−10,−17); bold 12px magenta annotation "−$10 feels like −17".
- **Right panel (coupon test):** axis origin x=400, width 280, baseline y=240, chart height 170, scale max 70; heading bold 12px `#444` "same $2 coupon, two wordings"; bar "get $2 off" = 34, fill `rgba(42,120,214,0.5)` (blue); bar "don't lose your $2 off" = 61, fill `rgba(213,81,129,0.5)` (magenta); wording labels 12px `#444` below, bold count labels "34 / 100" and "61 / 100" above each bar in the bar's color.
- **Takeaway (bold 13px `#1a5276`, bottom center):** "loss framing: same offer, nearly double the response".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Anchor Inside Your Own Forecast

**Tags:** `common mistake` (red), `where it's used` (blue)

- **Planted number** — teams first asked "over or under 2 weeks?" then estimate a median of 5 weeks
- **Same project** — teams asked "over or under 20 weeks?" estimate 14 weeks; it actually took 9
- **No anchor** — teams asked with no number in the question land near 8 weeks, closest to truth
- **Dashboards** — last quarter's figure sitting on a dashboard anchors every forecast made from it
- **Surveys** — asking "would you pay $50?" before "what would you pay?" inflates every answer

*Example (italic):* One planted number in the question moved the same project's median estimate from 5 weeks to 14.

**Common mistake:** Treating an estimate as independent of how it was asked. Collect the number before showing any reference value — otherwise the anchor, not the evidence, is the answer.

### Visualization (canvas `c4`, 720×300)

Single-panel bar chart: median schedule estimates for the same project under three question framings, with a dashed reference line at the actual duration.

- **Title (bold 15px, `#1a5276`, top center):** "Same Project, Three Question Framings (illustrative)".
- **Data:** bars "no anchor" = 8, "anchored at 2 weeks" = 5, "anchored at 20 weeks" = 14 (weeks); actual duration = 9 weeks.
- **Axes:** origin x=70, width 560, baseline y=240, chart height 175, y scale 0–16 weeks with ticks at 0/4/8/12/16 (12px `#6b7280`, thin `#e5e9ef` gridlines).
- **Bars:** centered, ~110px wide; "no anchor" fill `rgba(0,131,0,0.5)` (green), "anchored at 2 weeks" fill `rgba(42,120,214,0.5)` (blue), "anchored at 20 weeks" fill `rgba(217,89,38,0.5)` (orange); framing labels 12px `#444` below each bar; bold 13px value labels "8 wk", "5 wk", "14 wk" above each bar in the bar's color.
- **Actual line:** dashed magenta `#d55181` (dash 5/4) horizontal line at 9 weeks across the plot, bold 12px magenta label "actually took 9 weeks" at its right end.
- **Annotation (bold 13px `#1a5276`, top right of plot):** "the anchor in the question moved the estimate by 9 weeks".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays — no randomness; invented numbers carry an "(illustrative)" label in each chart title.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
