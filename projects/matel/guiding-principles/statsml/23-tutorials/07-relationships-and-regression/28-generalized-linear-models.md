# Generalized Linear Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Generalized Linear Models

**Subtitle:** One recipe — a straight line, a link function that adapts it to the outcome's range, and a matching noise family — covers rates, counts, and plain averages

## The Coupon That Broke the Straight Line

**Tags:** `core idea` (blue), `link function` (green), `logistic` (orange)

- **The coupon test** — a store emails coupons at 5–40% off and records the redemption rate at each level
- **The data** — rates climb 0.03, 0.08, 0.18, 0.38, 0.62, 0.82, 0.92, 0.97 as the discount rises
- **A straight line fails** — the best fit y = −0.20 + 0.031x predicts −4% at 5% off and 104% at 40% off
- **The fix** — keep the straight line, but pass it through an adapter that bends output into 0–1
- **The name** — that adapter is the link function; line + adapter + noise family = a GLM

*Example (italic):* At a 40% discount the straight line promises a 104% redemption rate — more redemptions than emails sent.

**Key point:** A GLM keeps linear regression's straight line but routes it through a link function, so predictions always land inside the outcome's legal range.

### Visualization (canvas `c1`, 720×300)

Single-panel scatter of the eight observed redemption rates with two overlaid fits: the straight line escaping the 0–1 band vs the logistic S-curve staying inside it.

- **Title (bold 15px, `#1a5276`, top center):** "Coupon Redemption Rate: Straight Line vs Logit Link".
- **Data:** discounts `[5, 10, 15, 20, 25, 30, 35, 40]` (% off); observed rates `[0.03, 0.08, 0.18, 0.38, 0.62, 0.82, 0.92, 0.97]`.
- **Axes:** origin x=60, plot width 620, baseline y=250, plot height 195; x range 0–45 (ticks every 5, labels 12px `#444` "5%"..."40%"); y range −0.15 to 1.15 with tick labels "0", "0.5", "1" (12px `#444`); axes 2px ink `#1a5276`.
- **Legal band:** dashed `#bdc3c7` (dash 4/3) horizontal lines at y-values 0 and 1, each with mute `#6b7280` 11px right-edge labels "rate = 0" and "rate = 1".
- **Observed points:** blue `#2a78d6` 5px dots at the eight (discount, rate) pairs.
- **Straight line:** orange `#d95926` 3px line for y = −0.20 + 0.031x drawn from x=0 (y=−0.20) to x=45 (y=1.20), crossing both dashed bounds.
- **S-curve:** green `#008300` 3px curve p = 1/(1+e^−(0.2x−4.5)) sampled at every 1 unit of x from 0 to 45.
- **Annotations:** magenta `#d55181` bold 12px near the line's top-right exit "line escapes: 104% at 40% off"; magenta bold 12px near its bottom-left exit "−4% at 5% off"; green bold 13px above the S-curve midpoint "logit link keeps every prediction in 0–1".
- **Caption (12px `#444`, bottom left):** "eight coupon levels, redemption rate per level (illustrative)".

## Three Parts, One Recipe

**Tags:** `worked example` (blue), `by hand` (green)

- **Part 1: the line** — the linear predictor η = 0.2×discount − 4.5 can output any number, −∞ to +∞
- **Part 2: the link** — the logit link's inverse p = 1/(1+e^−η) squashes any η into a 0–1 probability
- **Part 3: the family** — binomial noise treats each email as a coin flip with that probability p
- **By hand** — at 30% off: η = 0.2×30 − 4.5 = 1.5, and 1/(1+e^−1.5) = 0.82, an 82% redemption rate
- **Check another** — at 10% off: η = 0.2×10 − 4.5 = −2.5 gives p = 0.08, matching the observed 0.08

*Example (italic):* Plug in 20% off: η = −0.5, p = 1/(1+e^0.5) = 0.38 — every prediction on the page can be redone with a calculator.

**Key point:** Every GLM is the same three-part recipe — straight line, link adapter, noise family. Only the parts swap between problems; the recipe never changes.

### Visualization (canvas `c2`, 720×300)

Pipeline diagram: three rounded boxes (line → link adapter → prediction) with two worked number rows flowing left to right underneath.

- **Title (bold 15px, `#1a5276`, top center):** "The GLM Pipeline: Line, Adapter, Prediction".
- **Boxes (rounded 8px, 2px borders, y=70 to y=130):** box 1 at x=30 width 200, border blue `#2a78d6`, fill `rgba(42,120,214,0.08)`, bold 13px blue heading "1. linear predictor", 12px `#444` formula "η = 0.2x − 4.5"; box 2 at x=270 width 190, border violet `#4a3aa7`, fill `rgba(74,58,167,0.08)`, heading "2. link adapter", formula "p = 1/(1+e^−η)"; box 3 at x=500 width 190, border green `#008300`, fill `rgba(0,131,0,0.08)`, heading "3. prediction", formula "p always in 0–1".
- **Arrows:** 2px ink `#1a5276` arrows with solid triangular heads from box 1 to 2 (x=230→270) and box 2 to 3 (x=460→500), at y=100.
- **Worked row A (y=185, 13px):** "x = 30% off" in `#444` at x=55; bold blue "η = 1.5" at x=330; bold green "p = 0.82" at x=560; light 1px `#e5e9ef` connector line through the row.
- **Worked row B (y=225, 13px):** "x = 10% off" at x=55; bold blue "η = −2.5" at x=330; bold green "p = 0.08" at x=560; same connector styling.
- **Side note (bold 12px violet `#4a3aa7`, right of box 2 flow, y=155):** "the adapter is the only bent part — the line stays straight".
- **Caption (12px `#444`, bottom center, y=280):** "binomial family: each email then flips a coin with probability p".

## Same Recipe, New Outcome

**Tags:** `where it's used` (blue), `one framework` (green), `poisson` (orange)

- **Swap the parts** — the same store models daily orders vs ad spend: counts want a log link, Poisson family
- **The model** — η = 1.6 + 0.003×spend and orders = e^η, so a predicted count can never dip below zero
- **The numbers** — spend of $0 to $500 predicts 5, 6.7, 9, 12.2, 16.4, 22.2 orders per day
- **Multiplicative read** — each extra $100 multiplies orders by e^0.3 = 1.35, a steady +35% per step
- **One framework** — ordinary regression is a GLM too: identity link, normal family; nothing new to learn

*Example (italic):* Rates, counts, and plain averages all run on the one recipe — only the family line and the link line change.

**Key point:** GLMs unify regression: pick the family that matches the outcome, pick the link that maps its mean onto a straight line, and the rest is the machinery you already know.

### Visualization (canvas `c3`, 720×300)

Two-panel figure: a three-row family/link menu (left) and the Poisson log-link order curve (right), split by a vertical dashed divider at x=350.

- **Title (bold 15px, `#1a5276`, top center):** "One Framework: Swap the Family and the Link".
- **Left panel (menu, x=25 to x=330):** column headers bold 12px ink `#1a5276` "outcome / family / link" at y=60; three rows at y=100, y=150, y=200, each a rounded 6px pill-row with 13px text: row 1 "yes–no rate · binomial · logit" in green `#008300` on `rgba(0,131,0,0.07)`; row 2 "count per day · Poisson · log" in aqua `#199e70` on `rgba(25,158,112,0.08)`; row 3 "plain average · normal · identity" in blue `#2a78d6` on `rgba(42,120,214,0.07)`; footer 11px mute `#6b7280` at y=240 "same fitting machinery underneath all three".
- **Right panel (curve):** axis origin x=400, plot width 290, baseline y=245, plot height 185; x range spend $0–$500 (ticks at 0, 100, ..., 500, labels 11px `#444` "$0"..."$500"); y range 0–25 orders (labels "0", "10", "20").
- **Curve data:** aqua `#199e70` 3px curve orders = e^(1.6+0.003s) sampled every $25, with 5px aqua dots at spends `[0, 100, 200, 300, 400, 500]` and values `[5, 6.7, 9, 12.2, 16.4, 22.2]`; the two end dots labeled bold 12px aqua "5" and "22.2".
- **Zero floor:** dashed `#bdc3c7` (dash 4/3) line along y=0 with mute 11px label "log link: never below 0".
- **Annotation (bold 13px orange `#d95926`, above mid-curve):** "+$100 → ×1.35, every step".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=350 from y=38 to h-12.

## Reading the Coefficients

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Link-scale trap** — the logistic slope 0.2 is the per-point change in log-odds, not in probability
- **Mid-curve** — near 20% off (p = 0.38) one extra discount point adds about 4.7 points of probability
- **Near the top** — near 40% off (p = 0.97) the same +1 point adds only about 0.6 points
- **Same coefficient** — 0.2 never changed; the S-curve's steepness did — effects depend on where you stand
- **Count version** — the Poisson slope 0.003 per dollar reads as ×1.35 per $100, never "+1.35 orders"

*Example (italic):* A teammate multiplied 0.2 by 100 and reported "+20 points of redemption per discount point" — the true mid-curve effect is 4.7.

**Common mistake:** Reading GLM coefficients as raw outcome changes. They live on the link scale — exponentiate log-link slopes into multipliers, and remember logit slopes buy different probability changes at different points on the curve.

### Visualization (canvas `c4`, 720×300)

The fitted S-curve with short bold tangent segments at two discounts, showing the same coefficient buying a big probability step mid-curve and a tiny one near the top.

- **Title (bold 15px, `#1a5276`, top center):** "One Coefficient (0.2), Two Very Different Effects".
- **Curve:** green `#008300` 3px S-curve p = 1/(1+e^−(0.2x−4.5)) sampled every 1 unit; axis origin x=60, plot width 620, baseline y=250, plot height 195; x range 0–45 (ticks every 5, labels 12px `#444`); y range 0–1 (tick labels "0", "0.5", "1"); axes 2px ink `#1a5276`.
- **Point A (x=20, p=0.38):** blue `#2a78d6` 6px dot; blue 4px tangent segment with slope 0.047 per x-unit drawn from x=16 to x=24 through the dot; blue bold 13px callout above-left "+1 pt off → +4.7pp here".
- **Point B (x=40, p=0.97):** orange `#d95926` 6px dot; orange 4px tangent segment with slope 0.006 per x-unit drawn from x=36 to x=44 through the dot; orange bold 13px callout below-right "+1 pt off → +0.6pp here".
- **Guides:** dashed `#bdc3c7` (dash 4/3) vertical drop lines from each dot to the baseline, x-labels bold 12px "20%" (blue) and "40%" (orange).
- **Takeaway (bold 13px magenta `#d55181`, bottom center, y=285):** "the slope 0.2 lives on the log-odds scale — its probability payoff shrinks as the curve flattens".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
