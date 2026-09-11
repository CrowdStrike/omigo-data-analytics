# Hierarchical Bayes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hierarchical Bayes

**Subtitle:** When one group has little data, a hierarchical model lets it borrow strength from its siblings — the small store's estimate leans toward the chain average, by exactly as much as its thin data deserve

## A New Store Borrows From the Chain

**Tags:** `core idea` (blue), `partial pooling` (green), `borrowing strength` (orange)

- **The chain** — a coffee chain's 20 stores average $600/day in sales, spread about ±$100 apart
- **The newcomer** — one store open only 4 days has averaged $900/day — is it really that good?
- **Two voices** — its own 4 days say $900; its 19 siblings say new stores land near $600
- **The blend** — hierarchical Bayes averages the two voices and estimates $750/day
- **Partial pooling** — every store keeps its own estimate, but each one leans toward the chain

*Example (italic):* Four lucky days near a stadium event can fake a $900 average — the chain-wide prior tempers the hype to $750.

**Key point:** A store's estimate is a compromise between its own data and the chain's experience; the less data it has, the more it borrows.

### Visualization (canvas `c1`, 720×300)

Horizontal dollar axis with the 8 illustrated established-store averages as dots, plus a row above showing the new store's raw mean being pulled to its blended estimate.

- **Title (bold 15px, `#1a5276`, top center):** "20-Store Chain: the New Store's $900 Gets Pulled to $750 (illustrative)".
- **Axis:** horizontal 2px `#999` line at y=210 from x=70, width 580, mapping $380 (left) to $950 (right); ticks with 12px `#444` labels at $400, $500, $600, $700, $800, $900.
- **Established stores:** 8 blue `#2a78d6` 6px dots ON the axis at values `[440, 500, 550, 585, 615, 650, 700, 760]`; heading 12px `#444` just above the dot cluster: "established stores (8 of 20 shown), long-run averages".
- **Chain mean:** vertical dashed `#1a5276` (dash 4/3) line at $600 from y=70 to y=210, bold 12px ink label "chain mean $600" beside it.
- **New store row (y=115):** magenta `#d55181` 7px open-ring dot at $900 with bold 13px magenta label "new store, 4 days: $900" above; green `#008300` 7px filled dot at $750 with bold 13px green label "blended estimate: $750" above; 3px green arrow from the $900 ring to the $750 dot (arrowhead at $750).
- **Caption (12px `#444`, bottom center):** "the estimate moves toward the chain, not all the way".

## Blending Prior and Data, By Hand

**Tags:** `worked example` (blue), `posterior` (green)

- **Prior** — before day one, the chain says a new store's true rate is near $600, give or take $100
- **Data** — 4 days averaging $900 with $200 day-to-day noise pin the mean to ±$100 (= 200/√4)
- **Equal say** — prior uncertainty $100 and data uncertainty $100 match, so each voice gets 50%
- **Posterior** — 0.5 × $900 + 0.5 × $600 = $750, with uncertainty shrunk to about ±$71
- **Weights shift** — the store's own weight is τ²/(τ² + σ²/n): more days, more say

*Example (italic):* With 16 days at the same $900 average, the data's error drops to ±$50, its weight rises to 80%, and the estimate moves to $840.

**Key point:** The posterior mean is a precision-weighted average of prior and data — whichever voice is more certain gets more weight.

### Visualization (canvas `c2`, 720×300)

Three bell curves on one dollar axis: the chain prior, the 4-day likelihood, and the posterior sitting exactly between them, taller because it is more certain.

- **Title (bold 15px, `#1a5276`, top center):** "Prior × Likelihood = Posterior: $600 and $900 Meet at $750".
- **Axis:** baseline 2px `#999` at y=250 from x=60, width 620, mapping $250 (left) to $1,150 (right); ticks with 12px `#444` labels at $300, $450, $600, $750, $900, $1,050.
- **Curves (deterministic formula, no randomness):** each drawn by looping x over the axis and plotting y = baseline − peak · exp(−0.5·((v−m)/s)²):
  - prior: m=600, s=100, peak=120px, blue `#2a78d6` 3px line, bold 13px blue label "prior: chain $600 ± $100" near its peak;
  - likelihood: m=900, s=100, peak=120px, magenta `#d55181` 3px line, bold 13px magenta label "data: 4 days, $900 ± $100" near its peak;
  - posterior: m=750, s=71, peak=170px, green `#008300` 3px line, bold 13px green label "posterior: $750 ± $71" above its peak.
- **Posterior marker:** vertical dashed green (dash 4/3) line at $750 from the posterior peak down to the baseline, 12px `#444` tick label "$750" below.
- **Annotation (bold 12px `#199e70`, aqua, lower right):** "narrower than both parents: two clues beat one".
- **Caption (12px `#444`, bottom left):** "equal ±$100 uncertainties → an exact 50/50 blend".

## Where the Prior Comes From

**Tags:** `hyperprior` (orange), `core idea` (blue), `where it's used` (green)

- **Not hand-picked** — nobody typed in $600; the chain mean is estimated from all 20 stores at once
- **Hyperprior** — the chain-level mean μ and spread τ get their own vague prior, one level up
- **Three levels** — hyperprior → chain (μ = $600, τ = $100) → store means → daily sales (σ = $200)
- **Everything flows** — one store's data sharpens the chain estimate, which re-shrinks every store
- **Where it's used** — per-region conversion rates, per-hospital mortality, per-user preferences

*Example (italic):* Opening 5 more stores wouldn't just estimate those 5 — their data would tighten μ and τ and re-shrink all 25 estimates.

**Key point:** "Hierarchical" means the prior's own parameters are uncertain and learned from the groups — the data sets its own prior.

### Visualization (canvas `c3`, 720×300)

Three-level hierarchy diagram, top to bottom, with arrows showing how the prior at each level generates the quantities below it.

- **Title (bold 15px, `#1a5276`, top center):** "Three Levels: Hyperprior → Chain → Stores → Daily Sales".
- **Level 1 (y≈70):** rounded rectangle (violet `#4a3aa7` 2px border, fill `rgba(74,58,167,0.08)`) centered at x=360, ~330×44px, bold 13px violet text "hyperprior: μ and τ unknown, vague prior" with 11px `#6b7280` second line "the chain average is itself uncertain".
- **Level 2 (y≈150):** rounded rectangle (blue `#2a78d6` 2px border, fill `rgba(42,120,214,0.08)`) centered at x=360, ~400×44px, bold 13px blue text "chain: store means ~ Normal(μ = $600, τ = $100)"; inside the box, 8 small 4px blue dots spread proportionally at `[440, 500, 550, 585, 615, 650, 700, 760]` over a mini-axis $400–$800 to echo the c1 stores.
- **Level 3 (y≈235):** rounded rectangle (green `#008300` 2px border, fill `rgba(0,131,0,0.08)`) centered at x=360, ~430×44px, bold 13px green text "each day's sales ~ Normal(store mean, σ = $200)" with 11px `#6b7280` second line "new store observed: 4 days averaging $900".
- **Arrows:** 3px `#6b7280` vertical arrows (arrowheads down) connecting level 1 → 2 and level 2 → 3, each with an 11px `#6b7280` side label: "generates the store means" and "generates the daily data".
- **Annotation (bold 12px orange `#d95926`, right side, two lines):** "information also flows UP:" / "every store's data updates μ and τ" with a thin dashed orange upward arrow alongside levels 3 → 1.

## Neither Ignore the Chain Nor Erase the Store

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **No pooling** — trusting each store's raw mean alone lets 4 lucky days claim $900
- **Complete pooling** — averaging everything calls every store $600 and erases real differences
- **Partial pooling** — the middle road shrinks each store toward $600 by exactly its noise level
- **Unequal pulls** — 4 days: $900 → $750 (50% own weight); 60 days: $585 → $586 (94%)
- **Middling pulls** — 12 days: $700 → $675 (75% own weight); 6 days: $430 → $498 (60%)

*Example (italic):* Store D's grim $430 over 6 days shrinks to $498 — likely a rough opening week, not a doomed location.

**Common mistake:** Treating the raw store mean as "the truth" and shrinkage as bias — with few days the raw mean is mostly noise, and the shrunk estimate predicts next week's sales better.

### Visualization (canvas `c4`, 720×300)

Four-store dumbbell chart on a vertical dollar axis: open ring = raw mean, filled dot = partial-pooled estimate, arrow showing the pull toward the chain mean; short-history stores move far, the 60-day store barely moves.

- **Title (bold 15px, `#1a5276`, top center):** "Shrinkage Is Proportional to Noise: Raw Mean → Pooled Estimate".
- **Vertical axis:** 2px `#999` line at x=70 from y=50 to y=255, mapping $950 (top) to $380 (bottom); ticks with 12px `#444` labels at $400, $500, $600, $700, $800, $900.
- **Chain mean:** horizontal dashed `#1a5276` (dash 4/3) line across the plot at $600, bold 12px ink label "chain mean $600" at its right end.
- **Data (four columns at x = 200, 330, 460, 590):**
  - Store A — "4 days": raw 900 → pooled 750;
  - Store B — "60 days": raw 585 → pooled 586;
  - Store C — "12 days": raw 700 → pooled 675;
  - Store D — "6 days": raw 430 → pooled 498.
- **Marks per column:** magenta `#d55181` 7px open ring at the raw value with 12px magenta label showing the raw dollar amount; green `#008300` 7px filled dot at the pooled value with bold 12px green label showing the pooled amount; 3px green vertical arrow from ring to dot (arrowhead at the dot; for Store B draw only a 4px stub, no arrowhead, since it moves $1).
- **Column labels (below axis baseline, 12px `#444`, two lines):** "Store A" / "4 days", "Store B" / "60 days", "Store C" / "12 days", "Store D" / "6 days".
- **Annotation (bold 13px green, upper right, two lines):** "few days → pulled hard;" / "60 days → barely moves".
- **Caption (12px `#444`, bottom center):** "own-data weight: A 50%, B 94%, C 75%, D 60% (illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data integrity:** all arrays and values hardcoded exactly as specified above (no `Math.random()`); the bell curves in `c2` use the closed-form Gaussian formula only. The shrinkage arithmetic is internally consistent with τ = $100, σ = $200: own-data weight w = τ²/(τ² + σ²/n) gives w = 0.5 (n=4), 0.9375 (n=60), 0.75 (n=12), 0.6 (n=6), and pooled = w·raw + (1−w)·600.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
