# Crypto & DeFi — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, primary distribution canvas middle ~31%, insight canvas right ~31%; one table per section)
**HTML title tag:** Crypto & DeFi — Distribution Patterns

## Token Returns — So Extreme That Averages Are Meaningless

**Pitfall label (color `#795548`):** WILD SWINGS — NO RELIABLE AVERAGE

Imagine a game where most days are small wins or losses, but once in a while a single day is bigger than the previous five years combined. The simulated token returns here — drawn from a Cauchy distribution, a standard model for extreme heavy tails — behave like that: the swings are so large that the "average return" never settles down. Collect a year of data, then two, and the average you compute will be wildly different each time, because for this shape the average does not exist. Classic portfolio math (risk scores, return-per-unit-of-risk ratios, loss predictions) assumes it does.

- The extreme crashes and pumps dominate any average you try to compute
- More data does NOT help — adding history never makes the estimate settle down
- Any tool that scores risk with an average or a spread has nothing stable to hold onto in this shape
- Standard portfolio balancing assumes the average exists — for this distribution it does not

### Visualization (canvas `canvas1`, 420×340)

Histogram (shared `drawHistogram` helper) of Cauchy-like daily token returns.

- **Title:** "Token Daily Returns (Cauchy-like)".
- **Data generation:** seeded mulberry32(42); 5,000 draws of Cauchy via ratio of two standard normals, scaled by 0.03 (~3% typical move).
- **Bins/axes:** 80 bins; display trimmed to the central 96% of values; x-axis label "Return", 5 x ticks at 2 decimals.
- **Colors:** bar fill `rgba(230,126,34,0.35)`, stroke `#1a5276`; density line `#d35400` 2px with SE band `rgba(230,126,34,0.18)`; axes `#ccc`; title bold 13px `#1a5276`; padding top 40, right 20, bottom 40, left 50; white background.

### Visualization (canvas `canvas1b`, 400×340)

QQ-plot of the token returns against a normal distribution.

- **Title (bold 12px, `#1a5276`):** "QQ-Plot: Token Returns vs Normal"; **subtitle (10px `#e74c3c`):** "Tails explode away from the line = infinite variance".
- **Points:** ~200 empirical quantiles vs theoretical normal quantiles (inverse-normal rational approximation), drawn as 2.5px dots `rgba(230,126,34,0.7)`; x range [-3.5, 3.5]; y trimmed to the 2nd–98th percentile.
- **Reference:** dashed (6/3) green `rgba(39,174,96,0.8)` 2px normal reference line (mean 0, sd 0.03), labeled 10px "Normal reference".
- **Annotations:** bold 11px `#e74c3c` "FAT TAIL" labels with 2px red arrows at the upper-right and lower-left tail divergences.
- **Axes:** `#ccc` frame; x title "Theoretical Normal Quantiles"; rotated y title "Sample Quantiles" (10px `#555`); padding top 40, right 20, bottom 40, left 50; white background.

## Wallet Balances — 1% of Wallets, About 40% of the Coins

**Pitfall label (color `#2980b9`):** EXTREME CONCENTRATION

Think of a room of 10,000 people where the hundred richest hold nearly as much as everyone else put together. The simulated wallet balances here follow a power law — the shape repeatedly reported in studies of real token holdings, often in even steeper form. In this simulation the top 1% of wallets holds about 40% of the entire supply, and the ten largest wallets alone hold over a fifth. For a system marketed as "decentralized," the Lorenz curve is the quickest way to check how true that is for any given token.

- The top 1% of simulated wallets holds about 40% of the supply; the ten largest wallets alone hold over a fifth
- The Gini coefficient here is about 0.74 — one number summarizing the whole bow of the curve
- "Decentralized" is a testable claim: the Lorenz curve and Gini measure it directly
- How steep the curve is tells you how concentrated any given token really is

### Visualization (canvas `canvas2`, 420×340)

Histogram (shared `drawHistogram` helper) of power-law wallet balances.

- **Title:** "Wallet Balances (Power Law)".
- **Data generation:** 5,000 Pareto draws with alpha ≈ 1.05 (very heavy): `1 / (1-u)^(1/1.05)`.
- **Bins/axes:** 70 bins; display trimmed to the central 92%; x-axis label "Balance (largest 8% trimmed for display)", integer x ticks.
- **Colors:** bar fill `rgba(231,76,60,0.35)`, stroke `#1a5276`; density line `#d35400` with SE band; standard helper layout.

### Visualization (canvas `canvas2b`, 400×340)

Lorenz curve with Gini coefficient badge.

- **Title (bold 12px, `#1a5276`):** "Lorenz Curve: Wallet Wealth Concentration"; **subtitle (10px `#e74c3c`):** "Distance from the diagonal = concentration".
- **Curves:** perfect-equality diagonal dashed (5/3) green `rgba(39,174,96,0.8)` 2px, labeled 10px "Perfect equality"; Lorenz curve of the sorted balances `rgba(231,76,60,0.9)` 3px, labeled `#e74c3c` "Crypto wallets"; the area between them filled `rgba(231,76,60,0.15)`.
- **Gini badge:** bold 14px `rgba(231,76,60,0.9)` "Gini = 0.NNN" (computed, ~0.74) with a 2px red arrow pointing into the gap.
- **Annotation (bold 11px `#1a5276`, right-aligned):** "Top 1% holds NN% of supply" (computed, ~40%).
- **Axes:** `#ccc` frame; x title "Cumulative % of Wallets"; rotated y title "Cumulative % of Wealth" (10px `#555`); padding top 40, right 20, bottom 45, left 50; white background.

## DEX Swap Sizes — Small Fish and Whales in the Same Pool

**Pitfall label (color `#27ae60`):** TWO HUMPS — TWO SEPARATE WORLDS

Picture a swimming pool used by both toddlers and Olympic divers at the same time. The simulated DEX (decentralized exchange) swaps here show exactly that: a big cluster of everyday-sized trades around $50-$500, a near-empty stretch in the middle, and a second cluster of massive $100K+ swaps. Two very different populations share one pool, and no single "typical swap" describes both.

- One cluster at $50-$500 — everyday-sized swaps
- A second cluster at $100K+ — whale-sized and bot-sized swaps
- The near-empty gap between them is the classic signature of two populations mixed into one dataset
- The average swap here is about $55K — hundreds of times the typical small trade, a third of the typical whale trade; it describes neither group

### Visualization (canvas `canvas3`, 420×340)

Histogram (shared `drawHistogram` helper) of bimodal swap sizes on a log10 scale.

- **Title:** "DEX Swap Sizes (log10 USD)".
- **Data generation:** 5,000 swaps: 72% retail log-normal `exp(5.3 + 0.7·randn())` (~$200 center); 28% whale log-normal `exp(11.9 + 0.8·randn())` (~$150K center); plotted as log10(USD).
- **Bins/axes:** 60 bins; display trimmed to the central 99%; x-axis label "log10(USD)", 1-decimal ticks.
- **Colors:** bar fill `rgba(41,128,185,0.35)`, stroke `#1a5276`; density line `#d35400` with SE band; standard helper layout.

### Visualization (canvas `canvas3b`, 400×340)

Scatter strip of swap size vs transaction sequence, highlighting the empty middle band.

- **Title (bold 12px, `#1a5276`):** "Swap Size vs Sequence: Two Separate Markets"; **subtitle (10px `#2980b9`):** "Almost nothing trades between $1K and $30K".
- **Points:** ~800 subsampled swaps; x = sequence index (time proxy), y = log10(USD), y range 1–14; 2px dots colored — under $1,000 retail blue `rgba(41,128,185,0.6)`; over $10,000 whale red `rgba(231,76,60,0.6)`; in between yellow `rgba(241,196,15,0.7)`.
- **Dead zone:** horizontal band between log10 = 3 and 4.5 filled `rgba(241,196,15,0.2)` with dashed (4/3) `rgba(241,196,15,0.7)` edges, labeled bold 10px `rgba(180,130,0,0.9)` "DEAD ZONE"; a `#b8860b` 1.5px arrow with 9px label "Almost nobody here".
- **Legend (top left, bold 10px):** blue swatch "Retail ($50-$500)"; red swatch "Whales ($100K+)".
- **Axes:** y ticks "$10^2" … "$10^13" every 2 with faint `#eee` gridlines; x title "Transaction Sequence (time proxy)"; rotated y title "Swap Size (log USD)"; `#ccc` frame; padding top 40, right 20, bottom 45, left 55; white background.

## Gas Fees — Stable Most of the Time, Then Sudden Spikes

**Pitfall label (color `#e74c3c`):** CALM THEN SUDDEN EXPLOSION

Think of a highway that's smooth until it hits full capacity, then instantly turns into a parking lot. The simulated gas fees here look like that: most blocks sit at a calm, predictable level, and the rest are scattered along a long expensive tail — the worst block costs about 30 times the calm price. There is no wide middle regime. One explanation: the fee mechanism is designed to hold prices steady until blocks fill, then hand pricing over to a bidding war.

- Most of the time (about 82% of blocks), fees sit in a tight, predictable band
- The rest stretch into a long tail — up to roughly 30x the calm level in this simulation
- The ECDF races to ~82% within a few gwei, then crawls — that corner is the regime change
- Consistent with a mechanism that switches modes rather than degrading gradually — a cliff, not a slope

### Visualization (canvas `canvas4`, 420×340)

Histogram (shared `drawHistogram` helper) of gas fees with a calm cluster plus exponential tail.

- **Title:** "Gas Fees (Gwei) — Phase Transition".
- **Data generation:** 5,000 blocks: 82% normal operation `25 + 3·randn()` gwei (floored at 5); 18% congestion `25 + Exponential(rate 0.008)`.
- **Bins/axes:** 70 bins; display trimmed to the central 97%; x-axis label "Gas Price (Gwei)", integer ticks.
- **Colors:** bar fill `rgba(39,174,96,0.35)`, stroke `#1a5276`; density line `#d35400` with SE band; standard helper layout.

### Visualization (canvas `canvas4b`, 400×340)

ECDF chart annotating the phase-transition cliff.

- **Title (bold 12px, `#1a5276`):** "ECDF: Gas Fee Phase Transition"; **subtitle (10px `#27ae60`):** "82% of blocks are calm, then sudden spike".
- **Series:** ECDF of sorted gas prices (x trimmed at the 97th percentile) in `rgba(39,174,96,0.9)` 2.5px.
- **Reference:** horizontal dashed (4/3) `rgba(41,128,185,0.7)` line at 82% labeled bold 10px "82% (normal ops)".
- **Phase-transition band:** vertical band from 28 to 35 gwei filled `rgba(231,76,60,0.12)`, labeled bold 11px `#e74c3c` two-line "PHASE" / "TRANSITION" with a downward red arrow.
- **Zone labels (10px):** green "Calm (EIP-1559 base)" bottom-left; red "Auction chaos" top-right.
- **Axes:** x ticks in gwei (5 intervals), title "Gas Price (Gwei)"; y ticks 0%–100% in 20% steps, rotated title "Cumulative Probability"; `#ccc` frame; padding top 40, right 20, bottom 45, left 55; white background.

## NFT Prices — A Massive Pile at Floor Price Is a Distress Signal

**Pitfall label (color `#8e44ad`):** PILE-UP AT THE BOTTOM + LONG TAIL UP

Imagine a used car lot where most cars are priced at the absolute minimum — that lot is not doing well. The simulated collection here has the same look: about half of all sales sit within a hair of the floor price, a spread of higher-priced sales above it, and the occasional far-out outlier. A pile-up at the floor is consistent with holders selling just to get out; sales spread across many price points look more like genuine price discovery. The rare outlier sale doesn't change the picture either way.

- Pile-up at floor price — consistent with holders dumping to exit, not buyers competing upward
- Prices spread smoothly across a range — looks like genuine interest and price discovery
- The share of sales at the floor is a quick, readable health check for a collection
- In this simulation about half of all sales sit at the floor — the spike in the histogram is unmissable

### Visualization (canvas `canvas5`, 420×340)

Histogram (shared `drawHistogram` helper) of NFT sale prices with a floor spike.

- **Title:** "NFT Sale Prices (ETH) — Floor Spike + Log-Normal".
- **Data generation:** 5,000 sales, floor price 0.05 ETH: 45% floor spike `floor + rng()·0.005`; ~52% log-normal body `exp(ln(floor) + 1.2 + 0.8·randn())` (floored at floor price); ~3% extreme outliers `floor · (50 + 200·rng())`.
- **Bins/axes:** 70 bins; display trimmed to the central 95%; x-axis label "Price (ETH)", 2-decimal ticks.
- **Colors:** bar fill `rgba(142,68,173,0.35)`, stroke `#1a5276`; density line `#d35400` with SE band; standard helper layout.

### Visualization (canvas `canvas5b`, 400×340)

Waterfall decomposition of collection health into floor dumps, healthy trades, and whale outliers.

- **Title (bold 12px, `#1a5276`):** "Waterfall: NFT Collection Health Decomposition"; **subtitle (10px `#8e44ad`):** "NN% of sales at the floor — the waterfall shows the split" (computed floor share, ~45-50%).
- **Segments (computed from the data):** "Total Sales" 100% in `rgba(52,73,94,0.7)`; "Floor Dumps" (price < floor+0.01) in `rgba(231,76,60,0.75)`; "Healthy Trades" (price < 30× floor) in `rgba(39,174,96,0.75)`; "Whale Outliers" in `rgba(142,68,173,0.75)`; drops connected by dashed `#aaa` connector lines; white bold 12px percentage labels centered on each bar; two-line category labels 10px `#333` beneath.
- **Health indicator (bold 13px, centered below):** "Diagnosis: DISTRESSED" in `#e74c3c` when floor share > 30% (else "HEALTHY" in `#27ae60`); when distressed, a red arrow labeled bold 9px "DUMP SIGNAL" points at the floor bar.
- **Axes:** y ticks 0%–100% in 25% steps with faint `#eee` gridlines; padding top 45, right 15, bottom 50, left 50; white background.

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per section, single `<tr>` with three `<td>`s: text cell (38%) holding `<span class="pitfall-label">`, `<h3>`, `<p>`, `<ul>`; middle cell (31%, centered) with the primary canvas (width=420, height=340); right cell (31%, centered) with the insight canvas (width=400, height=340).
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; table cells `1px solid #2980b9`, 12px padding; h3 `#1a5276` 1.0em weight 700; p 14px line-height 1.6; li 14px line-height 1.5; `.pitfall-label` inline-block bold 0.72em uppercase letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned in document order from the cycling array `["#795548", "#2980b9", "#27ae60", "#e74c3c", "#8e44ad", "#e67e22", "#16a085", "#d35400", "#c0392b", "#1abc9c"]` via a small script setting `style.color` on each `.pitfall-label`.
- **Charts:** seeded mulberry32(42) RNG shared sequentially across all cards, with Box-Muller `randn()` and inverse-CDF `randExp(lambda)` helpers. Shared `drawHistogram(canvasId, data, options)` helper: scales for sharpness (set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor); optional symmetric percentile trimming for display (`trimPercentile`); white background, title bold 13px `#1a5276`, axes `#ccc`, bars with `#1a5276` 0.5px strokes, Gaussian-kernel smoothed density line `#d35400` 2px with 95% SE band `rgba(230,126,34,0.18)` (sigma 1.5, effective N clamped [30, 200]), 5 x ticks with configurable decimals, x-axis label 11px `#555`. Right-column insight charts (QQ-plot, Lorenz curve, scatter strip, ECDF, waterfall) are custom-drawn with the same dpr scaling and styling.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` / `#d35400`, purple `#8e44ad`, yellow `rgba(241,196,15,…)`, slate `rgba(52,73,94,…)`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
