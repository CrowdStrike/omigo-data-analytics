# Finance Domain: Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Finance Domain - Data Pitfalls

**Subtitle:** Financial data hides survivorship bias, look-ahead revisions, fat tails, regime shifts, and transaction costs — the traps that make backtests lie.

## Survivorship Bias in Funds

**Databases Drop the Losers, Lifting Reported Returns 2-4% a Year**

- Databases only contain funds that still exist today, creating an upward bias in reported returns
- Failed/merged/closed funds disappear from datasets, removing the worst performers
- Average hedge fund return inflated by 2-4% annually due to survivorship bias
- Backtesting on survivor-only data produces strategies that cannot be replicated
- Mutual fund databases lose ~3-5% of funds per year to closures

**Example:** A quant researcher backtests a momentum strategy on S&P 500 stocks from 2000-2020 using current index constituents. The strategy shows 18% annual returns. When delisted stocks are included (Enron, Lehman, WorldCom), returns drop to 9%.

### Visualization (canvas `canvas1`, 720×240)

Bar chart contrasting visible surviving-fund returns with invisible closed-fund losses.

- **Title (bold 17px, `#1a5276`):** "Fund Returns: Visible vs Closed".
- **Surviving funds (solid green `#27ae60` bars above the zero line, 28px wide, starting at x=30):** returns `[12.4, 15.1, 9.8, 11.2, 14.7, 8.9, 13.5, 16.2, 10.1, 12.8]` (%, scale 2.5 px/%).
- **Closed funds (translucent red bars below the zero line, fill `rgba(231,76,60,0.35)` with dashed `#e74c3c` outline, dash 4/3, starting at x=380):** returns `[-8.2, -15.4, -22.1, -5.7, -31.0, -12.3, -18.9, -7.4]`.
- **Zero line:** dark `#2c3e50` horizontal line across the chart.
- **Average line:** dashed green (dash 6/3, width 2) horizontal at +12.5% over the surviving group, labeled "Avg: +12.5%".
- **Bottom labels (13px):** green "Surviving Funds (visible)" left; red "Closed Funds (invisible in database)" right.

## Look-Ahead Bias / Backfill

**Your Backtest Reads the Revision the Market Had Not Yet Seen**

- Economic data (GDP, employment) is revised weeks or months after initial release
- Using revised data in backtests gives model access to information unavailable at decision time
- Hedge fund databases backfill returns when new funds join, importing their historical track record
- Point-in-time databases are essential but expensive and often unavailable

**Example:** US GDP Q3 2008 was initially reported as -0.3%. It was revised to -2.1% one month later, then to -6.3% in the final revision. A model using final revisions would have "known" the recession severity months early.

### Visualization (canvas `canvas2`, 720×240)

Two-line chart of quarterly GDP: preliminary release vs final revision.

- **Title (bold 17px, `#1a5276`):** "GDP Revisions: Preliminary vs Final".
- **X categories:** Q1-08, Q2-08, Q3-08, Q4-08, Q1-09, Q2-09, Q3-09, Q4-09 (11px `#2c3e50` labels at bottom).
- **Y axis:** −8% to +6% gridlines every 2% in `#ecf0f1` with 11px `#7f8c8d` labels; dark `#2c3e50` zero line. Origin x=80, y=140, scale 12 px/%.
- **Preliminary line (blue `#3498db`, width 2.5):** `[0.6, 1.9, -0.3, -3.8, -4.9, -1.0, 2.2, 5.7]`.
- **Final revision line (red `#e74c3c`, width 2.5):** `[-0.7, 0.5, -6.3, -8.2, -4.4, -0.6, 1.5, 4.5]`.
- **Legend (13px with color swatch bars):** blue "Preliminary (available at time)"; red "Final Revision (months later)".

## Fat Tails in Returns

**Kurtosis 25, Not 3 — the Million-Year Event Arrives Every Few**

- Financial returns have much heavier tails than a normal distribution predicts
- Events labeled "6-sigma" under normality occur every few years, not every million years
- VaR models using Gaussian assumptions systematically underestimate tail risk
- Kurtosis of daily S&P 500 returns is ~25, vs 3 for a normal distribution
- Black Monday 1987 was a 22-sigma event under normality - probability ~10^-100

**Example:** Long-Term Capital Management's models assumed normal distributions. Their portfolio experienced "25-sigma" daily moves in August 1998, events so improbable under Gaussian assumptions that they should never occur in the universe's lifetime.

### Visualization (canvas `canvas3`, 720×240)

Overlaid density curves: Gaussian vs fat-tailed return distribution with extreme events marked.

- **Title (bold 17px, `#1a5276`):** "Normal vs Actual Return Distribution".
- **X axis:** −6σ to +6σ, sigma tick labels every 2σ (11px `#2c3e50`); dark baseline; centered at canvas midpoint, scaleX 55 px/σ.
- **Normal curve (blue `#3498db`, width 2):** standard normal PDF, amplitude scale 150×2.5.
- **Fat-tailed curve (red `#e74c3c`, width 2):** mixture approximation `0.85·N(0,1)·0.75 + 0.15·N(0,2.5)`, amplitude scale 150×3.2 — lower peak, heavier tails.
- **Event markers (red 5px dots on the baseline with 10px labels):** "Black Monday 1987" at −4.8σ, "Mar 2020 Rebound" at +4.2σ, "Flash Crash" at −5.2σ.
- **Legend (13px, bottom):** blue "— Normal (Gaussian)"; red "— Actual Returns (fat tails)".

## Regime Changes

**Correlation 0.15 in Calm Markets, 0.85 in the Crash**

- Correlations between assets are not stable - they spike during crises
- Diversification fails precisely when it is most needed (correlations go to 1 in crashes)
- Models trained on calm markets catastrophically fail during regime shifts
- Interest rate regime changes (e.g., 2022 hiking cycle) invalidate decades of bond models

**Example:** Pre-2008, stocks and real estate had a correlation of 0.15. During the crisis, correlation jumped to 0.85. Portfolios "diversified" across these assets lost 40%+ because the historical low correlation vanished exactly when it mattered.

### Visualization (canvas `canvas4`, 720×240)

Side-by-side 4×4 correlation heatmaps: pre-crisis vs during crisis.

- **Title (bold 17px, `#1a5276`):** "Correlation Matrix: Pre vs Post 2008".
- **Assets (rows/columns):** Stocks, Bonds, Real Est, Commod; 42px cells; row labels on the left of the first matrix only.
- **Pre-2008 matrix (labeled "Pre-2008 (Normal)", at x=60):**
  `[[1.00, -0.20, 0.15, 0.10], [-0.20, 1.00, 0.05, -0.10], [0.15, 0.05, 1.00, 0.08], [0.10, -0.10, 0.08, 1.00]]`
- **Crisis matrix (labeled "During Crisis (2008)", at x=400):**
  `[[1.00, 0.60, 0.85, 0.78], [0.60, 1.00, 0.55, 0.50], [0.85, 0.55, 1.00, 0.72], [0.78, 0.50, 0.72, 1.00]]`
- **Cell color scale:** ≥0.7 `#c0392b`; ≥0.4 `#e74c3c`; ≥0.1 `#f5b7b1`; ≥−0.1 `#fdfefe`; ≥−0.3 `#aed6f1`; below `#2980b9`. Each cell shows its value to 2 decimals in 11px `#2c3e50`.
- **Between matrices:** red `#e74c3c` right-pointing arrow with 11px red label "Crisis!".

## Transaction Costs Eating Alpha

**22% Gross Becomes 4% Net Once Execution Is Priced In**

- Backtests assume zero or fixed transaction costs; reality is variable and path-dependent
- Slippage, market impact, and bid-ask spreads scale with order size and urgency
- High-frequency strategies lose 60-90% of gross alpha to execution costs
- Strategies requiring small-cap or illiquid instruments face severe capacity constraints
- Short selling costs (borrow fees) can exceed 20% annually for hard-to-borrow stocks

**Example:** A small-cap value strategy backtests at 22% gross annual return. After including 1.5% bid-ask spread per trade, 0.8% market impact, 12x annual turnover, and borrow fees, net return is 4% - barely beating the risk-free rate.

### Visualization (canvas `canvas5`, 720×240)

Grouped bar chart: gross vs net returns by strategy.

- **Title (bold 17px, `#1a5276`):** "Gross vs Net Returns After Costs".
- **Strategies (x categories, 11px `#2c3e50` labels):** Momentum, Mean Rev., Stat Arb, HFT, Value.
- **Gross returns (blue `#3498db` bars, 35px wide):** `[18.5, 14.2, 22.8, 45.0, 12.1]` %.
- **Net returns (red `#e74c3c` bars beside each gross bar):** `[9.2, 6.8, 8.4, 6.2, 9.8]` %.
- **Value labels:** each bar's percent value in 11px `#2c3e50` above the bar. Y scale max 50%; dark `#2c3e50` baseline.
- **Legend (13px with swatches):** blue "Gross Return"; red "Net Return (after costs)".

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then per pitfall an `<h2>` heading (1.4em `#1a5276` with 2px `#2980b9` bottom border) followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (a refined restatement of the pitfall name, never a copy of it) + bullet list + `.example` callout, right `<td>` (60%, centered) holds the canvas. Even table rows get background `#fafcfe`.
- **Callouts:** `.example` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 14px, 0.9em, with bold "Example:" lead-in. (A `.philosophy` style — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em — is defined but unused.)
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** all canvases 720×240 intrinsic; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. Chart fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c` (dark `#c0392b`), purple `#8e44ad`, dark slate `#2c3e50`, gray `#7f8c8d`.
