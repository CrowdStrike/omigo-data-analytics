# Time Series Basics

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Time Series Basics

**Subtitle:** How to read, clean, and forecast data that arrives in time order — one concept per page, each taught through a single concrete example.

## Cards

Each card links to a topic page under `time-series/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | COMPONENTS | Trend | [09-time-series-basics/01-trend.md](09-time-series-basics/01-trend.md) | The slow, steady direction a series drifts in once you ignore the day-to-day wiggles. | long-run direction, growth, drift |
| 2 | COMPONENTS | Seasonality | [09-time-series-basics/02-seasonality.md](09-time-series-basics/02-seasonality.md) | Patterns that repeat on a fixed clock — weekends, holidays, mornings — again and again. | repeating cycles, weekly pattern, calendar effects |
| 3 | COMPONENTS | Noise | [09-time-series-basics/03-noise.md](09-time-series-basics/03-noise.md) | The random jitter left over after trend and season — the part no model can predict. | randomness, residuals, irreducible error |
| 4 | COMPONENTS | Decomposition | [09-time-series-basics/04-decomposition.md](09-time-series-basics/04-decomposition.md) | Splitting one messy line into trend, season, and noise so each piece can be studied alone. | trend + season + noise, additive, multiplicative |
| 5 | WORKING WITH TIME | Moving Averages | [09-time-series-basics/05-moving-averages.md](09-time-series-basics/05-moving-averages.md) | Averaging each point with its neighbors to see the shape behind the noise. | rolling window, window size, lag |
| 6 | WORKING WITH TIME | Smoothing | [09-time-series-basics/06-smoothing.md](09-time-series-basics/06-smoothing.md) | Giving recent points more weight than old ones so the smooth line reacts faster to change. | exponential weights, recency, alpha |
| 7 | WORKING WITH TIME | Lag & Autocorrelation | [09-time-series-basics/07-lag-and-autocorrelation.md](09-time-series-basics/07-lag-and-autocorrelation.md) | Checking how much today's value resembles yesterday's — the series remembering itself. | lag, self-correlation, memory |
| 8 | WORKING WITH TIME | Stationarity | [09-time-series-basics/08-stationarity.md](09-time-series-basics/08-stationarity.md) | A series whose average and spread stay put over time — and why models quietly assume it. | stable mean, differencing, model assumption |
| 9 | FORECASTING | Naive Baselines | [09-time-series-basics/09-naive-baselines.md](09-time-series-basics/09-naive-baselines.md) | Dead-simple forecasts like "same as yesterday" that fancy models must beat to earn their keep. | last value, seasonal naive, benchmark |
| 10 | FORECASTING | Why Forecasting Is Hard | [09-time-series-basics/10-why-forecasting-is-hard.md](09-time-series-basics/10-why-forecasting-is-hard.md) | Regime changes, one-off shocks, and compounding errors — why the future keeps surprising models. | regime change, shocks, error growth |
| 11 | FORECASTING | Forecast Intervals | [09-time-series-basics/11-forecast-intervals.md](09-time-series-basics/11-forecast-intervals.md) | Reporting a range instead of one number, and why that range fans out the further ahead you look. | uncertainty band, horizon, coverage |
| 12 | FORECASTING | Backtesting: No Peeking at the Future | [09-time-series-basics/12-backtesting-no-peeking-at-the-future.md](09-time-series-basics/12-backtesting-no-peeking-at-the-future.md) | Testing a forecast the honest way — train on the past, score on what came after, never mix the two. | time-ordered split, rolling origin, leakage |
| 13 | MODELS & FILTERS | ARIMA | [09-time-series-basics/13-arima.md](09-time-series-basics/13-arima.md) | Forecasting with three moves — difference away the climb, lean on the last value, correct for the last miss. | differencing, autoregression, moving average |
| 14 | MODELS & FILTERS | State-Space Models | [09-time-series-basics/14-state-space-models.md](09-time-series-basics/14-state-space-models.md) | Treating a series as a smooth hidden state plus noisy readings, then recovering the line from the dots. | hidden state, observation noise, latent line |
| 15 | MODELS & FILTERS | The Kalman Filter | [09-time-series-basics/15-the-kalman-filter.md](09-time-series-basics/15-the-kalman-filter.md) | Blending a noisy prediction with a noisy measurement, weighted by how much you trust each — the blend beats both. | predict + correct, noisy sensor, weighted blend |
| 16 | PATTERNS & ALIGNMENT | Changepoint Detection | [09-time-series-basics/16-changepoint-detection.md](09-time-series-basics/16-changepoint-detection.md) | Finding the moment a process shifts to a new normal by scanning for the biggest before/after gap. | level shift, before/after, new normal |
| 17 | PATTERNS & ALIGNMENT | Dynamic Time Warping | [09-time-series-basics/17-dynamic-time-warping.md](09-time-series-basics/17-dynamic-time-warping.md) | Stretching the time axis so two curves tracing the same shape at different speeds line up before comparing. | elastic matching, sequence alignment, shape distance |
| 18 | FREQUENCY DOMAIN | Fourier Transform | [09-time-series-basics/18-fourier-transform.md](09-time-series-basics/18-fourier-transform.md) | Reading a wiggly signal as a recipe of simple waves — which wave speeds are in the mix, and how big each is. | sum of waves, frequency view, spectrum |
| 19 | FREQUENCY DOMAIN | The FFT | [09-time-series-basics/19-the-fft.md](09-time-series-basics/19-the-fft.md) | The same Fourier transform computed cleverly — halving the work over and over cuts n² multiplies to n log n. | divide and conquer, n log n, speedup |
| 20 | FREQUENCY DOMAIN | Sampling Theorem & Aliasing | [09-time-series-basics/20-sampling-theorem-and-aliasing.md](09-time-series-basics/20-sampling-theorem-and-aliasing.md) | Snapshot a fast cycle too slowly and the samples trace a different, slower cycle — the backwards wagon wheel. | sample rate, Nyquist, aliasing |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "COMPONENTS" `#2980b9`, "WORKING WITH TIME" `#27ae60`, "FORECASTING" `#8e44ad`, "MODELS & FILTERS" `#d35400`, "PATTERNS & ALIGNMENT" `#16a085`, "FREQUENCY DOMAIN" `#c0392b`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
