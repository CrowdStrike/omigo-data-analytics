# Distribution Gallery: 100+ Real-World Patterns

**Page type:** other (histogram gallery: 17 category sections, each an h2 plus a 3-column grid of cards; every card holds a title, tag pills, a short description, and one procedurally generated histogram canvas — cards are not links)
**HTML title tag:** Distribution Gallery - 100+ Real-World Patterns

**Subtitle:** Real data is messy. These examples show what actual feature distributions look like — spikes, gaps, clusters, mixed shapes, and combinations that don't fit any textbook formula.

(A final script line appends " (N examples shown)" to the subtitle at runtime, where N = number of `.card` elements; 164 on this page.)

## Shared chart machinery (applies to all 164 canvases)

Every card canvas is 560×240 (intrinsic attributes), displayed at width 100%, height 203px, background #fefefe, border 1px solid #ecf0f1, radius 4px. All data comes from one seeded PRNG stream shared across the whole page, so exact reproduction requires generating the cards in document order.

- **PRNG:** `mulberry32(42)` — state a: a = (a + 0x6D2B79F5)|0; t = imul(a ^ (a>>>15), 1|a); t = (t + imul(t ^ (t>>>7), 61|t)) ^ t; return ((t ^ (t>>>14))>>>0) / 4294967296. One global `rng` feeds every helper below.
- **Shorthand used in the tables:** r = rng(); Z = randn() = Box-Muller sqrt(−2·ln u)·cos(2π·v) with u, v fresh rng() draws (redrawn while zero); Exp(λ) = −ln(1 − rng())/λ; U(a,b) = a + rng()·(b−a); Par(α,xm) = xm/(1 − rng())^(1/α). "n× expr" means push n independent draws of expr; each Z, r, Exp, U, Par occurrence is a fresh draw.
- **Histogram (drawHistogram):** bin count = opts.bins if given, else min(60, max(15, floor(n/5))); range = [min, max] of the data (widened by ±1 if min = max); bin indices clamped into range. Bars: fill in the card color (default #3498db) at globalAlpha 0.5, bar width = canvasW/numBins with a 1px gap, heights normalized to the max bin count over H − 8px (4px top/bottom padding).
- **SE band (drawBand), overlaid on every histogram:** Gaussian-weighted smoothing of bin heights with σ = 1.2 bins, kernel radius ceil(3σ) = 4; winsorizing — smoothed value capped at 2× the raw bin height, and for zero-height bins capped at 2× the nearest nonzero neighbor within the kernel radius; effN = min(200, max(30, n)); band = smoothed ± 1.96·smoothed/√effN (lower edge floored at 0), filled rgba(230,126,34,0.25); smoothed center line through bin centers, stroke #1a5276, width 2.
- **Two-dataset overlay (category 11 only):** the data function returns [neg, pos]; neg is drawn as the main histogram, pos is binned on the same range, normalized to its own max, scaled ×0.8, filled with opts.color2 at alpha 0.4, and gets its own SE band (also scaled ×0.8).
- **Card DOM (createCard):** `<div class="card"><h3>title</h3><p><span class="tag">tag</span>… description</p><canvas width="560" height="240"></canvas></div>`.

## 1. Spike & Point Mass Patterns

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #1: Single Spike at Zero | zero-inflated, count | Most values are exactly 0, with a thin spread of positives. Common in count data, inactive accounts. | 800× 0; 200× Exp(0.3) |
| #2: Spike at Round Number | default-value, placeholder | Data clusters at 100 (default/placeholder value) with actual values scattered around. | 400× 100; 600× Z·30+70 |
| #3: Multiple Spikes (Tiered Pricing) | pricing, discrete-continuous | Values concentrate at 9.99, 19.99, 29.99, 49.99 with tiny noise between. | prices [9.99, 19.99, 29.99, 49.99] with weights [40, 30, 20, 10]: for each price, weight·8× price+Z·0.1; then 50× U(5,55) |
| #4: Spike at Max (Capped Sensor) | censored, sensor, ceiling | Sensor maxes out at 1023. Real values below, plus pile-up at ceiling. | 600× Z·200+700, each mapped min(v,1023); then 300× 1023 |
| #5: Spike at Min (Floor Effect) | floor, censored | Scores cannot go below 0. Many hit the floor, rest spread upward. | 1000× max(0, Z·15−5) |
| #6: Two Spikes + Gap | binary-like, flags | Binary-like but not quite: values cluster at 0 and 1 with a few in between. | 400× Z·0.02; 400× 1+Z·0.02; 50× U(0.1,0.9) |
| #7: Spike at -1 (Missing Encoded) | missing-encoded, sentinel | Missing values encoded as -1. Real data is positive and right-skewed. | 350× −1; 650× Exp(0.1)+5 |
| #8: Narrow Spike + Long Tail | heavy-tail, outlier-rich | 90% of values in tight range 0-5, then rare extreme values up to 500. | 900× Exp(1); 100× Par(1.2, 5) |
| #9: Spike + Uniform Spread | default-value, mixed | Half the data at exactly 50 (default), other half uniformly distributed 0-100. | 500× 50; 500× U(0,100) |
| #10: Multiple Spikes (Categorical Encoded) | encoded-categorical, ordinal | Categories encoded as 1,2,3,4,5 but stored as float. Slight noise from data errors. | cats [1..5] with counts [200, 350, 180, 150, 80]: count× cat+Z·0.01; then 40× U(1,5) |
| #11: Spike + Bimodal Background | zero-inflated, bimodal | Default value at 0, plus two separate populations at 30 and 70. | 300× 0; 350× Z·8+30; 350× Z·8+70 |
| #12: Weekend Spike (Hour-of-Day) | temporal, periodic | Activity data: spike at hours 9,12,17 with lower activity elsewhere. | 200× 9+Z·0.5; 150× 12+Z·0.5; 250× 17+Z·0.8; 400× U(0,24) |

## 2. Gaps & Clusters

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #13: Two Distinct Clusters | bimodal, gap | Clear gap between 20-40. Like age groups: young adults (18-25) and middle-aged (45-65). | 500× Z·3+22; 500× Z·7+55 |
| #14: Three Clusters Unequal Size | multimodal, unequal | Small cluster, large cluster, medium cluster with gaps between. | 100× Z·2+10; 600× Z·5+50; 300× Z·3+85 |
| #15: Dense Core + Isolated Outliers | outliers, tight-core | 95% of data in tight range, 5% scattered far away in both directions. | 950× Z·5+50; 25× U(−50,20); 25× U(80,150) |
| #16: Clusters at Powers of 10 | log-scale, orders-of-magnitude | Values cluster near 1, 10, 100, 1000. Log-scale structure. | 250× 10^(Z·0.1); 300× 10^(1+Z·0.15); 250× 10^(2+Z·0.12); 200× 10^(3+Z·0.1) |
| #17: One Dense + One Sparse Cluster | asymmetric, sparse-cluster | Primary cluster has 90% of data; secondary cluster at higher range is sparse. | 900× Z·3+25; 100× Z·10+80 |
| #18: Staircase Clusters | banded, salary-like | Data at 5 levels with gaps. Like salary bands or rating levels. | levels [30, 45, 60, 80, 110] with ns [150, 250, 300, 200, 100]: n× level+Z·2 |
| #19: Gap in Middle (Donut) | bimodal, polarized | Values at extremes but empty in center. Like confidence scores: high or low, rarely medium. | 500× Z·8+15; 500× Z·8+85; 30× U(40,60) |
| #20: Irregular Gaps | multi-range, irregular | Data exists in ranges [0-10], [25-35], [60-65], [90-100]. Random gaps. | 300× U(0,10); 250× U(25,35); 200× U(60,65); 250× U(90,100) |
| #21: Cluster + Scatter | mixed, noise | One tight cluster at 50, with individual scattered points everywhere else. | 700× Z·3+50; 300× U(0,100) |
| #22: Progressively Wider Clusters | heteroscedastic, widening | First cluster tight, each subsequent one wider. Like measurement uncertainty growing. | 250× Z·1+10; 250× Z·4+35; 250× Z·9+65; 250× Z·15+100 |

## 3. Skewed & Heavy-Tail

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #23: Extreme Right Skew (Income) | income-like, exponential | Most values low, long tail to the right. Classic income or transaction amount. | 1000× Exp(0.02)+20 |
| #24: Right-Skew (Response Times) | right_skew, latency | Multiplicative process. Bulk of values 50-200ms, tail extends to 5000ms. | 1000× exp(Z·0.8+4) |
| #25: Power-Law (City Sizes) | power-law, heavy-tail | Few very large values dominate. Most values tiny. | 1000× Par(1.5, 1) |
| #26: Pareto 80/20 | pareto, inequality | 80% of values below 20, remaining 20% spread across 20-1000. | 1000× Par(1.16, 5) |
| #27: Right Skew + Ceiling | truncated, ceiling | Would be exponential but hits a max at 100. Pile-up at boundary. | 1000× min(100, Exp(0.03)) |
| #28: Left Skew (Test Scores) | left-skew, scores | Most people score high, few score very low. Like an easy exam. | 1000× max(0, 100−Exp(0.08)) |
| #29: Extreme Kurtosis (Fat Tails) | leptokurtic, fat-tails | Looks normal-ish in the center but has way more extreme values than expected. | 900× Z·10+50; 100× Z·50+50 |
| #30: Exponential Decay with Noise | noisy, exponential | Decaying signal with measurement noise overlaid. | 1000× Exp(0.5)+Z·0.3, then all mapped max(0,v) |
| #31: Weibull (Time to Failure) | weibull, reliability | Infant mortality early, then steady failure rate. Shape parameter < 1. | 1000× 10·(−ln(1−r))^(1/0.5) (Weibull shape 0.5, scale 10) |
| #32: Half-Normal (Absolute Errors) | half-normal, errors | Only positive values, density highest at zero, decays smoothly. | 1000× abs(Z·20) |
| #33: Chi-Squared (k=2) | chi-squared, variance | Highly right-skewed, common in statistical tests and variance distributions. | 1000× Z1²+Z2² (two independent Z draws per value) |
| #34: Right Skew + Point Mass at Zero | zero-inflated, spending | Many exact zeros (no purchase), then exponential spending for buyers. | 400× 0; 600× Exp(0.05)+0.01 |

## 4. Multimodal & Mixed

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #35: Bimodal Equal (Two Populations) | bimodal, subpopulations | Two distinct populations mixed together. Like male/female heights. | 500× Z·3+165; 500× Z·3+178 |
| #36: Bimodal Very Unequal | bimodal, asymmetric | Primary mode has 85% of data, secondary mode is a small bump. | 850× Z·8+40; 150× Z·5+80 |
| #37: Trimodal (Three Shifts) | trimodal, shifts | Work shift data: morning, afternoon, night clusters. | 350× Z·1+7; 400× Z·1+14; 250× Z·1+22 |
| #38: Four Peaks (Quarterly) | periodic, quarterly | Data peaks at Q1-Q4 end dates with valleys between. | 250× Z·2+3; 300× Z·2+6; 250× Z·2+9; 200× Z·2+12 |
| #39: Modes + Connecting Bridge | bimodal, bridged | Two modes connected by low-density bridge rather than clean gap. | 400× Z·5+25; 400× Z·5+65; 200× U(30,60) |
| #40: Mixture of Narrow + Wide | mixed-variance, overlapping | One tight spike-like mode + one wide spread mode at same location. | 500× Z·2+50; 500× Z·15+50 |
| #41: Skewed + Symmetric Mix | mixed-shape, heterogeneous | Exponential population mixed with normal population. | 500× Exp(0.1); 500× Z·5+30 |
| #42: Many Small Modes (Comb) | rounding, comb-pattern | Regular spacing of peaks. Like rounding to nearest 5. | 1000× floor(r·20)·5 + Z·0.8 |
| #43: Bimodal + Heavy Tail | bimodal, tail | Two modes in main body, plus extreme outliers forming a tail. | 400× Z·5+30; 400× Z·5+60; 200× Par(2, 70) |
| #44: Overlapping Exponentials | mixed-exponential, shoulder | Two exponential populations with different rates, creating a shoulder. | 600× Exp(0.5); 400× Exp(0.1)+2 |
| #45: Five Unequal Modes | multimodal, complex | Complex real-world: 5 distinct populations with different sizes and spreads. | 150× Z·2+10; 300× Z·4+30; 100× Z·1+50; 250× Z·6+70; 200× Z·3+95 |

## 5. Bounded, Truncated & Censored

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #46: Beta-like (Probabilities) | bounded, U-shaped | Values between 0 and 1, U-shaped: most values near 0 or 1. | 500× r^0.3; 500× 1−r^0.3 |
| #47: J-shaped (Ratings) | ratings, J-shaped | Mostly 5s and 1s, few in between. Like product ratings. | 350× 5+Z·0.1; 200× 1+Z·0.1; 100× 4+Z·0.1; 50× 3+Z·0.1; 80× 2+Z·0.1 |
| #48: Truncated Normal (Positive Only) | truncated, positive-only | Underlying normal with mean near zero, but negatives impossible. | 1000 attempts of v=Z·20+10, keep only v>0 (final n < 1000) |
| #49: Bounded Uniform + Edge Pileup | bounded, edge-effects | Uniform in [0,100] but 0 and 100 have extra mass (boundary effects). | 100× 0; 100× 100; 800× U(0,100) |
| #50: Percentage Bunching | anchoring, percentage | Percentages that bunch at 0%, 25%, 50%, 75%, 100%. | anchors [0, 25, 50, 75, 100]: 120× anchor+Z·2 each; then 400× U(0,100) |
| #51: Right-Censored (Survival) | censored, survival | Study ended at time 365. Many observations censored at that point. | 700× min(365, Exp(0.005)); 300× 365 |
| #52: Double-Bounded Sigmoid | sigmoid, bounded | Values compressed between 0-1 with S-curve density. Like transformed probabilities. | 1000× 1/(1+exp(−x)) with x=Z·2 |
| #53: Triangular (Estimates) | triangular, estimates | Peak at mode, linear decay to min and max. Common in project estimates. | 1000×: u=r; if u<0.6 push 5+sqrt(u/0.6)·15, else push 20−((1−u)/0.4)·10 |
| #54: Interval-Censored (Age Ranges) | interval, age-bands | Only know age is in a range: 18-24, 25-34, 35-44, etc. Represented as midpoints. | mids [21, 29.5, 39.5, 49.5, 59.5, 69.5] with ns [200, 300, 250, 150, 70, 30]: n× mid+U(−3,3) |
| #55: Wrapped/Circular (Angles) | circular, angles | Data on a circle: values near 0/360 are actually close together. | 400× (Z·30+350) mod 360; 300× (Z·20+90) mod 360; 300× U(0,360) |

## 6. Sparse & Small-Sample

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #56: Only 15 Data Points | tiny-n, uncertain | Barely any data. Cannot determine shape. Each point matters. | 15× Z·10+50 (bins: 8) |
| #57: 30 Points with One Outlier | outlier-dominant, small-n | Small sample where single outlier dominates range and skews everything. | 29× Z·5+50; then single value 200 (bins: 10) |
| #58: Sparse Everywhere | sparse, no-pattern | Data points scattered with no clear pattern. N=50 over wide range. | 50× U(0,1000) (bins: 20) |
| #59: Mostly Empty Bins | rare-events, sparse | Large range but data only in few spots. Like rare event times. | 20× Exp(0.001) (bins: 15) |
| #60: Dense Region + Lone Points | cluster+outliers, sparse-tail | Cluster of 80 points near 50, plus 5 lone points scattered far away. | 80× Z·3+50; then singles 5, 12, 88, 95, 150 (bins: 25) |
| #61: All Same Value Except One | near-constant, anomaly | Constant feature with single anomaly. Nearly zero variance. | 99× 42; single 43.5 (bins: 10) |
| #62: Two Points Per Region | ultra-sparse, ambiguous | Extremely sparse: 2-3 points in each of several distant locations. | literal array [10, 11, 30, 31, 32, 55, 56, 78, 79, 80, 120, 121] (bins: 15) |
| #63: Sparse with Wide Confidence | ambiguous-shape, low-power | N=25 points. Shape could be anything - normal, uniform, skewed all plausible. | 25× exp(Z·0.5+3) (bins: 10) |

## 7. Discrete & Categorical-Like

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #64: Integer Counts (0-10) | discrete, counts | Discrete counts. Poisson-like: most are 0-3, rare values 7+. | 1000× Binomial(10, 0.2): v = number of 10 trials with r<0.2 |
| #65: Binary (0/1) Imbalanced | binary, imbalanced | 95% zeros, 5% ones. Like rare disease indicator. | 950× 0; 50× 1 (bins: 5) |
| #66: Discrete Uniform (Dice) | discrete-uniform, equal | Equal probability for each value 1-6. | 1000× floor(r·6)+1 (bins: 6) |
| #67: Zipf (Word Frequencies) | zipf, power-law | Rank 1 has huge count, rank 2 half that, rapid decay. | 1000× min(floor(1/r^(1/1.5)), 50) |
| #68: Geometric (Trials Until Success) | geometric, waiting-time | First success at trial 1,2,3,... Monotone decreasing. | 1000×: t=1; while r>0.3 increment t; push t (geometric, p=0.3) |
| #69: Discrete with Forbidden Values | discrete, gaps | Integers 1-20 but never 7 or 13. Like superstition or system rules. | draw v=floor(r·20)+1 repeatedly, rejecting v=7 and v=13, until 1000 accepted |
| #70: Negative Binomial (Overdispersed) | overdispersed, counts | Like Poisson but with extra variance. Clumpy counts. | 1000×: r=3, p=0.3; v = total failures across 3 geometric runs (each run: t=1; while rng()>p { t++; v++ }); push v |
| #71: Benford First Digit | benford, digits | First digits follow log distribution: 1 most common, 9 rarest. | 1000×: v=10^(r·4); push first decimal digit of floor(v) (bins: 9) |
| #72: Score with Grade Inflation | inflated, scores | Discrete 0-100 but bunched at top: 85,90,95,100 dominate. | 50× floor(r·70); 150× floor(U(70,85)); 300× floor(U(85,95)); 500× floor(U(95,100)) |

## 8. Real-World Domain Patterns

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #73: Income (USA-like) | income, real-world | Median ~60k, long right tail to millions. Spike at min wage. Gap in 200k-500k. | 200× U(15,25) (min wage); 500× exp(Z·0.5+10.8) (bulk); 200× Exp(0.00002)+100000 (high earners); 50× Par(1.5, 200000) (ultra-rich); all mapped max(0,v) |
| #74: Website Session Duration | session, web | Spike at 0-5sec (bounces), mode at 30sec, long tail to 30min. | 300× U(0,5) (bounces); 400× Exp(0.05)+10 (normal sessions); 200× Exp(0.01)+60 (engaged); 100× U(300,1800) (left tab open) |
| #75: Blood Pressure (Systolic) | medical, bimodal | Main mode at 120, small mode at 140+ (hypertension). Hard floor at ~80. | 700× Z·12+118; 200× Z·10+145; 100× Z·15+160; all mapped max(80,v) |
| #76: E-Commerce Order Value | ecommerce, pricing | Spikes at $0 (free), $9.99, $19.99. Exponential tail. Zero-inflated. | 150× 0; 200× 9.99+Z·0.5; 150× 19.99+Z·0.5; 300× Exp(0.03)+5; 100× Par(2, 50); all mapped max(0,v) |
| #77: Credit Score | credit, bounded | Range 300-850. Left-skewed, pile-up near 750-800. Floor at 300. | 1000× clamp(850−Exp(0.008), 300, 850) |
| #78: File Sizes (Bytes) | file-size, log-scale | Bimodal: small files (configs) ~1KB, large files (media) ~5MB. Log scale. | 400× exp(Z·1+7) (~1KB); 300× exp(Z·0.8+13) (~500KB); 200× exp(Z·0.5+15.5) (~5MB); 100× exp(Z·1+18) (large) |
| #79: Insurance Claims | insurance, zero-inflated | Most zero (no claim). Non-zero claims are right-skewed with occasional huge ones. | 600× 0; 300× Exp(0.002)+100; 80× Par(1.5, 1000); 20× Par(1.2, 10000) |
| #80: Temperature (City, Annual) | temperature, seasonal | Bimodal: winter mode at 5C, summer mode at 25C. | 400× Z·5+5; 200× Z·3+15; 400× Z·4+25 |
| #81: Network Packet Sizes | network, trimodal | Trimodal: ACKs at 64 bytes, standard at 576, jumbo at 1500. | 300× 64+Z·5; 400× 576+Z·100; 300× 1500+Z·10 |
| #82: App Load Time (Mobile) | latency, multi-network | Fast wifi: mode at 0.8s. Slow 3G: mode at 4s. Timeouts at 30s. | 400× abs(Z·0.3+0.8); 300× abs(Z·1+4); 150× Exp(0.1)+6; 50× 30+Z·0.1 |
| #83: Taxi Trip Distance | distance, transport | Spike at very short (0.5mi), mode at 2mi, long tail to 30mi airport trips. | 200× U(0.3,0.8); 500× exp(Z·0.5+0.7); 200× U(8,12) (airport); 100× Exp(0.1)+12 |
| #84: Employee Tenure (Years) | tenure, clustered | Spike at <1 year (new hires/turnover). Clusters at 5,10,15,20 year marks. | 300× Exp(2); 150× Z·0.8+5; 100× Z·0.8+10; 80× Z·0.8+15; 50× Z·0.8+20; all mapped max(0,v) |

## 9. Weird Combinations & Edge Cases

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #85: Normal + Uniform Background | signal-in-noise, mixed | Signal buried in noise: normal peak sitting on flat uniform background. | 500× Z·5+50; 500× U(0,100) |
| #86: Exponential + Reverse Exponential | opposing, symmetric-decay | Two opposing decays meeting in the middle. Like distance from two sources. | 500× Exp(0.1); 500× 50−Exp(0.1); all mapped clamp(v, 0, 50) |
| #87: Sawtooth Pattern | periodic, sawtooth | Periodic ramp-up then sudden drop. Like battery charge cycles. | 1000× (r·100) mod 25 |
| #88: Spike at Every 10th Value | quantized, artifacts | Regular spikes in otherwise smooth data. Like quantization artifacts. | 1000×: if r1<0.3 push round(r2·10)·10, else push r2·100 (two rng draws per value) |
| #89: Hollow Distribution (Ring) | hollow, ring-like | Data avoids the center. Like distance from a target - rarely exact. | 1000× abs(Z·3+15) |
| #90: Zigzag Density | interference, banded | Alternating high/low density bands. Like interference pattern. | 1000×: v=r1·100, band=floor(v/10); if band even and r2<0.7 push v; else if band odd and r2<0.3 push v; else push r3·100 (short-circuit: r2 drawn only for the matching parity branch) |
| #91: Contaminated Normal | contaminated, robust | Normal distribution with 5% values from a completely different process. | 950× Z·10+50; 50× U(−100,200) |
| #92: Folded Distribution | folded, absolute-value | Absolute value of something centered at 0. V-shape valley at zero. | 500× abs(Z·20); 500× abs(Z·5+15) |
| #93: Step Function Density | step-density, piecewise | Density changes abruptly at thresholds. Like different rules in different ranges. | 100× U(0,20) (sparse); 400× U(20,40) (dense); 150× U(40,70) (medium); 350× U(70,80) (very dense) |
| #94: Needle in Haystack | extreme-outlier, needle | 999 values in tight range, 1 value extremely far away. | 999× Z·2+50; single value 500 |
| #95: Mixture of Uniforms | mixed-uniform, overlapping | Three overlapping uniform distributions of different widths. | 300× U(20,80) (wide); 400× U(35,55) (narrow center); 300× U(50,90) (shifted wide) |
| #96: Discrete + Continuous Mix | mixed-type, categorical+continuous | Some values are exact integers (categories), others are continuous measurements. | 300× floor(r·5)+1 (discrete 1-5); 700× Z·1.5+3 (continuous around 3) |

## 10. Temporal & Periodic

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #97: Day-of-Week (Cyclic) | cyclic, weekly | Business days high, weekends low. Clear 5-2 pattern. | day weights [0.18, 0.19, 0.19, 0.18, 0.16, 0.05, 0.05]: 1000× pick day j by cumulative weight against r, push j+1+Z·0.1 (bins: 7) |
| #98: Time-of-Day (24h) | time-of-day, bimodal | Rush hours at 8-9, 17-18. Dead zone 2-5am. Lunch bump at 12. | 200× Z·0.5+8.5; 250× Z·0.7+17.5; 100× Z·0.5+12; 350× U(6,23); 100× U(0,6) |
| #99: Monthly Seasonality | seasonal, monthly | Sales peak in Nov-Dec (holiday), trough in Jan-Feb. | monthly weights [0.06, 0.05, 0.07, 0.08, 0.08, 0.09, 0.09, 0.09, 0.08, 0.08, 0.12, 0.11]: for month m (0-11), floor(weight·1000)× m+1+Z·0.2 |
| #100: Inter-Arrival Times | inter-arrival, bursty | Time between events. Mostly short, with occasional long waits. | 800× Exp(2); 150× Exp(0.2)+2; 50× U(10,60) |
| #101: Batch Arrivals | bursty, batch | Events come in bursts. Long quiet, then many at once. | 20 batches: batchTime=batch·50+Z·5; batchSize=floor(r·30)+5; batchSize× batchTime+Z·2 |
| #102: Deadline Clustering | deadline, clustering | Activity spikes just before deadlines at t=30,60,90. | for each dl in [30, 60, 90]: 150× dl−Exp(0.5); 30× dl−U(5,20); then 200× U(0,90) |

## 11. Pos/Neg Separation Patterns

Every card in this category passes opts color: #3498db and color2: rgba(231,76,60,0.6), and its data function returns [neg, pos] — neg draws first (blue main histogram), pos second (red overlay at 0.8 height scale).

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #103: Clean Separation | clean-split, ideal | Positive class clearly higher than negative. Ideal for classification. | neg: 500× Z·5+30. pos: 500× Z·5+60 |
| #104: Partial Overlap | overlap, partial | Distributions overlap in 40-60 range. Good signal but not perfect. | neg: 500× Z·10+40. pos: 500× Z·10+55 |
| #105: No Separation | no-signal, useless | Both classes identically distributed. Feature is useless. | neg: 500× Z·15+50. pos: 500× Z·15+50 |
| #106: Separation Only in Tail | tail-signal, subtle | Mostly overlapping but positive cases dominate the extreme right tail. | neg: 450× Z·10+50; 50× Z·5+50. pos: 350× Z·10+50; 150× Z·5+80 |
| #107: Multimodal Separation | multimodal, partial-signal | Neg is unimodal. Pos is bimodal - some overlap, some separate. | neg: 500× Z·8+50. pos: 250× Z·5+50; 250× Z·5+80 |
| #108: Variance Difference Only | variance-signal, same-mean | Same mean, but positive class has much wider spread. | neg: 500× Z·5+50. pos: 500× Z·20+50 |
| #109: Spike vs Spread | spike-vs-spread, shape-diff | Negative class all at one value. Positive class spread across range. | neg: 500× 50+Z·1. pos: 500× U(20,80) |
| #110: Reversed in Subranges | non-monotonic, crossing | In range 0-50, neg dominates. In range 50-100, pos dominates. Non-monotonic. | neg: 350× Z·10+30; 150× Z·10+70. pos: 150× Z·10+30; 350× Z·10+70 |
| #111: Rare Positive in Dense Negative | imbalanced, needle-signal | Neg: 950 samples everywhere. Pos: only 50 samples in narrow range. | neg: 950× Z·20+50. pos: 50× Z·3+75 |
| #112: Both Skewed, Different Direction | opposite-skew, crossing | Neg right-skewed, Pos left-skewed. Overlap in middle. | neg: 500× Exp(0.1)+10. pos: 500× 80−Exp(0.1) |
| #113: Separation Only With Enough Data | subtle, needs-power | With N=30, looks like no signal. With N=500, subtle shift visible. | neg: 500× Z·12+48. pos: 500× Z·12+52 |
| #114: Discrete Feature Separation | discrete, ordinal-signal | Feature has values 1-5. Neg clusters at 1-2, Pos clusters at 4-5. | neg: 200× 1+Z·0.1; 200× 2+Z·0.1; 100× 3+Z·0.1. pos: 100× 3+Z·0.1; 200× 4+Z·0.1; 200× 5+Z·0.1 |

## 12. Transformation Responses

(Categories 12-17 are not static HTML — the script creates each `.category` div, id cat-transform, cat-quality, cat-structural, cat-financial, cat-bio, cat-compound, and appends it after the previous category in order.)

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #115: Log-Transformable (Becomes Bell) | right_skew, transformable | Extreme right-skew that becomes nearly normal after log transform. | 1000× exp(Z·1.2+5) |
| #116: Sqrt-Transformable (Count Data) | sqrt, counts | Poisson-like counts where sqrt brings closer to symmetry. | 1000× Binomial(20, 0.15): v = number of 20 trials with r<0.15 |
| #117: Box-Cox Resistant (Multi-Mode) | non-transformable, multimodal | No single transform fixes this — fundamentally multimodal. | 300× Z·2+5; 400× Z·3+25; 300× Z·2+50 |
| #118: Reciprocal Transformable | reciprocal, rates | 1/x brings heavy-tail data closer to symmetric. Common in rates. | 1000× 1/(r·0.9+0.1) |
| #119: Arcsin-Sqrt (Proportion Data) | proportion, variance-stabilizing | Proportions between 0-1 with edge compression. Arcsin-sqrt stabilizes variance. | 1000× r^0.7·0.8+0.1 |
| #120: Log Fails (Zero-Inflated) | zero-inflated, log-fails | Right-skewed but has zeros — log transform undefined at zero without adjustment. | 400× 0; 600× Exp(0.05)+0.01 |

## 13. Contamination & Data Quality

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #121: 5% Wrong-Unit Contamination | unit-error, contamination | Mostly in kg, but 5% entered in lbs. Creates phantom right shoulder. | 950× Z·12+70; 50× (Z·12+70)·2.2 |
| #122: Placeholder -999 Values | sentinel, missing-encoded | Sentinel value -999 for missing mixed with real data. Bimodal artifact. | 200× −999; 800× Z·15+50 |
| #123: Duplicate Records (Spike) | duplicates, data-error | Same record repeated 100x creates artificial spike in otherwise smooth data. | 800× Z·10+50; 200× 47.3 |
| #124: Mixed Precision (Rounding) | mixed-precision, rounding | Old data rounded to integers, new data has 2 decimal places. | 500× round(Z·10+50); 500× Z·10+50 |
| #125: Truncated at Reporting Threshold | detection-limit, floor | Values below 0.01 reported as 0.01. Creates artificial floor. | 1000× max(0.01, Exp(5)) |
| #126: Drift Between Batches | batch-drift, temporal | Two collection periods with shifted means. Looks bimodal but isn't. | 500× Z·8+45; 500× Z·8+55 |
| #127: Systematic Bias + Noise | instrument-bias, additive | Underlying uniform data with additive instrument bias creating skew. | 1000× U(0,50)+Exp(0.1) |
| #128: Merged Datasets (Scale Mismatch) | scale-mismatch, merged | Two sources: one 0-100 scale, other 0-10 scale. Not normalized. | 500× Z·15+60; 500× Z·1.5+6 |
| #129: String-to-Numeric Artifacts | parse-error, zero-spike | Parsed numbers: most valid, some got 0 from failed parse. | 850× Z·20+100; 150× 0 |
| #130: Clipped Outliers (Winsorized) | winsorized, clipped | Outliers replaced by boundary values. Pile-up at clip points. | 1000× clamp(Z·20+50, 10, 90) |

## 14. Structural & Mechanical Patterns

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #131: Quantized to 0.25 Steps | quantized, step-size | Continuous value forced to nearest 0.25. Creates comb-like pattern. | 1000× round((Z·3+10)·4)/4 |
| #132: Hash Collision Pattern | hash, near-uniform | Hash mod 16: some buckets overfull due to collision. Nearly uniform with spikes. | bucket weights [1.2, 0.9, 1.5, 0.8, 1.1, 0.7, 1.8, 1.0, 0.9, 1.3, 0.6, 1.4, 1.0, 0.8, 1.1, 0.9] normalized by their sum: 1000× pick bucket j by cumulative weight against r, push j+Z·0.05 (bins: 16) |
| #133: Memory Allocation Sizes | power-of-2, allocation | Powers of 2: 64, 128, 256, 512, 1024, 2048, 4096 bytes. | sizes [64, 128, 256, 512, 1024, 2048, 4096] with probs [0.05, 0.15, 0.30, 0.25, 0.15, 0.07, 0.03]: 1000× pick size by cumulative prob against r, push size+Z·2 |
| #134: Rate Limiting Artifacts | rate-limited, ceiling | Values capped at 100 req/s. Natural traffic below, pile-up at limit. | 700× Exp(0.02)+5; all mapped min(100, v) |
| #135: Retry Backoff (Exponential) | backoff, retry | Retry delays: 1s, 2s, 4s, 8s, 16s with jitter. Clusters at powers. | attempts 0-4: base=2^attempt, n from [300, 200, 150, 100, 50]; n× base+r·base·0.3 |
| #136: Batch Size Effects | batch-processing, uniform-like | Processing in batches of 32. Latency depends on batch fill level. | 1000× (i = loop index 0-999): fillLevel=(i mod 32)/32; push 10+fillLevel·20+Z·2 |
| #137: Thread Contention | contention, multimodal | Response time bimodal: fast when no contention, slow when waiting. | 600× Z·2+5; 300× Z·5+25; 100× Z·10+50 |
| #138: GC Pause Distribution | gc-pauses, heavy-tail | Mostly short minor GCs, occasional long major GC pauses. | 800× Exp(5)+0.5; 150× Exp(0.2)+10; 50× U(50,200) |
| #139: Compression Ratio | compression, trimodal | Most files compress to 30-50%. Incompressible files at ~100%. Already-compressed near 100%. | 600× Z·8+40; 200× Z·3+15; 200× Z·2+98 |

## 15. Financial & Economic

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #140: Daily Returns (Fat-Tailed Normal) | returns, fat-tails | Stock returns: looks normal but tails are 5-10x fatter than Gaussian. | 1000×: v=Z; if r<0.05 then v=v·5; push v·2 |
| #141: Bid-Ask Spread | spread, microstructure | Tight spread for liquid stocks, wide for illiquid. Right-skewed with floor. | 700× 0.01+Exp(50); 200× 0.05+Exp(10); 100× U(0.1, 0.5) |
| #142: Transaction Amounts (Stratified) | transactions, stratified | Micro (<$1), small ($1-50), medium ($50-500), large ($500+). | 200× r·0.99+0.01; 400× Exp(0.05)+1; 250× Exp(0.005)+50; 150× Exp(0.001)+500 |
| #143: Loan Default (Time-to-Event) | default, survival | Most loans don't default (censored). Those that do: early or at specific terms. | 600× 360 (full term, no default); 150× Exp(0.1)+1 (early defaults); 100× Z·5+60 (5-year mark); 80× Z·5+120 (10-year mark); 70× U(1,360) |
| #144: Portfolio Volatility | volatility, regime-switch | Regime switching: low-vol periods (majority) and high-vol crisis periods. | 700× abs(Z·1.5); 200× abs(Z·5); 100× abs(Z·12) |
| #145: Interest Rate Changes | rate-changes, mixed-normal | Most periods: tiny change. Occasional jumps at Fed meetings. | 900× Z·0.02; 100× Z·0.1 + (0.25 if r>0.5 else −0.25) |

## 16. Biological & Natural

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #146: Gene Expression (Zero-Inflated) | gene-expression, zero-inflated | Many genes not expressed (zero). Expressed ones follow right_skew. | 600× 0; 400× exp(Z·1.5+2) |
| #147: Cell Size Distribution | cell-cycle, growth | Dividing cells: bimodal with small (just divided) and large (about to divide). | 400× Z·1+8; 350× Z·1.5+12; 250× Z·2+15 |
| #148: Earthquake Magnitudes | seismology, exponential | Gutenberg-Richter law: exponential decrease with magnitude. Rare large events. | 1000× 2+Exp(0.5) |
| #149: Species Abundance | ecology, hollow-curve | Few dominant species, many rare species. Hollow curve. | 1000× floor(Par(1.5, 1)) |
| #150: Heart Rate Variability | HRV, physiological | RR intervals: main mode at ~800ms, secondary at ~600ms (ectopic beats). | 800× Z·50+800; 100× Z·30+600; 100× Z·80+900 |
| #151: Reaction Time (Ex-Gaussian) | reaction-time, ex-gaussian | Normal component + exponential tail. Classic cognitive psych pattern. | 1000× Z·30+250+Exp(0.01) |
| #152: Rainfall (Mixed Zero) | rainfall, zero-inflated | Many dry days (0mm). Rainy days follow gamma distribution. | 500× 0; 500× sum of 3 independent Exp(0.3) draws |
| #153: Tree Diameters (Reverse-J) | forestry, reverse-J | Many small/young trees, few large/old ones. Classic ecology pattern. | 1000× 5+Exp(0.08) |
| #154: Pollen Count (Seasonal Burst) | seasonal-burst, zero-heavy | Zero most of year, extreme spike during bloom season. | 700× r·5; 200× Z·50+200; 100× Z·100+400; all mapped max(0,v) |

## 17. Compound & Hierarchical

| Title | Tags | Description | Data generation |
|---|---|---|---|
| #155: Gamma-Poisson (Negative Binomial) | hierarchical, overdispersed | Random rate per person, then Poisson counts. Overdispersed. | 1000×: rate=Exp(0.3)+0.5; count = number of 20 trials with r<rate/20; push count |
| #156: Mixture of Experts | mixture, heterogeneous | Three subpopulations each with own distribution shape. | 300× abs(Z·3) (half-normal); 400× Z·5+30 (normal); 300× 60−Exp(0.15) (left-skew); all mapped max(0,v) |
| #157: Nested Categories | nested, department | Department A (uniform 20-40), B (normal at 60), C (bimodal 80 and 95). | 300× U(20,40); 400× Z·5+60; 150× Z·3+80; 150× Z·2+95 |
| #158: Random Walk End Points | random-walk, CLT | Where 1000 random walks end up after 100 steps. Normal by CLT. | 1000×: pos = sum of 100 independent Z draws; push pos |
| #159: Sum of Uniforms (Irwin-Hall) | sum, convergence | Sum of 3 uniforms — triangular-ish. Sum of 12 — nearly Gaussian. | 1000×: s = sum of 4 independent r draws; push s |
| #160: Product of Normals (Heavy-Tailed) | product, heavy-tail | Product of two independent normals. Heavier tails than either alone. | 1000× Z1·Z2·5 (two independent Z draws per value) |
| #161: Max of N Samples (Extreme Value) | extreme-value, gumbel | Maximum of 10 normal samples. Shifted right, Gumbel-like. | 1000×: push max of 10 independent draws of Z·10+50 |
| #162: Min of N Samples (Weibull-like) | minimum, first-failure | Minimum of 5 exponential samples. Models first-failure time. | 1000×: push min of 5 independent draws of Exp(0.5) |
| #163: Conditional Distribution | conditional, asymmetric | X|Y<0 is left-normal, X|Y>0 is right-normal. Marginal looks odd. | 500× −abs(Z·10)+50; 500× abs(Z·10)+50 |
| #164: Censored Mixture | censored, mixture | Two populations: one observed fully, one right-censored at threshold. | 500× Z·8+30; 500× min(60, Z·15+55) |

## Regeneration instructions

- **Template:** self-contained gallery page (closest to nav-grid style, but cards contain canvases instead of links). Structure: h1, `.subtitle` paragraph, then one `.category` div per section (h2 + `.grid`). Categories 1-11 are static divs in the body with ids cat-spikes, cat-gaps, cat-skew, cat-multimodal, cat-bounded, cat-sparse, cat-discrete, cat-realworld, cat-combo, cat-temporal, cat-separation (grid ids grid-<same suffix>); categories 12-17 (cat-transform, cat-quality, cat-structural, cat-financial, cat-bio, cat-compound) are created by the script and appended after category 11 in order. All 164 cards are built by a `createCard(grid, title, desc, dataFn, opts)` helper; category h2 text carries the section number ("1. Spike & Point Mass Patterns" … "17. Compound & Hierarchical").
- **Layout:** `.grid` is CSS grid `repeat(3, 1fr)`, gap 16px; `.category` margin-bottom 40px; category h2 color #2c3e50, 1.3em, 2px solid #3498db bottom border, padding-bottom 6px, margin-bottom 16px.
- **Page CSS:** body font 'Segoe UI', system-ui, sans-serif; background #f8f9fa; text #2d3436; padding 20px. h1 centered, 1.8em, #2c3e50, margin-bottom 8px. `.subtitle` centered, #636e72, 0.95em, margin-bottom 30px. `.card` white background, radius 8px, padding 12px, shadow 0 2px 8px rgba(0,0,0,0.08); card h3 1.05em #2c3e50; card p 0.95em #636e72, line-height 1.4. `.tag` inline-block pill, 0.85em, background #e8f4fd, color #2980b9, padding 2px 6px, radius 3px, margin-right 4px. Universal reset `* { margin:0; padding:0; box-sizing:border-box }`.
- **Palette:** histogram bars #3498db (blue) at alpha 0.5; category-11 overlay rgba(231,76,60,0.6) (red #e74c3c) at alpha 0.4; SE band fill rgba(230,126,34,0.25) (orange #e67e22); band center line #1a5276 (primary blue); green #27ae60 unused on this page.
- **Canvases:** this page uses fixed 560×240 backing stores stretched by CSS to 100% width × 203px height — the original does not apply window.devicePixelRatio scaling (project convention elsewhere is dpr scaling via a setup helper; apply it if regenerating to convention).
- **Data:** regenerate all card data with the shared mulberry32(42) stream and helper formulas in the "Shared chart machinery" section, drawing cards in document order.
- Cards are not links (nothing to convert); if any card links are ever added, use .html extensions in the regenerated HTML.
- After all cards render, the script appends " (N examples shown)" to the subtitle text, N = `document.querySelectorAll('.card').length`.
