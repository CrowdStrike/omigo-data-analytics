# Cybersecurity — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, histogram canvas middle ~31%, insight canvas right ~31%, one table per section)
**HTML title tag:** Cybersecurity — Distribution Patterns

**Subtitle:** Distributions that expose the statistical structure of attacks, vulnerabilities, detection gaps, and why averages mislead in security

## Attack Inter-Arrival Times (Poisson with Bursts)

**Pitfall label:** CAMPAIGN CLUSTERING (color `#795548`)

Attack arrivals in this simulation are not uniform Poisson. They cluster into campaigns — intense probing for a stretch, then silence. The inter-arrival distribution is a mixture: exponential during quiet periods, near-zero during bursts.

- Quiet periods: exponential, mean ~4 hours between events
- Campaign bursts: events ~6 minutes apart
- A fixed "alerts per hour" threshold flaps during campaigns and is silent otherwise
- Changepoint detection fits this shape better than a rate threshold

### Visualization (canvas `canvas1`, 420×340)

Histogram of inter-arrival times (seeded mulberry32 RNG, seed 99, shared sequentially across all charts).

- **Title (bold `#1a5276`, top center):** "Inter-Arrival Time Between Attack Events".
- **Data:** 2000 draws: with probability 0.3 a campaign-burst value |Normal(0.1, 0.05)| hours; otherwise an exponential quiet-period value −4·ln(u) (mean 4 hours).
- **Bins/axes:** 40 bins, x range 0 to 20, x tick format "N.Nh"; x-axis label "Hours Between Events". Gray `#999` L-shaped axes; margins top 35 / right 20 / bottom 40 / left 50; white background.
- **Bars:** fill `rgba(192,57,43,0.5)`, border `#c0392b` 0.5px. Gaussian-smoothed density line overlay in `#1a5276` width 2 (sigma 1.5).

### Visualization (canvas `canvas1b`, 400×340)

7-day hourly attack-count timeline showing campaign bursts vs quiet periods.

- **Title (bold `#c0392b`):** "7-Day Attack Timeline: Campaigns vs Quiet"; subtitle (10px `#666`): "A fixed rate threshold fires only mid-campaign, after the burst begins".
- **Data:** 168 hourly counts from a two-state process: 2% chance/hour of entering a campaign, 15% chance/hour of leaving one; campaign hours count 20 + floor(uniform×80) events, quiet hours 0–2 events. Bars scaled to the max count. Padding top 40 / right 15 / bottom 55 / left 50.
- **Bars:** count > 15 → `rgba(192,57,43,0.7)` (campaign red); else `rgba(26,82,118,0.3)` (quiet blue).
- **Threshold line:** dashed orange `#e67e22` (dash 5/3, width 2) at 10 events/hr, labeled bold 10px "Alert threshold".
- **Campaign labels:** bold 9px `rgba(192,57,43,0.9)` "CAMPAIGN" placed above the start of each burst.
- **Axes:** gray L axes; x ticks "Day 0"–"Day 7"; x-axis label "Time (7 days)"; y annotation "Events/hr" right-aligned near top.

## Dwell Time — Days from Breach to Detection

**Pitfall label:** DETECTION LAG (color `#2980b9`)

Dwell time (how long an attacker is inside before detection) is right-skewed with a heavy tail — simulated here as a mixture of fast automated detections and slow-burn intrusions. Median ~10 days, mean ~22 days: reporting an "average time to detect" describes neither group.

- Fast mode: 1-3 days (automated alerts, commodity malware)
- Slow mode: centered ~20 days, tail past 200 days
- Median ~10d vs mean ~22d — the tail drives the gap
- One interpretation: damage compounds with dwell time (panel at right is that model, not measured data)

### Visualization (canvas `canvas2`, 420×340)

Histogram of dwell times (log-normal mixture).

- **Title:** "Dwell Time: Days from Breach to Detection".
- **Data:** 2000 draws: 35% fast detections exp(Normal(0.7, 0.8)) (≈1–5 days); 65% slow intrusions exp(Normal(3.0, 1.0)) (log-normal around 20 days).
- **Bins/axes:** 45 bins, x range 0 to 200, x tick format "Nd"; x-axis label "Days".
- **Bars:** fill `rgba(142,68,173,0.5)`, border `#8e44ad`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas2b`, 400×340)

Illustrative exponential damage-vs-dwell-time curve with severity zones.

- **Title (bold `#8e44ad`):** "Illustrative Model: Damage Compounds with Dwell Time"; subtitle (10px `#666`): "One model: longer dwell, more lateral movement (not measured data)".
- **Curve:** damage = exp(d/25) − 1 for d = 0–120 days, normalized to the value at 120 days; stroke `#8e44ad` width 3. Padding top 40 / right 15 / bottom 55 / left 55.
- **Zones:** 0–7 days shaded `rgba(39,174,96,0.15)` labeled green `#27ae60` "Contained"; 7–30 days `rgba(241,196,15,0.15)` labeled `#f39c12` "Spreading"; 30+ days `rgba(231,76,60,0.1)` labeled red `#e74c3c` "Full compromise" (bold 9px, near baseline).
- **Median marker:** vertical dashed dark slate `#2c3e50` line at 10 days labeled bold 10px "Median: ~10d".
- **Axes:** gray L axes; x ticks "0d" to "120d" every 30; x-axis label "Dwell Time (days)"; y annotation "Damage" right-aligned near top.

## CVSS Scores — Bimodal with Sparse Middle

**Pitfall label:** BIMODAL SEVERITY (color `#27ae60`)

The simulated CVSS scores here don't form a bell curve: a mass of low-severity findings (3-4 range) and a mass of criticals (8-10), with a sparse middle. "Average CVSS" hides this — a portfolio with mean 6.0 could be all mediums, or half trivial and half catastrophic.

- Mode 1: CVSS 3-4 (info disclosure, missing headers)
- Mode 2: CVSS 8-10 (RCE, auth bypass, SQLi)
- Sparse middle (5-7) — consistent with exploitability being closer to binary than gradual
- CVSS is ordinal, not interval — arithmetic on it is invalid

### Visualization (canvas `canvas3`, 420×340)

Histogram of simulated CVSS scores (bimodal).

- **Title:** "CVSS Score Distribution (simulated)".
- **Data:** 2000 draws: 40% low-severity Normal(3.5, 0.8) clamped [1, 5]; then 25% of the remainder sparse-middle Normal(6.0, 0.7) clamped [4, 7.5]; else high-severity Normal(8.8, 0.9) clamped [7, 10].
- **Bins/axes:** 36 bins, x range 0 to 10, x tick format one decimal; x-axis label "CVSS Score".
- **Bars:** fill `rgba(192,57,43,0.5)`, border `#c0392b`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas3b`, 400×340)

Side-by-side mini-histograms: two portfolios with identical mean 6.0 but opposite risk.

- **Title (bold `#c0392b`):** 'Same "Average CVSS 6.0" — Very Different Risk'; subtitle (10px `#666`): "Bimodal portfolio vs uniform — identical mean, opposite action".
- **Left panel — Portfolio A: "All Medium"** (label bold 10px `rgba(26,82,118,0.8)`): 10 bins with counts `[0, 0, 0, 2, 6, 12, 12, 6, 2, 0]` (peak at 5–7, mean 6.0), bars `rgba(41,128,185,0.6)`; caption 9px `#2980b9`: "Mean = 6.0" / "Action: prioritize uniformly".
- **Right panel — Portfolio B: "Half Trivial, Half Critical"** (label bold 10px `rgba(192,57,43,0.8)`): counts `[0, 2, 8, 8, 2, 0, 0, 0, 10, 10]` (bimodal, mean 6.0); bars in the lower half `rgba(241,196,15,0.6)` yellow, upper half `rgba(192,57,43,0.6)` red; caption 9px `#c0392b`: "Mean = 6.0" / "Action: triage the 9s NOW".
- **Divider:** vertical light-gray `#ccc` line between the panels with bold 14px gray `#999` "vs" centered on it. Padding top 40 / right 15 / bottom 50 / left 15.

## Time to Patch — Weibull with Never-Patch Tail

**Pitfall label:** WEIBULL SURVIVAL (color `#e74c3c`)

Time from disclosure to patch follows a Weibull-like survival curve with a twist: a fraction of systems never patch. The curve flattens instead of reaching zero, so "mean time to patch" is not a meaningful summary — the never-patch mass dominates it.

- ~10% patched within 7 days (automated pipelines)
- ~40% within 30 days; half patched by ~40 days
- ~75% within 90 days
- ~18% never patch (EOL systems, forgotten servers)

### Visualization (canvas `canvas4`, 420×340)

Histogram of days from disclosure to patch deployment.

- **Title:** "Days from Disclosure to Patch Deployment".
- **Data:** 2000 draws: 18% never patch, plotted censored at 365 days; otherwise Weibull(shape 1.2, scale 40) via 40·(−ln(1−u))^(1/1.2), capped at 364.
- **Bins/axes:** 40 bins, x range 0 to 365, x tick format "Nd"; x-axis label "Days".
- **Bars:** fill `rgba(230,126,34,0.5)`, border `#e67e22`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas4b`, 400×340)

Survival curve of % still unpatched, flattening at the never-patch plateau.

- **Title (bold `#e67e22`):** "Survival Curve: % Still Unpatched Over Time"; subtitle (10px `#666`): "Flattens at ~18% — those systems NEVER patch".
- **Curve:** S(t) = 0.18 + 0.82·exp(−(t/40)^1.2) for t = 0–180 days; stroke `#e67e22` width 3; y axis from 100% (top) to 0% unpatched. Padding top 40 / right 15 / bottom 55 / left 55.
- **Plateau line:** dashed red `#e74c3c` (dash 5/3, width 2) at 18%, labeled bold 10px "18% never patch" with 9px "(EOL, forgotten, shadow IT)".
- **Time markers:** dashed gray `#999` vertical lines at 7d, 30d, 90d with 9px labels.
- **Annotations (bold 9px, left side):** green `#27ae60` "~10% patched by 7d"; blue `#2980b9` "~40% by 30d".
- **Axes:** gray L axes; x-axis label "Days Since Disclosure"; y labels "100%" top, "0%" bottom, plus "% Unpatched" above the axis.

## Breach Cost — Zero-Inflated Pareto Distribution

**Pitfall label:** FAT-TAILED COST (color `#8e44ad`)

Most simulated incidents cost little, but the cost distribution has a Pareto tail — a small fraction cause catastrophic losses. A single "average breach cost" headline is dominated by those rare mega-breaches: here the mean lands around 20x the median.

- 50% of incidents: < $100K (contained, no data loss)
- 30%: $100K-$1M (limited exposure)
- 15%: $1M-$10M (regulatory + remediation)
- 5%: $10M+ mega-breaches — Pareto tail, α ≈ 1.2 (mean exists, variance doesn't)

### Visualization (canvas `canvas5`, 420×340)

Histogram of breach costs (zero-inflated with Pareto tail; units $K internally).

- **Title:** "Breach Cost Distribution".
- **Data:** 2000 draws by uniform u: u<0.20 → 0 (contained); u<0.50 → |Normal(50, 25)| (small, mostly <$100K); u<0.80 → 100·10^uniform ($100K–$1M log-uniform); u<0.95 → 1000·10^uniform ($1M–$10M log-uniform); else 10000·(1−u′)^(−1/1.2) (Pareto tail from $10M, α = 1.2). Values clipped at 50000 for display.
- **Bins/axes:** 45 bins, x range 0 to 10000, x tick format "$NM" (value/1000); x-axis label "Cost ($M)".
- **Bars:** fill `rgba(44,62,80,0.5)`, border `#2c3e50`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas5b`, 400×340)

Stacked segment bar decomposing breaches by cost tier, with mean-vs-median callout.

- **Title (bold `#2c3e50`):** 'Why an "Average Breach Cost" Misleads'; subtitle (10px `#666`): "Tail events dominate the mean — median is far lower".
- **Stacked bar (left, ~35% width) segments top to bottom, each with bold label + cost text at its right:**
  - "50% of breaches" / "< $100K" — `rgba(39,174,96,0.6)`, 12% of height
  - "30% of breaches" / "$100K - $1M" — `rgba(241,196,15,0.6)`, 18% of height
  - "15% of breaches" / "$1M - $10M" — `rgba(230,126,34,0.6)`, 25% of height
  - "5% of breaches" / "$10M+" — `rgba(192,57,43,0.6)`, 45% of height
  - White 2px borders between segments.
- **Callout box (dark slate `rgba(44,62,80,0.92)`, white text, bottom right):** bold "Median incident: ~$100K" / "Mean incident: ~$2M" and 9px "~20x gap — budget for your median," / "insure for the tail". Padding top 40 / right 15 / bottom 50 / left 15.

## Phishing Click Rate — Time-of-Day Conditioned Beta

**Pitfall label:** CONDITIONAL RATE (color `#e67e22`)

Click rate isn't a fixed percentage — it's a distribution whose shape shifts with conditions. This simulation mixes beta distributions by time slot; the pooled histogram (left) hides the conditional structure (right).

- Baseline: beta(2, 18) → mean 10%, mode ~6%
- Monday morning slot: beta(4, 16) → mean 20% — one explanation: post-weekend inbox overload
- Post-training: beta(1, 30) → mean ~3%, drifting back toward baseline over the following months

### Visualization (canvas `canvas6`, 420×340)

Histogram of pooled phishing click rates across all time conditions.

- **Title:** "Phishing Click Rate (%) — All Time Conditions".
- **Data:** 2000 draws (in %): 15% Monday-morning Beta(4, 16); 10% Friday-afternoon Beta(3, 17); 75% normal-hours Beta(2, 18). Beta sampled via sums of exponentials (gamma ratio) ×100.
- **Bins/axes:** 35 bins, x range 0 to 45, x tick format "N%"; x-axis label "Click Rate (%)".
- **Bars:** fill `rgba(41,128,185,0.5)`, border `#2980b9`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas6b`, 400×340)

Overlaid beta PDF curves conditioned on time/training.

- **Title (bold `#2980b9`):** "Click Rate Shifts by Time and Training"; subtitle (10px `#666`): "Same phish, different conditions = different distribution".
- **Curves (x = click rate 0–50%, normalized to the joint max PDF, 85% of plot height, width 2.5):**
  - Beta(4, 16) — red `#e74c3c`, legend "Monday 8-10am"
  - Beta(2, 18) — blue `#2980b9`, legend "Normal hours"
  - Beta(1, 30) — green `#27ae60`, legend "Post-training (1 week)"
- **Legend:** color line swatches + 10px labels at upper left. Padding top 40 / right 15 / bottom 55 / left 45.
- **Annotation box (dark slate `rgba(44,62,80,0.9)`, white text, bottom right):** bold 9px "Training effect decays over the following months," / 9px "drifting back toward baseline beta(2,18)".
- **Axes:** gray x baseline; x ticks "0%"–"50%" every 10%; x-axis label "Click Rate (%)".

## Password Entropy — Multimodal at Policy Boundaries

**Pitfall label:** POLICY-SHAPED (color `#16a085`)

Password entropy in this simulation is multimodal, and the modes sit where policy and tooling put them: a mass at minimum compliance, a second at passphrases, a third at password managers — with sparse gaps between. The shape tracks policy boundaries rather than a smooth "security skill" gradient.

- Mode 1 (~55%): minimum compliance, ~28 bits (word + year + "!" pattern)
- Mode 2 (~30%): passphrases, ~50 bits (four random words)
- Mode 3 (~15%): password managers, ~90 bits (truly random)
- Gap: ~60-80 bit range is sparse (too complex to memorize, not yet using a manager)

### Visualization (canvas `canvas7`, 420×340)

Histogram of password entropy (three modes).

- **Title:** "Password Entropy Distribution (bits)".
- **Data:** 2000 draws by uniform u: u<0.55 → max(15, Normal(28, 5)) minimum compliance; u<0.85 → max(35, Normal(50, 8)) passphrases; else Normal(90, 12) resampled until ≥ 70 (password managers — resampling, not clamping, so no artificial pile-up at 70).
- **Bins/axes:** 40 bins, x range 10 to 130, x tick format integer; x-axis label "Entropy (bits)".
- **Bars:** fill `rgba(22,160,133,0.5)`, border `#16a085`. Density line overlay `#1a5276`.

### Visualization (canvas `canvas7b`, 400×340)

Three labeled policy-zone boxes with representative passwords.

- **Title (bold `#16a085`):** "Modes Sit at Policy and Tooling Boundaries"; subtitle (10px `#666`): "Consistent with users optimizing to minimum compliance".
- **Zones (outlined filled rectangles across the width, each with bold 11px label, percentage, and 9px `#555` example + bits text):**
  - "Min. Compliance" / "55%" / example 'word + year + "!"' / "~28 bits" — fill `rgba(231,76,60,0.15)`, border `#e74c3c`, x 5%–35% of width
  - "Passphrase" / "30%" / example "four random words" / "~50 bits" — fill `rgba(241,196,15,0.15)`, border `#f39c12`, x 38%–63%
  - "Pwd Manager" / "15%" / example "16 random characters" / "~90 bits" — fill `rgba(39,174,96,0.15)`, border `#27ae60`, x 70%–95%
- **Gap annotation (italic 9px gray `#999`, between zones 2 and 3):** "Gap: ~60-80 bits (too hard to" / "memorize, not using manager)".
- **Bottom note (bold 9px `#c0392b`, centered):** "Cross-site reuse is widespread → per-site effective entropy ≈ 0 for those accounts". Padding top 40 / right 15 / bottom 55 / left 15.

## Regeneration instructions

- **Layout:** one `.obj-table` per section (seven total), each a single `<tr>` with three `<td>`s: text cell 38% (pitfall label span, `<h3>` title, paragraph, `<ul>` bullets), middle cell 31% centered (histogram canvas 420×340), right cell 31% centered (insight canvas 400×340).
- **Page style:** body system sans-serif, margin 20px, background `#f9f9f9`, text `#333`; h1 centered `#1a5276`; `.subtitle` centered `#666` 0.95em; table cells `border: 1px solid #2980b9`, padding 12px, vertical-align top; h3 `#1a5276` 1.0em weight 700; p/li 14px, line-height 1.5–1.6; `.pitfall-label` inline-block bold 0.72em uppercase letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by document order from the cycling palette `["#795548","#2980b9","#27ae60","#e74c3c","#8e44ad","#e67e22","#16a085","#d35400","#c0392b","#1abc9c"]` via a small script that colors each `.pitfall-label`.
- **Data:** all simulated with a seeded mulberry32 RNG (seed 99) shared sequentially across charts, plus a Box-Muller `randNormal(mean, std)` helper; a shared `drawHistogram(canvasId, data, options)` helper draws title, axes, bars, a Gaussian-smoothed density line (`#1a5276`, sigma 1.5), and 6 evenly spaced x tick labels (optional `xFormat`).
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus accents `#2980b9`, `#8e44ad`, `#c0392b`, `#2c3e50`, `#16a085`, `#f39c12`, yellow `rgba(241,196,15,…)`; gray text `#555`/`#666`/`#333`. No nav bar, no back/home links.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
