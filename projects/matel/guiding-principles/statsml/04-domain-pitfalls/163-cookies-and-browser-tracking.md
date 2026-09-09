# Cookies and Browser Tracking

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 163. Cookies and Browser Tracking

**Subtitle:** A web metric is not a measurement of people — it is a measurement of what the browser's storage and identity rules were willing to let you observe that month. Change the rules and every number moves with no product change behind it.

## Callout (philosophy box)

**The fundamental problem:** Cookies, client-side storage, fingerprints and beacons are the *instrument*, not the phenomenon. Each one has a scope (who set it), a lifetime (how long the browser keeps it), and a resolution (person, device, or group). When a tracking-prevention policy or a third-party phase-out changes any of those three, the time series takes a step — and the step is the instrument changing, not users changing. Comparing across that step compares two different instruments.

**Illustrative Example.** Every count, rate, entropy value and lifetime below is constructed so the arithmetic closes and can be checked by hand. None of it is a measurement of any real browser, vendor, or population. Real storage lifetimes, real deprecation timetables, and real fingerprint entropy are not asserted anywhere on this page.

## Shared scenario reconciliation

A reviewer should be able to check every figure on this page from these six blocks.

**Block A — reach (Sections One):** Vendor A's measurement code is embedded on 10 top-level sites.
Distinct cross-site pairs observable under one shared third-party identifier = C(10,2) = 45.
Under first-party-only identity each site is its own island → observable pairs = 0.
Fraction of the 10-site graph one first-party cookie can see = 1/10 = 10.0%.

**Block B — partitioning (Section Two):** 130,000 real people.
100,000 use a private device; their site-visit counts are 60,000 × 1 site, 25,000 × 2, 10,000 × 3, 5,000 × 5.
Under a partitioned storage model each (person, top-level site) pair is a separate identifier:
60,000×1 + 25,000×2 + 10,000×3 + 5,000×5 = 60,000 + 50,000 + 30,000 + 25,000 = **165,000 identifiers** for 100,000 people → inflation 165,000 / 100,000 = **1.65×**.
The other 30,000 people share 10,000 household devices, 3 people per device, 1 site each → **10,000 identifiers** for 30,000 people → deflation 10,000 / 30,000 = **0.333×**.
Combined: 130,000 people → 175,000 identifiers → **1.3462×**. The two errors do not cancel; both are present at once.

**Block C — eviction (Section Three):** 120,000 monthly visitors, behaviour held constant.
Return-gap composition: 40,000 at ≤3 days, 25,000 at 10 days, 20,000 at 21 days, 15,000 at 90 days, plus 20,000 genuinely first-time. Total 40,000+25,000+20,000+15,000+20,000 = 120,000.
True returning share = 100,000 / 120,000 = **83.33%**; true new share = 20,000 / 120,000 = **16.67%**.
Observed "new visitors" under an eviction cap of L days = 20,000 + (visitors whose gap > L):
L = 400 d → new 20,000 = **16.67%**; L = 30 d → new 20,000+15,000 = 35,000 = **29.17%**; L = 7 d → new 20,000+15,000+20,000+25,000 = 80,000 = **66.67%**.
Inflation of the new-visitor count from L=400 to L=7 = 80,000 / 20,000 = **4.0×**, with zero behaviour change.

**Block D — regime step (Sections Four and Eight):** true monthly distinct humans, in thousands, for a 12-month cycle:
`[120, 114, 126, 120, 126, 120, 114, 120, 126, 132, 138, 144]`, sum **1,500,000**; month 1 → month 12 growth = 144/120 − 1 = **+20.0%**.
Year 1 instrument observes a **5/6** share → `[100, 95, 105, 100, 105, 100, 95, 100, 105, 110, 115, 120]` ×1,000, total **1,250,000**.
Year 2 repeats the *identical* human series (behaviour held exactly constant) but the instrument now observes **1/3** → `[40, 38, 42, 40, 42, 40, 38, 40, 42, 44, 46, 48]` ×1,000, total **500,000**.
Naive year-over-year = 500,000 / 1,250,000 − 1 = **−60.0%**, which equals (1/3) / (5/6) − 1 = 0.4 − 1 exactly. True human change = **0.0%**.
Within-regime month 1 → month 12 growth = 120/100 − 1 = **+20.0%** in year 1 and 48/40 − 1 = **+20.0%** in year 2 — identical to the truth, because a ratio inside one regime cancels the observation share.
Denominator contamination: 3,000 server-side conversions in month 1. True rate 3,000/120,000 = **2.50%**; regime A reports 3,000/100,000 = **3.00%**; regime B reports 3,000/40,000 = **7.50%**. Ratio 7.50/3.00 = **2.5×** = (5/6)/(1/3). Neither instrument ever reports the true 2.50%.
Pooled 24-month mean = 1,750,000 / 24 = **72,917/month**, versus regime A mean 1,250,000/12 = **104,167** and regime B mean 500,000/12 = **41,667**. 72,917/104,167 = 0.70 → **−30.0%**; 72,917/41,667 = 1.75 → **+75.0%**. The pooled average describes no month of either regime.

**Block E — fingerprint entropy (Section Five):** H = 18 bits → configuration space 2^18 = **262,144**.
Expected number of *other* people sharing one exact fingerprint = (N−1)·2^−H.
Probability a given fingerprint is **not** unique in the population = 1 − (1 − 2^−H)^(N−1).

| N | (N−1)·2^−18 | P(not unique) |
|---|---|---|
| 1,000 | 0.0038 | 0.38% |
| 10,000 | 0.0381 | 3.74% |
| 100,000 | 0.3815 | 31.71% |
| 1,000,000 | 3.8147 | 97.80% |

Same instrument, same 18 bits — uniqueness falls from 99.62% to 2.20% purely because N grew.
Entropy required for 95% uniqueness at N = 1,000,000: solve (1 − 2^−H)^(N−1) = 0.95 → H = log2(999,999 / −ln 0.95) = **24.22 bits**, so 25 bits is the smallest whole-bit answer (25 bits gives **97.06%** unique). Going from 18 to 25 bits demands 2^7 = **128×** the configuration space.
Distinct-fingerprint count at H = 18, N = 1,000,000: 2^18 · (1 − (1 − 2^−18)^1,000,000) = **256,365** occupied configurations → a 1,000,000-person population is reported as 256,365 "unique users", a **74.36%** undercount.

**Block F — beacons (Section Six):** 1,000,000 beacon fires decompose as
620,000 human views + 140,000 automated fetches + 120,000 speculative/prefetch loads + 80,000 duplicate fires + 40,000 offscreen renders with no human = **1,000,000**. Human share = 620,000/1,000,000 = **62.0%**.
Separately, 155,000 human views fired no beacon at all. True human views = 620,000 + 155,000 = **775,000**.
Beacon recall = 620,000 / 775,000 = **80.0%**; beacon precision = **62.0%**; headline total error = 1,000,000/775,000 − 1 = **+29.03%**. The total looks close only because two large errors of opposite sign partly offset.

**Block G — aggregate reporting (Section Seven):** 1,006 true conversions across 9 groups:
`[400, 250, 160, 90, 50, 25, 15, 10, 6]`, sum = **1,006**.
A minimum-group threshold of 10 suppresses the group of 6 → released total **1,000** = 99.40%; **0.60%** vanishes with no error message.
Noise of σ = 10 is added per released group. Relative noise σ/count = **2.5%, 4.0%, 6.25%, 11.1%, 20.0%, 40.0%, 66.7%, 100.0%**.
Groups where σ ≤ count/3 (usable): 5 of 8, covering 400+250+160+90+50 = **950** = 94.43% of 1,006.
Groups where relative noise ≥ 40%: 25, 15, 10 → **50** conversions = 4.97% of the total, individually unusable.
Total-level noise = σ·√8 = **28.28** on 1,000 = **2.83%** — the aggregate is precise while every small cell is worthless.
Person-level records released: **0**. Reach and frequency are not derivable from group counts at any noise level.

## A Cookie's Scope Is Set by Who Wrote It, Not by Who Reads the Report

**One Shared Third-Party Identifier Sees 45 Site Pairs; First-Party Sees 0**

- **The definition:** "First-party" and "third-party" describe the top-level site in the address bar, not ownership.
- **The same vendor, both roles:** Vendor A's cookie is first-party on its own site and third-party inside an embed.
- **What first-party observes:** every event on one site, and nothing whatsoever about the other nine.
- **What third-party observes:** the same identifier across all 10 sites, so co-visits become visible.
- **The reach arithmetic:** 10 embedded sites give C(10,2) = 45 observable cross-site pairs; first-party gives 0.
- **Coverage per island:** one first-party cookie sees 1/10 = 10.0% of the 10-site graph it used to see whole.
- **Not a volume loss:** the per-site event counts are unchanged — only the joins between them disappear.
- **The reporting consequence:** cross-site reach, frequency, and attribution are not degraded, they are undefined.

### Visualization (canvas `c1`, 720×340)

Two side-by-side node diagrams over the same 10 sites: a hub-and-spoke third-party view with every pair joinable, and a first-party view of 10 disconnected islands. All counts computed in JS from `SITES = 10` (`pairs = SITES*(SITES-1)/2`, `coverage = 1/SITES`).

- **Title (bold 17px `#1a5276`, centered, y=22):** "Same Vendor, Same Code, Two Different Instruments" — subtitle (15px `#555`, centered, y=44): "Illustrative Example — measurement code embedded on 10 top-level sites".
- **Panel headings (bold 15px, y=72):** "Third-party cookie" in `#e67e22` centered at x=180; "First-party only" in `#27ae60` centered at x=530.
- **Left panel (third-party):** 10 site nodes as 9px-radius circles in `rgba(26,82,118,0.35)` with 1px `#1a5276` stroke, placed on a circle of radius 78 centred at (180,180), node *i* at angle −90° + i·36°. A central hub node: 16px-radius circle at (180,180) filled `#e67e22`, with the text "one ID" in white 11px centered inside it.
- **Left panel spokes:** 1px `rgba(230,126,34,0.75)` line from the hub to each of the 10 nodes.
- **Left panel chords:** all 45 pairs drawn as 0.5px `rgba(230,126,34,0.28)` straight lines between every pair of site nodes (loop i<j).
- **Left panel caption (centered at x=180, y=290 and y=310):** "45 joinable site pairs" bold `#e67e22` 15px (text built as `pairs + ' joinable site pairs'`); "reach, frequency, attribution all defined" 14px `#555`.
- **Right panel (first-party):** the same 10 nodes at the same angles on a circle of radius 78 centred at (530,180), same fill and stroke. No hub, no spokes, no chords. Each node gets a 1.5px `#27ae60` circle of radius 20 drawn around it (the storage island).
- **Right panel caption (centered at x=530, y=290 and y=310):** "0 joinable site pairs" bold `#27ae60` 15px (text built as `0 + ' joinable site pairs'`); "each site sees 10.0% of the graph" 14px `#555` (percentage computed as `coverage*100`).
- **Bottom bold red (`#e74c3c`, centered, y=332):** "Per-site event counts are identical in both panels. Only the joins vanish."

## A Partitioned Storage Model Splits One Person Into Many Identifiers

**165,000 Identifiers for 100,000 People — and 10,000 for Another 30,000**

- **The mechanism:** storage is keyed by (top-level site, embedded origin), so the same code gets a fresh slot per site.
- **One-to-many:** a person on 5 sites holds 5 identifiers, and a unique-visitor count adds all 5.
- **The inflation:** 60,000×1 + 25,000×2 + 10,000×3 + 5,000×5 = 165,000 identifiers for 100,000 people.
- **The factor:** 165,000 / 100,000 = 1.65×, so "unique visitors" overstates people by 65% on private devices.
- **Many-to-one:** 30,000 people sharing 10,000 household devices collapse to 10,000 identifiers, a 0.333× factor.
- **They do not cancel:** together, 130,000 people appear as 175,000 identifiers — a 1.346× net inflation.
- **Direction is unknowable:** the same reported number can be an overcount or an undercount by segment.
- **What to do:** treat identifier counts as an upper bound on devices, never as an estimate of people.

### Visualization (canvas `c2`, 720×340)

Sankey-style flow from four person-cohorts (plus a shared-device cohort) into identifier counts, with the inflation factor computed at render time. All widths derived in JS from the cohort literals `[[60000,1],[25000,2],[10000,3],[5000,5]]` and `SHARED_PEOPLE = 30000`, `SHARED_DEVICES = 10000`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "One Person, k Identifiers — and k People, One Identifier" — subtitle (15px `#555`, y=44): "Illustrative Example — 130,000 people under a partitioned storage model".
- **Column headings (bold 15px `#1a5276`, y=72):** "People" left aligned at x=50; "Sites visited" centered at x=330; "Identifiers created" right aligned at x=690.
- **Rows (five rows, row *i* at y = 92 + i·34, bar height 22):** bar scale `SC = 240` people or identifiers per pixel; left bar starts at x=50, width = people/SC px; right bar ends at x=690, width = identifiers/SC px (so it starts at 690 − width). Largest bar is 60,000/240 = 250px, keeping both columns clear of the centre label.
  - 60,000 people × 1 site → 60,000 ids. Left `rgba(26,82,118,0.35)`, right `rgba(26,82,118,0.35)`.
  - 25,000 × 2 → 50,000. Left `rgba(26,82,118,0.35)`, right `#e67e22`.
  - 10,000 × 3 → 30,000. Left `rgba(26,82,118,0.35)`, right `#e67e22`.
  - 5,000 × 5 → 25,000. Left `rgba(26,82,118,0.35)`, right `#e74c3c`.
  - 30,000 shared-device people × 1 site, 3 per device → 10,000. Left `rgba(26,82,118,0.35)`, right `#2980b9`.
- **Row labels:** people count in `#333` 14px left aligned at x=50 above its bar is replaced by an inline label — draw the formatted people count in `#1a5276` 14px at x = 50 + leftWidth + 6, vertically centred in the bar; draw the "×k sites" (or "3 per device") text in `#555` 14px centered at x=330; draw the formatted identifier count in its bar's color 14px right aligned at x = 690 − rightWidth − 6.
- **Totals rule:** 1.5px `#1a5276` horizontal line from x=50 to x=690 at y=272.
- **Totals line (bold 15px, y=292):** "130,000 people" in `#1a5276` left aligned at x=50; "175,000 identifiers" in `#e74c3c` right aligned at x=690. Both computed as sums.
- **Bottom bold red (centered, y=316):** "Private devices inflate 1.65×, shared devices deflate 0.333×, net 1.346× — they do not cancel." All three factors computed at render time.
- **Bottom gray (`#555`, centered, y=334):** "Identifier count is an upper bound on devices. It is not an estimate of people."

## A Storage Eviction Cap Rewrites "Returning Visitor" as a Policy Setting

**Same 120,000 Visitors: New-Visitor Share Moves 16.67% → 66.67% by Cap Alone**

- **The mechanism:** client-side storage carries a maximum retention window, and eviction is silent.
- **The consequence:** any visitor whose gap between visits exceeds the cap arrives with no prior identifier.
- **Reclassified, not changed:** that person is logged as new, though the behaviour is identical to last month.
- **The composition:** of 120,000 visitors, 40,000 return within 3 days, 25,000 at 10, 20,000 at 21, 15,000 at 90.
- **Truth:** 100,000 of the 120,000 are genuinely returning, so the true new-visitor share is 16.67%.
- **Cap 400 days:** nothing is evicted and the report reproduces the truth — new share 16.67%.
- **Cap 30 days:** the 90-day cohort resets, so new = 35,000 and the reported new share is 29.17%.
- **Cap 7 days:** the 10-, 21- and 90-day cohorts all reset, new = 80,000, and new share reads 66.67%.
- **The distortion factor:** 80,000 / 20,000 = 4.0× more "new visitors" from a storage setting, not from growth.

### Visualization (canvas `c3`, 720×340)

Grouped bar chart: three eviction caps side by side, each a stacked returning/new bar over the identical 120,000-visitor population, with the new-visitor percentage computed per bar. All values derived in JS from the gap-cohort literals `[[40000,3],[25000,10],[20000,21],[15000,90]]` and `TRUE_NEW = 20000`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Behaviour Held Constant. The Cap Moves the Metric." — subtitle (15px `#555`, y=44): "Illustrative Example — 120,000 monthly visitors, identical in all three bars".
- **Scale:** 120,000 visitors = 200px of bar height; baseline y=272; bar width 120.
- **Bars (x = 90, 300, 510; caps 400, 30, 7 days):** for each, `newCount` = TRUE_NEW + sum of cohort counts with gap > cap; `retCount` = 120,000 − newCount.
  - Returning segment: rect from y = 272 − retHeight, height retHeight, in `rgba(26,82,118,0.35)`.
  - New segment: rect stacked above it, height newHeight, in `#e74c3c`.
  - Value labels: returning count in `#1a5276` 14px centered in its segment; new count in white 14px centered in its segment (only when the segment exceeds 20px, otherwise in `#e74c3c` just above the bar).
- **Per-bar captions (centered under each bar):** cap text "Cap 400 days" / "Cap 30 days" / "Cap 7 days" in `#333` 15px at y=292; computed new-share "new = 16.67%" / "29.17%" / "66.67%" in bold `#e74c3c` 15px at y=312.
- **Truth line:** dashed (6/4) 2px `#27ae60` horizontal line from x=60 to x=660 at the height of the true returning count (272 − 100,000-scaled = y=105.33), labelled "true returning = 100,000 (83.33%)" in `#27ae60` 14px left aligned at x=60, y=99.
- **Bottom bold red (centered, y=334):** "4.0× more \"new visitors\" with zero change in who visited or when." Factor computed as `newAt7 / TRUE_NEW`.

## A Deprecation Wave Is a Regime Change, Not a Trend

**Behaviour Flat at 0.0%; the Year-Over-Year Report Says −60.0%**

- **The setup:** the same 12-month human series repeats exactly, so the true year-over-year change is 0.0%.
- **The only change:** the observation share drops from 5/6 to 1/3 when a tracking-prevention policy ships.
- **Regime A total:** the 12 observed monthly counts sum to 1,250,000 against 1,500,000 real humans.
- **Regime B total:** the identical humans are observed as 500,000 — the instrument, not the audience, halved.
- **The naive comparison:** 500,000 / 1,250,000 − 1 = −60.0%, exactly (1/3) ÷ (5/6) − 1, entirely instrumental.
- **What survives:** within-regime growth is +20.0% in both years, because a ratio cancels the observation share.
- **Denominator contamination:** server-side conversions are unaffected, so conversion rate moves the other way.
- **The rate numbers:** true 2.50% is reported as 3.00% in regime A and 7.50% in regime B — a 2.5× jump.
- **The tell:** a step with no ramp, aligned to a release date and not to any product change, is an instrument.

### Visualization (canvas `c4`, 720×360)

24-month line/bar chart with a vertical regime boundary: the flat true-human series on top, the two observed series stepping down beneath it, and both the naive YoY and the within-regime growth computed at render time. All series derived in JS from `TRUE_K = [120,114,126,120,126,120,114,120,126,132,138,144]`, `SHARE_A = 5/6`, `SHARE_B = 1/3`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "One Step in the Instrument, Read as a 60% Decline" — subtitle (15px `#555`, y=44): "Illustrative Example — the same 12 human months repeated twice".
- **Plot frame:** x from 60 to 690, baseline y=250, top of scale y=70; y-scale 150,000 visitors = 180px (so `y(v) = 250 − v/150000*180`).
- **Axis:** 1px `#ccc` baseline from (60,250) to (690,250); 1px `#e0e0e0` gridlines at 50,000 / 100,000 / 150,000 with labels "50K" / "100K" / "150K" in `#999` 12px right aligned at x=56.
- **True human series:** 24 points (the 12 values twice) at x = 60 + (i+0.5)·(630/24), joined by a 2px `#27ae60` line, drawn across the boundary without a break. Label "true humans (unchanged)" in `#27ae60` 14px at x=66, y = y(144000) − 8.
- **Observed series:** months 0-11 at `TRUE_K[i]*1000*SHARE_A`, months 12-23 at `TRUE_K[i]*1000*SHARE_B`. Drawn as two separate 2.5px `#1a5276` polylines with **no segment joining month 11 to month 12** — the gap is the point.
- **Observed markers:** 3px-radius `#1a5276` dots at each observed point.
- **Regime boundary:** dashed (5/5) 2px `#e74c3c` vertical line at x = 60 + 12·(630/24) = 375, from y=60 to y=258; label "policy ships" in `#e74c3c` bold 14px centered at x=375, y=54.
- **Regime labels (14px, centered):** "Regime A — observes 5/6" in `#1a5276` at x=215, y=270; "Regime B — observes 1/3" in `#1a5276` at x=535, y=270.
- **Regime totals (bold 15px, centered):** "12-mo total 1,250,000" in `#1a5276` at x=215, y=290; "12-mo total 500,000" in `#1a5276` at x=535, y=290. Both computed sums.
- **Naive YoY callout (bold 15px `#e74c3c`, centered, y=314):** "Naive year-over-year: −60.0% — true change 0.0%". The −60.0% computed as `totalB/totalA − 1`.
- **Within-regime callout (15px `#27ae60`, centered, y=334):** "Within regime A: +20.0%. Within regime B: +20.0%. Truth: +20.0%." All three computed as last/first − 1.
- **Bottom gray (`#555`, centered, y=354):** "Conversion rate on 3,000 server-side conversions: true 2.50%, regime A 3.00%, regime B 7.50%." All three computed from `CONV = 3000` divided by the month-1 true and observed counts.

## Fingerprinting Is a Probabilistic Identifier Whose Accuracy Falls With Scale

**18 Bits Is 99.62% Unique at N=1,000 and 2.20% Unique at N=1,000,000**

- **What it is:** a fingerprint is a hash of observable configuration, so it is a bucket label, not an ID.
- **The capacity:** H bits of entropy give 2^H distinguishable configurations — at H=18 that is 262,144 buckets.
- **Expected collisions:** the number of *other* people sharing your exact fingerprint is (N−1)·2^−H.
- **The birthday form:** P(not unique) = 1 − (1 − 2^−H)^(N−1), which grows with N at fixed H.
- **The degradation:** at H=18, P(not unique) runs 0.38% → 3.74% → 31.71% → 97.80% as N runs 10³ → 10⁶.
- **Scale is the enemy:** the instrument does not get worse — the population it must separate gets bigger.
- **The entropy needed:** 95% uniqueness at N=10⁶ requires H = log2(999,999 / −ln 0.95) = 24.22 → 25 bits.
- **The cost of those bits:** 18 → 25 bits is 2⁷ = 128× the configuration space for one 95% guarantee.
- **The undercount:** at H=18, N=10⁶, only 256,365 distinct fingerprints exist — a 74.36% deficit in "unique users".

### Visualization (canvas `c5`, 720×360)

Log-x curve of P(not unique) against population size for two entropy levels, with the four tabulated points marked and every probability computed from the closed form at render time. All values derived in JS from `H_LOW = 18`, `H_HIGH = 25`, `N_POINTS = [1e3, 1e4, 1e5, 1e6, 1e7]`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Fixed Entropy, Growing Population: Uniqueness Collapses" — subtitle (15px `#555`, y=44): "Illustrative Example — P(not unique) = 1 − (1 − 2^−H)^(N−1)".
- **Plot frame:** x from 90 to 660 mapping log10(N) over 3 → 7; y from 250 (P=0) to 80 (P=1), so `y(p) = 250 − p*170`.
- **Axes:** 1px `#ccc` lines along x (y=250, from x=90 to x=660) and y (x=90, from y=76 to y=250). X ticks at N = 10³, 10⁴, 10⁵, 10⁶, 10⁷ labelled "1K", "10K", "100K", "1M", "10M" in `#666` 13px centered at y=268. Y ticks at 0%, 25%, 50%, 75%, 100% labelled in `#666` 13px right aligned at x=84, with 1px `#eeeeee` gridlines.
- **H=18 curve:** 2.5px `#e74c3c` polyline, one sample per pixel of x, `p = 1 − Math.pow(1 − Math.pow(2,−18), N−1)` with N = 10^(3 + (x−90)/570·4). Label "H = 18 bits" in `#e74c3c` bold 14px placed at the curve, left aligned at x=300, y = y(p at x=300) − 10.
- **H=25 curve:** 2.5px `#27ae60` polyline, same formula with H=25. Label "H = 25 bits" in `#27ae60` bold 14px left aligned at x=520, y = y(p at x=520) + 18.
- **Marked points:** 4px-radius `#e74c3c` dots on the H=18 curve at N = 10³, 10⁴, 10⁵, 10⁶, each with its computed percentage printed in `#e74c3c` 13px, offset (+8, −8): "0.38%", "3.74%", "31.71%", "97.80%".
- **95% target line:** dashed (5/4) 1.5px `#1a5276` horizontal line at `y(0.05)` (i.e. P(not unique)=5%, the 95%-unique level) from x=90 to x=660, labelled "5% collision budget" in `#1a5276` 13px right aligned at x=656, y = y(0.05) − 6.
- **Required-entropy annotation (15px, centered, y=290):** "For 95% uniqueness at N = 1,000,000: H = 24.22 bits → 25 bits". The 24.22 computed as `Math.log2((N−1)/(−Math.log(0.95)))`.
- **Bottom bold red (centered, y=316):** "Same instrument, 99.62% unique at 1K and 2.20% unique at 1M." Both computed.
- **Bottom gray (`#555`, centered, y=340):** "At H=18, N=1M: only 256,365 distinct fingerprints exist — a 74.36% undercount of people." Both figures computed as `Math.pow(2,18)*(1 − Math.pow(1 − Math.pow(2,−18), 1e6))` and `1 − that/1e6`.

## A Fired Beacon Is Evidence of a Load, Not of a Human

**1,000,000 Fires Cover 775,000 Human Views: 62.0% Precision, 80.0% Recall**

- **What a beacon proves:** a request reached the collector, which means a load happened somewhere.
- **What it does not prove:** that a person was present, awake, or looking at the rendered pixel.
- **False positives:** of 1,000,000 fires, 140,000 are automated fetches and 120,000 are speculative preloads.
- **Double counting:** 80,000 fires are duplicates from retries and restored back-navigation pages.
- **Rendered but unseen:** 40,000 fires come from offscreen renders with no human view behind them at all.
- **Precision:** 620,000 of the 1,000,000 fires correspond to a human view, so precision is 62.0%.
- **False negatives:** 155,000 genuine human views fired nothing, so true human views are 775,000.
- **Recall:** 620,000 / 775,000 = 80.0%, and the two error directions partly offset by accident.
- **The dangerous part:** the headline total is only 29.03% high, so the 38% precision defect stays invisible.

### Visualization (canvas `c6`, 720×340)

Stacked decomposition bar of 1,000,000 fires against a separate true-human-views bar, with precision, recall and total error computed at render time. All widths derived in JS from `FIRES = [620000,140000,120000,80000,40000]` and `MISSED = 155000`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "The Beacon Log Is Not a Log of People" — subtitle (15px `#555`, y=44): "Illustrative Example — 1,000,000 beacon fires vs 775,000 true human views".
- **Scale:** 1,000,000 = 600px of bar width; bars start at x=60, height 40.
- **Fires bar (y=84):** five stacked segments left to right, width = count/1,000,000·600.
  - 620,000 human view — `#27ae60` (372px)
  - 140,000 automated fetch — `#e67e22` (84px)
  - 120,000 speculative preload — `#e67e22` at 0.6 alpha (72px)
  - 80,000 duplicate fire — `#e74c3c` (48px)
  - 40,000 offscreen render — `#e74c3c` at 0.6 alpha (24px)
- **Fires bar label:** "1,000,000 beacon fires" in `#1a5276` bold 15px left aligned at x=60, y=76.
- **Legend (five rows starting y=142, 18px apart, swatch 12×12 at x=60, text at x=78 in `#333` 14px):** each row reads "620,000 — human view (62.0%)" etc., with the percentage computed as count/total.
- **True-views bar (y=248):** single rect (60,248) width 465px (775,000 scaled) in `rgba(39,174,96,0.5)`, with the 620,000 matched portion overdrawn (60,248) 372×40 in `#27ae60` and the 155,000 unmatched remainder left at the lighter fill, hatched with 1px `#27ae60` diagonal lines every 6px. Label "775,000 true human views" in `#27ae60` bold 15px left aligned at x=60, y=240; "155,000 fired nothing" in `#27ae60` 13px left aligned at x=436, y=272.
- **Bottom bold red (centered, y=312):** "Precision 62.0%, recall 80.0% — total only 29.03% high." All three computed.
- **Bottom gray (`#555`, centered, y=332):** "Two large errors of opposite sign. A close total hides both."

## An Aggregate Reporting Scheme Changes the Unit of Observation

**1,006 Conversions Become 8 Noisy Group Totals and 0 Person Records**

- **The substitution:** per-person events are replaced by group counts with noise added and small groups withheld.
- **The unit changes:** the observation is no longer a person, so per-person questions have no denominator.
- **Not recoverable:** reach and frequency need a per-person visit count, which no group total contains.
- **Silent suppression:** a minimum-group threshold of 10 drops the group of 6, removing 0.60% with no error.
- **Noise is per group, not per total:** σ = 10 added to each of 8 groups is 2.83% of the 1,000 released total.
- **But per group it dominates:** relative noise runs 2.5% on the group of 400 and 100.0% on the group of 10.
- **The usable subset:** only 5 groups have count ≥ 3σ, covering 950 of 1,006 conversions — 94.43%.
- **The unusable tail:** the groups of 25, 15 and 10 carry 4.97% of conversions at ≥40% relative noise each.
- **The right read:** trust the aggregate, refuse the small cells, and stop reporting per-person metrics entirely.

### Visualization (canvas `c7`, 720×360)

Bar chart of the nine true group counts with per-group noise bars and a suppression threshold line, plus computed relative-noise labels. All values derived in JS from `GROUPS = [400,250,160,90,50,25,15,10,6]`, `SIGMA = 10`, `MIN_GROUP = 10`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Aggregate Precise, Every Small Cell Worthless" — subtitle (15px `#555`, y=44): "Illustrative Example — 1,006 conversions, σ = 10 per group, minimum group 10".
- **Scale:** log-free linear, 400 conversions = 170px; baseline y=250; nine bars, width 52, x = 62 + i·72.
- **Bars:** count ≥ 3σ → `rgba(26,82,118,0.35)` with 1px `#1a5276` stroke; 3σ > count ≥ MIN_GROUP → `#e67e22`; count < MIN_GROUP → `#e74c3c` at 0.45 alpha with a dashed (3/3) 1px `#e74c3c` outline.
- **Noise whiskers:** on every bar with count ≥ MIN_GROUP, a 1.5px `#e74c3c` vertical line spanning ±σ around the bar top, with 6px end caps.
- **Value labels:** true count in its bar's color 13px bold centered above the whisker; relative noise "σ/n" as "2.5%", "4.0%", "6.3%", "11.1%", "20.0%", "40.0%", "66.7%", "100.0%" in `#e74c3c` 12px centered at y=268, computed as `SIGMA/count*100`.
- **Suppressed bar annotation:** "suppressed" in `#e74c3c` bold 12px centered under the ninth bar at y=286.
- **3σ usability line:** dashed (5/4) 1.5px `#27ae60` horizontal line at the height of 3σ = 30 conversions, from x=56 to x=680, labelled "3σ = 30 conversions" in `#27ae60` 13px right aligned at x=676, 6px above the line.
- **Summary block (three lines, 14px, left aligned at x=62, y=306 / 324 / 342):**
  - "Released total 1,000 of 1,006 (99.40%) — 0.60% suppressed silently" in `#1a5276`.
  - "Total noise σ√8 = 28.28 on 1,000 = 2.83% — the aggregate is fine" in `#27ae60`.
  - "Person-level records released: 0. Reach and frequency are not derivable." in `#e74c3c` bold.
  All numerals in these lines computed at render time from `GROUPS`, `SIGMA` and `MIN_GROUP`.

## Segment by Observability Regime, Re-Baseline at the Step, Never Mix

**A Pooled 24-Month Mean of 72,917 Is 30.0% Below One Regime and 75.0% Above the Other**

- **The rule:** an observability regime is a dimension of the data, so put it in the group-by, not in the total.
- **Why pooling fails:** the 24-month mean is 1,750,000 / 24 = 72,917, a month that occurred in neither regime.
- **The two truths:** regime A averages 104,167/month and regime B averages 41,667/month.
- **The distance:** 72,917 is 30.0% below regime A and 75.0% above regime B — it describes no observed month.
- **Re-baseline, don't backfill:** index each regime to its own first month and compare shapes, not levels.
- **What is comparable:** ratios inside one regime — both years show +20.0% growth, matching the truth exactly.
- **What is not:** any level, any absolute count, and any rate whose denominator crosses the boundary.
- **Annotate the series:** mark the boundary in the data model itself so no future query pools across it.
- **Say it out loud:** report "instrument changed, level not comparable" rather than a number nobody can use.

### Visualization (canvas `c8`, 720×340)

Two-panel comparison of the same 24 observed months: a pooled view with one mean line that fits nothing, and a re-baselined index view where both regimes overlay exactly. All values derived in JS from the same `TRUE_K`, `SHARE_A`, `SHARE_B` constants as chart `c4`.

- **Title (bold 17px `#1a5276`, centered, y=22):** "Pool and the Mean Fits Nothing. Re-Baseline and Both Regimes Agree." — subtitle (15px `#555`, y=44): "Illustrative Example — identical human behaviour, two instruments".
- **Left panel — pooled levels (heading "Pooled levels" bold 15px `#1a5276` centered at x=190, y=72):** plot x from 60 to 330, baseline y=250, scale 150,000 = 160px. The 24 observed monthly counts drawn as 8px-wide bars in `rgba(26,82,118,0.35)`, x = 60 + i·11.
  - Pooled mean: 2px solid `#e74c3c` horizontal line at the 72,917 level across x=56 to x=334, labelled "pooled mean 72,917" in `#e74c3c` bold 13px left aligned at x=60, 6px above the line.
  - Regime means: 1.5px dashed (4/3) `#1a5276` lines at 104,167 (x=60 to x=192) and 41,667 (x=198 to x=330), labelled "104,167" and "41,667" in `#1a5276` 12px above each.
  - Caption (13px `#e74c3c` centered at x=190, y=274): "−30.0% vs A, +75.0% vs B" — both computed.
- **Right panel — re-baselined index (heading "Re-baselined per regime" bold 15px `#27ae60` centered at x=520, y=72):** plot x from 390 to 660, baseline y=250, index scale 100 = 120px so `y(idx) = 250 − idx/100*120`.
  - Regime A index: `obsA[i]/obsA[0]*100`, drawn as a 2.5px `#1a5276` polyline over 12 points at x = 390 + i·(270/11).
  - Regime B index: `obsB[i]/obsB[0]*100`, drawn as a 2.5px dashed (6/4) `#27ae60` polyline over the same 12 x positions.
  - Because both series are the same human series scaled, the two polylines coincide at every point; note "both lines coincide — identical shape" in `#27ae60` 13px centered at x=520, y=274.
  - Index 100 reference: 1px `#e0e0e0` line at `y(100)`; index 120 reference: 1px `#e0e0e0` line at `y(120)` labelled "120" in `#999` 12px right aligned at x=386.
  - End labels (bold 13px, right aligned at x=656): "A: 120.0" in `#1a5276` at `y(indexA[11]) − 8`; "B: 120.0" in `#27ae60` at `y(indexB[11]) + 16`. Both computed.
- **Bottom bold green (`#27ae60`, centered, y=302):** "Within-regime growth: A +20.0%, B +20.0%, truth +20.0%." All computed.
- **Bottom gray (`#555`, centered, y=326):** "Segment by regime. Compare ratios, never levels. Never average across the step."

## Regeneration instructions

- **Layout:** detail page. `h1` carrying the title text with **no index number** + `.subtitle` paragraph + one `.philosophy` callout, then one **unnumbered** `<h2>` per pitfall followed by a one-row `.obj-table`: left `<td>` (50%) holds an `.obj-title` div — a refined restatement of the pitfall carrying a concrete number from that section's own bullets, never a copy of the heading — plus a `<ul>` of labeled bullets; right `<td>` (50%, centered) holds the canvas. Even table rows have background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border, padding-bottom 8px; subtitle `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. Table cell borders `1px solid #e0e0e0`, padding 20px 24px. `td:first-child` and `td:last-child` are both **50%** — shrink a chart via the canvas `style.maxWidth`, never by narrowing the cell. No nav bar, no back/home links, no cross-page links, no `thead`, no status badges.
- **Canvas:** intrinsic `width` 720 on every chart, `height` per chart as given (340, 340, 340, 360, 360, 340, 360, 340). A shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). All drawing stays within x ≤ 702 so nothing overflows the logical canvas.
- **Single source of truth:** the script declares each scenario block's constants **once** at the top — `SITES = 10`; `COHORTS = [[60000,1],[25000,2],[10000,3],[5000,5]]`, `SHARED_PEOPLE = 30000`, `SHARED_PER_DEVICE = 3`; `GAPS = [[40000,3],[25000,10],[20000,21],[15000,90]]`, `TRUE_NEW = 20000`, `CAPS = [400,30,7]`; `TRUE_K = [120,114,126,120,126,120,114,120,126,132,138,144]`, `SHARE_A = 5/6`, `SHARE_B = 1/3`, `CONV = 3000`; `H_LOW = 18`, `H_HIGH = 25`, `TARGET_UNIQ = 0.95`; `FIRES = [620000,140000,120000,80000,40000]`, `MISSED = 155000`; `GROUPS = [400,250,160,90,50,25,15,10,6]`, `SIGMA = 10`, `MIN_GROUP = 10`. Every printed count, percentage, ratio, factor, entropy value and bar dimension is derived from those constants at render time. No statistic is typed as a string literal in any label.
- **Helpers:** `num(v)` formats an integer with thousands separators; `pct(v, d)` formats a fraction as a percentage with `d` decimals; `notUnique(H, N) = 1 − Math.pow(1 − Math.pow(2,−H), N−1)`; `sum(a)` reduces an array.
- **No `Math.random()` anywhere on this page.** All data is a hardcoded literal count or is computed in closed form from the constants above. If a future edit needs generated data, add an inline seeded generator per chart with its own fixed seed: `function lcg(seed){var s=seed;return function(){s=(s*16807)%2147483647;return s/2147483647;};}` — and still compute every printed statistic from the plotted points.
- **Palette:** primary blue `#1a5276` / `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#555`/`#333`, axis gray `#999`/`#ccc`/`#e0e0e0`.
- **Naming and sourcing:** no real browser, vendor, product, or feature names anywhere. Say "a tracking-prevention policy", "a partitioned storage model", "the browser's storage eviction policy", "an aggregate reporting scheme". Measurement vendors are "Vendor A" / "Vendor B"; people are Alice/Bob. No identifier strings, tokens, or `key=value` credential syntax appear at all — identity is drawn as nodes and counts. Every constructed figure is labeled "Illustrative Example"; no real-world lifetime, deprecation date, or entropy measurement is asserted.
- Chart ids are sequential in page order, `c1` through `c8`.
