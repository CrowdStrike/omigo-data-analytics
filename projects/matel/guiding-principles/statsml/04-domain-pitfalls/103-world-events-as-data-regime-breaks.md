# World Events as Data Regime Breaks

**Page type:** detail page (one h2 per event, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 103. World Events as Data Regime Breaks

**Subtitle:** Major events that changed EVERYTHING overnight — data before and after are from different worlds, so models trained on pre-event data are instantly obsolete.

Shared chart helpers: every chart uses gray L-shaped axes (`#666`, width 1) from (50,10) down to (50,170) and across to (700,170), with an x-axis label centered at the bottom and a rotated y-axis label on the left (17px `#666`). A shared `drawBreakLine` helper draws a vertical dashed red regime-break line (`#e74c3c`, dash 6/4, width 2) from y=10 to y=170 with a bold red label near the top. Series y-values below are canvas coordinates (lower y = higher value).

## COVID-19 (March 2020)

**Obj-title:** Every Behavior Shifted Overnight — the "Normal" Everything Was Calibrated Against Ceased to Exist

- **Overnight shifts:** Travel -95%, e-commerce +300%, restaurants -80% within a matter of weeks.
- **Zero-to-everything:** Remote work jumped 0% → 60% and telehealth 0 → 100%, from nothing to normal.
- **Instant obsolescence:** ALL models trained on 2019 data became worthless in March 2020.
- **New normal:** Recovery wasn't return-to-normal but a transition to a NEW normal that stayed.
- **What stayed:** Hybrid work, the delivery economy, and changed consumer spending patterns.
- **Key lesson:** Pre-COVID data is from a different world — you cannot predict post-COVID behavior from it.

### Visualization (canvas `canvas1`, 720×300 declared; setupCanvas renders at 720×200)

Three-series regime-break line chart. Axes: x "Time (2019-2021)", y "Activity Level". Vertical break line labeled "Mar 2020" at x=280.

- **Travel (blue `#2980b9`, width 2.5):** flat noisy pre period around y=140 (20 points), then post-break y path `[140, 130, 80, 20, 15, 12, 10, 10, 12, 15, 20, 30, 40, 55, 70, 80, 90, 95, 100, 105]` — note lower y = higher activity, so this shows a crash then partial recovery in canvas terms (drawn values used directly).
- **E-commerce (green `#27ae60`):** flat pre around y=130, then post `[130, 135, 100, 70, 55, 50, 45, 42, 40, 42, 45, 50, 55, 58, 60, 62, 65, 68, 70, 72]` (rises sharply, settles higher).
- **Remote work (purple `#8e44ad`):** flat pre around y=155, then post `[155, 150, 100, 70, 65, 62, 60, 60, 62, 65, 68, 70, 75, 80, 85, 88, 90, 92, 95, 98]`.
- **Legend (top right, 12px swatches):** blue "Travel", green "E-commerce", purple "Remote Work". Pre-period noise uses Math.random (small ±5 jitter).

## 2008 Financial Crisis / Real Estate Crash

**Obj-title:** Zero Correlations Became 1.0 Simultaneously

- **The impossible:** Housing prices that "can't go down" dropped 30-50%.
- **Correlation collapse:** Correlations that were zero became 1.0 simultaneously as all assets crashed at once.
- **Model failure:** Risk models built on "housing and stocks are uncorrelated" failed catastrophically.
- **AIG exposure:** AIG had insured billions in "impossible" correlated defaults that then happened together.
- **Key lesson:** Correlations restructure during crises; models assuming stable correlations fail catastrophically.

### Visualization (canvas `canvas2`, 720×300 declared; setupCanvas renders at 720×200)

Two-series regime-break line chart. Axes: x "Time (2000-2012)", y "Housing Index / Correlation". Break line "2007-2008" at x=320.

- **Housing price (solid blue `#2980b9`, width 2.5):** 36-point y path `[140, 135, 128, 120, 112, 105, 98, 90, 82, 74, 66, 58, 52, 48, 45, 43, 42, 42, 43, 43, 43, 50, 60, 70, 80, 85, 87, 88, 88, 87, 86, 85, 90, 95, 100, 105]` (steady rise into the crisis, then crash and slow recovery, in canvas-y terms).
- **Cross-asset correlation (dashed red `#e74c3c`, dash 4/3, width 2):** 36-point y path `[150, 148, 152, 149, 147, 150, 153, 148, 145, 150, 148, 146, 145, 142, 130, 90, 55, 40, 38, 40, 45, 55, 70, 90, 110, 130, 140, 145, 148, 150, 148, 145, 147, 149, 150, 148]` (flat near zero, spikes to ~1.0 during the crisis, then relaxes).
- **Legend:** blue "Housing Price", red "Cross-Asset Correlation".

## Dot-Com Bubble (1999-2001)

**Obj-title:** Data From 1997-2000 Reflects a Mass Delusion, Not Economic Reality

- **The crash:** Nasdaq fell 5000 → 1100, a 78% collapse from the March 2000 peak.
- **Zero-revenue valuations:** Companies valued at $10B with zero revenue went bankrupt outright.
- **Worthless models:** All valuation models based on "eyeballs" and "growth rate" became worthless.
- **Metric reset:** Revenue became the ONLY metric that mattered once the bubble deflated.
- **Poisoned training data:** Training on bubble-era data teaches that "losing money fast = high valuation."
- **Key lesson:** Bubble-era data encodes mass delusion; training on it produces delusional models.

### Visualization (canvas `canvas3`, 720×300 declared; setupCanvas renders at 720×200)

Nasdaq rise-and-crash line chart. Axes: x "Time (1996-2003)", y "Nasdaq Index". Break line "Mar 2000" at x=240.

- **Nasdaq (blue `#2980b9`, width 2.5):** 36-point y path `[150, 145, 138, 130, 120, 108, 95, 80, 65, 50, 40, 32, 28, 25, 23, 22, 25, 30, 40, 55, 75, 100, 120, 130, 138, 142, 145, 148, 150, 152, 153, 155, 156, 157, 158, 158]` (climb to peak, then 78% crash).
- **Training-region overlay:** rectangle x=55–255, full height, filled `rgba(231,76,60,0.12)`; bold 14px red label "Models trained here" plus 12px "(mass delusion period)".
- **Point labels (17px `#1a5276`):** "Peak: 5048" near the top, "Bottom: 1114" near the trough.

## ChatGPT Release (Nov 2022) / LLM Era

**Obj-title:** Developer Productivity Metrics Became Incomparable Pre/Post

- **The shift:** AI-generated code went from 0% to 30-40% of new commits within 12 months.
- **Displaced behavior:** Developer Q&A-site traffic fell 50% and conversational search queries tripled.
- **The cause:** People asked an AI assistant instead of searching or posting on a Q&A forum.
- **Broken metrics:** "Lines of code" is meaningless once AI generates 80% of the lines in a change.
- **Unattributable code:** Code review cannot tell human-written code from AI-written code.
- **Key lesson:** When the tool changes, all productivity metrics become incomparable across the boundary.

### Visualization (canvas `canvas4`, 720×300 declared; setupCanvas renders at 720×200)

Two crossing trend lines. Axes: x "Time (2021-2024)", y "AI-Generated Code %". Break line "Nov 2022" at x=260.

- **AI-generated code % (blue `#2980b9`, width 2.5):** 30-point y path — flat near y≈157 pre-break, then `[155, 150, 140, 125, 110, 95, 82, 70, 62, 55, 50, 47, 44, 42, 40, 38, 37, 36]` (rapid rise from zero).
- **Q&A-site traffic (orange `#e67e22`, width 2):** flat near y≈50 pre-break, then `[52, 60, 75, 90, 100, 108, 115, 120, 125, 128, 130, 132, 135, 137, 139, 140, 142, 143]` (steady decline).
- **Legend:** blue "AI-Generated Code %", orange "Q&A-Site Traffic".

## Claude Code / AI Coding Assistants (2024-2025)

**Obj-title:** The Tool Changed, Not the Developers — Baselines Invalidated Anyway

- **Workflow break:** Prototyping went from days to minutes with an AI assistant in the loop.
- **Different code:** AI-generated code has its own bug patterns, style, and test-coverage profile.
- **Shifted metrics:** "Time to first commit," "PR size," and "code review comments" all shifted.
- **Wrong attribution:** The TOOL changed, not the developers, yet every historical baseline moved with it.
- **Invalid baselines:** Quality metrics trained on human-written code don't apply to AI-generated code.
- **Key lesson:** Tool changes break metric comparability even when the humans haven't changed.

### Visualization (canvas `canvas5`, 720×300 declared; setupCanvas renders at 720×200)

Distribution shift: unimodal vs bimodal PR-size curves. Axes: x "PR Size (lines changed)", y "Frequency".

- **Pre-2024 (dashed gray `#999`, dash 5/3, width 2):** single Gaussian, height 120·exp(−((i−8)/5)²) over 30 points at 20px spacing from x=55, baseline y=165.
- **Post-2024 (solid blue `#2980b9`, width 2.5):** bimodal sum 90·exp(−((i−5)/3)²) + 80·exp(−((i−20)/4)²).
- **Legend:** gray "Pre-2024 (unimodal)", blue "Post-2024 (bimodal)".
- **Mode labels (13px `#1a5276`):** "Small human PRs" near the left peak, "Large AI PRs" near the right peak.

## iPhone Launch (2007) / Smartphone Era

**Obj-title:** A Data Modality That Simply Didn't Exist Before

- **New modality:** Mobile data didn't EXIST before 2007, and mobile surpassed desktop by 2015.
- **Non-transfer:** Models trained on desktop behavior simply don't apply to mobile traffic.
- **What differs:** Session length, conversion rate, and navigation patterns all differ on mobile.
- **Unprecedented streams:** Location data went from zero to ubiquitous across the whole user base.
- **Always connected:** The "always connected" phone created data streams with no historical precedent.
- **Key lesson:** New platforms create entirely new data modalities with no historical precedent.

### Visualization (canvas `canvas6`, 720×300 declared; setupCanvas renders at 720×200)

Desktop/mobile traffic-share crossover. Axes: x "Time (2007-2020)", y "Traffic Share %". Break line "2007" at x=150.

- **Desktop (blue `#2980b9`, width 2.5):** 24-point y path `[40, 40, 42, 44, 48, 52, 58, 65, 72, 80, 90, 100, 108, 115, 120, 125, 130, 135, 138, 140, 142, 145, 148, 150]` (high share declining).
- **Mobile (orange `#e67e22`, width 2.5):** 24-point y path `[160, 160, 158, 156, 152, 148, 142, 135, 128, 118, 108, 98, 90, 82, 78, 72, 68, 65, 62, 58, 55, 52, 50, 48]` (zero rising past desktop).
- **Annotation (13px `#1a5276`)** at the crossing point (~index 11): "Cross-over (~2015)".
- **Legend:** blue "Desktop", orange "Mobile".

## War (Ukraine 2022, Middle East)

**Obj-title:** Supply Chains, Trade Routes, and Whole Countries Vanish From the Data

- **Overnight breaks:** Energy, grain, and shipping supply chains broke within days of the invasion.
- **Price shock:** Commodity prices ran 2-5× higher within weeks of the supply chains breaking.
- **Vanishing data:** Sanctions made entire countries disappear from trade data outright.
- **Revenue to zero:** Companies carrying Russian-exposed revenue watched that revenue go to zero.
- **Route disruption:** Suez/Red Sea disruption added 30% to transit times.
- **Key lesson:** Geopolitical events invalidate supply chain and commodity models instantly.

### Visualization (canvas `canvas7`, 720×300 declared; setupCanvas renders at 720×200)

Two-series spike chart. Axes: x "Time (2021-2024)", y "Price / Transit Time Index". Break line "Feb 2022" at x=250.

- **Commodity prices (red `#e74c3c`, width 2.5):** 30-point y path `[130, 128, 130, 132, 130, 128, 125, 120, 110, 95, 60, 35, 30, 28, 32, 38, 45, 55, 65, 75, 85, 95, 100, 105, 110, 115, 118, 120, 122, 125]` (sharp spike at the invasion, gradual normalization).
- **Shipping transit time (blue `#2980b9`, width 2):** 30-point y path `[140, 140, 138, 140, 142, 140, 138, 136, 130, 120, 100, 85, 80, 78, 80, 85, 90, 95, 100, 105, 110, 115, 118, 120, 125, 128, 130, 132, 135, 138]`.
- **Legend:** red "Commodity Prices", blue "Shipping Transit Time".

## 2008 Recession / Great Financial Crisis

**Obj-title:** The STRUCTURE of Relationships Changed, Not Just the Levels

- **Behavior shift:** Savings rate doubled, discretionary spending collapsed, and housing starts dropped 75%.
- **Restructured relationships:** "Defensive stocks" turned out not to be defensive at all.
- **Cross-asset shift:** Correlations between asset classes restructured completely, not partially.
- **Out of range:** Unemployment models predicted at most 8%; actual hit 10%.
- **Key lesson:** Regime breaks change the structure of relationships, not just the levels of variables.

### Visualization (canvas `canvas8`, 720×300 declared; setupCanvas renders at 720×200)

Before/during correlation-matrix heatmap pair (no axes).

- **Titles (bold 15px `#1a5276`):** "Before Crisis (2005-2006)" at x=70 and "During Crisis (2008-2009)" at x=420.
- **Matrices:** two 5×5 grids, 28px cells, assets `['Stk', 'Bnd', 'RE', 'Cmd', 'FX']` as row/column labels (12px `#333`), starting at (80,55) and (430,55).
  - Before values (mostly near zero): rows `[1.0, 0.1, 0.3, 0.2, −0.1] / [0.1, 1.0, 0.0, −0.1, 0.2] / [0.3, 0.0, 1.0, 0.1, 0.0] / [0.2, −0.1, 0.1, 1.0, 0.3] / [−0.1, 0.2, 0.0, 0.3, 1.0]`.
  - During values (all high): rows `[1.0, 0.85, 0.92, 0.88, 0.80] / [0.85, 1.0, 0.80, 0.82, 0.78] / [0.92, 0.80, 1.0, 0.90, 0.85] / [0.88, 0.82, 0.90, 1.0, 0.87] / [0.80, 0.78, 0.85, 0.87, 1.0]`.
- **Color scale:** >0.7 `#c0392b`; >0.4 `#e67e22`; >0.1 `#f1c40f`; >−0.1 `#ecf0f1`; else `#3498db`.
- **Arrow:** thick red arrow (`#e74c3c`, width 3) between the matrices, labeled 13px red "Crisis hits".

## GDPR (May 2018) / Privacy Regulations

**Obj-title:** Measurement Capability Was Destroyed — Behavior Never Changed

- **Signal loss:** 30-50% of tracking data disappeared overnight once consent became mandatory.
- **Feature loss:** User-acquisition models lost 40% of their signal features in a single step.
- **Measurement break:** "Before GDPR" data ≠ "after GDPR" data even where behavior was identical.
- **Legal cause:** The break came from MEASUREMENT being legally restricted, not from any user change.
- **Compounding:** iOS App Tracking Transparency (2021) removed another 30% of the signal.
- **Gone for good:** Features that existed pre-2018 literally don't exist in the data anymore.
- **Key lesson:** Regulation can destroy measurement capability, making pre/post data incomparable.

### Visualization (canvas `canvas9`, 720×300 declared; setupCanvas renders at 720×200)

Stair-step signal-loss chart. Axes: x "Time (2016-2024)", y "Available Tracking Features".

- **Stair-step line (blue `#2980b9`, width 2.5):** flat at y=40 from x=55 to 220 → drop to y=80 until x=350 → drop to y=105 until x=480 → drop to y=140 until x=650.
- **Drop annotations (bold 13px `#e74c3c`):** "GDPR" / "May 2018" at the first step; "CCPA" / "Jan 2020" at the second; "ATT" / "Apr 2021" at the third.
- **Level labels (13px `#1a5276`):** "100% signal", "-40% signal", "-55% signal", "-70% signal" along the steps.

## Social Media Emergence (2008-2012)

**Obj-title:** The Measurement Tool CREATED the Phenomena Being Measured

- **New observable:** "Public opinion" became measurable for the first time, at population scale.
- **Reflexive instrument:** The platforms doing the measuring were ALSO shaping the opinion they measured.
- **New phenomena:** Arab Spring, political polarization, and viral misinformation have no pre-social precedent.
- **Changed instrument:** Sentiment analysis went from slow small-n surveys to fast massive post volumes.
- **New noise:** Those post streams carry bots, astroturfing, and algorithmic amplification too.
- **Key lesson:** New measurement platforms don't just observe reality — they create new phenomena.

### Visualization (canvas `canvas10`, 720×300 declared; setupCanvas renders at 720×200)

Information-speed regime chart. Axes: x "Time (2004-2016)", y "Information Speed". Break line "2008-2012" at x=250.

- **Pre-social (dashed gray `#999`, dash 5/3, width 2):** slow flat line around y=145 with small random jitter (12 points, 16px spacing from x=55); label 13px gray "Days/weeks via news".
- **Post-social (solid blue `#2980b9`, width 2.5):** fast spiky rising series from x=280 at 15px spacing, y path `[140, 130, 115, 95, 80, 60, 45, 35, 55, 30, 50, 28, 45, 25, 38, 22, 35, 20, 30, 25, 32, 22, 28, 20]`; label 13px blue "Minutes via social".
- **Volume overlay:** `rgba(41,128,185,0.1)` rectangle over the post-social region; caption 12px `#1a5276` "+ bots, astroturfing, amplification".

## Recurring Mega-Events (Black Friday, Christmas, Super Bowl, Chinese New Year)

**Obj-title:** Annual Extreme Distortions: Not Regime Breaks, but Models Must Survive Them

- **The distortions:** Black Friday e-commerce runs 10-20× normal inside a single 24-hour window.
- **Other extremes:** Super Bowl food delivery hits 4×, and Chinese New Year halts manufacturing 2 weeks.
- **The dilemma:** Models trained on normal days fail catastrophically on the event days.
- **The other side:** Training on event days makes extreme traffic look normal for the other 360.
- **The fix:** Detect event days, model them separately, and exclude them from baselines.
- **Windowed models:** Activate the event-specific models only inside the event window itself.
- **Key lesson:** Recurring extreme events need their own models, kept out of the normal-day baseline.

### Visualization (canvas `canvas11`, 720×300 declared; setupCanvas renders at 720×200)

One-year daily traffic line with event spikes vs a flat model baseline. Axes: x "Time (Jan-Dec, one year)", y "Traffic Volume".

- **Model baseline:** dashed gray line (`#999`, dash 4/3, width 1.5) at y=130, labeled 12px gray "Model baseline (trained on avg)".
- **Actual traffic (blue `#2980b9`, width 2):** 365 daily points with ±6 random jitter around y=130, plus event windows: Super Bowl days 33-37 spike up (y ≈ 60-80); Chinese New Year days 45-60 dip below baseline (y ≈ 160-170, manufacturing stops); Black Friday days 327-331 huge spike (y ≈ 25-40); Christmas days 355-363 spike (y ≈ 45-60).
- **Event markers (bold 12px `#e74c3c` with small tick lines):** "SB"; "CNY" with 10px "(mfg stops)"; "BF" with 10px "10-20x"; "Xmas".
- **Problem annotation (12px `#1a5276`, top left):** "Models without events: miss spikes entirely" / "Models with events: think every day is a spike".

## Celebrity Death / Viral Cultural Moment (Michael Jackson, Kobe Bryant, Queen Elizabeth)

**Obj-title:** Instant 10-100× Spikes That Look Exactly Like Attacks

- **The spike:** Michael Jackson's death (2009) hit the search engine as an apparent DDoS attack.
- **Blast radius:** Traffic rose 10× in seconds and crashed the microblogging platform outright.
- **Chart takeover:** His whole back catalog sat atop every music chart simultaneously.
- **Spike signature:** INSTANT (no ramp), MASSIVE (10-100× normal), UNPREDICTABLE, decaying over days-weeks.
- **Missing context:** Telling a cultural event from an attack, bug, or sensor failure needs EXTERNAL context.
- **No such input:** The data system doesn't have that context, so auto-responding ML fires inappropriately.
- **Key lesson:** Cultural events produce instant massive spikes indistinguishable from attacks or bugs.

### Visualization (canvas `canvas12`, 720×300 declared; setupCanvas renders at 720×200)

Instant spike with exponential decay. Axes: x "Time (hours/days after event)", y "Traffic (x normal)". Break line "Event" at the spike (x=119).

- **Traffic (blue `#2980b9`, width 2.5):** flat baseline at y=155 for 8 points, then a vertical jump to y=20 (10× spike), then exponential decay y = 20 + 135·(1 − exp(−i/12)) over 50 steps of 10px back toward baseline.
- **Annotations:** bold 13px red "10x instant" and 12px "No ramp!" beside the spike; 12px `#1a5276` "Exponential decay (~2 weeks)" and "1x baseline"; 11px `#666` "Model thinks: DDoS? Bug? Sensor failure?" and "Reality: Cultural event (needs external context)" at bottom right.

## Public Protests / Mass Gatherings (Concentrated Correlated Data)

**Obj-title:** Every Local Signal Breaks at Once — and Using the Data May Be Illegal

- **Correlated everything:** Cell towers overload, geo-tagged posts spike, transit converges on one point.
- **Local commerce:** Purchases near the gathering spike during it and then crash right afterward.
- **Total anomaly:** Models trained on "normal behavior in this area" see a catastrophic anomaly.
- **Every feature at once:** The anomaly shows up in EVERY feature simultaneously, not in one or two.
- **Ethical constraint:** Tracking attendance via cell or social data raises civil-liberties concerns.
- **Legal limits:** The data EXISTS, but using it may be unethical or illegal depending on jurisdiction.
- **Key lesson:** Mass gatherings create extreme spatial-temporal correlation that breaks local models.

### Visualization (canvas `canvas13`, 720×300 declared; setupCanvas renders at 720×200)

Side-by-side 8×8 activity heatmaps (no axes).

- **Titles (bold 15px `#1a5276`):** "Normal Day (distributed activity)" at x=70; "Protest Day (concentrated)" at x=420.
- **Normal-day grid** (16px cells from (80,35)): random low-medium intensity (0.1–0.45) everywhere, rendered in cool blue-gray tones (RGB computed from intensity).
- **Protest-day grid** (from (440,35)): radial hot spot centered at cell (3.5,3.5) — intensity ~0.95-1.0 within radius 1.5 (hot red), 0.6-0.8 in the next ring, 0.25-0.4 further out, and near-zero (0.02-0.1) elsewhere ("everyone at the protest"); red-based color mapping.
- **Captions (12px):** gray under left grid "Activity evenly distributed" / "across all zip codes"; red under right grid "ALL signals from one location" / "100% correlated for 4+ hours".
- **Arrow:** red arrow pointing at the hot spot, labeled 11px red "Protest site" / "(all features spike)".
- **Ethical note (italic 11px `#8e44ad`, bottom):** "Civil liberties concern: data exists but usage may be unethical/illegal".

## Regeneration instructions

- **Layout:** detail page — h1 + `.subtitle`, then one `h2` per event (1.4em `#1a5276`, bottom border `2px solid #2980b9`), each followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (the bold headline given above) and a `<ul>` whose bullets each start with a bold label (last bullet always "Key lesson:"); right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) though unused on this page. No nav bar, no back/home links.
- **Canvas:** each `<canvas>` declares `width="720" height="300"`, but the shared `setupCanvas(id)` helper sets both backing store and CSS size to 720×200 (× `window.devicePixelRatio`, then `ctx.scale` back to logical coordinates; default font 17px system). Shared helpers: `drawAxis` (gray L-axes + x/y labels) and `drawBreakLine` (vertical dashed red regime-break line with bold label). Several charts use Math.random for small jitter, so exact noise is non-deterministic.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, dark red `#c0392b`, orange `#e67e22`, yellow `#f1c40f`, purple `#8e44ad`, light blue `#3498db`, neutral `#ecf0f1`, grays `#666`/`#999`.
