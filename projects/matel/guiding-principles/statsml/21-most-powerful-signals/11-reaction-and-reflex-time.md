# Signal: Reaction & Reflex Time

**Page type:** detail page (compact card-sections: one h2 per section, two-column layout table with tag pills + labeled bullets left ~45%, canvas right ~55%)
**HTML title tag:** Reaction & Reflex Time

**Subtitle:** Millisecond response latency as a behavioral signal — it fires before deliberation, is nearly impossible to consciously mask, and is emitted on every single interaction

## The Human Reaction Floor

Tags: signal (blue), mechanism (blue), rule of thumb (blue)

- **Visual RT** — median ~200-250 ms; auditory faster, ~140-160 ms
- **Hard floor** — ~150 ms; elite gamers/athletes reach ~150-200 ms
- **Hick's law** — each choice alternative adds latency; 300-500+ ms typical
- **Log-normal shape** — hard left floor, long right tail, never below
- **Drift factors** — age (after ~25), fatigue, caffeine, time of day

*Example:* Example: World Athletics rules a sub-100 ms sprint start a false start — anticipation, not reaction.

**Key point:** Slowness has a thousand innocent causes; sub-floor speed has one — the response preceded human perception.

### Visualization (canvas `c1`, 720×300)

Filled density curve of reaction time. Title (bold 14px `#1a5276`, centered): "Simple Visual Reaction Time: Log-Normal With a Hard Floor". Padding top 40, bottom 50, left 50, right 30; x axis 0-800 ms with tick labels at 0/200/400/600/800 ms.

- **Density:** ex-Gaussian-like curve, zero below 150 ms — Gaussian component mu=210, sigma=35 plus exponential tail tau=110 (0.75 weight above mu); filled `rgba(26,82,118,0.35)` with blue `#1a5276` 2px stroke.
- **Impossible zone:** 0-150 ms shaded `rgba(231,76,60,0.10)` labeled red 10px two lines "impossible" / "zone".
- **Floor line:** dashed red (`#e74c3c`, 2px, dash 6/4) vertical at 150 ms, labeled bold red 11px "~150 ms floor" above.
- **Median marker:** dashed green (`#27ae60`, 1.5px, dash 4/3) vertical at 230 ms, labeled bold green 11px "median ~230 ms".
- **Tail annotation:** bold orange `#e67e22` 11px two lines "long right tail: lapses," / "distraction, fatigue".
- **Bottom caption (bold blue 11px, centered):** "Humans can be arbitrarily slow, never impossibly fast — the left edge is physiology".

## Impossibly Fast: Bot Detection

Tags: signal (blue), defense (green), failure mode (red)

- **Sub-100 ms click** — fired before a human could perceive; flag
- **Instant forms** — all fields filled under 1-2 s = scripted injection
- **CAPTCHA speed** — solved under 1 s, zero cursor hesitation
- **Honeypot + timing** — hidden field filled instantly is double confession

*Example:* Example: sneaker and ticket bots complete full checkout in under a second — the false-start rule applied to commerce.

**Failure mode:** Autofill, password managers, and power users are legitimately fast — impossible speed should raise a score, not issue a verdict.

### Visualization (canvas `c2`, 720×300)

Overlaid histograms of click latency after render (20 bins of 30 ms, 0-600 ms). Title (bold 14px `#1a5276`, centered): "Click Latency After Render: Bot Cluster vs. Human Distribution". Padding top 40, bottom 50, left 50, right 30; x ticks 0/150/300/450/600 ms; scale max 42.

- **Bot cluster (red, fill `rgba(231,76,60,0.55)` stroke `#e74c3c`):** `[22, 30, 16, 4, 0, 0, ...]` — all mass below ~120 ms.
- **Human distribution (fill `rgba(26,82,118,0.35)` stroke `#1a5276`):** `[0, 0, 0, 0, 0, 3, 11, 24, 36, 40, 34, 26, 18, 13, 9, 6, 4, 3, 2, 1]`.
- **Threshold:** dashed orange (2px, dash 6/4) vertical at 100 ms, labeled bold orange 11px "sub-100 ms = flag".
- **Annotations:** bold red 12px two lines "bots: respond" / "before perceiving" left; bold blue "humans: perception tax paid on every click" right.
- **Bottom caption (bold blue 11px, centered):** "Same logic on form fills (< 2 s total) and CAPTCHA solves (< 1 s, zero cursor hesitation)".

## Hesitation as Interest: Feed Ranking

Tags: signal (blue), mechanism (blue), privacy risk (red)

- **Dense signal** — emitted on every impression; likes on a few percent
- **Pre-conscious** — fires ~200-500 ms before any aware decision
- **Bimodal dwell** — instant reject under ~1 s vs hesitation bump predicting rewatch
- **Scroll micro-pauses** — where the thumb slows, the eyes stopped
- **Cold start** — tastes converge within tens of videos on dwell alone

*Example:* Example: TikTok, Shorts, and Reels fully profile a user who never likes anything from hover-before-swipe alone.

**Why it dominates:** Users curate clicks and ratings but cannot curate hesitation — density plus honesty beats explicit feedback.

### Visualization (canvas `c3`, 720×300)

Bimodal histogram of dwell before swipe (20 bins, 0-6 s; x ticks 0/1.5/3/4.5/6 s). Title (bold 14px `#1a5276`, centered): "Dwell Time Before Swipe: Bimodal Shape". Padding top 40, bottom 50, left 50, right 30; scale max 55.

- **Data:** `[38, 52, 44, 30, 18, 10, 7, 6, 8, 12, 17, 21, 19, 15, 11, 8, 6, 4, 3, 2]`; bins 8-15 (the hesitation bump) green fill `rgba(39,174,96,0.45)` stroke `#27ae60`, rest `rgba(26,82,118,0.35)` stroke `#1a5276`.
- **Annotations:** bold blue 12px "instant reject (< 1 s):" + 11px "most impressions" with blue pointer line to the first mode; bold green 12px two lines "hesitation bump =" / "interest, predicts rewatch" over the second mode.
- **Bottom caption (bold blue 11px, centered):** "Emitted on every impression — likes fire on only a few percent of views".

## Purchase Hesitation & Checkout Stalls

Tags: signal (blue), best practice (green), trade-off (orange)

- **Price-reveal pause** — long pause = sticker shock; abandonment climbs past a few seconds
- **Live intervention** — chat or discount fires on the stall, not the exit
- **Form stalls** — backspacing and pauses predict failure and card-testing fraud
- **Mobile gap** — no cursor; substitute scroll velocity and viewport dwell

*Example:* Example: a 6+ second stall after shipping cost appears triggers a free-shipping nudge in that window.

**How it's used:** Latency turns the funnel into a live instrument — intervene during the hesitation, while the decision is open.

### Visualization (canvas `c4`, 720×300)

Sigmoid curve of abandonment vs pause. Title (bold 14px `#1a5276`, centered): "Abandonment Rate vs. Pause After Price Reveal". Padding top 40, bottom 50, left 60, right 30; x 0-10 s (ticks every 2 s); rotated y label "abandonment %"; L-shaped gray axes.

- **Curve:** red `#e74c3c` 3px sigmoid — abandonment % = `12 + 60 / (1 + exp(-(t - 4.5) * 1.1))` over t in [0,10].
- **Zone shading:** 0-2 s `rgba(39,174,96,0.10)` labeled bold green 11px "pre-decided" / "buyer"; 5-10 s `rgba(231,76,60,0.08)` labeled bold red "sticker shock zone".
- **Intervention threshold:** dashed orange (2px, dash 6/4) vertical at 4 s, labeled bold orange 11px two lines "stall detected:" / "trigger chat / discount".
- **Bottom caption (bold blue 11px, centered):** "The intervention fires during the hesitation — while the decision is still open".

## Latency Reveals What You Won't Say

Tags: mechanism (blue), privacy risk (red), bias (orange)

- **IAT shift** — congruent pairings ~50-150 ms faster (typical ~80 ms shift)
- **IAT limits** — reliability ~0.4-0.5; behavior correlation r ≈ 0.15-0.25
- **Stroop cost** — conflicting word/ink color adds ~100+ ms
- **CIT probe** — recognized crime detail slows RT; lab detection >80%
- **Countermeasures backfire** — suppression itself adds detectable latency

*Example:* Example: only the person who recognizes the murder weapon shows the RT spike — unless the detail leaked to media.

**Failure mode:** Latency flags cognitive load, not guilt — group-level shifts replicate, a single person's single latency does not.

### Visualization (canvas `c5`, 720×300)

Split panel with dashed gray divider at mid-width. Title (bold 14px `#1a5276`, centered): "Latency Leaks Association: IAT Shift and CIT Probe Spike".

- **Left panel — IAT density curves** (subtitle bold blue 12px "IAT: 50-150 ms pairing shift"; x from 400 to 1400 ms): two ex-Gaussian curves (sigma 90, tail tau 220 weight 0.7) — congruent mu=650 green `#27ae60` 2.5px; incongruent mu=730 red `#e74c3c` 2.5px. Orange 2px bracket between the two peaks labeled bold orange 10px "~80 ms". Curve labels 10px "congruent" (green) and "incongruent" (red); x labels "400 ms" / "1400 ms".
- **Right panel — CIT probe bars** (subtitle bold blue 12px "CIT: probe RT spike (guilty only)"): five bars 40px wide — labels `['Irr 1', 'Irr 2', 'Probe', 'Irr 3', 'Irr 4']`, RTs `[545, 560, 720, 552, 548]` ms (scale max 800); Probe bar fill `rgba(231,76,60,0.6)` stroke red, others `rgba(26,82,118,0.35)` stroke blue; bold value labels above (red on Probe). Dashed green (1.5px, dash 5/4) horizontal line at 551 ms labeled green 10px "irrelevant baseline".
- **Bottom caption (bold blue 11px, centered):** "Group-level shifts are unmistakable over 100+ trials; a single trial is noise".

## Typing Cadence & Keystroke Dynamics

Tags: signal (blue), defense (green), failure mode (red)

- **Familiarity tell** — own data fluent; stolen data pauses at odd fields
- **Bot tell** — human inter-key CV 30-60%; near-zero variance = machine
- **Continuous auth** — mid-session rhythm drift flags takeover, no challenge issued
- **Mouse dynamics** — curvature, overshoot, corrections are individually distinctive
- **Deployed** — BioCatch/TypingDNA-class models run behind major bank logins

*Example:* Example: a fraudster stalls 400-500 ms exactly at stolen card digits — the stall localizes what isn't theirs.

**Failure mode:** Injury, fatigue, or device switch shifts the fingerprint — systems need per-device baselines and grace periods.

### Visualization (canvas `c6`, 720×300)

Three-series line chart of inter-keystroke latency across 20 keystrokes. Title (bold 14px `#1a5276`, centered): "Inter-Keystroke Latency Across a Payment Form (ms)". Padding top 40, bottom 50, left 55, right 130; y axis 0-600 ms (labels 0/300/600); L-shaped gray axes.

- **Human (blue `#1a5276` 2.5px, natural jitter):** `[120, 185, 95, 210, 140, 250, 110, 175, 90, 230, 155, 120, 200, 105, 240, 130, 180, 95, 215, 150]`.
- **Bot (red `#e74c3c` 2.5px, near-flat):** `[80, 82, 79, 81, 80, 83, 78, 80, 81, 79, 82, 80, 79, 81, 80, 82, 79, 80, 81, 80]`.
- **Fraudster (orange `#e67e22` 2px, stalls at stolen-data fields):** `[130, 150, 110, 170, 140, 520, 480, 160, 130, 550, 490, 150, 120, 170, 530, 140, 160, 130, 150, 140]`.
- **Annotations:** bold orange 11px "stalls at stolen-data fields" above the spikes; bold red "near-zero variance = machine" along the flat line.
- **Legend (x = w-122):** blue swatch "Human (jitter)"; red swatch "Bot (too flat)"; orange swatch "Fraud (stalls)".
- **Bottom caption (bold blue 11px, centered):** "Human CV of inter-key intervals: 30-60% — irreducible jitter is the fingerprint".

## Ad-Click Timing & Accidental Clicks

Tags: signal (blue), defense (green), abuse (red)

- **~1 s window** — sub-second clicks are disproportionately accidental taps
- **IVT filtering** — networks discount or refund fast clicks
- **Bounce corroboration** — landing-page dwell under ~2 s confirms accident
- **Reflow spike** — clicks clustered at layout-shift moments expose dark patterns

*Example:* Example: advertisers paid for sub-second mobile banner taps until latency filters became standard.

**How it's exploited:** Publishers shift buttons under descending fingers — the reflow-synchronized click spike exposes them.

### Visualization (canvas `c7`, 720×300)

Histogram of ad render-to-click time (20 bins of 0.5 s, 0-10 s; x ticks every 2 s). Title (bold 14px `#1a5276`, centered): "Time From Ad Render to Click: Accident Spike vs. Intent Hump". Padding top 40, bottom 70, left 50, right 30; scale max 50.

- **Data:** `[46, 22, 9, 11, 15, 19, 23, 25, 24, 21, 17, 13, 10, 8, 6, 5, 4, 3, 2, 2]`; first 2 bins (accidental, <1 s) fill `rgba(231,76,60,0.55)` stroke red, rest `rgba(26,82,118,0.35)` stroke blue.
- **Threshold:** dashed orange (2px, dash 6/4) vertical at 1 s, labeled bold orange 11px "~1 s: discount threshold".
- **Annotations:** bold red 11px "accident spike:" + 10px "fat-finger, layout shift"; bold blue 11px "deliberate hump: saw it, read it, chose it" over the second mode.
- **Bottom captions (centered):** bold red 11px "Corroboration: sub-second click + landing-page dwell < 2 s = unintended"; bold blue 11px "Ad networks discount or refund the red bars in invalid-traffic filtering".

## Decision Latency in Games: Anti-Cheat & Skill

Tags: signal (blue), defense (green), gaming (orange)

- **~150 ms floor** — even esports pros average ~150-200 ms
- **Wall vs slope** — one 120 ms flick is luck; left-truncated distribution is code
- **Session mining** — VAC/Vanguard-class systems test distributions, not single events
- **Skill inference** — low latency variance tracks rank better than peak speed
- **Cheat evolution** — injected jitter pushes detection to jerk, overshoot, correction curves

*Example:* Example: acquisition times piling at 80-120 ms with a hard cutoff flag the shape, not any single shot.

**Key point:** Humans have floors, bots have walls — a vertical left edge is a signature of code.

### Visualization (canvas `c8`, 720×300)

Overlaid histograms of target-acquisition RT (20 bins of 30 ms, 0-600 ms; x ticks 0/150/300/450/600 ms). Title (bold 14px `#1a5276`, centered): "Target-Acquisition RT: Human Floor vs. Bot Wall". Padding top 40, bottom 50, left 50, right 30; scale max 45.

- **Bot wall (fill `rgba(231,76,60,0.55)` stroke red):** `[0, 2, 14, 26, 12, 3, 0, ...]` — piled at 60-150 ms with a hard cutoff.
- **Human (fill `rgba(26,82,118,0.35)` stroke blue):** `[0, 0, 0, 0, 0, 2, 10, 24, 38, 42, 36, 27, 19, 13, 9, 6, 4, 3, 2, 1]` — right-skewed above the floor.
- **Floor:** dashed orange (2px, dash 6/4) vertical at 150 ms, labeled bold orange 11px "~150 ms physiological floor".
- **Skill markers:** dashed green (1.5px, dash 3/3) verticals at 180 ms ("pro ~180") and 250 ms ("average ~250"), green 10px labels.
- **Annotations:** bold red 12px two lines "bot: wall," / "impossible"; bold blue "human: right-skewed slope, floored".
- **Bottom caption (bold blue 11px, centered):** "One fast flick is pre-aim; a left-truncated distribution is code".

## Failure Modes: Confounds, Jitter & A/B Contamination

Tags: failure mode (red), bias (orange), rule of thumb (blue)

- **Device lag** — touchscreen/browser pipelines add 50-100+ ms variable delay
- **Physiology drift** — age, fatigue, caffeine, time of day move RT
- **Client-side timing** — network jitter contaminates server timestamps
- **Skewed stats** — use medians; within-person SD exceeds most effects
- **A/B contamination** — treatment render lag shifts every timing metric mechanically

*Example:* Example: heavier JavaScript adds 120 ms to first-click; the dashboard misreads render lag as engagement.

**Rule:** Trust latency for population-scale ranking and detection; distrust it for judging any one person on any one occasion.

### Visualization (canvas `c9`, 720×300)

Horizontal bar chart comparing effect size vs confounds. Title (bold 14px `#1a5276`, centered): "Effect of Interest vs. Everyday Confounds (ms)". Padding top 40, bottom 45, left 210, right 80; scale max 140 ms; bars 20px tall at 60% alpha with 1.5px matching strokes, right-aligned labels left of the bars and bold "~N ms" value labels after each bar.

- **Rows:** "Effect of interest (IAT shift)" 80 ms green `#27ae60`; "Trial-to-trial SD (same person)" 120 ms red `#e74c3c`; "Device / browser event lag" 100 ms orange `#e67e22`; "Network jitter (server timing)" 80 ms orange; "A/B treatment render lag" 120 ms orange; "Age (25 vs 65)" 110 ms orange.
- **Reference:** dashed green (1.5px, dash 4/3) vertical line at the 80 ms effect size.
- **Bottom caption (bold red 12px, centered):** "Every confound is as large as the signal — instrument client-side, use medians, compare distributions".

## Gaming the Signal & Countermeasures

Tags: gaming (orange), defense (green)

- **Humanized bots** — uniform delay clears the floor but leaves flat-topped plateau
- **Shape tests** — matching skew, tails, and autocorrelation together is hard; KS-style catches it
- **Goodhart on feeds** — fake countdowns manufacture dwell while satisfaction stalls
- **Re-anchoring** — blend latency with slow explicit signals and surveys
- **Escalation** — mimicked timing pushes detection to jerk, overshoot, correction curves

*Example:* Example: a bot adding uniform 150-250 ms delay passes every floor check but fails a shape test within one session.

**Defense:** Never deploy a point threshold — model the expected human distribution and alarm on walls, plateaus, and missing tails.

### Visualization (canvas `c10`, 720×300)

Two overlaid density curves over 0-800 ms (x ticks 0/200/400/600/800 ms). Title (bold 14px `#1a5276`, centered): "Humanized Bot vs. Human: Both Pass the Threshold, Only One Has the Shape". Padding top 40, bottom 65, left 50, right 30.

- **Human density (fill `rgba(26,82,118,0.35)` stroke blue 2px):** ex-Gaussian, zero below 150 ms — mu=230, sigma=45, tail tau=120 (0.7 weight).
- **Humanized-bot density (fill `rgba(231,76,60,0.25)` stroke red 2px):** flat-topped plateau from ~155 to ~255 ms (uniform random delay) with hard 8 ms ramp edges, height 1.15.
- **Threshold:** dashed orange (2px, dash 6/4) vertical at 150 ms, labeled bold orange 11px "150 ms point threshold: both pass".
- **Annotations:** bold red 11px "bot + uniform delay:" + 10px "flat plateau, hard edges, no tail"; bold blue 11px "human: skew + long tail" + 10px "+ serial autocorrelation".
- **Bottom captions (centered):** bold green 11px "Countermeasure: KS-style shape test on skew, tails, and autocorrelation — not a single cutoff"; bold blue 11px "Same lesson for feeds: hesitation bait inflates dwell, so re-anchor with slow explicit signals".

## Regeneration instructions

- **Layout:** most-powerful-signals compact style. One `.card-section` per topic: `<h2>` (unnumbered, 1.3rem `#1a5276`, 2px solid `#2980b9` bottom border), then a `table.layout` with one row — left `td.text-col` (45%) holding `.tags` pill row, a `<ul>` of labeled bullets (`<li><b>Label</b> — text`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one `<canvas width="720" height="300">` scaled to `width: 100%` with 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `<strong>` lead-in label.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `ul` 0.92rem with `li b` in `#1a5276`; `.example` italic `#555` 0.9rem. No nav bar, no back/home links.
- **Canvas:** shared `setup(id)` helper — backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, logical size 720×300.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, gray text `#444`/`#555`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
