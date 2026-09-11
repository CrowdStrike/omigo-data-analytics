# Odds Ratios & the Logit

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Odds Ratios & the Logit

**Subtitle:** Odds count wins against losses, not wins against everyone — so "3× the odds" of joining is never "3× the probability", and the gap grows the more likely the event already is

## Joins vs Walk-Aways at the Gym Desk

**Tags:** `core idea` (blue), `odds vs probability` (green), `logit` (orange)

- **The gym** — 100 people take a free trial this month; 20 join and 80 walk away
- **Probability** — joins out of everyone: 20/100 = 20%, a share of the whole group
- **Odds** — joins against walk-aways: 20:80 = 0.25, "one join for every four who leave"
- **Same count** — 20 joiners give a 20% probability but odds of only 0.25; the ruler differs
- **The logit** — the log of the odds: ln(0.25) = −1.39; it turns 0–100% into an open scale

*Example (italic):* The desk clerk tallies "1 join : 4 walk-aways" — that ratio is the odds; the 20% on the monthly report is the probability.

**Key point:** Probability divides wins by everyone; odds divide wins by losses. They agree only when the event is rare — and the logit is just the odds on a log scale.

### Visualization (canvas `c1`, 720×300)

Dual-panel picture of the same 20-of-100 trial month: probability as a share-of-whole stacked bar (left) vs odds as two facing bars (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "20 Joins out of 100 Trials: Probability vs Odds".
- **Data:** joins 20, walk-aways 80; probability 0.20; odds 20/80 = 0.25; logit ln(0.25) = −1.39.
- **Left panel (probability):** heading bold 12px `#444` "probability = joins ÷ everyone"; one horizontal stacked bar at y=110, x=50, width 280, height 34: green `rgba(0,131,0,0.55)` segment 56px (20%) then blue `rgba(42,120,214,0.35)` segment 224px (80%); segment labels bold 12px "20 join" (green) and "80 walk away" (blue) above; green bold 13px annotation below: "20 / 100 = 20%".
- **Right panel (odds):** heading "odds = joins ÷ walk-aways"; two vertical bars from baseline y=230, x=430 and x=530, width 56: green bar height 40px (count 20) labeled "20 join", blue bar height 160px (count 80) labeled "80 walk away", count labels bold 12px above each bar; orange `#d95926` bold 13px annotation "odds = 20:80 = 0.25"; caption 12px `#444` "logit = ln(0.25) = −1.39".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Tripling the Odds, Step by Step

**Tags:** `worked example` (blue), `odds ratio` (orange)

- **The claim** — a new coaching program "triples the odds of joining": multiply odds by 3
- **From 20%** — odds 0.25 → 0.75; back to probability: 0.75/1.75 = 42.9%, not 60%
- **From 60%** — odds 1.5 → 4.5; probability 4.5/5.5 = 81.8%; naive 3×60% = 180% is impossible
- **From 90%** — odds 9 → 27; probability 27/28 = 96.4%, a gain of just 6.4 points
- **The recipe** — p → odds p/(1−p), multiply by the odds ratio, then odds/(1+odds) → new p

*Example (italic):* At the gym's 20% baseline, tripled odds lift joining to 42.9% — the "3×" headline suggests 60%, off by 17 points.

**Key point:** An odds ratio multiplies the odds, never the probability. Convert to odds, multiply, convert back — the probability gain shrinks as the baseline climbs.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: three baselines (20%, 60%, 90%), each with a "before" bar, an "after tripled odds" bar, and a hatched "naive 3× probability" marker — the 60% naive bar bursts past the 100% line.

- **Title (bold 15px, `#1a5276`, top center):** "Odds ×3 at Three Baselines: Real Lift vs Naive 3×".
- **Data:** baselines `[20, 60, 90]` %; after tripled odds `[42.9, 81.8, 96.4]` %; naive 3× `[60, 180, 270]` % (clip at chart top); point gains `[+22.9, +21.8, +6.4]`.
- **Layout:** axis origin x=60, width 620, baseline y=245, chart height 190; y scale 0–110% with a solid 2px magenta `#d55181` reference line at 100% labeled bold 12px "100% ceiling" at the right end; three groups centered at x=170, 380, 590.
- **Bars (per group, 42px wide, 8px apart):** before fill `rgba(42,120,214,0.5)` blue; after fill `rgba(0,131,0,0.55)` green; naive fill none, 2px dashed `#d95926` orange outline, clipped at the 110% top with a jagged cut and bold 12px orange label "60%", "180% (impossible)", "270% (impossible)".
- **Labels:** value bold 12px above each before/after bar ("20%", "42.9%", etc.); gain bold 12px green above each after bar ("+22.9 pts", "+21.8 pts", "+6.4 pts"); group captions 12px `#444` below baseline "start 20%", "start 60%", "start 90%".
- **Takeaway (bold 13px `#d55181`, bottom center):** "same ×3 odds, three different probability jumps — and 3× probability breaks the ceiling".

## Why Regression Lives on the Logit Scale

**Tags:** `where it's used` (blue), `logistic regression` (green)

- **The problem** — a straight line for probability would happily predict 180% or −40%
- **The fix** — logistic regression fits a straight line to the logit, which runs −∞ to +∞
- **Coefficients** — each coefficient adds to the logit; e^coefficient is an odds ratio
- **Tripling** — "odds ×3" is always the same step on the logit: +ln(3) = +1.10, anywhere
- **Reading output** — a coaching coefficient of 1.10 means tripled odds, not +110% probability

*Example (italic):* The gym's model gives coaching a coefficient of 1.10 on the logit — exactly the ×3 odds move, whatever the member's baseline.

**Key point:** The logit stretches 0–100% onto an endless line where effects add. Equal steps on the logit are equal odds multipliers — that is why model coefficients are odds ratios, not probability changes.

### Visualization (canvas `c3`, 720×300)

Logistic S-curve mapping logit (x) to probability (y), with three identical +1.10 horizontal steps drawn at low, middle, and high baselines producing very different vertical jumps.

- **Title (bold 15px, `#1a5276`, top center):** "One Curve: Equal Logit Steps, Unequal Probability Jumps".
- **Curve data (hardcoded):** logit x `[-4, -3.5, -3, -2.5, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4]`, probability y `[0.018, 0.029, 0.047, 0.076, 0.119, 0.182, 0.269, 0.378, 0.500, 0.622, 0.731, 0.818, 0.881, 0.924, 0.953, 0.971, 0.982]`.
- **Layout:** axis origin x=70, width 580, baseline y=245, chart height 190; x maps logit −4 to +4; y maps 0 to 1; x ticks at −4, −2, 0, +2, +4 (12px `#444`), y ticks at 0%, 50%, 100%; light gridline `#e5e9ef` at 50%.
- **Curve:** blue `#2a78d6` 3px smooth polyline through the 17 points.
- **Step arrows (each a horizontal +1.10 segment then a vertical riser, 2.5px):** green `#008300` from (−1.39, 0.20) to (−0.29, 0.429) labeled bold 12px "20% → 42.9%"; orange `#d95926` from (0.41, 0.60) to (1.50, 0.818) labeled "60% → 81.8%"; violet `#4a3aa7` from (2.20, 0.90) to (3.30, 0.964) labeled "90% → 96.4%"; 5px dots at all six endpoints.
- **Annotation (bold 13px `#1a5276`, upper left):** "every arrow is the same width: +ln(3) = +1.10 on the logit".
- **Caption (12px `#444`, bottom right):** "p = 1 / (1 + e^−logit)".

## "3× More Likely" in the Headline

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The headline** — "coaching makes members 3× more likely to join" usually reports an odds ratio
- **Rare events** — at a 1% baseline, odds ×3 gives 2.9%; there the OR and "3× likely" agree
- **Common events** — at a 50% baseline, odds ×3 gives 75%, only 1.5× as likely, half the claim
- **Rule of thumb** — below ~10% baseline, read OR ≈ risk ratio; above it, always convert
- **Why it spreads** — case-control studies allow only odds ratios; logistic coefficients give them

*Example (italic):* The same OR = 3 means "2.9× as likely" for a 1%-rare event but just "1.07× as likely" at a 90% baseline.

**Common mistake:** Reading an odds ratio as "times more likely". That works only for rare outcomes — for common ones the true probability multiplier is far smaller than the OR.

### Visualization (canvas `c4`, 720×300)

Line chart of the real probability multiplier (risk ratio) implied by a fixed OR = 3 as the baseline probability rises, with a dashed line at 3 showing what the headline implies.

- **Title (bold 15px, `#1a5276`, top center):** "What OR = 3 Really Multiplies Probability By".
- **Data (hardcoded):** baseline p % `[1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90]`; implied risk ratio `[2.94, 2.73, 2.50, 2.14, 1.88, 1.67, 1.50, 1.36, 1.25, 1.15, 1.07]`.
- **Layout:** axis origin x=70, width 580, baseline y=245, chart height 190; x maps baseline 0–90% with ticks at 0, 20, 40, 60, 80, 90 (12px `#444`, axis label "baseline probability"); y maps ratio 1.0–3.2 with ticks at 1.0, 1.5, 2.0, 2.5, 3.0.
- **Headline line:** dashed `#d95926` orange (dash 5/4) 2px horizontal at ratio 3.0, bold 12px orange label ""3× more likely" headline" above its left end.
- **Curve:** green `#008300` 3px polyline through the 11 points with 4px dots; bold 12px green labels at p=1 ("2.94×") and p=50 ("1.50×").
- **Shaded zone:** left band x from baseline 0 to 10% filled `rgba(0,131,0,0.08)` with 11px green caption "rare zone: OR ≈ RR" rotated or stacked at top of the band.
- **Annotation (bold 13px magenta `#d55181`, right side near p=90):** "at 90% baseline, OR 3 is only 1.07× as likely".
- **Caption (12px `#444`, bottom center):** "fixed odds ratio 3, converted back to probabilities at each baseline".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
