# Priors & Conjugacy

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Priors & Conjugacy

**Subtitle:** A prior is what you believed before the data arrived, written as a distribution — and with a conjugate prior, updating that belief is nothing more than adding counts

## A Guess Before the First Customer

**Tags:** `core idea` (blue), `prior belief` (green), `pseudo-counts` (orange)

- **The truck** — a food truck parks at a brand-new corner and asks: what share of passers-by will buy?
- **Past corners** — at earlier corners roughly 1 in 5 passers-by bought, so 20% is the honest guess
- **The prior** — that guess as a curve: Beta(2, 8), mean 0.20, a skewed hill peaking near 13%
- **Pseudo-counts** — Beta(2, 8) behaves like having already watched 10 people and seen 2 buy
- **Still humble** — the curve gives real weight to anything from 5% to 45%; a guess, not a verdict

*Example (italic):* Before opening, the owner would bet on 20% but wouldn't be shocked by 10% or 35% — that whole spread of plausibility is the prior.

**Key point:** A prior is your pre-data belief written as a distribution: its center is the guess, its width is your honesty about doubt.

### Visualization (canvas `c1`, 720×300)

Single-panel density plot: the Beta(2, 8) prior curve over the buy-rate axis, with the prior mean marked and a flat know-nothing prior shown dashed for contrast.

- **Title (bold 15px, `#1a5276`, top center):** "The Owner's Prior: Beta(2, 8) — 'about 20%, but wide open'".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x axis = buy rate 0 to 0.7 with 12px `#444` tick labels "0%", "10%", ..., "70%" every 0.1; y axis unlabeled density 0 to 4 (three light `#e5e9ef` gridlines at 1, 2, 3).
- **Prior curve:** blue `#2a78d6` 3px line through hardcoded points at x = `[0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]`, density = `[0, 2.51, 3.44, 3.46, 3.02, 2.40, 1.78, 1.24, 0.81, 0.49, 0.28, 0.15, 0.07, 0.03, 0.01]`; fill under the curve `rgba(42,120,214,0.15)`.
- **Mean marker:** vertical dashed blue line (dash 4/3) at x=0.20 from baseline to y=60; bold 13px blue label above: "prior mean 20%".
- **Flat prior:** horizontal dashed `#6b7280` (dash 4/3) line at density 1.0 across the plot; 12px `#6b7280` label at its right end: "know-nothing prior Beta(1, 1)".
- **Annotation (bold 12px orange `#d95926`, near x=0.42, y=120):** two lines: "worth 10 pseudo-customers:" / "2 buys, 8 walk-pasts".
- **Caption (12px `#444`, bottom right):** "illustrative — belief before any customer at this corner".

## Day One: 10 Buyers Out of 25

**Tags:** `worked example` (blue), `updating` (green)

- **Day one** — 25 people walk past, 10 stop and buy: the data alone says 10/25 = 40%
- **The update** — add buys to the first number, walk-pasts to the second: Beta(2+10, 8+15) = Beta(12, 23)
- **New belief** — the posterior Beta(12, 23) has mean 12/35 ≈ 0.34, between the 20% prior and 40% data
- **Weights** — the prior holds 10 pseudo-counts, the data brings 25 real ones, so the data pulls harder
- **No calculus** — the entire Bayesian update was two additions; that is the gift of conjugacy

*Example (italic):* Prior 20%, data 40%, posterior 34% — the answer lands closer to the data because 25 real customers outweigh 10 pseudo-counts.

**Key point:** Posterior mean = (2+10)/(2+10+8+15) = 12/35 ≈ 0.34 — a weighted compromise between the prior guess and the observed rate.

### Visualization (canvas `c2`, 720×300)

Single-panel overlay of three curves on the buy-rate axis: prior (blue), scaled likelihood of day one's data (orange, dashed), and posterior (green), with the three key rates marked.

- **Title (bold 15px, `#1a5276`, top center):** "Prior × Day-One Data = Posterior".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = buy rate 0 to 0.7, tick labels "0%"–"70%" every 0.1 (12px `#444`); y density 0 to 5.5, light gridlines at 1–5.
- **Shared x grid for all three curves:** `[0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]`.
- **Prior Beta(2, 8):** blue `#2a78d6` 2px line, values `[0, 2.51, 3.44, 3.46, 3.02, 2.40, 1.78, 1.24, 0.81, 0.49, 0.28, 0.15, 0.07, 0.03, 0.01]`; 12px blue label "prior" near its peak (x≈0.13).
- **Likelihood (10 of 25, scaled to peak 3.5):** orange `#d95926` 2px dashed (dash 6/4) line, values `[0, 0, 0, 0.04, 0.26, 0.90, 1.99, 3.06, 3.50, 3.08, 2.12, 1.13, 0.46, 0.14, 0.03]`; 12px orange label "data (scaled)" near x≈0.47.
- **Posterior Beta(12, 23):** green `#008300` 3px line, values `[0, 0, 0.01, 0.16, 1.01, 2.84, 4.63, 4.94, 3.69, 1.99, 0.78, 0.22, 0.04, 0.01, 0]`; fill under `rgba(0,131,0,0.12)`; bold 12px green label "posterior" near its peak (x≈0.33).
- **Rate markers:** three vertical dashed (dash 4/3) lines from baseline to y=60 — blue at 0.20 labeled "prior 20%", orange at 0.40 labeled "data 40%", green at 0.34 with bold 13px label "posterior 34%" (labels 12–13px, staggered heights to avoid overlap).
- **Annotation (bold 12px green, near x=0.50, y=95):** "posterior sits between prior and data — closer to the data".

## Conjugacy: Updates That Stay in the Family

**Tags:** `where it's used` (blue), `conjugate pairs` (green), `clean math` (orange)

- **The word** — a prior is conjugate to a likelihood when the posterior comes back in the same family
- **Beta–Binomial** — Beta prior + buy/no-buy data → Beta posterior; update = add successes and failures
- **Gamma–Poisson** — Gamma prior + event counts → Gamma posterior via the same add-the-counts trick
- **Normal–Normal** — Normal prior + Normal data → Normal posterior; means average, weighted by precision
- **Day by day** — each day's posterior is the next day's prior; the belief narrows toward 0.345
- **Without it** — non-conjugate pairs need numerical integration or MCMC just to see the posterior

*Example (italic):* After three days (10/25, then 14/40, then 12/35 buyers) the belief is Beta(38, 72): mean 0.345, and the ±2-sd band has shrunk from ±0.24 to ±0.09.

**Key point:** Conjugacy makes updating pure bookkeeping — the posterior stays in the prior's family, so each new day is just two additions.

### Visualization (canvas `c3`, 720×300)

Horizontal interval chart: four belief stages (prior, then after each of three days) drawn as ±2-sd bands with mean dots on a shared buy-rate axis, showing the belief narrowing while staying Beta the whole way.

- **Title (bold 15px, `#1a5276`, top center):** "Three Days of Data: the Belief Narrows but Stays a Beta".
- **Axis:** horizontal 2px `#999` line at y=260 from x=180 to x=680 (width 500), buy rate 0 to 0.6; tick labels "0%", "10%", ..., "60%" (12px `#444`) below.
- **Rows (top to bottom at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20:**
  - "prior — Beta(2, 8)": band 0.00–0.44, mean dot 0.20
  - "+ day 1 (10/25) — Beta(12, 23)": band 0.19–0.50, mean dot 0.343
  - "+ day 2 (14/40) — Beta(26, 49)": band 0.24–0.46, mean dot 0.347
  - "+ day 3 (12/35) — Beta(38, 72)": band 0.26–0.44, mean dot 0.345
- **Band style:** 10px-tall rounded bar, prior fill `rgba(42,120,214,0.30)`, the three posteriors fill `rgba(0,131,0,0.30)`; mean dot 7px, prior blue `#2a78d6`, posteriors green `#008300`; bold 12px mean label above each dot ("20%", "34%", "35%", "35%").
- **Guide line:** vertical dashed `#6b7280` (dash 4/3) line at rate 0.345 from y=60 to the axis, 11px `#6b7280` label "settling near 34.5%" at its top.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=150):** two lines: "same family every time —" / "just add the day's counts".
- **Caption (12px `#444`, bottom right):** "bands are mean ± 2 sd, illustrative".

## Same Mean, Different Stubbornness

**Tags:** `common mistake` (red), `prior strength` (orange)

- **Two priors** — Beta(2, 8) and Beta(20, 80) both say "20%", but one holds 10 pseudo-counts, one 100
- **Same data** — feed both the same day-one result: 10 buys out of 25 passers-by
- **Weak prior moves** — Beta(2, 8) jumps to mean 0.34, most of the way toward the data's 40%
- **Strong prior barely moves** — Beta(20, 80) becomes Beta(30, 95), mean 0.24; the day is outvoted
- **The mistake** — reporting only the prior mean; the pseudo-count total a+b decides the tug-of-war

*Example (italic):* Two analysts both "assumed 20%", watched the same 40% day, and published 34% vs 24% — the hidden difference was prior strength.

**Common mistake:** Choosing a prior by its mean alone. The pseudo-count total a+b sets how much data it takes to change your mind — say it out loud when you state the prior.

### Visualization (canvas `c4`, 720×300)

Two-row arrow chart on a shared buy-rate axis: each row shows a prior mean dot with an arrow to its posterior mean after the identical day of data, making the strong prior's stubbornness visible.

- **Title (bold 15px, `#1a5276`, top center):** "Same Stated Belief (20%), Same Data (10/25) — Different Answers".
- **Axis:** horizontal 2px `#999` line at y=250 from x=230 to x=680 (width 450), buy rate 0 to 0.6; tick labels "0%"–"60%" every 0.1 (12px `#444`).
- **Data marker:** vertical dashed `#6b7280` (dash 4/3) line at rate 0.40 from y=55 to the axis, bold 12px `#6b7280` label "data: 40%" at its top.
- **Row 1 (y=110), label 12px `#444` at x=20:** "weak prior — Beta(2, 8), 10 pseudo-counts"; blue `#2a78d6` 7px dot at 0.20, 3px blue arrow to a green `#008300` 7px dot at 0.343 with arrowhead; bold 12px labels "20%" above the start and green "34%" above the end.
- **Row 2 (y=180), label:** "strong prior — Beta(20, 80), 100 pseudo-counts"; blue 7px dot at 0.20, short 3px blue arrow to an orange `#d95926` 7px dot at 0.24 with arrowhead; bold 12px labels "20%" and orange "24%".
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "same mean, same data — a 10-point gap; prior strength a+b did that".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all curve points and interval endpoints are the hardcoded arrays above (no randomness); density arrays are true Beta pdf values rounded to 2 decimals, likelihood is scaled to peak 3.5 and labeled as scaled.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
