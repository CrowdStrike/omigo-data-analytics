# Overdispersion & Negative Binomial

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Overdispersion & Negative Binomial

**Subtitle:** Real-world counts often swing far more wildly than the Poisson model allows — the negative binomial adds one knob to soak up the extra spread

## Ticket Counts That Refuse to Behave

**Tags:** `core idea` (blue), `count data` (green), `variance > mean` (orange)

- **The helpdesk** — a small IT helpdesk logs its support tickets every day, averaging 6 per day
- **Poisson's promise** — if tickets arrived independently, variance would equal the mean: about 6
- **The reality** — many near-dead days AND occasional 15–20 ticket blow-ups when a system fails
- **Overdispersion** — counts spreading wider than variance = mean is called overdispersion
- **The tell** — the observed histogram is fatter at both ends than the Poisson(6) prediction

*Example (italic):* Over 60 days the helpdesk saw 8 days with 0–1 tickets and 5 days with 15+ — Poisson(6) predicts about one 0–1 day and essentially zero 15+ days.

**Key point:** Poisson quietly assumes variance = mean. Bursty real-world counts routinely break that promise, and the histogram shows it immediately.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: observed daily-ticket frequencies over 60 days vs the Poisson(6) expected frequencies, side by side per bin.

- **Title (bold 15px, `#1a5276`, top center):** "60 Days of Tickets: Observed vs Poisson(6) Expected (illustrative)".
- **Data:** bins `["0–1", "2–3", "4–5", "6–7", "8–9", "10–11", "12–14", "15+"]`; observed days `[8, 14, 12, 8, 6, 4, 3, 5]`; Poisson-expected days `[1, 8, 18, 18, 10, 4, 1, 0]`.
- **Panel:** axis origin x=60, plot width 620, baseline y=240, chart height 180, y scale 0–20 with gridlines `#e5e9ef` and 12px `#444` tick labels every 5.
- **Bars:** each bin gets a blue observed bar fill `rgba(42,120,214,0.55)` and a violet expected bar fill `rgba(74,58,167,0.4)` side by side (each ~28px wide, 4px gap); bin labels 12px `#444` below baseline; x-axis caption 12px `#444` "tickets per day".
- **Legend (top right, 12px):** blue swatch "observed" and violet swatch "Poisson(6) expected".
- **Annotations:** magenta `#d55181` bold 12px "8 near-dead days" with arrow to the 0–1 observed bar; orange `#d95926` bold 12px "5 blow-up days, Poisson expects ~0" with arrow to the 15+ observed bar.

## Checking Dispersion by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Ten days** — one incident-heavy stretch: 4, 3, 5, 2, 20, 4, 5, 8, 4, 5 tickets per day
- **The mean** — the counts sum to 60, so the mean is 60 / 10 = 6 tickets per day
- **Squared gaps** — squared deviations from 6 are 4, 9, 1, 16, 196, 4, 1, 4, 4, 1, summing to 240
- **The variance** — 240 / 10 = 24, which is four times the mean of 6
- **Dispersion ratio** — variance / mean = 24 / 6 = 4; anything well above 1 flags overdispersion

*Example (italic):* Day 5's outage alone contributes 196 of the 240 squared units — one bad day supplies most of the extra spread.

**Key point:** Divide the variance by the mean before fitting anything. Poisson needs a ratio near 1; a ratio of 4 says the model's error bars will be badly wrong.

### Visualization (canvas `c2`, 720×300)

Bar chart of the 10 daily counts with a dashed mean line, the outage day highlighted, and the dispersion arithmetic as a caption.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Days of Tickets: Mean 6, Variance 24".
- **Data:** days 1–10 with counts `[4, 3, 5, 2, 20, 4, 5, 8, 4, 5]`.
- **Panel:** axis origin x=60, plot width 600, baseline y=235, chart height 170, y scale 0–22, gridlines `#e5e9ef` with 12px `#444` labels at 0, 6, 12, 18.
- **Bars:** ~44px wide, fill `rgba(42,120,214,0.55)`; day 5 instead fill `rgba(217,89,38,0.65)`; each bar's count bold 12px above it (day 5's "20" in orange `#d95926`); day labels "D1"–"D10" 12px `#444` below baseline.
- **Mean line:** dashed green `#008300` (dash 5/4) horizontal at count 6 across the panel, bold 12px green label "mean = 6" at its right end.
- **Annotation:** orange `#d95926` bold 13px, two lines near day 5's bar: "one outage day:" / "196 of 240 squared units".
- **Caption (bold 13px magenta `#d55181`, bottom center):** "variance 24 = 4 × mean 6 — four times what Poisson allows".

## The Negative Binomial Fix

**Tags:** `where it's used` (blue), `dispersion parameter` (orange), `rule of thumb` (green)

- **One extra knob** — the negative binomial keeps the mean but adds a dispersion parameter k
- **Its variance** — variance = mean + mean² / k, so extra spread grows with the mean squared
- **Fit the helpdesk** — mean 6 with k = 2 gives 6 + 36 / 2 = 24, exactly the observed variance
- **Smaller k, burstier** — k = 2 is very bursty; as k grows huge the model collapses back to Poisson
- **Why it works** — it lets the underlying daily rate itself vary (calm days vs outage days)

*Example (italic):* At mean 6, Poisson insists on variance 6, negative binomial with k = 6 allows 12, and k = 2 allows the 24 the helpdesk actually shows.

**Key point:** Negative binomial is Poisson with the "variance = mean" handcuff removed — one parameter k tunes how far above the mean the variance may sit.

### Visualization (canvas `c3`, 720×300)

Line chart of variance vs mean: the Poisson identity line and two negative binomial curves, with the helpdesk's observed point marked.

- **Title (bold 15px, `#1a5276`, top center):** "Variance vs Mean: Poisson Line and Negative Binomial Curves".
- **Data:** means `[0, 1, 2, 3, 4, 5, 6, 7, 8]`; Poisson variance `[0, 1, 2, 3, 4, 5, 6, 7, 8]`; NB k=6 variance `[0, 1.17, 2.67, 4.5, 6.67, 9.17, 12, 15.17, 18.67]`; NB k=2 variance `[0, 1.5, 4, 7.5, 12, 17.5, 24, 31.5, 40]`.
- **Panel:** axis origin x=65, plot width 590, baseline y=245, chart height 185; x scale 0–8 (12px `#444` labels every 2, axis caption "mean count"), y scale 0–40 (labels every 10, axis caption "variance"), gridlines `#e5e9ef`.
- **Lines:** Poisson blue `#2a78d6` 3px with 12px bold blue end label "Poisson: variance = mean"; NB k=6 aqua `#199e70` 3px, end label "NB k=6"; NB k=2 magenta `#d55181` 3px, end label "NB k=2".
- **Observed point:** 7px orange `#d95926` dot at (6, 24) with bold 13px orange callout "the helpdesk: mean 6, variance 24" and a short arrow.
- **Caption (12px `#444`, bottom right):** "NB variance = mean + mean²/k".

## The False Alarm Poisson Creates

**Tags:** `common mistake` (red), `worked example` (blue)

- **The comparison** — week A averaged 6.0 tickets/day, week B averaged 9.0, over 10 days each
- **Poisson error bars** — assuming variance = mean gives 95% CIs of 6.0 ± 1.5 and 9.0 ± 1.9
- **Poisson verdict** — the intervals miss each other and z = 2.5, so the jump looks real
- **NB error bars** — using the true variances (24 and 49.5, k = 2) widens them to ± 3.0 and ± 4.4
- **NB verdict** — the intervals overlap heavily and z = 1.1, so the jump is plausibly noise

*Example (italic):* The same 3-ticket rise is a "significant increase" under Poisson and an unremarkable wobble under the negative binomial.

**Common mistake:** Fitting Poisson to counts just because they are counts. With a dispersion ratio of 4, every standard error is understated by √4 = 2×, and false discoveries follow.

### Visualization (canvas `c4`, 720×300)

Dual-panel dot-and-error-bar chart: the week A vs week B comparison under Poisson CIs (left) and negative binomial CIs (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Data, Two Verdicts: Poisson vs Negative Binomial 95% CIs".
- **Data:** week A mean 6.0, week B mean 9.0; Poisson CIs A `[4.5, 7.5]`, B `[7.1, 10.9]`; NB CIs A `[3.0, 9.0]`, B `[4.6, 13.4]`.
- **Left panel (Poisson):** vertical value axis at x=70, scale 0–15 with 12px `#444` labels every 5 and gridlines `#e5e9ef`; week A at x=160 and week B at x=270, each a 7px dot with a 3px vertical error bar and 8px caps, blue `#2a78d6` for A and violet `#4a3aa7` for B; means labeled bold 12px beside dots ("6.0", "9.0"); panel heading bold 13px `#1a5276` "Poisson CIs"; magenta `#d55181` bold 12px annotation, two lines: "no overlap, z = 2.5" / "'significant!'".
- **Right panel (NB):** same layout shifted, week A at x=470 and week B at x=580, same colors and value scale; wider error bars from the NB CI data; panel heading "negative binomial CIs (k = 2)"; green `#008300` bold 12px annotation, two lines: "heavy overlap, z = 1.1" / "plausibly noise".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Caption (bold 13px orange `#d95926`, bottom center):** "dispersion ratio 4 → Poisson error bars are 2× too narrow".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
