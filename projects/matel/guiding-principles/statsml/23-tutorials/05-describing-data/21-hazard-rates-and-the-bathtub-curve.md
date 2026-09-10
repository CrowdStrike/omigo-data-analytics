# Hazard Rates & the Bathtub Curve

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Hazard Rates & the Bathtub Curve

**Subtitle:** The hazard rate asks "of the units still alive right now, what share fails next?" — plot it over a lifetime and many products trace a bathtub: high early, flat in the middle, rising at the end

## One Thousand Hard Drives, Logged Every Quarter

**Tags:** `core idea` (blue), `hazard rate` (green), `at-risk pool` (orange)

- **The fleet** — a data center installs 1,000 identical drives and logs failures every quarter
- **Raw counts mislead** — 50 drives fail in Q1 but only 7 in Q6; is Q6 really seven times safer?
- **Shrinking pool** — by Q6 only 900 drives are still running, so fewer drives are even at risk
- **The fix** — divide failures by drives alive at the quarter's start: that share is the hazard rate
- **Read it as** — "given a drive has survived this far, what is its chance of failing right now?"

*Example (italic):* Q1 hazard is 50 / 1,000 = 5.0%; Q6 hazard is 7 / 900 = 0.8% — per surviving drive, Q6 really is much safer.

**Key point:** The hazard is a conditional rate: risk now, given survival so far. Always divide by the pool still alive at that moment, never by the original 1,000.

### Visualization (canvas `c1`, 720×300)

Dual-panel chart: survivors over time as a line (left) and failures per quarter as bars (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "1,000 Drives: Survivors and Failures, Quarter by Quarter (illustrative)".
- **Data:** quarters Q1–Q12; survivors at end of each quarter `[950, 926, 914, 907, 900, 893, 886, 877, 864, 842, 802, 753]`; failures per quarter `[50, 24, 12, 7, 7, 7, 7, 9, 13, 22, 40, 49]`.
- **Left panel (survivors):** axis origin x=55, width 280, baseline y=245, chart height 185, y scale 700–1,000 with ticks 700/800/900/1,000 (12px `#444`); blue `#2a78d6` 3px line with 4px dots; quarter labels "Q1", "Q4", "Q8", "Q12" only, 12px `#444` below baseline; magenta `#d55181` bold 12px annotation near the end of the line "1 in 4 gone by Q12"; caption 12px `#444` "drives still running".
- **Right panel (failures):** axis origin x=400, width 280, same baseline/height, y scale 0–55; bars fill `rgba(42,120,214,0.45)`; bold 12px blue value labels "50" above the Q1 bar and "49" above the Q12 bar; same four quarter labels below; orange `#d95926` bold 12px annotation, two lines: "big at both ends —" / "same badness?"; caption "failures per quarter (count)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Computing the Hazard by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Setup** — at each quarter's start, count drives still alive; that is the "at risk" pool
- **Q1** — 50 failures / 1,000 at risk = 5.0% hazard; 950 drives survive into Q2
- **Q6** — 7 failures / 900 at risk = 0.8% hazard; the quiet middle of the drives' life
- **Q12** — 49 failures / 802 at risk = 6.1% hazard; old drives are the riskiest of all
- **Chain it** — 247 total failures over 12 quarters leave 753 of the original 1,000 running

*Example (italic):* Three quarters, one recipe: 50/1,000 = 5.0%, 7/900 = 0.8%, 49/802 = 6.1% — only the denominator's shrinkage needs care.

**Key point:** One division per period: failures this period ÷ units alive at its start. Update the pool after every period and repeat.

### Visualization (canvas `c2`, 720×300)

Three mini-panels, one per quarter (Q1, Q6, Q12), each showing the at-risk pool as a bar with the failed slice colored, plus the fraction and resulting hazard.

- **Title (bold 15px, `#1a5276`, top center):** "Hazard by Hand: Failures ÷ Drives Still Alive".
- **Data:** Q1: 50 fail / 1,000 at risk = 5.0%; Q6: 7 fail / 900 at risk = 0.8%; Q12: 49 fail / 802 at risk = 6.1%.
- **Panels:** left edges at x=30, x=265, x=500, each 190px wide; panel colors Q1 magenta `#d55181`, Q6 green `#008300`, Q12 orange `#d95926`.
- **Per panel:** heading bold 14px in panel color at y=70 ("Q1 — burn-in", "Q6 — mid-life", "Q12 — old age"); label 12px `#444` "at risk: 1,000" (then 900, 802) at y=100; pool bar at y=110, height 20, width = at-risk/1,000 × 190px, fill `#e5e9ef` with 1px `#999` border; failed slice at the bar's left end in the panel color, width = failures/1,000 × 190px (minimum 3px), 12px label "50 fail" (then "7 fail", "49 fail") just below the bar; fraction line bold 15px in panel color at y=185: "50 ÷ 1,000 = 5.0%" (then "7 ÷ 900 = 0.8%", "49 ÷ 802 = 6.1%").
- **Takeaway (bold 13px `#1a5276`, bottom center at y=280):** "same recipe every quarter: failures ÷ still-alive pool".

## The Bathtub Appears

**Tags:** `where it's used` (blue), `bathtub curve` (orange), `three phases` (green)

- **Plot it** — hazard per quarter: 5.0, 2.5, 1.3, then a flat floor near 0.8, then a climb to 6.1
- **Infant mortality** — Q1–Q3: factory defects surface early; hazard falls from 5.0% to 1.3%
- **Useful life** — Q4–Q8: hazard sits at 0.8–1.0%; failures are essentially random accidents
- **Wear-out** — Q9–Q12: mechanical aging bites; hazard climbs 1.5% → 2.5% → 4.8% → 6.1%
- **Where it's used** — burn-in tests, warranty lengths, customer churn, and patient survival

*Example (italic):* A vendor "burns in" drives for one quarter before shipping and sets the warranty to end at Q8 — both choices read straight off this curve.

**Key point:** Hazard high-low-high over a lifetime is the bathtub curve. Where each phase starts and ends is a business decision worth real money.

### Visualization (canvas `c3`, 720×300)

Single line chart of the hazard rate across all 12 quarters with three shaded phase bands forming the bathtub shape.

- **Title (bold 15px, `#1a5276`, top center):** "The Bathtub Curve: Hazard per Quarter over Three Years (illustrative)".
- **Data:** hazard % `[5.0, 2.5, 1.3, 0.8, 0.8, 0.8, 0.8, 1.0, 1.5, 2.5, 4.8, 6.1]` for Q1–Q12.
- **Axes:** origin x=60, width 600, baseline y=240, chart height 180, y scale 0–7%; y ticks at 0, 2, 4, 6 labeled "0%", "2%", "4%", "6%" (12px `#444`); all quarter labels "Q1"–"Q12" 11px `#444` below baseline.
- **Phase bands (drawn first, full chart height):** Q1–Q3 fill `rgba(213,81,129,0.10)` with bold 12px `#d55181` label "infant mortality" at top of band; Q4–Q8 fill `rgba(0,131,0,0.08)`, bold 12px `#008300` label "useful life"; Q9–Q12 fill `rgba(217,89,38,0.10)`, bold 12px `#d95926` label "wear-out".
- **Line:** ink `#1a5276` 3px with 4px dots at each quarter.
- **Annotation:** blue `#2a78d6` bold 12px "flat floor ≈ 0.8% per quarter" above the Q4–Q7 stretch.
- **Caption (12px `#444`, bottom right):** "fleet of 1,000 drives; hazard = failures ÷ drives alive".

## Counts Lie, Rates Don't

**Tags:** `common mistake` (red), `denominator` (orange)

- **The trap** — Q1 and Q12 both show about 50 failures, so people call them "equally bad"
- **The pool** — Q1 had 1,000 drives at risk; Q12 had only 802 — the denominators differ
- **Per drive** — Q1 hazard 5.0% vs Q12 hazard 6.1%: an old drive is the riskier one
- **General rule** — late-life failure counts look small only because few units survive to fail
- **Same trap elsewhere** — "most churn happens in month 1" may just mean most customers are new

*Example (italic):* An ops team deprioritized old-drive replacement because "failure counts stopped rising" — while each surviving drive's risk kept climbing.

**Common mistake:** Comparing raw failure counts across ages. Counts mix risk with pool size; only the hazard compares like with like.

### Visualization (canvas `c4`, 720×300)

Dual-panel bar comparison of Q1 vs Q12: nearly identical failure counts on the left, clearly different hazards on the right, split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Failure Count, Different Risk: Q1 vs Q12".
- **Data:** counts Q1 = 50, Q12 = 49; hazards Q1 = 5.0%, Q12 = 6.1%; at-risk pools Q1 = 1,000, Q12 = 802.
- **Left panel (counts):** axis origin x=60, width 270, baseline y=235, chart height 165, y scale 0–55; two bars (Q1, Q12) 70px wide, fill `rgba(42,120,214,0.45)`; bold 12px blue `#2a78d6` value labels "50" and "49" above the bars; labels "Q1" and "Q12" 12px `#444` below; blue bold 12px annotation between the bars "almost identical"; caption 12px `#444` "failures (count)".
- **Right panel (hazard):** axis origin x=405, width 270, same baseline/height, y scale 0–7%; two bars, Q1 fill `rgba(213,81,129,0.50)` magenta, Q12 fill `rgba(217,89,38,0.55)` orange; bold 12px value labels "5.0%" and "6.1%" above the bars in matching colors; same quarter labels below; orange `#d95926` bold 12px annotation, two lines: "only 802 drives left —" / "each one riskier"; caption "hazard (% of live drives)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.
- **Takeaway (bold 13px magenta `#d55181`, centered at y=290):** "counts compare pools of different sizes; the hazard compares like with like".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
