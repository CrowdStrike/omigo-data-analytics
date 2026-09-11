# Reject vs Fail-to-Reject

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Reject vs Fail-to-Reject

**Subtitle:** A hypothesis test returns "guilty" or "not proven" — never "innocent" — so a non-significant result never means the effect is zero

## A Button Test That Came Back "Not Proven"

**Tags:** `core idea` (blue), `the verdict` (green), `running example` (orange)

- **The test** — a store shows a new checkout button to 2,000 visitors and the old one to 2,000
- **The result** — old button converts 80/2,000 = 4.0%; new one converts 92/2,000 = 4.6%
- **The lift** — the new button looks +0.6 points better, but the test says p = 0.35
- **The verdict** — p = 0.35 means "fail to reject": the data did not prove the buttons differ
- **Not innocent** — like a courtroom, "not proven guilty" is a weaker claim than "innocent"
- **The trap** — the analyst reports "the new button has no effect", which the test never said

*Example (italic):* The same evidence that fails to convict a suspect also fails to clear them — the trial simply didn't settle it.

**Key point:** A test can reject the null ("the difference is real") or fail to reject it ("we couldn't tell"). It has no third verdict that says "the difference is zero".

### Visualization (canvas `c1`, 720×300)

Two-panel chart: conversion-rate bars for the two buttons (left) and a courtroom-style verdict ladder (right), split by a vertical dashed divider at x=390.

- **Title (bold 15px, `#1a5276`, top center):** "New Checkout Button: +0.6 Points, p = 0.35".
- **Left panel (bars):** two bars, "old button" 4.0% and "new button" 4.6%; axis origin x=70, panel width 280, baseline y=240, chart height 175, y scale 0–6%; old bar fill `rgba(42,120,214,0.45)` with 2px `#2a78d6` border, new bar fill `rgba(0,131,0,0.4)` with 2px `#008300` border; bold 13px rate labels "4.0%" and "4.6%" above each bar in the bar's border color; 12px `#444` labels "80 / 2,000" and "92 / 2,000" below the bar names; y ticks at 0, 2, 4, 6 with 12px `#444` labels and `#e5e9ef` gridlines.
- **Right panel (verdict ladder):** heading bold 13px `#1a5276` at x=420, y=70: "what the test can say:"; three rounded text rows at x=420, width 270, 44px tall, starting y=90 with 14px spacing: row 1 "REJECT — 'the buttons differ'" fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, bold 12px `#2a78d6` text; row 2 "FAIL TO REJECT — 'couldn't tell'" fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, bold 12px `#d95926` text, plus a bold 12px `#d95926` arrow annotation "← p = 0.35 lands here" to its right edge; row 3 "'NO EFFECT'" fill `rgba(231,76,60,0.08)`, 2px dashed `#e74c3c` border, bold 12px `#e74c3c` text with a 12px `#e74c3c` note below inside the row: "not a verdict the test can return".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=390 from y=38 to h-12.
- **Caption (12px `#444`, bottom center):** "illustrative A/B test: two-proportion z-test, two-sided".

## The Interval Behind p = 0.35

**Tags:** `worked example` (blue), `confidence interval` (green)

- **The math** — the +0.6-point lift has a standard error of 0.64 points at 2,000 visitors per arm
- **The interval** — the 95% CI is 0.6 ± 1.26, so the true lift sits anywhere in −0.7 to +1.9 points
- **Zero inside** — 0 is inside the interval, which is exactly why the test failed to reject
- **A win inside too** — +1.5 points, a big commercial win, is also inside the same interval
- **Break-even inside** — even the +0.3-point lift that pays for the redesign is inside it
- **Honest reading** — the data is compatible with "no effect" and with "big win" at the same time

*Example (italic):* Saying "no effect" here is like reading the interval (−0.7, +1.9) and quoting only the single value 0 from it.

**Key point:** "Not significant" means the confidence interval still contains zero — along with plenty of non-zero effects the data cannot rule out.

### Visualization (canvas `c2`, 720×300)

Horizontal number line of the lift in percentage points with the 95% CI drawn as a band, and three flags inside the band marking effects the data cannot tell apart.

- **Title (bold 15px, `#1a5276`, top center):** "95% CI for the Lift: −0.7 to +1.9 Points — Zero Is Just One Resident".
- **Axis:** horizontal 2px `#999` line at y=170 from x=70, width 580, spanning −1.5 to +2.5 points; ticks and 12px `#444` labels at −1.5, −1.0, −0.5, 0, +0.5, +1.0, +1.5, +2.0, +2.5.
- **CI band:** rectangle from −0.7 to +1.9 (x mapped linearly), y=140 to y=200, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 13px `#2a78d6` label centered above the band at y=128: "95% CI: −0.7 to +1.9".
- **Point estimate:** 7px `#2a78d6` dot on the axis at +0.6 with bold 12px `#2a78d6` label "observed +0.6" below the axis at y=192.
- **Flags inside the band (vertical 2px lines from y=145 to y=195, labels bold 12px above at staggered heights y=105/88/105):** at 0, color `#d95926`, label "no effect (0)"; at +0.3, color `#c98500`, label "pays for itself (+0.3)"; at +1.5, color `#008300`, label "big win (+1.5)".
- **Takeaway (bold 13px `#d55181`, centered at y=250):** "all three worlds fit the data — the test cannot pick one".
- **Caption (12px `#444`, bottom center):** "lift = new − old, in percentage points; SE = 0.64, CI = 0.6 ± 1.96 × 0.64".

## Same Lift, More Visitors

**Tags:** `worked example` (blue), `sample size` (green), `statistical power` (orange)

- **Rerun small** — at 500 visitors per arm the same +0.6 lift gives p = 0.64, CI −1.9 to +3.1
- **Rerun medium** — at 2,000 per arm it gives p = 0.35, CI −0.7 to +1.9: still "not proven"
- **Rerun large** — at 20,000 per arm it gives p = 0.003, CI +0.2 to +1.0: now it rejects
- **Nothing changed** — the effect was +0.6 points every time; only the evidence got stronger
- **Power check** — a test with 2,000 per arm has only a 15% chance of detecting a true +0.6 lift
- **So "not significant"** — often just means the study was too small to see the effect it chased

*Example (italic):* The button was identical in all three runs — the verdict flipped from p = 0.64 to p = 0.003 purely because the sample grew.

**Key point:** Failing to reject often reflects the sample size, not the world. An underpowered test fails to reject almost everything, including real effects.

### Visualization (canvas `c3`, 720×300)

Two-panel chart: three stacked confidence intervals by sample size (left) and a power curve for detecting a true +0.6-point lift (right), split by a vertical dashed divider at x=390.

- **Title (bold 15px, `#1a5276`, top center):** "The Same +0.6 Lift at Three Sample Sizes".
- **Left panel (stacked CIs):** vertical zero line dashed `#d95926` (dash 4/3) at lift 0 from y=55 to y=235 with bold 12px `#d95926` label "0" at top; x axis at y=245 from x=60 width 300 spanning −2.5 to +3.5 points, ticks/12px `#444` labels at −2, −1, 0, +1, +2, +3; three horizontal CI bars (4px line, 6px endpoint caps, 6px center dot at +0.6): n=500/arm at y=95, from −1.9 to +3.1, color `#2a78d6`, 12px `#444` left label "n = 500  (p = 0.64)"; n=2,000/arm at y=150, from −0.7 to +1.9, color `#c98500`, label "n = 2,000  (p = 0.35)"; n=20,000/arm at y=205, from +0.2 to +1.0, color `#008300`, label "n = 20,000  (p = 0.003)"; bold 12px `#008300` annotation near the bottom bar: "clears zero → reject".
- **Right panel (power curve):** axis origin x=430, width 240, baseline y=245, chart height 180; x = visitors per arm, points at `[500, 2000, 5000, 10000, 20000, 30000]` spaced evenly with 11px `#444` labels "0.5k", "2k", "5k", "10k", "20k", "30k"; y = power to detect a true +0.6-point lift, values `[7, 15, 32, 55, 84, 95]` percent, y scale 0–100 with ticks at 0, 25, 50, 75, 100 and `#e5e9ef` gridlines; violet `#4a3aa7` 3px line with 4px dots; dashed `#199e70` horizontal guide at 80% with 11px `#199e70` label "80% power"; bold 12px `#4a3aa7` annotation at the second point: "15% at n = 2,000"; heading bold 13px `#1a5276` above panel: "chance of detecting +0.6".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=390 from y=38 to h-12.
- **Caption (12px `#444`, bottom center):** "illustrative: two-proportion z-test, baseline 4.0%, two-sided α = 0.05".

## Saying It Wrong vs Saying It Right

**Tags:** `common mistake` (red), `reporting` (orange)

- **Wrong** — "p = 0.35, so the new button has no effect" turns silence into a positive claim
- **Also wrong** — "we accept the null hypothesis" — tests never accept, they only fail to reject
- **Right** — "we found no evidence of a difference; effects between −0.7 and +1.9 remain plausible"
- **The asymmetry** — absence of evidence is not evidence of absence when power is only 15%
- **Proving "no effect"** — needs its own tool: an equivalence test against a stated margin
- **Equivalence idea** — declare "no meaningful effect" only if the whole CI fits inside ±0.5 points

*Example (italic):* A team killed a genuinely better button because "p = 0.35 means it doesn't work" — a claim their 15%-power test could never support.

**Common mistake:** Reading "fail to reject" as "the null is true". To claim no meaningful effect you must show the whole confidence interval fits inside a pre-declared "too small to matter" band — not just that it contains zero.

### Visualization (canvas `c4`, 720×300)

Number line with a shaded equivalence band of ±0.5 points and two confidence intervals compared against it: the actual wide CI (fails equivalence) and the narrow CI that a real "no effect" claim would need.

- **Title (bold 15px, `#1a5276`, top center):** "To Say 'No Effect', the Whole CI Must Fit the Grey Band".
- **Axis:** horizontal 2px `#999` line at y=225 from x=70, width 580, spanning −2.0 to +2.5 points; ticks and 12px `#444` labels at −2, −1.5, −1, −0.5, 0, +0.5, +1, +1.5, +2, +2.5.
- **Equivalence band:** rectangle from −0.5 to +0.5, y=70 to y=225, fill `rgba(107,114,128,0.15)`, 1px `#6b7280` border; bold 12px `#6b7280` label centered above at y=60: "'too small to matter': ±0.5 points".
- **CI 1 (actual test):** horizontal bar (4px line, 6px caps, 6px dot at +0.6) at y=115 from −0.7 to +1.9, color `#e74c3c`; bold 12px `#e74c3c` label above-left: "our CI (−0.7 to +1.9): sticks out both sides → cannot claim no effect".
- **CI 2 (what it would take):** horizontal bar, same styling, at y=175 from −0.3 to +0.4 centered near +0.05, color `#008300`; bold 12px `#008300` label above-left: "a CI like (−0.3, +0.4): fits inside → equivalence shown".
- **Takeaway (bold 13px `#d55181`, centered at y=272):** "'not significant' = undecided; 'no effect' = a separate claim needing a separate test".
- **Caption (12px `#444`, bottom right):** "illustrative equivalence (TOST-style) margin".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All data is hardcoded literal arrays (CI endpoints, p-values, power percentages) — no randomness.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
