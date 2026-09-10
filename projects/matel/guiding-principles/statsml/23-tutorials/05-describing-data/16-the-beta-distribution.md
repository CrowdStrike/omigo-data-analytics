# Beta Distribution

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Beta Distribution

**Subtitle:** A Beta distribution is a curve over the conversion rate itself — ten clicks give a wide hump of maybes, a hundred clicks squeeze it into a confident spike

## Two Buttons, Same 30%, Different Stories

**Tags:** `core idea` (blue), `distribution over rates` (green), `uncertainty` (orange)

- **The buttons** — signup button A got 3 signups from 10 clicks; button B got 30 from 100 clicks
- **Same rate** — both convert at exactly 30%, yet B's evidence is ten times heavier than A's
- **The Beta** — a Beta distribution is a curve over the conversion rate itself, from 0 to 1
- **Width = doubt** — A's curve spreads wide, from about 10% to 60%; B's is a narrow spike near 30%
- **One number lies** — reporting "30%" alone hides that A's true rate could easily be 15% or 50%

*Example (italic):* With only 10 clicks, a true rate anywhere from about 12% to 55% could plausibly have produced button A's 3 signups.

**Key point:** The Beta treats the rate itself as the unknown and draws every plausible value — more data does not move the truth, it narrows the curve around it.

### Visualization (canvas `c1`, 720×300)

Single-panel overlay of two Beta density curves over the conversion rate: wide Beta(4,8) for button A vs sharp Beta(31,71) for button B, with a dashed marker at 30%.

- **Title (bold 15px, `#1a5276`, top center):** "Plausible Conversion Rates: 3/10 Clicks vs 30/100 Clicks".
- **Axes:** origin x=60, plot width 600, baseline y=245, chart height 190; x maps rate 0→1 with 12px `#444` tick labels "0%", "20%", "40%", "60%", "80%", "100%"; y scale 0–9 (density, unlabeled axis line only).
- **Curve A (blue `#2a78d6`, 3px, fill `rgba(42,120,214,0.15)`):** Beta(4,8) density at x = 0 to 1 step 0.05: `[0, 0.12, 0.63, 1.43, 2.21, 2.75, 2.94, 2.77, 2.36, 1.83, 1.29, 0.82, 0.47, 0.23, 0.10, 0.03, 0.01, 0, 0, 0, 0]`; draw as a smooth polyline.
- **Curve B (green `#008300`, 3px, fill `rgba(0,131,0,0.12)`):** Beta(31,71) density at x = 0.15 to 0.50 step 0.025: `[0.03, 0.15, 0.63, 1.93, 4.33, 7.18, 8.78, 7.91, 5.26, 2.58, 0.93, 0.25, 0.05, 0.01, 0]`; zero outside this range.
- **Marker:** dashed `#1a5276` (dash 4/3) vertical line at rate 0.30 from baseline to y=45, 12px ink label "30%" at its top.
- **Annotations:** blue bold 12px "A: 3/10 — wide = unsure" near the blue peak (around rate 0.45, y≈130); green bold 13px "B: 30/100 — narrow spike" to the right of the green peak (around rate 0.40, y≈65).
- **Caption (12px `#444`, bottom right):** "same 30% rate, very different certainty (illustrative)".

## Building the Curve One Click at a Time

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Start flat** — before any data, Beta(1, 1) is a flat line: every rate from 0 to 1 is equally plausible
- **The rule** — each signup adds 1 to the first number, each miss adds 1 to the second — that's all
- **After 4 clicks** — S, M, M, S give 2 signups and 2 misses: Beta(3, 3), a broad hump at 50%
- **After 10 clicks** — 3 signups and 7 misses give Beta(4, 8), a curve peaking near 30%
- **The mean** — the curve's average is 4/(4+8) = 33%, gently pulled toward 50% by the flat start

*Example (italic):* Button A's full click sequence S, M, M, S, M, M, M, S, M, M turns Beta(1,1) into Beta(4,8) by simple counting.

**Key point:** Beta(a, b) is just "signups + 1" and "misses + 1" — updating it as clicks arrive is arithmetic, not calculus.

### Visualization (canvas `c2`, 720×300)

Three mini-panels showing the same Beta curve sharpening as clicks accumulate: flat Beta(1,1), humped Beta(3,3), peaked Beta(4,8).

- **Title (bold 15px, `#1a5276`, top center):** "From Flat to Peaked: the Curve After 0, 4, and 10 Clicks".
- **Panels:** three plots with axis origins x=60, x=290, x=520, each width 180, baseline y=225, chart height 140; shared y scale 0–3; each x-axis maps rate 0→1 with 11px `#444` end labels "0%" and "100%".
- **Panel 1 (blue `#2a78d6`, 3px):** Beta(1,1) — a horizontal line at density 1.0 across the panel; heading bold 12px `#444` "0 clicks — Beta(1,1)"; 11px `#6b7280` caption "any rate equally plausible".
- **Panel 2 (aqua `#199e70`, 3px):** Beta(3,3) density at x = 0 to 1 step 0.1: `[0, 0.24, 0.77, 1.32, 1.73, 1.88, 1.73, 1.32, 0.77, 0.24, 0]`; heading "4 clicks (2 signups) — Beta(3,3)"; caption "a broad hump at 50%".
- **Panel 3 (green `#008300`, 3px, fill `rgba(0,131,0,0.12)`):** Beta(4,8) density at x = 0 to 1 step 0.05, same array as c1 curve A; heading "10 clicks (3 signups) — Beta(4,8)"; dashed `#1a5276` vertical at rate 0.33 with bold green 12px label "mean 33%".
- **Outcome strip (bottom center, 12px):** the click sequence "S M M S M M M S M M" with each "S" bold green `#008300` and each "M" `#6b7280`.

## Picking a Winner With Uneven Evidence

**Tags:** `where it's used` (blue), `ranking` (orange), `a/b testing` (green)

- **Three designs** — button X converted 2/2 clicks (100%), Y 45/100 (45%), Z 300/1000 (30%)
- **Raw winner** — X's perfect 100% from just 2 clicks tops any naive conversion leaderboard
- **Beta view** — X becomes Beta(3, 1): its 90% band runs from 37% all the way up to 98%
- **Steady rivals** — Y's band is 37% to 53% around mean 45%; Z's is 28% to 32% around 30%
- **The call** — Y's whole band clears Z's, so Y wins; X needs more clicks to claim anything

*Example (italic):* A 100% rate from 2 clicks loses to a 45% rate from 100 clicks the moment the uncertainty bands are drawn.

**Key point:** Comparing Beta bands instead of raw rates stops tiny samples from winning leaderboards — the standard fix for ranking reviews, sellers, and ad variants.

### Visualization (canvas `c3`, 720×300)

Horizontal interval chart: one row per button design showing its Beta 90% band, Beta mean dot, and raw rate marker on a shared 0–100% axis.

- **Title (bold 15px, `#1a5276`, top center):** "Raw Rate vs Beta 90% Band for Three Button Designs".
- **Axis:** horizontal scale from x=140 to x=670 mapping 0%→100%; 12px `#444` tick labels at 0%, 20%, 40%, 60%, 80%, 100% along y=255; light `#e5e9ef` vertical gridlines at each tick from y=60 to y=245.
- **Rows (row labels bold 12px `#2c3e50`, left-aligned at x=15):**
  - y=100 — "X: 2/2": orange `#d95926` 5px band from 37% to 98%, 7px orange dot at mean 75%, hollow 6px orange circle (2px stroke) at raw 100%.
  - y=160 — "Y: 45/100": green `#008300` 5px band from 37% to 53%, 7px green dot at mean 45%, hollow circle at raw 45%.
  - y=220 — "Z: 300/1000": blue `#2a78d6` 5px band from 28% to 32%, 7px blue dot at mean 30%, hollow circle at raw 30%.
- **Annotations:** orange bold 12px above row X: "raw 100%, but the band spans 37–98%"; green bold 12px below row Y: "Y's band clears Z's — a real winner".
- **Legend (11px `#6b7280`, top right):** filled dot = Beta mean, hollow circle = raw rate, thick line = 90% band.

## Beta Is Not Binomial

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Two questions** — binomial asks "how many signups will 10 clicks give?"; Beta asks "what is the rate?"
- **Different x-axis** — binomial lives on counts 0, 1, ..., 10; Beta lives on rates from 0 to 1
- **Fixed vs unknown** — binomial assumes the rate (say 30%) is known; Beta treats it as the unknown
- **Same data, both ways** — 3 signups in 10 clicks is one binomial outcome but a whole Beta curve
- **The trap** — reading 2/2 as "the rate is 100%" mistakes a two-click count for a known rate

*Example (italic):* With a known 30% rate, 10 clicks most often give exactly 3 signups — but seeing 3 signups does not prove the rate is 30%.

**Common mistake:** Plotting signup counts and calling it "the distribution of the rate." The binomial's x-axis is the number of successes; the Beta's x-axis is the rate itself.

### Visualization (canvas `c4`, 720×300)

Dual-panel comparison: binomial bar chart over signup counts (left) vs Beta density curve over rates (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Binomial Counts vs Beta Rates: Two Different X-Axes".
- **Left panel (binomial):** probabilities of k = 0..10 signups from 10 clicks at a fixed 30% rate: `[0.028, 0.121, 0.233, 0.267, 0.200, 0.103, 0.037, 0.009, 0.001, 0.000, 0.000]`; axis origin x=55, width 280, baseline y=240, chart height 165, y scale max 0.30; bars fill `rgba(42,120,214,0.45)` with 1px `#2a78d6` stroke; k labels 11px `#444` below each bar; bold blue 12px annotation above the k=3 bar: "counts, rate fixed at 30%"; caption 12px `#444` "Binomial(10, 0.3) — x = number of signups".
- **Right panel (Beta):** Beta(4,8) density at x = 0 to 1 step 0.05, same array as c1 curve A; axis origin x=400, width 280, same baseline/height, y scale 0–3; green `#008300` 3px curve with fill `rgba(0,131,0,0.12)`; x tick labels "0%", "50%", "100%" (12px `#444`); bold green 13px annotation near the peak: "rates, 3 signups observed"; caption "Beta(4, 8) — x = the rate itself".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- All curve data is hardcoded literal arrays (no randomness); density values above are exact Beta pdfs rounded to 2 decimals (Beta(31,71) via its normal approximation, mean 0.304, sd 0.045). In regenerated HTML, any card links would use `.html` extensions (this page has no links).
