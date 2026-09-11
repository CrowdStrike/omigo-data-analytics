# The t-Test

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with h2 + two-column `table.layout` — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** The t-Test

**Subtitle:** The workhorse for comparing two averages — are these two groups really different, or is the gap just noise?

## Two Coffee Blends, Eight Tasters Each

**Tags:** `core idea` (blue), `running example` (green), `two groups` (orange)

- **The setup** — 8 tasters rate Blend A, 8 different tasters rate Blend B, on a 0-10 scale
- **The averages** — Blend A scores 7.5 on average, Blend B scores 7.0
- **The doubt** — a 0.5-point gap could be a real quality gap, or just lucky tasters
- **The t-test** — asks: is the gap big compared to the wobble inside each group?
- **The verdict here** — ratings barely wobble (±0.3), so 0.5 stands out clearly

*Example:* Blend A: 7.9, 7.3, 7.7, 7.1, 7.5, 7.4, 7.8, 7.3 — Blend B: 7.1, 6.7, 7.3, 6.6, 7.0, 6.9, 7.2, 7.2.

**Key point:** the t-test compares the gap BETWEEN groups to the noise WITHIN groups — a gap only counts if it beats the noise.

### Visualization (canvas `c1`, 720×300)

Dot plot of the two blends' ratings with mean lines and a gap bracket.

- **Title (bold 15px, `#1a5276`, top center):** "16 Taster Ratings: the Gap vs the Wobble".
- **Data:** Blend A = `[7.9, 7.3, 7.7, 7.1, 7.5, 7.4, 7.8, 7.3]` (mean 7.5); Blend B = `[7.1, 6.7, 7.3, 6.6, 7.0, 6.9, 7.2, 7.2]` (mean 7.0).
- **Axes:** L-shaped gray `#999` axes, padding top 56, bottom 52, left 60, right 30; y range 6.4–8.1, gridlines and right-aligned labels at 6.5, 7.0, 7.5, 8.0 (12px, gridlines `#e5e9ef`).
- **Dots:** radius 6, Blend A in blue `#2a78d6` centered at 28% of plot width, Blend B in orange `#d95926` at 72%; horizontal jitter offsets `[-42, -30, -18, -6, 6, 18, 30, 42]`.
- **Mean lines:** 3px horizontal segments (±58px) at 7.5 (blue) and 7.0 (orange).
- **Gap bracket:** magenta `#d55181` vertical bracket at plot center from y(7.5) to y(7.0) with 5px end ticks, labeled bold 13px magenta: "gap = 0.5".
- **Group labels (bold 13px below axis):** "Blend A  (mean 7.5)" in blue, "Blend B  (mean 7.0)" in orange.
- **Annotation (bold 13px green `#008300`, top center of plot):** "groups barely overlap — the gap beats the wobble".

## Computing t by Hand: Gap Divided by Wobble

**Tags:** `worked example` (green), `small numbers` (blue)

- **Step 1** — gap between averages: 7.5 − 7.0 = 0.5 points
- **Step 2** — spread inside each group: A wobbles ±0.28, B wobbles ±0.25
- **Step 3** — wobble of the GAP itself: √(0.077/8 + 0.063/8) ≈ 0.13
- **Step 4** — t = 0.5 / 0.13 ≈ 3.8 — the gap is 3.8 wobbles wide
- **Step 5** — chance alone reaches t = 3.8 about 0.2% of the time: p ≈ 0.002

*Example:* t = 3.8 reads as: "the gap we saw is 3.8 times bigger than the gap luck typically fakes."

**Key point:** t is just signal ÷ noise. Past t ≈ 2 things get interesting; 3.8 is hard for luck to explain.

### Visualization (canvas `c2`, 720×300)

Split panel: left shows the two ingredients of t as horizontal bars; right shows the t distribution under "no difference" with t = 3.8 marked.

- **Title (bold 15px, `#1a5276`, top center):** "t = Gap 0.5 / Wobble 0.13 = 3.8".
- **Divider:** dashed light-gray `#bdc3c7` vertical line (dash 4/3) at x=300.
- **Left panel — horizontal bars** (x=40, max width 200px at value 0.55, 0.75 alpha):
  - "gap between averages": 0.5, magenta `#d55181`
  - "typical gap luck fakes (wobble)": 0.13, gray `#6b7280`
  - Value labels bold 13px right of each bar; captions 12px gray above each bar.
  - Below (bold 13px `#1a5276`, centered at x=165): "0.5 is 3.8 of those wobbles"; footnote 12px gray: "wobble = √(0.077/8 + 0.063/8)".
- **Right panel — t density curve** (df = 14, shape `(1 + t²/14)^-7.5`), t from -5 to 5, blue `#2a78d6` width 2.5, baseline at y=235, curve height 150px; x tick labels -4, -2, 0, 2, 4; axis caption 12px: 't values a lucky "no difference" world produces'.
- **Observed marker:** red `#e74c3c` vertical line (width 2.5) at t = 3.8, labeled bold 13px red right-aligned: "our t = 3.8", then bold 12px: "luck gets here 0.2%" / "of the time: p ≈ 0.002".
- **Center label (bold 12px blue, at t=0):** "luck usually lands here".

## Same Gap, Noisier Tasters — the Verdict Flips

**Tags:** `where it's used` (blue), `why it matters` (orange)

- **Everywhere** — A/B tests, before/after metrics, drug vs placebo: two averages, one question
- **Eyeballing fails** — a 0.5 gap looks identical on a dashboard whether noise is 0.3 or 1.0
- **Noisy twin** — same 0.5 gap but tasters wobbling ±1.0 gives t = 1.0, p ≈ 0.33 (illustrative)
- **The flip** — clean data: convincing (p ≈ 0.002); noisy data: a shrug (p ≈ 0.33)
- **Without it** — teams ship "winners" that are just noise, then wonder why lifts vanish

*Example:* Two dashboards both show "+0.5"; only the t-test can tell which one is worth believing.

**Key point:** the gap alone is never enough — the same 0.5 is strong evidence in quiet data and nothing in noisy data.

### Visualization (canvas `c3`, 720×300)

Two side-by-side dot-plot panels: same 0.5 gap with quiet vs noisy data.

- **Title (bold 15px, `#1a5276`, top center):** "Same 0.5 Gap — Opposite Verdicts (noisy side illustrative)".
- **Divider:** dashed `#bdc3c7` vertical line at x=360.
- **Shared y scale:** 5.0–9.5, plot top y=52, plot height 175.
- **Left panel (center x=185):** quiet data — A = `[7.9, 7.3, 7.7, 7.1, 7.5, 7.4, 7.8, 7.3]`, B = `[7.1, 6.7, 7.3, 6.6, 7.0, 6.9, 7.2, 7.2]`; verdict line bold 13px green `#008300`: "t = 3.8   p ≈ 0.002" then "believable"; panel header bold 13px green: "quiet data: wobble ±0.3".
- **Right panel (center x=545):** noisy twin (same means 7.5 / 7.0, spread ~1.0, illustrative) — A = `[9.0, 6.2, 8.3, 6.6, 7.5, 6.9, 8.7, 6.8]`, B = `[8.4, 5.6, 7.9, 5.9, 7.0, 6.5, 8.1, 6.6]`; verdict line bold 13px red `#e74c3c`: "t = 1.0   p ≈ 0.33" then "could be luck"; panel header bold 13px red: "noisy data: wobble ±1.0".
- **Each panel:** A dots blue `#2a78d6` (radius 5, jitter `[-24,-16,-8,0,8,16,24,32]` minus 4) at center−70, B dots orange `#d95926` at center+70; 3px mean lines (±38px) at 7.5 (blue) and 7.0 (orange); group labels 12px: "A (7.5)", "B (7.0)".

## What People Get Wrong: Significant ≠ Big

**Tags:** `common mistake` (red), `assumptions` (orange)

- **Tiny but "significant"** — a 0.05-point gap with 5,000 tasters per blend gives t ≈ 8.9
- **The trap** — huge samples make trivial gaps "significant"; t answers "real?", not "big?"
- **Report both** — the gap (0.5 points) AND the verdict (p ≈ 0.002), never just one
- **Fine print** — assumes independent tasters and roughly bell-shaped ratings per group
- **Outlier alert** — one taster scoring 1/10 can swamp a small sample; check the raw dots first

*Example:* With 5,000 tasters per blend, even a 0.05-point gap no customer would notice passes the test.

**Key point:** t tells you whether a gap is real, not whether it matters — size and significance are separate questions.

### Visualization (canvas `c4`, 720×300)

Three-bar chart contrasting gap size with significance verdict across scenarios.

- **Title (bold 15px, `#1a5276`, top center):** '"Significant" Is About Certainty, Not Size'.
- **Axes:** L-shaped gray `#999`, padding top 60, bottom 66, left 60, right 30; y = gap in points, max 0.6; y labels gray 12px: "gap", "0.5", "0".
- **Bars (70px wide, 0.7 alpha), one per scenario:**
  - gap 0.50 pts, "8 / blend", "t = 3.8", verdict "significant — real AND big", green `#008300`
  - gap 0.05 pts, "5,000 / blend", "t ≈ 8.9", verdict "significant — real but trivial", yellow `#c98500`
  - gap 0.50 pts, "8 / blend, noisy", "t = 1.0", verdict "not significant — maybe big, unproven", red `#e74c3c`
- Value labels ("0.5 pts", "0.05 pts", "0.5 pts") bold 13px above bars; sample-size line and t line 12px below baseline; verdict line bold 12px in the bar's color.
- **Annotation (bold 13px violet `#4a3aa7`, centered above plot):** "a huge sample can certify a gap nobody can taste".

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
