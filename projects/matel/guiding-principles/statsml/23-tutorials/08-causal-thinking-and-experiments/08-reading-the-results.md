# Reading the Results

**Page type:** detail page (tutorial page: `.card-section` blocks, each h2 + two-column `table.layout` — text left 50%, canvas right 50%, tag pills above bullets)
**HTML title tag:** Reading the Results

**Subtitle:** Three numbers come back — the lift, the confidence interval, and the p-value — and none of them says "ship it" on its own

## The Test Ends: Three Numbers on the Dashboard

Tags: `running example` (green), `core idea` (blue)

- **The lift** — +0.3pt (3.4% − 3.1%): the best single guess for green's true effect
- **The interval** — 95% CI [−0.1, +0.7]: the range of true lifts that fit the data
- **The p-value** — 0.14: how often pure chance would fake a gap at least this big
- **The verdict** — the CI includes 0 and p > 0.05: not statistically significant
- **Still informative** — the same CI also reaches +0.7pt: a solid win fits the data too

*Example:* The dashboard row reads: lift +0.3pt, 95% CI [−0.1, +0.7], p = 0.14, n = 15,000 per arm.

**Key point:** "Not significant" does not mean "no effect" — it means this much data cannot yet tell +0.3pt apart from 0.

### Visualization (canvas `c1`, 720×300)

Confidence interval drawn on a horizontal number line.

- **Title (bold 16px, ink `#1a5276`, top center):** "The Result as a Range: 95% CI [−0.1, +0.7]".
- **Axis:** horizontal number line at y=200, from x-padding left 70 / right 40; value range −0.3 to 0.9. Tick marks and labels (12px, mute `#6b7280`) at −0.3, −0.1, 0, +0.1, +0.3, +0.5, +0.7, +0.9 (positive values prefixed "+"). Axis stroke `#999`. Axis caption below (mute): "true lift of the green button, percentage points".
- **Zero line:** vertical dashed red (`#e74c3c`, dash 6/4, width 1.5) at x=0 from y=60 to the axis; bold red 12px label above: "0 = no effect".
- **CI whisker:** blue (`#2a78d6`) horizontal bar at y=130 from −0.1 to +0.7, line width 4, with vertical end caps (±12px, width 3). Bold blue 12px endpoint labels below: "−0.1" and "+0.7".
- **Point estimate:** filled orange (`#d95926`) circle radius 8 at +0.3; bold 13px label above: "best guess: +0.3pt".
- **Annotations:** bold red 13px two-line text starting just right of zero at y=70/88: "zero is still inside the range —" / "\"no effect\" has not been ruled out". Bold green (`#008300`) 13px right-aligned near x=0.88 at y=84: "but +0.7pt fits too".
- **Caption (bottom center, mute 12px):** "p = 0.14, n = 15,000 per arm".

## What Each Number Does — and Doesn't — Tell You

Tags: `worked example` (green), `common mistake` (red)

- **CI by hand** — lift ± 2 × standard error ≈ 0.3 ± 2 × 0.2 = [−0.1, +0.7]
- **p = 0.14 means** — a truly useless button fakes a gap this big in ~14 tests out of 100
- **p does NOT mean** — "86% chance green works" or "14% chance this result is wrong"
- **CI says more** — it rules out losses beyond −0.1pt and wins beyond +0.7pt
- **None say why** — no number explains WHY green did better (color? contrast? novelty?)

*Example:* Ruling out anything worse than −0.1pt is itself useful: green is very unlikely to be hurting much.

**Key point:** Three jobs, three numbers: the lift is the best guess, the CI is the plausible range, the p-value grades the noise.

### Visualization (canvas `c2`, 720×300)

Dot-grid frequency chart: 100 dots, 14 highlighted, illustrating what p = 0.14 counts.

- **Title (bold 16px, ink, top center):** "100 A/B Tests of a Do-Nothing Button".
- **Grid:** 10×10 grid of dots, origin (120, 62), cell spacing 19px, dot radius 6.5. Dots at hardcoded "lucky" indices `[3, 7, 18, 22, 26, 39, 44, 50, 58, 63, 71, 81, 88, 95]` filled orange `#d95926`; all others filled `rgba(42,120,214,0.30)`.
- **Right-side text block (left-aligned, starting 40px right of the grid):** text `#2c3e50` 12px: "Each dot: one full test where the button" / "truly changes nothing." Then bold orange 13px: "~14 of 100 still show a gap of +0.3pt" / "or more, by luck alone." Then bold ink 13px: "That frequency is ALL that p = 0.14 says." Then mute 12px: "It is not the chance that OUR result" / "is wrong, and not 86% proof of a win."
- **Caption (mute 12px, centered under the grid):** "illustrative — expected count at p = 0.14".

## Ship, Kill, or Extend?

Tags: `rule of thumb` (blue), `where it's used` (orange)

- **Extend** — the default here: 2x the data shrinks the CI by ~30%, 4x halves it
- **Projected** — if +0.3pt holds at 4x data, CI ≈ 0.3 ± 0.2 = [+0.1, +0.5]: clear win
- **Ship anyway** — defensible when the change is free and the CI rules out real harm
- **Kill** — right call when the test slot is needed for ideas with bigger expected lifts
- **Cost matters** — the decision weighs engineering cost and risk, not just the p-value

*Example:* The team extended to 4x the data; the CI tightened to [+0.1, +0.5] and green shipped.

**Key point:** Extend when the CI is wide and the answer matters; ship when harm is ruled out and the change is cheap; kill when the slot is worth more elsewhere.

### Visualization (canvas `c3`, 720×300)

Three stacked CI whiskers showing the interval tightening as data grows.

- **Title (bold 16px, ink, top center):** "Extending the Test: the CI Tightens With √n".
- **Axis mapping:** value range −0.3 to 0.9 mapped to x with padding left 210 / right 40 (no drawn horizontal axis).
- **Zero line:** vertical dashed red (`#e74c3c`, dash 6/4, width 1.5) at value 0 from y=54 to y=232, bold red 12px label "0" above.
- **Rows (each a 4px-wide whisker with 3px end caps ±9px, orange `#d95926` dot radius 6 at +0.3):**
  - y=80, blue `#2a78d6`: [−0.1, +0.7]; right-aligned bold 12px label "now: n = 15k/arm" with mute note "CI [−0.1, +0.7]".
  - y=140, aqua `#199e70`: [+0.02, +0.58]; label "2x data: n = 30k/arm", note "CI [+0.02, +0.58]".
  - y=200, green `#008300`: [+0.1, +0.5]; label "4x data: n = 60k/arm", note "CI [+0.1, +0.5]".
- **Annotations:** bold green 13px at value ~0.52, y=204: "at 4x, zero excluded — clear win". Bottom center mute 12px: "projected intervals, IF the +0.3pt lift holds up (orange dot = lift)". Bottom center bold orange 13px: "doubling the data never doubles the certainty — the CI shrinks by ~30% per 2x".

## The Common Misreadings

Tags: `common mistake` (red)

- **"p = 0.14 = 14% fluke odds"** — no: 14% is how often no-effect tests fake this gap
- **"Not significant = no effect"** — no: a wide CI means "can't tell yet", not "nothing"
- **"Significant = big"** — with huge n, a worthless +0.02pt can reach p = 0.001
- **"0.049 ships, 0.051 dies"** — 0.05 is a convention, not physics; read the CI

*Example:* A test with 16M users per arm found p = 0.001 on a +0.02pt lift — real, and worth almost nothing.

**Key point:** Read the CI first and the p-value second.

### Visualization (canvas `c4`, 720×300)

Two contrasting CIs: wide/not-significant vs narrow/tiny-but-significant.

- **Title (bold 16px, ink, top center):** "Significant ≠ Big, Not Significant ≠ Zero".
- **Axis mapping:** value range −0.3 to 0.9, padding left 250 / right 40.
- **Zero line:** vertical dashed red (`#e74c3c`, dash 6/4, width 1.5) at value 0 from y=54 to y=226, bold red 12px "0" above.
- **Interval A (y=100):** blue `#2a78d6` whisker [−0.1, +0.7], width 4, end caps ±9px, orange dot radius 6 at +0.3. Right-aligned labels: bold 12px text `#2c3e50` "our test: n = 15k/arm", mute 12px "p = 0.14 — not significant". Bold blue 12px annotation above the bar: "wide CI: could be nothing, could be +0.7pt — \"can't tell yet\"".
- **Interval B (y=185):** violet `#4a3aa7` bar from +0.01 to +0.03, line width 6 (no caps). Right-aligned labels: bold "huge test: n = 16M/arm", mute "p = 0.001 — very significant". Thin violet callout line from the bar up-right to bold violet 12px text: "narrow CI [+0.01, +0.03]: definitely real, definitely tiny".
- **Captions:** bold ink 13px centered: "the p-value graded the noise; only the CI shows the size — read the CI first". Mute 12px centered at bottom: "true lift, percentage points (both intervals illustrative)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (see `tutorials/CLAUDE.md` and `most-powerful-signals/07-social-graph-connections.html` skeleton). `<h1>` (no index number) with 2px `#2980b9` bottom border, `.subtitle` in `#666` 0.95rem, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one `<tr>`: left `td.text-col` (50%) and right `td.viz-col` (50%) holding the canvas.
- **Left column structure:** `.tags` row of colored pill spans (0.72rem, 600 weight, radius 10px — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (`#555`, 0.9rem); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, 0.9rem) beginning with `<strong>Key point:</strong>`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box; }`; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvas JS:** shared palette object `P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`; shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; this page also has a shared `makeAxis(padLeft, cw, lo, hi)` linear x-mapper used by c1, c3, c4. All data hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Site palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions (this page has none).
