# Repeated Measures & ANCOVA

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Repeated Measures & ANCOVA

**Subtitle:** Measuring the same subject twice cancels person-to-person noise, and ANCOVA compares groups after adjusting each to a common baseline covariate

## The Same Heart, Measured Twice

**Tags:** `core idea` (blue), `paired design` (green), `within-subject` (orange)

- **The gym** — eight members do an 8-week program; resting heart rate is measured before and after
- **Same subject twice** — each member is their own control, so person-to-person noise cancels out
- **Big spread** — before values run 68 to 91 bpm; people differ far more than the program's effect
- **Small, steady drop** — every member falls 2 to 6 bpm; the paired view makes this visible
- **Unpaired blindness** — mixing all 16 numbers hides a 4 bpm drop inside a 23 bpm person spread

*Example (italic):* Member 3 goes 91 → 85 and member 4 goes 68 → 66 — very different hearts, the same downward story.

**Key point:** When the same subject is measured twice, compare each subject to themselves — the design removes between-person variation before any statistics start.

### Visualization (canvas `c1`, 720×300)

Paired slopegraph: eight before→after lines, one per member, all sloping down between a "before" column and an "after" column.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Members, Before vs After: Every Line Slopes Down".
- **Data:** members M1–M8; before `[82, 75, 91, 68, 88, 79, 72, 85]`; after `[77, 73, 85, 66, 82, 74, 70, 81]` (bpm).
- **Layout:** y maps 60–95 bpm onto y=250 (60) up to y=55 (95); y ticks at 60/70/80/90 with 12px `#444` labels at x=45 and light grid lines `#e5e9ef` from x=70 to x=650; before column at x=230, after column at x=490; bold 13px `#1a5276` column headers "before" and "after" at y=45.
- **Marks:** one 2px `#6b7280` line per member connecting its two dots; before dots 5px blue `#2a78d6`, after dots 5px green `#008300`; 11px `#444` value labels only on the extremes — "91" and "68" left of their before dots, "85" and "66" right of their after dots.
- **Means:** short dashed `#1a5276` horizontal ticks (x=210–250 at 80 bpm, x=470–530 at 76 bpm) labeled bold 12px ink "mean 80" and "mean 76".
- **Annotation (bold 13px orange `#d95926`, right of the lines):** "every member drops 2–6 bpm".
- **Caption (12px `#444`, bottom center):** "resting heart rate in bpm (illustrative)".

## Eight Differences Do All the Work

**Tags:** `worked example` (blue), `paired differences` (green)

- **Subtract first** — after minus before per member gives −5, −2, −6, −2, −6, −5, −2, −4
- **Mean change** — the differences sum to −32, so the average drop is 32 / 8 = 4 bpm
- **Noise collapses** — people vary with sd 8.0 bpm, but the eight drops vary with sd only 1.8
- **Paired t ≈ 6.4** — a one-sample test of the 8 differences against zero is decisive
- **Unpaired t ≈ 1.0** — the same 16 numbers analysed as two separate groups show nothing

*Example (italic):* Treating before and after as two unrelated groups of 8, the 4 bpm drop drowns in the 8 bpm person-to-person spread.

**Key point:** The paired analysis runs on the differences alone — sd falls from 8.0 to 1.8 and t jumps from 1.0 to 6.4 with the exact same data.

### Visualization (canvas `c2`, 720×300)

Dual panel split by a vertical dashed divider at x=360: per-member difference bars (left) and an unpaired-vs-paired t-value comparison (right).

- **Title (bold 15px, `#1a5276`, top center):** "Same 16 Numbers: Differences per Member, and the t-Value They Unlock".
- **Left panel (differences):** values `[-5, -2, -6, -2, -6, -5, -2, -4]` for M1–M8; zero line 2px `#999` at y=70 from x=55 to x=335; bars grow downward, 1 bpm = 24px (so −6 reaches y=214); bar width 24px, fill `rgba(0,131,0,0.4)` with 1px `#008300` stroke; member labels "M1"–"M8" 11px `#444` above the zero line; dashed magenta `#d55181` horizontal line at y=166 labeled bold 12px "mean −4 bpm"; caption 12px `#444` "after − before, per member".
- **Right panel (t values):** two bars from baseline y=245, axis origin x=400, width 280, scale max t=7 over 185px height; "unpaired t ≈ 1.0" bar fill `rgba(42,120,214,0.45)` with blue `#2a78d6` bold 13px value label above; "paired t ≈ 6.4" bar fill `rgba(0,131,0,0.4)` with green `#008300` label; dashed `#1a5276` horizontal line at t=2.4 labeled 12px ink "significance cutoff ≈ 2.4"; bar name labels 12px `#444` below baseline.
- **Annotation (bold 13px green `#008300`, top of right panel):** "pairing turns nothing into decisive".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Two Programs, One Covariate

**Tags:** `ANCOVA` (blue), `covariate adjustment` (green), `where it's used` (orange)

- **New question** — 6 members try a new program, 6 keep the old routine; which group ends lower?
- **Covariate** — baseline heart rate strongly predicts the final one, so it must be adjusted for
- **ANCOVA** — fit final = slope × baseline + a group shift; here final ≈ 0.9 × before per group
- **Parallel lines** — both groups share slope 0.9; the vertical gap between the lines is the effect
- **Adjusted effect** — at any shared baseline, the new program sits 4 bpm below the old routine

*Example (italic):* Two members who both start at 80 bpm are predicted to finish at 76 on the old routine but 72 on the new program.

**Key point:** ANCOVA compares groups at the same covariate value — like comparing members who started out identical, even when none of them actually did.

### Visualization (canvas `c3`, 720×300)

Scatter of final vs baseline heart rate for both groups, with two parallel fitted lines and a vertical gap arrow marking the adjusted effect.

- **Title (bold 15px, `#1a5276`, top center):** "Final vs Baseline Heart Rate: Two Parallel Lines, One 4 bpm Gap".
- **Data (old routine, blue `#2a78d6` 5px dots):** baseline `[70, 74, 76, 82, 88, 92]`, final `[67, 71, 72, 78, 83, 87]`.
- **Data (new program, green `#008300` 5px dots):** baseline `[66, 72, 76, 78, 84, 90]`, final `[59, 65, 68, 70, 76, 81]`.
- **Axes:** x maps baseline 64–94 bpm from x=60 to x=640; y maps final 55–90 bpm onto y=250 (55) up to y=55 (90); x ticks at 65/75/85 and y ticks at 60/70/80, 12px `#444`; axis titles 12px `#444` "baseline bpm" (bottom center) and "final bpm" (rotated, left).
- **Fitted lines (2.5px):** blue line y = 0.9x + 4 and green line y = 0.9x, both drawn from x-value 64 to 94; 11px matching-color labels "old routine" and "new program" at the right ends.
- **Gap arrow:** vertical double-headed arrow `#d95926` 2.5px at baseline 80 between the two lines (final 76 down to 72), labeled bold 13px orange "adjusted gap = 4 bpm at any baseline".
- **Legend (top left, 12px):** blue dot "old routine", green dot "new program".
- **Caption (12px `#444`, bottom right):** "illustrative; both lines share slope 0.9".

## The Head-Start Trap

**Tags:** `common mistake` (red), `baseline imbalance` (orange)

- **Raw means** — final averages are 76.3 (old) vs 69.8 (new), a tempting raw gap of 6.5 bpm
- **Unequal start** — new-program members began at 77.7 bpm vs 80.3, a 2.7 bpm head start
- **Carried over** — with slope 0.9, the head start alone explains about 2.5 bpm of the gap
- **Honest answer** — the adjusted effect is 4.0 bpm; the raw gap overstates it by half again
- **Rule** — never compare raw post means across groups with different baselines; adjust or randomize

*Example (italic):* A report bragging "new program wins by 6.5 bpm" is really 4.0 bpm of program plus 2.5 bpm the group walked in with.

**Common mistake:** Reading the raw post-program gap as the treatment effect. Part of it is just the head start one group already had at baseline — ANCOVA strips that part out.

### Visualization (canvas `c4`, 720×300)

Horizontal bar decomposition: the 6.5 bpm raw gap on top, split underneath into the 4.0 bpm real effect and the 2.5 bpm head-start carry-over on the same scale.

- **Title (bold 15px, `#1a5276`, top center):** "Where the 6.5 bpm Raw Gap Really Comes From".
- **Scale:** bars start at x=70; 1 bpm = 80px, so 6.5 bpm spans 520px; light `#e5e9ef` vertical grid ticks every 1 bpm with 11px `#6b7280` labels "0"–"6" below y=235.
- **Top bar (y=80, 30px tall):** length 520px, fill `rgba(42,120,214,0.45)`, 1px `#2a78d6` stroke; bold 13px blue label "raw post-program gap = 6.5 bpm" above; row label 12px `#444` "what the naive report shows" left-aligned at x=70, y=70.
- **Bottom bar (y=160, 30px tall):** two segments — green `rgba(0,131,0,0.4)` with `#008300` stroke from x=70, length 320px (4.0 bpm), bold 12px green inside-label "real effect 4.0"; orange `rgba(217,89,38,0.5)` with `#d95926` stroke continuing to 520px total (2.5 bpm), bold 12px orange label "head start ≈ 2.5" above its segment; row label 12px `#444` "what ANCOVA finds" at x=70, y=150.
- **Takeaway (bold 13px magenta `#d55181`, centered at y=272):** "6.5 raw = 4.0 program effect + 2.5 head start — adjust before you compare".
- **Caption (11px `#444`, bottom right):** "head start = 0.9 × 2.7 bpm baseline gap (rounded)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
