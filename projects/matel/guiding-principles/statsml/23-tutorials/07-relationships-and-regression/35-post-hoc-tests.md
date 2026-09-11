# Post-hoc Tests

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Post-hoc Tests

**Subtitle:** ANOVA only says "some group differs" — a post-hoc test like Tukey's HSD names which pairs differ, using one honest yardstick so the 5% error promise still holds

## One Garden, Four Fertilizers, One Vague Verdict

**Tags:** `core idea` (blue), `ANOVA follow-up` (green), `pairwise comparisons` (orange)

- **The garden** — a gardener feeds 10 tomato plants each with fertilizers A, B, C, D and weighs the yield
- **The means** — average yield per plant: A 5.1 kg, B 5.3 kg, C 6.0 kg, D 7.0 kg (SD about 0.7 kg)
- **ANOVA's answer** — F = 15.0, p < 0.001: at least one fertilizer differs, but it never says which
- **Six pairs** — with 4 groups there are 6 pairs to compare: A–B, A–C, A–D, B–C, B–D, C–D
- **Post-hoc test** — the follow-up step that names the differing pairs while keeping error control

*Example (italic):* The gardener knows "something differs" — a post-hoc test turns that into "buy D, skip B."

**Key point:** ANOVA is a smoke alarm: it says a difference exists somewhere. A post-hoc test is the search that finds which pairs differ, without cheating on error.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of the four fertilizer mean yields with SD whiskers, and an annotation stating ANOVA's vague verdict.

- **Title (bold 15px, `#1a5276`, top center):** "Tomato Yield by Fertilizer: ANOVA Says 'Differs' — Not Where".
- **Data:** groups A, B, C, D; mean yields `[5.1, 5.3, 6.0, 7.0]` kg; whiskers ±0.7 kg on every bar; n = 10 per group.
- **Axes:** origin x=70, plot width 560, baseline y=245, chart height 185, y scale 0–8 kg with gridlines `#e5e9ef` and 12px `#444` tick labels at 0, 2, 4, 6, 8.
- **Bars:** four bars 70px wide, evenly spaced; A, B, C fill `rgba(42,120,214,0.45)`; D fill `rgba(0,131,0,0.4)` (it looks like the winner); 1.5px ink `#1a5276` whisker lines with 10px caps; bold 12px ink mean label above each whisker ("5.1", "5.3", "6.0", "7.0"); group letters 13px `#444` below baseline.
- **Annotation (bold 13px orange `#d95926`, upper left):** "F = 15.0, p < 0.001 — but WHICH pairs differ?".
- **Caption (12px `#444`, bottom center):** "10 plants per fertilizer, SD ≈ 0.7 kg (illustrative)".

## Why Six Plain T-Tests Cheat

**Tags:** `why it matters` (blue), `error inflation` (red)

- **The temptation** — run an ordinary 5% t-test on each of the 6 pairs and report whatever lights up
- **The catch** — each test carries its own 5% false-alarm risk, and the risks stack across 6 pairs
- **The math** — the chance of at least one false alarm is 1 − 0.95^6 ≈ 26%, not the promised 5%
- **It grows fast** — 5 groups mean 10 pairs (40% risk); 7 groups mean 21 pairs (66% risk)
- **Family-wise error** — the 5% budget should cover the whole family of comparisons, not each one

*Example (italic):* With 7 identical fertilizers and no correction, a gardener still "finds a winner" 66% of the time.

**Key point:** Uncorrected pairwise t-tests almost guarantee a false discovery once groups multiply — the 5% error rate must apply to all comparisons together.

### Visualization (canvas `c2`, 720×300)

Line chart of family-wise false-alarm probability versus number of groups, with the promised 5% level as a dashed reference line.

- **Title (bold 15px, `#1a5276`, top center):** "Chance of at Least One False Alarm, Uncorrected T-Tests".
- **Data:** groups `[2, 3, 4, 5, 6, 7]`; pairs `[1, 3, 6, 10, 15, 21]`; family-wise error % `[5, 14, 26, 40, 54, 66]` (= 100 × (1 − 0.95^pairs), rounded).
- **Axes:** origin x=70, plot width 560, baseline y=245, chart height 185, y scale 0–70% with gridlines `#e5e9ef` and 12px `#444` labels at 0, 20, 40, 60; x labels 12px `#444` under each point: "2 (1 pair)", "3 (3)", "4 (6)", "5 (10)", "6 (15)", "7 (21)".
- **Curve:** magenta `#d55181` 3px line with 5px dots; 12px magenta value labels ("5%" … "66%") above each dot.
- **Reference line:** dashed green `#008300` (dash 4/3) horizontal line at 5%, labeled bold 12px green "the promised 5%" at its right end.
- **Annotation (bold 13px orange `#d95926`, near the k=4 dot):** "4 fertilizers → 6 pairs → 26%".
- **Caption (12px `#444`, bottom right):** "family-wise error = 1 − 0.95^pairs".

## Tukey's Honest Yardstick

**Tags:** `worked example` (blue), `Tukey's HSD` (green)

- **One yardstick** — Tukey's HSD computes a single "honestly significant difference" for every pair
- **The formula** — HSD = q × √(MSE/n), where q comes from the studentized range table
- **The numbers** — q(4 groups, 36 df) = 3.81, MSE = 0.49, n = 10, so HSD = 3.81 × 0.221 = 0.84 kg
- **The rule** — any two fertilizer means further apart than 0.84 kg are declared different
- **The verdict** — D–A 1.9, D–B 1.7, D–C 1.0, C–A 0.9 clear the bar; C–B 0.7 and B–A 0.2 do not

*Example (italic):* The gardener's report: fertilizer D beats everything, C edges A, and B is indistinguishable from A.

**Key point:** One number — HSD = 0.84 kg — settles all six comparisons at once, and the whole family of verdicts shares a single 5% error budget.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of the six pairwise mean gaps against a single vertical Tukey HSD threshold line, colored by verdict.

- **Title (bold 15px, `#1a5276`, top center):** "Six Pairwise Gaps vs One Tukey Yardstick (HSD = 0.84 kg)".
- **Data:** pairs `["D–A", "D–B", "D–C", "C–A", "C–B", "B–A"]`; gaps `[1.9, 1.7, 1.0, 0.9, 0.7, 0.2]` kg.
- **Layout:** bars start at x=140, gap scale 0–2.0 kg over 520px width; six rows from y=62, row spacing 31px, bar height 20px; pair labels bold 12px `#444` left of each bar.
- **Bars:** gaps ≥ 0.84 fill `rgba(0,131,0,0.4)` with green `#008300` 12px value + "differs" label right of the bar; gaps < 0.84 fill `rgba(213,81,129,0.35)` with magenta `#d55181` 12px value + "no call" label.
- **Threshold:** vertical dashed ink `#1a5276` 2px line (dash 5/4) at the 0.84 position from y=48 to y=252, labeled bold 13px ink "HSD = 0.84 kg" above it.
- **Caption (12px `#444`, bottom center):** "HSD = q × √(MSE/n) = 3.81 × √(0.49/10) = 0.84 kg".

## Best-vs-Worst Is Still Six Tests

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **The shortcut** — eyeballing the chart, testing only best (D) vs worst (A), and calling it one test
- **Hidden family** — picking the extremes after looking at the data implicitly compared all 6 pairs
- **Right tool** — Tukey's HSD is built exactly for all pairwise comparisons; its bar here is 0.84 kg
- **Bonferroni** — splitting 5% six ways gives a slightly stricter bar of 0.87 kg — safe but blunter
- **No correction** — a plain t-test bar sits at 0.64 kg and would wrongly flag the 0.7 kg C–B gap

*Example (italic):* An uncorrected t-test declares C beats B (0.7 > 0.64); Tukey's 0.84 bar correctly withholds judgment.

**Common mistake:** Testing only the biggest-looking gap "because it's just one comparison". Choosing that pair by looking at the data spends the whole family's error budget — use Tukey's all-pairs bar instead.

### Visualization (canvas `c4`, 720×300)

The same six gap bars in a neutral color, crossed by three vertical threshold lines — no correction, Tukey, Bonferroni — with the C–B gap called out as the avoided false alarm.

- **Title (bold 15px, `#1a5276`, top center):** "Three Thresholds for the Same Six Gaps".
- **Data:** pairs `["D–A", "D–B", "D–C", "C–A", "C–B", "B–A"]`; gaps `[1.9, 1.7, 1.0, 0.9, 0.7, 0.2]` kg; thresholds: no correction 0.64 kg, Tukey 0.84 kg, Bonferroni 0.87 kg.
- **Layout:** identical to c3 — bars from x=140, scale 0–2.0 kg over 520px, six rows from y=62, spacing 31px, height 20px; all bars fill `rgba(42,120,214,0.35)` with 12px `#444` gap values right of each bar; pair labels bold 12px `#444` on the left.
- **Threshold lines (each from y=48 to y=232):** magenta `#d55181` dashed (dash 4/3) 2px at 0.64 labeled bold 12px "no correction 0.64"; green `#008300` solid 2.5px at 0.84 labeled bold 12px "Tukey 0.84"; orange `#d95926` dashed (dash 4/3) 2px at 0.87 labeled bold 12px "Bonferroni 0.87"; stagger the three labels at the top so they don't overlap.
- **Annotation (bold 12px magenta `#d55181`, two lines, right of the C–B row):** "0.7 clears 0.64 but not 0.84" / "— false alarm avoided".
- **Caption (12px `#444`, bottom center):** "thresholds from MSE = 0.49, n = 10, df = 36; Bonferroni uses t at 0.05/6 per pair".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
