# ANOVA: Comparing Many Groups

**Page type:** detail page (tutorial layout: `.card-section` blocks, each with h2 + two-column `table.layout` — text left 50% with tag pills/bullets/example/key-point, canvas right 50%)
**HTML title tag:** ANOVA: Comparing Many Groups

**Subtitle:** One test for three or more group averages at once — instead of running a risky pile of pairwise comparisons

## Three Store Branches, Five Days of Sales Each

**Tags:** `core idea` (blue), `running example` (green), `many groups` (orange)

- **The setup** — branches A, B, C log daily sales (in $100s) for 5 days each
- **The averages** — A averages 50, B averages 56, C averages 53
- **The question** — do the branches really differ, or would three fair copies drift this much?
- **The trick** — compare spread BETWEEN branch averages to spread WITHIN each branch
- **One dial** — ANOVA rolls all three groups into a single number, F

*Example:* A: 52, 48, 50, 54, 46    B: 58, 54, 56, 60, 52    C: 51, 55, 53, 49, 57.

**Key point:** ANOVA asks one question about all groups at once — "do the group averages spread out more than the day-to-day noise predicts?"

### Visualization (canvas `c1`, 720×300)

Dot plot of the three branches' daily sales with per-branch mean lines and the grand mean.

- **Title (bold 15px, `#1a5276`, top center):** "15 Days of Sales: Three Branches Around One Grand Mean".
- **Data:** Branch A = `[52, 48, 50, 54, 46]` (mean 50, blue `#2a78d6`); Branch B = `[58, 54, 56, 60, 52]` (mean 56, orange `#d95926`); Branch C = `[51, 55, 53, 49, 57]` (mean 53, aqua `#199e70`).
- **Axes:** L-shaped gray `#999`, padding top 56, bottom 52, left 60, right 30; y range 44–62, gridlines `#e5e9ef` and right-aligned labels "$4500", "$5000", "$5500", "$6000" at 45, 50, 55, 60 (12px, values × 100 with "$" prefix).
- **Grand mean line:** dashed gray `#6b7280` horizontal line (dash 7/5, width 2) at 53, labeled bold 12px: "grand mean 53".
- **Dots:** radius 6 in each branch's color; branch centers at 18%, 50%, 82% of plot width; jitter offsets `[-28, -14, 0, 14, 28]`.
- **Mean lines:** 3px horizontal segments (±42px) at each branch mean in the branch color; labels bold 13px below axis: "Branch A (50)", "Branch B (56)", "Branch C (53)".
- **Annotation (bold 13px magenta `#d55181`, top center of plot):** "do the mean lines spread more than the dots wobble?".

## Between-Spread Over Within-Spread: F = 4.5

**Tags:** `worked example` (green), `small numbers` (blue)

- **Grand mean** — (50 + 56 + 53) / 3 = 53 across all 15 days
- **Between** — branch means sit 3, 3, 0 away: 5×(9+9+0) = 90; 90 ÷ 2 (= 3 groups − 1) = 45
- **Within** — wobble around each branch's own mean: 40+40+40 = 120; 120 ÷ 12 (= 15 days − 3) = 10
- **The ratio** — F = 45 / 10 = 4.5: branch gaps are 4.5x the size noise predicts
- **The verdict** — identical branches reach F = 4.5 only ~3.5% of the time: p ≈ 0.035

*Example:* If the branches were truly identical, between-spread and within-spread would match: F near 1.

**Key point:** F near 1 means "averages drift exactly as much as noise predicts"; F well above 1 means something real separates the groups.

### Visualization (canvas `c2`, 720×300)

Two horizontal ingredient bars (between vs within) plus an F result box.

- **Title (bold 15px, `#1a5276`, top center):** "F = Between-Group Spread / Within-Group Spread".
- **Bars** (start x=250, width scaled to max value 50, height 34px, 0.75 alpha):
  - "BETWEEN: spread from gaps" with sub-label "5×(9+9+0) = 90, ÷2 (3 groups − 1) → 45": value 45, magenta `#d55181`
  - "WITHIN: spread from noise" with sub-label "40+40+40 = 120, ÷12 (15 days − 3) → 10": value 10, gray `#6b7280`
  - Value labels bold 15px right of each bar; labels bold 12px `#1a5276` and sub-labels 11px gray right-aligned left of the bars.
- **F box (x = w−170, y=82, 145×115):** background `#f8f9fa`, 2px `#1a5276` border; contents centered bold 22px `#1a5276`: "F = 45/10" / "= 4.5"; below bold 12px gray: "p ≈ 0.035".
- **Takeaway (bold 13px green `#008300`, centered, y=240):** "identical branches would give F near 1 — the alarm line here is F = 3.9".
- **Footnote (12px gray, centered, y=266):** "branch gaps are 4.5x what day-to-day noise predicts".

## Why Not Just Run t-Tests on Every Pair?

**Tags:** `why it matters` (orange), `false alarms` (red)

- **The pile-up** — 3 groups make 3 pairs; 5 groups make 10; 10 groups make 45 pairs
- **Each roll** — every pairwise test carries its own 5% false-alarm chance
- **The math** — 3 pairs: ~14% chance of a fake "finding"; 10 pairs: ~40%; 45 pairs: ~90%
- **The scam** — test enough pairs and something will look "significant" by luck alone
- **ANOVA's job** — one test, one 5% false-alarm budget, no matter how many groups

*Example:* Compare 10 store branches pairwise and you are ~90% likely to "discover" a difference that isn't there.

**Key point:** every extra comparison is another lottery ticket for a false alarm — ANOVA buys one ticket for the whole family of groups.

### Visualization (canvas `c3`, 720×300)

Line chart: family-wise false-alarm rate vs number of groups, with ANOVA's flat 5% line.

- **Title (bold 15px, `#1a5276`, top center):** 'Chance of at Least One FAKE "Finding" (all groups truly identical)'.
- **Data:** groups = `[2, 3, 4, 5, 7, 10]`; pairs = `[1, 3, 6, 10, 21, 45]`; family-wise error % = `[5, 14, 26, 40, 66, 90]` (1 − 0.95^pairs).
- **Axes:** L-shaped gray `#999`, padding top 56, bottom 66, left 65, right 35; y 0–100% with gridlines `#e5e9ef` and labels at 0, 25, 50, 75, 100%; x caption 12px: "number of groups compared".
- **ANOVA line:** dashed green `#008300` horizontal line (dash 6/5, width 2.5) at 5%, labeled bold 13px green: "one ANOVA: stays at 5%".
- **Pairwise curve:** red `#e74c3c` polyline width 3 with 5px-radius red dots; percentage labels bold 12px above each point; below the baseline per point: group count (12px) and "N pair(s)" (11px gray).
- **Annotation (bold 13px red, upper area):** "pairwise t-tests: 10 groups → ~90% chance of a false alarm".

## What People Get Wrong: ANOVA Never Says WHICH Group

**Tags:** `common mistake` (red), `follow-up` (green)

- **The verdict** — F = 4.5, p ≈ 0.035 says only "at least one branch differs"
- **Not the culprit** — it does not say B is the star or A is the laggard
- **Follow-up** — corrected pairwise checks (e.g. Tukey) find the culprit safely
- **Here** — the corrected bar is ~5.3: B−A = 6 clears it; B−C = 3 and C−A = 3 do not
- **Order matters** — ANOVA first, THEN corrected pairwise — never naked t-tests after

*Example:* Team reads p = 0.035, declares "branch B wins!" — but only the B-vs-A gap actually clears the corrected bar.

**Key point:** ANOVA is a smoke alarm, not a room finder — it says "something's burning", then corrected pairwise tests locate the fire.

### Visualization (canvas `c4`, 720×300)

Bar chart of the three pairwise gaps against the corrected (Tukey) bar.

- **Title (bold 15px, `#1a5276`, top center):** "The Follow-Up: Which Gaps Clear the Corrected Bar?".
- **Data:** "B − A" = 6 (clears, green `#008300`); "B − C" = 3 (does not, gray `#6b7280`); "C − A" = 3 (does not, gray `#6b7280`).
- **Axes:** L-shaped gray `#999`, padding top 60, bottom 62, left 65, right 35; y max 8 with gridlines `#e5e9ef` and labels at 0, 2, 4, 6, 8.
- **Bars:** 90px wide, 0.7 alpha; value labels bold 14px above bars; pair labels 13px below baseline; verdict bold 12px in bar color: "clears the bar" / "does not clear".
- **Tukey bar:** dashed red `#e74c3c` horizontal line (dash 7/5, width 2.5) at 5.3, labeled bold 13px red: "Tukey bar ≈ 5.3 (corrected for 3 comparisons, illustrative)".
- **Annotation (bold 13px violet `#4a3aa7`, centered above plot):** 'ANOVA said "something differs" — only B vs A survives the corrected check'.

## Regeneration instructions

- **Layout:** tutorial topic page. `<h1>` (no index number), `.subtitle` paragraph, then four `.card-section` blocks. Each section: `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) + `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets (each starting with `<b>` term in `#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; ul 0.92rem.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled by `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links use `.html` extensions.
