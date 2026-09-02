# Simpson's Paradox: The Winner of Every Round Can Still Lose the Match

**Page type:** detail page — card-section template (see `tutorials/llms-generative-ai/08-positional-encoding.html`)
**HTML title tag:** Simpson's Paradox — Statistical Paradoxes

**Subtitle:** One treatment cured more of the mild patients and more of the severe patients than its rival, yet cured 140 fewer patients in total — and nobody miscounted a single case

---

## Section 1 — Wins the Mild Cases, Wins the Severe Cases, Loses the Ward

**Tags:** `core idea` (blue), `weighted average` (green), `lopsided intake` (orange)

**Bullets:**
- **Two treatments** — 1,000 patients received treatment A, another 1,000 received treatment B
- **Mild patients** — A cured 180 of its 200, B cured 640 of its 800, so 90% against 80%
- **Severe patients** — A cured 400 of its 800, B cured 80 of its 200, so 50% against 40%
- **A wins both** — ten points better on the mild cases and ten points better on the severe ones
- **The published total** — A cured 58% of its patients and B cured 72%, so A reads much worse
- **The lopsided intake** — 80% of A's patients were severe, against 20% of B's patients
- **Why that sinks A** — severe patients rarely recover, so a severe-heavy arm posts a low total
- **On one shared intake** — A cures 70% against B's 60%, the ten-point lead both wards saw

**Example line (italic):** Treatment A: 180 cures from 200 mild patients plus 400 from 800 severe — 580 of 1,000, or 58%, against treatment B's 720 of 1,000.

**Key point:** An overall rate is a weighted average of the group rates, and the weights differ between the two arms. Load one arm with the hard cases and its total can contradict every single group inside it.

**Source note (`.src`):** Illustrative Example — constructed patient counts; every rate on the chart is computed from them at render time.

### Visualization — canvas `c1`, 720×340

A slope chart: three lines from "treatment B" to "treatment A" — one per patient group, both rising, plus the everyone line falling the other way. Bubble area encodes how many patients sit at that point. Every rate is computed in the draw function from the cure/patient counts.

- **Data (literal counts, no random):** mild — B 640/800, A 180/200; severe — B 80/200, A 400/800. The everyone row is summed in the draw function, never typed: B 720/1,000, A 580/1,000.
- **Title (bold 15px `P.ink`, centered, y=22):** "Wins Both Wards. Loses the Total."
- **Plot box:** `PX0=118`, `PX1=w−214`, `PY0=62`, `PY1=208`; y is cure rate from 30% up to 100%. The legend column starts at `PX1+54`, clear of the widest right-hand point label.
- **Grid:** `P.grid` horizontal lines every 10% from x=70 to `PX1`, with 12px `P.mute` right-aligned "%"-suffixed labels at x=62 — well left of the plot, because the left-hand point labels hang outside their bubbles; `#ccc` axis line at 30%.
- **X labels (bold 12px `P.ink`, centered under `PX0` and `PX1` at `PY1+18`):** "TREATMENT B" and "TREATMENT A"; a 12px `P.mute` note centered at `PY1+34` — "1,000 patients in each arm".
- **Series (line width 3, bubbles filled at 0.45 alpha and stroked in the series color):**

  | Series | B → A | Color | Meaning |
  |--------|-------|-------|---------|
  | mild cases | 80% → 90% | `P.blue` | group, rises |
  | severe cases | 40% → 50% | `P.aqua` | group, rises |
  | everyone | 72% → 58% | `P.magenta`, width 3.5 | the misleading total, falls |

- **Bubbles:** radius `4 + 7·sqrt(patients/1000)`, so 200 patients → 7.1px, 800 → 10.3px, 1,000 → 11.0px; a 12px `P.mute` note "bubble size = patients" sits at the top right of the plot box. The 80/20 intake keeps the everyone marker 16.7px clear of the nearest group marker on both sides, so no two point labels collide.
- **Point labels:** bold 12px in the series colour, printed from the computed rate as a whole percent — left-hand points right-aligned at `x − radius − 8`, right-hand points left-aligned at `x + radius + 8`.
- **Right-hand legend column at `PX1+54`:** per series, bold 12px name in its colour then a 12px `P.mute` verdict — "mild cases" / "A better: +10 pts", "severe cases" / "A better: +10 pts", "everyone" / "A worse: −14 pts", each delta computed as `A − B`.
- **Intake strip (bold 12px `P.orange` "WHO EACH TREATMENT GOT" left at x=30, y=264):** two 14px-tall bars 150px wide from x=150 at y=272 and y=294, row label 12px `P.mute` right-aligned at 142 ("treatment B" / "treatment A"), mild in `rgba(42,120,214,0.45)` stroked `P.blue` and severe in `rgba(25,158,112,0.45)` stroked `P.aqua`, with a 12px `P.mute` readout at `x=310` printed from the counts — "80% mild / 20% severe" and "20% mild / 80% severe".
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "The intake decided the total, not the medicine."

---

## Section 2 — The Checkout Page That Won on Every Screen and Sold Half as Much

**Tags:** `where it bites` (blue), `A/B test` (green), `traffic mix` (orange)

**Bullets:**
- **The test** — a new checkout page against the old one, 10,000 visits sent to each version
- **On phones** — the new page lifted purchases from 2.5% of visits up to 3.0%
- **On laptops** — the new page lifted them from 11.5% to 12.0%, so both screens say ship it
- **The site total** — the old page sold to 9.7% of its visits, the new page to only 4.8%
- **Nobody's rate fell** — within each screen type the new page converted more often, not less
- **The traffic split** — the new page drew 8,000 phone visits, the old page only 2,000
- **Why that sinks it** — phone visitors rarely buy, so a phone-heavy arm posts a low total
- **On one shared split** — the new page reads 7.5% against the old page's 7.0%, still ahead

**Example line (italic):** New page: 240 purchases from 8,000 phone visits plus 240 from 2,000 laptop visits — 480 of 10,000, or 4.8%, against the old page's 970.

**Key point:** Randomised traffic can still land a lopsided split, and channels, regions and devices produce one by default. Check that both arms saw the same audience mix before you believe a total that disagrees with every segment.

**Source note (`.src`):** Illustrative Example — constructed visit counts; every rate, total and standardised figure on the chart is computed from them.

### Visualization — canvas `c2`, 720×340

Paired conversion bars by screen type beside the two site totals, with a traffic-mix strip on the right explaining the gap. All rates derive from the purchase and visit counts inside the draw function.

- **Data (literal counts):** phone — old 50/2,000, new 240/8,000; laptop — old 920/8,000, new 240/2,000. Totals summed in the draw function: old 970/10,000, new 480/10,000.
- **Derived figures:** phone 2.5% vs 3.0%; laptop 11.5% vs 12.0%; all visits 9.7% vs 4.8%; standardised on the pooled 50/50 device mix, 7.0% vs 7.5%.
- **Title (bold 15px `P.ink`, centered, y=22):** "Both Screens Improve. The Site Total Drops."
- **Scale note (12px `P.mute`, left at x=30, y=44):** "Bar height = share of visits ending in a purchase."
- **Left panel** (`x=30`, width `0.52w`): three bar clusters on `baseY=236` — "ON PHONES", "ON LAPTOPS", "ALL VISITS" — each with an old bar and a new bar. Cluster labels bold 12px `P.ink` at `baseY+32`; per-bar 12px `P.mute` "old" / "new" at `baseY+15`.
- **Bars:** width 40, gap 12; old in `rgba(107,114,128,0.40)` stroked `P.mute`, new in `rgba(42,120,214,0.45)` stroked `P.blue`; the ALL VISITS pair stroked 2px `P.magenta` to mark it as the misleading view; bold 13px value above each bar to one decimal with "%"; scaled so the tallest of the six (12.0%) fills `barMaxH = baseY − 72`.
- **Verdicts under the clusters (bold 12px, `baseY+48`):** "new better" (`P.green`) under phones and laptops, "new looks half as good" (`P.magenta`) under all visits.
- **Right panel** (`RX = 0.60w`): bold 12px `P.orange` "WHY — WHO EACH PAGE GOT" at y=72, then two mix bars 150px wide and 16px tall on a 52px pitch — old page 20% phone / 80% laptop, new page 80% phone / 20% laptop; phone in `rgba(74,58,167,0.45)` stroked `P.violet`, laptop in `rgba(201,133,0,0.50)` stroked `P.yellow`; bold 12px row label above each bar and a 12px `P.mute` readout beneath, percentages computed from the visit counts. The 52px pitch (not 44px) is what keeps the first row's readout off the second row's label.
- **Right panel callout (below the mix bars):** bold 12px `P.green` "IF BOTH PAGES HAD THE SAME MIX", then bold 19px `P.green` "7.5% vs 7.0%" and a 12px `P.mute` line "the new page leads by 0.5 points" — both printed from the computed standardised rates.
- **Footnote (12px `P.mute`, left at x=30, `h−26`):** "Each arm got 10,000 visits. Only the phone/laptop split differed."
- **Caption (bold 13px `P.magenta`, centered, `h−9`):** "Ship it in both segments, kill it in the total."

---

## Section 3 — Berkeley, 1973: The Gap Was in Where People Applied

**Tags:** `real case` (blue), `published data` (green), `reversal` (orange)

**Bullets:**
- **The 1973 headline** — Berkeley admitted men to graduate study far more often than women
- **The six largest departments** — 44.5% of male applicants got in, against 30.4% of female
- **Department by department** — four of the six admitted women at the higher rate
- **The two exceptions** — favoured men by under four points, against a 14.2-point overall gap
- **The largest department** — admitted 82.4% of the women who applied and 62.1% of the men
- **Where the men applied** — 51.5% chose the two departments admitting about 64% of everyone
- **Where the women applied** — 40.0% chose the two hardest, one admitting 6.4% of its applicants
- **On one shared mix** — 43.0% for women against 38.7% for men, the headline exactly reversed

**Example line (italic):** Department A admitted 89 of 108 women (82.4%) and 512 of 825 men (62.1%) — yet only 108 of its 933 applicants were women.

**Key point:** The overall gap tracked which departments people applied to, not how those departments treated applicants. Bickel, Hammel & O'Connell reported exactly this in *Science* in 1975: the aggregate gap was not the result of any pattern of discrimination by the admissions committees.

### Visualization — canvas `c3`, 720×340

Six department rows drawn as dumbbells on an admit-rate axis, sorted easiest first, with a right-hand strip showing what share of each sex's applications landed in that department. Both aggregates are obtained by summing the same table.

- **Data (published figures, literal arrays):** A 825 men / 512 admitted, 108 women / 89; B 560/353, 25/17; C 325/120, 593/202; D 417/138, 375/131; E 191/53, 393/94; F 373/22, 341/24.
- **Derived in the draw function:** per-department rates; the six-department totals 1,198/2,691 = 44.5% for men and 557/1,835 = 30.4% for women; the standardised figures 38.7% and 43.0%, each sex's own department rates re-weighted by the pooled applicant counts (4,526 applications).
- **Title (bold 15px `P.ink`, centered, y=22):** "Every Department Nearly Even. The Total Was Not."
- **Axis:** `AX0=118` to `AX1=0.62w`, 0% to 90%; `P.grid` vertical lines from y=94 to y=244 at 0/30/60/90%, with 12px `P.mute` centered tick labels at y=260.
- **Header row (y=88):** `P.blue` "● men" left-aligned at `AX0`, `P.violet` "● women" at `AX0+62`, and a 12px `P.mute` "share of applicants admitted →" right-aligned at `AX1`.
- **Rows:** six rows on a 26px pitch starting at y=102. Row label 12px `P.ink` right-aligned at `AX0−10`, formatted from the data as "A · 933 apps".
- **Dumbbells:** a 3px connector between the two rates — `P.green` when the women's rate is higher, `P.mute` when it is not; a `P.blue` dot radius 5 for men, a `P.violet` dot radius 5 for women; the lower rate labelled bold 12px right-aligned at its dot `−9`, the higher labelled left-aligned at its dot `+9`, both in the dot's colour and printed to one decimal so no two labels can overlap.
- **Right strip** (`SX = 0.66w`, bars 90px wide): bold 12px `P.orange` header "WHERE THEY APPLIED" at y=88; per row two 6px-tall bars — men's share of all male applications (`rgba(42,120,214,0.45)` stroked `P.blue`) at `y−9`, women's share of all female applications (`rgba(74,58,167,0.45)` stroked `P.violet`) at `y−1` — scaled so 35% fills the width; a 12px `P.mute` note "bar = share of that sex's applications" under the strip at y=260.
- **Aggregate callouts:** left at x=30 — bold 12px `P.magenta` "AS PUBLISHED IN THE TOTAL" at y=282 and bold 19px `P.magenta` "44.5% of men, 30.4% of women" at y=304; at `x=0.52w` — bold 12px `P.green` "ON ONE SHARED APPLICANT MIX" and bold 19px `P.green` "38.7% of men, 43.0% of women". All four figures printed from the computed variables.
- **Caption (bold 13px `P.ink`, centered, `h−8`):** "Women applied where almost nobody was admitted."

---

## Section 4 — When Does the Reversal Actually Spring?

**Tags:** `rule of thumb` (blue), `the boundary` (green), `common mistake` (red)

**Bullets:**
- **Two ingredients** — groups with very different base rates, plus an unequal mix across the arms
- **Equal mixes are safe** — matched weights make the total a plain average of the group results
- **Unequal arm sizes are fine** — 1,000 against 100,000 cannot flip a sign on its own
- **Random assignment helps** — but a small test can still draw a lopsided mix by chance
- **Not "always segment"** — splitting on a step the change itself caused invents a fresh reversal
- **The payment-page trap** — the better page pulled in browsers, then read worse among them
- **The one question** — was this variable already settled before the change touched anyone
- **Phone or laptop qualifies** — "reached the payment page" does not, the new page moved it

**Example line (italic):** Keep the new page's phone share under 25.6% and the total agrees with both screens; at the 80% it actually drew, the total calls it 4.9 points worse.

**Key point:** Segmenting is a judgement, not a reflex. Split on something already settled before the treatment and you remove a confound; split on something the treatment moved and you manufacture a fresh reversal in the other direction.

**Source note (`.src`):** Illustrative Example — both panels are computed from constructed rates in which the new page is genuinely better for every visitor type.

### Visualization — canvas `c4`, 720×340

Top: how the apparent verdict swings with the new arm's phone share, with the sign-flip point marked. Bottom: the mirror-image mistake — segmenting on a step the change itself moved. Both are computed in the draw function.

- **Top panel title (bold 15px `P.ink`, centered, y=22):** "The Mix Decides the Verdict"
- **Top panel construction:** segment rates held fixed at phone 2.5% old / 3.0% new and laptop 11.5% old / 12.0% new; the old arm is pinned at 20% phone so its total stays 9.7%. Sweep the new arm's phone share from 0% to 100% in 5-point steps, computing `gap = newTotal − 9.7` at each step.
- **Derived:** the gap runs from +2.30 points at 0% phone down to −6.70 at 100%; the zero crossing is at 25.6% phone, interpolated between the two straddling steps; the standardised gap is a constant +0.50 points at any shared mix, computed as the mean of the two segment gaps (both are +0.5).
- **Top plot box:** `PX0=76`, `PX1=w−34`, `PY0=52`, `PY1=196`; y from +3 down to −7 points; `P.grid` lines at +2/0/−2/−4/−6 with 12px `P.mute` labels; a darker `#999` zero line.
- **Top series:** the swept gap as a 2.5px `P.magenta` line; the constant honest gap as a 2px dashed `P.green` line (dash 5/4) labelled 12px right-aligned at `PX1−6`, 8px above the line — "the honest gap: +0.5 pt, at any shared mix"; a `P.orange` dot radius 5 at the crossing with a 12px `P.orange` label 18px *below* the zero line, "verdict flips here — 25.6% phone"; a `P.magenta` dot radius 6 at 80% phone with a bold 12px right-aligned label 18px below it, "the split this test actually drew". Both offsets are below their marks so neither lands on the green dashed line or the magenta curve.
- **Top axes:** 12px `P.mute` x-ticks at 0/20/40/60/80/100%, x title "how phone-heavy the new arm is (old arm pinned at 20%)"; rotated y title "what the total says, in points".
- **Bottom panel header (bold 12px `P.violet`, left at x=30, y=246):** "THE OPPOSITE MISTAKE — SEGMENTING ON A STEP THE CHANGE MOVED".
- **Bottom rows:** two dumbbells sharing a 0–50% axis from `AX0=200` to `w−150`, at y=268 and y=292; 12px `P.mute` row labels right-aligned at `AX0−10` ("all visitors", "reached payment only"); `P.mute` dot radius 5 for the old page and a coloured dot radius 5 for the new; the lower value labelled right-aligned at its dot `−9` and the higher left-aligned at `+9`, to one decimal; connector 3px `P.green` for the honest row and `P.magenta` for the reversed one; bold 12px verdict at `x=w−140` — "new better" (`P.green`) and "new looks worse" (`P.magenta`). 12px `P.mute` ticks at 0/25/50% on y=312.
- **Bottom construction:** 10,000 visitors, 30% keen and 70% browsing; the old page brings 90% of keen and 20% of browsing visitors to the payment page, the new page 95% and 60%; of those who arrive, keen buy 60% (old) or 62% (new), browsers 10% or 12%. The new page is strictly better for both types, yet the reached-payment row reverses — 42.9% down to 32.2% — because the new page's arrivals are only 40.4% keen against the old page's 65.9%.
- **Caption (bold 13px `P.ink`, centered, `h−8`):** "Split on a cause, never on a consequence."

---

## Regeneration instructions

- **Template:** the card-section layout from `tutorials/llms-generative-ai/08-positional-encoding.html`. One `.card-section` per section, each holding an `<h2>` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px bottom padding) and a `table.layout` with `td.text-col` 50% / `td.viz-col` 50%.
- **Canvas placement:** `td.viz-col` gets `text-align: center` and the canvas `display: block; width: 100%; margin: 0 auto`. Since the canvas is capped at 720px, a wide cell leaves slack — centering puts the chart in the middle of the right half instead of flush against the text.
- **Text column order:** `.tags` pill row → `<ul>` of one-line bullets each opening `<b>term</b>` → one italic `.example` line → one `.key-point` callout → optionally one `.src` note. No paragraph blocks, no `.philosophy` box, no data tables, no `.labels` strip, no `.subhead` line — the h2 and the tags carry that job.
- **Bullets:** 6–8 per section, each ONE line that does not wrap at 50% column width (~≤100 characters), opening with a `<b>bold term</b>` followed by an em dash and the fact.
- **Page CSS:** body system-ui, white, `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`, 8px bottom padding. `.subtitle` `#666` 0.95rem, 32px bottom margin. `.card-section` 40px bottom margin. `table.layout` full width, border-collapse, cells vertical-align top padding 12px. `ul` 0.92rem, margin `8px 0 8px 20px`, `li` 4px bottom margin, `li b` in `#1a5276`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.src` 0.78rem `#888`. No nav, no back/home links, no cross-page links.
- **Tag pills:** `display:inline-block`, 0.72rem, weight 600, padding 2px 10px, radius 10px; `.blue` `rgba(26,82,118,0.12)`/`#1a5276`, `.green` `rgba(39,174,96,0.15)`/`#27ae60`, `.red` `rgba(231,76,60,0.12)`/`#e74c3c`, `.orange` `rgba(230,126,34,0.15)`/`#e67e22`. These fills are on pills, not behind body text.
- **Canvas:** CSS `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px. Intrinsic `width="720"` plus the per-chart `height`. `setup(id)` caches the logical size in `dataset` on the first call (because `canvas.width` overwrites the attribute), sets `style.maxWidth = 720px`, computes `scale = (cssW/720) × devicePixelRatio`, sizes the backing store to `logical × scale`, and `ctx.scale(scale, scale)` back to logical coordinates. The logical coordinate system stays 720 wide at every window size, so no chart re-lays itself out. Draws registered in `__charts`, re-run on debounced (150ms) resize.
- **Canvas font sizes:** chart title bold 15px; in-chart section header bold 12–13px; body/axis labels 12px (floor); the single big callout figure bold 19px; caption bold 13px. Do not push titles to 17–19px.
- **No tables drawn on canvas.** The first version of this page rendered its numbers as a canvas table; that is banned — a reversal is a slope chart, a paired-bar comparison, or a set of dumbbell rows.
- **Palette** (shared `P` object): `blue #2a78d6`, `green #008300`, `magenta #d55181`, `yellow #c98500`, `aqua #199e70`, `orange #d95926`, `violet #4a3aa7`, `ink #1a5276`, `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef`. `P.green` is the honest view, `P.magenta` the misleading total, `P.orange` the mechanism that causes it.
- **Determinism:** no `Math.random()` anywhere. All four charts run from literal count arrays; rates, totals, deltas, standardised figures and the zero crossing are computed inside each draw function and printed from those variables, so a label can never drift from the plotted data.
- **Chart order in the document:** `c1`, `c2`, `c3`, `c4`, matching section order.
