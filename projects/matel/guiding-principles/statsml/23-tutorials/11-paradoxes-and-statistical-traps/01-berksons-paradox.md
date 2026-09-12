# Berkson's Paradox

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Berkson's Paradox

**Subtitle:** Keep only the rows that cleared a bar built from two traits, and the bar paints a negative correlation between them that never existed in the full population

## The Loan Book That Invented a Trade-off

**Tags:** `core idea` (blue), `selection effect` (orange), `collider` (green)

- **The scorecard** — a lender scores 40 applicants on income and on credit history, each out of 100
- **The cutoff** — the two scores are added and anyone reaching 130 points is approved
- **The applicant pool** — across all 40 applicants the two scores are unrelated: r = 0.00
- **The loan book** — among the 14 approved, higher income now means worse credit: r = −0.65
- **Nobody traded anything** — no borrower swapped income for credit history; the cutoff did it

*Example (italic):* The risk analyst opens the loan book, sees that high earners carry the patchy credit files, and writes up "wealthy borrowers are careless about repayment."

**Key point:** When a table exists because its rows cleared a bar built from two traits, the bar manufactures a negative link between them. That is Berkson's paradox — the correlation your sample created.

### Visualization (canvas `c1`, 720×300)

Single scatter of all 40 applicants with the diagonal approval cutoff; approved points above the line, declined below, legend and annotations in the empty left margin.

- **Title (bold 15px, `#1a5276`, top center):** "40 Applicants: Approve if Income + Credit ≥ 130".
- **Data (fixed arrays, index-aligned):** income `[61,87,13,89,72,53,49,32,48,30,83,91,30,18,56,29,25,28,45,84,95,82,18,43,87,49,46,84,59,49,45,50,45,60,94,94,52,60,95,26]`; credit `[93,33,17,84,81,24,81,56,17,84,52,27,42,93,54,22,47,57,88,49,52,49,93,17,96,26,63,36,86,91,39,40,32,60,32,54,36,52,54,38]`. A point is approved when income + credit ≥ 130 (14 points qualify).
- **Plot area:** origin x=230, width 320, baseline y=250, chart height 195 (top y=55); x scale 0–100 (income), y scale 0–100 (credit); axes 1.5px ink `#1a5276`; axis titles 12px `#444`: "income score" below center, "credit score" rotated left.
- **Cutoff line:** dashed (dash 5/4) magenta `#d55181` 2px from point (34, 96) to (96, 34); bold 12px magenta label "income + credit = 130" along its upper side.
- **Points:** approved = green `#008300` filled 5px dots; declined = mute `#6b7280` hollow 4px circles (1.5px stroke).
- **Legend (x=40, from y=70, 12px):** green dot "approved — the loan book", hollow gray circle "declined — never scored again".
- **Annotations (left margin, x=40):** magenta bold 13px two lines at y=132 "only the green dots" / "reach your data warehouse"; gray 12px at y=172 "whole pool r = 0.00 (no link)"; magenta bold 12px at y=192 "loan book r = −0.65".
- **Caption (12px `#444`, bottom left):** "illustrative applicant pool; both scores out of 100".

## Eight Applications You Can Check by Hand

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **Eight applicants** — A(85,45), B(90,75), C(55,90), D(40,75), E(35,45), F(75,55), G(70,70), H(50,55)
- **Add and decide** — B scores 90 + 75 = 165 and is approved; E scores 35 + 45 = 80 and is declined
- **On the line** — A (85+45) and F (75+55) both land exactly on 130, the weakest approvals
- **Five in the book** — A, B, C, F, G clear the bar; D, E, H are declined and leave no trace
- **Two correlations** — all eight applicants: r = 0.00; the five approved alone: r = −0.60

*Example (italic):* B is the strongest file on both counts (90, 75), yet inside the loan book B looks like a mediocre credit risk sitting next to C's near-perfect 90.

**Key point:** Inside the approved group each extra income point looks "paid for" with credit points — strong-on-both cleared the bar from any angle, weak-on-both is gone, and mostly the trade-off diagonal is left.

### Visualization (canvas `c2`, 720×300)

Dual-panel scatter: all eight lettered applicants (left) vs the five approved with their downward trend line (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same Eight Applicants, Two Correlations".
- **Data:** letters `["A","B","C","D","E","F","G","H"]`; income `[85,90,55,40,35,75,70,50]`; credit `[45,75,90,75,45,55,70,55]`; approved = A, B, C, F, G (sum ≥ 130); declined = D, E, H.
- **Left panel (all eight):** origin x=60, width 260, baseline y=245, chart height 185; x and y scales 0–100; approved as blue `#2a78d6` filled 6px dots with bold 12px blue letters offset 10px above-right; D, E, H as mute `#6b7280` hollow 6px circles with gray letters; dashed light magenta cutoff line (dash 4/3, 1.5px, `rgba(213,81,129,0.5)`) from (40,90) to (90,40) so the reader sees which side each letter is on; caption 12px `#444` below "all 8 applicants: r = 0.00".
- **Right panel (approved only):** origin x=405, width 260, same baseline/height and scales; only A, B, C, F, G as green `#008300` filled 6px dots with green letters; magenta `#d55181` dashed 2px trend line from (52, 88) to (92, 50); green bold 13px annotation near top "the loan book: r = −0.60"; caption "D, E, H removed".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Berkson's Own 1946 Hospital Charts

**Tags:** `where it's used` (blue), `sampling` (orange), `failure mode` (red)

- **The finding** — hospital charts suggested diabetes protected patients against gallbladder disease
- **The town** — of 10,000 residents 5% have diabetes and 10% gallbladder disease, unrelated
- **Two routes in** — either illness can put you on the ward, so it collects 937 of those 10,000
- **In town** — gallbladder patients and everyone else carry diabetes at the same 5% rate
- **On the ward** — patients without gallbladder disease are there for diabetes: 21% versus 6%

*Example (italic):* A ward patient with no gallbladder trouble needed some other reason to be admitted, and diabetes is the reason on offer — so the two illnesses look mutually exclusive on the chart rack.

**Key point:** Any dataset that exists because its rows cleared a bar — approved, hired, funded, reviewed, admitted, survived — carries built-in correlations that describe the bar, not the world.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: share of people with diabetes, split by gallbladder-disease status, shown for the whole town (left group) and for ward patients only (right group).

- **Title (bold 15px, `#1a5276`, top center):** "Diabetes Rate by Gallbladder Status: Town vs Ward (illustrative)".
- **Data:** town of 10,000 splits into both 50, gallbladder-only 950, diabetes-only 450, neither 8,550. Admission rates 60% / 50% / 20% / 4% give a ward of both 30, gallbladder-only 475, diabetes-only 90, neither 342 — total 937. Town bars: 5.0% (has gallbladder = 50/1,000) and 5.0% (no gallbladder = 450/9,000). Ward bars: 5.9% (has gallbladder = 30/505) and 20.8% (no gallbladder = 90/432).
- **Axis:** origin x=80, baseline y=232, chart height 168, y scale 0–25%; y ticks 0/5/10/15/20/25 with gridlines `#e5e9ef`; axis lines 1.5px ink `#1a5276`; y-axis title 12px `#444` "% with diabetes".
- **Bars:** 56px wide; town pair at x=145 and x=215 filled `rgba(42,120,214,0.45)` with 2px `#2a78d6` top edge; ward pair at x=420 and x=490 filled `rgba(217,89,38,0.5)` with 2px `#d95926` top edge; bold 13px value labels above each bar: "5.0%", "5.0%", "5.9%", "20.8%"; 12px `#444` labels below each bar "has gallbladder" / "no gallbladder" (11.5px so they do not collide); group headings bold 13px `#444` at y=278: "whole town (10,000)" and "ward patients (937)".
- **Annotations:** green bold 12px under the town pair at y=262 "same rate — no link"; magenta bold 12px under the ward pair at y=262 "3.5x gap — pure artifact"; magenta bold 12px two lines right of the tall bar at x=556, y=100/118: "the ward's own" / "admission rule did this".

## It's Not a Confounder — It's Your Filter

**Tags:** `common mistake` (red), `the fix` (green)

- **Wrong reflex** — analysts hunt for a hidden third factor driving both traits; here there is none
- **The direction** — a confounder causes both traits; here both traits cause entry into the sample
- **The name** — "being in the table" is a collider: arrows from both traits point into it
- **The test** — ask whether either trait alone could raise a row's chance of entering this table
- **The fix has a name** — lenders call it reject inference: model the declined rows back in

*Example (italic):* Restore the 26 declined applicants to the scatter and the income-versus-credit trade-off vanishes, r sliding from −0.65 back to 0.00.

**Common mistake:** Filtering on a collider — restricting analysis to the approved, the hired, the admitted, or the surviving — creates bias rather than removing it. That filter is not "cleaning" the data; it is the source of the correlation.

### Visualization (canvas `c4`, 720×300)

Left half: a three-node collider diagram (two traits pointing into "approved — in your table"). Right half: two horizontal correlation bars around a zero line, comparing loan-book vs whole pool.

- **Title (bold 15px, `#1a5276`, top center):** "Filtering on a Collider Creates the Correlation".
- **Diagram (left half):** rounded boxes 120×34, 1.5px ink `#1a5276` border, bold 13px labels: "income" at (46, 70), "credit" at (46, 170); box "in your table" 150×38 filled `rgba(213,81,129,0.12)` with 2px magenta `#d55181` border at (226, 118); solid 2px ink arrows from the right edge of each trait box to the left edge of the table box (arrowheads 7px); crossed-out dashed gray line between the two trait boxes labeled "no real link" (11.5px `#6b7280`) to show the absence explicitly; magenta bold 12px caption under the collider box at y=176 "a collider — do not filter on it".
- **Correlation bars (right half):** center zero line at x=520, vertical 1.5px `#999` from y=70 to y=210; r scale −1 to +1 mapped at 140px per unit of r; tick labels 11px `#6b7280` "−1", "0", "+1" below at y=225.
- **Bar 1 (loan book only):** at y=100, 22px tall, from x=520 leftward to r = −0.65 (x=429), fill `rgba(213,81,129,0.55)` with 2px `#d55181` edge; bold 12px magenta label above "loan book only: r = −0.65".
- **Bar 2 (whole pool):** at y=160, 22px tall, drawn as a 3px flat marker at x=520 since r = 0.00, with 2px `#008300` edge; bold 12px green label above "all 40 applicants: r = 0.00".
- **Takeaway (bold 13px green `#008300`, centered at y=272):** "the fix is upstream: recover or model the rows the filter deleted".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Verified arithmetic — reuse these exact arrays.** Every r quoted in the text is the true Pearson correlation of the stated data: 40-applicant pool r = 0.000 and its 14 approved rows r = −0.647 (→ −0.65); the eight hand applicants r = 0.000 and their five approved rows r = −0.603 (→ −0.60). The hospital cells are exact integers: town 50 / 950 / 450 / 8,550 with diabetes at 5.0% on both sides, ward 30 / 475 / 90 / 342 (total 937) with diabetes at 5.9% versus 20.8%, a ward odds ratio of 0.24 from genuinely independent illnesses.
- **Why these examples:** underwriting scorecards really do add factor points against a cutoff, so the "sum ≥ bar" rule is the actual selection mechanism rather than a teaching contrivance, and the industry's own remedy (reject inference) makes the fix section concrete. The hospital section carries Berkson's original 1946 diabetes/gallbladder finding with real admission logic instead of anonymous "illness A / illness B". Correlations are kept near −0.6, not −0.99, so the reader does not learn that selection produces near-perfect anti-correlation.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
