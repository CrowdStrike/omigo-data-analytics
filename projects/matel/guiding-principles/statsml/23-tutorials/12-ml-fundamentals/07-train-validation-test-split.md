# Train / Validation / Test Split

**Page type:** detail page (tutorial: 4 card-sections, each h2 + two-column layout table, text left 50% / canvas right 50%)
**HTML title tag:** Train / Validation / Test Split

**Subtitle:** Split the data into study material, a practice exam, and a final exam taken once — because grading a model on what it studied inflates the score

## 10,000 Emails, Three Piles

**Tags:** `core idea` (blue), `running example` (green)

- **One dataset** — 10,000 emails, each already labeled spam or not spam
- **Train (7,000)** — the study material; the model learns its rules from these only
- **Validation (1,500)** — the practice exam; used to compare versions and tune settings
- **Test (1,500)** — the final exam; locked away and graded exactly once, at the end
- **No overlap** — each email lives in one pile; the exams contain nothing studied

*Example:* A student who grades themselves on the textbook's solved problems looks brilliant — until the real exam.

**Key point:** **The split exists for one reason:** the only score that matters is on emails the model has never seen. Everything else is grading on the study material.

### Visualization (canvas `c1`, 720×300)

Single horizontal segmented bar showing 10,000 emails split into three labeled piles.

- **Title (bold 16px, `#1a5276`, top center):** "10,000 Labeled Emails, Split 70 / 15 / 15".
- **Bar:** starts at x=60, y=95, height 74, total width 600; segment widths proportional to counts (n/10000 × 600), each drawn 3px short for a gap.
- **Segments:**
  - TRAIN: n=7000, label "TRAIN — 7,000" (bold 13px), role caption "study material", blue `#2a78d6`, fill alpha 0.32, 2px stroke.
  - VALIDATION: n=1500, label "VALIDATION" plus "1,500" below it, role "practice exam", orange `#d95926`, same fill/stroke style.
  - TEST: n=1500, label "TEST" plus "1,500", role "final exam", green `#008300`, same style.
- Role captions in text color `#2c3e50` 12px, centered 20px below the bar.
- **Annotations:**
  - Blue 13px centered inside train segment area (x=60+210, y within bar): "the model reads these — and only these".
  - Green bold 13px above the test segment (near right end, 12px above bar): "graded once, at the very end".
  - Magenta `#d55181` bold 13px bottom center (30px from bottom): "no email appears in two piles — the exams contain nothing the model studied".

## Grading the Same Model Three Times

**Tags:** `worked example` (green), `arithmetic` (blue)

- **On train** — 6,930 of 7,000 studied emails right: 6,930 / 7,000 = 99%
- **On validation** — 1,395 of 1,500 practice emails right: 1,395 / 1,500 = 93%
- **On test** — 1,380 of 1,500 final-exam emails right: 1,380 / 1,500 = 92%
- **The 99% is inflated** — the model partly memorized quirks of the emails it studied
- **92% is the honest number** — that is what to expect on tomorrow's inbox

*Example:* Same model, same day: 99% on the pile it studied, 92% on emails it had never seen.

**Key point:** **Hand-checkable:** each score is just right ÷ total. The 7-point drop from 99% to 92% is not a bug — it is the memorization bonus being removed.

### Visualization (canvas `c2`, 720×300)

Three-bar chart of the same model graded on train / validation / test.

- **Title (bold 16px, `#1a5276`, top center):** "One Model, Three Grades".
- **Bars:** values `[99, 93, 92]` percent; labels "train  6,930 / 7,000", "validation  1,395 / 1,500", "test  1,380 / 1,500"; colors blue `#2a78d6`, orange `#d95926`, green `#008300`; fill alpha 0.45 with 2px solid stroke; bar width 130, gap 62, first bar at x=105.
- **Axes:** y-axis from 80% to 100%, gridline labels at 80/85/90/95/100% (gray `#6b7280`, 12px, right-aligned at x=62); baseline y=240, chart height 165; L-shaped axis in `#999`.
- **Value labels:** bold 14px in each bar's color, above bar; category labels 12px `#2c3e50` below baseline.
- **Gap bracket:** magenta `#d55181` dashed lines (dash 5/4, width 1.5) extending right from the tops of the train bar (99%) and test bar (92%) to x≈w−55, joined by a solid vertical connector at x=w−60.
- **Annotation (magenta bold 13px, bottom center, 44px below baseline):** "7 points of the 99% were grading on the study material".

## Why the Final Exam Is Opened Only Once

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Eight versions** — you build 8 spam filters; each gets a validation score
- **Pick by practice exam** — version 7 scores highest on validation: 93.0%
- **One final grade** — version 7 alone takes the test and scores 92.0%
- **Slightly lower is normal** — picking the validation winner flatters it a little
- **Reuse the test and it dies** — a test peeked at repeatedly becomes a second practice exam

*Example:* The choosing happened on validation; the test set only certified the winner.

**Key point:** **Rule of thumb:** validation is for choosing, test is for reporting. Any number used to make a decision stops being an honest final grade.

### Visualization (canvas `c3`, 720×300)

Line chart of 8 model versions' validation scores, winner marked, plus a single test-score diamond.

- **Title (bold 16px, `#1a5276`, top center):** "Choose on Validation, Certify on Test".
- **Data:** validation scores v1–v8: `[88.0, 90.2, 89.1, 91.4, 92.6, 92.1, 93.0, 92.4]`.
- **Axes:** y from 87% to 94%, labels at every 1% (gray 12px right-aligned); x points spread with 40px inset each side; padding top 52, bottom 55, left 70, right 40; L-shaped `#999` axis; x tick labels "v1"…"v8" 12px below axis.
- **Series:** connecting line orange `#d95926` width 3; dots orange radius 4.5, except v7 (index 6) drawn green `#008300` radius 7.
- **Winner annotation:** green bold 13px near v7, offset left/up: "winner: v7 at 93.0% on validation".
- **Test marker:** violet `#4a3aa7` filled diamond (8px half-diagonals) at x = X(v7)+26, y = Y(92.0), with bold 13px left-aligned label "test, opened once: 92.0%".
- **Caption (gray 12px, bottom center):** "spam filter versions, in the order they were tried (illustrative scores)".

## The Confusion: Tuning Against the Test Set

**Tags:** `common mistake` (red), `caution` (orange)

- **The temptation** — check the test score after every tweak, keep tweaks that raise it
- **What you see** — the reported test score climbs from 91% to 95% over 8 peeks
- **What is real** — on genuinely new emails the model still scores about 91%
- **The 4 points are fiction** — you tuned to the quirks of those 1,500 test emails
- **The fix** — do all picking on validation; the test answers one question, once

*Example:* A team reported 95%; the deployed filter ran at 91% — the gap was eight peeks at the final exam.

**Key point:** **The confusion:** peeking does not improve the model, it only improves the score on that one pile — the test set quietly turns into training data for your decisions.

### Visualization (canvas `c4`, 720×300)

Two-line chart: reported test score drifts up with peeks while real accuracy stays flat, with shaded gap.

- **Title (bold 16px, `#1a5276`, top center):** "Eight Peeks at the Final Exam".
- **Data (9 points, x = 0..8 peeks):**
  - reported: `[91.0, 91.8, 92.5, 93.1, 93.6, 94.1, 94.5, 94.8, 95.0]` — magenta `#d55181`, solid, width 3.
  - real: `[91.0, 91.0, 91.1, 91.0, 90.9, 91.0, 91.0, 90.9, 91.0]` — blue `#2a78d6`, dashed 7/5, width 3.
- **Shaded gap:** region between the two lines filled `rgba(213,81,129,0.15)`.
- **Axes:** y from 89% to 96%, labels every 1% (gray 12px); x ticks 0–8; padding top 52, bottom 55, left 70, right 175; L-shaped `#999` axis.
- **Annotation (magenta bold 13px, mid-chart):** "the 4-point gap is fiction — tuned to those 1,500 emails".
- **Legend (right side, x=w−165):** magenta swatch "reported test score"; blue swatch "score on truly new" / "emails (illustrative)" (two lines, 12px).
- **X-axis caption (gray 12px, bottom center):** "number of times the test set was used to pick a tweak".

## Regeneration instructions

- **Template:** tutorials topic page (per `tutorials/CLAUDE.md`, social-graph reference skeleton). `<h1>` (no index number), `.subtitle` line, then 4 `.card-section` blocks, each an `<h2>` with 2px `#2980b9` bottom border plus a `table.layout` row: left `td.text-col` (50%) with `.tags` pills, 5 one-line bullets (each opening with `<b>` in `#1a5276`), an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) with one canvas 720×300.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; h2 1.3rem `#1a5276`. `.key-point`: background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example`: italic `#555` 0.9rem. ul 0.92rem. Canvas: `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas scaling:** shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; intrinsic width/height attributes as given per chart. All data arrays hardcoded (no `Math.random()`). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- No nav bar, no back/home links, no cross-page links. In regenerated HTML, any card links would use `.html` extensions.
