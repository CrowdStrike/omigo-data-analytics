# Devin & the Autonomous-Engineer Claim

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Devin & the Autonomous-Engineer Claim

**Subtitle:** A tool that aces its own demos can still fail most real tasks — the gap between demo success and dependable-in-production is an evaluation lesson, not a scandal

## The First AI Software Engineer

**Tags:** `core idea` (blue), `demo vs base rate` (orange), `2024` (green)

- **The launch** — in March 2024, startup Cognition unveiled Devin as "the first AI software engineer"
- **The demos** — launch videos showed Devin planning, coding, debugging, and deploying tasks end to end
- **The benchmark** — Cognition reported 13.86% of SWE-bench issues resolved unassisted, vs 1.96% prior best
- **The reaction** — headlines split between "engineers are obsolete" and "it's all smoke"
- **The real question** — not "can it ever succeed?" but "how often, on tasks it didn't pick?"

*Example (italic):* 13.86% was a genuine 7× jump over the prior state of the art — and still meant roughly 86% of benchmark issues went unresolved.

**Key point:** A capability demo answers "is this possible?"; dependability requires a rate — successes divided by a task set chosen before you saw the results.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart: SWE-bench unassisted issue-resolution rates, prior best vs Devin as reported at launch, with the unresolved remainder called out.

- **Title (bold 15px, `#1a5276`, top center):** "SWE-bench Resolved Unassisted: a Real Jump, Not a Solved Problem".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = 0 to 16% with gridlines `#e5e9ef` at 4/8/12 and 12px `#444` tick labels; no x gridlines.
- **Bar 1 (prior best):** blue `#2a78d6` fill `rgba(42,120,214,0.30)` with 2px solid edge, x=170, width 120, value 1.96% (height 22px); bold 13px `#2a78d6` value label "1.96%" above the bar; 12px `#444` label "prior best" below baseline.
- **Bar 2 (Devin):** green `#008300` fill `rgba(0,131,0,0.30)` with 2px solid edge, x=430, width 120, value 13.86% (height 156px); bold 13px `#008300` value label "13.86%" above; 12px `#444` label "Devin (reported)" below baseline.
- **Reference line:** dashed `#6b7280` (dash 4/3) horizontal line at y for 13.86%, extending left to the y-axis.
- **Annotation (bold 13px orange `#d95926`, near x=250, y=95):** "7× the prior best — and ~86% of issues still unresolved".
- **Caption (12px `#444`, bottom right):** "percentages as reported by Cognition, March 2024".

## Twenty Tasks, Three Finishes

**Tags:** `worked example` (blue), `independent review` (green)

- **The reviewers** — in early 2025 an independent team published a month-long hands-on review of Devin
- **The protocol** — they gave it about 20 real tasks from their own work, chosen before seeing results
- **The tally** — 3 tasks completed end to end, 14 failed, 3 inconclusive, as reported
- **The rate** — 3 of 20 is a 15% end-to-end completion rate on their representative set
- **Hand-check** — 3 + 14 + 3 = 20; success rate 3/20 = 0.15, exactly the 15% quoted
- **Both true at once** — the polished demos and the 15% rate describe the same tool

*Example (italic):* The same product that looked flawless in a launch video finished 3 of the reviewers' 20 everyday tasks — both observations are accurate.

**Key point:** A fixed task list written down before testing turns "it worked when I tried it" into a rate you can compare, budget around, and re-measure later.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked bars on a shared task-count scale: a curated demo reel (all successes) vs the independent 20-task review (3 / 14 / 3 split).

- **Title (bold 15px, `#1a5276`, top center):** "Curated Reel vs Fixed Task List: Same Tool, Different Denominators".
- **Layout:** bars start at x=200, scale 23px per task, bar height 34px; row labels 12px `#444` right-aligned at x=190.
- **Row 1 (y=105), label "launch demos (curated)":** single green `#008300` fill `rgba(0,131,0,0.30)` segment, 5 tasks wide (115px), 2px solid edge; bold 12px green label "5 of 5 shown succeed" at the segment's right end; 11px `#6b7280` note "illustrative" just below the bar.
- **Row 2 (y=195), label "independent review, 20 tasks":** three segments left to right — green `rgba(0,131,0,0.30)` 3 tasks (69px) labeled "3 done" (bold 12px `#008300` above), red `rgba(231,76,60,0.20)` with 2px `#e74c3c` edge, 14 tasks (322px) labeled "14 failed" (bold 12px `#e74c3c` above), gray `rgba(107,114,128,0.20)` 3 tasks (69px) labeled "3 unclear" (12px `#6b7280` above).
- **Bracket:** thin `#6b7280` bracket under row 2's green segment with 12px label "15% end-to-end".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=260):** "the reel samples successes; the list samples tasks".
- **Caption (12px `#444`, bottom right):** "row 1 illustrative; row 2 counts as reported by the reviewers, Jan 2025".

## Demos Are a Biased Sample

**Tags:** `where it's used` (blue), `selection bias` (red), `evaluation` (green)

- **The mechanism** — a demo reel is sampled from successes, so its success rate is near 100% by construction
- **The DS parallel** — it is the same error as judging a model on hand-picked examples instead of a held-out set
- **The pattern** — demo-to-field gaps show up across tools; the size of the gap is the number that matters
- **The fix** — write the task list first, run every task, report the full tally including failures
- **The payoff** — a measured 15% still has uses (drafts, scaffolding) if you plan for a 15% tool, not a 100% one

*Example (italic):* A data scientist who ships a churn model after eyeballing ten flattering predictions is watching a demo reel of their own model.

**Key point:** Cherry-picked examples estimate the ceiling; a pre-committed representative task set estimates the base rate — decisions should ride on the base rate.

### Visualization (canvas `c3`, 720×300)

Dumbbell chart: four tools' success rate in their demo reel vs on a fixed representative task set; three rows illustrative, one row the reported Devin review.

- **Title (bold 15px, `#1a5276`, top center):** "Demo Reel Rate vs Fixed-Task-Set Rate".
- **Axis:** horizontal scale 0–100% mapped to x=230..670 (440px); 2px `#999` baseline at y=250 with 12px `#444` tick labels at 0/25/50/75/100%; light `#e5e9ef` vertical gridlines at those ticks.
- **Rows (y = 80, 125, 170, 215), each: 12px `#444` left label at x=20, a 2px `#c9ced6` connector line between the two dots, dots radius 7:**
  - "code autocomplete (illustrative)": demo dot blue `#2a78d6` at 100%, field dot aqua `#199e70` at 62%
  - "SQL chatbot (illustrative)": demo 100%, field 48%
  - "autoML pipeline (illustrative)": demo 100%, field 35%
  - "Devin review (reported)": demo dot at 100%, field dot orange `#d95926` at 15%, bold 12px `#d95926` label "15%" beside it
- **Value labels:** 11px `#6b7280` percentages next to each field dot (62 / 48 / 35 / 15).
- **Legend (12px, top right, y=55):** blue dot "demo reel", aqua dot "fixed task set".
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=245 area above baseline):** "the gap is the rule, not the exception".
- **Caption (12px `#444`, bottom right):** "first three rows illustrative; Devin row 3/20 as reported".

## Neither Miracle Nor Fraud

**Tags:** `common mistake` (red), `even-handed read` (orange)

- **Mistake one** — "the demos were great, so it's ready": generalizing from a curated sample
- **Mistake two** — "it failed my task, so it's useless": generalizing from a sample of one
- **What held up** — the SWE-bench jump was real progress; independent reviewers said so too
- **What didn't** — "autonomous engineer" implied a near-100% rate the measured 15% didn't support
- **The habit** — ask every vendor (and yourself): "what's the rate, on whose task list, chosen when?"

*Example (italic):* One reviewer's verdict was roughly "impressive when it works, but I can't predict when that is" — a statement about variance, not just the mean.

**Common mistake:** Treating a capability demo as a reliability claim. Demos prove the ceiling exists; only a pre-registered representative task set tells you how often you'll reach it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: evaluating from the demo reel (surprise in production) vs evaluating on a fixed task list (calibrated adoption), shown as decision boxes.

- **Title (bold 15px, `#1a5276`, top center):** "Two Evaluation Paths for the Same Tool".
- **Row 1 (y=95), label 12px `#444` at x=20:** "demo-based"; blue `#2a78d6` rounded box at x=140 labeled "watch 5 curated demos" (12px), 3px arrow to a blue box at x=350 labeled "conclude ~100% reliable", 3px arrow to a red `#e74c3c` box at x=560 labeled "production surprise" with bold 12px red "✗ unbudgeted failures".
- **Row 2 (y=205), label:** "base-rate"; green `#008300` rounded box at x=140 labeled "fix 20 tasks up front", arrow to a green box at x=350 labeled "measure 3/20 = 15%", arrow to a green box at x=560 labeled "adopt for what 15% covers" with bold 12px green "✓ no surprise".
- **Box style:** 150–175px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, wrapped to two lines where needed.
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "same tool, same 15% — only the second path knew it in advance".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness). Reported figures — SWE-bench 13.86% vs 1.96% (Cognition, March 2024) and the 3 done / 14 failed / 3 unclear split of ~20 review tasks (independent review, Jan 2025) — are labeled "as reported"; the demo-reel 5/5 and the 62/48/35 dumbbell rows are invented and labeled "illustrative"; 3/20 = 15% is exact arithmetic. Text numbers must match chart numbers everywhere.
- **Tone:** even-handed — the page credits the genuine benchmark jump and criticizes only the inference from demos to dependability; it is an evaluation lesson, not a takedown.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
