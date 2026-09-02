# Data Collection Window Cost

**Page type:** detail page (backlog-style layout: intro callout, numbered h2 sections, mixed 2-col text/viz tables and full-width text sections)
**HTML title tag:** Data Collection Window Cost

**Subtitle:** In traditional software, mistakes cost compute time. In data work, mistakes cost calendar time you cannot recover.

**Intro callout (blue accent):** The asymmetric cost structure of data work: a software bug is fixed in hours or days, but a data-collection mistake burns weeks or months of calendar time that no amount of compute or headcount can buy back. Getting schema, instrumentation, and experimental design right the first time is the only way to stay in the game.

## 1. The Core Problem

**Traditional software: fast feedback**

- Write code → test → bug → fix → retest. Cycle time: minutes to hours.
- Deploy → issue → rollback → fix → redeploy. Cycle time: hours to days.
- Cost of mistake: compute time, engineering time. Both can be parallelized or accelerated.

**Data work: calendar-locked feedback**

- Design data collection → wait weeks/months → discover schema was wrong → redesign → wait weeks/months again.
- Start A/B test → wait for statistical significance → discover tracking bug → fix → restart test → wait again.
- Cost of mistake: **calendar time you cannot buy back**. No amount of compute or headcount recovers lost months.

**The asymmetry**

- **Software:** 10x more engineers → 3-5x faster delivery (diminishing returns, but real).
- **Data:** 10x more engineers → still waiting 3 months for data. The bottleneck is the calendar, not the team.

**Key-point callout (red accent):** **Implication:** Mistakes in data work have exponentially higher opportunity cost than mistakes in traditional software. Getting the collection schema, tracking instrumentation, and experimental design right THE FIRST TIME is not premature optimization — it's the only way to stay in the game.

### Visualization (canvas `c1`, 720×460)

Two horizontal stage timelines comparing mistake recovery in software vs data. Light gray plot background `#f9f9f9`.

- **Title (bold 18px sans-serif, `#1a5276`, top center):** "Mistake Recovery Timeline: Software vs Data".
- Plot margins: left 140, right 20, top 60, bottom 40; each row 80px tall, 40px gap between rows.
- **Row 1 label (bold 16px, `#27ae60`, left):** "Software Development". Stage bars scaled to 4 total days across the plot width; each bar has `#333` 1px border, white bold 14px stage label and 13px day count:
  - "Code" 1d, `#3498db`
  - "Bug found" 0.5d, `#e74c3c`
  - "Fix" 1d, `#27ae60`
  - "Retest" 0.5d, `#3498db`
  - "Ship" 1d, `#27ae60`
  - Right-aligned total (bold 16px `#27ae60`): "Total: 4 days".
- **Row 2 label (bold 16px, `#e67e22`, left):** "Data Collection". Stage bars scaled to 141 total days; narrow bars (<30px) draw their label rotated 90° in white bold 9px:
  - "Collect data" 60d, `#3498db`
  - "Wrong schema" 2d, `#e74c3c`
  - "Redesign" 5d, `#e67e22`
  - "Collect again" 60d, `#3498db`
  - "Build model" 14d, `#27ae60`
  - Right-aligned totals (bold 16px `#e74c3c`): "Total: 141 days" / "(4.7 months)".
- **Caption (italic 13px `#555`, bottom center):** "Software mistake: 4 days to recover. Data mistake: 4.7 months. 35× slower."

## 2. Specific Scenarios

**Wrong feature logged.** Logged user_id when you needed session_id; realized after 2 months of collection. Cannot use any of the collected data — must restart from scratch. If the business opportunity window was 6 months, you just burned 33% of it. Traditional software equivalent: a bug that takes 2 months to compile the fix. Doesn't exist.

**A/B test tracking bug.** Test variant assignments not logged correctly; discovered after 6 weeks when checking preliminary results. All 6 weeks of data unusable. Restart test, need another 6 weeks minimum — 12 weeks for a 6-week test. Competitor shipped their version in week 8. Organizational cost: stakeholders lose confidence, budget gets reallocated, project dies.

**Insufficient granularity.** Collected daily aggregates; later realized you need hourly breakdowns to detect fraud patterns. Raw data not retained, historical reconstruction impossible — must wait 3+ months to collect new data with hourly granularity. Competitor with hourly data ships fraud detection in 2 weeks; you're 3 months behind and haven't started model training yet.

**Wrong sampling strategy.** Sampled 1% of events to save storage; realized you need 100% coverage for rare-event detection (fraud, abuse, edge cases). Model trained on sampled data has poor recall on rare events. Full data wasn't saved — go back 4 months, start logging 100%, wait 4 more months for a training set. 8 months elapsed. Market moved on.

## 3. Why This Is Unique to Data Work

**Software: mistakes are local**

- A bug in feature X doesn't prevent you from shipping feature Y.
- A bad API design can be versioned (v1 stays, v2 ships).
- Rollbacks are cheap. Previous state still exists.

**Data: mistakes are global time sinks**

- Wrong data schema affects EVERYTHING downstream: all models, all analyses, all dashboards.
- No such thing as "data versioning" for past events. You cannot re-collect historical data with a different schema.
- Rollbacks don't exist. The data you didn't collect is gone forever.

**Software: iteration is fast**

- Ship v1 → learn → ship v2 in days/weeks. Fail fast, iterate, improve.

**Data: iteration is measured in months**

- Collect v1 → wait 2 months → learn it's wrong → collect v2 → wait 2 more months.
- "Fail fast" is not an option. Every failure costs months.

### Visualization (canvas `c2`, 720×460)

Horizontal bar chart comparing parallelizability. Light gray plot background `#f9f9f9`.

- **Title (bold 18px sans-serif, `#1a5276`, top center):** "What You Can Parallelize".
- Margins: left 180, right 40, top 60, bottom 30; bar height 45px, gap 20px. Four bars (value = fraction of max width), each with `#333` 1px border, right-aligned 15px `#1a5276` label to the left, and italic 11px `#333` note just right of the bar end:
  - "Software: Add engineers" — 0.6, `#27ae60`, note "3-5× faster with 10× engineers"
  - "Software: Add compute" — 0.8, `#27ae60`, note "Near-linear speedup possible"
  - "Data: Add engineers" — 0.15, `#e74c3c`, note "Still waiting months for data"
  - "Data: Add compute" — 0.12, `#e74c3c`, note "Cannot buy back calendar time"
- **Divider:** dashed (5/5) gray `#999` width-2 horizontal line between the software and data pairs, spanning x=20 to w−20.
- **Caption (bold 16px `#555`, bottom center):** "The calendar is the bottleneck. Money and headcount cannot fix it."

## 4. Organizational Consequences

- **Stakeholder patience:** Business expects software-speed iteration. Data teams operate on data-speed iteration. Mismatch kills projects.
- **Budget attrition:** "We've been working on this for 5 months and still don't have a model." Budget gets reallocated to faster-moving teams.
- **Talent retention:** Engineers leave because "progress is too slow." They don't realize the bottleneck is the calendar, not the team.
- **Competitive loss:** Competitor gets the data collection right on first try. They ship in 3 months. You're on iteration 2 of data collection at month 6 and haven't started modeling yet. Game over.

## 5. What Makes a Mistake "Costly"

A data collection mistake is costly when:

- **Non-recoverable:** Cannot reconstruct the data you failed to collect (events are gone, users moved on, competitors already captured the market).
- **Blocking:** Everything downstream (models, analyses, dashboards, business decisions) is blocked until new data is collected.
- **Long-tailed:** Minimum viable sample size requires weeks/months, not hours/days.
- **One-shot:** The business opportunity window is finite. Miss it and the project dies (seasonal product, time-sensitive campaign, competitive response window).

## 6. Mitigation Strategies

**Over-collect early**

- Log more than you think you need. Storage is cheap. Calendar time is not.
- Retain raw events, not just aggregates. You can always aggregate later; you cannot disaggregate.

**Schema review as a first-class gate**

- Treat data schema design like you treat security review: mandatory, blocking, multi-reviewer.
- Senior data scientists + domain experts review every schema BEFORE collection starts.
- Ask: "What questions will we wish we could answer 3 months from now?"

**Dry-run on sample data**

- Before full rollout, collect 1 week of data. Run actual analysis/model training on it.
- Discover schema issues in 1 week instead of 3 months.

**Parallel collection hedges**

- If uncertain between schema A and schema B, collect both for first 2 weeks.
- Cost: 2x storage for 2 weeks. Benefit: avoid 8-week redo if you picked wrong.

**Real-time validation checks**

- Automated checks that data is arriving, schema matches expectations, no nulls in required fields, distributions look sane.
- Catch errors in hours, not months.

### Visualization (canvas `c3`, 720×460)

Two scenario timelines showing the ROI of upfront review. Light gray plot background `#f9f9f9`.

- **Title (bold 18px sans-serif, `#1a5276`, top center):** "Mistake Prevention ROI".
- Margins: left 60, right 60, top 60, bottom 80; x scale = 21 weeks across the plot width; stage bars 50px tall with `#333` borders, white bold 14px stage labels and 13px week counts.
- **Scenario 1 (label bold 16px `#1a5276`): "No upfront review"** — stages:
  - "Collect (wrong)" 8w, `#e74c3c`
  - "Discover" 1w, `#e67e22`
  - "Redesign" 2w, `#e67e22`
  - "Collect (right)" 8w, `#3498db`
  - "Model" 2w, `#27ae60`
  - Total annotation after the bar (bold 16px `#e74c3c`): "Total: 21 weeks" / "(5.3 months)".
- **Scenario 2 (label bold 16px `#1a5276`): "1-week upfront review"** — stages:
  - "Review" 1w, `#9b59b6`
  - "Collect (right)" 8w, `#3498db`
  - "Model" 2w, `#27ae60`
  - Total annotation (bold 16px `#27ae60`): "Total: 11 weeks" / "(2.8 months)".
- **Savings arrow:** green `#27ae60` width-3 leftward arrow between the two rows (from week 21 back to week 11) with filled triangular head; bold 16px green labels: "Save 10 weeks" above, "(2.5 months)" below.
- **Captions (italic 13px `#555`, bottom center, two lines):** "1 week of upfront review saves 10 weeks of rework. 10:1 ROI." / "In data work, prevention is the ONLY cost-effective strategy."

## Regeneration instructions

- **Layout:** backlog detail page. h1 with bottom border `2px solid #2980b9`, `.subtitle` paragraph, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem). One `.lang-section` per numbered section, each with an h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`). Sections 1, 3, and 6 use `table.layout` (full width) with one `<tr>`: left `td.text-col` (45%) text, right `td.viz-col` (55%) canvas. Sections 2, 4, 5 are full-width text (paragraphs or bullet list) with no canvas.
- **Callout styles:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `strong` in default color (no override on this page); ul 0.92rem with 20px left margin. Canvases styled `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width="720" height="460"`; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes logical size from the attributes, and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; secondary blue `#3498db` and purple `#9b59b6` for timeline stages; chart backgrounds `#f9f9f9`; gray text `#555`/`#333`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
