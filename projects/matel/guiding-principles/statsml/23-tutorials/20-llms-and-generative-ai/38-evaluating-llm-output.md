# Evaluating LLM Output

**Page type:** detail page (tutorial layout: `.card-section` blocks, each an h2 + two-column `table.layout`, text left 50%, canvas right 50%)
**HTML title tag:** Evaluating LLM Output

**Subtitle:** "It looks good" is an opinion — a rubric scored on a sample turns it into a number you can track

## 100 Ticket Summaries That All "Looked Good"

**Tags:** `core idea` (blue), `running example` (green)

- **The setup** — an LLM writes summaries for 100 support tickets; the team skims a few
- **The verdict** — "looks good, ship it" — nobody compared a summary to its ticket
- **A rubric** — three yes/no questions per summary: accurate? complete? concise?
- **Score a sample** — 20 summaries, read next to their tickets, graded by hand
- **The surprise** — accurate 85%, concise 90%, but complete only 65%

*Example:* Summary #11 read beautifully — and silently dropped the customer's refund deadline.

**Key point:** Skimming judges how output reads. A rubric judges whether it did the job — and those are different questions.

### Visualization (canvas `c1`, 720×300)

Vertical bar chart of rubric pass rates.

- **Title (bold 15px, `#1a5276`, centered):** "Rubric Pass Rates on 20 Sampled Summaries".
- **Axes:** padding top 52 / bottom 46 / left 60 / right 30; y axis 0–100% with 12px mute labels at 0%, 50%, 100% and light grid lines (`#e5e9ef`) at 50 and 100; `#999` L-shaped axis lines.
- **Bars (width 130, evenly gapped):** "Accurate? (17/20)" 85% in blue `#2a78d6`; "Complete? (13/20)" 65% in orange `#d95926`; "Concise? (18/20)" 90% in aqua `#199e70`. Bold 13px value labels ("85%" etc.) above each bar; 12px category labels below the baseline.
- **Annotation (orange `#d95926`, near the top, offset right of the middle bar):** bold 13px "1 in 3 summaries misses key facts" and bold 12px "skimming never caught it".
- **Caption (12px mute `#6b7280`, bottom-right):** "illustrative data".

## Grading 20 Summaries by Hand

**Tags:** `worked example` (green), `core idea` (blue)

- **Accurate?** — no invented facts: 3 fail (#5, #9, #17) → 17/20 pass = 85%
- **Complete?** — nothing important missing: 7 fail → 13/20 pass = 65%
- **Concise?** — under 3 sentences: 2 fail (#8, #20) → 18/20 pass = 90%
- **Pass all three** — 9 summaries fail at least one check → 11/20 = 55%
- **One number** — "55% pass rate" replaces "seems fine" in every future discussion

*Example:* Summary #5 fails twice — it invents a date AND drops the order number — but counts once in 9.

**Key point:** A rubric is just yes/no questions anyone can answer the same way. Counting the yeses is what makes quality measurable.

### Visualization (canvas `c2`, 720×300)

Pass/fail scoring-sheet heat grid: 20 columns (summaries 1–20) × 3 rows (checks), plus a pass-all marker row.

- **Title (bold 15px, `#1a5276`, centered):** "The Scoring Sheet: 20 Summaries x 3 Checks".
- **Grid geometry:** starts at x=110, y=60; cells 28×38 with 2px horizontal and 6px vertical gaps; 11px mute column numbers 1–20 above; bold 12px ink row labels right-aligned at the left: "Accurate?", "Complete?", "Concise?".
- **Cell data (fail = red `#e74c3c` with white bold "x", pass = `rgba(0,131,0,0.55)` with white "ok"):**
  - Accurate? fails: #5, #9, #17
  - Complete? fails: #2, #5, #8, #11, #14, #17, #19
  - Concise? fails: #8, #20
- **Pass-all row (label "pass all 3?", 11px mute):** under each column, bold 12px green `#008300` "P" if the summary passes all three checks, red "-" otherwise (11 P's, 9 dashes).
- **Summary lines (centered):** bold 14px green: "11 of 20 pass all three checks = 55% overall"; 12px mute: "fails: accurate 3, complete 7, concise 2 (9 distinct summaries fail something)".

## The Golden Set: Same Yardstick Every Time

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **Golden set** — keep those 20 hand-checked tickets as a fixed, reusable exam
- **Every change** — new prompt, new model: re-run the same 20, re-score with the rubric
- **Comparable** — v1 scored 55%, v2 scored 70%, v3 scored 85% — on identical tickets
- **Without it** — every prompt tweak is judged by fresh vibes on different examples
- **Catch regressions** — v2 "felt better" for accuracy but golden set showed completeness dropped

*Example:* One prompt line — "include any dates and amounts" — moved completeness from 65% to 90%.

**Rule of thumb:** Before tuning anything, freeze a small graded test set. Progress you can't re-measure on the same examples isn't progress — it's noise.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of golden-set pass rate across prompt versions.

- **Title (bold 15px, `#1a5276`, centered):** "Same 20 Golden Tickets, Re-Scored After Each Prompt Change".
- **Axes:** same frame as `c1` — padding 52/46/60/30, y 0–100% with labels at 0/50/100% and light grid lines.
- **Bars (width 140):** "prompt v1" 55% in blue `#2a78d6`; "v2: \"no invented facts\"" 70% in violet `#4a3aa7`; "v3: \"+ dates & amounts\"" 85% in green `#008300`. Bold 13px value labels above bars: "55% (11/20)", "70% (14/20)", "85% (17/20)".
- **Annotation (bold 13px green `#008300`, top center):** "same tickets every run — the improvement is real, not luck".
- **Caption (12px mute `#6b7280`, bottom-right):** "illustrative data".

## The Confusion: Fluent Is Not the Same as Faithful

**Tags:** `common mistake` (red), `trap` (orange)

- **The trap** — LLM output is always grammatical, so skimming almost always says "good"
- **Our sample** — 19 of 20 summaries (95%) read well; only 13 of 20 (65%) were complete
- **Why skimming fails** — you can't spot a missing fact without reading the source ticket
- **Errors hide** — a wrong date looks exactly like a right date until you check
- **The fix** — grade against the source, not against your impression of the prose

*Example:* "The model writes so well" and "the model missed the deadline in 1 of 3 tickets" are both true here.

**Common mistake:** Treating polish as proof of quality. Fluency is the one thing LLMs never fail at — so it is the one thing not worth testing.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart contrasting the skim impression with rubric scores.

- **Title (bold 15px, `#1a5276`, centered):** "What Skimming Sees vs What the Rubric Finds (same 20 summaries)".
- **Axes:** same frame as `c1` — padding 52/46/60/30, y 0–100% with labels at 0/50/100% and light grid lines.
- **Bars (width 140):** "\"reads well\" (skim)" 95% in mute `#6b7280`; "accurate (rubric)" 85% in blue `#2a78d6`; "complete (rubric)" 65% in orange `#d95926`. Bold 13px value labels above bars: "95% (19/20)", "85% (17/20)", "65% (13/20)".
- **Annotation (bold 13px red `#e74c3c`, placed near the top of the bars, offset left of the third bar):** "30-point gap: fluent is not faithful".

## Regeneration instructions

- **Layout:** tutorial page — `<h1>` + `.subtitle`, then 4 `.card-section` blocks; each has an `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` with one row: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets each opening with a `<b>` term (`#1a5276`), an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (50%) holding one `<canvas>` 720×300 (CSS `width:100%`, `1px solid #e0e0e0` border, 4px radius).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; bullets 0.92rem. No nav bar, no back/home links.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)`/`#1a5276`, green = `rgba(39,174,96,0.15)`/`#27ae60`, red = `rgba(231,76,60,0.12)`/`#e74c3c`, orange = `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper reads the width/height attributes, scales the backing store by `window.devicePixelRatio`, and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette `P`:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange. All numbers are hardcoded literal arrays (no `Math.random()`).
- In regenerated HTML, any card links use `.html` extensions (this page has none).
