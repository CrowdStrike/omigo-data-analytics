# Long LLM Sessions as a Data Collection Channel

**Page type:** detail page (backlog-style two-column layout: text left 50%, canvas right 50%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Long LLM Sessions as a Data Collection Channel

**Subtitle:** A multi-hour session captures pasted files, stack traces, internal names, and the reasoning path to a decision — a richer record than any form ever collected.

**Intro callout:** A form collects the fields it asked for. A long session collects whatever was needed to make progress — and what gets pasted to unblock a problem is not the same population as what someone would choose to disclose. The sampling frame is set by friction, not by intent.

## 1. Why a Session Is a Rich Instrument

The transcript records the path to an answer, not just the answer.

- **Not just prompts** — pasted files, tracebacks, config fragments, and schema dumps.
- **Internal vocabulary** — service names, table names, and team shorthand no document lists.
- **The reasoning path** — rejected options and the argument that killed them.
- **Duration compounds it** — a multi-hour session accumulates context a single query never would.
- **Nobody chose the contents** — inclusion is driven by what unblocked the next step.
- **Denser than a survey** — no question limits it, so it captures more per unit of effort.

**Key point:** The richest field is the one nobody would have answered if a form had asked for it.

### Visualization (canvas `c1`, 720×340)

Horizontal bar chart: what a long session captures that a structured form does not.

- **Title (bold 16px, `#1a5276`, top center):** "What a Session Records vs What a Form Asks".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example".
- **Categories (7, top to bottom):** `stated question`, `pasted code`, `stack traces`, `config values`, `internal names`, `rejected options`, `reasoning path`.
- **Data (share of sessions containing each, percent):** form `[100, 0, 0, 0, 0, 0, 0]`; session `[100, 78, 64, 41, 57, 45, 82]`.
- **Plot area:** x=150, y=64, width = canvas−200, height = canvas−110; vertical axis line `#95a5a6` (1.4px) at x=150.
- **Bars:** 7 rows; per row two bars each 0.34·row-height — upper bar (form) fill `rgba(149,165,166,0.45)` stroke `#95a5a6` 1.2px; lower bar (session) fill `rgba(26,82,118,0.35)` stroke `#1a5276` 1.4px.
- **Scale:** 0–100%, gridlines every 25% (dashed `#ecf0f1`, 1px), tick labels below axis (12px `#5a6875`).
- **Y labels:** category names right-aligned at x=142 (12px `#4a5866`).
- **Legend (top-right inside plot):** gray swatch + "structured form", blue swatch + "long session" (13px `#2c3e50`).
- **Annotation (13px `#e67e22`, right of the `reasoning path` session bar):** "never a form field".

## 2. Three Leak Classes, Three Different Fixes

Grouping these as one risk hides that each has a different countermeasure.

- **Incidental secrets** — a token inside a pasted log, a connection string in a traceback.
- **Customer data** — records pasted deliberately so the model can help analyze them.
- **Unpublished ideas** — architecture, roadmap, and pricing logic that exist in no file.
- **Different detectability** — scanners catch secret patterns; an idea has no pattern to match.
- **Different reversibility** — a leaked credential can be rotated; a disclosed plan cannot.
- **Different blame** — the first is an accident, the second a decision, the third invisible.

**Key point:** Only the first class has a technical control; the other two need a habit.

### Visualization (canvas `c2`, 720×340)

Grouped bar chart: detectability vs reversibility across the three leak classes.

- **Title (bold 16px, `#1a5276`, top center):** "Each Leak Class Fails Differently".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example — scored 0–10".
- **Categories (3):** `incidental secrets`, `customer data`, `unpublished ideas`.
- **Data (0–10 scores):** detectable by a scanner `[9, 4, 1]`; reversible after the fact `[8, 2, 1]`.
- **Plot area:** x=66, y=76, width = canvas−120, height = canvas−140; L-shaped axes `#95a5a6` (1.4px).
- **Bars:** 3 slots; per slot two bars each 0.30·slot-width — left bar (detectable) fill `rgba(39,174,96,0.50)` stroke `#27ae60` 1.4px; right bar (reversible) fill `rgba(41,128,185,0.50)` stroke `#2980b9` 1.4px.
- **Scale:** 0–10, tick labels every 2 (12px `#5a6875`, right-aligned).
- **X labels:** class names (13px `#4a5866`) centered under each slot.
- **Legend (top-right inside plot):** green swatch + "a scanner can find it", blue swatch + "can be undone" (13px `#2c3e50`).
- **Annotation (13px `#e74c3c`, above the `unpublished ideas` slot):** "no pattern, no undo".

## 3. Retention, Training, and Review Are Three Separate Questions

"The model learns from my session" collapses three independent settings into one.

- **Retention window** — how long the transcript is stored, which is not a training claim.
- **Training consent** — whether the text may update a model, often tier-dependent.
- **Human review** — whether a person may read it, typically for abuse and quality work.
- **Independent settings** — zero training with long retention is a coherent configuration.
- **Check per provider** — the terms differ by consumer tier, API, and enterprise contract.
- **Not a universal** — a claim true of one product is not evidence about another.

**Key point:** Ask which of the three you mean; the answer is a contract term, not a property of models.

### Visualization (canvas `c3`, 720×340)

Matrix diagram: three settings across three account archetypes, showing they vary independently.

- **Title (bold 16px, `#1a5276`, top center):** "Three Settings That Vary Independently".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example — archetypes, not specific products".
- **Grid:** 3 columns (`retention`, `training`, `human review`) × 3 rows (`consumer tier`, `API access`, `negotiated contract`).
- **Cell geometry:** left margin 168, top 96; cell 128 wide × 56 tall, 8px gap; rounded 4px.
- **Cell states:** `on` fill `rgba(231,76,60,0.18)` stroke `#e74c3c`, glyph "yes" (13px `#c0392b`); `off` fill `rgba(39,174,96,0.15)` stroke `#27ae60`, glyph "no" (13px `#1e8449`); `varies` fill `rgba(230,126,34,0.15)` stroke `#e67e22`, glyph "by term" (12px `#b9770e`).
- **Values by row:** consumer tier `[on, varies, on]`; API access `[on, off, on]`; negotiated contract `[varies, off, varies]`.
- **Row labels:** right-aligned at x=160 (13px `#4a5866`); column headers centered above each column (13px bold `#1a5276`).
- **Annotation (13px `#e67e22`, below the grid, centered):** "no row is all-red or all-green — that is the point".

## 4. The Mechanisms Behind an Unexpected Capture

Most surprises come from a mechanism the user did not know was running.

- **Memory features** — context deliberately persisted across sessions, not just within one.
- **Agents with tool access** — a file read to answer a question was never pasted by anyone.
- **Third-party wrappers** — a layer that logs upstream of the provider it calls.
- **Shared history** — a transcript sitting in a workspace others can open.
- **Screenshots in tickets** — the excerpt escapes the session entirely and lands in a tracker.
- **Each is mundane** — none requires a model to memorize anything.

**Key point:** The agent that reads files broadens the capture beyond anything a user pasted.

### Visualization (canvas `c4`, 720×340)

Funnel/flow diagram: how session content reaches destinations the user did not pick.

- **Title (bold 16px, `#1a5276`, top center):** "Paths Out of a Session".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example".
- **Source node (left, centered vertically):** rounded rect 150×64 at x=40, fill `rgba(26,82,118,0.35)` stroke `#1a5276` 1.5px, label "session content" (13px `#1a5276`, 2 lines centered).
- **Destination nodes (right column, x=498, width 182, height 44, 14px vertical gap, 5 nodes):** `provider storage`, `training corpus (if permitted)`, `human review queue`, `shared team history`, `ticket attachment`.
- **Destination style:** fill `rgba(230,126,34,0.14)` stroke `#e67e22` 1.2px, label 12px `#4a5866` centered.
- **Edges:** bezier curves from the source's right edge to each destination's left edge, stroke `#95a5a6` 1.4px; the edges to `shared team history` and `ticket attachment` stroke `#e74c3c` 1.6px dashed (dash 5/4).
- **Edge labels (11px, above each curve's midpoint):** `retention`, `consent`, `abuse review`, `workspace default`, `copy-paste`.
- **Annotation (13px `#e74c3c`, bottom right under the last two nodes):** "these two never involve the provider".

## 5. The Tail Risk and the Likely One

The theoretical failure gets the attention; the mundane one does the damage.

- **Regurgitation** — a model reproducing training text verbatim, hard to demonstrate on demand.
- **Membership inference** — asking whether a specific record was in the training set.
- **Both are real** — and both are weak, low-rate effects that need contrived conditions.
- **The likely failure** — a transcript in a shared history, or a screenshot in a ticket.
- **Attention is inverted** — the dramatic mechanism gets the policy, the boring one gets the leak.
- **Base rates decide** — a rare mechanism with a tiny rate loses to a common one.

**Key point:** Guard against the ordinary path first; it has the larger expected loss.

### Visualization (canvas `c5`, 720×340)

Two-panel comparison: attention paid vs expected loss, for the exotic and the mundane path.

- **Title (bold 16px, `#1a5276`, top center):** "Where Attention Goes vs Where Loss Comes From".
- **Subtitle (12px `#7f8c8d`, centered under title):** "Illustrative Example — expected loss = rate × severity".
- **Table data (computed at render time, printed in the chart):**
  - `regurgitation`: rate 0.001, severity 9 → expected loss 0.009
  - `membership inference`: rate 0.002, severity 6 → expected loss 0.012
  - `shared history`: rate 0.120, severity 5 → expected loss 0.600
  - `ticket screenshot`: rate 0.090, severity 4 → expected loss 0.360
- **Left panel (share of attention, percent):** `[45, 25, 18, 12]` for the four paths above.
- **Right panel (expected loss, normalized to the largest = 100):** computed in JS from the rate × severity products above, so the printed bar values derive from the table rather than being hardcoded.
- **Plot areas:** two side-by-side regions, each width = (canvas−160)/2, y=88, height = canvas−150; 44px gutter; L-shaped axes `#95a5a6` (1.4px) per panel.
- **Bars (horizontal, 4 per panel):** left panel fill `rgba(149,165,166,0.45)` stroke `#95a5a6`; right panel fill `rgba(231,76,60,0.45)` stroke `#e74c3c` 1.4px.
- **Panel headers (13px bold, centered above each):** "attention paid" `#5a6875`; "expected loss" `#c0392b`.
- **Value labels:** printed at each bar's end (12px), computed from the arrays at render time — never hardcoded strings.
- **Y labels:** path names on the left panel only (12px `#4a5866`, right-aligned).
- **Annotation (13px `#e74c3c`, under the right panel):** "the two mundane paths carry ~98% of expected loss" — computed in JS as (0.600+0.360)/(0.009+0.012+0.600+0.360) and printed, not asserted.

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 50% and `td.viz-col` 50%, both `vertical-align: top`, 12px padding. No index number in the h1.
- **Text blocks:** intro `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; secondary `#2980b9`; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`; bar fill `rgba(26,82,118,0.35)`.
- **Canvas:** intrinsic 720×340; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), `ctx.scale` back to logical coordinates via a shared `setupCanvas(id)` helper; redraws on resize.
- **No generated random data on this page** — every chart uses hardcoded literal arrays, so no PRNG is needed. Every printed statistic in chart 5 is computed in JS from those arrays at render time.
- **Every figure on this page is constructed** and labeled "Illustrative Example" in a chart subtitle. No figure is presented as a measurement, and no provider or product is named.
- **No credential-shaped strings anywhere** — leak classes are described in prose ("a token inside a pasted log"), never illustrated with a realistic secret or `key=value` syntax.
- In regenerated HTML, any card links use `.html` extensions (this page has none). No nav, back, or home links.
