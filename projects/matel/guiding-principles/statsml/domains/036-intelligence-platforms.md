# Intelligence Platform Pitfalls (intelligence-style)

**Page type:** detail page (obj-table layout: one h2 + one-row table per pitfall, text left 50%, canvas right 50%)
**HTML title tag:** Intelligence Platform Pitfalls - Domain-Specific Statistical Issues

**Subtitle:** Statistical and analytical traps in entity resolution, link analysis, and intelligence fusion

## Entity Resolution Across Dirty Sources

**Same Person, Three Names, Three Addresses — Which Records Match?**

- **The ambiguity:** "John Smith" in system A, "J. Smith" in B, "John W. Smith Jr." in C.
- **False match:** Wrongly merged records connect innocent people to criminal networks.
- **Missed match:** One person's activity splits across unlinked records — a permanent blind spot.
- **The rate:** 5-15% error is normal even best-in-class; every downstream analysis inherits it.

### Visualization (canvas `canvas1`, 500×300)

Three source-record boxes with match arrows and an error-rate callout.

- **Background:** full-canvas fill `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Entity Resolution: Match vs No-Match Errors" at (95, 25). Margins: top 50, right 20, bottom 40, left 40.
- **Source boxes (fill `#ecf0f1`, border `#2980b9` 1.5px, width 25% of plot, height 65; heading bold 10px "System A/B/C", records 9px `#333`):**
  - System A: "John Smith", "123 Main St", "DOB: 1985"
  - System B: "J. Smith", "456 Oak Ave", "DOB: 1985"
  - System C: "John W. Smith Jr.", "123 Main St", "DOB: 1986"
- **Match arrows (horizontal lines between box centers, stacked below the boxes; correct = solid `#27ae60`, incorrect = dashed [4,4] `#e74c3c`; 10px confidence label with ✓/✗):**
  - A↔B: "72% ✓" (correct)
  - B↔C: "45% ✗" (incorrect)
  - A↔C: "68% ✓" (correct)
- **Error-rate box (fill `#fff3cd`, border `#f39c12` 1px, 11px `#333`, two lines):** "False Match Rate: 8% → Wrong connections in analysis" / "Miss Rate: 12% → Hidden connections never found".

## Analyst Labeling is Subjective

**Inter-Annotator Agreement 67% → Model Accuracy Ceiling 67%**

- **The disagreement:** One analyst calls an event "suspicious," another calls the same event "routine."
- **The ceiling:** If humans disagree 33% of the time, no model beats 67% against those labels.
- **Not ground truth:** Labels carry the annotator's training, experience, priors, and institutional culture.
- **The trap:** Accuracy above the agreement rate is just fitting one annotator's bias.

### Visualization (canvas `canvas2`, 500×300)

2×2 inter-annotator confusion matrix with agreement statistics.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Inter-Annotator Agreement: 100 Cases" at (125, 25). Margins: top 50, right 30, bottom 50, left 50; cell size 70px.
- **Matrix headers (bold 12px `#1a5276`):** "Analyst B" across the top, "Analyst A" rotated on the left; row/column labels 11px `#555` "Suspicious" / "Routine".
- **Cells (fill = color + "33" alpha, stroke = color 1.5px, value bold 18px in color):**
  - Both suspicious: 35 — `#27ae60`
  - A suspicious / B routine: 15 — `#e74c3c`
  - A routine / B suspicious: 18 — `#e74c3c`
  - Both routine: 32 — `#27ae60`
- **Stats panel (right):** "Agreement:" bold 13px `#333`, "67%" bold 24px `#27ae60`; "Disagreement:" bold 13px `#e74c3c`, "33%" bold 24px `#e74c3c`; 11px `#555` "Model accuracy" / "ceiling = 67%".
- **Bottom annotation (bold 12px `#c0392b`):** "If humans disagree 33% of the time, no model can exceed 67% on these labels".

## Temporal Knowledge Graph Evolution

**"Connected To" Is Meaningless Without a Timestamp**

- **The change:** Person A works at Company B in 2020 and Company C in 2022 — both edges exist.
- **Static conflation:** An untimed graph treats historical and current relationships as equals.
- **The fix costs:** "As of date X" queries are essential, expensive, and rarely built correctly.
- **What you actually have:** A snapshot presented as timeless truth.

### Visualization (canvas `canvas3`, 500×300)

Two side-by-side knowledge-graph snapshots (2020 vs 2023) with the same four nodes but different active edges.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Knowledge Graph: Same Query, Different Time = Different Answer" at (30, 25). Margins: top 50, right 20, bottom 40, left 30; each subgraph occupies 45% of plot width.
- **Nodes (both graphs, `#2980b9` filled circles radius 12, 10px `#333` labels):** Person A (top center), Company B (bottom left), Company C (bottom center), Person D (bottom right).
- **Edges:** active = solid `#2980b9` 2px with 9px `#2980b9` label; inactive = dashed [3,3] `#ddd` 1px with 9px `#aaa` label.
  - **"As of 2020" (bold 12px `#1a5276` heading):** A–B active "works at"; A–D active "knows"; A–C inactive; B–D inactive.
  - **"As of 2023":** A–B inactive "(left)"; A–C active "works at"; A–D inactive "(estranged)"; C–D active "partners".
- **Bottom annotation (bold 11px `#c0392b`):** "Static graph conflates past and present → wrong conclusions".

## Adversarial Entities Deliberately Hiding

**Targets Know Your Detection Patterns and Avoid Creating Them**

- **The tradecraft:** Aliases, shell companies, encrypted channels, cash payments, cutouts.
- **Absence as signal:** Zero transactions for a known wealthy individual IS suspicious.
- **Why it fails:** Absence is not a row in any dataset, so no feature encodes it.
- **Anti-patterns:** Sophisticated actors work at looking deliberately normal.

### Visualization (canvas `canvas4`, 500×300)

Bar chart: detection rate falling with adversary sophistication.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Detection vs Sophistication of Adversary" at (115, 25). Margins: top 50, right 30, bottom 55, left 60. L-shaped axes `#333` 1px.
- **Data (y-scale 0–100%; bar color: green `#27ae60` if >70, amber `#f39c12` if >30, else red `#e74c3c`):**
  - Amateur — 92% (green)
  - Organized — 65% (amber)
  - Professional — 35% (amber)
  - State-level — 12% (red)
  - Insider — 5% (red)
- **Value labels:** bold 12px `#333` "<pct>%" above each bar; category names 10px below axis.
- **Annotation (bold 11px `#c0392b`, top, two lines):** "Sophisticated actors: absence of data IS the signal" / '(but absence is not a "feature" in any model)'.
- **Axis labels (12px `#555`):** x "Adversary Sophistication →"; y (rotated) "Detection Rate (%)".

## "Connecting the Dots" Confirmation Bias

**In a Dense Graph, Everything Connects Within 3 Hops**

- **The search:** Analyst starts with a theory, queries for confirming links, and finds them.
- **Real but meaningless:** A shared associate is unremarkable when any two of 10M people sit 3-4 hops apart.
- **Missing baseline:** Without a base rate, the existence of a path carries no information.
- **The right question:** Is this path shorter or stronger than chance predicts?

### Visualization (canvas `canvas5`, 500×300)

Dense random network with one highlighted "suspicious" 3-hop path.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Dense Graph: Everything Connects in 3 Hops" at (100, 25). Margins: top 45, right 20, bottom 40, left 20.
- **Graph (randomized at render):** 25 nodes at random positions; edges `#ddd` 0.5px drawn between node pairs closer than 100px (with ~60% probability), producing a dense mesh.
- **Highlighted path:** node indices 0 → 3 → 8 → 15 connected with `#e74c3c` 3px lines; path nodes are `#e74c3c` radius-7 dots, all other nodes `#2980b9` radius-4 dots.
- **Path labels (bold 10px `#e74c3c`):** "SUSPECT A" at the first node, "SUSPECT B" at the last; 9px "(3 hops)" at the middle node.
- **Annotation box (fill `#fff3cd`, bottom right; bold 10px `#c0392b`, two lines):** "Connection is REAL but" / "MEANINGLESS without base rate".

## Classification Confidence ≠ Actionability

**85% Confidence Is Not Enough to Raid a Home**

- **Cost asymmetry:** A false positive means frozen assets, a ruined reputation, the wrong door.
- **At scale:** 85% accuracy across 10,000 flags leaves 1,500 innocent people harmed.
- **Threshold by consequence:** Review can run at 50%; irreversible action needs 99.9%+.
- **Leaderboard vs field:** Kaggle-grade accuracy is operationally insufficient.

### Visualization (canvas `canvas6`, 500×300)

Horizontal bars comparing required confidence thresholds per action against a fixed 85% model confidence.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Model Confidence vs Action Threshold" at (125, 25). Margins: top 50, right 30, bottom 55, left 60. L-shaped axes `#333` 1px.
- **Rows (each: gray `#ddd` track spanning 80% of plot width, model-confidence fill = row color + "88" alpha up to 85%, black 2px threshold tick with 9px "Need: <pct>%" label; row name 11px `#333` at right):**
  - Flag for review — threshold 50% — `#27ae60`
  - Enhanced monitoring — threshold 75% — `#27ae60`
  - Freeze assets — threshold 95% — `#e74c3c`
  - Arrest/raid — threshold 99.5% — `#e74c3c`
  - Drone strike — threshold 99.99% — `#e74c3c`
- **Model line:** dashed [5,5] vertical `#2980b9` 2px at 85%, labeled bold 11px `#2980b9` "Model: 85%".
- **Bottom annotation (bold 11px `#c0392b`):** "85% is NOT enough for consequential actions".

## Data Provenance and Chain of Custody

**HUMINT ≠ SIGINT ≠ OSINT ≠ FININT — Reliability Differs Wildly**

- **Distinct error modes:** Informants lie, intercepts get spoofed, open source gets fabricated, records get structured.
- **Equal weighting is wrong:** One trust assumption applied to sources that fail in different ways.
- **Exploitable:** Adversaries feed whichever source they know you weight highest.
- **What's needed:** Per-source reliability weights carried through to the final estimate.

### Visualization (canvas `canvas7`, 500×300)

Bar chart of source reliability with a dark spoofability overlay per bar.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Intelligence Source Reliability Spectrum" at (115, 25). Margins: top 50, right 20, bottom 40, left 40; baseline x-axis `#333` 1px.
- **Data (bar height = reliability % of 100; dark overlay `#00000033` covering spoofable% of the bar from its top; two-line 9px `#555` names below; bold 11px `#333` reliability label above):**
  - FININT (Bank records) — reliability 92, spoofable 15 — `#27ae60`
  - SIGINT (Intercepts) — reliability 85, spoofable 25 — `#2980b9`
  - IMINT (Imagery) — reliability 80, spoofable 20 — `#3498db`
  - OSINT (Open source) — reliability 55, spoofable 60 — `#f39c12`
  - HUMINT (Informants) — reliability 40, spoofable 70 — `#e74c3c`
  - Social media (Unverified) — reliability 20, spoofable 90 — `#c0392b`
- **Legend (top right, 11px `#333`):** "Bar height = reliability" / "Dark overlay = spoofability".
- **Annotation (bold 10px `#c0392b`):** "Treating all sources equally = exploitable".

## Feedback Loops from Intervention

**Investigating Changes the Behavior You Are Measuring**

- **The sequence:** Flag → investigate → target notices → behavior changes → no crime observed.
- **Mislabeled outcome:** The case closes as "false positive" even though deterrence worked.
- **No counterfactual:** You can never observe what they would have done unwatched.
- **Perverse metric:** The better the deterrence, the worse the apparent precision.

### Visualization (canvas `canvas8`, 500×300)

Circular four-step feedback-loop diagram with arc arrows.

- **Background:** `#f0f4f8`. **Title (bold 15px `#1a5276`):** "Investigation Feedback Loop: Paradox of Success" at (90, 25). Margins: top 50, right 20, bottom 30, left 30; circle center ≈ (50%, 45%) of plot, radius 80.
- **Steps at compass points (colored arc arrows 2.5px connect each step to the next, ending in a radius-4 dot arrowhead; two-line bold 10px labels outside the circle in step color):**
  - Top (`#e74c3c`): "Model flags" / "entity as suspicious"
  - Right (`#f39c12`): "Investigation" / "initiated"
  - Bottom (`#2980b9`): "Entity notices," / "changes behavior"
  - Left (`#27ae60`): "No crime observed" / '→ "false positive"'
- **Center text (bold 11px `#c0392b`, three lines):** "Success" / "looks like" / "failure".
- **Bottom annotation (11px `#555`):** "The better the system works at deterrence, the higher its apparent false positive rate".

## Regeneration instructions

- **Layout:** standard domains detail page. h1, `.subtitle` paragraph, then per pitfall an `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border, padding-bottom 8px) followed by a one-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (bold one-line summary) + a `<ul>` of labeled one-line bullets (`<strong>` label + short phrase), right `<td>` (60%, centered) holds the canvas. Even table rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `ul` 0.9em `#333` with 20px left margin, `li` 4px vertical margin; `strong` `#1a5276`; `.philosophy` callout style available (background `#f0f4f8`, left border 4px `#2980b9`) but unused. No nav bar, no back/home links.
- **Canvases:** each declared `<canvas id="canvasN" width="500" height="300">`; a shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) at 500×300px, and calls `ctx.scale` so drawing stays in logical coordinates, and sets base font 17px system sans-serif. Each chart is an IIFE painting on a `#f0f4f8` background.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (unused here; family colors `#f39c12`, `#c0392b` used), secondary blue `#2980b9`/`#3498db`, gray `#555`/`#333`, warning fill `#fff3cd`.
- Chart 5 generates node positions and mesh edges with `Math.random()` at render time (the highlighted 0→3→8→15 path indices are fixed).
