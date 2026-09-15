# Pitfall: Positive-Only Data Collection (No Negative Class Design)

**Page type:** detail page (three card-sections, each a two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Positive-Only Data Collection (No Negative Class Design)

**Subtitle:** Pipeline collects rich positives but has no negative class strategy, crippling model training.

## The Problem

**Tags:** `the trap` (red), `negative class` (blue)

- **Rich positives only** — clicks and purchases get full features; negatives are an afterthought
- **No sampling strategy** — "everything else" becomes a massively imbalanced, incoherent class
- **Asymmetric definition** — "didn't convert" mixes non-viewers, abandoners, failures, and bots
- **Feature gap** — tracking fires only on the positive path, so negatives miss features
- **Retrospective negatives** — positives log live, negatives are queried later with time skew
- **Broken boundary** — the model separates data completeness, not positive from negative

*Example:* A click model with full-context positives and missing-feature negatives hits 95% training accuracy but drops to 58% in production.

**Impact:** The decision boundary becomes an artifact of data collection — the model separates complete from incomplete data, not positive from negative.

### Visualization (canvas `c1`, 720×300)

Side-by-side comparison: a rich positives box (left) vs a sparse negatives box (right), with a contamination warning below.

- **Title (bold 14px `#1a5276`, top center):** "Positive-Only Data: Asymmetric Collection".
- **Positives box:** 260×180 at (40,40), stroke `#27ae60` width 3; header bold 13px green "POSITIVES (confirmed cases)". Six list items, each a bold green "+" followed by 11px `#333` text: "Lab results", "Imaging scans", "Treatment records", "Follow-up visits", "Symptom timeline", "Biopsy confirmed". Light green fill `#27ae60` at alpha 0.15 behind the list; caption bold 10px green "100% feature coverage".
- **Negatives box:** 260×180 at (420,40), stroke `#e74c3c` width 3; header bold 13px red "NEGATIVES (assumed healthy)". Large translucent red "?" (bold 80px, alpha 0.2) centered; 10px red labels "Never tested?", "Undiagnosed?", "Truly healthy?".
- **Contamination arrow:** dashed orange `#e67e22` width 2 (dash 5/3) dropping from the negatives box to a contamination box, with a small orange arrowhead.
- **Contamination box:** 300×40 at (380,235), stroke `#e67e22` width 2; bold 11px orange "CONTAMINATION: latent positives hiding as negatives"; 10px line: "Model learns \"has data\" vs \"missing data\", not disease vs healthy".
- **Bottom annotation (bold 11px `#e74c3c`, centered):** "Decision boundary = data completeness, not true class difference".

## Why It Happens

**Tags:** `root cause` (orange), `event logging` (blue)

- **Recording follows action** — pipelines capture what happened, never what could have happened
- **Event-driven logging** — systems record clicks and purchases but never non-actions
- **No non-event signal** — negatives leave no trace because a non-event generates no record
- **Survivorship bias** — only cases that progressed far enough ever reach the warehouse
- **Prior gatekeeping** — an earlier model or human review filters who enters the data at all

*Example:* A loan default model trained only on approved loans never sees the rejected applicants who would have defaulted.

**Root Cause:** Event-driven systems record occurrences only — if your positive class IS the event, no natural negative class exists.

### Visualization (canvas `c2`, 720×300)

Funnel diagram: all potential cases at top, non-events escaping to the sides unrecorded, only events reaching the dataset.

- **Title (bold 14px `#1a5276`, top center):** "Event-Driven Systems: Non-Events Disappear".
- **Funnel top:** trapezoid from (160,40)-(560,40) narrowing to (460,130)-(260,130); fill `rgba(26,82,118,0.15)`, stroke `#1a5276` width 2. Labels: bold 12px "ALL POTENTIAL CASES", 11px "(Eligible population: events + non-events)".
- **Escaping non-events:** two faint red side shapes (`rgba(231,76,60,0.1)`) drifting left and right from the funnel edges. Red bold 11px labels: left side "x No click", "x No purchase"; right side "x No complaint", "x No default". Dashed red arrows (width 1.5, dash 4/3) point outward from the funnel edges. Bold 10px red "UNRECORDED" labels centered at (120,210) and (600,210).
- **Funnel bottom:** green trapezoid from (290,140)-(430,140) to (400,230)-(320,230); fill `rgba(39,174,96,0.2)`, stroke `#27ae60` width 2. Labels: bold 12px "EVENTS ONLY", 10px "(Recorded in DB)".
- **Dataset box:** 160×40 at (280,245), stroke `#27ae60` width 2; bold 11px green "DATASET", 10px `#333` "100% positive class"; green arrow connecting funnel to box.
- **Bottom warning (bold 11px `#e74c3c`, centered):** "Result: No negatives in training data — non-events were never logged".

## The Correct Approach

**Tags:** `the fix` (green), `eligibility` (blue)

- **Design negatives first** — define the negative class before modeling, with positive-class rigor
- **Anchor on eligibility** — negatives come from those who could have converted but did not
- **Time-match the sample** — draw negatives from the same time window as the positives
- **Instrument both paths** — log negatives with the same feature coverage as positives
- **Correct selection bias** — use reject inference or propensity weights for gatekept cases
- **Exclude the unknowable** — rejected applicants are neither positive nor negative

*Example:* For loan default, positives = approved and defaulted, negatives = approved and repaid, rejected = excluded as unknown outcome.

**Fix:** Define both classes before collecting data — positive = event occurred, negative = eligible but no event; if you can't define eligibility, you can't build a valid classifier.

### Visualization (canvas `c3`, 720×300)

Three-way split diagram: an eligible population box branching into positive, negative, and excluded classes, converging into a clean binary problem.

- **Title (bold 14px `#1a5276`, top center):** "Correct: Define Both Classes From Eligible Population".
- **Eligible population box:** 360×45 at (180,35), fill `rgba(26,82,118,0.12)`, stroke `#1a5276` width 2; bold 13px "ELIGIBLE POPULATION", 10px "(All who could have experienced the event)".
- **Three branch lines:** green solid to the left box, blue solid straight down to the middle box, red dashed (dash 4/3) to the right box.
- **Positive box (left):** 200×110 at (60,120), fill `rgba(39,174,96,0.15)`, stroke `#27ae60` width 2; bold 12px "POSITIVE CLASS", 11px "Event occurred"; bold 22px green "✓"; 10px lines: "Approved + Defaulted", "Eligible + Converted", "Full feature vector".
- **Negative box (middle):** 200×110 at (275,120), fill `rgba(26,82,118,0.1)`, stroke `#1a5276` width 2; bold 12px "NEGATIVE CLASS", 11px "Eligible, event did NOT occur"; bold 22px green "✓"; 10px lines: "Approved + Did NOT default", "Eligible + Did NOT convert", "Full feature vector".
- **Excluded box (right):** 180×110 at (500,120), fill `rgba(231,76,60,0.08)`, dashed stroke `#e74c3c` width 2 (dash 5/4); bold 12px "EXCLUDED", 11px "Unknown outcome"; bold 22px red "✗"; 10px lines: "Rejected applicants", "Never saw product", "Outcome unknowable".
- **Result box:** 360×38 at (130,250), fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2, with green connector lines from the positive and negative boxes; bold 12px green "✓ Clean Binary Classification Problem"; 10px `#333` "Both classes well-defined, feature-complete, from same eligible population".

## Regeneration instructions

- **Layout:** three `.card-section` blocks (The Problem / Why It Happens / The Correct Approach), each an h2 with blue bottom border followed by a `table.layout` with one row: left `td.text-col` (45%) holding `.tags` pills, a `ul` of labeled bullets, an italic `.example` paragraph, and a `.key-point` callout; right `td.viz-col` (55%) holding one 720×300 canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `ul` 0.92rem; `li b` in `#1a5276`.
- **Tag pills:** `.tag` inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors: blue `rgba(26,82,118,0.12)`/`#1a5276`; green `rgba(39,174,96,0.15)`/`#27ae60`; red `rgba(231,76,60,0.12)`/`#e74c3c`; orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem; `strong` in `#1a5276`. `.example` italic `#555` 0.9rem.
- **Canvas:** intrinsic 720×300, CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) through a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
