# Semantic Validation of Decision Tree Branches (Paths)

**Page type:** detail page (backlog kusto-style two-column layout: text left 45%, canvas right 55%, one table row per section)
**HTML title tag:** Semantic Validation of Decision Tree Branches (Paths) — Discussion Backlog

**Subtitle:** Does the whole root-to-leaf path describe anyone real?

**Intro callout:** Validates an entire root-to-leaf path as a coherent meaningful subpopulation description. Each split may be individually valid while the combined path describes nobody real.

## 1. The Problem

Each split individually valid, but combined path may describe nobody real:

- `age > 65 AND employed=full-time AND student_loan=yes AND children > 4`
- `cholesterol > 280 AND blood_pressure < 90 AND BMI < 18` (medically contradictory)
- `income > $200K AND education=no-diploma AND age < 25` (vanishingly rare)

**What to check:**

- **Population coherence:** Recognizable real-world group?
- **Joint probability:** Near zero = fitting noise
- **Contradiction detection:** Mutually contradictory conditions
- **Diminishing returns:** After depth 3-4, adding genuine info?
- **Path narrative:** Can be stated as plain English a domain expert nods at?

### Visualization (canvas `c1`, 720×300)

Side-by-side comparison panels: coherent branch (green) vs incoherent branch (red).

- **Title (bold 16px, `#1a5276`, top center):** "Coherent Branch vs Incoherent Branch"
- **Left panel** (320×230 at x=20, y=35, fill `#e8f8e8`, stroke `#27ae60` width 2), heading "COHERENT" (bold 15px `#27ae60`, centered). Three white condition boxes (260×25, stroke `#27ae60`, 14px `#2c3e50` text) stacked with green down-arrows: "age > 55", "cholesterol > 240", "smoker = yes". Result (bold 15px `#27ae60`, centered): "78% positive (n=142)". Then three centered 13px `#2c3e50` lines: "\"Older smoker with high cholesterol\"" / "— Doctor nods, makes medical sense" / "Known high-risk cardiac profile".
- **Right panel** (320×230 at x=380, y=35, fill `#fde8e8`, stroke `#e74c3c` width 2), heading "INCOHERENT" (bold 15px `#e74c3c`, centered). Three white condition boxes (stroke `#e74c3c`) with red down-arrows: "age < 25", "retired = yes", "mortgage_years > 20". Result (bold 15px `#e74c3c`): "100% positive (n=3!)". Then centered 13px lines: "\"Young retiree with 20yr mortgage\"" / "— Nobody real fits this description" (both `#2c3e50`) and "Fitting noise, not signal" in `#e74c3c`.

## 2. Implementation

- LLM coherence scoring: Rate 0-1 likelihood
- Joint frequency check: < 20 samples = unreliable
- Conditional independence test
- Path simplification: Detect redundant conditions
- Cross-validation stability

**Key Questions:**
(1) At what depth is it essential?
(2) Rare-but-real subpopulations?
(3) During construction or post-hoc?
(4) Path = multi-dimensional range?
(5) Semantic metadata for contradiction pairs?

### Visualization (canvas `c2`, 720×300)

Horizontal coherence-score band with three colored zones, example markers, and action text.

- **Title (bold 16px, `#1a5276`, top center):** "Branch Coherence Score — Decision Thresholds"
- **Score bar** (from x=60 to x=660, y=80, height 50), three zones:
  - 0–0.3: fill `rgba(231,76,60,0.3)`, stroke `#e74c3c`, centered label "REJECT" (bold 15px `#e74c3c`)
  - 0.3–0.6: fill `rgba(230,126,34,0.2)`, stroke `#e67e22`, label "FLAG" (bold 15px `#e67e22`)
  - 0.6–1.0: fill `rgba(39,174,96,0.2)`, stroke `#27ae60`, label "ACCEPT" (bold 15px `#27ae60`)
- **Scale labels** (14px `#2c3e50`, below bar): "0", "0.3", "0.6", "1.0" at zone boundaries.
- **Example markers** (each with a colored down-arrow to the bar, a filled triangle marker on the bar top, and a 13px centered label at staggered heights above):
  - score 0.15, label "age<25 + retired (0.15)", color `#e74c3c`
  - score 0.45, label "income>200K + no-diploma (0.45)", color `#e67e22`
  - score 0.82, label "age>55 + high-chol + smoker (0.82)", color `#27ae60`
- **Action lines** (14px, left-aligned at x=60, starting y=185):
  - Red `#e74c3c`: "< 0.3: Auto-reject branch. Likely fitting noise or contradictory path."
  - Orange `#e67e22`: "0.3-0.6: Flag for review. May be rare-but-real or may be artifact."
  - Green `#27ae60`: "> 0.6: Accept. Path describes recognizable, populated subgroup."
- **Footnote (13px `#95a5a6`):** "Score = f(joint_probability, contradiction_count, domain_alignment, sample_size)"

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem), then one `.lang-section` per numbered section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with a single `<tr>` — left `td.text-col` (45%) holds paragraphs/bullets/key-point, right `td.viz-col` (55%) holds the canvas. No index number in the h1.
- **Inline code style:** `code` — background `#f4f4f4`, padding 2px 6px, radius 3px, 0.85em, color `#1a5276`.
- **Key-point callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem with 4px li spacing; canvases `width: 100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, accents `#2980b9`, gray `#95a5a6`.
- **Canvas:** intrinsic width/height attributes as given (720×300 each); scale via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setupCanvas(id)` helper.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
