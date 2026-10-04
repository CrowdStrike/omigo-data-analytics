# Reasoning Fallacies — Viz

**Page type:** detail page — one `.card-section` per fallacy, each a two-column layout table (text left 50%, canvas right 50%)
**HTML title tag:** Reasoning Fallacies
**Template:** none named in the source; the section structure is specified inline (see Page-specific constraints)

House style (bullet form, tag-pill markup, text-column order, canvas DPR scaling, shared palette `P`, canvas font floors, section-header borders) lives in `ui-templates/` and is not restated here.

**Figures printed in the sibling `.txt.md` are computed here.** Changing a seed or the construction invalidates that prose — re-read the computed values and update the text to match.

**Determinism:** no stochastic data anywhere on this page and no `Math.random()`. Section 2 is the only generated series: the filter counts are derived in JS from the stated conditional rates, so no seeding rule is needed.

---

## 1. Gambler's Fallacy

### canvas `c1` — 720×300

Coin-flip sequence diagram on `#f9fafb` background.

- **Header (bold 14px `#555`, centered at x=200, y=30):** "Past flips (independent events):".
- **Coins:** five circles radius 28, 80px apart starting at x=68, y=80; gold fill `#f4d03f`, stroke `#d4ac0d` width 2, each with bold 20px `#1a5276` letter "H".
- **Arrow:** solid `#1a5276` line width 3 with arrowhead pointing right after the fifth coin, leading to a sixth dashed-outline coin (fill `#eaf2f8`, dashed (5/4) stroke `#2980b9` width 2.5) containing bold 22px `#2980b9` "?".
- **Correct answer (green `#27ae60`):** bold 16px "P(next H) = 50%" then 14px "Each flip is independent. The coin has no memory."
- **Wrong belief (red `#e74c3c`):** bold 16px "\"Tails is due! P(T) must be higher now!\"" with a red strikethrough line through it; below, 14px red "WRONG — confuses sequence probability with next-event probability."
- **Bottom note (12px `#555`):** "P(HHHHH) = 1/32 ← rare sequence. But P(6th = H | first 5 = H) = 1/2 ← unchanged."

---

## 2. Conjunction Fallacy

### canvas `c2` — 720×300

Area-proportional nested rectangles (left) plus a filter list with n values (right), on `#f9fafb` background.

- **Counts:** `[30000, 15000, 1500, 330, 231, 5]` for filters `Population`, `+ Male`, `+ Age 45–50`, `+ Income $80–100k`, `+ Homeowner`, `+ Bought last 30 days`. Each count is derived in JS from the previous one via the conditional rates `[0.50, 0.10, 0.22, 0.70, 0.022]` and rounded, so the chart, its labels, and the prose cannot drift.
- **Nested rectangles:** each rectangle's **area is proportional to n** — linear scale = `sqrt(n_i / n_0)` applied to a base 420×250 box, computed at render time, giving widths 420, 297, 94, 44, 37 and heights 250, 177, 56, 26, 22. All boxes share the center of the base box at (230, 150). Fills progressively darker blue: `rgba(26,82,118,0.08)`, `0.12`, `0.18`, `0.25`, `0.35`; borders `#1a5276` width 1.5. The final level (n = 5) would be ~5×3px, so it is drawn instead as a red dot radius 4 (`#e74c3c`) at the shared center with a bold 11px red "n = 5" label beside it.
- **Right list (x=470):** heading bold 13px `#1a5276` "Each filter shrinks n:", then rows 30px apart starting at y=52 (13px, ↓ arrows between): "Population n = 30,000" / "+ Male n = 15,000" / "+ Age 45–50 n = 1,500" / "+ Income $80–100k n = 330" / "+ Homeowner n = 231" / "+ Bought last 30 days n = 5" (last row bold red `#e74c3c`); n values right-aligned at x=700, bold 13px `#1a5276` (last red).
- **Bottom notes (centered):** bold 12px red "P(A∩B∩C∩D∩E) ≤ P(A) — specificity guarantees rarity" at y=272; 10px `#999` "box area ∝ n (to scale)" at y=289.

---

## 3. McNamara Fallacy

### canvas `c3` — 720×300

Two-column comparison of boxed items on `#f9fafb` background, with a "vs" divider between columns.

- **Left column (orange, header "Easy to Measure"** bold 15px `#e67e22` with 3px orange underline bar): five solid-bordered boxes 240×32 (fill `rgba(230,126,34,0.12)`, stroke `#e67e22` width 1.5), 14px `#2c3e50` centered text: "Body count", "Handle time (sec)", "Lines of code", "Accounts opened", "Test coverage %".
- **Right column (green, header "Hard but Matters"** bold 15px `#27ae60` with green underline bar): five dashed-bordered (4/3) boxes (fill `rgba(39,174,96,0.08)`, stroke `#27ae60`): "Territory control", "Customer satisfaction", "Code quality / maintainability", "Genuine customer trust", "System reliability".
- **Divider:** bold 18px `#999` "vs" centered between columns.
- **Bottom captions (11px):** red under left column "← optimized (but irrelevant)"; green under right column "ignored (but critical) →".

---

## 4. Goodhart's Law

### canvas `c4` — 720×300

Two-line divergence chart over time on `#f9fafb` background.

- **Title (bold 14px `#1a5276`, top center):** "Metric vs Underlying Value Over Time".
- **Axes:** L-shaped `#ccc` axes; plot area padded 80 left, 40 right, 50 top, 60 bottom; rotated y label "Performance" and x label "Time →" (12px `#555`).
- **Target marker:** dashed (4/4) gray `#999` vertical line at 25% of x-range, labeled "Metric becomes target" (11px `#999`) below the axis.
- **Metric line (solid orange `#e67e22`, width 3):** rises gently until t=0.25, then rises steeply afterwards (metric goes UP).
- **Underlying value line (dashed 6/4 green `#27ae60`, width 3):** rises gently until t=0.25, then falls steadily afterwards (value goes DOWN).
- **Legend (at 60% x, near top):** orange solid line + "Reported metric (accounts opened)"; green dashed line + "Underlying value (customer trust)" (12px).
- **Annotation:** bold 12px red "DIVERGENCE" at ~72% x, mid-chart, with short red vertical arrows above and below indicating the widening gap.

---

## 5. Lead Time Bias

### canvas `c5` — 720×300

Dual-timeline comparison over an age axis on `#f9fafb` background.

- **Title (bold 14px `#1a5276`, top center):** "Same Death Date, Different \"Survival\" Due to Earlier Detection".
- **Age axis:** horizontal `#999` line at y=260 spanning padded width (100 left, 40 right); ticks and 12px `#555` labels at ages 40, 50, 55, 60, 65, 70, 75; "Age →" label at right end. Linear scale age 40–75.
- **Death line:** dashed (4/3) red `#e74c3c` vertical line at age 70, labeled bold 12px red "Death: age 70" at top.
- **Bar A (Screen-detected):** rectangle from age 50 to age 70 at y=80, height 30; fill `rgba(26,82,118,0.25)`, stroke `#1a5276` width 2; blue `#2980b9` detection dot radius 5 at left edge; left label bold 13px `#1a5276` "Screen-detected" / 12px "(age 50)"; centered bold 13px annotation "20 yr \"survival\"".
- **Bar B (Symptom-detected):** rectangle from age 65 to age 70 at y=155, height 30; fill `rgba(231,76,60,0.15)`, stroke `#e74c3c` width 2; red detection dot; left label bold 13px `#e74c3c` "Symptom-detected" / 12px "(age 65)"; centered bold 13px red annotation "5 yr \"survival\"".
- **Lead-time bracket:** orange `#e67e22` bracket between age 50 and age 65 at y=125, labeled bold 12px orange "Lead time (15 yr) — NOT extra life".
- **Bottom note (12px `#555`, centered):** "Both patients die at age 70. Earlier detection only moves the diagnosis clock, not the outcome."

---

## Page-specific constraints

- **No tag-pill row on this page.** None of the five sections carries tags; the text column opens with a short lead paragraph instead.
- **Text-column order for this page:** short lead paragraph → labeled-bullet `<ul>` → `.key-point` box → italic `.example` paragraph. The lead paragraph is specific to this page; most detail pages open with the pill row.
- **Boxes:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem, `strong` label inside. `.example` — italic, `#555`, 0.9rem, no box.
- **Canvas height is a uniform 720×300 for all five charts.** Keep them equal — the five sections are read as a set, and one taller panel makes a fallacy look more important than the others.
- **Section 2's counts must stay derived, never typed.** Compute each n from the previous one via the conditional rates `[0.50, 0.10, 0.22, 0.70, 0.022]` and round, so the nested boxes, the right-hand list, the bullet cascade, and the "5 people out of 30,000" line cannot drift apart.
- **Section 2's rectangle scale is `sqrt(n_i / n_0)`, computed at render time** — area, not width, is proportional to n, and the label "box area ∝ n (to scale)" is only honest while that holds.
- **Known degenerate case, section 2:** the n = 5 level would draw at ~5×3px, below legibility. It is deliberately drawn as a red dot with a label rather than a rectangle; do not "fix" this by inflating it into a visible box, which would break the area-proportional claim.
- **Section 4's two lines share a common rise before t=0.25**, then diverge. The pre-target agreement is the point — a chart where the lines diverge from t=0 shows a bad metric, not Goodhart's Law.
- **Section 5's age scale is linear 40–75** so the 20-year and 5-year bars are visually comparable; the 15-year lead-time bracket must line up exactly with the gap between the two detection dots.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
