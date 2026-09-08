# Legal / Compliance: Domain-Specific Pitfalls

**Page type:** detail page (h2 section per pitfall, each with a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Domain Pitfalls: Legal / Compliance

**Subtitle:** Legal data is uniquely adversarial — selection bias in case law, jurisdiction effects, mandatory explainability, and interpretive attacks on model outputs.

## Callout (philosophy box)

**Core challenge:** Legal systems are built on precedent, interpretation, and adversarial scrutiny. Models trained on case law inherit survivorship bias (only litigated cases exist in datasets), while any deployed model must withstand cross-examination by opposing counsel seeking to discredit its logic.

## Selection Bias in Cases

**Only Litigated Cases Are Visible**

Legal datasets overwhelmingly consist of cases that went to trial or produced written opinions. The vast majority of disputes — those settled, dropped, or resolved informally — leave no trace in training data.

- **Iceberg effect:** ~95% of civil cases settle before trial
- Litigated cases are systematically different (higher stakes, unclear law, stubborn parties)
- Models trained only on opinions will overweight contentious edge cases
- Settlement patterns contain critical information about "obvious" outcomes

### Visualization (canvas `canvas1`, 720×300)

Iceberg diagram: visible litigated cases above the waterline, invisible settled cases below.

- **Waterline:** horizontal dashed blue line (`#2980b9`, dash 6/4, width 2) at y=65 from x=40 to x=w-40; area below filled light blue `#e8f4fd`.
- **Iceberg tip (above water):** dark blue `#1a5276` triangle, apex at (center, 10), base 100px wide at the waterline; white 17px label "5%" inside.
- **Iceberg body (below water):** light blue `#85c1e9` trapezoid from the 100px base widening to 280px at y=h-15; dark blue outline `#1a5276` 1.5px around the whole iceberg; `#1a5276` 17px label "95%" inside at y=140.
- **Side labels (14px, left-aligned right of iceberg):** `#1a5276` two lines "Litigated cases" / "(in datasets)" at y≈40–57; `#2980b9` two lines "Settled / dropped cases" / "(invisible to models)" at y≈120–137.
- **Waterline label:** red `#e74c3c` 12px right-aligned just above the waterline, left of the iceberg: "DATA VISIBILITY LINE".

## Jurisdiction Dominates Outcome

**Same Law, Different Interpretation by Court**

Identical factual patterns can yield opposite outcomes depending on which court hears the case. Circuit splits, state-level variation, and judicial philosophy create systematic jurisdiction effects.

- **Circuit splits:** Federal circuits can disagree for decades
- State courts apply "same" common law doctrines differently
- Judge-specific tendencies (strict vs. lenient sentencing)
- Venue shopping exploits these differences strategically

### Visualization (canvas `canvas2`, 720×300)

Grouped bar chart: win rate by jurisdiction for the same claim.

- **Title (14px `#1a5276`, top center):** "Same Patent Claim — Win Rate by Jurisdiction".
- **Categories (x-axis):** "9th Circuit", "5th Circuit", "2nd Circuit", "11th Circuit".
- **Data:** Plaintiff wins `[78, 32, 61, 25]` %; Defendant wins `[22, 68, 39, 75]` %.
- **Bars:** two per group, 28px wide, 12px gap; plaintiff bars green `#27ae60`, defendant bars red `#e74c3c`; baseline at y=170, max bar height 130px (100%); value labels ("78%", etc., 11px) in the matching bar color above each bar; court name labels 12px `#333` below the baseline.
- **Axes:** horizontal baseline `#ccc`; y-axis labels 0%–100% every 25% in gray `#666` 12px with light `#eee` gridlines.
- **Legend (top right):** green swatch "Plaintiff wins", red swatch "Defendant wins" (12px `#333`).

## Temporal Invalidity

**Law Overturned — All Precedent Instantly Worthless**

Unlike most domains where historical data retains partial relevance, a single Supreme Court decision or legislative repeal can instantly invalidate decades of precedent and all models trained on it.

- **Cascade failure:** One ruling can invalidate thousands of downstream decisions
- No gradual deprecation — change is instantaneous and retroactive
- Models cannot predict their own obsolescence
- Training data becomes actively misleading, not just stale

### Visualization (canvas `canvas3`, 720×300)

Timeline diagram: precedents struck through before an overturn event, new valid law after.

- **Timeline axis:** horizontal `#1a5276` line width 2 at y=100 from x=60 to x=w-40, with a filled arrowhead at the right end.
- **Invalidation zone:** faint red band `rgba(231, 76, 60, 0.08)` behind the timeline from x=60 to the overturn marker (x=500), 60px tall.
- **Precedent dots (invalidated):** red `#e74c3c` 6px-radius dots at x = 110, 180, 240, 310, 370, 430 on the timeline, each with a red diagonal strike-through line (width 2) across it.
- **Overturn event at x=500:** thick red vertical line (width 3) spanning ±45px around the timeline, plus a filled red lightning-bolt polygon over it; red 17px label below: "OVERTURNED".
- **Post-overturn zone:** faint green band `rgba(39, 174, 96, 0.15)` from x=520 to near the arrow end, 60px tall, with two green `#27ae60` 6px dots at x=560 and x=610.
- **Labels (13px, centered):** red "All prior precedents invalidated" above the invalidation zone at x=270; green "New law applies" above the green zone at x=585.
- **Year markers (11px gray `#666`):** "1990" at x=110, "2000" at x=240, "2010" at x=370 below the timeline; "2023" under the overturn marker; "Now" at x=585.

## Explainability Mandatory

**"Algorithm Said Deny" Is Legally Insufficient**

In regulated domains (credit, employment, housing), decisions must be explainable to affected parties. Due process requires reasons, not just outcomes. Black-box models are legally non-compliant regardless of accuracy.

- **Legal requirement:** ECOA, FCRA, GDPR Art. 22 mandate explanations
- Adverse action notices must cite specific reasons
- "The model decided" is not a legally defensible position
- Accuracy without interpretability has zero legal value

### Visualization (canvas `canvas4`, 720×300)

Side-by-side flow diagrams: black-box vs explainable decision paths, split by a dashed divider.

- **Divider:** vertical dashed light gray line (`#e0e0e0`, dash 4/4) at x = w/2+10.
- **Left side, header 14px `#1a5276`:** "BLACK-BOX MODEL".
  - Input box: `#f0f4f8` fill with `#2980b9` border, 80×30 at (30,40), label "Application" (12px `#333`).
  - Gray arrow → black box: solid dark `#2a2a2a` rectangle 80×40 with white 14px "? ? ?" centered.
  - Gray arrow → output box: red `#e74c3c` 80×30 with white 13px "DENIED".
  - Below: red 17px text "Legally Insufficient" and a large red 28px "✘".
- **Right side, header 14px `#1a5276`:** "EXPLAINABLE MODEL".
  - Input box: same style, "Application".
  - Decision tree: root node "Income < 30k?" (90×24 box, fill `#eaf2e8`, border `#27ae60`, 10px `#333` text) with two green branch lines to child nodes "DTI > 45%?" and "History?" (65×22 boxes, same style).
  - Gray arrow → output box: green `#27ae60` 95×55 with white text, three lines: "DENIED:" (11px), "1. High DTI ratio", "2. Short history" (10px).
  - Below: green 17px text "Legally Compliant" and a large green 28px "✔".

## Adversarial Interpretation

**Opposing Counsel Will Attack Any Model Weakness**

Unlike academic peer review, legal adversaries are incentivized to find and exploit every flaw. Any limitation acknowledged in documentation becomes ammunition. Models must survive hostile cross-examination.

- **Daubert standard:** Expert testimony (including models) must survive challenge
- Known error rates will be magnified ("wrong 5% of the time = wrong for my client")
- Training data provenance will be questioned
- Every assumption becomes a line of attack

### Visualization (canvas `canvas5`, 720×300)

Attack diagram: central model box with adversarial arrows converging on weakness points from all corners.

- **Central box:** 140×70 at canvas center, fill `#f0f4f8`, border `#1a5276` width 2; inside, `#1a5276` 17px two-line text "YOUR" / "MODEL".
- **Weakness points:** five red `#e74c3c` 5px-radius dots on the box perimeter (left edge, right edge, two on the bottom edge, top center).
- **Attack arrows:** orange `#e67e22` lines width 2 with filled orange arrowheads, from five sources — four corners (x=80/w-80, y=35/h-35) and top center (cx,10) — pointing at the weakness dots (stopped 12px short of each dot).
- **Attack labels (orange 11px, two lines each, near their source; above source when in top half, below when in bottom half):**
  - "Cross-examination:" / "\"Biased sample?\"" (top left)
  - "Expert challenge:" / "\"R² only 0.6?\"" (top right)
  - "Statistical attack:" / "\"Wrong 1-in-20\"" (bottom left)
  - "Omitted variable:" / "\"Ignored key factor\"" (bottom right)
  - "Temporal challenge:" / "\"Law changed since\"" (top center)
- **Caption (bottom center, red 12px):** "Opposing counsel targets every documented weakness".

## Regeneration instructions

- **Layout:** h1 + `.subtitle` + `.philosophy` callout, then one `h2` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by a `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.obj-title` (1.05em, weight 600, `#1a5276`), an intro paragraph, and a `<ul>` of bullets; right `<td>` (55%, centered) holds the canvas.
- **Table style:** full width, border-collapse; cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; paragraphs `#333` 0.95em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, light blue `#85c1e9`/`#e8f4fd`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
