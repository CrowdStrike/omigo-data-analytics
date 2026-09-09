# DNA Ancestry → Criminal Identification

**Page type:** detail page (backlog-style two-column layout: numbered h2 sections, text left ~45%, canvas right ~55%; CASE STUDY status badge next to h1)
**HTML title tag:** DNA Ancestry Used to Identify Criminals — Case Study

**Status badge (next to h1):** CASE STUDY

**Subtitle:** Investigative genetic genealogy (IGG) — uploading crime-scene DNA to consumer ancestry databases, finding distant relatives, and narrowing to a suspect through family-tree reconstruction.

## Intro callout

**Core idea:** A suspect never submitted their own DNA. But a third cousin did — to a consumer ancestry service. That partial match, combined with public genealogy records, narrows millions of candidates to one person.

## 1. The Method — An Identification Funnel

Each step is a probabilistic filter. No single step identifies anyone — the composition of filters does.

- Crime-scene DNA is sequenced and uploaded to a public genealogy database (e.g., GEDmatch, FamilyTreeDNA)
- The system returns partial matches — not the suspect, but distant relatives
- Investigators build a reverse family tree from each matched relative back several generations
- Trees converge on a small set of living descendants in the right age/geography/sex
- Identity is confirmed with a direct DNA sample (discarded cup, door handle, trash)

**Key point callout:** The final confirmation is deterministic, but everything before it is inference. The funnel does the identification — the direct sample only ratifies it.

### Visualization (canvas `c1`, 720×300)

Horizontal funnel diagram narrowing left to right through five stages.

- **Title (bold 14px, `#1a5276`, top center):** "From Millions of Candidates to One".
- **Stages (left to right), each a trapezoid segment with a count label above (bold 12px, in stage color) and stage label below (11px `#666` at y=262):**
  1. "Population" — count "~10M" — stroke `#1a5276`, fill `rgba(26,82,118,0.35)`
  2. "Relative matches" — count "~20" — stroke `#27ae60`, fill `rgba(39,174,96,0.35)`
  3. "Tree descendants" — count "~1,000" — stroke `#e67e22`, fill `rgba(230,126,34,0.35)`
  4. "Age / sex / geo" — count "~5" — stroke `#8e44ad`, fill `rgba(142,68,173,0.35)`
  5. "Direct sample" — count "1" — stroke `#e74c3c`, fill `rgba(231,76,60,0.35)`
- **Geometry:** funnel spans x 60–660, centered vertically at y=150; segment boundary heights taper 180, 110, 65, 32, 14, 14 px. Light `#ddd` tick lines connect segments to their labels.
- **Final annotation:** bold red `#e74c3c` text "confirmed" under the last stage label (y=282).

## 2. How Little DNA a Match Needs

Shared autosomal DNA halves with each step of relationship distance. By the 3rd-cousin level it is under 1% — yet still detectable and still informative.

- **Parent / sibling:** ~50% shared
- **1st cousin:** ~12.5%
- **2nd cousin:** ~3.1%
- **3rd cousin:** ~0.8% — the typical IGG entry point
- **4th cousin:** ~0.2% — near the noise floor

**Key point callout:** Most people have hundreds of 3rd cousins and don't know who they are. Any one of them submitting a sample creates a searchable path back to you.

### Visualization (canvas `c2`, 720×300)

Bar chart of average shared autosomal DNA by relationship, with rounded-top bars.

- **Title (bold 14px, `#1a5276`, top center):** "Average Shared Autosomal DNA vs. Relationship".
- **Data (label, value, stroke color, fill):**
  - "Parent" 50% — `#1a5276`, `rgba(26,82,118,0.35)`
  - "Sibling" 50% — `#2980b9`, `rgba(41,128,185,0.35)`
  - "1st cousin" 12.5% — `#27ae60`, `rgba(39,174,96,0.35)`
  - "2nd cousin" 3.1% — `#e67e22`, `rgba(230,126,34,0.35)`
  - "3rd cousin" 0.78% — `#e74c3c`, `rgba(231,76,60,0.35)` (highlighted)
  - "4th cousin" 0.2% — `#8e44ad`, `rgba(142,68,173,0.35)`
- **Scale/axes:** linear y 0–50%, plot area x 70–690, y 50–245; recessive `#eee` gridlines with `#999` labels at 0%, 25%, 50%; `#ccc` baseline; bars 56px wide with 4px rounded top corners, minimum height 3px; value labels ("50%", "12.5%", …) bold 11px in the bar color above each bar; category labels 11px `#666` below.
- **Annotation at 3rd cousin:** italic red 11px two-line text "under 1% — still enough" / "to identify you" at y≈175/189, with a dashed red (3/3) vertical guide down toward the bar.

## 3. Family-Tree Reconstruction as Graph Traversal

The genealogy step is graph search: nodes are people, edges are parent-child links. Investigators walk *up* from the matched relative to a common ancestor, then *down* through all descendant branches.

- A 3rd-cousin match implies great-great-grandparents in common
- Every descendant branch of that ancestor couple is a candidate lineage
- Public records (obituaries, census, marriage records) supply the edges
- Multiple independent matches triangulate — trees must converge on the same branch

**Key point callout:** Entity resolution at scale: matching partial genetic markers across millions of profiles is probabilistic, not deterministic. Wrong-branch errors happen when the tree has an undocumented edge (adoption, misattributed parentage).

### Visualization (canvas `c3`, 720×340)

Four-generation family-tree diagram with a highlighted traversal path.

- **Title (bold 14px, `#1a5276`, top center):** "Up From the Match, Down to the Suspect".
- **Nodes:** circles radius 9. Root A at (360,65) labeled "common ancestors" (bold orange `#e67e22` label above, node filled orange). Generation rows: B1(180,130), B2(360,130), B3(540,130); C1(120,195), C2(240,195), C3(330,195), C4(410,195), C5(480,195), C6(600,195); D1(120,262), D2(240,262), D3(410,262), D4(480,262), D5(600,262). Plain nodes white fill with `#aaa` stroke.
- **Special nodes:** D1 filled green `#27ae60`, labeled "3rd cousin — in database" (bold green, below-right); D5 filled red `#e74c3c`, labeled "suspect" (bold red, below).
- **Edges:** A→B1/B2/B3; B1→C1/C2; B2→C3/C4; B3→C5/C6; C1→D1; C2→D2; C4→D3; C5→D4; C6→D5. Default `#ddd` width 1; highlighted path A–B1–C1–D1 in green `#27ae60` width 2 (walk up from the match) and A–B3–C6–D5 in red `#e74c3c` width 2 (walk down to the suspect).
- **Generation labels:** "G0"–"G3" in `#999` 10px at the left margin (x=30) beside each row.
- **Caption (italic 11px, bottom center, mixed colors):** "walk up from the match" (green) "to the shared ancestors, then" (`#666`) "down every branch" (red).

## 4. The Consent Paradox — Coverage Through Relatives

The database doesn't need YOU — it needs enough of your extended family. Identifiability saturates long before enrollment does.

- The suspect never submitted DNA — a relative did, years earlier, for ancestry curiosity
- DNA is shared biology — you cannot opt out of what your relatives share
- Each enrollee exposes hundreds of relatives who never consented
- Researchers estimate that once ~2% of a population is enrolled, nearly everyone has a 3rd-cousin-or-closer match

**Key point callout:** Asymmetric privacy: your genetic privacy depends on decisions made by relatives you may never meet.

### Visualization (canvas `c4`, 720×300)

Saturation curve: share identifiable vs. share enrolled.

- **Title (bold 14px, `#1a5276`, top center):** "Identifiability Saturates Long Before Enrollment Does".
- **Axes:** x 0–5% (labels 0%–5% each 1%), y 0–100% (recessive `#eee` gridlines with `#999` labels at 0/25/50/75/100%); plot area x 80–670, y 50–235; `#ccc` baseline. Axis titles (11px `#666`): x "share of population enrolled in the database"; y rotated "share identifiable via 3rd-cousin-or-closer match".
- **Curve data (qualitative saturation shape, bezier-smoothed):** (0,0), (0.25,10), (0.5,22), (0.75,35), (1,50), (1.5,72), (2,90), (2.5,95), (3,97), (4,99), (5,99.5). Area under curve filled `rgba(26,82,118,0.08)`; stroke width 2.5 with a horizontal gradient `#27ae60` (0) → `#e67e22` (0.3) → `#e74c3c` (0.5 to 1).
- **Threshold marker:** purple `#8e44ad` dashed (4/4) guides from the axes to the point (2%, 90%), a 4.5px purple dot there, and bold 11px purple label "~2% enrolled → ~90% identifiable".
- **Tinted zone:** area right of x=2% shaded `rgba(231,76,60,0.06)` with italic red 10px label "effectively everyone" centered near the top.

## 5. Why This Is a Data Science Problem

One case, five classic data problems — each a discipline in its own right.

- **Entity resolution at scale** (label colored `#2980b9`): matching partial genetic markers across millions of profiles — probabilistic, not deterministic
- **Graph traversal** (label colored `#27ae60`): family trees are graph search — nodes are people, edges are parent-child links
- **Shrinking candidate set** (label colored `#e67e22`): each matched relative adds a constraint — geography, age, sex — exponentially reducing candidates
- **Consent propagation** (label colored `#e74c3c`): one person's submission exposes hundreds of relatives who never consented
- **Coverage threshold** (label colored `#8e44ad`): at ~2% enrollment, nearly everyone is identifiable through relatives

**Key point callout:** **The statistical angle:** Each familial match provides a likelihood ratio, not a certainty. The tree narrows candidates, but confirmation always requires a direct sample. At what point does probabilistic narrowing become de facto identification — and who oversees the intermediate steps?

### Visualization (canvas `c5`, 720×340)

Hub-and-spoke diagram: central IGG hub with five satellite pills on an ellipse.

- **Title (bold 14px, `#1a5276`, top center):** "One Case, Five Data Problems".
- **Hub:** filled `#1a5276` circle radius 42 at (360,190), white text: "IGG" (bold 15px) / "genetic genealogy" (9px).
- **Satellites:** five rounded pills (172×44, radius 8, white base with a 10%-alpha tint of the satellite color, 1.5px colored stroke) placed on an ellipse Rx=215, Ry=100 starting at top and stepping 72°; each connected to the hub by a 1.5px spoke in its color. Pill text: main line bold 12px in satellite color, sub line 10px `#666`:
  1. "Entity resolution" / "partial-marker matching" — `#2980b9`
  2. "Graph traversal" / "trees are graph search" — `#27ae60`
  3. "Candidate shrinking" / "constraints compound" — `#e67e22`
  4. "Consent propagation" / "one submits, hundreds exposed" — `#e74c3c`
  5. "Coverage threshold" / "~2% enrolled is enough" — `#8e44ad`

## 6. Discussion Points

- **Coverage math** (label colored `#1a5276`): how many submissions to cover 90% of a population via familial links?
- **False positive risk** (label colored `#e74c3c`): partial matches have error rates — what happens when the tree points to the wrong branch?
- **Cold case success rate** (label colored `#27ae60`): what's the selection bias in reported successes vs. dead ends?
- **Policy tension** (label colored `#2980b9`): GEDmatch now requires opt-in for law enforcement — after the technique became public
- **Asymmetric privacy** (label colored `#e67e22`): your genetic privacy depends on relatives you may never meet
- **Precedent expansion** (label colored `#8e44ad`): initially reserved for violent crimes — where does the boundary settle?

**Key point callout:** **TODO — sources needed:** Add a sourced case study — the most well-known example is the Golden State Killer identification (2018) via GEDmatch. Confirm details before citing. Other candidates: the BTK Killer daughter's DNA submission, or the Idaho murder case (2022). At minimum, link to a credible news report or the peer-reviewed paper on population coverage thresholds.

### Visualization (canvas `c6`, 720×340)

2×2 quadrant scatter map placing each discussion question on technical↔policy and settled↔open axes.

- **Title (bold 14px, `#1a5276`, top center):** "Where Each Question Sits".
- **Frame:** `#ddd` rectangle x 95–645, y 55–280, with dashed (3/4) midlines splitting quadrants. Quadrant tints: bottom-left `rgba(39,174,96,0.06)` with italic green 10px label "well understood" (bottom-left corner); top-right `rgba(231,76,60,0.06)` with italic red label "most contested" (top-right corner).
- **Axis labels (11px `#666`):** below frame left "technical ←", right "→ policy & ethics"; rotated on left side "settled ←" (bottom) and "→ open" (top).
- **Points:** circles radius 11 (35%-alpha fill, 1.5px colored stroke) with bold 11px `#2c3e50` labels, at fractional (x, y) positions where x=0 technical/x=1 policy and y=0 settled/y=1 open; labels sit 24px below the dot unless noted:
  - "Coverage math" (0.13, 0.30) — `#1a5276`, `rgba(26,82,118,0.35)`
  - "False positives" (0.30, 0.72) — `#e74c3c`, `rgba(231,76,60,0.35)`
  - "Cold-case bias" (0.45, 0.55) — `#27ae60`, `rgba(39,174,96,0.35)`
  - "Policy tension" (0.72, 0.40) — `#2980b9`, `rgba(41,128,185,0.35)`
  - "Asymmetric privacy" (0.82, 0.70) — `#e67e22`, `rgba(230,126,34,0.35)`
  - "Precedent expansion" (0.62, 0.90) — `#8e44ad`, `rgba(142,68,173,0.35)` (label 16px above the dot)

## Regeneration instructions

- **Layout:** backlog-style detail page. h1 with inline `.status` badge, `.subtitle`, one `.intro-callout`, then one `.card-section` per numbered h2; inside each, `table.layout` with a single row: left `td.text-col` (45%) holding a lead paragraph, bullets, and a `.key-point` callout, right `td.viz-col` (55%) holding the canvas.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; section h2 1.3rem `#1a5276` with the same 2px `#2980b9` bottom border; subtitle `#666` 0.95rem; bullets 0.92rem; some bullet lead-ins use inline colored `<strong style="color:...">`.
- **Callouts:** `.intro-callout` — background `#f8f9fa`, left border `3px solid #2980b9`, padding 10px 14px, 0.93rem. `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Status badge:** `.status` inline-block, background `#fef9e7`, border `1px solid #f39c12`, color `#b7950b`, padding 2px 10px, radius 4px, 0.8em, text "CASE STUDY".
- **Canvas:** each declared 720×(300 or 340 as specified) with `width: 100%`, border `1px solid #e0e0e0`, radius 4px; scaled via a shared `setup(id, hgt)` helper using `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar/point fills as 35%-alpha rgba of the stroke color. No nav bar, no back/home links. (Any links in regenerated HTML use `.html` extensions.)
