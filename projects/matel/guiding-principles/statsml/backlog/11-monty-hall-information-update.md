# Monty Hall as Information Update Paradox

**Page type:** detail page (backlog 2-col layout: text left 45%, canvas right 55%, one `table.layout` row per `.card-section`)
**HTML title tag:** Backlog: Monty Hall as Information Update Paradox

**Subtitle:** Backlog item for 29-statistical-paradoxes. Hard to detect, hard to explain, but real.

## 1. The Classic Setup

Three doors. One has a prize. You pick door 1. The host (who knows what's behind each door) opens door 3 — it's empty. Should you switch to door 2?

- **Yes — switch.** Switching wins 2/3 of the time.
- Staying wins 1/3 — exactly your original pick's prior.
- Most intuition says 50/50 after the reveal.
- The host's action supplied new information that reshaped the probability landscape.

**Key-point callout (red left border):**
**The mechanics:** Your door was locked in at 1/3 before the reveal and nothing about it changed. The reveal only re-scored the doors you did *not* pick.

*Example (italic):* Example: Run it 300 times — stay wins about 100, switch wins about 200.

### Visualization (canvas `c1`, 720×300)

Two-panel grouped bar chart: probability mass before vs after the reveal.

- **Title (bold 14px, `#1a5276`, top center):** "Probability Mass: Before vs After the Reveal".
- **Y gridlines:** light `#f0f0f0` lines at 0%, 33%, 67%, 100% (labels 9px `#999`, right-aligned; computed as round(g·100/3) for g=0..3). Baseline at y=236 conceptually (base=248, bar area 150px tall).
- **Left panel (x=40, width 280), title "Before reveal" (bold 11px `#1a5276`):** three bars 56px wide — "Door 1" (sub "your pick", 33.3%, stroke `#1a5276`), "Door 2" (33.3%, stroke `#27ae60`), "Door 3" (33.3%, stroke `#e67e22`). All bars filled `rgba(26,82,118,0.35)`, stroke width 2, value label "1/3" bold 10px in the bar's stroke color above each bar. Gray `#ccc` baseline under each panel.
- **Right panel (x=400, width 280), title "After host opens Door 3":** "Door 1" (sub "stay", 33.3%, `#1a5276`, label "1/3"), "Door 2" (sub "switch", 66.7%, `#27ae60`, label "2/3"), "Door 3" (sub "empty", 0% — drawn as a small dashed red outline rectangle 12px tall, dash 3/3, with bold red "0" above it).
- **Bar labels:** door name 10px `#2c3e50` below baseline; sub-label 9px `#888` beneath.
- **Panel divider:** vertical `#e0e0e0` line at x=360.
- **Caption (11px, `#888`, bottom center):** "Your door keeps its 1/3 — only the doors you did not pick get re-scored".

## 2. Why It's Hard

The core issue: **new information arrived mid-decision, and ignoring it is the mistake.**

- People treat the situation as if nothing changed — "it's still just two doors".
- But the host's reveal was **not random** — it was informed AND constrained.
- His elimination can land on neither the prize nor your door, so it carries information about the doors you did not pick — and none about yours.
- That makes the remaining unpicked door more likely to be the winner.

**Key-point callout (red left border):**
**The trap:** The eliminated option's probability mass does not split evenly across survivors. It flows entirely to the option you did NOT pick.

*Example (italic):* Example: Door 3's 1/3 does not become 1/6 + 1/6 — all of it lands on door 2.

### Visualization (canvas `c2`, 720×300)

Flow diagram: three door boxes with the eliminated probability mass flowing to the unpicked door.

- **Title (bold 14px, `#1a5276`, top center):** "Where the Eliminated 1/3 Goes".
- **Boxes:** three rectangles 140×78 at y=120, fill `rgba(26,82,118,0.06)`, stroke width 2:
  - x=45: "DOOR 1" (stroke `#1a5276`), sub "you picked" (10px `#888`), value "1/3" (bold 13px `#2c3e50`).
  - x=290: "DOOR 2" (stroke `#27ae60`), sub "not picked", value "1/3 → 2/3".
  - x=535: "DOOR 3" (stroke `#e74c3c`), sub "host opened", value "1/3 → 0".
- **Flow arrow:** green (`#27ae60`, width 2.5) bezier curve arcing above from Door 3's top (605, 120) to Door 2's top (375, 120), with a filled green arrowhead; label "all 1/3 flows here" (bold 11px, `#27ae60`) above the arc at (490, 55).
- **Blocked flow:** dashed gray (`#ccc`, dash 4/4, width 2) bezier arcing below from Door 3's bottom to Door 1's bottom; crossed out mid-path by a red X (`#e74c3c`, width 2, around x=352–368, y=240–256); label "nothing flows back to your pick" (10px, `#e74c3c`) to the right of the X.
- **Caption (11px, `#888`, bottom center):** "Informed, constrained elimination redistributes unequally — not 1/6 and 1/6".

## 3. Data / Business Parallels — Where the Analogy Breaks

The Monty Hall transfer is rarer in business than it looks. It needs an eliminator who **(a) knows the answer** and **(b) is forbidden to touch your pick**. Most real eliminations fail one or both — they are evidence about the eliminated option itself, and the survivors simply renormalize, keeping their relative order:

- **A/B/C test mid-flight:** Variant C proves significantly worse (p<0.001). That is evidence about C, not about A vs B — their relative odds are unchanged (an even split stays even). The right update is operational: reallocate C's traffic so A vs B resolves faster. It is NOT "switch to the variant you didn't favor".
- **Hiring pipeline:** One of three candidates bombs the technical round. Bombing is evidence about that candidate only — the remaining two keep their original relative ranking, renormalized. No probability mass secretly flows to the underdog.
- **Troubleshooting:** You definitively rule out one of three root causes. The other two do NOT swap places — whichever you suspected more is still ahead, at a higher absolute probability. Boosting the long shot "because the space narrowed" is the false Monty Hall.
- **Where it DOES transfer:** an informed, constrained eliminator. A rival lead who knows which design will win the review is told to cut one option — and politics forbid cutting yours. The option they spared absorbs the eliminated odds; your own pick learns nothing.

**Key-point callout (red left border):**
**Impact:** Two distinct errors. Ignoring the elimination (a third of traffic still serving a known loser) wastes budget. Over-applying Monty Hall (promoting the long shot after an ordinary evidence-based elimination) corrupts the ranking. Act on the information — but shift odds unevenly only when the eliminator was informed AND constrained.

### Visualization (canvas `c3`, 720×380)

Two-row bar-panel diagram: A/B/C test traffic before and after variant C is ruled out.

- **Title (bold 14px, `#1a5276`, top center):** "A/B/C Test: Traffic After Variant C Is Ruled Out".
- **Row 1 (panel x=180, width 360, baseline y=150, bar area 70px), title "Week 1 — traffic split evenly" (bold 11px `#1a5276`):** bars A 33% (`#1a5276`), B 33% (`#27ae60`), C 33% (`#e67e22`); bar width 46, fill `rgba(26,82,118,0.35)`, stroke width 2, percentage labels bold 10px in the bar's color, letter labels 10px `#2c3e50` below the gray `#ccc` baseline.
- **Information banner:** horizontal dashed orange line (`#e67e22`, dash 4/3) across the page at y=196; centered bold 11px orange text just above it: "INFORMATION ARRIVES: C is significantly worse (p < 0.001)".
- **Row 2, left panel (x=50, width 300, baseline y=340, bar area 80px), title "Frozen: \"let the test run\"" (bold 11px, `#e74c3c`):** A 33%, B 33%, C 33% where C is "dead" — dashed red outline (dash 3/3, width 1.5) with fill `rgba(231,76,60,0.15)` and red percentage label. Note below (10px, `#e74c3c`): "a third of traffic spent on a known loser".
- **Row 2, right panel (x=370, width 300), title "Updated: reallocate C's traffic" (bold 11px, `#27ae60`):** A 50% (`#1a5276`), B 50% (`#27ae60`), C 0% (dead). Note (10px, `#27ae60`): "same budget, faster resolution on A vs B".
- **Divider:** vertical `#e0e0e0` line at x=360 between the two row-2 panels (y 210–356).
- **Caption (11px, `#888`, bottom center):** '"Let it run" is a decision too — it just refuses the information update'.

## 4. Why It's Hard to Detect in Practice

Unlike most paradoxes, this one hides in the timing rather than in the math:

- The "new information" often arrives subtly — a competitor launches, a segment churns, a feature gets deprecated.
- Teams don't re-evaluate probabilities when options are eliminated — they treat remaining options as if nothing changed.
- The original decision framework stays frozen while the world updated around it.
- It requires you to notice that information ARRIVED and then ACT on it.

**Key-point callout (red left border):**
**Detection cue:** Any time an option leaves the candidate set for a reason, the priors on the survivors are stale until you recompute them. Usually the recompute is a plain renormalization — the uneven Monty-style shift applies only when the eliminator was informed and constrained.

*Example (italic):* Example: A roadmap ranked in January is still being executed in June, after two of its assumptions were falsified.

### Visualization (canvas `c4`, 720×300)

Step-line divergence chart: world information state vs a frozen decision framework.

- **Title (bold 14px, `#1a5276`, top center):** "Frozen Framework vs Updated World".
- **Plot area:** x=70, width 590, y=96, height 140; gray `#ccc` L-shaped axes; rotated y-axis label "Information state" (10px, `#666`).
- **Data:** world information state step line at values `[10, 32, 48, 66, 82, 94]` (% of plot height) over 6 time points t0–t5; frozen framework flat line at 10.
- **World line:** red `#e74c3c` step line, width 2.5. **Frozen line:** blue `#1a5276` horizontal line, width 2.5.
- **Divergence shading:** area between the two lines filled `rgba(231,76,60,0.10)`.
- **Event markers:** orange `#e67e22` dots (radius 3.5) at t1–t5 on the world line, each with a thin `#ccc` leader line up to two-line 9px orange labels: "competitor / launches", "segment / churns", "feature / deprecated", "option / eliminated", "priors / now stale".
- **X-axis labels:** "t0"…"t5" in 9px `#999`.
- **Legend (below axis):** red line sample + "World information state"; blue line sample + "Decision framework (frozen at t0)" (10px, `#2c3e50`).
- **Caption (11px, `#888`, bottom center):** "The shaded gap is unused information — it grows silently".

## 5. Potential One-Liner

**"When an option is eliminated by someone who knows the answer — and who wasn't allowed to touch your pick — the remaining options are no longer equally likely."**

- TWO load-bearing clauses: the eliminator **knew the answer** and **could not eliminate your pick** (or the prize).
- Both constraints hold → stay 1/3, switch 2/3.
- Drop either one — a random opener that happened to show an empty door, or a knowing host who was free to open your door — and it's genuinely 1/2 vs 1/2.
- Same visible end state, different probabilities — the difference is who chose, what they knew, and what they were forbidden to do.

**Key-point callout (red left border):**
**Ask first:** Was the option removed by a process that knew the answer AND had to steer around your pick? If yes, switch. If it was removed by evidence about itself, or by chance, the survivors just keep their old relative odds.

*Example (italic):* Example: A variant killed by its own bad results is evidence about that variant — the survivors renormalize evenly. A stakeholder who knows all the results kills one variant but can't touch your favorite — that's Monty, and the spared variant got stronger.

### Visualization (canvas `c5`, 720×300)

Two-panel bar comparison: informed vs accidental elimination.

- **Title (bold 14px, `#1a5276`, top center):** "Who Eliminated the Option? Informed vs Accidental".
- **Y gridlines:** `#f0f0f0` lines at 0/25/50/75/100% with 9px `#999` labels; baseline y=236, bar area 140px.
- **Left panel (x=60, width 260):** title "Host KNOWS & must avoid your door" (bold 11px, `#27ae60`), sub "informed, constrained elimination" (10px, `#888`). Bars 70px wide: "Stay" 33.3% (label "1/3", stroke `#1a5276`) and "Switch" 66.7% (label "2/3", stroke `#27ae60`); fill `rgba(26,82,118,0.35)`, stroke width 2, value labels bold 11px. Verdict below (bold 11px, `#27ae60`): "→ SWITCH".
- **Right panel (x=400, width 260):** title "Door opened at random" (bold 11px, `#e67e22`), sub "happened to be empty". Bars: "Stay" 50% ("1/2", `#1a5276`) and "Switch" 50% ("1/2", `#e67e22`). Verdict (bold 11px, `#e67e22`): "→ INDIFFERENT".
- **Divider:** vertical `#e0e0e0` line at x=360.
- **Caption (11px, `#888`, bottom center):** "Identical visible outcome — the eliminator's knowledge and constraints change the odds".

## Status callout (philosophy box, after section 5)

**Status:** Backlog. This is a real and important paradox but difficult to explain visually without the classic game-show framing. Needs a data-native example that makes the "information update" visceral — and one that respects the boundary: the uneven transfer requires an informed, constrained eliminator. The key insight for a layman: "when the eliminator knew the answer and couldn't touch your pick, the freed-up probability flows entirely to the option you DIDN'T pick; when the elimination was blind or was evidence about the eliminated option itself, the survivors just renormalize."

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col layout). Structure: h1, `.subtitle` paragraph, then one `.card-section` per section, each with an `<h2>` and a `table.layout` single `<tr>`: left `<td class="text-col">` (45%) with paragraph/bullets/key-point/example, right `<td class="viz-col">` (55%) with the canvas. A `.philosophy` status callout follows the last section. No index number in the h1, no `.intro` block on this page.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9rem. `ul` 0.92rem; `strong` colored `#1a5276`. Canvas `width: 100%`, border `1px solid #e0e0e0`, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fill `rgba(26,82,118,0.35)`, muted gray `#888`/`#999`.
- **Canvas:** canvases c1, c2, c4, c5 are 720×300 via a shared `setup(id)` helper; c3 is 720×380 with inline setup. All scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
