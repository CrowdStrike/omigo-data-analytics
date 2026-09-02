# 1. Decoy Effect (Asymmetric Dominance)

**Page type:** detail page (numbered h2 sections, each an obj-table row: text left 45% with math-boxes and bullets, canvas right 55%; philosophy callouts at top and bottom)
**HTML title tag:** 1. Decoy Effect (Asymmetric Dominance)

**Subtitle:** Introduce an option nobody should pick — and watch it change what people DO pick. The decoy doesn't compete. It reframes the comparison.

## Callout (philosophy box, top)

**Formal name:** Asymmetric Dominance Effect (Huber, Payne & Puto, 1982). A third option that is clearly worse than one alternative (but not the other) shifts preference toward the option that dominates it.

## 1. The Movie Theater Popcorn

**Obj-title:** The Classic Setup

**Math-box 1:**
**The menu:**

Small: `$3.99` (4 oz)
Medium: `$7.99` (8 oz)
Large: `$8.99` (12 oz)

**Math-box 2:**
**What your brain does:**

**Without the medium:**
Small $3.99 vs Large $8.99 → "$5 more? I'll just get small."

**With the medium:**
Medium $7.99 vs Large $8.99 → "Only $1 more for 50% more popcorn? Large is a no-brainer."

The medium `reframes which comparison you make`.

- **The medium is the decoy.** It's not there to be bought.
- **It exists to make large look like a bargain** by being the worse deal nearby.
- **Without it:** most people buy small. **With it:** most people buy large.

### Visualization (canvas `canvas1`, 720×380)

Pictorial diagram: three popcorn containers drawn as trapezoids of increasing size, with the medium flagged as the decoy.

- **Title (bold 14px `#1a5276`, centered):** "Movie Theater Popcorn Pricing" at y=22.
- **Containers** (trapezoids on baseline y=280, yellow `#f4d03f` popcorn dots inside each):
  - SMALL at x=130: height 100, top width 50, bottom width 35, color `#27ae60` (fill color+`22` alpha), price "$3.99", "4 oz", "$1.00/oz".
  - MEDIUM at x=330: height 140, top width 65, bottom width 45, decoy — stroke `#e74c3c`, fill `rgba(231,76,60,0.15)`, price "$7.99", "8 oz", "$1.00/oz".
  - LARGE at x=530: height 180, top width 80, bottom width 55, color `#2980b9`, price "$8.99", "12 oz", "$0.75/oz".
- **Per container (centered under it):** label bold 14px (decoy label in `#e74c3c`, others `#1a5276`), price bold 18px, size and per-oz 12px `#666`.
- **Decoy annotation (centered at x=330, `#e74c3c`):** "← THE DECOY →" bold 12px at y=50; "Nobody buys this." 11px at y=66; "It exists to make Large look like a steal." at y=80.
- **Green dashed arrow** (`#27ae60`, dash 5/3, width 2) from x=380 to x=480 at the price row, labeled "Only $1 more!" bold 11px `#27ae60`.

## 2. The Mechanism

**Obj-title:** Asymmetric Dominance

**Math-box 1:**
**The rule:**

Option C (decoy) is *dominated* by Option B (target) — worse on every dimension.
Option C is NOT dominated by Option A (competitor) — it's worse on some dimensions, better on others.

Result: people prefer B over A, even though C is irrelevant to that choice.

**Math-box 2:**
**In the popcorn example:**

- Large dominates Medium (more popcorn, barely more money)
- Small does NOT dominate Medium (less popcorn, less money — tradeoff)

So the medium makes Large look good, without making Small look bad.
Net effect: `preference shifts toward Large`.

- **Key insight:** People don't evaluate options in isolation. They evaluate relative to what's nearby.
- **The decoy gives you a "reason" to pick the target.** "It's better than the medium" feels like rational justification.
- **Without the decoy:** no easy comparison → people default to cheapest.

### Visualization (canvas `canvas2`, 720×380)

Two-axis scatter plot (Quantity → on x, Value for Money → on y) with three labeled points and a dominance arrow.

- **Title (bold 14px `#1a5276`, centered):** "How the Decoy Shifts Preference" at y=22.
- **Axes:** origin (120, 320), plot width 480, height 250, stroke `#1a5276` width 2. X label "Quantity →" centered below; y label "Value for Money →" rotated −90° at x=40.
- **Points** (10px radius filled circles, bold 12px labels above):
  - Small at fraction (0.2, 0.8), color `#27ae60`, label "Small".
  - Medium at (0.5, 0.35), color `#e74c3c`, label "Medium (Decoy)".
  - Large at (0.85, 0.7), color `#2980b9`, label "Large (Target)".
- **Dominance arrow:** red `#e74c3c` dashed (5/3) line width 2 from Medium to Large, labeled in 11px `#e74c3c`: "Large dominates Medium" / "(more quantity + better value)".
- **Non-dominance note** (11px `#666`, midway between Small and Medium): "Small ≠ dominates Medium" / "(tradeoff: less quantity, better value)".
- **Bottom takeaway (12px `#1a5276`, centered):** "The decoy is dominated by the target — making the target look like the obvious winner."

## 3. Where You See This Every Day

**Obj-title:** Real-World Applications

**Math-box 1:**
**SaaS pricing pages:**
Basic: $9/mo, Pro: $29/mo, Enterprise: $49/mo
The "Pro" tier is designed to be the target. "Basic" is too limited, "Enterprise" is overkill. Pro looks like the sweet spot — but that sweet spot was engineered by placing the other two around it.

**Math-box 2:**
**The Economist subscription (famous study):**

- Digital only: $59
- Print only: $125
- Print + Digital: $125

Print-only is the decoy. Nobody picks it — but its presence makes Print+Digital look like a steal ("I get digital FREE!"). Without the decoy, most chose digital-only. With it, most chose print+digital.

**Math-box 3:**
**Real estate:**
Agent shows you 3 houses. One is slightly worse AND same price as another. You pick the one that dominates it — feeling like you made a rational comparison. The dominated house was shown specifically to make the target look good.

- **Electronics:** Three TV models where the middle one is clearly worse than the expensive one for nearly the same price
- **Restaurants:** An expensive wine on the menu exists to make the second-most-expensive one feel reasonable
- **Job offers:** Companies sometimes present a weaker internal candidate to make the preferred hire look like the obvious choice

### Visualization (canvas `canvas3`, 720×380)

Two-panel horizontal bar comparison of purchase distribution with vs without the decoy, plus revenue comparison.

- **Title (bold 14px `#1a5276`, centered):** "What People Actually Buy (With vs Without Decoy)" at y=22.
- **Panels:** left panel at x=60, right at x=380, each 280 wide; headers bold 13px `#666`: "WITHOUT Medium (Decoy)" and "WITH Medium (Decoy)".
- **Bars:** horizontal, 40px tall, 20px gap, max width 220px scaled by percent, fill color+`44` alpha, stroke color width 1.5; label 12px `#333` at left, percent bold 14px in bar color at bar end.
  - Without decoy: Small 70% `#27ae60`, Large 30% `#2980b9`.
  - With decoy: Small 20% `#27ae60`, Medium 5% `#e74c3c`, Large 75% `#2980b9`.
- **Revenue comparison (centered):** "Per 100 customers:" 12px `#666`; "Revenue: ~$550" (left panel) and "Revenue: ~$755" (right panel) bold 13px `#333`; "+37% revenue" bold 14px `#27ae60` under right panel; "just by adding an option nobody buys" 12px `#666`.
- **Bottom line (12px `#1a5276`, centered):** "The Economist study showed similar results: adding a decoy shifted 80% of choices to the premium tier."

## Callout (philosophy box, bottom)

**The defense:** Ask yourself — "If I remove the middle option, would I still make the same choice?" If the answer is no, the middle option was doing its job on you. Evaluate each option against your needs, not against each other.

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, top `.philosophy` callout, then numbered `<h2>` sections (1.4em `#1a5276`, 2px solid `#2980b9` bottom border), each containing a `.obj-table` with one `<tr>`: left `<td>` (45%) holds `.obj-title`, `.math-box` divs, and a `<ul>`; right `<td>` (55%, centered) holds the canvas. Closing `.philosophy` callout after the last section.
- **Page style:** body -apple-system/Segoe UI sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; `.math-box` background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em, with `code` background `#eef2f7`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="380"` attributes; scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, popcorn yellow `#f4d03f`, grays `#666`/`#333`.
- Note: in regenerated HTML, any card links would use `.html` extensions.
