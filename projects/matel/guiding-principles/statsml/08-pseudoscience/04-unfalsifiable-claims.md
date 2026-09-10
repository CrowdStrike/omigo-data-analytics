# Unfalsifiable Claims

**Page type:** detail page (two-column obj-table layout: text left 40%, canvases right 60%, single section)
**HTML title tag:** Unfalsifiable Claims — Pseudoscience in Data Analysis

**Subtitle:** Claims structured so no possible evidence could disprove them

## Section 1: "It Works — You Just Need More Data / More Time / To Believe Harder"

- **Homeopathy:** "The study didn't show an effect? The sample was too small, the instrument wasn't sensitive enough, and you have to take it for 6 months first." No possible evidence could disprove the claim — it's unfalsifiable.
- **ML model:** "Accuracy is only 52%? We need more training data, more features, more epochs, a bigger model, different hyperparameters." If no result could make you conclude the signal doesn't exist and abandon the approach, it's faith, not science.
- **Business strategy:** "OKRs didn't improve team performance? You're not doing them right — better alignment, more check-ins, different cadence." The methodology can never fail, only the practitioner — unfalsifiable by design.
- **Technical debt:** "The rewrite didn't fix everything? We need another rewrite — the first one didn't go far enough." An infinite regress where the cure for failure is more of the same treatment.

**Why it's pseudoscience:** Karl Popper's demarcation criterion says science must be falsifiable — if you observe the evidence that should make you abandon the claim and still don't, you're not doing science. Pre-define failure criteria: "If accuracy is <60% after 100K samples, the signal doesn't exist and we stop."

### Visualization (canvas `c1`, 720×300)

Stacked list of excuse rows showing that every piece of counter-evidence gets deflected.

- **Title (bold 17px, `#1a5276`, top center):** "Unfalsifiable: No Possible Evidence Could Disprove the Claim".
- **Rows:** five rows starting at y=50, step 30px; each is a faint red band (fill `#e74c3c` at globalAlpha 0.08, 520×25 at x=100) with left-aligned 17px `#e74c3c` text at x=120: "Evidence against → Excuse: "<excuse>"" for each excuse in order: "Sample too small", "Wrong measuring instrument", "Need more time", "Not doing it RIGHT", "External factors interfered".
- **Bottom annotation (bold 17px `#e74c3c`, centered):** "If NO evidence could make you abandon the claim → it's faith, not science."

### Visualization (canvas `c2`, 720×300)

Horizontal gradient spectrum bar from falsifiable (good science) to unfalsifiable (not science), with example claims plotted along it.

- **Title (bold 17px Arial, `#1a5276`, top center):** "Falsification Spectrum".
- **Gradient bar:** at x=60, y=55, width w-120, height 30; linear gradient left to right: `#27ae60` (0) → `#f39c12` (0.5) → `#e74c3c` (1); stroke `#2c3e50` width 1.
- **End labels (bold 18px Arial):** left in `#27ae60`: "Highly Falsifiable" above the bar, "(good science)" below; right-aligned in `#e74c3c`: "Unfalsifiable" above, "(not science)" below.
- **Examples plotted on the spectrum** (each with a colored dot radius 5 on the bar, a colored marker line dropping below it, and a centered bold 17px multi-line label):
  - Position 0.05, `#27ae60`: "Drug trial" / "with placebo".
  - Position 0.3, `#2ecc71`: "Specific ML" / "accuracy target".
  - Position 0.7, `#e67e22`: ""OKRs will help" / "eventually"".
  - Position 0.95, `#e74c3c`: "Homeopathy".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: one full-width table with a single `<tr>`; left `<td>` (40%) holds `.obj-title` + `<ul>` bullets + trailing `<p>` paragraph, right `<td>` (60%, centered) holds both canvases (`c1` then `c2`) stacked.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Each chart drawn in its own IIFE. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60` (light green `#2ecc71`), red `#e74c3c`, orange `#e67e22`, amber `#f39c12`, dark slate `#2c3e50`, grays `#666`/`#333`.
