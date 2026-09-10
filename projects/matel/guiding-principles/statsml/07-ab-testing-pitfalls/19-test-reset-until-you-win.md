# Test Reset Until You Win

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Test Reset Until You Win — A/B Testing Pitfalls

**Subtitle:** Deliberate — Restart the test whenever control is winning. Only keep the run where treatment wins.

## Section 1: Deliberate — Restart the test whenever control is winning. Only keep the run where treatment wins.

- Week 1: control winning. "Logging bug, restart." Week 2: control winning. "Contamination, restart." Week 3: treatment winning. "Clean test! Ship!"
- P(significant at least once in 3 tries | null) = 1-(0.95)^3 ≈ 14%. 5 tries = 23%.
- Each restart has PLAUSIBLE reason. Bugs DO happen. Nobody tracks restart count.

**Correct approach:** Log ALL runs. Don't discard data without pre-specified criteria. Track restart frequency.

**The tell:** Correlation between restart count and final winner. Tests that "win" have more restarts than tests that "lose."

### Visualization (canvas `c1`, 720×340)

Timeline diagram: three trial rows, each a horizontal line with status and outcome, red for discarded runs and green for the kept one.

- **Background:** full-canvas fill `#f8f9fa`.
- **Rows (row height 55, starting y=25); each row has a bold 16px `#1a5276` left label, a colored timeline line (width 3) from x=140 to x≈600, a 16px `#555` status text centered above the line, a gray "→" arrow, and a bold 18px colored reason/action at the right:**
  - Trial 1 — status "Control winning", line color red `#e74c3c`, action `"Bug" → restart`.
  - Trial 2 — status "Control winning", line color red `#e74c3c`, action `"Contamination" → restart`.
  - Trial 3 — status "Treatment winning", line color green `#27ae60`, action `SHIP!`.
- **Bottom label (bold 16px `#555`, centered):** "P(win in 3 tries under H₀) = 14%".

## Section 2: Illustrative Example: Third Time's the "Charm"

- A growth team at a large online retailer tested a new signup flow, saw it losing to the old one, blamed "a logging bug", and started the whole test over from scratch.
- The second run also lost and was thrown out over a "traffic contamination" worry; the third run finally won, and it was the only run anyone reported or remembered when the feature shipped.
- Even if the new flow does nothing at all, giving it three fresh tries roughly triples the chance that one of them looks like a winner by luck alone — and a later holdout check on this feature showed no real gain.

### Visualization (canvas `c2`, 720×300)

Bar chart: chance of a fake win grows with each restart.

- **Title (bold 17px `#2a2a2a`, centered):** "Every Restart Buys Another Lottery Ticket" at y=28.
- **Bars:** labels `['1 run', '2 runs', '3 runs', '5 runs']`, values `[5, 10, 14, 23]` (% chance at least one run "wins" when the feature does nothing). Bar width 90, gap 70, group centered; baseline y=235, scale max 25% over 160px.
  - Strokes: `#1a5276`, `#e67e22`, `#e74c3c`, `#e74c3c` (width 2); fills: `rgba(26,82,118,0.35)`, `rgba(230,126,34,0.3)`, `rgba(231,76,60,0.3)`, `rgba(231,76,60,0.3)`.
  - Value labels bold 16px in the bar's stroke color above each bar ("5%", "10%", "14%", "23%"); x labels 14px gray `#666` below the baseline.
- **Annotation under the bars (14px red `#e74c3c`, centered):** `chance a do-nothing feature "wins" at least once`.
- **Takeaway (15px `#333`, bottom center):** "Report every run — the two discarded losses were data, not bugs".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; paragraphs 0.95em `#333`; bullets 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
