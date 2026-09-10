# Tech Industry Broscience

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Tech Industry Broscience — Pseudoscience in Data Analysis

**Subtitle:** Practices adopted as gospel from conference talks, never tested with control groups

## Practices Adopted as Gospel Without Evidence They Work

- **"Microservices are always better":** A 10,000-engineer company uses them; for an 8-person startup, the overhead of service mesh, API gateways, tracing, and deployment coordination exceeds the value of isolation. Adopted because "big tech does it," without asking whether your context matches.
- **"You need ML for this problem":** For most business classification problems, a few hand-written rules outperform any ML model for months — but "we use AI" sounds better in fundraising decks than "we use if-else statements." The tool is chosen for narrative value, not effectiveness.
- **"Test coverage should be >90%":** No evidence the threshold improves quality — tests that assert nothing count toward coverage, so 90% of trivial code plus 0% of critical code meets the metric while the goal (working software) is unchanged.
- **"Standup meetings improve productivity":** Never demonstrated in a controlled study; companies with standups grow, so people read post-hoc correlation as causation, and nobody rigorously tests teams without standups.
- **"Hire from top universities":** No evidence prestige predicts performance (one company's internal study found no correlation after 2 years); the belief persists through survivorship (only successful Ivy Leaguers are visible) and status signaling (hiring from Harvard makes the manager look good).

**Why tech broscience persists:** Failures are silent (survivorship), practices spread by conference talks rather than experiments, no company can run controlled trials on itself (n=1), questioning the orthodoxy is career-risky, and frameworks, certifications, and consultants give the form of science without the substance.

### Visualization (canvas `c1`, 720×340)

Three-column text table: claim / evidence status / source.

- **Title (bold 17px `#1a5276`, top center):** "Tech Broscience: Practices Adopted as Gospel, Never Tested"
- **Rows (one per 35px starting y≈57; claim in `#333` left-aligned at x=50, evidence in red `#e74c3c` centered at x=380, source in gray `#999` right-aligned at x=w−40):**
  - "Microservices always better" / "Zero evidence at <50 engineers" / "Conference talk"
  - "You need ML for this" / "If-else rules often beat it" / "Fundraising deck"
  - ">90% test coverage" / "No link to quality proven" / "Blog post"
  - "Stand-ups help" / "Never tested with control group" / "Agile consultant"
  - "Hire from Ivy League" / "No correlation after 2yr (internal study)" / "Status signaling"
- **Bottom line (bold 17px `#e74c3c`, centered, y=h-8):** "Spread by CONFERENCE TALKS not controlled experiments. Questioning = career risk."

### Visualization (canvas `c2`, 720×300)

Two horizontal flow diagrams comparing how tech practices spread vs the scientific method.

- **Title (bold 17px `#1a5276`, top center):** "How Tech Practices Spread vs How Science Works"
- **Top flow ("Tech:", bold 18px `#e74c3c` label at left), y=55; four 140×32 boxes with 15px gaps, centered horizontally; boxes 1-3 fill `rgba(231,76,60,0.1)`, box 4 fill `rgba(231,76,60,0.25)`, all stroked `#e74c3c` width 1.5; red arrows (line + filled triangle head) between boxes; two-line box texts in `#2c3e50` 16px centered:**
  - "Conference talk / by big co", "Blog posts / & tweets", "Adopted by / 500 companies", "Never tested / w/ control group"
- **Annotation under the gap between boxes 2 and 3 (bold 17px `#e74c3c`, centered):** "(no experiment step!)"
- **Bottom flow ("Science:", bold 18px `#27ae60` label at left), y=130; four boxes same geometry, fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 1.5, green arrows:**
  - "Hypothesis", "Experiment / with control", "Replicate", "Adopt"

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` with left `<td>` (40%) holding `.obj-title`, a `<ul>` of bullets, and a closing `<p>`; right `<td>` (60%, centered) holding the two canvases stacked.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#555`/`#999`, dark slate `#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions.
