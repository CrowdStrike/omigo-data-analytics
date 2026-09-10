# Fix Failures as Encountered Without Quantifying Frequency

**Page type:** detail page (anti-pattern-pairs two-section layout: one `.card-section` per pattern, each with a two-column table — text left 45%, canvas right 55%)
**HTML title tag:** Fix Failures as Encountered Without Quantifying Frequency

**Subtitle:** Spent 2 weeks on a 0.3% problem while a 12% failure class was never investigated

## The Anti-Pattern

See a failure → fix it immediately. Never ask "how often does this happen?" Effort follows accident of observation, not impact.

**Key point (red-left-border callout):** The failure you happened to notice becomes top priority, regardless of whether it affects 0.3% or 30% of cases.

**Domain examples:**

- Model debugging — fixing the edge case you spotted in a notebook while systematic misclassification goes unexamined
- Bug prioritization — the bug a senior engineer hit personally jumps the queue over high-frequency user-reported issues
- Incident response — post-mortem focuses on the trigger, not the most common failure mode in the preceding month

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of failure classes by frequency, with the rarest one highlighted as the (wrongly) fixed one.

- **Title (bold 13px `#2c3e50`, centered, y=20):** "Failure Classes by Frequency — Effort Misallocated".
- **Data (top to bottom):** Class A 12%, Class B 8%, Class C 5%, Class D 3%, Class E 0.3%. Scale max = 14%. Margins: left 80, right 60, top 40, bottom 50; bar height = row height − 12.
- **Bar colors:** Class E (0.3%) solid red `#e74c3c` with `#c0392b` 1px border (it was "fixed"); Class A `rgba(26,82,118,0.2)`; Classes B–D `rgba(26,82,118,0.35)`; non-red borders `#1a5276`.
- **Labels:** class name right-aligned in 12px `#2c3e50` left of each bar; percentage value ("12%", "8%", "5%", "3%", "0.3%") in bold 12px `#2c3e50` just right of each bar end.
- **Red arrow annotation:** diagonal 2px `#e74c3c` line with filled arrowhead pointing to the Class E bar; label to its right in bold 11px `#e74c3c`: "Fixed first (because seen first)!".
- **Gray annotation on Class A bar (bold 11px `#999`, centered right of the 12% bar):** "← Never investigated".
- **Axis:** thin `#ccc` x-axis line under the bars; x-axis label in 11px `#666`, centered at bottom: "Failure Frequency (%)".

## The Design Pattern

Classify failures first → quantify each class → fix largest first regardless of observation order.

**Key point (green-left-border callout, `#27ae60`):** Effort allocation is proportional to measured impact, not recency of discovery.

**Steps:**

- Collect and log all failure instances systematically
- Classify failures into distinct categories
- Count frequency of each category over a meaningful window
- Rank categories by impact (frequency × severity)
- Allocate effort top-down from highest impact
- Re-measure after each fix to confirm reduction

### Visualization (canvas `c2`, 720×300)

Same horizontal bar chart sorted by impact, with the top three classes marked for fixing in priority order.

- **Title (bold 13px `#2c3e50`, centered, y=20):** "Failure Classes Sorted by Impact — Effort Follows Quantification".
- **Data:** same five classes/values as `c1` (12, 8, 5, 3, 0.3), scale max 14. Margins: left 80, right 140, top 40, bottom 50.
- **Bar colors:** top 3 bars green with decreasing opacity — `rgba(39,174,96,0.7)`, `rgba(39,174,96,0.55)`, `rgba(39,174,96,0.4)` — bordered `#27ae60`; bottom 2 bars `rgba(26,82,118,0.25)` bordered `#1a5276`.
- **Labels:** class names left of bars (12px `#2c3e50`), bold percentage values right of bar ends.
- **Priority annotations (top 3 bars only):** a bold 16px `#27ae60` "✓" right of the value, followed by an 11px `#27ae60` label — "Fix 12% first", "Fix 8% first → then", "Fix 5% first → then".
- **Flow arrow:** vertical dashed green line (`#27ae60`, dash 4/3, width 2) at x = w−100 running from below bar 1 to bar 3, ending in a downward filled green arrowhead.
- **Bottom caption (italic 11px `#27ae60`, centered, two lines):** "Effort follows quantification," / "not accident".
- **Axis:** thin `#ccc` x-axis line under the bars.

## Regeneration instructions

- **Template/layout:** anti-pattern-pairs detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, then two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"). Each section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) followed by a `table.layout` (width 100%, border-collapse) with one row: `td.text-col` (45%) holding a paragraph, a `.key-point` callout, a bold "Domain examples:"/"Steps:" lead-in (inline style: margin-top 12px, weight 600, 0.92rem) and a `<ul>`; `td.viz-col` (55%) holding the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c` (design-pattern callout overrides border-left-color to `#27ae60`), padding 8px 12px, 0.9rem; ul 0.92rem. Canvas elements `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic size 720×300 via a shared `setup(id)` helper that sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, bar fills `rgba(26,82,118,0.2–0.35)` and `rgba(39,174,96,0.4–0.7)`, gray text `#666`/`#999`.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
