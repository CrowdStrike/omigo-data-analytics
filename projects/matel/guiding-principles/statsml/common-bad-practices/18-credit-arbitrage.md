# Credit Arbitrage

**Page type:** detail page (two-column obj-table layout: text left ~40%, two stacked canvases right ~60%, single row)
**HTML title tag:** Credit Arbitrage — Common Bad Practices

**Subtitle:** Attribution Gaming — A 10% contributor authors the summary and becomes "the person who did it."

## The Practice

- A project spans 5 teams, 20 people, 6 months. Dana contributes about 10% of the work.
- Dana also writes the launch blog post, presents at the all-hands, prepares the executive summary, and gives the leadership demo.
- Organizational memory is largely narrative, so Dana becomes the name attached to the project.

## The Mechanism

- The person who communicates the work is recorded as the person who did the work.
- The contributors behind the other 90% produced no narrative artifacts, so their share is under-attributed.
- Credit is a measurement problem: attribution flows to visible artifacts, not to underlying contribution.

## Variant — Naming Credit

- Whoever coins the project's name ("Operation Phoenix") gains a durable association with it.
- The name enters org vocabulary — a lasting association independent of technical contribution.

## Variant — The Summary Email

- The weekly summary email to leadership aggregates everyone's work ("we did X") under one sender.
- The sender field functions as the credit line.

**Why it persists:** Leadership forms opinions from presentations, summaries, and write-ups — not git commits. Individual contributions across 20 people are hard to observe, so the narrative becomes the record of who did what.

**The tell:** Compare git/Jira/doc history to who presents. If presenting and building are consistently done by different people, attribution is drifting from contribution. Compare who leadership believes did the work against the contribution record.

### Visualization (canvas `c1`, 720×340)

Split diagram: horizontal contribution bars on the left, a podium/presentation scene on the right, connected by a dashed orange arrow. Background `#f9f9f9`.

- **Left panel (x 0–280), title (bold 16px `#1a5276`, centered at x=140, y=20):** "Actual Contribution".
- **Bars:** 5 horizontal bars starting at x=30, y=35, height 28px, gap 6px; width scaled as pct/40 × 180px. Data: Alice 35% `#1a5276`; Bob 25% `#2980b9`; Chen 20% `#3498db`; Dana 10% `#e67e22`; Eli 10% `#95a5a6`. Each bar labeled to its right in 16px `#333`: "Alice (35%)" etc.
- **Highlight:** Dana's row (the presenter) gets a background band `rgba(230,126,34,0.15)` from x=10 spanning the left panel width.
- **Arrow:** dashed orange (`#e67e22`, dash 4/3, width 2) from Dana's bar (x≈270) to the right panel (x≈360, y=h/2).
- **Right panel (x 380–720), title (bold 16px `#1a5276`, centered):** "Who Leadership Sees".
- **Podium:** orange `#e67e22` rectangle 60×40 centered at right-panel midpoint (y = h/2+20), white 16px label "PODIUM" inside. Above it a stick figure drawn in orange stroke width 2: circle head (r=10), body line, arms line.
- **Audience:** 3 small leadership icons below the podium (filled `#1a5276` circles r=6 with 8×10 body rects, spaced 30px), labeled "Leadership" in 16px `#666` below.
- **Thought bubble:** gray (`#999`, width 1) ellipse (rx=70, ry=22) to the upper right of the podium containing italic 9px `#555` text: `"This person drove it"`.
- **Bottom-left of right panel, 16px `#ccc`:** two lines — "Alice (35%): not visible" and "Bob (25%): not visible".

### Visualization (canvas `c2`, 720×300)

Slopegraph of credit drift: contribution share on the left axis, credit received on the right axis, one line per role; the presenter's line crosses all the others as credit flows away from the people who did the work. Background `#f9f9f9`.

- **Title (bold 17px `#1a5276`, centered at w/2, y=22):** "Contribution vs Credit (Slopegraph)".
- **Axes:** two vertical lines (`#ccc`, width 1) at x=210 and x=510, from y=58 to y=250. Column headers in bold 13px `#333`, centered above each axis at y=46: "Contribution share" (left) and "Credit received" (right).
- **Scale:** a value v (percent, 0–60 span) maps to y = 250 − v × (190/60).
- **Data (deterministic, one line per role):**
  - The builder: 40% contribution → 20% credit, `#1a5276`, line width 2.
  - The architect: 30% contribution → 15% credit, `#27ae60`, line width 2.
  - The tester: 20% contribution → 10% credit, `#95a5a6`, line width 2.
  - The presenter: 10% contribution → 55% credit, `#e67e22`, line width 3 (the only rising line — it crosses all three falling lines).
- **Lines & dots:** each role is a straight line between the two axes with filled dots (r=4) at both endpoints, all in the role's color.
- **Labels:** left of the left axis, right-aligned 12px in the role color at x=200: "The builder  40%" etc. Right of the right axis, left-aligned bold 12px in the role color at x=520: "20%" etc. All labels vertically centered on their endpoint (y+4).
- **Insight annotation (bold 13px `#e74c3c`, centered at w/2, y=272):** "The presenter does 10% of the work and receives 55% of the credit."
- **Caption (italic 12px `#555`, centered at w/2, y=292):** *"When the lines cross, attribution has drifted away from the people who did the work."*

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds `.obj-title` headings + bullet lists + closing `<p><strong>` paragraphs, right `<td>` (60%, centered) holds the two canvases stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276` (subsequent ones get inline `margin-top:14px`); `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display:block; margin:0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#555`/`#333`.
- **Links:** this page has no card links; in regenerated HTML any card links elsewhere use `.html` extensions.
