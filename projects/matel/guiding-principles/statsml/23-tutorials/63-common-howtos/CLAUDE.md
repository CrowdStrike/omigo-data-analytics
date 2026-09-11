# Common How-Tos — Folder Conventions

Pages here are end-to-end "how does this actually work" walkthroughs (wearable
data, analytics systems, mobile apps). They follow `23-tutorials/CLAUDE.md` for
page skeleton, text columns, canvas mechanics, and data integrity — with the
folder-specific visualization rules below layered on top.

## Visualization Style: Pictorial Scenes, Not Diagram Templates

These pages tell a physical story (a ring on a finger, a factory of data, a
phone in a pocket). The canvases must look like small illustrated scenes, not
generic architecture diagrams.

- **Draw the nouns** — every canvas contains at least one pictogram built from
  canvas primitives: a ring as a torus, a phone silhouette, a cloud from arcs,
  a database cylinder, a storefront awning, a crescent moon, a gear, a funnel.
- **A labeled rounded rect is not a picture** — boxes may support a scene but
  cannot BE the scene. If every element on a canvas is a rect with text inside,
  redraw it.
- **Vary the form across the page** — no two canvases on one page may share the
  same layout skeleton. The four stock forms (box-chain flow, twin pros/cons
  panels, sequence lifelines, circle stepper) may each appear at most once per
  page, and only dressed in that page's pictorial language.
- **Compose like a scene** — use ground lines, sky areas, big central objects,
  small supporting props, and one bold annotation stating the insight. Think
  "children's-encyclopedia cutaway", not "cloud architecture slide".
- **Bigger fonts than the base tutorials spec** — chart titles bold 17px,
  primary labels 13px, secondary/mute captions 12px, bold insight annotations
  14px. Nothing below 12px in this folder.
- **Everything else still applies** — 720×300 logical size, dpr scaling,
  deterministic hardcoded data, "illustrative" captions.

## Per-Page Color Themes

Page chrome (CSS, headings, tag pills, key-point callout) stays site-standard.
Only the CHART palette changes: each page declares its own `P` object so the
whole folder doesn't render in the same blue/green/yellow.

| Page | Theme | Core hues | Accent |
|------|-------|-----------|--------|
| 01 wearables | night sky / sleep | indigo `#4f46e5`, violet `#7c3aed`, plum `#a21caf` | amber `#d97706` (moon, alarms) |
| 02 analytics system | factory floor / daybreak | teal `#0f766e`, cyan `#0e7490`, deep sea `#155e75` | copper `#b45309`, magenta `#be185d` |
| 03 mobile apps | workshop / storefront | rose `#e11d48`, burnt orange `#c2410c`, forest `#15803d` | ink-brown `#78350f` |

- Keep the role keys of the standard palette object (`ink`, `text`, `mute`,
  `grid`) but retint `ink` toward the page theme for chart titles if desired;
  `text #2c3e50`, `mute #6b7280`, `grid #e5e9ef` stay fixed.
- All theme colors must stay readable on white (no pastels for text or lines);
  fills use the theme hue at 0.08–0.18 alpha.
- A new page in this folder picks a fresh theme not already in the table above,
  and adds itself to the table.

## Everything Else

- md-first: rewrite the `.md` visualization specs, then regenerate the `.html`
  from the md — the two must never drift.
- Text columns, bullets, examples, key points follow `23-tutorials/CLAUDE.md`.
- No cross-page links, no nav, no item counts, no `Math.random()`.
