# Template 08 — Card Grid Hub

**Use for:** white-background hub/index pages with categorized link cards. The white-bg sibling of Template 02 (nav-grid, beige). Supports two modes: **sectioned** (cards grouped under h2 section headers) and **flat** (one grid).

**Source pages:** root `17-*.html`/`18-*.html` hubs, `backlog/87-digital-theft.html`, `recently-added-misc/03-tracking-data-collection-methods.html`.

## Anatomy

- `h1` + `.subtitle` — page title and one-line scope.
- `.philosophy` callout (optional) — blue-left-border intro box.
- `.toc` box (optional, long hubs) — anchor links to sections.
- **Sectioned mode:** repeated [`.section-title` (h2, `#d6e4ee` underline) + optional `.section-blurb` + `.grid`]. **Flat mode:** single `.grid`.
- `.grid` — `repeat(3, 1fr)` (or 4), 16px gap; responsive collapse 3→2→1 at 1100/800/500px.
- Card = `<a class="card">`: **`.card-label` at top** (0.72em bold uppercase, inline color per category — same category text always same color), `<h3>N. Title</h3>` (index matches file index), one-line `<p>`, **`.topics` row of `.topic-tag` pills at bottom**.

## Numbering rule

Card index numbers match file index numbers and must read ascending down the page. In sectioned mode, inserting a card mid-section renumbers all later cards AND their files. Keep numbers ascending for consistency.

## Type & color

- Font: Type A apple-em stack (see THEMES.md); h1 1.8em `#1a5276`; card h3 1.0em `#1a5276`; card p 0.85em `#555`.
- Background `#ffffff`; card bg `#f8fafb`, border `#e0e0e0`, radius 8px; hover shadow + `#2980b9` border.
- Category label colors from the THEMES.md category palette.

## Viz / js

None — grid pages have zero canvases and no scripts (except optional card-color mapping script).
