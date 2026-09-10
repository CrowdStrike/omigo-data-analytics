# Real-World Distributions

Gallery of surprising distribution shapes from actual domains — e-commerce, betting, crypto, retail, web search, sports.

## UI Template: 3-Column (Folder-Specific — Do Not Convert)

This folder intentionally uses its own 3-column layout. It is a separate UI template, NOT drift from the 2-column metric-testing template used in `14-metrics-design/`. Do not restyle these pages to match other folders.

Each concept is its own `<table class="obj-table">` with one 3-column row:

| Column | Width | Content |
|--------|-------|---------|
| 1 | 38% | `.pitfall-label` pill (uppercase shape nickname, e.g. "DROPS OFF A CLIFF"), `<h3>` title with distribution name in parentheses, one paragraph, bullet list |
| 2 | 31% | primary canvas, 420×340 |
| 3 | 31% | secondary canvas, 400×340 |

Template styling that belongs to this folder:

- Blue `1px solid #2980b9` cell borders, `#f9f9f9` page background
- Centered `h1` and `.subtitle`
- No `<thead>` header row, no numbered titles
- Canvases scale to cell width via `width: 100%; height: auto`
