# UI Templates

Distilled HTML templates from the project's best docs. Copy-paste and customize.

| # | Template | Use For | Source |
|---|----------|---------|--------|
| 01 | [Landing Page](01-landing-page.html) | Top-level hub, project homepage. 3-col card grid + objective box + key principles. | `statsml/index.html` |
| 02 | [Nav Grid](02-nav-grid.html) | Section indexes, catalog listings. Auto-fit cards with category badges + topic tags. | `reference/index.html` |
| 03 | [TOC Reference](03-toc-reference.html) | Long-form reference docs. TOC → numbered h2 sections → 2-col table (text 45% \| canvas 55%) + comparison table. | `reference/01-foundations-ml-assumptions.html` |
| 04 | [Two-Col Catalog — Badges](04-two-col-catalog-badges.html) | "Hall of shame" style: domain pill badges, `<th>` header row, 17px bold canvas fonts, `#fafafa` bg, nav-link back button. | `reference/metrics/bad-examples.html` |
| 05 | [Two-Col Catalog — Clean](05-two-col-catalog-clean.html) | Technical reference: CSS reset, white bg, centered canvas (`margin: 0 auto`), h2 section dividers between groups, philosophy callout box. | `reference/metrics/metric-testing.html` |
| 06 | [Sectioned Cards — Callout](06-sectioned-cards-callout.html) | Bias catalogs, pitfall lists, concept galleries. Repeated card sections each with h2 + 2-col table (prose/key-point/example \| tall canvas). White bg, red-accent callout boxes, italic examples. | `reference/cognitive-biases/06-measurement-reporting.html` |
| 07 | [Claim Dissection Cards](07-claim-dissection-cards.html) | Dissecting claims, proverbs, or assertions into component fallacies. TOC → repeated cards with quote, flaw table, undefined terms, counterexamples. Optional math callout. | `18-folk-wisdom-fallacies.html` |

## Template 05 vs 06 — Key Differences

| Aspect | 05 (Clean Catalog) | 06 (Sectioned Cards) |
|--------|---------------------|----------------------|
| Background | `#fafafa` | `#ffffff` |
| Layout | One `<table>` with many `<tr>` rows | Repeated `<div>` sections, each with own `<table>` |
| Column split | 40% text \| 60% canvas | 45% text \| 55% canvas |
| Canvas height | 200px | 300px (taller, for diagrams) |
| Callout style | `.philosophy` (blue border-left) | `.key-point` (red border-left) |
| Example text | None | `.example` (italic, muted) |
| Section wrapper | `<h2>` between table groups | `<div class="card-section">` wrapping h2 + table |
| Font stack | `-apple-system, BlinkMacSystemFont` | `system-ui, -apple-system` |
| Body padding | `20px 10px` | `40px` |

## Template 04 vs 05 — Key Differences

| Aspect | 04 (Badges) | 05 (Clean) |
|--------|-------------|-------------|
| CSS reset | None | `* { margin:0; padding:0; box-sizing:border-box }` |
| Font stack | `-apple-system, BlinkMacSystemFont, sans-serif` | `+ 'Segoe UI', Roboto` |
| Background | `#fafafa` | `#ffffff` |
| Table header | `<th>` with `#1a5276` bg | No `<th>` |
| Domain indicator | Colored pill badge (`.metric-domain`) | None (title only) |
| Canvas font size | `17px` (large, bold) | `13px` (smaller, readable) |
| Canvas CSS | `width: 720px; height: 200px; margin-top: 8px` | `display: block; margin: 0 auto` |
| Section breaks | Single table, no h2 breaks | h2 dividers between groups |
| Nav | `.nav-link` back link | `.nav` back link |
| Callout | None | `.philosophy` box with border-left |

## Shared Conventions

- **Color palette:** `#1a5276` (primary blue), `#27ae60` (green/positive), `#e74c3c` (red/negative), `#e67e22` (warning), `rgba(26,82,118,0.35)` (bar fill)
- **Category label colors:** Use a **different color per category** for `.card-num` / `.card-label` labels. Same category text = same color. Assign via a `<script>` block mapping category names to colors. Pick from: `#795548` `#2980b9` `#27ae60` `#e74c3c` `#8e44ad` `#e67e22` `#16a085` `#d35400` `#c0392b` `#1abc9c` `#f39c12` `#1a5276`. See `02-nav-grid.html` for the script pattern.
- **Canvas:** always use `window.devicePixelRatio` for retina scaling
- **Section headers:** h2 with `border-bottom: 2px solid #2980b9`
- **Alternating rows:** `:nth-child(even) td { background: #f0f8ff }` (badges) or `#fafcfe` (clean)
