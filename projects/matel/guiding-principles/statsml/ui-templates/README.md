# UI Templates

Distilled HTML templates from the project's best docs. Copy-paste and customize. Colors/labels/fonts: see `THEMES.md`. Shared chart code: see `js/`.

| # | Template | Use For | Source |
|---|----------|---------|--------|
| 01 | [Landing Page](01-landing-page.html) | Top-level hub, project homepage. 3-col card grid + objective box + key principles. | `statsml/index.html` |
| 02 | [Nav Grid](02-nav-grid.html) | Section indexes on beige `#f5f5f0`. Auto-fit cards with category badges + topic tags. | `reference/index.html` |
| 03 | [TOC Reference](03-toc-reference.html) | Long-form reference docs. TOC → numbered h2 sections → 2-col table (text 50% \| canvas 50%) + comparison table. | `reference/01-foundations-ml-assumptions.html` |
| 04 | [Two-Col Catalog — Badges](04-two-col-catalog-badges.html) | "Hall of shame" style: domain pill badges, `<th>` header row, 17px bold canvas fonts, `#fafafa` bg. Also: brainstorm profiling deep-dives. | `reference/metrics/bad-examples.html` |
| 05 | [Two-Col Catalog — Clean](05-two-col-catalog-clean.html) | Obj-table detail pages: one concept per table row, text 50% \| canvas 50%, white bg, h2 dividers, philosophy callout. | `reference/metrics/metric-testing.html` |
| 06 | [Sectioned Cards — Callout](06-sectioned-cards-callout.html) | Card-section detail pages: repeated h2 sections each with 2-col table (45\|55), key-point callouts, tag pills, italic examples. | `reference/cognitive-biases/06-measurement-reporting.html` |
| 07 | [Claim Dissection Cards](07-claim-dissection-cards.html) | Proverbs/claims decomposed: quote, flaw table, undefined terms, counterexamples. Prose-only — zero js. | `17-folk-wisdom-fallacies/21-hard-work-always-pays-off.html` |
| 08 | [Card Grid Hub](08-card-grid-hub.html) | White-bg hub with categorized cards; sectioned or flat mode; label top, tags bottom. | root `17/18-*.html`, `02-backlog/87-digital-theft-what-gets-stolen-online-and-how.html` |
| 09 | [Three-Col Distribution](09-three-col-distribution.html) | Distribution details: text 38% \| raw chart 31% \| insight chart 31%; pitfall-label pills; `#f9f9f9` bg. | `20-real-world-distribution-gallery/` (all) |
| 10 | [Q&A Obj-Table](10-qa-obj-table.html) | Survey/reference rows answering one question each; right cell = chart OR monospace payload block. | `22-recently-added-misc/` subfolders |

## Which template does the corpus actually use? (2026-08 census)

| Template family | Folders | ~Pages |
|---|---|---|
| 06 card-section detail | 23-tutorials details, 06-ml-pipeline-pitfalls, 10-anti-patterns, 21-most-powerful-signals, 13-statistical-paradoxes, 12-statistical-tests, 05-cognitive-biases, 02-backlog top-level + security subfolders (digital-theft, MITM, credential-token, simulation-models, file-formats, data-query-languages, data-acquisition) | ~1,530 |
| 05 obj-table detail | 04-domain-pitfalls, 14-metrics-design, 15-ml-assumptions, 09-common-bad-practices, 08-pseudoscience, 07-ab-testing-pitfalls, 18-interesting-problems-paradoxes, applied-game-theory (variant: + math-box) | ~330 |
| 10 Q&A obj-table | 22-recently-added-misc subfolders, 02-backlog/72-platform-privacy-policies-collect-use-keep-return, 02-backlog/64-programming-languages | ~220 |
| 02 nav-grid | 23-tutorials 64 grids, ~9 root hubs, 02-backlog survey/archive indexes | ~86 |
| 09 three-col distribution | 20-real-world-distribution-gallery (all 34; "Do Not Convert") + 3-col rows inside ~56 23-tutorials pages | ~34 |
| 07 claim dissection | 17-folk-wisdom-fallacies (all 26) | 26 |
| 08 card-grid hub | ~13 root hubs, 4 02-backlog/misc hubs | ~17 |
| 04 badges/profiling | 03-brainstorm 01-07 | 7 |
| 01 landing | index.html | 1 |
| One-offs (fence, don't template) | mermaid causal-chains, 04-domain-pitfalls A/B/C, sports-wearable special top element, archive drafts/sandbox dashboards | ~30 |

## Grid rules

- Card anatomy: **label at top** (`.card-label`/`.card-num`, colored per category), title `N. Title` (index matches file index), one-line description, **`.topic-tag` pills at bottom**.
- Two modes: **sectioned** (h2 groups) and **flat**. Keep whichever mode the page uses.
- Numbers ascend down the page; inserting a card mid-section renumbers later cards and their files (rule also recorded in each grid's viz.md).
- Grid pages have zero canvases.

## Shared js (`js/`)

| File | Provides | Used by |
|---|---|---|
| `js/base.js` | `setupCanvas`/`setup` (high-DPI), `registerChart` + resize re-render, `mulberry32`/`randn`/`randExp`, `roundRectPath`, `drawArrow`, `drawAxes` | every canvas-bearing template |
| `js/three-col-dist.js` | `drawHistogram` (overlays, density line, SE band), `drawBarChart` | template 09 |

Pages include with a relative path (`<script src="../ui-templates/js/base.js">`); a page that moves across folder depths fixes only that line. Page-specific chart code stays inline in the page's VIZ fences — shared js is a floor, not a ceiling; bespoke elements (e.g., the sports-wearable top widget) always stay inline and are preserved exactly.

## Shared Conventions

- **Color palette & accent themes:** see `THEMES.md`. Default accent pair `#1a5276`/`#2980b9`; pages may swap to a named accent theme (digital-theft pattern). Semantic red/green/orange and body text never theme.
- **Colored labels are information** — category labels, semantic chips, colored bullet leads, pitfall labels are content; never strip or normalize them.
- **Canvas:** always devicePixelRatio-scaled (use `setupCanvas`); min width 720px full-col / 420px three-col; height 200-460px; `width: 100%`.
- **Section headers:** h2 with `border-bottom: 2px solid` secondary accent.
- **Fonts:** two type systems (A apple-em, B system-rem) — see `THEMES.md`; each template declares which it uses.
