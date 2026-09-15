# Color & Type Themes

How pages convey information through color, labels, and fonts. Structure comes from the template; theme is chosen per page or per folder.

## Accent themes

Every template uses an **accent pair**: a dark primary (h1, h2, strong, obj-title, li b leads) and a lighter secondary (h2 underline, borders, hover). The default is house blue. Pages MAY swap the pair for any theme below — variety across pages in a folder is encouraged (see `02-backlog/87-digital-theft-what-gets-stolen-online-and-how/` for the reference implementation: one hue per page, body text stays neutral).

| Theme | Primary | Secondary pairing |
|-------|---------|-------------------|
| house-blue (default) | `#1a5276` | `#2980b9` |
| deep-purple | `#4527a0` | `#673ab7` |
| teal | `#00695c` | `#00897b` |
| indigo | `#283593` | `#3f51b5` |
| brown | `#5d4037` | `#795548` |
| magenta | `#880e4f` | `#c2185b` |
| slate | `#37474f` | `#546e7a` |
| royal-blue | `#0d47a1` | `#1976d2` |
| olive | `#827717` | `#9e9d24` |
| cyan-dark | `#006064` | `#0097a7` |
| violet | `#6a1b9a` | `#8e24aa` |
| forest | `#1b5e20` | `#388e3c` |
| cyan | `#00838f` | `#00acc1` |
| espresso | `#3e2723` | `#5d4037` |
| navy | `#1a237e` | `#303f9f` |
| purple | `#7b1fa2` | `#9c27b0` |
| green | `#2e7d32` | `#43a047` |
| sky | `#0277bd` | `#039be5` |
| steel | `#455a64` | `#607d8b` |

Rules:

- **Body text never themes:** `#2c3e50` (rem-based templates) or `#2a2a2a` (em-based templates), always.
- **Semantic colors never theme:** red `#e74c3c` = bad/key-point, green `#27ae60` = good/counterexample, orange `#e67e22` = warning/real-world, purple `#8e44ad` = special/causation. These carry meaning and stay fixed regardless of accent.
- **Chart palette:** `#1a5276` primary series, `#e74c3c` bad/overlay, `#27ae60` good, `#e67e22` warn, `rgba(26,82,118,0.35)` bar fill. A themed page may use its accent as the primary series color.
- The page's `viz.md` header records the chosen theme by name.

## Colored labels (information-bearing — never strip)

Colored labels and mixed fonts/colors CONVEY information; migration and regeneration must preserve them exactly.

| Vocabulary | Where | Pattern |
|------------|-------|---------|
| Category card labels | grid cards, top of card | `.card-label` / `.card-num`, 0.72-0.75em bold uppercase, inline color per category; same category text = same color everywhere on the page |
| Topic tags | grid cards, bottom of card | `.topic-tag` grey pill chips |
| Semantic chips | detail pages | `.tag.blue/.green/.red/.orange` pills (rgba tints); `.pt-label` Fact/Risk/Defense/Scene (digital-theft); `.lbl-*` purpose/effect chips; `.domain-*` badges (brainstorm) |
| Bullet leads | detail pages | `li b` colored in the page accent — bold colored label + one-line phrase |
| Pitfall labels | three-col distribution pages | `.pitfall-label` uppercase shape nickname, colored per concept |

Category label palette (pick distinct colors per category): `#795548` `#2980b9` `#27ae60` `#e74c3c` `#8e44ad` `#e67e22` `#16a085` `#d35400` `#c0392b` `#1abc9c` `#f39c12` `#1a5276` `#922b21` `#555`.

## Font templates

Two type systems split the corpus; each template declares which it uses. Do not mix within a page.

| | Type A ("apple-em") | Type B ("system-rem") |
|---|---|---|
| Stack | `-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif` | `system-ui, -apple-system, sans-serif` |
| Body text color | `#2a2a2a` | `#2c3e50` |
| h1 | 1.6-1.8em | 2rem |
| h2 | 1.3-1.4em | 1.3rem |
| Body/bullets | 0.9-0.95em | 0.92-0.95rem |
| Chips/labels | 0.7-0.75em | 0.72rem |
| Used by | obj-table detail, three-col distribution, claim dissection, all grid pages | card-section detail (23-tutorials, pitfalls, 02-backlog) |

Shared everywhere: `* { margin:0; padding:0; box-sizing:border-box }`, `line-height: 1.6`, white background on detail pages (`#f5f5f0` on nav-grid hubs, `#f9f9f9` on three-col distribution).
