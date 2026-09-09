# Semantic-Aware & Ontology-Based Lineage

**Page type:** detail page (backlog-style two-column layout table: text left 45%, canvas right 55%, one `.lang-section` per numbered h2)
**HTML title tag:** Semantic-Aware &amp; Ontology-Based Lineage

**Subtitle:** Encoding domain knowledge into the lineage graph

**Intro callout (blue accent):** Layer domain meaning — semantic types, concepts, constraints — on top of physical lineage so the graph can catch transformations that are mechanically valid but semantically wrong. Stub — to be expanded.

## 1. Core Idea

**Problem:** Raw lineage tracks physical transformations (join, filter, aggregate) but misses semantic meaning. Two columns named "revenue" from different sources may mean different things.

**Solution:** Layer ontological concepts on top of physical lineage:

- **Semantic type:** "monetary_amount", "identifier", "timestamp"
- **Domain concept:** "patient_age", "transaction_value"
- **Relationships:** "is_derived_from", "is_proxy_for", "conflicts_with"
- **Validity constraints:** age ∈ [0, 120], revenue ≥ 0

**Key point (red-accent callout):** **Payoff:** Detect semantic conflicts (averaging IDs), validate transforms preserve meaning, auto-generate documentation.

### Visualization (canvas `c1`, 720×300)

Node-and-edge lineage diagram with semantic annotations on each node.

- **Title (bold 16px, top center, `#222`):** "Ontology Layer on Physical Lineage"
- **Nodes:** five boxes, each 120×36 (drawn centered at x,y: rect from x-60, y-18), fill `#f8fafb`, 2px stroke in node color; bold 14px label (dark `#222`) on the top line and a 12px semantic annotation (node color) on the second line:
  - (100, 90) "src.revenue" / "monetary_amount" — color `#2980b9`
  - (100, 200) "src.cost" / "monetary_amount" — color `#2980b9`
  - (360, 145) "profit" / "derived_monetary" — color `#27ae60`
  - (580, 90) "margin_pct" / "ratio [0,1]" — color `#27ae60`
  - (580, 200) "avg(src.id)" / "⚠ meaningless!" — color `#e74c3c`
- **Edges:** solid gray `#aaa` lines (width 1.5): (180,90)→(300,145); (180,200)→(300,145); (420,145)→(520,90). One dashed red `#e74c3c` line (dash 4/3): (180,200)→(520,200) — the invalid path to avg(src.id).
- **Caption (bottom center, 14px `#555`):** "Ontology catches \"averaging an ID\" as a semantic error"

## 2. Key Questions

(Full-width bullet list, no canvas.)

- How much ontology is worth maintaining vs. deriving from data?
- Integration with Layer 1 (Assessment) semantic metadata
- Can LLMs auto-assign semantic types from column names + sample values?
- When ontological constraints conflict with observed data — which wins?

## Regeneration instructions

- **Template:** backlog detail page (kusto-style 2-col text/viz layout). Structure: h1, `.subtitle` paragraph, `.intro` callout, then one `.lang-section` per section — each with an h2 (numbered "N. Title") and a `table.layout` with `td.text-col` (45%) and `td.viz-col` (55%) holding the canvas. Section 2 is a plain bullet list without a layout table or canvas. No index number in the page h1/title.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border. `.intro` — background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` italic `#555` 0.9rem. `ul` 0.92rem, margin 8px 0 8px 20px. Canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic width/height attributes per chart (720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; gray text `#555`/`#666`.
- **Links:** none on this page; if any card links exist in regenerated HTML, use `.html` extensions.
