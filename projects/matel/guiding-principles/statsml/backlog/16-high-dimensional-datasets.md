# High-Dimensional Datasets (100s–1000s of Columns)

**Page type:** detail page (backlog kusto-style two-column layout: text left 45%, canvas right 55%, one table row per section)
**HTML title tag:** High-Dimensional Datasets (100s–1000s of Columns) — Discussion Backlog

**Subtitle:** Triage, cluster, profile — taming feature explosion before validation

**Intro callout:** When a dataset has 500+ columns, the profiling pipeline faces unique challenges.

## 1. Challenges

- **Computational explosion:** 1000 features × multiple candidates × validation windows
- **Correlation structure dominates:** Many redundant (different encodings of same info)
- **Multiple testing problem:** 1000 features at α=0.05 → 50 false positives
- **Feature groups, not individuals:** Genes in same pathway, sensors on same device
- **Curse of dimensionality:** Many tests assume sufficient n relative to dims
- **Sparse signal:** Maybe 20-50 features carry real signal

### Visualization (canvas `c2`, 720×320)

Correlation cluster diagram: dashed circles of grouped features, each with one filled representative dot.

- **Title (bold 16px, `#1a5276`, top center):** "Correlation Clusters — One Representative Per Group"
- **Seven dashed circles** (dash 4/3, stroke width 1.5, fill = cluster color at ~8% alpha via hex suffix `15`), each with member dots placed around the center and a two-line label below the circle (12px, cluster color):
  - center (120,120), r=65, 8 members, label "Income / variants", color `#1a5276`
  - center (310,100), r=50, 5 members, label "Age / related", color `#27ae60`
  - center (470,130), r=55, 6 members, label "Education / encodings", color `#e67e22`
  - center (620,110), r=45, 4 members, label "Location / features", color `#e74c3c`
  - center (200,220), r=40, 3 members, label "Health / metrics", color `#8e44ad`
  - center (400,230), r=48, 5 members, label "Work / history", color `#2980b9`
  - center (570,240), r=35, 3 members, label "Family / size", color `#16a085`
- **Dots:** radius 5. The first member of each cluster is the representative: filled in the cluster color with a white 2px ring and a white bold 8px "R" centered inside. Other members: near-white fill `rgba(255,255,255,0.8)` with 1.5px cluster-color outline. Members are placed at evenly spaced angles (offset by a per-cluster seed) at distances between 0.5r and 0.8r.
- **Legend (bottom, 13px `#2c3e50`):** filled blue dot "= Representative (profiled)"; outlined dot "= Redundant member (skipped)".

## 2. Proposed Pipeline

- **Phase 1 — Triage:** Fast univariate screen (variance, MI). 1000 → 100 in seconds
- **Phase 2 — Correlation groups:** Cluster into redundant groups. Pick representative per group
- **Phase 3 — Full profiling:** Multi-candidate pipeline on ~30-50 representatives
- **Phase 4 — Expansion:** For winning representatives, check if group members add signal

**Key Questions:**
1. Triage threshold?
2. Non-linear signal in Phase 1?
3. Global vs conditional correlation?
4. Feature expansion interaction?
5. Pathway-aware grouping?

### Visualization (canvas `c1`, 720×300)

Funnel chart: five centered horizontal bars narrowing by stage, connected by gray down-arrows, phase labels on the right.

- **Title (bold 16px, `#1a5276`, top center):** "Feature Reduction Funnel"
- **Stages** (bar width proportional to count out of 1000, centered horizontally; bar height 35, gap 10, marginTop 35, marginLeft 80, max bar width w−160; label inside bar in bold 14px `#1a5276`, all bars stroked `#1a5276` width 1; phase label in 13px `#95a5a6` to the right):
  | Bar label | Count | Fill | Phase label |
  |---|---|---|---|
  | 1000 Raw Features | 1000 | `rgba(26,82,118,0.35)` | Input |
  | 100 Screened | 100 | `rgba(230,126,34,0.35)` | Phase 1: Triage |
  | 35 Group Representatives | 35 | `rgba(39,174,96,0.35)` | Phase 2: Cluster |
  | 35 Fully Profiled | 35 | `rgba(41,128,185,0.35)` | Phase 3: Profile |
  | 8-15 Validated | 12 | `rgba(39,174,96,0.5)` | Phase 4: Expand |
- **Arrows:** small gray (`#95a5a6`) down-arrows between consecutive bars at the funnel center line.
- **Bottom annotation (13px `#e74c3c`, centered):** "99% of features eliminated — only validated signal survives"

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. h1 with 2px `#2980b9` bottom border, `.subtitle`, `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem), then one `.lang-section` per numbered section: h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with a single `<tr>` — left `td.text-col` (45%) holds bullets/key-point, right `td.viz-col` (55%) holds the canvas. Note: Section 1 hosts canvas `c2` (320 tall) and Section 2 hosts canvas `c1` (300 tall) — keep these ids/placements. No index number in the h1.
- **Inline code style:** `code` — background `#e8f0f8`, padding 2px 6px, radius 3px, 0.85em, color `#1a5276`.
- **Key-point callout:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; ul 0.92rem with 4px li spacing; canvases `width: 100%` with `1px solid #e0e0e0` border, radius 4px. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, plus `#8e44ad` purple, `#2980b9` mid blue, `#16a085` teal for cluster colors; gray `#95a5a6`.
- **Canvas:** intrinsic width/height attributes as given (c2 720×320, c1 720×300); scale via `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) in a shared `setupCanvas(id)` helper that reads the element's declared width/height.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
