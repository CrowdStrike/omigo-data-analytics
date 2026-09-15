# TEMPLATE: Navigation Grid — Auto-Fit Cards with Topic Tags

**Page type:** other (UI template file: section index / nav grid with grouped auto-fit cards, category badges, topic-tag pills)
**HTML title tag:** TEMPLATE: Navigation Grid — Auto-Fit Cards with Topic Tags

**CSS header comment (verbatim):**
```
═══ TEMPLATE: Section index with auto-fit nav cards and topic tags ═══
Use for: Sub-section navigation, catalog indexes, doc listings
Pattern: Title + grouped nav-grids with category badges and topic tags
Source: reference/index.html
```

**In-CSS comment on category badges (verbatim):**
```
Category badge (e.g. FOUNDATIONS, PROFILING, PITFALLS)
Use a DIFFERENT color per category to visually separate concepts.
Same category text = same color. Different category = different color.
Add a script block at the bottom to assign colors by category text.
Palette: #795548 #2980b9 #27ae60 #e74c3c #8e44ad #e67e22 #16a085 #d35400 #c0392b #1abc9c #f39c12 #1a5276
```

## Document content (in order)

**h1:** Section Title — Subtitle

**Subtitle:** One-line description of what this catalog/index covers.

### Group A (h2 `.section-header`)

`.nav-grid` with three placeholder `.nav-card` anchors (href="#"). Each card: `.card-num` category badge, h3 numbered title, description paragraph, `.topics` row of `.topic-tag` pills.

| # | Category | Title | Description | Tags |
|---|----------|-------|-------------|------|
| 1 | CATEGORY | 1. Document Title | Brief one-line description of what this document covers. | tag-one, tag-two, tag-three |
| 2 | CATEGORY | 2. Document Title | Brief one-line description of what this document covers. | tag-one, tag-two |
| 3 | CATEGORY | 3. Document Title | Brief one-line description of what this document covers. | tag-one, tag-two, tag-three |

### Group B (h2 `.section-header`)

`.nav-grid` with two placeholder cards.

| # | Category | Title | Description | Tags |
|---|----------|-------|-------------|------|
| 4 | CATEGORY | 4. Document Title | Brief one-line description. | tag-one, tag-two |
| 5 | CATEGORY | 5. Document Title | Brief one-line description. | tag-one, tag-two |

### Category color script

A `<script>` block at the bottom maps category badge text to colors and applies them to every `.card-num` element (same label text = same color, different categories = different colors). Placeholder map: CATEGORY-A `#e74c3c`, CATEGORY-B `#27ae60`, CATEGORY-C `#8e44ad`, CATEGORY-D `#e67e22`. Script comments (verbatim): "Add one entry per unique .card-num text in this page." and "Pick from: #795548 #2980b9 #27ae60 #e74c3c #8e44ad #e67e22 #16a085 #d35400 #c0392b #1abc9c #f39c12 #1a5276". HTML comment above the script: "Category color assignment: same label text = same color, different categories = different colors".

## Regeneration instructions

- **Template:** this file IS ui-template 02-nav-grid — a self-documenting HTML template with placeholder content. Structure: h1 + `.subtitle`, then per group an h2 `.section-header` followed by a `.nav-grid` of `.nav-card` anchors; a category-color assignment `<script>` at the end. Keep the CSS header comment block and in-CSS/in-script guidance comments.
- **Page style:** body font `-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif`, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 `#2980b9` 1.8em; `.subtitle` `#666` 1.05em, margin-bottom 30px.
- **Section header:** `#1a5276`, 1.2em, margin `35px 0 15px 0`, `border-bottom: 2px solid #d0d0d0`, padding-bottom 8px.
- **Grid:** CSS grid `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap, margin-top 15px.
- **Card:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover border `#2980b9` + `translateY(-2px)`; h3 `#1a3a4a` 1em; `.card-num` default `#2980b9`, 0.75em bold, margin-bottom 4px (colored per category by the script); description p `#555` 0.85em.
- **Topic tags:** `.topics` flex-wrap row with 4px gap, margin-top 8px; `.topic-tag` background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`.
- **Card numbering convention:** h3 uses unpadded "N. Title" matching the target file's index number; `.card-num` holds the colored uppercase category label.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; category badge palette also includes `#795548 #2980b9 #8e44ad #16a085 #d35400 #c0392b #1abc9c #f39c12`.
- No canvases on this page; pages in this family that add canvases scale them with `window.devicePixelRatio`. In regenerated HTML, card links use `.html` extensions (here they are `#` placeholders). No nav bar, no back/home links.
