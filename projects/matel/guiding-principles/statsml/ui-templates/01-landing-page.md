# TEMPLATE: Landing Page — 3-Column Card Grid

**Page type:** other (UI template file: landing/index page with hero title, objective box, 3-column colored section-card grid, key-principle boxes)
**HTML title tag:** TEMPLATE: Landing Page — 3-Column Card Grid

**CSS header comment (verbatim):**
```
═══ TEMPLATE: Landing/Index page with colored section cards ═══
Use for: Top-level navigation hubs, project homepages
Pattern: Hero title + objective box + 3-col card grid + key principles
Source: statsml/index.html
```

## Document content (in order)

**h1:** Project Title — Tagline

**Subtitle:** One-line description of what this section/project covers

### Objective/thesis box (`.objective`)

**Core Thesis** (h2)

State the core principle or thesis in 1-2 sentences. This box draws the eye first.

### Section header

**Sections** (h2 `.section-header`)

### Navigation grid (`.section-grid`, three `.section-card` anchors, href="#")

Each card contains an h2 title, a description paragraph, a 3-item ul, and a `.doc-count` div.

| Card class | Title | Description | List items | Doc count |
|------------|-------|-------------|-----------|-----------|
| section-card purple | Section A | Brief description of what this section contains. | Topic one / Topic two / Topic three | N docs + resources |
| section-card green | Section B | Brief description of what this section contains. | Topic one / Topic two / Topic three | N docs + resources |
| section-card blue | Section C | Brief description of what this section contains. | Topic one / Topic two / Topic three | N docs + resources |

### Section header

**Key Principles** (h2 `.section-header`)

### Key-principle boxes (`.key-principle`, each with a bold lead)

- **Principle One:** Statement of the principle in one sentence.
- **Principle Two:** Statement of the principle in one sentence.
- **Principle Three:** Statement of the principle in one sentence.

## Regeneration instructions

- **Template:** this file IS ui-template 01-landing-page — a self-documenting HTML template with placeholder content. Structure: h1 + `.subtitle` + `.objective` box + `.section-header` + `.section-grid` of `.section-card` anchors + `.section-header` + `.key-principle` boxes. Keep the CSS header comment block.
- **Page style:** body font `-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif`, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 centered `#1a5276` 2.2em; `.subtitle` centered `#666` 1.1em, margin-bottom 40px.
- **Objective box:** background `#ffffff`, left border `4px solid #1a5276`, padding 20px, border-radius `0 8px 8px 0`, shadow `0 2px 4px rgba(0,0,0,0.05)`; its h2 `#1a5276`.
- **Section header:** `#1a5276`, 1.3em, margin `50px 0 15px 0`, `border-bottom: 2px solid #d0d0d0`, padding-bottom 8px.
- **Grid:** CSS grid `repeat(3, 1fr)`, 30px gap, margin `30px 0`.
- **Card:** background `#ffffff`, border `2px solid #d8d8d8`, radius 16px, padding 30px, shadow `0 2px 8px rgba(0,0,0,0.06)`; hover `translateY(-3px)` + shadow `0 4px 12px rgba(0,0,0,0.1)`; card h2 1.3em; card p `#555` 0.92em; `.doc-count` 0.8em `#888` weight 600 uppercase letter-spacing 0.5px; card ul 0.85em `#555`, margin `12px 0 0 18px`.
- **Card color variants:** `.purple` border/h2 `#8e44ad` (hover border `#6c3483`); `.green` border/h2 `#27ae60` (hover border `#1e8449`); `.blue` border/h2 `#2980b9` (hover border `#1a5276`).
- **Key-principle box:** background `#ffffff`, border `1px solid #d8d8d8`, radius 8px, padding 15px 20px, margin `12px 0`, shadow `0 1px 3px rgba(0,0,0,0.05)`; `strong` in `#c62828`.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange (this template also uses `#8e44ad` purple, `#2980b9` blue accent, `#c62828` principle red).
- No canvases on this page; when the template family does use canvases they are scaled with `window.devicePixelRatio`. In regenerated HTML, card links use `.html` extensions (here they are `#` placeholders). No nav bar, no back/home links.
