# 📦 Archive — Drafts & Sandbox

**Page type:** grid page (card navigation grid, auto-fit columns min 300px, two cards linking to subfolders)
**HTML title tag:** Archive — Drafts & Sandbox

**Subtitle:** Historical drafts, architecture sketches, CNN experiments, and pipeline sandbox runs.

## Cards

Each card links to a subfolder index. The card shows an uppercase gray category label (`.card-num`, no index numbers), a title, a description, and a row of topic-tag pills.

| Category | Title | Link | Description | Topics |
|----------|-------|------|-------------|--------|
| DRAFTS | Drafts — Ideas & Sketches | [drafts/index.md](drafts/index.md) | Rough ideation on pipeline design, CNN shape classifiers, feature ontology, and architecture. | architecture, CNN, pipeline |
| SANDBOX | Sandbox — Experiments & Prototypes | [sandbox/index.md](sandbox/index.md) | Concepts tried on real data — Ames Housing, Adult Census, CNN classifier results. | experiments, datasets, pipeline runs |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style. Single page: `h1` (includes the 📦 emoji), `.subtitle` paragraph, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fit, minmax(300px, 1fr))`, 16px gap.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (`drafts/index.html`, `sandbox/index.html`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>` (no inline color; class default `#7f8c8d`), `<h3>Title</h3>` (no index numbers), `<p>description</p>`, and a `<div class="topics">` of `<span class="topic-tag">` pills.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#7f8c8d`, `translateY(-2px)`. `.card-num` 0.72em weight 700 uppercase letter-spacing 0.5px `#7f8c8d`; h3 `#1a3a4a` 1em; description `#555` 0.85em. `.topics` flex wrap gap 4px, margin-top 8px; `.topic-tag` background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em, `#666`.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#7f8c8d`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (this page's accent is archive gray `#7f8c8d`).
- No canvases on this page; canvases elsewhere use `window.devicePixelRatio` scaling.
