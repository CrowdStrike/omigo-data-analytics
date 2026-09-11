# Common Howtos

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Common Howtos

**Subtitle:** Step-by-step recipes for everyday data tasks — the big pieces, the paperwork, and the plumbing, written for someone doing it the first time.

## Cards

Each card links to a topic page under `common-howtos/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | GETTING DATA IN | Getting Data From Wearable Devices | [63-common-howtos/01-getting-data-from-wearable-devices.md](63-common-howtos/01-getting-data-from-wearable-devices.md) | How your ring's sleep score reaches a table you own — pull vs push, the public address, the vendor paperwork, and the one-afternoon recipe. | webhooks, pull vs push, public URL, provisioning |
| 2 | BUILDING SYSTEMS | Building a Data Analytics System | [63-common-howtos/02-building-a-data-analytics-system.md](63-common-howtos/02-building-a-data-analytics-system.md) | The five pieces every analytics system is made of — pull, store, clean, serve, show — and the order to build them in. | pipeline, files vs database, ETL, dashboard |
| 3 | BUILDING SYSTEMS | Building Mobile Apps | [63-common-howtos/03-building-mobile-apps.md](63-common-howtos/03-building-mobile-apps.md) | What a phone app is made of, the iPhone-and-Android choice, the Apple and Google paperwork, and the path from sketch to the store. | iPhone & Android, cross-platform, store review, push |

## Regeneration instructions

- **Template:** tutorials category grid page (per `tutorials/CLAUDE.md`). Body: h1 (1.8em `#2980b9`), `.subtitle` paragraph (`#666` 1.05em), then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Grid:** `repeat(4, 1fr)`, 16px gap; responsive fallbacks: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Card:** white `.nav-card` (1px `#d8d8d8` border, 10px radius, 20px padding, subtle shadow, hover lifts 2px with `#2980b9` border); `.card-num` = uppercase subcategory label (0.75em bold); `<h3>` = "N. Title" (unpadded N matching the topic file's index); one-line `<p>` description (0.85em `#555`); `.topics` row of `.topic-tag` pills (0.7em, `#f0f0f0` background, 1px `#ccc` border).
- **Category label colors (set by inline script keyed on `.card-num` text):** GETTING DATA IN `#0b7285`, BUILDING SYSTEMS `#6c3483`.
- **Page CSS:** body system-ui sans-serif, background `#f5f5f0`, text `#2a2a2a`, 40px padding, line-height 1.6. No nav bar, no back/home links, no item counts.
- In regenerated HTML, card hrefs use `.html` extensions (`common-howtos/NN-slug.html`); this spec links the `.md` siblings.
