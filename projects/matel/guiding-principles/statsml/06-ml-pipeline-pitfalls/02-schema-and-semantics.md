# Schema & Semantics

**Page type:** grid page (card navigation grid, 4 columns, one philosophy callout, cards with category labels and topic tags)
**HTML title tag:** Schema & Semantics — ML Pipeline Pitfalls

**Subtitle:** The column kept its name and changed its meaning. Types line up, values parse, and the number now measures something else.

## Callout (philosophy box)

**Why this matters:** A schema check compares names and types, which is exactly the part that tends not to break. What breaks is meaning: a column that held dollars starts holding cents, a timestamp loses its zone, `NULL` stops meaning “unknown” and starts meaning “zero.” Every validator passes because nothing is malformed. These are the failures where the data is well-typed and wrong, and they stay invisible until somebody notices a total that is off by exactly one hundred.

## Cards

Each card links to a detail page under `02-schema-and-semantics/`. The card shows a colored uppercase category label, a numbered title, a one-line description, and a row of small topic tags.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | SCHEMA | Silent Data Drift (Schema/Pipeline Changes) | [02-schema-and-semantics/01-silent-data-drift-schema-pipeline-changes.md](02-schema-and-semantics/01-silent-data-drift-schema-pipeline-changes.md) | Upstream data changes without notification — model degrades silently | drift, schema, monitoring |
| 2 | SCHEMA | Silent Column Semantics Change | [02-schema-and-semantics/02-silent-column-semantics-change.md](02-schema-and-semantics/02-silent-column-semantics-change.md) | Column name stays the same but meaning changes | schema, semantics, documentation |
| 3 | SCHEMA | Timezone / Timestamp Ambiguity | [02-schema-and-semantics/03-timezone-timestamp-ambiguity.md](02-schema-and-semantics/03-timezone-timestamp-ambiguity.md) | Timestamps without locale, precision, or format spec | timezone, timestamp, DST |
| 4 | SCHEMA | Character Encoding / Locale Issues | [02-schema-and-semantics/04-character-encoding-locale-issues.md](02-schema-and-semantics/04-character-encoding-locale-issues.md) | Text data corrupted by encoding mismatches — silent data loss | encoding, UTF-8, locale |
| 5 | SCHEMA | NULL Semantics Mismatch | [02-schema-and-semantics/05-null-semantics-mismatch.md](02-schema-and-semantics/05-null-semantics-mismatch.md) | NULL means different things in different columns | NULL, semantics, imputation |
| 6 | SCHEMA | Unit Mismatch (Dollars vs Cents) | [02-schema-and-semantics/06-unit-mismatch-dollars-vs-cents.md](02-schema-and-semantics/06-unit-mismatch-dollars-vs-cents.md) | Same column name, different units across sources or over time | units, schema, validation |
| 7 | SCHEMA | Multi-Source Schema Inconsistency | [02-schema-and-semantics/07-multi-source-schema-inconsistency.md](02-schema-and-semantics/07-multi-source-schema-inconsistency.md) | Multiple upstream sources fit same schema but encode differently | multi-source, semantics, reconciliation |

## Regeneration instructions

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle`, a `.philosophy` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, margin-top 15px; responsive: 3 columns below 1400px, 2 columns below 1100px, 1 column below 600px.
- **Links:** the table links to `.md` versions for markdown navigation; in the HTML each card's `href` is the same path with an `.html` extension.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index matching the file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors:** inline script maps `.card-num` text through a `categoryColors` object and sets `el.style.color`: SCHEMA `#1a5276`. Base `.card-num` style is `#2980b9`, 0.75em, bold.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. Philosophy callout background `#f0f4f8`, left border `4px solid #2980b9`. No nav bar, no back/home links.
