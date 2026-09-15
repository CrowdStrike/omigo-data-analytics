# PostgreSQL

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** PostgreSQL

**Subtitle:** One open-source database became the 2020s consensus default — a correct, standards-strict core whose extensions absorb whole database categories

## The Everything Database

**Tags:** `core idea` (blue), `open source` (green), `extensions` (orange)

- **The database** — PostgreSQL: open-source, community-run, free to use, born at UC Berkeley
- **Standards-strict** — it implements the SQL standard faithfully instead of inventing dialect quirks
- **Extensible core** — JSONB and full-text search are built in; PostGIS and pgvector plug in as extensions
- **The pattern** — each capability absorbs a database category: documents, geo, search, vectors
- **The result** — by the 2020s "just use Postgres" became the default answer to "which database?"

*Example (italic):* A startup needing documents, maps, search, and embeddings installs one database, not five.

**Key point:** PostgreSQL became the consensus default because one dependable core plus extensions covers most specialized needs — you add a feature, not a new database.

### Visualization (canvas `c1`, 720×300)

Hub-and-spoke diagram: the PostgreSQL core in the center, four capability boxes at the corners, each labeled with the specialized database category it absorbs.

- **Title (bold 15px, `#1a5276`, top center):** "Built-in Features and Extensions Absorb Whole Database Categories".
- **Center box:** rounded rect 160×50 at (280, 125), fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 8px radius, bold 13px `#1a5276` two-line label "PostgreSQL core / (relational SQL)".
- **Corner boxes (each 170×44, 8px radius, 12px `#2c3e50` two-line text: extension name bold, category below):**
  - (55, 55): "JSONB / → documents (built-in)", fill `rgba(0,131,0,0.12)`, 2px `#008300` border
  - (495, 55): "PostGIS / → geo database", fill `rgba(217,89,38,0.12)`, 2px `#d95926` border
  - (55, 200): "full-text search / → search (built-in)", fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border
  - (495, 200): "pgvector / → vector database", fill `rgba(213,81,129,0.12)`, 2px `#d55181` border
- **Spokes:** 2px `#6b7280` lines from the center box edges to the nearest edge of each corner box.
- **Annotation (bold 13px green `#008300`, centered near y=285):** ""just use Postgres" — one database, five categories covered".
- **Caption (12px `#444`, bottom right):** "categories representative, not exhaustive".

## A Bookstore on a Single Database

**Tags:** `worked example` (blue), `JSONB + pgvector` (green)

- **The app** — an online bookstore: 40,000 orders, 8,000 books, one "similar books" recommender
- **Relational** — orders live in a plain table; monthly revenue is one JOIN plus GROUP BY
- **Documents** — book attributes vary by genre, so each book row carries a JSONB blob
- **Vectors** — every book gets a 384-dim embedding; a pgvector index makes similarity fast
- **One transaction** — adding a book writes the row, its JSONB, and its embedding atomically

*Example (italic):* "Books like this one" is one SQL query — `ORDER BY embedding <-> :vec LIMIT 5` — with no second system involved.

**Key point:** Three workload types — relational, document, vector — live in one database, share one transaction, and can join with each other inside a single query.

### Visualization (canvas `c2`, 720×300)

Flow diagram: the bookstore app on the left sends three kinds of queries into three stores that all sit inside one dashed "one PostgreSQL instance" boundary.

- **Title (bold 15px, `#1a5276`, top center):** "One Bookstore App, Three Workloads, One Database".
- **App box:** rounded rect 120×60 at (30, 120), fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 12px `#1a5276` label "bookstore app".
- **Instance boundary:** rounded rect from (215, 45) to (695, 265), 2px dashed `#1a5276` (dash 6/4), bold 13px `#1a5276` label "one PostgreSQL instance" at top left inside (x=230, y=62).
- **Three inner boxes (each 430×46, 8px radius, x=245, at y = 75, 135, 195), 12px `#2c3e50` text, bold lead term:**
  - "orders — 40,000 rows · SQL JOIN for monthly revenue", fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border
  - "books.attrs — 8,000 JSONB docs · attrs vary by genre", fill `rgba(0,131,0,0.12)`, 2px `#008300` border
  - "embeddings — 8,000 × 384-dim · pgvector index, top-5 lookup", fill `rgba(213,81,129,0.12)`, 2px `#d55181` border
- **Arrows:** three 3px `#6b7280` arrows from the app box's right edge to the left edge of each inner box.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=285):** "no sync jobs — one backup, one login, one transaction log".
- **Caption (12px `#444`, bottom right):** "row counts illustrative".

## Why the Boring Choice Won

**Tags:** `where it's used` (blue), `MVCC` (green), `operations` (orange)

- **MVCC** — readers never block writers: analysts query freely while orders keep inserting
- **Correctness first** — a decades-old reputation that committed data stays committed
- **Skills transfer** — standards-strict SQL means tools, ORMs, and hires all just work
- **One of everything** — one backup, one auth setup, one monitoring dashboard to watch
- **No sync drift** — data living in one place cannot disagree with a stale copy elsewhere

*Example (italic):* A team replacing a four-system stack drops from 4 nightly backups and 3 sync pipelines to 1 and 0 (illustrative).

**Key point:** The consensus formed on two legs — a correctness reputation earned over decades, and extensions that made running specialized systems optional rather than mandatory.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: the operational bill of a four-system specialized stack vs one Postgres, across three ops metrics.

- **Title (bold 15px, `#1a5276`, top center):** "Four Specialized Systems vs One Postgres: the Ops Bill".
- **Layout:** metric labels left-aligned 12px `#444` at x=20; bars start at x=250, scale 100px per unit, bars 14px tall; metric rows at y = 75, 140, 205; in each row the specialized bar sits at the row y, the Postgres bar at y+18.
- **Rows (specialized bar orange `#d95926` solid, Postgres bar green `#008300` solid, 11px value labels at bar ends):**
  - "systems to install & patch": orange width 400 (label "4"), green width 100 (label "1")
  - "nightly backups to verify": orange width 400 (label "4"), green width 100 (label "1")
  - "cross-system sync pipelines": orange width 300 (label "3"), green width 0 (label "0" at x=254)
- **Legend (12px, top right near y=55):** orange swatch "specialized stack", green swatch "Postgres + extensions".
- **Annotation (bold 13px green `#008300`, centered near y=265):** "fewer moving parts is why the default won".
- **Caption (12px `#444`, bottom right):** "counts illustrative for a typical small team".

## "Just Use Postgres" Is a Default, Not a Law

**Tags:** `common mistake` (red), `scale limits` (orange)

- **The slogan** — "just use Postgres" means start there, not never leave
- **JSONB ≠ no schema** — dumping everything into one JSONB column recreates the document-store mess
- **Search gaps** — built-in full-text search lacks the relevance tuning of a dedicated engine
- **Vector limits** — pgvector handles millions of embeddings well; billions favor dedicated engines
- **The rule** — move a workload out only after measuring it, not because a benchmark blog said so

*Example (italic):* At 100k vectors both answer in ~6 ms (illustrative); at 100M the dedicated engine's 20 ms beats pgvector's 240 ms.

**Common mistake:** Treating the default as a guarantee. Extensions cover the common 90% of each category; a workload that lives at a category's extreme still deserves the specialized system — after you measure.

### Visualization (canvas `c4`, 720×300)

Line chart of similarity-query latency vs collection size: pgvector vs a dedicated vector database, crossing as the collection grows.

- **Title (bold 15px, `#1a5276`, top center):** "When to Graduate: pgvector vs a Dedicated Vector Database (illustrative)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = collection size with 12px `#444` tick labels "10k", "100k", "1M", "10M", "100M" at x = 60, 210, 360, 510, 660 (even spacing, log-feel by decade labels — not a real log axis); y = query latency 0 to 250 ms, gridlines `#e5e9ef` at 60/120/180 ms with 12px `#444` labels.
- **pgvector line:** blue `#2a78d6` 3px line through the five tick x-positions at latencies `[3, 6, 15, 60, 240]` ms, 4px dots.
- **Dedicated line:** green `#008300` 3px line through the same x-positions at latencies `[5, 6, 8, 12, 20]` ms, 4px dots.
- **Line labels:** bold 12px blue "pgvector" near (x=560, y=105); bold 12px green "dedicated engine" near (x=560, y=215).
- **Threshold marker:** vertical dashed `#6b7280` (dash 4/3) line at x=360 (1M), 12px `#6b7280` label "measure here before migrating" at its top.
- **Annotation (bold 12px blue `#2a78d6`, near x=150, y=90):** "below ~1M vectors the extension is fine".
- **Caption (12px `#444`, bottom right):** "latencies illustrative — the shape, not the numbers, is the lesson".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and pixel widths above (no randomness); row counts (40,000 orders / 8,000 books / 8,000 × 384-dim embeddings), ops counts (4/4/3 vs 1/1/0), and latency curves ([3, 6, 15, 60, 240] vs [5, 6, 8, 12, 20] ms) are invented and labeled illustrative; publicly documented facts (open-source, standards-strict SQL, JSONB / PostGIS / full-text search / pgvector, MVCC readers-don't-block-writers) are stated as facts without numeric claims.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
