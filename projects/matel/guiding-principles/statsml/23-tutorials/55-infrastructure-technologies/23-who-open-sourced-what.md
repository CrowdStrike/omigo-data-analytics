# Who Open-Sourced What

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Who Open-Sourced What

**Subtitle:** Most of the modern data stack was born inside one company solving its own scale problem — this page maps which company gave the industry which piece, and why they gave it away

## Papers First, Then Everyone Ships Code

**Tags:** `core idea` (blue), `two eras` (green), `Google papers` (orange)

- **The papers** — Google published GFS (2003), MapReduce (2004), and Bigtable (2006) as papers, not code
- **The builders** — Doug Cutting and Yahoo turned the papers into Hadoop (2006); HBase followed (2008)
- **The lag** — paper-to-working-clone took roughly 2-3 years each time, built by outsiders
- **The shift** — Google then began releasing code directly: Protocol Buffers (2008), Go (2009)
- **The flood** — Kubernetes (2014) and TensorFlow (2015) shipped as code on day one, no clone needed

*Example (italic):* Bigtable's 2006 paper begat HBase; Kubernetes skipped the paper stage entirely and became the industry standard itself.

**Key point:** The family tree has two eras — first Google described its infrastructure and others rebuilt it from the papers; later companies released the code directly, so the original became the standard.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline 2002–2016: Google papers on the top lane, released code on the bottom lane, dashed arrows showing which paper begat which project and the 2-3 year lag.

- **Title (bold 15px, `#1a5276`, top center):** "Two Eras: Papers Others Rebuilt, Then Code Released Directly".
- **Axes:** light 2px `#999` time axis at y=245 from x=60 to x=662; year mapped as `x = 60 + (year − 2002) × 43`; 12px `#444` tick labels every 2 years (2002, 2004, ... 2016).
- **Lanes:** 12px `#6b7280` lane labels at x=20 — "papers" at y=110, "code" at y=200.
- **Paper markers (violet `#4a3aa7` diamonds, 10px, on y=110):** GFS at x=103 (2003), MapReduce at x=146 (2004), Bigtable at x=232 (2006); bold 12px violet labels above each, staggered to avoid overlap.
- **Code markers (10px dots on y=200):** blue `#2a78d6` — Hadoop at x=232 (2006), HBase at x=318 (2008); green `#008300` — Protocol Buffers at x=318 (2008, label staggered below HBase's), Go at x=361 (2009), Kubernetes at x=576 (2014), TensorFlow at x=619 (2015); 12px labels in the marker color.
- **Arrows:** dashed `#6b7280` (dash 4/3) 2px arrows GFS→Hadoop, MapReduce→Hadoop, Bigtable→HBase; blue markers are paper-derived, green markers are direct releases.
- **Annotation (bold 13px green `#008300`, near x=430, y=70):** "direct release: the original is the standard".
- **Caption (12px `#444`, bottom right):** "release years as publicly documented".

## The Family Tree, Company by Company

**Tags:** `worked example` (blue), `the map` (green)

- **Google** — papers begat Hadoop and HBase; direct: Kubernetes, TensorFlow, Go, Protocol Buffers
- **Facebook/Meta** — Cassandra (2008), Hive (2008), Presto (2013), React (2013), PyTorch (2016)
- **LinkedIn** — Kafka (2011), Samza (2013), Pinot (2015): a full streaming-and-analytics stack
- **Yahoo** — Hadoop (2006), ZooKeeper (2008), BookKeeper (2011), Pulsar (2016)
- **Twitter & Airbnb** — Storm (2011) via Twitter; Airflow (2015) and Superset (2016) via Airbnb
- **Netflix & Uber** — Iceberg (2018) and the Chaos tools via Netflix; Hudi (2017) via Uber

*Example (italic):* Kafka left LinkedIn as an Apache project in 2011; today it moves data at companies that never spoke to LinkedIn.

**Key point:** Nearly every load-bearing piece of open data infrastructure — Hadoop, Kafka, Cassandra, Airflow, Kubernetes — started as one company's internal fix for its own scale problem.

### Visualization (canvas `c2`, 720×300)

Company-to-projects map: six rows, each a company label on the left and its released projects as colored pills with years.

- **Title (bold 15px, `#1a5276`, top center):** "The Family Tree: Which Company Released Which Piece".
- **Rows (y = 62, 100, 138, 176, 214, 252), bold 12px `#1a5276` company label at x=20:** "Google", "Facebook/Meta", "LinkedIn", "Yahoo", "Twitter / Airbnb", "Netflix / Uber".
- **Pills:** rounded rects starting at x=150, 26px tall, 8px gap, 8px radius, 11px `#2c3e50` text "Name ’YY":
  - Google: "Kubernetes ’14", "TensorFlow ’15", "Go ’09", "Protobuf ’08" — fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border
  - Facebook/Meta: "Cassandra ’08", "Hive ’08", "Presto ’13", "React ’13", "PyTorch ’16" — fill `rgba(0,131,0,0.12)`, 1px `#008300` border
  - LinkedIn: "Kafka ’11", "Samza ’13", "Pinot ’15" — fill `rgba(217,89,38,0.12)`, 1px `#d95926` border
  - Yahoo: "Hadoop ’06", "ZooKeeper ’08", "BookKeeper ’11", "Pulsar ’16" — fill `rgba(74,58,167,0.12)`, 1px `#4a3aa7` border
  - Twitter / Airbnb: "Storm ’11", "Airflow ’15", "Superset ’16" — fill `rgba(213,81,129,0.12)`, 1px `#d55181` border
  - Netflix / Uber: "Iceberg ’18", "Chaos Monkey ’12", "Hudi ’17" — fill `rgba(25,158,112,0.12)`, 1px `#199e70` border
- **Annotation (bold 12px `#1a5276`, right-aligned near x=690, y=285):** "each pill: one internal tool, released".
- **Caption (12px `#444`, below annotation or bottom left):** "major releases only, years as documented".

## Why Give Away the Crown Jewels

**Tags:** `where it's used` (blue), `strategy` (green), `the pattern` (orange)

- **Not the edge** — companies open-source plumbing, rarely the product: LinkedIn shared Kafka, not its graph
- **Hiring** — engineers join to work on the famous project; the repo doubles as a recruiting ad
- **Ecosystem** — outsiders write connectors, fix bugs, and battle-test the code at other scales for free
- **Standardization** — releasing first makes your internal design the industry default, as Kubernetes did
- **The contrast** — AWS mostly sells managed services instead; infrastructure IS its edge, so it keeps it

*Example (italic):* Google released Kubernetes in 2014 and sells Google Cloud on top of it — the standard is open, the hosting is the business.

**Key point:** The pattern: open-source what is not your business edge, keep what is, and collect hiring, ecosystem, and standard-setting benefits from the give-away.

### Visualization (canvas `c3`, 720×300)

Kept-vs-gave diagram: five company rows, each with a green "gave away" box (the plumbing) and a blue "kept" box (the edge); the AWS row breaks the pattern.

- **Title (bold 15px, `#1a5276`, top center):** "Give the Plumbing, Keep the Edge".
- **Column headers (bold 12px, y=52):** green `#008300` "open-sourced" centered at x=310; blue `#2a78d6` "kept closed" centered at x=560.
- **Rows (y = 80, 122, 164, 206, 248), 12px `#444` company label at x=20:**
  - "Google": green box at x=210 "Kubernetes, TensorFlow" — blue box at x=470 "search ranking, ads"
  - "Meta": green box "React, PyTorch" — blue box "feed ranking"
  - "LinkedIn": green box "Kafka, Pinot" — blue box "the member graph"
  - "Netflix": green box "Iceberg, Chaos tools" — blue box "recommendations"
  - "AWS": one wide orange `#d95926` box at x=210, width 440, "mostly neither — sells managed versions of others' open source"
- **Box style:** 200px wide (except the AWS row), 30px tall, 8px radius, 11px `#2c3e50` text; green fill `rgba(0,131,0,0.12)`, blue fill `rgba(42,120,214,0.15)`, orange fill `rgba(217,89,38,0.12)`; 1px borders in the line colors.
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "the released tool is never the moneymaker".
- **Caption (12px `#444`, bottom right):** "kept/gave split schematic".

## Open Source Is Not Community-Run

**Tags:** `common mistake` (red), `governance` (orange)

- **The assumption** — "it's open source" gets read as "a neutral community steers it"; often false
- **Vendor-led** — React and TensorFlow are open source, but Meta and Google set the roadmaps
- **Foundation-run** — Kafka, Hadoop, and Airflow sit at Apache; Kubernetes at CNCF, with neutral rules
- **License moves** — a controlling vendor can relicense: Elasticsearch did in 2021, Terraform in 2023
- **The check** — read who holds the trademark and who approves commits, not just the license file

*Example (italic):* Terraform was open source for nine years, then HashiCorp moved it to a source-available license in 2023 — the community responded by forking OpenTofu.

**Common mistake:** Treating the open-source label as a governance guarantee. The license says what you may do today; the owner decides what the project becomes tomorrow.

### Visualization (canvas `c4`, 720×300)

Governance spectrum: a horizontal axis from "single vendor controls" to "neutral foundation", with project pills placed along it and relicensing casualties flagged at the vendor end.

- **Title (bold 15px, `#1a5276`, top center):** "Same Label, Different Owners: the Governance Spectrum".
- **Axis:** 3px `#999` horizontal arrow at y=165 from x=60 to x=660; bold 12px end labels — red `#e74c3c` "single vendor controls" under the left end (x=60, y=195), green `#008300` "neutral foundation" under the right end (x=660, y=195, right-aligned).
- **Pills above the axis (26px tall, 8px radius, 11px text, stems down to the axis), left to right:**
  - "React (Meta)" centered x=140, y=110 — fill `rgba(231,76,60,0.12)`, border `#e74c3c`
  - "TensorFlow (Google)" centered x=250, y=70 — same red styling
  - "PyTorch (LF, 2022)" centered x=430, y=110 — fill `rgba(201,133,0,0.15)`, border `#c98500` (moved to the Linux Foundation in 2022)
  - "Kafka (Apache)" centered x=560, y=70 — fill `rgba(0,131,0,0.12)`, border `#008300`
  - "Kubernetes (CNCF)" centered x=600, y=120 — same green styling
- **Relicensed markers (below axis, near left end):** bold 12px red `#e74c3c` "✗ Elasticsearch 2021" at (x=90, y=230) and "✗ Terraform 2023" at (x=90, y=252) — projects whose controlling vendor changed the license.
- **Annotation (bold 13px violet `#4a3aa7`, near x=420, y=230):** "check the owner, not just the license".
- **Caption (12px `#444`, bottom right):** "positions schematic, events as documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all marker positions, pill contents, and row layouts are the hardcoded values above (no randomness); release years, foundation moves (PyTorch→Linux Foundation 2022), and relicensing events (Elasticsearch 2021, Terraform 2023) are publicly documented facts; the kept/gave split boxes and spectrum positions are schematic and labeled as such.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
