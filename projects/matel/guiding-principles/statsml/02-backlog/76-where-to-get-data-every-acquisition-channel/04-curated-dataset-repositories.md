# Curated Dataset Repositories — Datasets One Download Away

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Curated Dataset Repositories — Datasets One Download Away

**Subtitle:** The fastest way to get data is to download someone else's: hub sites host ready-to-load datasets complete with documentation, license tags, and loading code, turning acquisition from a multi-week project into a search query.

**Intro callout (blue-left-border box):** Most practitioners' first thousand hours of practice run on repository data, and for good reason: someone already collected it, cleaned it, documented it, and answered the first fifty questions about it in a public forum. The skill to build here is not downloading — it is reading what comes with the download: the license, the dataset card, and the collection story that decides whether the data fits your problem at all.

## 1. The big general hubs — Kaggle and Hugging Face

Two general-purpose hubs dominate: if a public dataset exists, it is very likely on one of them, packaged with documentation and the code to use it.

- **Kaggle Datasets:** founded 2010, part of Google since 2017
- **Kaggle scale:** hundreds of thousands of public datasets
- **Per-dataset page:** description, version history, community discussion
- **Notebooks attached:** popular datasets ship with public notebooks
- **Worked examples:** how others loaded, cleaned, and modeled the data
- **Hugging Face Hub:** default channel for NLP, vision, audio corpora
- **Data plus weights:** datasets and model weights live side by side
- **One loading API:** the `datasets` library loads any Hub dataset in one call
- **Standardized away:** downloading, caching, and format parsing

Key point: These hubs collapse "getting data" from a multi-week acquisition project into a few minutes of searching — which is exactly why every tutorial starts here, and why raw real-world data later comes as a shock.

### Visualization (canvas `c1`, 720×380)

Two hub boxes side by side at top, each fanning down to two artifact boxes, all converging into one "ready to load" box at the bottom.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Two hubs, one pattern: data arrives packaged with the code to use it"
- **Kaggle box:** 280×54 at (50, 48), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered: "KAGGLE DATASETS"; 11px `#666`: "founded 2010 · part of Google since 2017".
- **Hugging Face box:** 280×54 at (390, 48), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 12px `#27ae60` centered: "HUGGING FACE HUB"; 11px `#666`: "default channel for NLP, vision, audio".
- **Artifact boxes (each 150×46 at y=170, white fill, 2px border in box color; bold 12px label in box color, 11px `#666` subline, both centered):**
  - x=50 "Dataset files" `#1a5276` — "CSV, images, docs"
  - x=220 "Public notebooks" `#e67e22` — "worked examples"
  - x=390 "Dataset files" `#27ae60` — "hosted, versioned"
  - x=560 "datasets API" `#8e44ad` — "one-call loading"
- **Connectors:** 1px `#bbb` lines from each hub's bottom center (Kaggle at x=190, HF at x=530, y=102) to the top centers of its two artifact boxes (y=170); then 1px `#bbb` lines from each artifact box's bottom center (y=216) to the top of the bottom box (x=360, y=278).
- **Bottom box:** 360×46 at (180, 278), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered: "READY TO LOAD IN MINUTES"; 11px `#666`: "documentation, license tag, loading code included".
- **Caption (12px `#999`, centered, y = h−14):** "The hub packages the dataset; the community packages the know-how"

## 2. The academic classics — UCI and OpenML

Before the hubs there were the archives: two academic projects made shared benchmark datasets a norm of machine learning research.

- **UCI Repository:** the archive running since 1987
- **Textbook classics:** Iris, Adult, Wine — small, clean, endlessly cited
- **Shared benchmarks:** everyone tested on the same UCI datasets
- **Comparable papers:** "state of the art" became a measurable claim
- **OpenML:** standardized task definitions wrapped around each dataset
- **Prior runs:** new methods compare against thousands of uploaded results
- **Reproducibility:** dataset, split, and metric fixed by the repository
- **Same experiment:** two researchers on different continents, one setup

Key point: The classics are teaching instruments, not production stand-ins: Iris has 150 rows and its measurements were collected in the 1930s. They exist so that methods can be compared, not so that models can be shipped.

### Visualization (canvas `c2`, 720×340)

Timeline from 1987 to today with five milestone dots, labels alternating above and below the axis.

- **Title (bold 14px `#1a5276`, centered, y=22):** "From one FTP archive to a web-wide ecosystem"
- **Timeline:** 2px `#999` line at y=170 from x=50 to x=680 with a filled right-arrowhead.
- **Steps (filled dot radius 7 in step color on the line; labels alternate above/below with thin `#ccc` connectors; label bold 12px in step color, subline 11px `#666`, year tag 10px `#999` on the opposite side of the line):**
  - x=90, "1987", "UCI Repository" — "Iris, Adult, Wine" — `#1a5276` (above)
  - x=235, "2010", "Kaggle founded" — "datasets + notebooks" — `#e67e22` (below)
  - x=380, "2013", "OpenML" — "standardized tasks" — `#8e44ad` (above)
  - x=510, "2018", "Google Dataset Search" — "web-wide index" — `#e74c3c` (below)
  - x=630, "2020s", "HF Hub datasets" — "one loading API" — `#27ae60` (above)
- **Caption (12px `#999`, centered, y = h−14):** "Four decades of shared datasets — each generation lowered the barrier further"

## 3. Finding and going deeper — search and domain archives

When the general hubs do not have what you need, a search layer and specialized archives go deeper.

- **Google Dataset Search:** indexes schema.org dataset metadata web-wide
- **Catalog of catalogs:** a search layer over hosts, not a host itself
- **Cloud-hosted open data:** AWS Open Data puts data next to the compute
- **Analyze in place:** satellite, genomics, weather — no terabyte downloads
- **Research archives:** Zenodo and Dataverse hold paper-linked datasets
- **Citable DOI:** each deposit gets a fixed version and citation
- **Social science depth:** ICPSR, decades of curated survey data
- **Stricter docs:** documentation standards above most ML hubs

Key point: Discovery usually runs shallow to deep: a search engine finds the candidates, a general hub gives you the quick packaged copy, and a domain archive holds the documented, citable original.

### Visualization (canvas `c3`, 720×340)

Three-layer discovery funnel: stacked centered boxes of decreasing width connected by downward arrows.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Discovery runs shallow to deep"
- **Layer 1:** 600×56 at (60, 48), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered: "SEARCH LAYER — Google Dataset Search"; 11px `#666`: "indexes schema.org dataset metadata across the web".
- **Layer 2:** 460×56 at (130, 140), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 12px `#27ae60` centered: "GENERAL HUBS — Kaggle, Hugging Face, OpenML"; 11px `#666`: "fast, packaged, community-documented copies".
- **Layer 3:** 340×56 at (190, 232), fill `rgba(142,68,173,0.12)`, 2px `#8e44ad` border. Bold 12px `#8e44ad` centered: "DOMAIN ARCHIVES"; 11px `#666`: "AWS Open Data · Zenodo · Dataverse · ICPSR".
- **Arrows:** 2px `#999` vertical arrows centered at x=360, from y=108 to y=136 and from y=200 to y=228, each with a small filled down-arrowhead.
- **Caption (12px `#999`, centered, y = h−14):** "Search finds it, the hub gives you a quick copy, the archive holds the documented original"

## 4. What the repository does not tell you

A repository page shows the download button, the license tag, and the docs — the important gaps are the things it cannot show.

- **Licenses vary per dataset:** hosting on a hub does not make data free to use
- **Commercial use:** often requires reading the license line by line
- **Label errors:** famous benchmarks contain mislabeled examples
- **Penalized for being right:** models get scored against wrong labels
- **Dataset cards / datasheets:** who collected the data, how, and why
- **Read before the CSV:** the best guard against silent selection bias
- **Pre-cleaned by construction:** already deduplicated, filtered, formatted
- **Real-world contrast:** raw data arrives with none of that done

Key point: The download is the easy part. Provenance, license, and collection bias remain your responsibility — the repository hands you a file, not an understanding.

### Visualization (canvas `c4`, 720×380)

Two-column diagram: green "in the download" column on the left, red "not in the download" column on the right, separated by a dashed divider.

- **Title (bold 14px `#1a5276`, centered, y=22):** "What ships in the download — and what never does"
- **Left header:** 300×36 at (40, 50), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 12px `#27ae60` centered: "IN THE DOWNLOAD".
- **Left item boxes (each 300×52 at x=40, white fill, 1.5px `#27ae60` border; bold 12px `#27ae60` label, 11px `#666` subline, both centered):**
  - y=104 "Data files" — "versioned, formatted, ready to load"
  - y=168 "License tag" — "named, but still yours to read"
  - y=232 "Dataset card / datasheet" — "how and why it was collected"
- **Right header:** 300×36 at (380, 50), fill `rgba(231,76,60,0.08)`, 2px `#e74c3c` border. Bold 12px `#e74c3c` centered: "NOT IN THE DOWNLOAD".
- **Right item boxes (each 300×52 at x=380, white fill, 1.5px `#e74c3c` border; bold 12px `#e74c3c` label, 11px `#666` subline, both centered):**
  - y=104 "Collection bias" — "who was sampled, who was skipped"
  - y=168 "Label errors" — "famous benchmarks contain mislabeled rows"
  - y=232 "Real-world mess" — "the cleaning already done for you, invisibly"
- **Divider:** dashed (6/5) 2px `#ccc` vertical line at x=360 from y=50 to y=284.
- **Caption (12px `#999`, centered, y = h−14):** "The repository hands you a file — provenance and fitness for use stay your job"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 380/340/340/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(39,174,96,0.10)`, `rgba(142,68,173,0.12)`, `rgba(231,76,60,0.08)`.
- **Bullet style:** each bullet is a bold label plus a short phrase that fits on one line (no text wrap); labels are colored via `li strong { color: #1a5276; }`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
