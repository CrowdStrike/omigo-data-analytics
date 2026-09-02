# Scientific & Research Corpora — Published to Advance Science, Reused Far Beyond It

**Page type:** detail page (two-column layout table per section: text left 45%, canvas right 55%, one `.lang-section` per topic)
**HTML title tag:** Scientific & Research Corpora — Published to Advance Science, Reused Far Beyond It

**Subtitle:** Openness is the norm of science: papers, genomes, telescope surveys, and web crawls are released so results can be verified and built on. But once a corpus is public, it serves every purpose equally — including purposes the release never anticipated.

**Intro callout:** Science publishes by default. Preprints, sequence archives, sky surveys, climate model runs, and research web crawls were all released under the same implicit contract: share the data so other scientists can check and extend the work. That contract is one-directional — it constrains why the data was released, not how it can be used. The same open genome database that enables population genetics enables forensic family searching; the same research web crawl that enables linguistics enables commercial model training. Intent does not travel with data.

## 1. The literature itself — papers as a dataset

The scientific record is now a machine-readable corpus, not just a library:

- **arXiv** — preprints in physics, math, CS since 1991; full text and LaTeX source, bulk-downloadable.
- **PubMed / PubMed Central** — biomedical abstracts and open-access full text, backed by public funder mandates requiring deposit.
- **Semantic Scholar / OpenAlex** — citation graphs linking papers, authors, and institutions into one queryable network.
- **Open-access mandates worldwide** — funders increasingly require that publicly funded results be publicly readable, which also makes them publicly minable.
- **Reuse beyond reading** — text mining, citation-based rankings of people and departments, and full-text corpora feeding language model training.

**Key point:** A paper published so peers could read it is now also a node in a citation graph that scores its author, and a training document for models its author never heard of. Publication granted all of these at once.

### Visualization (canvas `c1`, 720×340)

Pipeline diagram: four stage boxes connected by arrows, with two contrast brackets underneath.

- **Title (bold 14px `#1a5276`, top center):** "One paper's lifecycle: written once, reused as data forever".
- **Stages (boxes 150×90 at y=60, 32px gaps, centered as a row; white fill, 2px colored stroke; bold 13px title in stage color, 11px `#555` subtitle, 11px `#999` note; `#888` arrows between boxes):**
  - "Preprint" / "arXiv / bioRxiv" / "shared for peers" — `#1a5276`
  - "Indexed" / "PubMed, S2, OpenAlex" / "node in citation graph" — `#27ae60`
  - "Mined" / "text + graph analytics" / "scores authors, fields" — `#e67e22`
  - "Trained on" / "LLM corpora" / "model training document" — `#e74c3c`
- **Brackets (square brackets drawn at y≈190–200 with bold 11px centered labels at y=220):** green `#27ae60` bracket under stage 1 only, labeled "what the author decided"; red `#e74c3c` bracket under stages 2–4, labeled "what publication granted".
- **Note (11px `#666`, centered, y=262):** "Open-access mandates make the record readable — and therefore minable — by design".
- **Caption (12px `#999`, bottom center):** "The author chose stage 1; the corpus took care of the rest".

## 2. Genomes — one upload publishes a family

Sequence data is the most shared data in biology — and the most personal:

- **GenBank & the international archives** — every published sequence deposited, mirrored across the US, Europe, and Japan; withdrawal effectively impossible.
- **1000 Genomes** — full human genomes released publicly by consenting volunteers to map human variation.
- **GEDmatch** — hobbyists uploaded consumer DNA results to find relatives for genealogy.
- **The Golden State Killer case** — police uploaded crime-scene DNA to GEDmatch, matched distant cousins who had joined for genealogy, and walked the family tree to the suspect.
- **Forensic genetic genealogy** is now routine — a database built for hobby ancestry doubles as an investigative index of people who never joined it.

**Key point:** DNA is inherently familial: you share large segments with relatives you have never met. One person's voluntary upload partially publishes their whole extended family — consent by one, identifiability for all.

### Visualization (canvas `c2`, 720×400)

Family-tree network: three generations of nodes; the uploader (green) and suspect (red) highlighted, with the investigative match path drawn in red.

- **Title (bold 14px `#1a5276`, top center):** "One cousin uploads to GEDmatch — the whole tree becomes findable".
- **Nodes (circles radius 15; uploader filled `#27ae60`, suspect filled `#e74c3c`, all others filled `rgba(26,82,118,0.35)` with `#1a5276` stroke; 11px labels below, bold for uploader/suspect):**
  - "great-grandparents" (360,70); "grandparent A" (200,150); "grandparent B" (520,150)
  - "parent" (110,235); "aunt / uncle" (290,235); "parent" (440,235); "aunt / uncle" (610,235)
  - "UPLOADER" (110,320, green); "cousin" (290,320); "suspect" (440,320, red); "cousin" (610,320)
- **Edges:** parent-child tree edges; edges on the investigator path UPLOADER → parent → grandparent A → great-grandparents → grandparent B → parent → suspect drawn in `#e74c3c` 2.5px, all other edges `#ccc` 1.5px.
- **Legend (top left, 11px):** green dot "uploaded DNA (voluntary)"; faded blue dot "never uploaded — findable anyway"; red line "match path walked by investigators".
- **Caption (12px `#999`, bottom center):** "Crime-scene DNA matched the uploader as a distant cousin; genealogy walked the tree to the suspect".

## 3. The instruments' output — big science, fully public

The largest scientific instruments publish their raw output at petabyte scale:

- **Sloan Digital Sky Survey** — periodic public data releases of imaging and spectra; more papers from archive users than from the survey team itself.
- **CERN Open Data** — collision events from the LHC released with the software to reanalyze them.
- **CMIP climate archives** — coordinated climate model runs from centers worldwide, the shared evidence base behind IPCC reports.
- **Ocean and seismic networks** — continuous sensor streams (Argo floats, global seismographs) open in near real time.
- **Professionally curated** — versioned releases, documented calibration, stable identifiers; the gold standard of published data.

**Key point:** This is openness working as designed — yet even here, reuse escapes the frame: seismic networks built for earthquakes detect nuclear tests, and climate archives become exhibits in litigation. Well-curated data is simply easier for everyone to reuse, for anything.

### Visualization (canvas `c3`, 720×330)

Flow diagram: four instrument boxes feeding one curated-release box, with an arrow to a laptop on the right.

- **Title (bold 14px `#1a5276`, top center):** "Billion-dollar instruments, petabyte archives, zero-cost access".
- **Instruments (white boxes 185×36 at x=35, 2px colored stroke, bold 12px label; `#bbb` connectors to the release box):**
  - "Sky survey (SDSS)" — `#1a5276`, y=60
  - "Collider (CERN)" — `#8e44ad`, y=110
  - "Climate models (CMIP)" — `#e67e22`, y=160
  - "Ocean / seismic nets" — `#27ae60`, y=210
- **Curated release box:** 175×115 at (290,95), fill `rgba(26,82,118,0.10)`, 2px `#1a5276` stroke; bold 12px "Curated public release"; 11px `#555` lines: "versioned data releases" / "documented calibration" / "stable identifiers" / "open tools to reanalyze".
- **Arrow:** `#888` arrow from the release box to the laptop.
- **Laptop (right):** 130×62 box at (535,115) with 2px `#27ae60` stroke, inner fill `rgba(39,174,96,0.10)`, green base line beneath; bold 12px green "Anyone's laptop"; 11px `#555` lines: "student, hobbyist, rival," / "lawyer, model builder".
- **Caption (12px `#999`, bottom center):** "The archive outproduces the team that built it — and answers questions the instrument was never aimed at".

## 4. The web as corpus — collected for research, used for everything

The web itself was turned into a research dataset — then the dataset outgrew research:

- **Common Crawl** — a nonprofit's free monthly scrape of the web, framed as a research resource; now the backbone of most LLM training mixtures.
- **Wikipedia dumps** — the full encyclopedia and its edit history, downloadable; the default clean-text corpus for a generation of NLP.
- **ImageNet and benchmark datasets** — images gathered from the web and labeled for a research competition, later foundational to commercial computer vision.
- **Kaggle** — datasets published for contests, redistributed and reused far beyond their original problem statements.
- **Internet Archive / Wayback Machine** — the historical web preserved for scholarship, routinely cited as evidence of what a page said on a given date.

**Key point:** "Research use" was the framing under which the web was collected — but a corpus, once published, serves every purpose equally. Intent does not travel with data; only access does.

### Visualization (canvas `c4`, 720×360)

Fan-out diagram: one Common Crawl box on the left with connectors to four use-case boxes on the right, plus a margin annotation.

- **Title (bold 14px `#1a5276`, top center):** "One crawl snapshot, every purpose at once".
- **Crawl box:** 175×105 at (35,130), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` stroke; bold 13px "Common Crawl"; 11px `#555` lines: "monthly scrape of the web" / "free, bulk-downloadable" / "framed as \"for research\"".
- **Uses (white boxes 245×50 at x=305, 2px colored stroke, bold 12px label in use color, 11px `#666` subtitle; `#bbb` connectors from the crawl box):**
  - "Search & web research" / "the intended framing" — `#27ae60`, y=55
  - "LLM training sets" / "backbone of commercial models" — `#8e44ad`, y=130
  - "Litigation evidence" / "what a page said, and when" — `#e67e22`, y=205
  - "OSINT & profiling" / "people and orgs, at scale" — `#e74c3c`, y=280
- **Margin annotation (11px `#999`, right side at x=575):** "same bytes," / "same license," / "no gatekeeper".
- **Caption (12px `#999`, bottom center):** "Wikipedia dumps, ImageNet, Kaggle, the Wayback Machine — the same fan-out applies to each".

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style 2-col): h1, `.subtitle`, `.intro` callout, then one `.lang-section` per numbered topic. Each section: `<h2>` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row: `td.text-col` (45%) holding an intro sentence, a `<ul>` of labeled bullets (bold lead terms), and a `.key-point` div; `td.viz-col` (55%) holding the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with 3px `#2980b9` left border; `.key-point` background `#f8f9fa` with 3px `#e74c3c` left border; ul 0.92rem. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvases:** intrinsic width 720, heights as given per chart (340/400/330/360); shared `setupCanvas(id, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
