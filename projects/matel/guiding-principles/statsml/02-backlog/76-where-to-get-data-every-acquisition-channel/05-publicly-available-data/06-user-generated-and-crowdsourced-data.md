# User-Generated & Crowdsourced Data — Volunteered One Post at a Time

**Page type:** detail page (two-column layout table per section: text left 45%, canvas right 55%, one `.lang-section` per topic)
**HTML title tag:** User-Generated & Crowdsourced Data — Volunteered One Post at a Time

**Subtitle:** Individuals voluntarily publish posts, reviews, commits, and GPS traces. Each contribution is small and deliberate. In aggregate they become a dataset with properties — coverage, joinability, inference power — that no individual contributor consented to or anticipated.

**Intro callout:** This is the inverse of government open data. Nobody mandated collection; every record was volunteered, one post at a time, each under an implicit contract of "my followers will read this" or "this helps other runners". Aggregation rewrites that contract: the same content becomes a training corpus, a behavioral profile, or a map of things nobody meant to map. The contributor consented to publishing a record — not to the dataset the records form together.

## 1. Public social & knowledge platforms

The largest text corpora in existence were written by volunteers who thought they were talking to each other:

- **Public posts** — X, Reddit, YouTube comments, TikTok captions: written for a feed, harvested as a corpus. Reddit alone underpins much of modern LLM conversational tone.
- **Wikipedia + Wikidata** — encyclopedic prose plus a structured knowledge graph; the closest thing to a canonical free fact base, curated entirely by volunteers.
- **Review platforms** — Yelp, Glassdoor, TripAdvisor: opinions attached to real businesses and employers, mined for sentiment, pricing, and labor signals.
- **Q&A** — Stack Overflow answers written to help one asker now steer code-generation models for everyone.

**Key point:** Researchers and model-trainers treat all of it as corpus. The author's mental audience was "people like me on this platform"; the actual audience includes every crawler, and the actual use includes training systems the author never imagined.

### Visualization (canvas `c1`, 720×380)

Aggregation diagram: four source clusters of small post squares on the left, connectors converging into one corpus box on the right.

- **Title (bold 14px `#1a5276`, top center):** "Written for a feed, harvested as a corpus".
- **Sources (each a 3×2 cluster of 14px squares, fill at 35% alpha of source color plus 1px stroke; bold 12px name in source color; italic 11px `#999` quote):**
  - "X / Reddit posts" / "\"my followers will read this\"" — `#1a5276`, y=55
  - "Wikipedia + Wikidata" / "\"improving the encyclopedia\"" — `#27ae60`, y=115
  - "Yelp / Glassdoor reviews" / "\"warning other customers\"" — `#e67e22`, y=175
  - "Stack Overflow answers" / "\"helping one asker\"" — `#8e44ad`, y=235
- **Connectors:** `#bbb` lines from each source (x=295) converging to (400,165).
- **Corpus box:** 280×150 at (400,90), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` stroke. Bold 13px heading "THE CORPUS"; 11px `#555` bullets: "• LLM training data" / "• sentiment and market research" / "• behavioral and labor studies" / "• knowledge-graph construction"; italic 11px `#e74c3c` line: "none of these uses were the author's intent".
- **Caption (12px `#999`, bottom center):** "The mental audience was the platform; the actual audience is every crawler".

## 2. Public work trails

Professional activity leaves a public exhaust that reads like a behavioral log:

- **GitHub public repos** — commits carry an email address and a timestamp; issues and PR reviews carry writing style and collaborators; the org you contribute to implies your employer.
- **Commit timestamps** — your contribution graph exposes your timezone, working hours, weekends, and vacations. A schedule change is visible before you announce anything.
- **Package registries** — npm/PyPI maintainer identities link projects to people and companies; a maintainer going quiet is a public supply-chain signal.
- **Professional profiles** — public LinkedIn-style pages get scraped into recruiting and sales datasets; job-change events become a tracked signal.

**Key point:** None of this was posted as "my schedule" or "my employer" — it is inferable from metadata on work published for entirely different reasons. The work is the content; the profile is the byproduct.

### Visualization (canvas `c2`, 720×340)

Heatmap: commit intensity by day-of-week (rows Mon–Sun) × hour-of-day (24 columns), with three annotated inference regions.

- **Title (bold 14px `#1a5276`, top center):** "One developer's public commit timestamps, hour by day".
- **Grid:** 7 rows × 24 columns, cells 24×26 px (minus 2px gutters), left=70, top=50; hour labels every 4 hours ("00", "04", … "20") in 10px `#999` above; day labels ("Mon"–"Sun") in 11px `#666` at left. Cell fill: `#f4f6f8` when intensity 0, otherwise `rgba(26,82,118, 0.12 + 0.7×v)`.
- **Intensity rule (deterministic):** weekends only a faint window at hours 10–12 (0.15); weekdays 0 before 08 and after 21; lunch dip 0.2 at hour 12; work hours 9–17 heavy (0.55 + 0.35·|sin(d·3.1 + hr·1.7)|); evenings 19–21 light (0.2); other weekday hours 0.1.
- **Annotation boxes (1.5px stroked rectangles over grid regions, each with a bold 11px caption below the grid in the same color):**
  - `#e74c3c` around hours 0–7 × Mon–Fri: "empty 00–07 → timezone readable"
  - `#e67e22` around hours 9–17 × Mon–Fri: "dense weekday 9–17 → employed, on a schedule"
  - `#8e44ad` around all 24 hours × Sat–Sun: "near-empty weekends → work account, not hobby"
- **Caption (12px `#999`, bottom center):** "Nothing here was posted as personal information — it is metadata on published work".

## 3. Aggregate leaks: the Strava lesson

Sometimes the aggregate discloses something no single contribution contains:

- **Strava heatmap (2018)** — the global fitness heatmap, built from users' shared jogging GPS traces, revealed the layouts of secret military bases and patrol routes: soldiers jogging the perimeter drew the perimeter.
- **WiGLE** — hobbyist wardrivers have mapped hundreds of millions of WiFi SSIDs to street locations; your router name is a geocodable public record you never published anywhere.
- **Flight and ship spotting** — enthusiast communities logging tail numbers and hull sightings collectively track corporate jets, deportation flights, and sanctioned tankers better than any single official feed.

**Key point:** Emergent disclosure: each contribution is individually harmless — one jog, one SSID, one tail-number photo. The aggregate reveals what no single contributor knew they were revealing, so no contributor could have consented to it and no per-record review could have caught it.

### Visualization (canvas `c3`, 720×400)

Two-panel before/after: individual GPS jog fragments on the left, overlaid aggregate heatmap perimeter on the right, joined by an "overlay" arrow.

- **Title (bold 14px `#1a5276`, top center):** "Emergent disclosure: harmless traces, revealing aggregate".
- **Left panel:** `#e0e0e0` bordered box at (35,45) size 300×280, headed in bold 12px `#27ae60`: "Each shared jog: individually harmless". Five jittered partial traces (colors `#27ae60`, `#e67e22`, `#8e44ad`, `#1a5276`, `#e74c3c`, alpha 0.7, 1.5px) each covering a 28% arc-fragment of the same rectangular perimeter path (rectangle x0=85, y0=110, 200×170), offset starts of 0.2 each, sinusoidal jitter ±4px. Notes in 11px `#999`: "5 soldiers, 5 partial routes," / "no route shows the whole shape".
- **Arrow:** gray `#888` arrow from (345,185) to (385,185), labeled "overlay" in 10px above.
- **Right panel:** `#e0e0e0` bordered box at (395,45) size 300×280, headed in bold 12px `#e74c3c`: "Global heatmap: the base perimeter". A glowing red rectangle at (455,110) size 190×165 drawn in three passes (`rgba(231,76,60,…)` alphas 0.15/0.3/0.9, widths 10/6/2) plus a vertical "patrol spur" line from (550,110) up to (550,70) with the same glow, labeled "patrol spur" in 11px `#999` above. Notes in 11px `#666`: "perimeter + patrol route," / "at a site on no public map".
- **Caption (12px `#999`, bottom center):** "Same mechanism: WiGLE's SSID map, flight/ship-spotter tracking — the aggregate knows what no contributor did".

## 4. Persistence & scraping

Deleting a public post removes one copy — the one you control:

- **Archives** — the Wayback Machine and community mirrors snapshot public pages continuously; deleted posts survive as archived pages.
- **Quotes and screenshots** — replies, embeds, and screenshots propagate the content into places the delete button cannot reach.
- **Scraped datasets** — research dumps and training corpora are cut from the platform at a point in time; deletion after the cut changes nothing downstream.
- **ToS vs practice** — platform terms may forbid scraping, but "public" technically means visible to any logged-out crawler, not just your followers. Enforcement is civil and after the fact; the copy already exists.

**Key point:** Delete removes the original, not the copies. Publication to a public platform is effectively irreversible — the realistic model is "everything public is permanently archived somewhere", and consent-revocation features only bind the one party that offered them.

### Visualization (canvas `c4`, 720×360)

Fan-out diagram: a deleted original post on the left, dashed connectors to four surviving-copy boxes on the right.

- **Title (bold 14px `#1a5276`, top center):** "Delete removes the original — the copies were already made".
- **Original post:** 170×90 box at (40,130), fill `rgba(231,76,60,0.07)`, 2px `#e74c3c` stroke; bold 12px "Original post", 11px `#666` "public, 2021"; a red X (2.5px diagonals) crosses the box; bold 11px red label below: "DELETED 2024".
- **Copies:** four 340×46 white boxes at x=340 with 2px colored stroke, bold 12px name in copy color plus 11px `#666` subtitle, each reached by a dashed `#bbb` connector (dash 5/4) from the original:
  - "Wayback Machine snapshot" / "archived within hours of posting" — `#1a5276`, y=55
  - "Research / training dump" / "corpus cut in 2022 — delete changes nothing" — `#8e44ad`, y=125
  - "Quote-posts and screenshots" / "propagated beyond the platform" — `#e67e22`, y=195
  - "Third-party scraper mirror" / "ToS forbids it; the copy exists anyway" — `#27ae60`, y=265
- **Caption (12px `#999`, bottom center):** "\"Public\" means visible to any logged-out crawler, not just followers — publication is effectively irreversible".

## Regeneration instructions

- **Layout:** backlog detail page (kusto-style 2-col): h1, `.subtitle`, `.intro` callout, then one `.lang-section` per numbered topic. Each section: `<h2>` with 2px `#2980b9` bottom border, then a `table.layout` (border-collapse, full width) with one row: `td.text-col` (45%) holding an intro sentence, a `<ul>` of labeled bullets (bold lead terms), and a `.key-point` div; `td.viz-col` (55%) holding the canvas. No index number in the h1.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with 3px `#2980b9` left border; `.key-point` background `#f8f9fa` with 3px `#e74c3c` left border; ul 0.92rem. Canvases `width: 100%`, 1px `#e0e0e0` border, 4px radius.
- **Canvases:** intrinsic width 720, heights as given per chart (380/340/400/360); shared `setupCanvas(id, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
