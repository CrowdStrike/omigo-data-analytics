# Web Scraping & Crawl Archives

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Web Scraping & Crawl Archives — Extracting Data From Pages Built for Humans

**Subtitle:** When a site offers no export, no API, and no download button, the page itself is the dataset: you can extract it with your own toolchain, or start from archives like Common Crawl that already crawled the web for you.

**Intro callout (blue-left-border box):** Most of the web was built for human eyes, not for analysis — the data is there, but it is wrapped in layout, navigation, and advertising. Scraping inverts that: a program fetches pages meant for people and turns markup into rows and columns. The technique is simple; doing it politely, legally, and reliably is where the real work is.

## 1. The DIY toolchain — escalate only as far as the page demands

The toolchain scales with the difficulty of the page: a static page needs only an HTTP request and a parser, while a modern JavaScript application needs a real browser.

- **Static pages:** an HTTP client such as requests fetches the raw HTML.
- **Parsing:** BeautifulSoup walks the tag tree to extract the fields you want.
- **JavaScript pages:** view-source shows an empty shell; scripts build the content.
- **Browser automation:** Selenium or Playwright scrapes the rendered DOM.
- **Crawling at scale:** Scrapy manages queues, retries, throttling, and pipelines.
- **Politeness:** rate-limit requests, honor robots.txt, send an honest User-Agent.

Key point: Escalate only as far as the page demands: request-plus-parser covers a static page, a headless browser covers a JavaScript app, and a crawl framework covers scale. Each step up costs speed and complexity, so the cheapest tool that works is the right one.

### Visualization (canvas `c1`, 720×420)

Decision-flow diagram: three page situations in a left column, each routed by an arrow to the matching tool box on the right, with dashed "if not" connectors between questions and a politeness bar spanning all paths at the bottom.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Match the tool to the page — escalate only when forced"
- **Question boxes (left column, each 300×56 at x=40, white fill, 1.5px `#999` border; bold 12px `#555` first line at +22, 11px `#666` second line at +40, both left-aligned at +14):**
  - y=52: "Data already in the HTML?" / "view-source shows the values"
  - y=148: "Content renders via JavaScript?" / "view-source shows an empty shell"
  - y=244: "Thousands of pages to fetch?" / "retries, scheduling, dedup needed"
- **"If not" connectors:** dashed (5/4) 1.5px `#bbb` vertical lines at x=190 between consecutive question boxes, with a 10px `#999` label "if not" to the right of each.
- **Arrows:** 1.5px `#bbb` horizontal lines from each question box's right edge (x=340) to the matching tool box's left edge (x=400) at the box's vertical center, with a small filled `#bbb` right-arrowhead.
- **Tool boxes (right column, each 300×56 at x=400, 2px border in box color; bold 12px label in box color at +22, 11px `#666` subline at +40, left-aligned at +14):**
  - y=52 "requests + BeautifulSoup" `#27ae60`, fill `rgba(39,174,96,0.10)` — "fetch the HTML, parse the tag tree"
  - y=148 "Selenium / Playwright" `#e67e22`, white fill — "drive a real browser, scrape the rendered DOM"
  - y=244 "Scrapy" `#1a5276`, fill `rgba(26,82,118,0.12)` — "managed crawl: queues, throttling, pipelines"
- **Politeness bar:** 660×44 at (30, 340), fill `rgba(231,76,60,0.08)`, 2px `#e74c3c` border. Bold 12px `#e74c3c` centered at +18: "POLITENESS ON EVERY PATH"; 11px `#666` centered at +34: "rate-limit requests · honor robots.txt · identify your crawler in the User-Agent".
- **Caption (12px `#999`, centered, y = h−14):** "The cheapest tool that works is the right one — every escalation costs speed and complexity"

## 2. Pre-crawled archives — someone already fetched the web

Before writing a scraper, check whether someone already crawled the pages you need — several archives publish the web itself as a downloadable dataset.

- **Common Crawl:** nonprofit petabyte-scale web crawls, running since 2008.
- **LLM lineage:** its crawls seeded many language-model training corpora.
- **Wayback Machine:** the Internet Archive preserves page history over time.
- **History:** see what a price or policy said last year — no live scrape can.
- **Wikipedia / Wikidata:** full database dumps — never scrape the live pages.
- **The trade-off:** snapshots lag by weeks but skip crawling and defenses.

Key point: The archives invert the default question: instead of asking "how do I crawl this?", ask "has it already been crawled?". For historical questions the archive is not merely cheaper — it is the only source, because the live page has already changed.

### Visualization (canvas `c2`, 720×380)

Layered stack: four horizontal bands from the live web down to structured dumps, with a vertical effort arrow on the right showing crawling work decreasing as you descend.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The web as a downloadable dataset — layers of already-collected pages"
- **Bands (each 560×58 at x=40, 2px border in band color; bold 12px label in band color at +22, 11px `#666` subline at +40, both left-aligned at +14):**
  - y=48 "LIVE WEB" `#999`, white fill — "pages exist only until the next edit — scrape it yourself, subject to defenses"
  - y=124 "COMMON CRAWL" `#1a5276`, fill `rgba(26,82,118,0.12)` — "nonprofit crawling since 2008 — petabyte snapshots that seeded LLM corpora"
  - y=200 "WAYBACK MACHINE (Internet Archive)" `#8e44ad`, fill `rgba(142,68,173,0.12)` — "historical captures — see what a page said last year, not just today"
  - y=276 "WIKIPEDIA / WIKIDATA DUMPS" `#27ae60`, fill `rgba(39,174,96,0.10)` — "full database exports on a schedule — never scrape what ships as a dump"
- **Effort arrow:** 1.5px `#999` vertical line at x=632 from y=56 to y=326 with a filled `#999` down-arrowhead at the bottom. Left-aligned 10px `#999` labels at x=644: two lines "you run" / "the crawl" at y=64/76, and two lines "download" / "and go" at y=306/318.
- **Caption (12px `#999`, centered, y = h−14):** "Check the archives before building a crawler — the fetch you skip is the cheapest one"

## 3. The legal landscape — public does not mean free of contract

US courts have separated two questions that beginners conflate: whether scraping public pages is "hacking", and whether it breaks a contract you agreed to.

- **CFAA track:** hiQ v. LinkedIn — scraping public pages is not "hacking".
- **The reasoning:** no access barrier is circumvented on a public page.
- **Contract track:** hiQ ultimately lost on breach-of-terms grounds.
- **Terms bind:** a ToS can be enforceable even when the CFAA claim fails.
- **Privacy law:** GDPR covers scraped personal data regardless of source.
- **Practical reading:** one scrape can breach contract and privacy at once.

Key point: Treat legality as three separate gates — anti-hacking law, the site's terms, and privacy law — and clear all of them. This survey describes the landscape; for any real project the call belongs to a lawyer, not a tutorial.

### Visualization (canvas `c3`, 720×380)

Two-track diagram: one scraping scenario at top splitting into a CFAA track (green, scraper prevails) and a contract track (red, scraper loses), with a privacy-law band underneath both.

- **Title (bold 14px `#1a5276`, centered, y=22):** "hiQ v. LinkedIn — two legal tracks, two different answers"
- **Scenario box:** 380×46 at (170, 44), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered at +19: "SCRAPING PUBLICLY VISIBLE PAGES"; 11px `#666` centered at +36: "hiQ scraped public profile pages at scale".
- **Split connectors:** 1.5px `#bbb` lines from the scenario box's bottom center (x=360, y=90) to the top centers of the two track boxes (x=190 and x=530, y=140).
- **Track boxes (each 300×88 at y=140, white fill, 2px border in track color; bold 12px heading in track color at +22, 11px `#666` lines at +44 and +62, all left-aligned at +14):**
  - x=40 "CFAA — anti-hacking statute" `#27ae60` — "no access barrier was circumvented;" / "public pages are not 'unauthorized access'"
  - x=380 "CONTRACT — terms of service" `#e74c3c` — "the terms prohibited scraping;" / "terms can bind even when CFAA does not"
- **Outcome boxes (each 300×40 at y=252, 2px border in track color; bold 12px in track color at +17, 11px `#666` at +33, left-aligned at +14), connected to their track box by a 1.5px `#bbb` vertical line at the box center:**
  - x=40, fill `rgba(39,174,96,0.10)` — "Not hacking" / "the appeals court sided with the scraper"
  - x=380, fill `rgba(231,76,60,0.08)` — "hiQ ultimately lost" / "on breach-of-terms grounds"
- **Privacy band:** 660×36 at (30, 314), fill `rgba(142,68,173,0.12)`, 1.5px `#8e44ad` border. Bold 11px `#8e44ad` centered at +23: "GDPR and other privacy laws apply to scraped personal data on top of both tracks".
- **Caption (12px `#999`, centered, y = h−14):** "'Publicly visible' answers the hacking question — not the contract or privacy questions"

## 4. Engineering reality — every scraper is born decaying

A scraper's dependency is another team's HTML, and that team owes you nothing: markup changes without notice, so every scraper starts decaying the day it ships.

- **Scraper rot:** sites rename classes and restructure markup without notice.
- **Silent failure:** parsers return nulls while the job still reports success.
- **Late discovery:** weeks of empty fields pass before an analyst notices.
- **Anti-bot escalation:** rate limits, CAPTCHAs, and IP blocking raise costs.
- **Product, not script:** field tests, volume monitoring, and alerting.
- **Ownership:** a scraper that matters has an on-call owner, like any service.

Key point: Budget for the scraper's whole life, not its first run: the one-off script that worked in an afternoon becomes a liability at the first site redesign. If the data matters, the scraper is a monitored product with an owner.

### Visualization (canvas `c4`, 720×360)

Scraper-rot timeline: four dated steps from clean launch to late discovery, with a two-box contrast row underneath comparing a one-off script to a monitored product.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The life of an unmonitored scraper — rot is silent until someone looks"
- **Timeline:** 2px `#999` line at y=120 from x=50 to x=670 with a filled right-arrowhead.
- **Steps (filled dot radius 7 in step color on the line; labels alternate above/below with thin `#ccc` connectors; label bold 12px in step color, subline 11px `#666`, time tag 10px `#999` on the opposite side of the line):**
  - x=95, "day 0", "Scraper ships" — "all fields extracted cleanly" — `#27ae60` (above)
  - x=250, "week 6", "Site redesign" — "CSS classes renamed, no notice" — `#e67e22` (below)
  - x=405, "same day", "Silent breakage" — "parser returns nulls, job still 'green'" — `#e74c3c` (above)
  - x=565, "week 10", "Analyst notices" — "a month of empty columns" — `#e74c3c` (below)
- **Contrast boxes (each 310×70 at y=230, white fill, 2px border in box color; bold 12px heading in box color at +22, 11px `#666` lines at +42 and +58, all left-aligned at +14):**
  - x=45 "ONE-OFF SCRIPT" `#e74c3c` — "breakage found weeks later," / "by a confused downstream analyst"
  - x=380 "MONITORED PRODUCT" `#27ae60` — "field-level tests and volume alerts" / "fire the day the markup shifts"
- **Caption (12px `#999`, centered, y = h−14):** "Sites owe your parser nothing — if the data matters, the scraper is a product with an owner"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 420/380/380/360 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (multiply backing store, `ctx.scale(dpr,dpr)`). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(39,174,96,0.10)`, `rgba(142,68,173,0.12)`, `rgba(231,76,60,0.08)`.
- **Bullets:** each bullet is a bold label plus a short one-line phrase — no text wrap. Labels are `<strong>` elements colored `#1a5276` via `li strong { color: #1a5276; }`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
