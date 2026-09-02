# APIs & Web Services — Structured Data From Official Endpoints

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** APIs & Web Services — Structured Data From Official Endpoints

**Subtitle:** The front-door channel: a provider publishes documented endpoints that return structured records on request, and access is governed by keys, quotas, and terms — you get clean data in exchange for accepting the provider's rules.

**Intro callout (blue-left-border box):** An official API is the provider saying "here is exactly how to ask, and here is exactly what you will get back." Compared with scraping, everything is easier: the schema is documented, the format is machine-readable, and the provider wants you there — up to a point. That point is defined by three things every API pipeline must respect: the key that identifies you, the quota that meters you, and the terms that constrain what you may do with the responses.

## 1. What an official API gives you

A documented endpoint returns structured JSON with a stable schema, authenticates you with an API key, and paginates large results so you can pull data in bulk without parsing a single web page.

- **Documented schema:** every field named, typed, and defined in the reference docs.
- **Contract, not guesswork:** your parser targets the spec, not today's HTML.
- **API key:** identifies the caller on every single request.
- **Metering and revocation:** the key lets the provider meter, notify, and cut off.
- **Pagination:** bulk results arrive as numbered or cursor-linked pages.
- **Examples:** GitHub (repo activity), FRED (economic time series).
- **More examples:** OpenWeatherMap (weather), Alpha Vantage (market quotes).

Key point: The API is the provider's front door: the data arrives clean, typed, and versioned, and in exchange the provider always knows who you are, how much you take, and can change the deal at any time.

### Visualization (canvas `c1`, 720×400)

Request/response flow: a client box with a key sends a request to an endpoint box, paginated response pages stack up and merge into a local table.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The front door: key in, structured pages out"
- **Client box:** 190×86 at (40, 60), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` left-aligned at (+14, +24): "YOUR CLIENT"; 11px `#555` lines: "script or pipeline" (+44), "sends an API key" (+60); 11px `#999`: "identified on every call" (+78).
- **Request arrow:** 2px `#1a5276` line from (230, 96) to (420, 96) with filled right arrowhead; bold 11px `#1a5276` centered above at (325, 84): "GET /series?page=1"; 10px `#999` centered below at (325, 110): "key identifies the caller".
- **Endpoint box:** 250×86 at (430, 60), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 12px `#27ae60` left-aligned: "API ENDPOINT"; 11px `#555` lines: "documented JSON schema" / "checks key, applies quota"; 11px `#999`: "GitHub · FRED · OpenWeatherMap".
- **Response pages:** three overlapping 150×64 white rectangles with 1.5px `#27ae60` borders, offset by (12, 12) each, top-left corners at (470, 180), (482, 192), (494, 204). On the front page: bold 11px `#27ae60` "page 3 of 3"; 10px `#666` lines "{ \"date\": ..., \"value\": ... }" and "next: null". Curved or angled 1.5px `#bbb` connector from endpoint box bottom to the page stack, labeled 10px `#999`: "paginated responses".
- **Merge arrow:** 2px `#999` line with arrowhead from the page stack's left edge toward the local table, labeled 10px `#999`: "loop until last page".
- **Local table:** at (60, 210), a 260×130 grid — header row fill `rgba(26,82,118,0.12)` with bold 10px `#1a5276` column labels "date | value | series"; 3 data rows of 10px `#666` placeholder values separated by 1px `#e0e0e0` lines; 2px `#1a5276` outer border. Bold 12px `#1a5276` above the table: "LOCAL TABLE"; 11px `#666` below: "typed rows, ready for analysis".
- **Caption (12px `#999`, centered, y = h−14):** "No parsing, no guessing — the schema is a documented contract"

## 2. The rate-limit economy

Quotas attached to your key and tier decide how fast you can collect, so serious pipelines are designed around the limit rather than against it.

- **Quota per key:** free keys allow tens of calls a minute, paid tiers thousands.
- **Tier sets speed:** your plan, not your bandwidth, caps collection rate.
- **Batching:** many records per call stretches each unit of quota.
- **Caching:** store responses locally; never spend quota on a repeat question.
- **Exponential backoff:** on errors, wait progressively longer before retrying.
- **Incremental sync:** pull only records changed since the last run.

Key point: A rate limit is not an obstacle to route around — it is the price schedule of the API. Pipelines that batch, cache, back off, and sync incrementally get more data per unit of quota than pipelines that simply loop faster.

### Visualization (canvas `c2`, 720×380)

Horizontal collection-speed bars by tier, plus an exponential-backoff inset showing retry delays doubling.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Your tier sets your speed — your design sets your efficiency"
- **Tier bars (left region x=40..430, three horizontal bars, bar height 34, labels left-aligned bold 12px in bar color at bar start y−8):**
  - y=70 "Free tier" `#e74c3c` — bar width 70, fill `rgba(231,76,60,0.25)`, 2px `#e74c3c` border; 11px `#666` right of bar: "~60 requests/hour".
  - y=150 "Basic tier" `#e67e22` — bar width 190, fill `rgba(230,126,34,0.25)`, 2px `#e67e22` border; 11px `#666` right of bar: "~5,000 requests/hour".
  - y=230 "Enterprise tier" `#27ae60` — bar width 340, fill `rgba(39,174,96,0.20)`, 2px `#27ae60` border; 11px `#666` right of bar: "negotiated, effectively bulk".
  - Under the bars, 11px `#999` left-aligned at (40, 300): "same endpoint, same data — the key decides the flow rate".
- **Backoff inset (right region):** 240×200 panel at (450, 88), white fill, 1.5px `#bbb` border, bold 12px `#1a5276` centered title inside at top: "Exponential backoff". Four small `#e74c3c` squares (10×10) along a baseline representing failed attempts at increasing horizontal gaps (gaps 18, 36, 72 px), each labeled below in 10px `#666`: "wait 1s", "wait 2s", "wait 4s"; a final 10×10 `#27ae60` square labeled "success". Thin `#ccc` baseline under the squares; 10px `#999` note at panel bottom: "retry later, not harder".
- **Bottom strip:** four labeled 150×34 boxes in a row at y=316, x=40/205/370/535, 1.5px borders, bold 11px labels centered, colors `#1a5276`/`#1a5276`/`#e67e22`/`#27ae60`: "BATCH" ("many records per call" 10px `#666` below inside), "CACHE" ("never ask twice"), "BACK OFF" ("respect the errors"), "SYNC DELTAS" ("only what changed").
- **Caption (12px `#999`, centered, y = h−14):** "Efficient pipelines spend quota like a budget, not like a race"

## 3. The closing of the free era

For years generous free API tiers were the norm; when language-model training made text archives commercially valuable, major platforms repriced access — an API is a business decision that can change under you.

- **The open years:** 2010s platforms gave free access to grow their ecosystems.
- **Reddit, 2023:** the previously free API moved to paid pricing.
- **X (Twitter), 2023:** the free access tier ended the same year.
- **The driver:** text archives became valuable as LLM training data.
- **Downstream breakage:** clients, mod tools, and studies paid up or shut down.
- **The lesson:** every external API is a dependency with deprecation risk.

Key point: An API is not a public utility — it is a product feature that exists as long as it serves the provider's business. Treat every external API the way you treat a vendor contract: assume the terms can change, keep raw copies of what you are allowed to keep, and know your fallback.

### Visualization (canvas `c3`, 720×340)

Timeline of the free-API era closing: open-access era band, 2023 repricing events, paid-tier era band.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The free-API era, and the year the door narrowed"
- **Timeline axis:** 2px `#999` line at y=170 from x=50 to x=670 with filled right arrowhead; 10px `#999` year ticks below the line at x=90 "2008", x=230 "2013", x=370 "2018", x=510 "2023", x=640 "today".
- **Era bands (rounded rectangles behind the axis, height 56, centered vertically on y=170):**
  - Open era: from x=60 to x=470, fill `rgba(39,174,96,0.12)`, 1.5px `#27ae60` border; bold 12px `#27ae60` centered above the band at y=118: "OPEN ACCESS ERA"; 11px `#666` centered at y=134: "generous free tiers grow the ecosystem".
  - Paid era: from x=530 to x=660, fill `rgba(231,76,60,0.10)`, 1.5px `#e74c3c` border; bold 12px `#e74c3c` centered above at y=118: "PAID TIERS"; 11px `#666` centered at y=134: "metered, priced, contract-gated".
- **2023 event marker:** vertical dashed (5/4) 2px `#e74c3c` line at x=510 from y=60 to y=280; filled `#e74c3c` dot (radius 6) on the axis at x=510. Bold 12px `#e74c3c` centered at (510, 50): "2023".
- **Event callouts (two boxes below the axis, each 240×52, white fill, 1.5px `#e74c3c` border, thin `#ccc` connector to the marker dot):**
  - Box A at (250, 232): bold 11px `#e74c3c` "Reddit prices its API"; 10px `#666` "free tier ends; third-party" / "clients shut down".
  - Box B at (520, 232) shifted left to fit (x=470): bold 11px `#e74c3c` "X (Twitter) ends free access"; 10px `#666` "researchers and tools" / "lose the open tap".
- **Driver note (above the marker, right side):** 11px `#8e44ad` left-aligned near (525, 84), two lines: "driver: text archives became" / "LLM training data — suddenly valuable".
- **Caption (12px `#999`, centered, y = h−14):** "An API is a business decision — plan for the day the terms change"

## 4. Terms of use — what you may do with the responses

Getting the bytes is the easy part; the API's terms of service decide how long you may store responses, whether you may share them onward, and increasingly whether you may train models on them.

- **Storage windows:** terms may cap how long responses can be cached.
- **Market-data example:** some quote APIs require discard or refresh after a period.
- **Redistribution:** analyzing internally and republishing are different rights.
- **Free-tier norm:** analysis granted, redistribution reserved for paid licenses.
- **Model training:** newer terms forbid it or price it as a separate tier.
- **Attribution:** many terms require crediting the source.
- **Revocation:** keys can be pulled; record the terms version you collected under.

Key point: Read the terms before you build, not after: "I can fetch it" establishes only access, while storage, redistribution, and training rights are separate permissions that the same API may grant, meter, or deny independently.

### Visualization (canvas `c4`, 720×380)

Rights matrix: rows are four uses (store, analyze internally, redistribute, train models), columns are typical free tier vs commercial license, cells marked allowed / limited / restricted.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Access is one right — each use of the data is another"
- **Grid geometry:** row-label column x=40..250; two data columns x=250..460 ("TYPICAL FREE TIER") and x=460..670 ("COMMERCIAL LICENSE"); header row y=50..86; four data rows of height 56 from y=86 to y=310. Grid lines 1px `#ccc`; outer border 2px `#1a5276`; header fill `rgba(26,82,118,0.12)` with bold 11px `#1a5276` centered column titles; row labels bold 11px `#2c3e50` left-aligned at x=52 with a 10px `#999` subline: rows "STORE RESPONSES" ("keep a local copy"), "ANALYZE INTERNALLY" ("charts, stats, reports"), "REDISTRIBUTE" ("republish to others"), "TRAIN MODELS" ("fit ML on responses").
- **Cell rendering:** each cell contains a centered status chip 150×26 with 1.5px border and bold 10px centered text — allowed: `#27ae60` border, fill `rgba(39,174,96,0.10)`, text "ALLOWED"; limited: `#e67e22` border, fill `rgba(230,126,34,0.12)`, text as noted; restricted: `#e74c3c` border, fill `rgba(231,76,60,0.10)`, text as noted.
  - Store: free "LIMITED — cache window" (orange) / commercial "ALLOWED" (green)
  - Analyze internally: free "ALLOWED" (green) / commercial "ALLOWED" (green)
  - Redistribute: free "RESTRICTED" (red) / commercial "LICENSED" (orange)
  - Train models: free "OFTEN FORBIDDEN" (red) / commercial "SEPARATE TIER" (orange)
- **Footnote (11px `#999`, left-aligned at x=40, y=336):** "illustrative pattern — every API's terms differ; the row/column split is the point"
- **Caption (12px `#999`, centered, y = h−14):** "Caching for analysis and republishing the data are very different rights"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; `li strong` colored `#1a5276`; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Bullet style:** each bullet is a bold `#1a5276` label plus a short phrase that fits on one line at normal page width — no wrapping; split dense content into more bullets rather than longer ones.
- **Canvases:** intrinsic width 720, heights 400/380/340/380 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (multiply backing store, `ctx.scale(dpr,dpr)`). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(39,174,96,0.10)`, `rgba(231,76,60,0.10)`, `rgba(230,126,34,0.12)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
