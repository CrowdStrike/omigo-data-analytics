# Government Statistical & Open-Data Portals — Published as Policy

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Government Statistical & Open-Data Portals — Published as Policy

**Subtitle:** Governments deliberately publish aggregate data — for markets, science, and accountability. Unlike legal-mandate records that are public as a side effect, this data is collected, aggregated, and curated specifically to be released.

**Intro callout (blue-left-border box):** This is the second major family of public data: data that exists *because* a government decided the public should have it. Census bureaus, statistical offices, central banks, weather services, and health agencies run permanent pipelines whose end product is a public release. The defining contract differs from legal-mandate records: individual responses are confidential by law, and what gets published is the aggregate — suppressed, curated, and often released on an engineered calendar.

## 1. National statistics — the aggregation contract

Every country runs a statistics office whose core bargain is the same:

- **US Census Bureau** — decennial census, American Community Survey, plus public-use microdata samples (PUMS): anonymized individual records, deliberately released for researchers.
- **Eurostat** — harmonizes member-state statistics into one comparable schema; the rare case of a cross-country standard actually enforced.
- **India NSO, and peers everywhere** — household consumption, employment, enterprise surveys; nearly every nation maintains an equivalent office under a UN statistical framework.
- **The contract:** individual responses are confidential by statute (in the US, Title 13 makes disclosure a crime); only aggregates and anonymized samples leave the building.
- **Disclosure control is active curation** — small cells suppressed, values rounded or perturbed, geographies coarsened so no aggregate can be reversed into a person.

Key point: The aggregation is not a limitation of the data — it is the reason the data exists. People answer honestly because the law guarantees their row will never be published; the public table is what buys the truthful microdata underneath it.

### Visualization (canvas `c1`, 720×380)

Pipeline flow diagram: confidential forms → disclosure control → two public outputs.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The aggregation contract: confidential in, public out"
- **Left — stack of 5 offset "forms":** white 130×150 rects with 1.5px `#e74c3c` borders, offset 6px per layer, top form at (40, 70). On the front form: bold 12px `#e74c3c` "Individual forms"; 11px `#555` "name, income," / "household, job"; four ruled lines in `#f0c0ba`; bold 10px `#e74c3c` centered at bottom: "CONFIDENTIAL BY STATUTE".
- **Arrows:** gray `#888` 2px lines with filled arrowheads: forms → middle box (from (206,155) to (258,155)); middle box → each output ((442,125)→(500,100) and (442,185)→(500,218)).
- **Middle — disclosure control box:** 180×140 at (260, 85), fill `rgba(26,82,118,0.10)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered "Disclosure control"; 11px `#555` left-aligned bullet lines: "• suppress small cells" / "• round / perturb values" / "• coarsen geography" / "• strip identifiers".
- **Output 1 — public tables:** 185×80 at (502, 62), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 12px `#27ae60` "Public aggregate tables"; 11px `#555`: "median income by tract," / "counts by age, industry".
- **Output 2 — microdata:** 185×80 at (502, 182), fill `rgba(142,68,173,0.08)`, 2px `#8e44ad` border. Bold 12px `#8e44ad` "Anonymized microdata"; 11px `#555`: "PUMS: person-level rows," / "identity removed by design".
- **Caption (12px `#999`, centered, three lines at h−46/h−28/h−10):** "Census bureaus, Eurostat, India NSO — the same pipeline everywhere:" / "the confidentiality of the row is what buys honest answers;" / "the curated aggregate is the only thing that leaves the building"

## 2. Economic series — releases that move markets

Economic indicators are the most consequential public datasets on earth, and their release process is engineered like a launch:

- **FRED** — the St. Louis Fed's aggregator, hundreds of thousands of time series from dozens of sources, all with a free API; the de facto front end for US economic data.
- **BLS** — CPI, jobs report, unemployment; single numbers that reprice trillions in assets within seconds of release.
- **ECB, World Bank, IMF, OECD, UN** — the international layer: exchange rates, GDP, development indicators, cross-country panels.
- **Release calendars are published months ahead** — everyone knows the exact second the number appears; fairness means simultaneity, not secrecy.
- **Embargo lockups** — journalists see the number early in a sealed room with no connectivity, so coverage is instant but no one trades ahead.

Key point: Because markets move on these numbers, the engineering problem is not access but timing: a leak of even milliseconds is a trading advantage, so the entire pipeline — pre-announced calendar, lockup, simultaneous drop — exists to make one instant identical for everyone.

### Visualization (canvas `c2`, 720×340)

Five-stage pipeline boxes on a timeline, plus a price-reaction sketch below with a release-instant marker and embargo bracket.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One CPI number: from survey to the market, on an engineered clock"
- **Timeline base:** `#ccc` 2px line at y=130 from x=30 to x=690.
- **Stage boxes (each 92×76 at y=60, white fill, 2px border in stage color; bold 12px name centered, two 10px `#555` sublines; small gray `#888` arrowheads between boxes):**
  - x=30 "Survey" `#1a5276` — "price collectors," / "thousands of items"
  - x=172 "Compile" `#8e44ad` — "weight, seasonally" / "adjust, verify"
  - x=314 "Lockup" `#e67e22` — "press sealed in," / "no connectivity"
  - x=456 "Release" `#e74c3c` — "8:30:00 AM sharp," / "pre-announced"
  - x=598 "Reaction" `#27ae60` — "markets reprice" / "in seconds"
- **Price sketch:** green `#27ae60` 2px polyline through points (340,230), (500,228), (506,196), (520,204), (540,192), (575,196), (640,190) — flat before release, jumps at the release instant. Left label 11px `#666`: "asset price".
- **Release marker:** dashed (4/3) 1.5px `#e74c3c` vertical line at x=502 from y=170 to y=250, labeled above in bold 11px `#e74c3c` centered: "release instant".
- **Embargo bracket:** 1.5px `#e67e22` bracket from x=314 to x=498 (top at y=158, arms down to y=168), labeled below in 11px `#e67e22` centered: "embargo window: known to a few, tradable by none".
- **Caption (12px `#999`, centered, y = h−12):** "FRED, BLS, ECB, World Bank, IMF, OECD — fairness engineered as simultaneity: one instant, identical for everyone"

## 3. Environment & hazards — the oldest open-data culture

Weather and hazard data has been shared internationally since the telegraph era, because a forecast is only as good as the upstream observations:

- **NOAA** — surface stations, radar, satellites, ocean buoys; raw observations and model output published continuously.
- **ECMWF** — long the gold-standard forecast model, its output moved to open data; global forecasts anyone can download.
- **USGS earthquakes** — detected, located, and published in near-real-time feeds; the alert is the product.
- **Satellite climate records** — Landsat and Sentinel imagery archives opened fully; decades of consistent earth observation, free.
- **Air-quality networks** — government monitor grids publishing hourly readings, increasingly merged with low-cost citizen sensors.

Key point: Weather is the original open data: countries have exchanged observations for over a century — through wars and rivalries — because the atmosphere ignores borders. No national model works without foreign stations, so sharing is not generosity; it is the only architecture that functions.

### Visualization (canvas `c3`, 720×380)

Flow diagram: three countries' station grids → shared international exchange layer → two open global models.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Weather: every country's stations feed every country's model"
- **Region boxes (dashed 1.5px borders, 170×78 at y=44, each containing 8 station dots radius 3.5 at fixed offsets [(22,34),(58,22),(96,40),(40,58),(130,28),(110,58),(70,44),(148,48)]; bold 11px name centered below the box):**
  - "Country A stations" — x=40, `#1a5276`
  - "Country B stations" — x=275, `#27ae60`
  - "Country C stations" — x=510, `#e67e22`
- **Connectors:** 1.5px `#bbb` lines from each region box down and into the exchange bar.
- **Exchange layer:** 420×44 at (150, 192), fill `rgba(26,82,118,0.10)`, 2px `#1a5276` border. Bold 12px `#1a5276` centered "International observation exchange"; 11px `#666`: "shared for over a century — WMO-coordinated, continuous".
- **Arrows:** gray `#888` 2px arrows from the exchange layer down to the two model boxes ((280,236)→(220,264) and (440,236)→(500,264)).
- **Model boxes (each 220×60 at y=266):**
  - (110, 266): fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border; bold 12px `#27ae60` "NOAA global model"; 11px `#555` "output published openly".
  - (390, 266): fill `rgba(142,68,173,0.08)`, 2px `#8e44ad` border; bold 12px `#8e44ad` "ECMWF global model"; 11px `#555` "now open data too".
- **Caption (12px `#999`, centered, y = h−12):** "Same pattern: USGS quake feeds, satellite archives, air-quality grids — hazards ignore borders, so the data must cross them"

## 4. Health, crime & the portal era

The newest layer: agencies publishing operational data for accountability, and portals cataloging all of it:

- **WHO / CDC surveillance** — disease incidence, mortality, outbreak tracking; the datasets behind every epidemic dashboard.
- **ClinicalTrials.gov** — registration required before trials run, precisely so negative results cannot quietly disappear.
- **FBI crime statistics & city incident maps** — national UCR/NIBRS aggregates down to per-incident city feeds with block-level locations.
- **The portal wave** — data.gov, data.gov.uk, data.gov.in, the EU Open Data Portal: central catalogs listing thousands of machine-readable datasets across every agency.
- **The catch** — a portal counts a "dataset" whether it is a live API or a scanned PDF from 2011; coverage and freshness vary wildly by agency.

Key point: Portal counts overstate usability. The headline number rewards listing, not maintaining — many entries are stale exports, broken links, or PDFs. For any analysis, the real inventory is the subset that is machine-readable, documented, and still being updated, and that subset must be measured, not assumed.

### Visualization (canvas `c4`, 720×360)

Grouped bar chart: datasets listed vs actually usable, per agency (illustrative, not real figures).

- **Title (bold 14px `#1a5276`, centered, y=22):** "Portal inventories: what is listed vs what is actually usable"
- **Data (fractions of max bar height 190px):**
  - Agency A — listed 1.0, usable 0.72
  - Agency B — listed 0.85, usable 0.40
  - Agency C — listed 0.70, usable 0.18
  - Agency D — listed 0.55, usable 0.08
- **Layout:** baseline `#ccc` axis at y=270 from x=45 to x=680; groups 150px apart starting at x=60. Per group: "listed" bar 52px wide, fill `rgba(26,82,118,0.35)` with 1.5px `#1a5276` stroke; "usable" bar 52px wide at +62px offset, solid `#27ae60`. Agency name 11px `#555` centered below each group.
- **Gap annotation (on Agency D only):** dashed (4/3) 1.5px `#e74c3c` line across the group at the listed-bar top; above it, bold 11px `#e74c3c` "the gap:" then 10px lines "stale PDFs, dead" / "links, one-off dumps".
- **Legend (at y=300):** swatch `rgba(26,82,118,0.35)` with `#1a5276` stroke + 11px `#555` "datasets listed in catalog"; swatch `#27ae60` + "machine-readable and still updated".
- **Caption (12px `#999`, centered, y = h−12):** "Illustrative pattern on data.gov-style portals: the catalog rewards listing, not maintaining — freshness varies wildly by agency"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 380/340/380/360 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.10)`, `rgba(26,82,118,0.35)`, `rgba(39,174,96,0.10)`, `rgba(142,68,173,0.08)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
