# Publicly Available Real-Estate Data — The Record Follows the Parcel

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one section per h2)
**HTML title tag:** Publicly Available Real-Estate Data — The Record Follows the Parcel

**Subtitle:** Every property carries a public paper trail — ownership deeds, tax assessments, loan filings, parcel details, permit history, listing history. It was always public; apps like Redfin and Zillow collapsed the effort needed to read it.

**Intro callout:** Real estate is the most heavily documented asset an ordinary person owns, and most of that documentation is public by design: deeds and mortgages are recorded to establish legal priority, assessments to justify taxes, permits to enforce building codes. Each record type has its own custodian — county recorder, county assessor, city permit office, regional MLS — each maintaining it in its own way. Aggregator apps join all of them under a single address search, turning a scattered archive into a browsable profile.

## 1. The public record stack

One property accumulates records across at least five independent systems:

- **County recorder** — deeds: every ownership transfer, buyer/seller names, sale price (via transfer tax), dates.
- **County assessor** — parcel details: APN, lot size, zoning, square footage, assessed value, tax payment history.
- **Loan filings** — mortgages and deeds of trust, refinances, liens, notices of default. Recorded because recording is what makes a lender's claim enforceable — public by design, not by accident.
- **City permit office** — remodel and construction history: scope of work, contractor, declared valuation, inspection results.
- **MLS** — listing history: asking prices, days on market, photos of the interior. Semi-public via syndication to consumer apps.

**Key point:** None of these systems was built for browsing. Each exists to serve a legal function — priority of claims, taxation, code enforcement — and openness is the mechanism that makes the function work.

### Visualization (canvas `c1`, 720×400)

Diagram: one parcel box on the left fanning out to five record-system boxes.

- **Title (bold 14px, `#1a5276`, top center):** "One parcel, five independent public record systems".
- **Parcel box:** at (30,160), 130×90, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` stroke; bold 13px "THE PARCEL", 11px `#666` sublines "one address" / "one APN".
- **Five record rows** (480×46 white boxes at x=215, starting y=50, 62px row pitch, 2px stroke in row color; bold 12px name left-aligned, 11px `#555` "keeps" line below, 11px `#999` right-aligned "reveals" text; gray `#bbb` connector line from parcel box to each row):
  1. County recorder, `#1a5276` — keeps: "deeds, transfers, sale prices"; reveals: "who owns it, what they paid".
  2. County assessor, `#27ae60` — keeps: "APN, lot, zoning, assessed value"; reveals: "parcel details, tax history".
  3. Loan filings, `#e74c3c` — keeps: "mortgages, refis, liens, defaults"; reveals: "borrowing history on the property".
  4. City permit office, `#e67e22` — keeps: "permits, contractors, inspections"; reveals: "what was built or remodeled".
  5. Regional MLS, `#8e44ad` — keeps: "listings, prices, interior photos"; reveals: "market history, the inside of the home".
- **Bottom caption (12px `#999`, centered):** "Each system exists for a legal function — openness is what makes the function work".

## 2. Availability vs accessibility

The legal status of these records has barely changed in decades. What changed is the **effort curve**. Answering "what did the neighbors pay, and how much did they borrow?" used to mean a trip to the county office, an index lookup by name or APN, and per-page copy fees. The records were public but practically obscure.

Aggregators removed every step: they bulk-license or scrape the county feeds, normalize them, join them to MLS listing data, and index everything by street address. One search now returns ownership, sale history, tax history, permit history, and a price estimate — for any address, not just your own.

**Key point:** "Public" was doing two jobs: legally inspectable, and practically obscure. Aggregation kept the first and deleted the second — a privacy change with no change in law and no new data collected.

### Visualization (canvas `c2`, 720×340)

Before/after comparison: five red step boxes vs one green box.

- **Title (bold 14px, `#1a5276`, top center):** "\"What did the neighbors pay, and how much did they borrow?\"".
- **Before column (left):** bold 13px red `#e74c3c` heading "Before: public but obscure" at (50,58). Five numbered step boxes (300×28 at x=50, 40px pitch starting y=84, fill `rgba(231,76,60,0.08)`, 1px `#e74c3c` stroke, 11px `#2c3e50` text):
  1. Drive to the county recorder's office
  2. Look up the grantor/grantee index
  3. Pull the deed, compute price from transfer tax
  4. Pull the deed of trust for the loan amount
  5. Pay per-page copy fees
- **Gray arrow** `#888` from (370,170) to (420,170) with filled arrowhead.
- **After column (right):** bold 13px green `#27ae60` heading "Now: public and indexed" at (440,58). One box 240×120 at (440,76), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` stroke; 11px `#2c3e50` text "1. Type the address into any" / "    listing app"; then 11px `#666`: "Sale history, tax history, loan" / "records, permits, photos — joined" / "and rendered in one profile".
- **Bottom caption (12px `#999`, centered):** "No law changed and no new data was collected — only the effort curve moved".

## 3. Fragmentation — same record type, thousands of custodians

There is no national schema. Each county recorder and assessor runs its own system; MLSs are regional with their own rules; and **permits are the extreme case** — maintained city by city, with detail levels ranging from scanned PDFs to structured records listing contractor, valuation, and every inspection visit.

Aggregators bridge this by entity resolution: joining sources on **address strings and parcel numbers** — both fuzzy keys. Address formats differ across systems, parcels split and merge, and a bad join silently attaches one property's history to another.

- Wrong square footage or bed/bath counts inherited from a stale assessor feed.
- Permit history missing entirely because that city's portal isn't ingested.
- Ownership shown pre-transfer because recorder updates lag by weeks.

**Key point:** The consumer app presents one clean profile per address, hiding that it is a probabilistic join across systems with different schemas, update cadences, and error rates. The polish of the surface overstates the reliability of the source.

### Visualization (canvas `c3`, 720×380)

Three side-by-side city cards showing the same permit recorded three ways.

- **Title (bold 14px, `#1a5276`, top center):** "The same kitchen remodel, recorded by three cities".
- **City cards** (190×250 white boxes at y=50, 2px stroke in card color, bold 12px heading, thin `#eee` divider, then 11px `#555` bullet lines at 24px pitch):
  - City A: structured portal, green `#27ae60`, x=35 — bullets: "permit #, issue + final dates" / "contractor name + license #" / "declared valuation: $48,000" / "every inspection visit logged" / "searchable API".
  - City B: minimal record, orange `#e67e22`, x=265 — bullets: "permit # and issue date" / "type: \"residential alteration\"" / "no valuation shown" / "no inspection detail" / "web form, address search only".
  - City C: scanned paper, red `#e74c3c`, x=495 — bullets: "PDF scan of paper form" / "handwritten scope of work" / "not text-searchable" / "in-person request for older files" / "not ingested by aggregators".
- **Bottom captions (12px `#999`, centered, two lines):** "Permits are the extreme case of fragmentation: each city maintains its own system, format, and depth" and "Aggregator coverage silently varies — a missing permit history means \"not ingested\", not \"no work done\"".

## 4. Why it matters as a data problem

The defining property of this data: **records are scoped to the parcel, not the person.** Buy a house and you inherit its full public history; sell it and your financial trail stays attached to the address.

- **Joins create derived disclosures.** Purchase price minus loan amount approximates the down payment; a refinance date signals a financial event; a lien or notice of default is a public statement of distress. No single record says these things — the join does.
- **Photo history persists across owners.** MLS photos of a previous owner's furnished interior remain browsable on apps that show full listing history — consent was scoped to one transaction, retention to the address.
- **There is no deletion path.** Public records cannot be recalled, and removal from one aggregator does nothing to the county source or the other syndicated surfaces.

**Key point:** Open questions for a full doc: whether derived fields (implied down payment, implied equity) deserve different treatment than the source records; how aggregators should handle join uncertainty in what they display; and where else the parcel-not-person pattern appears — vehicles (VIN history), businesses (registered-agent filings), domains (WHOIS).

### Visualization (canvas `c4`, 720×380)

Join diagram: five source-record boxes on the left, three derived-disclosure boxes on the right, connectors between them.

- **Title (bold 14px, `#1a5276`, top center):** "Derived disclosures: the join says what no single record does".
- **Source boxes (left)** — 270×34 at x=35, fill `rgba(26,82,118,0.08)`, 1.5px `#1a5276` stroke, 11px Menlo monospace `#2c3e50` labels, at y = 60/110/160/210/260:
  - "Deed: sold for $900k (2021)"
  - "Deed of trust: $630k loan (2021)"
  - "Refi filing: new lender (2023)"
  - "MLS: prior listing photos (2016)"
  - "Permit: ADU built, $120k (2022)"
- **Derived boxes (right)** — 285×50 at x=400, fill `rgba(231,76,60,0.07)`, 2px `#e74c3c` stroke, bold 12px red label + 11px `#666` subline, at y = 75/145/215:
  - "Down payment ≈ $270k (30%)" — subline "deed price − loan amount".
  - "Refinanced within 2 years" — subline "a financial event, dated".
  - "Prior owner's interior still browsable" — subline "photo consent scoped to 2016 sale".
- **Connectors:** gray `#bbb` 1.5px lines from source right edge (x=305) to derived left edge (x=400) pairing source y-offsets [77,127,177,227]+17 to derived y [100,100,170,240].
- **Bottom caption (12px `#999`, centered):** "Records are scoped to the parcel, not the person — sell the house and the trail stays attached".

## Regeneration instructions

- **Template/layout:** backlog detail page, kusto-style 2-column layout. Each section is a `.lang-section` with an `h2` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout`: left `td.text-col` 45% (paragraphs, bullets, key-point callouts), right `td.viz-col` 55% (one canvas).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro`: background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point`: background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `ul` 0.92rem.
- **Canvas:** intrinsic widths 720, heights per chart (c1 400, c2 340, c3 380, c4 380); CSS `width: 100%`, `1px solid #e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setupCanvas(id, height)` helper (`ctx.scale` back to logical coordinates).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, grays `#555`/`#666`/`#999`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
