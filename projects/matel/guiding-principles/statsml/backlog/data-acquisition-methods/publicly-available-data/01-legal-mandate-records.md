# Public-by-Legal-Mandate Records — Open Because the Law Needs Witnesses

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Public-by-Legal-Mandate Records — Open Because the Law Needs Witnesses

**Subtitle:** Records about people, property, and companies that are public not by accident but because openness is the legal mechanism itself: recording establishes priority, disclosure enables accountability, registries enable verification.

**Intro callout (blue-left-border box):** These records were never leaked and never collected by surveillance — the law requires them to be inspectable, because a claim nobody can check enforces nothing. A deed proves ownership only if rival buyers can find it; a court is legitimate only if its judgments can be read; limited liability is granted only in exchange for a public filing. Each record family below carries that logic, and each has quietly become bulk-downloadable, name-searchable input for anyone who wants to profile a person or an entity.

## 1. Property & land — recording establishes priority

Land is the oldest record family of this kind: recording a transfer is what makes ownership enforceable against the world, so nearly every jurisdiction runs an inspectable register.

- **US counties** — deeds, assessments, liens, and permits, fragmented across thousands of county and city systems.
- **UK Land Registry** — price paid data for sales since 1995, downloadable in bulk; a separate register lists property held by overseas companies.
- **France** — cadastre parcel maps plus DVF, a geocoded open dataset of individual sale prices.
- **India** — state sub-registrar offices hold sale deeds and encumbrance certificates; digitization varies widely by state.
- **Australia** — state Torrens registers where the register itself guarantees title, searchable per fee.

Key point: Recording is not a side effect of the transaction — it is the transaction's legal force. A deed nobody can inspect cannot establish priority against a rival claim, so openness is the mechanism, not a leak.

### Visualization (canvas `c1`, 720×340)

Five side-by-side jurisdiction cards comparing land registries.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The same record family, one registry per jurisdiction"
- **Cards:** five boxes, each 126×240, starting x = 25 + i×138, y=46; white fill, 2px border in the card color, with a 26px-tall solid header band in the card color holding the bold 12px white centered jurisdiction name. Body lines in 10px `#555`, left-aligned, 20px line spacing starting at y+48:
  - "US (county)" — `#1a5276` — lines: "recorder: deeds" / "assessor: value, tax" / "liens, defaults" / "city permits" / "thousands of" / "local systems"
  - "UK" — `#27ae60` — lines: "one national" / "registry" / "title + owner" / "price paid data" / "bulk CSV download" / "overseas-owner list"
  - "France" — `#e67e22` — lines: "cadastre parcel" / "maps" / "DVF: sale prices" / "geocoded open data" / "free bulk download"
  - "India" — `#e74c3c` — lines: "state sub-" / "registrars" / "sale deeds," / "stamp duty" / "encumbrance certs" / "portals vary" / "by state"
  - "Australia" — `#8e44ad` — lines: "state Torrens" / "registers" / "register guarantees" / "title" / "search per fee" / "owner + mortgages"
- **Caption (12px `#999`, centered, y = h−14):** "Everywhere the logic is identical: an unrecorded claim is an unenforceable claim"

## 2. Courts & justice — the record outlives the case

Justice must be seen to be done, so the paper trail of a case is public almost everywhere — and it stays public after the case ends.

- **Dockets & filings** — PACER exposes US federal case records for a per-page fee: complaints, motions, exhibits, party names.
- **Judgments** — published and indexed in the UK, EU, and India (eCourts and free judgment archives); opinions name the parties and recite the facts in detail.
- **Bankruptcies** — a detailed financial confession made public by design, so creditors can verify claims against each other.
- **Registries of status** — sex-offender registries, inmate and warrant lookups: the state deliberately broadcasting a person's legal condition.

Key point: The record persists long after the legal event, and name-based search collapses the effort to find it. In a search result, a dismissed case and a lost one look the same — the artifact carries no verdict-weighted decay.

### Visualization (canvas `c2`, 720×340)

Case timeline with artifact boxes hanging below each event, plus a red persistence band.

- **Title (bold 14px `#1a5276`, centered, y=22):** "One lawsuit, a trail of permanent public artifacts"
- **Timeline:** horizontal gray `#888` line (width 2) at y=88 from x=50 to x=680, with a filled gray right-arrowhead at the end.
- **Events (dot radius 5 in event color on the line, bold 11px name label 16px above, thin `#bbb` connector down to an artifact box 124×44 at y=128 with fill `rgba(26,82,118,0.08)` and 1.5px border in the event color; artifact lines 10px `#555` centered):**
  - x=105 "Complaint filed" `#1a5276` — "docket entry," / "party names"
  - x=255 "Hearings" `#e67e22` — "transcripts," / "exhibits"
  - x=405 "Judgment" `#e74c3c` — "published opinion," / "indexed & citable"
  - x=555 "Appeal" `#8e44ad` — "appellate opinion," / "restated facts"
- **Persistence band:** rectangle 630×66 at (50, 208), fill `rgba(231,76,60,0.09)`, 2px `#e74c3c` border. Bold 13px `#e74c3c` centered: "20 years later: a name search returns all of it in seconds". Below it, 11px `#666`: "The case ended; the artifacts did not — and a dismissal reads like a loss in a results page".
- **Caption (12px `#999`, centered, y = h−14):** "Bankruptcies, offender registries, and inmate lookups follow the same persistence pattern"

## 3. Companies & money — disclosure is the price of limited liability

Limited liability is a privilege granted in exchange for disclosure: the public registry is the price of the corporate shield.

- **SEC EDGAR** — every filing by US public companies, free full text: financials, risk factors, executive pay, insider trades.
- **UK Companies House** — free complete filings for every company: accounts, director names and birth month/year, and home-address history in older documents.
- **Germany & India** — Handelsregister and MCA filings expose directors, shareholdings, and charges on company assets.
- **OpenCorporates** — aggregates registries from ~140 jurisdictions into one graph searchable by company or officer name.
- **Political money** — campaign contributions and lobbying spend disclosed with donor names, employers, and amounts.

Key point: Director and donor names are join keys. Each registry discloses one company; the join across registries discloses a person's entire corporate footprint — a derived disclosure no single legislature ever voted on.

### Visualization (canvas `c3`, 720×380)

Three registry cards joined by connectors into one resolution box (entity-resolution join diagram).

- **Title (bold 14px `#1a5276`, centered, y=22):** "One director name joins three national registries"
- **Registry cards (each 205×130 at y=46, white fill, 2px border in card color; bold 12px name in card color; thin `#eee` divider line; company name in 11px Menlo monospace `#2c3e50`; the line "Director: R. K. ALDER" in bold 11px Menlo `#e74c3c`; note in 10px `#999`):**
  - x=30 "UK Companies House" `#1a5276` — company "Alder Trading Ltd" — note "free full filings, address history"
  - x=258 "Handelsregister (DE)" `#e67e22` — company "Alder Handel GmbH" — note "directors, shareholdings"
  - x=486 "MCA (India)" `#27ae60` — company "Alder Ventures Pvt Ltd" — note "directors via DIN, charges"
- **Connectors:** 1.5px `#bbb` lines from the bottom-center of each card down to (360, 240).
- **Resolution box:** 400×72 at (160, 240), fill `rgba(142,68,173,0.08)`, 2px `#8e44ad` border. Bold 12px `#8e44ad` centered: "OpenCorporates — registries from ~140 jurisdictions". Below in 11px `#555`: "same officer name across borders ⇒ one cross-border corporate network" / "assembled entirely from free, mandated public filings".
- **Caption (12px `#999`, centered, y = h−14):** "Campaign finance and lobbying disclosures join the same graph: names, entities, money"

## 4. People directly — the openness dial is a policy choice

Some mandated records are not about assets or cases — they are about the person directly, and how open they are varies enormously by country.

- **Voter rolls** — purchasable in many US states: name, address, party, and turnout history (not the vote itself); India publishes electoral rolls as downloadable documents.
- **Professional licenses** — doctor, lawyer, and contractor lookups with disciplinary history; public verification is the entire point of licensure.
- **Nordic tax transparency** — Norway, Sweden, and Finland make individual income and tax figures accessible. Norway logs every search and shows you who looked; Finland's annual release each November is nicknamed "national envy day", with the press publishing top earners.

Key point: Same record type, radically different openness settings per country. The dial position is a policy judgment about what accountability requires — not a technical fact about the data itself.

### Visualization (canvas `c4`, 720×360)

Horizontal "openness dial" spectrum axis from sealed to broadcast with five country stops in callout boxes alternating above/below the axis.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The openness dial for one record: an individual's income"
- **Axis:** gray `#888` line (width 2) at y=185 from x=55 to x=680 with a filled right-arrowhead; endpoint labels in bold 11px `#666`: "sealed" (left, below axis) and "broadcast" (right, below axis).
- **Stops (dot radius 5 in stop color on the axis, `#bbb` connector to a 140px-wide white box with 2px border in the stop color, box height 20 + 14×lines; stop name bold 11px in stop color, lines 10px `#555`; boxes alternate above/below):**
  - x=115 "Most countries" `#e74c3c` (above) — "tax returns are secret;" / "disclosing them is" / "itself an offence"
  - x=250 "US" `#1a5276` (below) — "returns sealed; only" / "public-employee" / "salaries are open"
  - x=385 "Sweden" `#e67e22` (above) — "figures released on" / "request; the subject" / "can learn who asked"
  - x=515 "Norway" `#8e44ad` (below) — "income, tax, wealth" / "searchable online —" / "every viewer is logged" / "and shown to you"
  - x=640 "Finland" `#27ae60` (above) — "data released each" / "Nov; press publishes" / "top earners on" / "\"national envy day\""
- **Caption (12px `#999`, centered, y = h−12):** "Same record type, different dial setting — the dial is a policy choice, not a technical fact"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 340/340/380/360 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`; translucent fills `rgba(26,82,118,0.08)`, `rgba(231,76,60,0.09)`, `rgba(142,68,173,0.08)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
