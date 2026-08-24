# Tracking Pages — Review Findings (temporary notes)

Review of all pages under `backlog/tracking/` for professional, non-controversial
language and examples. Six parallel reviewers, all findings spot-checked verbatim
against the files. **Nothing in this file has been fixed yet** except the section-08
removal noted at the bottom.

Reassess slowly. Ordered by severity, not by file.

---

## The core pattern

The **prose** across this set was cleaned to the tone rules in `CLAUDE.md`. The
**`<script>` blocks were not.** Roughly 20 pages assert in their canvases exactly
what their prose disclaims. `CLAUDE.md` already warns that the chart "is the part a
skimmer reads first," which makes this the highest-value single pass.

Verified instances of prose/canvas contradiction:

| Page | Prose says | Canvas asserts |
|---|---|---|
| `50:89` | front-running "is prohibited… the mechanism here is internalisation" | `:287` `Front-run trades` under `Citadel / HFTs` |
| `36:34` | "password fields are masked by default" | `:371` `typed password` in red |
| `11:71` | `"device_type": "unknown", // proxy UA hides it` | `:220` `Device: iPhone 15` |
| `02:73` | records `"filled": true`, not field contents | `:218` `Type in box` / `Delete text` |
| `12:97` | cluster membership is uncertain | `:154` `YOUR PROFILE` |
| `51` | stated purpose / additional consequence | `:286-295` `safety vs surveillance`, red/green panels |

---

## Tier 1 — BLOCKING: allegation of illegal conduct against named real companies

### `50-stock-trading-order-flow.html`

Prose at `:89` states front-running is prohibited and explicitly *not* the mechanism
described. The canvases then assert it anyway, against two real firms, with invented
timings and no source. `CLAUDE.md` already lists this page's Robinhood/Citadel figures
as removed-unverified — the prose was cleaned, the canvases were missed.

- `:127-129` `CITADEL` + `(sees your order first)` in a `#e74c3c` box
- `:207-209` `Order sent to Citadel`, `HFT sees order (+0.001s)`, `HFT buys stock (+0.002s)`
- `:284,287` `Citadel / HFTs` + `Front-run trades`
- `:323` `Order data sold`
- `:382-383,434` `Robinhood`, `Citadel`, `trades ahead`
- `:155` `Your order sold here` — "sold" attributes intent; the order is routed
- `:367` `Direct, fair, instant` — sets the real route up as unfair by contrast
- `:45-49` second-person, `panic-sell`, `your risk tolerance`

Fix: `broker` / `wholesaler` throughout the canvases, drop every front-running label,
keep the concept only in the prose where it is correctly hedged. Also `:72` derives
"77 ms" from an illustrative payload and states it as measured.

---

## Tier 2 — Fabricated numbers under authoritative captions

- **`06:283`** — `Actual usage breakdown` with `15%` / `85%` (`:298`, `:305`).
  Invented, unmeasurable from outside, published under the word "Actual."
- **`12:228`** — `Data collected from each device type`, sixteen 0–100 values comparing
  `Location 95` against `Files 40`. No unit, no source, and **incommensurable
  quantities** — the exact error `CLAUDE.md` documents having already fixed once on
  page 06.
- `06:189` — `(unique among 300,000+ browsers)`; reintroduces the invented-uniqueness
  class of figure `CLAUDE.md` records removing from this page.
- `06:199-209` — six invented entropy contributions, no "illustrative" label.
- `04:356` — title asserts a "sample of 100 sites" and the grid yields ~75%; the
  in-code comment at `:370` admits it is not measured.
- `43:57` — `"gsd_m": 0.31` sits under `// ── documented catalogue fields ──`. This is
  the 30cm resolution figure `CLAUDE.md` records as already removed as unsourced, now
  re-asserted under a provenance header claiming it is documented. File has an
  `// ── inferred / derived downstream ──` group at `:65` it could move to.
- Missing "illustrative/schematic" captions: `02:201`, `02:358`, `03:122`, `03:187`,
  `11:335`, `11:416`.

---

## Tier 3 — Concealment framing surviving only in charts

`CLAUDE.md` explicitly bans the "What they tell you / What they don't tell you"
structure and its red-green panel coding. Removed from prose; still rendered:

- **`02:259-317`** — `The Friendly Story` vs `The Reality`, green smiley vs red panel,
  including an **invented quotation** (`"We watch what you click"` / `"to make the
  site better"` / `"for everyone"`), then `Sold behavioral profiles`,
  `Manipulation triggers`, `Dark pattern optimization`. Plus a drawn eye at `:311-317`.
- **`03:254`** — `water line — what you can see vs what you can't`, and `:287-296`
  `Train addiction algorithms`, `Re-identify "anonymous" users`,
  `Predict & manipulate decisions`, `Dynamic pricing per person`.
- **`04:320-346`** — `Complete surveillance map`, `Every site you visit — logged`,
  `Real-time bidding on YOUR attention`, divider labelled `vs` (rule requires `AND`).
- **`05:118,168-170,289`** — `one helpful, one not`; `Tracks you across the entire web`;
  `Builds a profile of everything you do`; `Sells that profile to advertisers`;
  `badSteps` red/green fork.
- **`06:314`** — `(what they tell Congress)`, while its paired label at `:322` was
  already fixed to the neutral "(how the match is then used)" — the two halves of one
  chart contradict each other.
- `51:291-296,333` — red/green opposed panels, green for stated purpose, unlabelled divider.
- `56:282-286` — red/green panels for earthquake vs explosion (physical source types,
  not opposed claims).

`01` C3 is the compliant reference implementation: `#1a5276` + `#e67e22`, `AND` divider.

---

## Tier 4 — The watcher motif

- **`36:418-520`** — canvas C4 draws eight disembodied eyes with sight lines to a
  monitor, labelled `Product Manager` / `UX Analyst` / `Marketing` / `Growth Team` /
  `Data Team` / `Engineering`, captioned **`Your "private" session has an audience`**
  in bold `#e74c3c`. `CLAUDE.md` records that "How much can they see?" was rewritten
  precisely because it "presumes a watcher." Teaches nothing about the mechanism —
  the page's actual argument (replay localises a mechanism but does not size it,
  because only the filtered slice is watched) is absent from it. Also `:448`
  `YOUR SESSION`.
- `05:158,165` — `// Spy icon` comment and a `👁` emoji.
- `02:156` — canvas label `form spy`.
- `04:222` — `You can't see it. But it sees you.` (a 1x1 GIF does not observe anyone).
- `14:101` — `// C1: … ISP (watching eye) …` comment; the drawn canvas was fixed, the
  comment preserves the old framing.

### "they / they see / you" wording left in canvas titles

`03:306`, `06:314`, `11:345`, `11:410`, `12:407`, plus stale HTML comments at
`13:75`, `13:87`, `45:77`, `45:89`, `46:76`, `46:88` still carrying the deprecated
headings ("Why do they collect it?" / "How much can they see?").

---

## Tier 5 — Real-company internals leaking through the renames

The naming rule renamed these pages to broad categories so an invented schema would
not read as one firm's internals. The payloads still identify the firm:

- **`58`** `:70,79,88,97,131,176` — `"item_id": "B0C7R9K2LM"` etc. `B0` + 8
  alphanumerics is Amazon's ASIN format. Rename defeated.
- **`59`** `:40,59,69,84,108` — `pin_closeup`, `pin_id`, `is_repin`,
  `original_pinner_id`, `"embedding_version": "unified_v7"` — Pinterest's actual
  vocabulary.
- **`60`** `:32,276` — asserts Shop Pay uses a **shared email hash** as the
  cross-merchant join key. Named company + specific undocumented internal mechanism,
  not on the sourced-claims list. `:32` also "five or six destinations" (unsourced count).
  `:109-213` whole webhook block presented as documented with a realistic HMAC and no
  `inferred` group.
- **`41:34`** — quotes `"people you may know"`, identifying the platform the page
  otherwise carefully avoids naming.
- `10:33` — names TikTok for an undocumented behaviour on the author's own unrecorded
  observation, with a date. Rest of the page uses `platform.com` placeholders.
- `34:32` — `Target Circle`, `CVS ExtraCare` named four rows above a chart asserting
  baskets yield infant/allergy labels. Naming is permitted; the proximity is the problem.

### Live identifiers in illustrative payloads

- `60:194` `"browser_ip": "73.14.208.61"` — routable public IP. Page 62 correctly uses
  the `203.0.113.0/24` documentation range.
- `72:57` `"serving_cell_id": "310-260-…"` — real allocated MCC-MNC (US carrier).
- `33:56,284` — `SQ *` and `TST*` are live payment-facilitator descriptor prefixes.
- `51:45` `UAL417` real airline prefix while the payload sanitises to `ABC417`.
- `05:330-336` — `Amazon` and `YouTube` each given a red "same cookie present" dot.

---

## Tier 6 — Gratuitous sensitive-category examples

**Keep these — legitimate and load-bearing:**
- `65:110`, `66` — demographic error-rate disparity with unlabelled placeholder groups.
- `42:48,85` — reference-panel bias in ancestry estimates. Correctly attributes
  unevenness to panel coverage, not to any property of the groups.
- `46:59` — `"chest pain left side worse lying down"`. The page argues
  P(condition | searched) ≠ P(searched | condition); a health example is the clearest case.
- `36:76` `"penicillin allergy"` — the masked/unmasked contrast only lands if the
  unmasked value is genuinely sensitive.
- `38:79`, `39:60` — third-party disclosure and envelope-cannot-see-content arguments.
- `30:86`, `47:88` — differential error rates, well hedged, no group named.
- `08:82` — "went to the clinic" vs "walked past the clinic" demonstrates the accuracy radius.

**Replace these — decoration, neutral example works identically:**
- **`34:204,206`** — `Diapers Size 3 → infant in household?` and
  `Allergy Meds → allergy sufferer?`. `CLAUDE.md` records the retailer
  pregnancy-prediction anecdote being removed "from prose **and from the chart that
  rendered it**" — this reinstates the same inference in that chart. `:205` `Wine $40 →
  income band?` milder but avoidable.
- **`04:334-336`** — `Medical searches sold to insurers`, `Political views mapped from
  reading`, `Relationship status from behavior`. Protected attributes asserted as
  established outputs, unsourced; `CLAUDE.md` removed the equivalent claim from page 46.
- **`04:250-251`** — `Health Info`, `Dating App` nodes where News/Shopping/Travel already
  carry the point.
- **`05:330-336`** — `Health forum`, `Dating app`, `Pharmacy` nodes.
- **`49:41,60,176`** — `voter registration` / `voter_reg`. Political-affiliation-adjacent;
  the linkage argument works identically with a licence registry.
- `59:158` — `medical or pregnancy context`; the free-text-PII point survives "health context".
- `70:464` / `45:125` — `clinic` label in trajectory charts; unused by the prose.
- `14:50,62,179,236` — medical example repeated four times; one anchor is enough.
- `46:153-154` — `is it normal to feel alone`, `divorce lawyer free consultation` added
  for range rather than argument.
- `64:94,119,390` — `// a named employee`, and insurance/medical judgement about
  athletes asserted as fact.

---

## Tier 7 — Unsourced specific claims (prose)

- `sections/03:148` — "GM's sale of driving data to insurance-scoring brokers was
  documented in 2024." Named company + year + conduct, no source.
- `sections/05:82` — "Funds count cars to estimate revenue ahead of earnings reports."
  Same class `CLAUDE.md` removed from page 13.
- `sections/04:60` — "Footage is stored by Amazon or Google and can be requested by police."
- `sections/01:170` — "which is how TikTok and others were found doing it" — concealment
  framing, and `CLAUDE.md` records this incident as removed-unsourced.
- `sections/01:159` — TikTok named on personal observation.
- `sections/01:236` — names ChatGPT, Claude, and Gemini, then asserts users "routinely
  disclose health, legal, and financial detail." Unsourced behavioural-frequency claim.
  (Also note: this doc is authored by Claude and names Claude as an example.)
- `sections/02:82` — "licensed to pharma, and searched by police through relatives" —
  `CLAUDE.md` lists the subpoena claim as unsourced.
- `39:33` — training defaults "differ by product and by plan… commonly differ";
  `CLAUDE.md` records that no provider's terms were checked.
- `61:99` — AI detectors "have been shown to misfire disproportionately on writing by
  non-native English speakers." In-scope topic, missing citation.
- `65:112` — "research has demonstrated reconstruction of usable images from templates."
- `62:90` — "a small single-digit figure in most of the world" (household size).
- `34:78` — label-driven differential pricing asserted as fact.
- `54:32` — NEXRAD named + asserted scan cadence.
- `74:26` `rarely`, `75:92` `disproportionately`, `69:34` `commonly`, `73:83` — prevalence
  and behavioural-response claims stated as fact rather than as predicted consequences.
- `33:78` — appeal to "the re-identification literature" with no citation.

---

## Tier 8 — Absolutes and editorialising

- `34:41` — "Every item purchased" — false by the page's own `:88` (other chains, cash).
- `38:42-48` and `34:43-46` — second-person bullet lists ("Who **you** message",
  "the coupon **they** emailed"), the last unconverted survivors of the register rewrite.
  Both pages' payloads directly below are correctly third-person.
- `03:177` — "exactly where each person gives up", refuted by its own `:96`
  ("The unit is an identifier, not a human").
- `04:232` — "Sites you visit **all** report back to the same place", refuted by `:96`.
- `05:168-169` — "the entire web", "everything you do".
- `06:330-335` — `Clear cookies ✗`, `Private mode ✗`, `VPN ✗`, `New account ✗`; the
  prose at `:83,91-92` refutes all four.
- `12:123` — "**All** your devices", refuted by `:97` (work laptop on a corporate VPN).
- `01:33` — "the one collection mechanism that cannot be blocked from the client side" —
  page 14 describes another.
- `11:228` — "You open the email. The pixel phones home." — `:34` says the measured
  event is an image request, not a read.
- `69:34` — "The measurement rewards sloppiness." Pejorative about the people measured;
  the mechanical sentence before it is sharper. Cut rather than soften.
- `68:71` — asserts a senior engineer contributes "most" — an unmeasured quantity, the
  same error the page criticises.
- `03:266`, `12:154` — `YOUR PROFILE` / red central node, contradicting each page's own
  cluster-uncertainty argument.

---

## Red `#e74c3c` on neutral canvas content

`CLAUDE.md`: "Red implies alarm." Applied to ordinary mechanism facts, documented API
scopes, configuration choices, and measuring instruments:

`02:253,349`, `03:346`, `04:289`, `06:328`, `11:349`, `12:301,491`, `13:164,201`
(the sensor, while the unreliable derived path is correctly orange — inverted),
`29:139,155`, `30:273`, `59:373,383,415`, `60:477,506-525`, `61:228-232`,
`62:205,258,339`, `65:395-404`, **`68:287,293`** (red on the *goal* series — semantics
inverted), `63:174-176` (red on a reassuring fact), `63:347` off-palette `#c0392b`.

Distinct from red *card labels* on section index pages, which are severity badges doing
legitimate work — the rule is scoped to callout borders and chart regions.

---

## Findings deliberately rejected

- **`61:37` `Kirchenbauer et al. (2023)`** — keep. Citing authors for published work is
  scholarship, not a privacy exposure; stripping it makes a rigor-focused doc less
  rigorous. Same for `Google DeepMind's SynthID-Text` (named public product).
- **`42:61` real rsids** (`rs4988235`, `rs1815739`) — keep. Public catalogue identifiers
  for lactase persistence and ACTN3, not anyone's data. Scrubbing them costs
  instructional value for no privacy gain. Do fix the provenance header, since the
  genotype values beside them are invented.
- **`sections/02:60` "No biometric system recognises anyone"** — keep. A precise, true
  technical claim, not a rhetorical absolute. The no-absolutes rule exists because
  absolutes are "usually false as well as alarming"; this is neither.
- **Red card labels on section index pages** — leave unless the stricter reading is wanted.

---

## Structural items (verified programmatically)

- Link integrity was clean before the section-08 removal: 77 files, 77 links, no
  orphans, no duplicates, all card indices matched their target filename prefixes.
- **27 dead CSS rules** — `.section-title`, `.section-blurb`, `.philosophy` declared and
  unused in all 9 section pages. Same class as the global `.nav` TODO. On the hub,
  `.philosophy` *is* used; only the other two are dead there.
- **Hub blurbs advertise entries that live on other pages** — `61:64` promises flight and
  ship feeds under card 3; `61:97` promises satellite observation under card 6; `61:86`
  omits page 43, which is actually in section 05. A reader following a card will not find
  what it named. (Partly resolved by the section-08 removal — recheck.)
- `50` (order flow) sits in section 06 "Government and Public Space" with a `FINANCIAL`
  label; it is a private brokerage mechanism. Section 04 already carries page 33 under a
  `FINANCIAL` label. Editorial, not a rule breach.
- `08-os-service-requests.html` — `<title>`/`<h1>` read "Phone Permission Requests",
  disagreeing with the filename.
- Emoji in canvas `fillText`, inconsistent with the rest of the set and unstable across
  platforms: `34`, `37:176-179`, `38:150,329`, `40:279-281`, `55:469,491,516,535`.
- `13` canvases are 420×320, not the 720×240 the folder rules specify; `13` C2 uses
  off-palette yellow.
- `35:192` uses a well-known fictional TV address as sample data — reads as an in-joke.

---

## Removed / changed 2026-08-23

### Pages deleted (11)

Section 08 in full, plus three individually named cards:

`43-satellite-imagery`, `49-data-fusion-platforms`, `51-flight-tracking`,
`52-ship-tracking`, `53-spy-satellite-tracking`, `54-weather-stations-radar`,
`55-wildlife-tracking-collars`, `56-geological-seismic-sensors`,
`57-water-level-sensors`, `70-satellite-positioning`,
`sections/08-things-vehicles-and-environment.html`

This resolves the **entire military / intelligence / dual-use finding class** in one
step — page 53 (NRO named, `CLASSIFIED` canvas label, a chart labelling "Middle East"
captioned `= something is happening here`, an operational how-to, five unsourced orbital
figures), page 56 (SOSUS, CTBTO, nuclear-test monitoring, "politically inaccessible
regions"), page 52 (sanctions evasion, "going dark"), page 51 (FAA LADD,
`safety vs surveillance` red/green panels), page 43 (the `gsd_m: 0.31` figure
re-asserted under a "documented" header), page 49 (`voter_reg`, screening base-rate
framing), and the three `military` / two `dual-use` topic tags.

### Kept, against the literal instruction — flagged and confirmed

- **`71`** was named for removal as "satellites related" but is *Wi-Fi / cell indoor
  positioning*, not a satellite page. Kept and reworked: retitled **"Indoor
  Positioning"**, subtitle and four prose passages reframed off the satellite contrast,
  canvas labels changed (`satellites` → `outdoor signal`, `Satellite` → `Outdoor fix`).
  One physical mention remains at `:37` — signals need sky view, which is *why* indoor
  methods exist; removing it would make the page incoherent.
- **`76-iot-device-telemetry`** survived the section-08 deletion and moved to section 04.
  Connected appliances are an everyday-user topic, and the reviewer rated this page's
  epistemic hygiene the best in its batch (`:33` states the payload fields are
  public-spec concepts, "not a leak of anyone's internals").

### Section 06 retitled and retoned

`Government and Public Space` → **`Public Space Sensing`** (`<title>` and `<h1>`).
Subtitle was "Collection operated by public bodies or on public roads, where consent is
not part of the model" → "Sensors on roads and in public places that record passing
traffic as a by-product of doing their job."

Card tones moved off the misuse/surveillance framing onto the everyday-device concept:

- Labels `GOVERNMENT` / `IDENTIFICATION` → `PUBLIC ROAD`
- Tags `government`, `surveillance`, `retroactive` → `public-road`, `plates`,
  `selection-effect`, `ocr-error`, `retention`
- Card 20 lost "building a travel history that stays queryable by police for weeks";
  now leads with the OCR-error point (discarding the image removes the only way to check
  a misread plate) — which is the statistical content anyway
- Card 19 lost "not only violators" phrasing → "not only the ones it acts on"

### Card 50 moved

Out of section 06 (it is a private brokerage mechanism, not public space) into section 04
beside payments. Its card copy was also reworded off the Tier-1 problem — "The market
maker sees the order before it is filled" → "Retail orders are routed to wholesalers that
fill them from inventory. The routing step is what creates the record." **The page's own
canvases are still unfixed** — see Tier 1.

### Hub updated

Dead section-08 card removed. Three blurbs corrected that advertised entries not on their
target pages (card 3 promised flight/ship feeds; card 4 omitted appliances and named
retail face matching; card 6 promised data fusion and orbital observation). Card 6
relabelled `PUBLIC SPACE`.

### Verified after all changes

67 pages, 8 sections, 67 links — zero orphans, zero dangling links, zero duplicates,
all card indices match their target filename prefixes.

### Open housekeeping from these removals

- **`tracking/CLAUDE.md` claims-to-source list references deleted pages** — entries for
  the old 53, 43, 50's Robinhood figures and the section-08 pages are now dead. The file's
  renaming-rule log also cites old numbers (58, 59, 49, 17, 20, 21, 31, 45, 16) which have
  all shifted. Partially updated for the flat structure; the per-claim numbers were left
  as historical record.
- Section 05 lost its only overhead-imaging entry, so its blurb is accurate as written.

---

## Restructure 2026-08-23 (flat grid)

### Section 09 removed

Held exactly one card. Page kept and moved into Everyday Phone and Desktop Use, which
already carried the AI chatbot page.

### Section 06 renamed

`06-government-and-public-space.html` → `06-traffic-monitoring.html`; title, `<h1>` and
subtitle changed to "Traffic Monitoring" / "Roadside cameras that record passing vehicles
as a by-product of managing traffic." Hub card label `PUBLIC SPACE` → `TRAFFIC`.

### Flat grid replaces the section-page layer

`sections/` is **deleted**. All 67 cards now live in the hub
(`backlog/61-tracking-data-collection-methods.html`) in one flat grid, grouped under seven
`.section-title` headings with `.section-blurb` lines, preceded by a `.toc` block linking
to each heading anchor with its card range.

This retired the 27 dead CSS rules noted above — `.section-title` and `.section-blurb`
were declared-but-unused in the old section pages and are now actually used in the hub.

### Renumbered 1–67

Every page renamed to a sequential index in section order, slug preserved
(`11-email-beacons.html` → `04-email-beacons.html`). 62 of 67 files changed name.
Executed as a two-phase rename through temporary names, because the target namespace
overlapped the source namespace (e.g. 11→04 while a different 04 still existed) and a
one-phase rename would have silently overwritten files.

Four pages carried their own index inside `<h1>`/`<title>` and were stripped, since the
number is now owned by the filename and the hub card: the old 19, 22, 27, 28.

### Verified after restructure

- 67 pages on disk, 67 cards in the hub, every href resolves
- Card indices sequential 1–67 with **zero** filename/heading mismatches
- 7 TOC anchors ↔ 7 section ids, no unmatched entries either direction
- No remaining reference to `sections/` or to any `../NN-` sibling path

Note on method: the first numbering check used a regex that matched nothing and reported
`cards: 0, sequential: False` — a check that cannot pass is not evidence. Re-run with a
working pattern before trusting the result, per the data-integrity rule in `CLAUDE.md`.

### Still unfixed — the review findings above

The restructure moved and renumbered pages; it did **not** touch page content. Every Tier
1–8 finding above still stands, though **file numbers in this document refer to the OLD
numbering.** Mapping for the highest-severity items:

| Finding | Old | New |
|---|---|---|
| Front-running canvases (Tier 1) | 50 | **54**-stock-trading-order-flow |
| `Actual usage breakdown` 15/85% | 06 | **08**-browser-fingerprinting |
| Incommensurable 0–100 values | 12 | **10**-cross-device-tracking |
| Eight-eyes watcher canvas | 36 | **06**-session-replay |
| `The Friendly Story` vs `The Reality` | 02 | **02**-javascript-tags (unchanged) |
| Iceberg `what you can see vs what you can't` | 03 | **03**-analytics-platforms (unchanged) |
| `Diapers → infant in household?` | 34 | **47**-loyalty-cards-store-apps |
| ASIN-format item ids | 58 | **50**-two-sided-marketplace-events |
| Pinterest vocabulary | 59 | **51**-visual-search-commerce-events |
| Shop Pay email-hash claim, routable IP | 60 | **52**-shopify-merchant-platform |
| Red on the *goal* series | 68 | **63**-version-control-activity-metrics |
| Real MCC-MNC in payload | 72 | **57**-carrier-network-records |



---

## Layout & Visualization Pass — completed

Separate from the content review above. None of this touched a finding; it was
structure, colour, and typography only.

### Palette

Replaced the 4-colour set (navy / green / red / orange) with a 7-hue categorical
rotation declared once per page as `P`, validated for colour-vision-deficiency
separation. **Red is out of the rotation by construction**, which permanently fixes
the decorative-red class of bug rather than fixing instances of it. Navy `#1a5276`
fails the validator as a series colour and survives only as ink — headings, axes,
callout borders.

Method note: the validator was first run from a copied path, which broke its own CLI
guard and made it exit 0 silently. It was then sanity-checked against two
near-identical blues and confirmed to FAIL before any PASS was trusted.

### Row-2 restructure

The "What does it collect?" row had a long bullet list on the left and a short canvas
on the right, leaving a tall empty band. The payload moved to the right column below
that row's canvas, with a CSS override to left-align code inside the centre-aligned
cell. 62 pages moved. Three event-schema pages (49, 50, 51) use a 3-column card grid
with the canvas already above the payload — no imbalance, left as-is. Two further pages
(11 hidden-page-content, 24 sports-athlete-tracking) needed doing by hand: their collect
row has a wide inner table instead of a bullet list, so the bulk pass skipped them.

### The whitespace bug

`setupCanvas` hardcoded `h = 240`. When the markup moved to `height="320"`, the JS
overrode it, so 63 pages drew into the top 240px of a 320px box. This — not the
layout — was the real source of the dead space. Fixed to read the element's
attributes. Two follow-on breaks were caught: the same regex had patched IIFE-level
`const w = 720, h = 240` where `c` is out of scope, and one page names its variable
`canvas` rather than `c`.

### Text overflow

A font bump was shipped across 265 canvases on the strength of a harness that stubbed
`measureText` to a constant — i.e. a harness structurally incapable of detecting width
overflow, a limitation that had been noted and then shipped past anyway. The user
reported overlapping text.

The replacement harness estimates per-character advance widths **and tracks the 2D
transform matrix**. That second part matters: rotated y-axis titles drawn at `(0,0)`
after `translate()`+`rotate()` read as gross overflow to a naive checker. The naive
count was 58 labels across 30 pages; with transforms handled it was **33 across 18** —
so 25 of the apparent problems were false positives that would have sent someone
rewriting labels that were always fine.

All 33 fixed by repositioning, wrapping at word boundaries, or widening a canvas —
**never by shrinking type**, which is what caused the original problem. Verified
afterwards that the fixes were structural only: no new hex literals, no data array
altered, no font smaller than before, and **every word of every caption preserved
exactly** (token-level diff against a pre-edit backup).

### Final state

| Check | Result |
|---|---|
| Canvases execute, none blank | 67 / 67 |
| Overflowing text labels | 0 |
| Payload in a left cell | none |
| Payload missing its caption | none |
| Hub links resolve | 67 / 67 |
| Card number ↔ filename | 0 mismatches |
| Orphan / dangling files | none |

---

## Addition: Head-Worn Cameras and Assistants (card 31)

Added to *Wearables and the Body*; files 31–67 shifted to 32–68, hub cards and
filenames renumbered together.

**The requested framing was "always-on subtle recording" and that was not used.**
Capture on the current products is an explicit button press or voice command, and the
capture indicator is not defeatable in software; the documented complaint is that the
light is easy to miss, not that it is absent. Asserting always-on recording would have
broken the no-attributed-intent and no-unsourced-specifics rules in `CLAUDE.md` — and
it is the weaker argument regardless.

**What the card argues instead — the bystander as unit of observation.** Every other
page in this set records the device owner. This one records whoever is in front of the
wearer, and that is the structural break no other card covers:

- Consent, settings, and deletion all attach to the wearer's account. The recorded
  party has no account, so "the user agreed to the terms" does not describe most of the
  people in the data. The payload carries `"subject_consent": null` — no such field
  exists and there is no path to populate one.
- An indicator light is a detection problem with two error types, not a guarantee. It
  notifies someone looking at it, close enough, in adequate light, who knows what it
  means. C3 draws the 2×2 rather than asserting a rate; cell sizes are equal by
  construction and the caption says so.
- First-person framing is non-random sampling. The camera points where the wearer
  chose to look, so the corpus samples wearer *attention*, weighted by where wearers
  go, at one height. C4 draws this as attended-subset-of-present.
- A multimodal query converts a frame into a sentence. The sentence is small,
  searchable, and cheap to keep after the frame is gone; a description naming who was
  present is a record about those people. C2 contrasts the four records by size.

Ray-Ban Meta and Snap Spectacles are named in prose as examples; the title stays
generic because the payload is reconstruction, the same rule that keeps AirTag out of
the item-tracker title.

Verified: 4 canvases execute, 0 overflowing labels, 7 palette hues across the four
charts, no red, no `Math.random()`, one payload with its caption, 5 illustrative labels.
