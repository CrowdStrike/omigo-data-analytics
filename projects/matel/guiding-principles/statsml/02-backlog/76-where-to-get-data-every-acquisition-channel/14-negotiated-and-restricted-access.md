# Negotiated & Restricted Access

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Negotiated & Restricted Access — Data You Sign For

**Subtitle:** Some of the world's most valuable datasets are never downloadable: you apply, sign, and comply. The friction is the moat — training requirements, data-use agreements, secure rooms, and records requests are what make it possible to work with data that could never be posted openly.

**Intro callout (blue-left-border box):** Every channel on this page trades convenience for depth. A hospital cannot post patient records on a portal, and a census bureau cannot publish every respondent's exact address — but both can grant access to a vetted researcher who has signed for it. The paperwork is not an obstacle in front of the data; it is the mechanism that lets the data exist for outsiders at all.

## 1. Credentialed research datasets — trained, signed, admitted

Some of the richest research datasets are free but gated: you complete ethics training, sign a data-use agreement, and only then does the download link appear.

- **MIMIC:** de-identified critical-care records from a large hospital system.
- **PhysioNet gate:** ethics training plus a signed data-use agreement unlock it.
- **UK Biobank:** deep genomic and health data on half a million volunteers.
- **Approved applications:** access is granted per research proposal, for a fee.
- **All of Us:** the NIH program tiers data access by researcher credential level.
- **Accountability:** every user is named, trained, and bound by the agreement.

Key point: The gate is the point. Training and signatures make every user identifiable and accountable, which is exactly what lets hospitals and biobanks release data at a depth no open portal could ever match.

### Visualization (canvas `c1`, 720×400)

Access ladder: four ascending steps from open download to secure enclave, with friction rising along the climb.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The access ladder — each rung trades friction for depth"
- **Staircase:** four step platforms (2px `#bbb` horizontal lines, each 150px wide) rising left to right: step tops at y=300 (x=40–190), y=230 (x=200–350), y=160 (x=360–510), y=90 (x=520–670); thin `#ccc` vertical risers connecting consecutive steps.
- **Step boxes (each 140×54, sitting 60px above its step line, white fill, 2px border in step color; bold 12px label in step color centered, two 10px `#666` sublines centered):**
  - Step 1 `#27ae60` "OPEN DOWNLOAD" — "click and go" / "anonymous users"
  - Step 2 `#1a5276` "REGISTRATION" — "create an account" / "agree to terms"
  - Step 3 `#e67e22` "CREDENTIALED DUA" — "ethics training + signed" / "agreement (MIMIC)"
  - Step 4 `#e74c3c` "SECURE ENCLAVE" — "approved project, analysis" / "inside their environment"
- **Connector:** thin `#ccc` line from each box bottom to its step line.
- **Friction arrow:** 2px `#e74c3c` diagonal arrow from (60, 350) to (660, 62) with filled arrowhead; rotated bold 11px `#e74c3c` label along it: "friction rises — and so does what you are allowed to see".
- **Caption (12px `#999`, centered, y = h−14):** "MIMIC, UK Biobank, and All of Us all live on the upper rungs — valuable because they are gated"

## 2. Restricted government microdata — the detail stays in the room

Statistical agencies publish deliberately coarsened public extracts; the full-detail records exist, but only inside secure facilities where outputs are reviewed before release.

- **PUMS:** public-use microdata samples with the identifying detail sanded off.
- **Deliberate coarsening:** geography is aggregated and top incomes are capped.
- **Why blur:** so no combination of columns can re-identify a respondent.
- **Research data centers:** vetted researchers work on locked-down machines on site.
- **Sworn status:** analysts take an oath and face penalties for any disclosure.
- **Output review:** every table and figure is checked before it leaves the room.

Key point: The data never moves — the researcher does. Disclosure review of every output is the price of seeing government microdata at full resolution, and agencies consider that price non-negotiable.

### Visualization (canvas `c2`, 720×380)

Two-detail-levels diagram: a coarse public extract panel on the left, full microdata inside a secure-room boundary on the right, with a reviewed-output gate at the boundary.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Same survey, two resolutions — the fine grain never leaves the secure room"
- **Public panel (left):** 280×230 box at (40, 60), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 12px `#27ae60` centered at top: "PUBLIC EXTRACT (PUMS)". Inside, a coarse 3×3 grid of cells (thin `#27ae60` lines, ~70px cells) occupying 210×140 centered; 10px `#666` lines below the grid: "state-level geography" / "top-coded incomes, banded ages".
- **Secure room (right):** 300×260 dashed-border box (dash 8/6, 2px `#e74c3c`) at (390, 50), fill `rgba(231,76,60,0.05)`. Bold 12px `#e74c3c` centered at top: "SECURE RESEARCH DATA CENTER". Inside, an inner 220×130 box (1.5px `#1a5276`, fill `rgba(26,82,118,0.12)`) labeled bold 11px `#1a5276` "FULL MICRODATA" containing a fine 8×5 grid (thin `rgba(26,82,118,0.35)` lines); 10px `#666` lines below the inner box: "exact geography, exact income" / "locked-down machines, sworn researchers".
- **Output gate:** small 120×40 white box straddling the secure room's left edge at mid-height (2px `#e67e22` border), bold 10px `#e67e22` two lines centered: "OUTPUT REVIEW" / "every result checked"; 1.5px `#e67e22` arrow from the gate to the public side labeled 10px `#666` "approved aggregates only".
- **Blocked path:** 2px `#e74c3c` arrow from the microdata box toward the left boundary, cut by a bold 14px `#e74c3c` "✕" and 10px `#e74c3c` label "raw rows never exit".
- **Caption (12px `#999`, centered, y = h−14):** "Coarse for everyone, exact for the vetted few — and even the vetted few only export reviewed results"

## 3. Partnerships & consortia — the analysis travels to the data

When neither side can release data outright, they negotiate a structure that lets the analysis happen anyway — and increasingly, the code moves instead of the records.

- **Hospital-university ties:** clinical data flows inside IRB-governed projects.
- **Industry agreements:** companies share data under negotiated legal terms.
- **Consortia:** many institutions contribute to one jointly governed pool.
- **Federated analysis:** the query visits each silo; raw records never leave.
- **Model-to-data:** you submit code, it runs beside the data, results return.
- **The unlock:** structures like these open data no side would publish alone.

Key point: The negotiation replaces the download. Because raw records never change hands, each custodian keeps control — and that retained control is precisely what makes them willing to participate.

### Visualization (canvas `c3`, 720×360)

Model-to-data loop: researcher on the left, data enclave on the right; code travels in along the top arc, only aggregate results travel out along the bottom arc, and raw data is blocked from crossing.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Model-to-data — the code makes the trip, the records stay home"
- **Researcher box:** 190×90 at (50, 140), fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border. Bold 12px `#1a5276` "RESEARCHER"; 11px `#555` lines: "writes analysis code" / "never touches raw rows"; 10px `#999`: "outside the boundary".
- **Enclave box:** 230×130 at (450, 120), fill `rgba(39,174,96,0.10)`, 2px `#27ae60` border. Bold 12px `#27ae60` "DATA CUSTODIAN'S ENCLAVE"; inner 190×46 box (1.5px `#1a5276`, white fill) labeled bold 11px `#1a5276` "PRIVATE DATA" with 10px `#666` subline "clinical records, user logs"; 10px `#666` line under the inner box: "code executes here, beside the data".
- **Top arc (code in):** 2px `#e67e22` curved arrow from the researcher box top to the enclave box top (control point above, ~y=70), filled arrowhead; bold 11px `#e67e22` label above the arc: "code / model travels in"; 10px `#666` subline: "reviewed before it runs".
- **Bottom arc (results out):** 2px `#27ae60` curved arrow from the enclave box bottom to the researcher box bottom (control point below, ~y=305), filled arrowhead; bold 11px `#27ae60` label below the arc: "only aggregate results travel out"; 10px `#666` subline: "metrics, coefficients, model weights".
- **Blocked direct path:** dashed (6/5) 2px `#e74c3c` horizontal line between the two boxes at mid-height, interrupted by a bold 16px `#e74c3c` "✕" at its midpoint; 10px `#e74c3c` label beneath: "raw data never crosses".
- **Caption (12px `#999`, centered, y = h−14):** "Federated setups repeat this loop across many silos — each custodian keeps its records, everyone shares the answer"

## 4. Records requests — data pried loose by law

Freedom-of-information laws compel government agencies to hand over records on request — the slowest acquisition channel on this page, and sometimes the only one.

- **FOIA:** any person can request US federal agency records, no reason needed.
- **Equivalents abroad:** most democracies have a comparable disclosure law.
- **The wait:** statutory deadlines exist, but extensions and backlogs are routine.
- **Redactions:** exemptions black out personal, security, and deliberative material.
- **Appeals:** denials can be appealed, and litigation can pry out more.
- **Journalist's edge:** a granted request yields a dataset nobody else holds.

Key point: FOIA data arrives slowly, partially redacted, and inconveniently formatted — but exclusivity is the reward. A dataset assembled through requests, appeals, and patience has no competing copy anywhere.

### Visualization (canvas `c4`, 720×340)

Records-request timeline: request filed, statutory response window, partial release with redactions, appeal, fuller release — with the released fraction growing along the way.

- **Title (bold 14px `#1a5276`, centered, y=22):** "A records request in five acts — slow, partial, and ultimately exclusive"
- **Timeline:** 2px `#999` line at y=140 from x=50 to x=670 with a filled right-arrowhead.
- **Steps (filled dot radius 7 in step color on the line; labels alternate above/below with thin `#ccc` connectors; label bold 12px in step color, subline 11px `#666`, time tag 10px `#999` on the opposite side of the line):**
  - x=90, "day 0", "Request filed" — "records described in writing" — `#1a5276` (above)
  - x=225, "statutory window", "Agency response" — "deadline set by law; extensions common" — `#e67e22` (below)
  - x=360, "months", "Partial release" — "pages arrive, exemptions redacted" — `#8e44ad` (above)
  - x=495, "more months", "Appeal" — "challenge withheld material" — `#e74c3c` (below)
  - x=630, "eventually", "Fuller release" — "more pages, fewer redactions" — `#27ae60` (above)
- **Release bar (y=250):** horizontal stacked bar 560×26 starting at x=80, thin `#bbb` outline, labeled 11px `#666` above at x=80 left-aligned: "share of requested records in hand"; segments: x=80–304 fill `rgba(26,82,118,0.12)` labeled 10px `#999` centered "nothing yet", x=304–472 fill `rgba(142,68,173,0.25)` labeled 10px `#555` "partial, redacted", x=472–640 fill `rgba(39,174,96,0.35)` labeled 10px `#555` "after appeal".
- **Caption (12px `#999`, centered, y = h−14):** "Illustrative Example: a typical request cycle — the result is late and incomplete, but it exists nowhere else"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Bullet style:** each bullet is a bold label (`li strong { color: #1a5276; }`) plus a short phrase that fits on one line at normal page width — bullets must not text-wrap. Never cut information to fit; split it into more labeled bullets instead.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Canvases:** intrinsic width 720, heights 400/380/360/340 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.35)`, `rgba(39,174,96,0.10)`, `rgba(142,68,173,0.12)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
