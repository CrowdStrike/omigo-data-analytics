# Proprietary Data on the Job — The Data You Inherit With Employment

**Page type:** detail page (four titled sections, each a two-column row: text left 45%, canvas right 55%)
**HTML title tag:** Proprietary Data on the Job — The Data You Inherit With Employment

**Subtitle:** The largest and most common data acquisition channel is not a download link or an API: it is a job offer. Join a company and you inherit access to datasets that nobody outside the building can touch — years of transactions, clickstreams, tickets, and telemetry that the business generated as a side effect of operating.

**Intro callout (blue-left-border box):** Most working data scientists spend their careers on proprietary data. Every purchase, page view, support call, and sensor reading a company handles leaves a record behind, and those records accumulate into a warehouse that no public dataset can match for scale, history, or label quality. The trade is access with strings attached: the data comes scoped, governed, and non-portable — it belongs to the employer, not to you.

## 1. The employer's warehouse — what is inside

Every function of a running business writes data as a by-product of doing its job, and the warehouse is where those by-products land as queryable tables.

- **Transaction records:** every order, payment, refund, and invoice lands in a ledger table.
- **Financial history:** the ledger covers every customer relationship end to end.
- **Clickstream logs:** an event fires for every page view, tap, search, and scroll.
- **Behavioral scale:** billions of rows describe how people actually use the product.
- **CRM and support tickets:** sales notes, account histories, and conversations captured as text.
- **Customer voice:** complaints, questions, and churn warnings in the customers' own words.
- **Operational telemetry:** servers, trucks, machines, and apps report health metrics continuously.
- **Always recording:** status events are written whether or not anyone is watching.
- **Nothing collected "for data science":** each table exists because a team needed it to operate.
- **Goldmine by accident:** billing needed the ledger, support needed the tickets — analysis is a side effect.

Key point: The warehouse is a map of the business itself: every product function leaves a table behind. A newcomer's first job is not modeling — it is learning what tables exist and which team's work produced each one.

### Visualization (canvas `c1`, 720×400)

Warehouse diagram: four source-system boxes on the left, arrows feeding a large warehouse box on the right containing stacked table rows.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Every product function leaves a table behind"
- **Source boxes (each 195×58 at x=40, white fill, 2px border in box color; bold 12px label in box color at +12/+22, 11px `#666` subline at +12/+40):**
  - y=52 "Billing & payments" `#27ae60` — "orders, refunds, invoices"
  - y=134 "Product frontend" `#1a5276` — "clicks, searches, page views"
  - y=216 "Sales & support" `#e67e22` — "CRM notes, tickets, calls"
  - y=298 "Infrastructure & devices" `#8e44ad` — "server metrics, app telemetry"
- **Connectors:** 1.5px `#bbb` lines from each source box's right edge (mid-height) to the warehouse box's left edge at matching heights, each with a small filled `#bbb` arrowhead.
- **Warehouse box:** 330×320 at (350, 48), fill `rgba(26,82,118,0.06)`, 2px `#1a5276` border. Bold 13px `#1a5276` centered at top: "COMPANY WAREHOUSE". Inside, six stacked table rows (290×34, x=370, starting y=90, spaced 42px): white fill, 1.5px `#ccc` border, bold 11px `#555` table name left-aligned at +12, 10px `#999` note right-aligned at width−12 —
  - "orders" / "9 years of rows"
  - "click_events" / "billions of rows"
  - "support_tickets" / "full text kept"
  - "crm_accounts" / "every relationship"
  - "device_telemetry" / "one row per heartbeat"
  - "…and hundreds more" / "one per team need"
- **Caption (12px `#999`, centered, y = h−14):** "None of it was collected for data science — the goldmine is a side effect of operating"

## 2. Why it beats anything downloadable

Public benchmark datasets are curated snapshots; the employer's warehouse is a living record of a real system, and it wins on every axis that matters for applied work.

- **Scale:** a week of product events exceeds most public benchmarks in total size.
- **Sample size solved:** data volume stops being the binding constraint.
- **Longitudinal depth:** the warehouse follows the same customers for years.
- **Slow questions answerable:** retention, lifetime value, and behavioral drift become studyable.
- **Snapshot limit:** a one-time public snapshot can never answer those questions.
- **Labels from real outcomes:** churn, chargebacks, conversions, and returns are ground truth.
- **No annotator guessing:** the business generates the labels itself, not paid raters.
- **Living context:** the people who built the billing system sit two desks away.
- **Anomalies explained:** every oddity in the data has a colleague who knows why.

Key point: The moat is not the algorithms — the same models are in every open-source library. The moat is years of proprietary, labeled, longitudinal data plus the colleagues who know how it was made.

### Visualization (canvas `c2`, 720×380)

Grouped horizontal comparison bars: four axes (scale, history, label quality, context on demand), each with a short gray bar for "public benchmark" and a long colored bar for "internal warehouse".

- **Title (bold 14px `#1a5276`, centered, y=22):** "Public benchmark vs. the warehouse you inherit"
- **Layout:** four axis groups starting at y=60, spaced 74px. Each group: bold 12px `#555` axis label left-aligned at x=40; two horizontal bars starting at x=200 (max length 440), height 16, 8px apart.
- **Bars per group (top bar `#ccc` fill = public benchmark; bottom bar colored fill = internal warehouse; 10px `#999` / colored value label just right of each bar end):**
  - "Scale" — public 90px "one curated sample" / internal 440px `rgba(26,82,118,0.55)` "billions of live events"
  - "History" — public 70px "single snapshot" / internal 420px `rgba(39,174,96,0.55)` "same users, year after year"
  - "Label quality" — public 150px "annotator guesses" / internal 400px `rgba(230,126,34,0.55)` "real outcomes: churn, chargebacks"
  - "Context on demand" — public 60px "a README" / internal 430px `rgba(142,68,173,0.55)` "the team that built it, two desks away"
- **Legend (11px, y=44):** `#999` square + "public benchmark" at x=200; `#1a5276` square + "internal warehouse" at x=360.
- **Caption (12px `#999`, centered, y = h−14):** "The labels are generated by the business itself — no annotation budget can buy that"

## 3. The strings attached

Access is granted for a purpose, inside a fence, and it does not travel with you — the same properties that make the data valuable make it governed.

- **Role-based access:** you see the tables your role needs and nothing more.
- **Sensitive-table gates:** requests go through approval, logging, and periodic review.
- **Privacy regulations:** GDPR, HIPAA, and PCI DSS constrain joins, exports, and retention.
- **Masked fields:** personal data is often tokenized before an analyst ever sees it.
- **Purpose limitation:** data consented for billing is consented for billing only.
- **Legal, not technical:** reusing it for an unrelated model can break the law even if the query runs.
- **Non-portability:** skills and judgment leave with you; the data stays behind.
- **Extract risk:** taking even a summary extract is fireable and often prosecutable.

Key point: Treat access as a scoped privilege, not a personal asset. The question "am I technically able to query this?" and the question "am I permitted to use this for this purpose?" have different answers surprisingly often.

### Visualization (canvas `c3`, 720×400)

Concentric access rings: four nested rounded rectangles from outermost (public) to innermost (need-to-know), each labeled, with a small "you are here" marker in the second ring.

- **Title (bold 14px `#1a5276`, centered, y=22):** "Access comes in rings — employment moves you inward, not everywhere"
- **Rings (nested rectangles centered at x=360, all with 2px borders, drawn outermost first; bold 12px ring label in ring color at top-left inside each ring, 11px `#666` subline below the label):**
  - Outer: 640×330 at (40, 46), border `#bbb`, fill `#fff` — "PUBLIC" / "marketing site, published reports — anyone"
  - Second: 520×250 at (100, 86), border `#27ae60`, fill `rgba(39,174,96,0.06)` — "EMPLOYEE" / "general warehouse, dashboards — badge required"
  - Third: 400×170 at (160, 126), border `#e67e22`, fill `rgba(230,126,34,0.08)` — "TEAM" / "raw product tables — role-based grant"
  - Inner: 280×90 at (220, 166), border `#e74c3c`, fill `rgba(231,76,60,0.08)` — "NEED-TO-KNOW" / "PII, payment data — approval + audit log"
- **Marker:** filled `#1a5276` dot (radius 5) at (560, 300) inside the second ring, with bold 11px `#1a5276` label "you, on day one" to its left.
- **Caption (12px `#999`, centered, y = h−14):** "Each inward step is a grant with a purpose attached — and it is revoked the day you leave"

## 4. What a newcomer should do

The instinct is to start modeling immediately; the payoff comes from spending the first weeks learning how the data came to exist.

- **Schema and lineage first:** trace each table back to the system that writes it.
- **Silent failure risk:** a model built on a misunderstood column fails without warning.
- **Sit with domain owners:** an hour with the support lead or billing engineer explains the quirks.
- **Beyond solo querying:** those quirks never surface from queries alone.
- **Scoped-privilege habit:** request the minimum and use it for the stated purpose.
- **No convenience copies:** never move data outside governed systems.
- **Provenance reflex:** ask "who writes this, when, and what breaks it?" before trusting a table.
- **Silent rot:** pipelines change and columns decay without announcement.

Key point: The most valuable habit is provenance-first thinking: before modeling any table, know who produced it, why, and what could make it wrong. That habit transfers to every dataset you will ever touch — even though the data itself does not.

### Visualization (canvas `c4`, 720×320)

Learning-path timeline: horizontal arrow with four milestone dots from "day 1" to "week 6+", labels alternating above and below.

- **Title (bold 14px `#1a5276`, centered, y=22):** "The newcomer's path — understand the data before modeling it"
- **Timeline:** 2px `#999` line at y=160 from x=50 to x=670 with a filled right-arrowhead.
- **Steps (filled dot radius 7 in step color on the line; labels alternate above/below with thin `#ccc` connectors; label bold 12px in step color, subline 11px `#666` split across two lines, time tag 10px `#999` on the opposite side of the line):**
  - x=110, "day 1", "Map the schema" — "what tables exist," / "which team owns each" — `#1a5276` (above)
  - x=270, "week 1-2", "Trace the lineage" — "which system writes it," / "when, and what breaks it" — `#27ae60` (below)
  - x=430, "week 2-4", "Sit with domain owners" — "learn the quirks no" / "query will reveal" — `#e67e22` (above)
  - x=590, "week 4-6+", "Model with confidence" — "provenance known," / "assumptions checkable" — `#8e44ad` (below)
- **Caption (12px `#999`, centered, y = h−14):** "Weeks spent on lineage repay themselves the first time a pipeline silently changes"

## Regeneration instructions

- **Template:** backlog multi-section detail page. After h1, `.subtitle`, and `.intro` callout, one `.lang-section` per numbered section: `h2` (1.3rem `#1a5276`, `border-bottom: 2px solid #2980b9`, 4px padding-bottom), then a `table.layout` with one `<tr>`: left `<td class="text-col">` (45%) with lead paragraph, bullets, `.key-point`; right `<td class="viz-col">` (55%) with the canvas. Section h2 headings carry the "1.–4." numbers shown above.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9`; `.subtitle` `#666` 0.95rem; `.intro` — background `#f0f4f8`, `border-left: 3px solid #2980b9`, padding 8px 12px, 0.9rem, 32px margin-bottom; `.key-point` — background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 8px 12px, 0.9rem; `ul` 0.92rem; canvases `width: 100%`, `border: 1px solid #e0e0e0`, radius 4px; `.lang-section` 40px margin-bottom.
- **Bullet style:** each bullet is one line — a bold label plus a short phrase, no text-wrap; labels are bold `#1a5276` via `li strong { color: #1a5276; }`.
- **Canvases:** intrinsic width 720, heights 400/380/400/320 as specified; shared `setupCanvas(id, h)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Canvas fonts use `-apple-system, sans-serif`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`; grays `#555`/`#666`/`#888`/`#999`/`#bbb`/`#ccc`; translucent fills `rgba(26,82,118,0.12)`, `rgba(26,82,118,0.06)`, `rgba(39,174,96,0.06)`, `rgba(230,126,34,0.08)`, `rgba(231,76,60,0.08)`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
