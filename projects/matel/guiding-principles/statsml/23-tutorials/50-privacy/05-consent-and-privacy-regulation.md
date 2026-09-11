# Consent & Privacy Regulation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Consent & Privacy Regulation

**Subtitle:** GDPR (EU, 2018) and CCPA (California, 2020) turned privacy into engineering requirements — this page maps the concepts to system design, and is not legal advice

## The Day Personal Data Got a Rulebook

**Tags:** `core idea` (blue), `GDPR / CCPA` (green), `not legal advice` (orange)

- **The shift** — GDPR (EU, 2018) made processing personal data need a legal basis
- **Legal basis** — consent, contract, legitimate interest, or another lawful basis must exist before processing
- **Enforceable rights** — GDPR and CCPA (California, 2020) give users access, deletion, portability, and opt-out — with penalties
- **Personal data is broad** — user IDs, IP addresses, and device fingerprints count, not just names and emails
- **Concept level only** — this page shows the engineering shape of the rules; what applies to you is a lawyer's call

*Example (italic):* A recommender trained on browsing history now needs a recorded legal basis for that history — "we already had the logs" stopped being a reason in 2018.

**Key point:** The regulations changed the default: personal data processing is forbidden unless justified, and users hold rights your systems must be able to honor on demand.

### Visualization (canvas `c1`, 720×300)

Two-row before/after flow diagram: the pre-2018 pipeline (collect everything, keep forever) vs the regulated pipeline (legal basis gate in front, user rights pointing back in).

- **Title (bold 15px, `#1a5276`, top center):** "Before: Collect and Keep. After: Justify, and Answer to the User".
- **Row 1 (y=95), label 12px `#444` at x=20:** "before 2018"; blue `#2a78d6` rounded box at x=150 labeled "app events" (12px), 3px arrow to a blue box at x=350 labeled "collect everything", arrow to a blue box at x=560 labeled "keep forever".
- **Row 2 (y=210), label:** "under GDPR/CCPA"; blue box at x=150 "app events", 3px arrow into a green `#008300` diamond-ish box at x=320 labeled "legal basis?" (12px), arrow to a blue box at x=530 labeled "process + retention limit".
- **Rights arrows:** four thin 2px violet `#4a3aa7` arrows rising from a 12px violet label "user rights: access · delete · port · withdraw" at (x=360, y=285) into the row-2 processing box.
- **Box style:** 130–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, right side near y=170):** "the gate and the arrows are new".
- **Caption (12px `#444`, bottom right):** "schematic — concept level, not legal advice".

## One Deletion Request, Six Copies

**Tags:** `worked example` (blue), `right to erasure` (green), `hard in practice` (red)

- **The request** — user 4127 clicks "delete my account"; GDPR gives roughly a month to comply
- **The easy copy** — the row in the production users table is gone in about 5 minutes
- **The findable copies** — the analytics warehouse clears in 1 day, downstream tables in 3, the search index in 7
- **Backups** — nightly snapshots rotate every 35 days; the common pattern is to let them age out, documented
- **The training set** — a model trained on a data snapshot holds the hardest copy: purge at the next retrain, ~30 days
- **Same pipes, other rights** — access ("export it all") and consent withdrawal must reach the same six places

*Example (italic):* User 4127's production row dies in 5 minutes, but their last trace — the 35-day backup rotation — outlives the ~30-day response window and is handled by documented policy, not deletion.

**Key point:** "Erase me" is a fan-out problem: one request must reach every copy — prod, warehouse, derived tables, the search index, backups, and the ML training set — and you can only fan out to copies you know about.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: days until user 4127's data is actually gone from each copy, with a dashed marker at the ~30-day response window.

- **Title (bold 15px, `#1a5276`, top center):** "'Erase Me': Days Until Each Copy of User 4127 Is Gone".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, plot width 440 mapping 0–40 days linearly (11px per day); x tick labels "0", "10", "20", "30", "40" (12px `#444`) under the plot at y=262, gridlines `#e5e9ef` at 10/20/30/40.
- **Rows (top to bottom at y = 60, 95, 130, 165, 200, 235), each with a left-aligned 12px `#444` label at x=20, bars 16px tall:**
  - "prod users table — 5 min": green `#008300` bar width 2
  - "analytics warehouse — 1 day": green bar width 11
  - "downstream tables — 3 days": green bar width 33
  - "search index — 7 days": blue `#2a78d6` bar width 77
  - "ML training set — 30 days": orange `#d95926` bar width 330, 11px orange label "next retrain" at bar end
  - "backup rotation — 35 days": red `#e74c3c` bar width 385, 11px red label "ages out, documented" at bar end
- **Window marker:** vertical dashed `#6b7280` (dash 4/3) line at x=560 (day 30), bold 12px `#6b7280` label "~30-day response window" at its top.
- **Annotation (bold 13px orange `#d95926`, near x=300, y=45):** "the training set is the copy teams forget".
- **Caption (12px `#444`, bottom right):** "timelines illustrative".

## What It Does to System Design — and the Fines Behind It

**Tags:** `where it's used` (blue), `design consequences` (green), `enforcement` (red)

- **Inventory and lineage** — you cannot delete or export what you cannot find; cataloging personal data comes first
- **Purpose limitation** — data collected for fraud checks cannot silently feed ad targeting or model training
- **Retention limits** — default-delete after a set window replaces default-keep-forever
- **Consent in the schema** — consent state rides with every event as a first-class field, so each consumer can filter
- **Privacy-by-design reviews** — new pipelines get a privacy check at design time, like a security review
- **The stick** — GDPR fines reach 4% of global annual revenue; billion-euro penalties are on the public record

*Example (italic):* Meta's 2023 GDPR fine over data transfers was €1.2 billion — one publicly documented number that moved privacy from a slide bullet to a launch blocker.

**Key point:** Each user right maps to a system property — lineage for deletion, exports for access, propagating consent flags for withdrawal — and the fine schedule is what got those properties funded.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of the largest publicly documented GDPR fines, in millions of euros.

- **Title (bold 15px, `#1a5276`, top center):** "The Fines That Made It Real (publicly documented GDPR penalties, €M)".
- **Axis:** vertical 2px `#999` baseline at x=200, bars extend right, plot width 460 mapping €0–1,300M linearly; gridlines `#e5e9ef` at 250/500/750/1000/1250 with 11px `#999` labels at y=262.
- **Rows (top to bottom at y = 60, 95, 130, 165, 200, 235), each with a left-aligned 12px `#444` label at x=20, bars 16px tall, fill `rgba(231,76,60,0.35)` with 2px `#e74c3c` edge, bold 12px `#e74c3c` value label at bar end:**
  - "Meta (2023)": width 425 — "€1,200M"
  - "Amazon (2021)": width 264 — "€746M"
  - "Instagram (2022)": width 143 — "€405M"
  - "TikTok (2023)": width 122 — "€345M"
  - "WhatsApp (2021)": width 80 — "€225M"
  - "Google (2019)": width 18 — "€50M"
- **Annotation (bold 13px ink `#1a5276`, near x=420, y=45):** "cap: 4% of global annual revenue".
- **Caption (12px `#444`, bottom right):** "fine amounts as publicly reported at issuance".

## "We Anonymized It, So the Rules Don't Apply"

**Tags:** `common mistake` (red), `re-identification` (orange)

- **The claim** — strip names and emails, call the dataset anonymous, and keep it outside the rules
- **The reality** — pseudonymized data that can be re-linked to a person stays personal data under GDPR
- **The classic result** — 87% of the US population is unique on ZIP code + birth date + sex (documented study)
- **Linkage attacks** — join a "de-identified" release with a public dataset and rows get names back
- **The real bar** — anonymization must survive linkage against outside data, not just pass a column checklist
- **The takeaway** — privacy is a system requirement like uptime: cheapest when designed in, brutal to retrofit

*Example (italic):* The field's founding demo joined a "de-identified" hospital dataset with a public voter roll on ZIP + birth date + sex and pulled out a state governor's medical records.

**Common mistake:** Treating "we removed the name column" as anonymization. Regulators judge by whether a person can be re-identified — and a handful of quasi-identifiers usually suffices — so weak anonymization buys no exemption, only false confidence.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: share of the US population uniquely identified by birth date + sex plus increasingly precise location, from Sweeney's documented study.

- **Title (bold 15px, `#1a5276`, top center):** "Three 'Harmless' Columns: % of US Population Uniquely Identified".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, plot width 420 mapping 0–100% linearly; gridlines `#e5e9ef` at 25/50/75/100 with 11px `#999` labels at y=250.
- **Rows (top to bottom at y = 80, 135, 190), each with a left-aligned 12px `#444` label at x=20, bars 22px tall, bold 13px value label at bar end:**
  - "birth date + sex + county": blue `#2a78d6` bar width 76 — "18%"
  - "birth date + sex + city": orange `#d95926` bar width 223 — "53%"
  - "birth date + sex + 5-digit ZIP": red `#e74c3c` bar width 365 — "87%"
- **Annotation (bold 13px red `#e74c3c`, near x=330, y=55):** "no name column needed".
- **Caption (12px `#444`, bottom right):** "percentages from Sweeney's published re-identification study (2000)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the deletion-fanout timelines in `c2` are invented and labeled illustrative; the GDPR fine amounts in `c3` (1,200 / 746 / 405 / 345 / 225 / 50 €M) and the re-identification percentages in `c4` (18 / 53 / 87) are publicly documented figures and must match the text bullets.
- **Tone guard:** keep the "concept level, not legal advice" framing in the subtitle and the `c1` caption in any regeneration.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
