# Secrets That Leak Into Logs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Secrets That Leak Into Logs

**Subtitle:** Nobody commits a password — but the request logger prints the whole body, and logs travel further than any database

## One Debug Line, Seven Copies

**Tags:** `core idea` (blue), `request logging` (orange), `log fan-out` (red)

- **The team did it right** — every secret lives in a vault, fetched at startup; nothing sensitive sits in the code
- **The debug line** — chasing a bug, Alice logs each incoming request: method, path, headers, and body
- **What it caught** — a login body carrying a `<password-field>`, and an API call carrying an `<auth-header>`
- **A database is guarded** — access-controlled, audited, and nobody ships nightly copies of it to seven places
- **A log is built to be copied** — shipping, aggregating, sampling, and archiving is the entire point of a log
- **So it multiplies** — host file, shipper buffer, aggregator, archive, alerting sampler, error tracker, ticket paste
- **The definition** — a *logged secret* is a credential written into a stream whose blast radius is everywhere that stream goes

*Example (italic):* One debug line, added for one afternoon, puts the user's password in seven systems that were each provisioned for observability and none for secrets.

**Key point:** The blast radius of a logged secret is the union of everywhere logs go — a set far larger, and far less access-controlled, than the database the team was busy protecting.

### Visualization (canvas `c1`, 720×300)

Fan-out diagram: one log line at the left branching to seven destination boxes at the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Log Line, Seven Places It Now Lives".
- **Source box:** rounded box (8px radius) at x=20, y=118, 190×64, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border; bold 12px `#2c3e50` "log line" at (115, 140), then 11px `#2c3e50` "POST /login — body includes" at (115, 157) and "`<password-field>`" at (115, 172).
- **Hub dot:** filled 5px circle `#6b7280` at (250, 150); 3px `#6b7280` line from x=210 to x=250 at y=150 with an arrowhead at x=248.
- **Destination boxes (seven, left edge x=330, width 355, height 26, 6px radius, fill `rgba(42,120,214,0.13)`, 2px `#2a78d6` border), top edges at y = 26, 62, 98, 134, 170, 206, 242**; 12px `#2c3e50` text left-aligned at x=340, vertically centered in each box:
  1. "1. local log file on each host"
  2. "2. log shipper's on-disk buffer"
  3. "3. central log system — searchable by 120 engineers"
  4. "4. cold archive — 400-day retention"
  5. "5. alerting system that samples log lines"
  6. "6. third-party error tracker (exception context)"
  7. "7. screenshot or snippet pasted into a ticket"
- **Highlight:** box 3 gets border `#d95926` and fill `rgba(217,89,38,0.15)`; box 6 gets border `#4a3aa7` and fill `rgba(74,58,167,0.13)`.
- **Fan-out lines:** 2px `#2a78d6` lines from the hub dot (250, 150) to each box's left edge midpoint (326, boxTop+13), each ending in a 7px arrowhead.
- **Annotation (bold 12px `#d95926`, at (20, 292), left-aligned):** "120 readers at the central log system vs 8 at the database = 15× more people".
- **Caption:** none (annotation occupies the bottom strip).

## Rotating on Day 30 Leaves 370 Days of Copies

**Tags:** `worked example` (blue), `retention` (orange), `rotation` (green)

- **Day 0** — the debug line runs; the login body with the `<password-field>` is written to the log
- **Day 30** — Bob spots it in a code review, the credential is rotated, and the incident is closed
- **Hot retention 30 days** — the day-0 line ages out of the searchable log exactly as the leak is found
- **Archive retention 400 days** — the nightly archive copy of that same day keeps it far longer
- **Hand-check** — 400 − 30 = **370 days** the secret sits in cold storage after the incident is "closed"
- **Rotation is necessary, not sufficient** — it kills the credential's usefulness; it deletes zero copies
- **What to also do** — record the archive expiry date, and check whether any other copy needs purging

*Example (italic):* The searchable log looks clean on day 30, which is exactly why the team believes it is over — the archive holds the same line until day 400 (retention windows illustrative).

**Key point:** Incident response ends in days; log retention ends in months. Rotate immediately, then track every copy's expiry date — the "clean" search result on day 30 is the hot tier only.

### Visualization (canvas `c2`, 720×300)

Retention timeline: a day-axis from 0 to 400 with a short hot-tier bar, a long archive bar, and the 370-day remainder marked.

- **Title (bold 15px, `#1a5276`, top center):** "Rotated on Day 30 — the Archive Copy Lives to Day 400".
- **Axis:** origin x=70, axis line 2px `#999` at y=250 from x=70 to x=670; day 0 at x=70, day 400 at x=670, so `px(d) = 70 + d * 1.5`; ticks + 12px `#444` labels at days 0, 30, 100, 200, 300, 400 rendered as "0", "30", "100", "200", "300", "400"; 12px `#444` axis caption "days since the secret was logged" centered at (370, 290).
- **Hot-tier bar (24px tall, top edge y=90):** from px(0)=70 to px(30)=115, fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; 12px `#444` right-aligned label "hot log (30 d)" ending at x=64, y=107.
- **Archive bar (24px tall, top edge y=140):** from px(0)=70 to px(400)=670, fill `rgba(217,89,38,0.30)`, 2px `#d95926` border; 12px `#444` right-aligned label "archive (400 d)" ending at x=64, y=157.
- **370-day remainder:** inside the archive bar, hatch the span px(30)=115 to px(400)=670 by drawing 8 evenly spaced 1px `#d95926` vertical lines across it; bold 13px `#d95926` centered label "370 days of copies after the incident closed" at (395, 190); 2px `#d95926` horizontal measure line at y=205 from x=115 to x=670 with arrowheads at both ends.
- **Day-0 marker:** dashed (4/3) 1.5px `#e74c3c` vertical line at x=70 from y=70 to y=250; 12px `#e74c3c` label "day 0: secret logged" at (76, 50), left-aligned.
- **Day-30 marker:** dashed (4/3) 1.5px `#008300` vertical line at x=115 from y=70 to y=250; 12px `#008300` two-line label at (190, 50) and (190, 66): "day 30: noticed" / "+ credential rotated".
- **Caption (12px `#444`, bottom right at (708, 294), right-aligned):** "retention windows illustrative".

## Redact at Creation, and Allowlist the Fields

**Tags:** `defensive` (green), `redaction` (orange), `rule of thumb` (blue)

- **The generic logger is the danger** — "log the whole request" catches every field, including ones nobody listed
- **Exception handlers are worst** — they attach full request context exactly when something has gone wrong
- **Denylist by field name** — Alice's filter names `<password-field>` and `<auth-header>`; both get `<redacted>`
- **The one nobody listed** — a `<recovery-answer-field>` is not on the list, so 1 of the 3 secrets is logged in full
- **Allowlist instead** — permit `user-id`, `device-type`, `page-locale`; all 3 of 3 secrets stay out by construction
- **Same open-set problem** — a denylist must enumerate every bad field forever; an allowlist enumerates the safe few
- **Redact at creation** — scrub before the line is written, or every downstream copy becomes a race you lose
- **Add a scanner** — automated secret scanning of the log stream catches the field that review missed

*Example (italic):* The request has 6 fields, 3 of them sensitive: the denylist redacts 2 and leaks 1, while the allowlist logs 3 harmless fields and leaks 0.

**Key point:** Denylisting field names fails on the field nobody thought of; allowlisting the fields that may be logged fails closed. Apply it where the line is created, not in the pipeline.

### Visualization (canvas `c3`, 720×300)

Two side-by-side panels over the same 6-field request: denylist (one field slips through) vs allowlist (nothing sensitive logged).

- **Title (bold 15px, `#1a5276`, top center):** "Same 6-Field Request: Denylist Leaks the Unlisted Field".
- **Field list (shared, 12px `#2c3e50`, left-aligned at x=20), rows at y = 78, 104, 130, 156, 182, 208:** "user-id", "device-type", "page-locale", "`<password-field>`", "`<auth-header>`", "`<recovery-answer-field>`"; the last three drawn in `#d55181`.
- **Panel headers (bold 12px, centered):** "denylist by field name" in `#d95926` at (390, 50); "allowlist by field name" in `#008300` at (600, 50).
- **Denylist column (x=330, chip width 120, height 20, 4px radius), one chip per field row (vertically centered on the row y):** rows 1–3 chips fill `rgba(42,120,214,0.15)`, border 1.5px `#2a78d6`, 11px `#2c3e50` centered text "logged"; rows 4–5 chips fill `rgba(0,131,0,0.12)`, border 1.5px `#008300`, text "`<redacted>`"; row 6 chip fill `rgba(231,76,60,0.18)`, border 2px `#e74c3c`, bold 11px `#e74c3c` text "LOGGED IN FULL".
- **Allowlist column (x=540, chip width 120, height 20, same radius):** rows 1–3 chips fill `rgba(42,120,214,0.15)`, border 1.5px `#2a78d6`, text "logged"; rows 4–6 chips fill `rgba(0,131,0,0.12)`, border 1.5px `#008300`, text "`<redacted>`".
- **Column rule:** 1px `#e5e9ef` vertical line at x=520 from y=40 to y=225.
- **Score line (bold 12px, at y=248):** "2 of 3 secrets redacted" in `#d95926` centered at (390, 248); "3 of 3 secrets redacted" in `#008300` centered at (600, 248).
- **Annotation (bold 13px `#e74c3c`, left-aligned at (20, 248)):** "the field nobody listed";  second line 12px `#6b7280` at (20, 268): "denylists must enumerate every bad field, forever".
- **Caption (12px `#444`, bottom right at (708, 294), right-aligned):** "field names are placeholders".

## "It's Only in Our Internal Logs"

**Tags:** `common mistake` (red), `blast radius` (orange)

- **The reassurance** — "the log is internal, the leak was accidental, nobody malicious ever read it"
- **Internal is large** — 120 engineers can search the central log; 8 people can reach the production database
- **Hand-check** — 120 ÷ 8 = **15×** more people, with a weaker approval path and no per-row audit trail
- **Not internal at all** — the third-party error tracker sits outside the company and holds full request context
- **Not access-controlled at all** — a screenshot in a ticket is readable by everyone the ticket is shared with
- **Accidental ≠ uncompromised** — you cannot prove no one read it, so the only sound assumption is that someone did
- **The response** — rotate the credential now; the prevention is never writing that field to a log at all

*Example (italic):* The team protected a database 8 people could reach, then wrote the same secret into a system 120 people can search and a vendor's service nobody in the room administers (populations illustrative).

**Common mistake:** Treating "internal" as a control. It is a population — here 15× the database's — plus a third party and an unbounded set of screenshots. A secret that entered a log is compromised; rotate it.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart of how many people can read the secret at each destination, with the production database as the reference bar.

- **Title (bold 15px, `#1a5276`, top center):** "How Many People Can Read It, by Destination".
- **Axis:** bars start at x=290; scale 1.5 px per person, so `barPx(n) = n * 1.5`; 2px `#999` vertical baseline at x=290 from y=45 to y=255; 12px `#444` axis caption "people who can read the secret" centered at (480, 288).
- **Rows (bar height 20px, top edges at y = 52, 88, 124, 160, 196, 232), each with a right-aligned 12px `#444` label ending at x=280 and a bold 12px value label in the bar's border color placed 8px past the bar's right end**, from the hardcoded array `[8, 12, 45, 60, 120, 200]`:
  1. "production database" — 8 → 12px, fill `rgba(0,131,0,0.55)`, border `#008300`, label "8"
  2. "host shell access" — 12 → 18px, fill `rgba(25,158,112,0.45)`, border `#199e70`, label "12"
  3. "alerting system" — 45 → 67.5px, fill `rgba(201,133,0,0.45)`, border `#c98500`, label "45"
  4. "third-party error tracker" — 60 → 90px, fill `rgba(74,58,167,0.40)`, border `#4a3aa7`, label "60 + vendor staff"
  5. "central log system" — 120 → 180px, fill `rgba(217,89,38,0.50)`, border `#d95926`, label "120"
  6. "ticket with a screenshot" — 200 → 300px, fill `rgba(213,81,129,0.45)`, border `#d55181`, label "200"
- **Reference line:** dashed (5/4) 1.5px `#008300` vertical line at x = 290 + 12 = 302 from y=45 to y=255; 11px `#008300` label "database = 8" placed at (306, 42), left-aligned.
- **Annotation (bold 13px `#d95926`, left-aligned at (300, 82)):** "120 ÷ 8 = 15× more readers than the database".
- **Second annotation (bold 12px `#4a3aa7`, left-aligned at (300, 154)):** "outside the company".
- **Caption (12px `#444`, bottom right at (708, 294), right-aligned):** "populations illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are hardcoded literal arrays — no `Math.random()`, no seeded draws needed. The invented figures are the seven destinations, the reader populations `[8, 12, 45, 60, 120, 200]`, and the retention windows (30-day hot tier, 400-day archive); each is labeled illustrative. Every derived figure must be recomputable by hand and must match the text to the digit: 400 − 30 = 370 days of post-incident archive copies; 120 ÷ 8 = 15× readers; the 6-field request splits 3 harmless / 3 sensitive, denylist redacts 2 of 3, allowlist redacts 3 of 3.
- **Scope boundary:** this page does **not** cover vaults, rotation mechanics, or committing secrets to source control — that is `24-secrets-management`. This page's failure mode occurs when secrets management is done correctly and the logging layer defeats it.
- **Framing:** defensive/educational — how a secret reaches a log and how to keep it out; no operational attack guidance, no real product, vendor, or company names, no invented brand names. People are Alice and Bob.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: every credential on this page is written as an obvious placeholder such as `<password-field>`, `<auth-header>`, or `<redacted>` — deliberately, so the page contains no realistic credential string and no secret scanner flags it."
