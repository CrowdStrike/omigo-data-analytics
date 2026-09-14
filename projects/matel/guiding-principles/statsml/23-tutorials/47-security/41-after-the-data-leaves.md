# After the Data Leaves

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** After the Data Leaves

**Subtitle:** Detection lag, not the break-in, decides how bad a breach gets — and the notification clock starts at discovery, not at entry

## One Compromised Account, Ninety Quiet Days

**Tags:** `core idea` (blue), `detection lag` (orange), `defensive` (green)

- **Day 0** — a company is entered through a single compromised account; nothing breaks and nothing alarms
- **The quiet part** — the intruder sits inside copying records out, a little every day, for the next 90 days
- **Day 90** — a payments partner calls: card numbers from the company's customers are being used for fraud
- **Dwell time** — the 90 days from entry to detection; damage accumulates the whole time, so it is the interval that matters
- **Containment** — day 90 to day 92, when the intruder finally loses access; short if rehearsed, long if the team is still guessing
- **Investigation** — day 92 to day 104, working out *what* was taken, which is harder than stopping the intruder
- **Notification** — the regulator hears by day 93, affected individuals by day 104; then remediation runs for months
- **The spine** — six named intervals, and only one of them (dwell time) is where the records actually left

*Example (italic):* Alice's card details were copied on day 12 and again on day 71, but nobody at the company knew a copy had been made until a partner phoned on day 90 (timeline illustrative).

**Key point:** Severity is mostly a function of time, not of the cleverness of the entry — an intruder present for 90 days takes far more than the same intruder present for 7, with identical skill.

### Visualization (canvas `c1`, 720×300)

Linear incident timeline, days 0 to 110, with each interval drawn as a labeled band and the 72-hour clock marked at discovery rather than at entry.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "One Incident, Six Intervals — The Clock Starts at Day 90".
- **Scale:** `ox = 55`, `pxPerDay = 635 / 110` (so day 0 at x=55, day 110 at x=690); band top y=120, band height 32.
- **Bands (fill then 2px border, drawn as plain rectangles):** days 0–90 fill `rgba(231,76,60,0.22)` border `#e74c3c`; days 90–92 fill `rgba(217,89,38,0.30)` border `#d95926`; days 92–104 fill `rgba(201,133,0,0.28)` border `#c98500`; days 104–110 fill `rgba(0,131,0,0.20)` border `#008300`.
- **Band labels:** bold 13px `#2c3e50` centered inside the dwell band: "dwell time — 90 days"; bold 11px `#d95926` "contain" rotated-free above the 90–92 band is *not* used — instead place 12px labels with leader lines as below.
- **Interval callouts (12px, with a 1px `#6b7280` leader line from the band to the label):** "containment 2 days" in `#d95926` at (600, 100) with leader from (591, 120) up to (596, 104); "investigation 12 days" in `#c98500` at (470, 196) left-aligned with leader from (568, 152) down to (568, 190); "remediation — long tail →" in `#008300` at (560, 232).
- **Day-0 marker:** 2px `#e74c3c` vertical tick at x=55 from y=112 to y=160; bold 12px `#e74c3c` left-aligned "day 0: initial access" at (55, 104).
- **Discovery marker:** 2px `#1a5276` vertical tick at x = 55 + 90·pxPerDay (≈574.5) from y=96 to y=160; bold 12px `#1a5276` right-aligned "day 90: discovered" at (570, 88).
- **Notification markers (12px, right-aligned, with 1px `#6b7280` ticks from y=152 to y=176):** "day 93: regulator notified (72 h)" in `#4a3aa7` at (690, 188) with tick at x = 55 + 93·pxPerDay; "day 104: individuals notified" in `#d55181` at (690, 208) with tick at x = 55 + 104·pxPerDay.
- **Axis:** 2px `#999` line at y=162 from x=55 to x=690; ticks and 12px `#444` labels "0", "30", "60", "90" at days 0/30/60/90, drawn at y=180 centered; 12px `#444` centered axis title "days since initial access" at (372, 258).
- **Annotation (bold 13px `#e74c3c`, left-aligned at (55, 282)):** "86.5% of the exposure window happened before anyone knew".
- **Caption (12px `#444`, bottom right at y=282):** "timeline illustrative".

## Seven Days or Ninety: The Same Intruder

**Tags:** `worked example` (blue), `linear in time` (orange), `notification clock` (violet)

- **The setup** — assume the intruder copies 8,000 records per day, undetected, at a steady rate
- **Scenario A** — detected on day 7: 8,000 × 7 = 56,000 records copied out before access was cut
- **Scenario B** — detected on day 90: 8,000 × 90 = 720,000 records, from the very same access
- **The ratio** — 720,000 ÷ 56,000 = 12.86, so 12.86× the loss with nothing about the attacker changed
- **What differed** — only detection speed; skill, tooling and entry vector were identical in both scenarios
- **The clock** — entry day 0, discovery day 90, regulator by day 93 (72 hours), individuals day 104
- **Where the window sits** — 90 of those 104 days are dwell time: 90 ÷ 104 = 0.8654, i.e. 86.5% of the exposure
- **The perverse look** — detecting fast brings the deadline sooner, which good practice and duty of care both override

*Example (italic):* Illustrative Example — a 12.86× difference in records lost, produced entirely by the defender's detection speed; the 8,000-per-day rate is invented and the arithmetic on top of it is exact.

**Key point:** Exfiltration is roughly linear in dwell time, so halving detection time halves the loss — money spent on detection buys more than money spent on a faster press release. This is a general pattern, not legal guidance; notification requirements vary by jurisdiction.

### Visualization (canvas `c2`, 720×300)

Two cumulative-exfiltration lines against days present, one stopping at day 7 and one running to day 90.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "Records Copied Grows With Dwell Time: 56,000 vs 720,000".
- **Axes:** `ox = 75`, `baseY = 245`, plot width 590, plot height 175; x = days 0→90 mapped across 590px; y = records 0→720,000 mapped across 175px.
- **Gridlines (`#e5e9ef`, 1px) at 180,000 / 360,000 / 540,000 / 720,000** with right-aligned 12px `#444` labels at `ox − 8` reading "180,000", "360,000", "540,000", "720,000"; x-axis 2px `#999` at y=245 with 12px `#444` centered tick labels "0", "30", "60", "90" at y=263.
- **Scenario B line (3px `#e74c3c`):** straight from (day 0, 0) to (day 90, 720,000); 5px `#e74c3c` filled dot at the day-90 end; bold 13px `#e74c3c` right-aligned label "720,000 records (detected day 90)" at (690, 62).
- **Scenario A line (3px `#008300`):** straight from (day 0, 0) to (day 7, 56,000) — it stops there because access was cut; 5px `#008300` dot at the end; bold 12px `#008300` left-aligned label "56,000 (detected day 7)" at (105, 222).
- **Both lines share the same slope** (8,000 records/day) — the only difference is where they stop, which is the whole point of the chart.
- **Dashed guide (1px `#6b7280`, dash 4/4):** vertical from the day-7 endpoint down to the axis, and vertical from the day-90 endpoint down to the axis.
- **Annotation (bold 13px `#4a3aa7`, centered at (372, 120)):** "720,000 ÷ 56,000 = 12.86× — same attacker, slower defender".
- **Axis title (12px `#444`, centered at (370, 285)):** "days present before detection".
- **Caption (12px `#444`, bottom right at y=285):** "8,000 records/day illustrative".

## You Cannot Investigate With Logs You Did Not Keep

**Tags:** `where it's used` (blue), `log retention` (orange), `rule of thumb` (green)

- **The gap** — logs were retained for 30 days, but the intruder had been present for 90 days
- **Unobservable window** — 90 − 30 = 60 days sit outside available evidence, with no record of what was read
- **As a share** — 60 ÷ 90 = 0.6667, so 66.7% of the intrusion cannot be examined at all, ever
- **The consequence** — scope cannot be bounded, so the company must notify on the worst-case assumption
- **The rule** — retention must exceed plausible dwell time, and that is decided long before any incident
- **Detection is usually external** — a partner, a researcher, a customer seeing fraud, or the data appearing for sale
- **Containment fights evidence** — pulling the plug stops the intruder and destroys the record of what was taken
- **Rotate anyway** — invalidate credentials even if the credential store looks untouched; incomplete logs cannot prove a negative

*Example (italic):* Illustrative Example — the team can say exactly what happened on days 60 to 90 and nothing at all about days 0 to 60, so the notice has to cover every record the account could reach.

**Key point:** Log retention shorter than plausible dwell time makes accurate disclosure impossible — and "we could not determine what was accessed" forces the broadest, most alarming notice you could have sent.

### Visualization (canvas `c3`, 720×300)

Two aligned horizontal bars on a shared day axis: the 90-day intrusion above, the 30 days of retained logs below, with the 60-day gap hatched as un-investigable.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "30 Days of Logs, 90 Days of Intrusion — 60 Days Unobservable".
- **Scale:** bars start at x=170, 90 days = 450px (5px per day), so the bar span is x=170 (day 0) to x=620 (day 90, the discovery point).
- **Row 1 (y=80, height 30), right-aligned 12px `#444` label "intrusion (dwell time)" ending at x=160:** full-width bar x=170 w=450, fill `rgba(231,76,60,0.22)`, 2px `#e74c3c` border; bold 12px `#e74c3c` centered inside: "90 days present".
- **Row 2 (y=150, height 30), right-aligned 12px `#444` label "logs available" ending at x=160:** green segment covering days 60–90 at x=470 w=150, fill `rgba(0,131,0,0.30)`, 2px `#008300` border, bold 12px `#008300` centered inside "30 days"; grey-red gap covering days 0–60 at x=170 w=300, fill `rgba(107,114,128,0.10)`, 2px dashed (dash 5/4) `#e74c3c` border, hatched with 1px `rgba(231,76,60,0.35)` diagonal lines every 10px, and bold 12px `#e74c3c` centered inside: "no logs — 60 days".
- **Day axis:** 2px `#999` line at y=200 from x=170 to x=620; ticks and 12px `#444` centered labels "day 0", "day 30", "day 60", "day 90" at y=218 (x = 170, 320, 470, 620).
- **Discovery marker:** 2px `#1a5276` vertical line at x=620 from y=70 to y=200; bold 12px `#1a5276` right-aligned "discovered" at (616, 64).
- **Annotation (bold 13px `#e74c3c`, centered at (395, 252)):** "60 ÷ 90 = 66.7% of the intrusion cannot be investigated".
- **Second annotation (12px `#4a3aa7`, centered at (395, 272)):** "scope unbounded → notify on the worst case".
- **Caption (12px `#444`, bottom right at y=292):** "retention and dwell figures illustrative".

## "How Did They Get In?" Is the Second Question

**Tags:** `common mistake` (red), `what to tell users` (magenta), `what to do` (green)

- **The pull** — the entry vector is the most interesting question, so the review spends its time there
- **The mismatch** — the entry took one day; the outcome was set by the 90 days in which nobody noticed
- **The arithmetic** — 90 of 104 days, 86.5%, of the exposure window ran before the first alarm sounded
- **The other error** — treating disclosure as a legal formality instead of the mechanism that lets people act
- **Vague notice** — "some information may have been accessed" leaves a reader with no proportionate response
- **Specific notice** — naming the fields lets a reader change a reused password, watch a card, freeze credit
- **Rehearse it** — an incident is the wrong moment to discover who can revoke a production credential
- **Fix the lag first** — closing one entry vector while detection stays at 90 days leaves severity unchanged

*Example (italic):* Illustrative Example — Bob reads that "an unauthorised party may have accessed some account information" and can do nothing; had the notice named the reused password and the stored card, he had two clear actions.

**Common mistake:** Optimising the post-mortem around the break-in. Ask "how long until we would have noticed?" first — and write notices that name the affected fields, because a reader who knows what leaked can respond and one who does not cannot.

### Visualization (canvas `c4`, 720×300)

A single stacked exposure bar splitting the 104 days at discovery, above two side-by-side notice panels contrasting vague and specific wording by the actions each enables.

- **Title (bold 15px, `#1a5276`, top center at y=24):** "86.5% of the Exposure Window Was Before Anyone Knew".
- **Stacked bar (x=60 to x=660, total 600px = 104 days, y=46, height 34):** left segment x=60 w=519 (90 days) fill `rgba(231,76,60,0.22)`, 2px `#e74c3c` border, bold 12px `#e74c3c` centered "before detection — 90 days (86.5%)"; right segment x=579 w=81 (14 days) fill `rgba(0,131,0,0.22)`, 2px `#008300` border, bold 11px `#008300` centered "after — 14 days (13.5%)".
- **Under-bar note (12px `#6b7280`, left-aligned at (60, 98)):** "90 ÷ 104 = 86.5%,  14 ÷ 104 = 13.5%".
- **Notice panel A (rounded 8px box x=45, y=118, 300×132, fill `rgba(107,114,128,0.08)`, 2px `#6b7280` border):** bold 13px `#6b7280` centered header "vague notice" at (195, 140); 12px `#2c3e50` centered lines at y=164 and y=182: "\"some information may" / "have been accessed\""; bold 12px `#e74c3c` centered at (195, 214): "actions the reader can take: 0"; 11px `#6b7280` centered at (195, 234): "cannot respond proportionately".
- **Notice panel B (rounded 8px box x=375, y=118, 300×132, fill `rgba(0,131,0,0.08)`, 2px `#008300` border):** bold 13px `#008300` centered header "specific notice" at (525, 140); 12px `#2c3e50` centered lines at y=164 and y=182: "\"your stored card and your" / "account password were copied\""; bold 12px `#008300` centered at (525, 214): "actions the reader can take: 2"; 11px `#6b7280` centered at (525, 234): "replace the card, change the reuse".
- **Annotation (bold 13px `#4a3aa7`, centered at (360, 272)):** "disclosure is a control, not a formality".
- **Caption (12px `#444`, bottom right at y=292):** "wording and counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`, violet `rgba(74,58,167,0.13)`/`#4a3aa7`, magenta `rgba(213,81,129,0.13)`/`#d55181`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is used only for genuine alarm states: the dwell-time interval, the unobservable log window, and the zero-action notice.
- **Data:** every value is a hardcoded literal — no `Math.random()`, no seeded generator, nothing regenerated per render. Invented and labeled illustrative: the 8,000-records-per-day rate, the 7-day and 90-day detection points, the 2-day containment, the 12-day investigation, the 30-day log retention, and the notice action counts. All arithmetic on top of them is exact and text numbers match chart numbers to the digit: 8,000 × 7 = 56,000; 8,000 × 90 = 720,000; 720,000 ÷ 56,000 = 12.857… ≈ 12.86; 90 ÷ 104 = 0.86538… ≈ 86.5%; 14 ÷ 104 = 0.13461… ≈ 13.5% (86.5% + 13.5% = 100%); 90 − 30 = 60; 60 ÷ 90 = 0.6667 ≈ 66.7%. Pixel widths close too: 519 + 81 = 600 for the 104-day stacked bar, and 300 + 150 = 450 for the 90-day retention row.
- **Scope:** this page owns the defender's timeline — detection lag, containment, investigation, notification. It deliberately does not re-teach what happens to stolen credentials downstream (cracking, combolists, replay at other sites), which belongs to the breach-to-combolist page.
- **Regulatory framing:** no real company, breach, regulator, or statute is named. Notification timing is described generically as "many regulatory regimes commonly require notification within 72 hours of discovery", presented as a general pattern with an explicit note that it is not legal guidance and requirements vary by jurisdiction.
- **Credential hygiene:** no sample credential strings, tokens, or `key=value` credential syntax; affected data is described by field name only ("a stored card", "an account password"). People are Alice/Bob; the organisation is "a company".
- **Framing:** defensive/educational — the page explains why detection speed dominates breach severity and what preparation (log retention, rehearsed revocation, notice wording) has to exist beforehand; no operational attack guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
