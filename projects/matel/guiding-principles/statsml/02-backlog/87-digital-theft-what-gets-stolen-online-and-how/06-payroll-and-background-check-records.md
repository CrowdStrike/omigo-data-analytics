# Payroll & Background-Check Records

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Payroll & Background-Check Records

**Subtitle:** HR systems concentrate the most private facts an employer holds — salaries, bank details, background checks, and personal disclosures — so one stolen folder describes an entire workforce.

**Intro callout (blue-left-border box):** An HR folder is one of the few places where pay, bank routing, legal history, and personal disclosures sit side by side — for every employee at once.

## 1. What one HR folder holds

Each record type alone is private; the folder stacks them all.

- **Fact:** salary and bonus history sits there for every employee.
- **Fact:** bank details that route each paycheck (direct deposit).
- **Fact:** background checks list old addresses and court records.
- **Fact:** past employers and references gathered during hiring.
- **Fact:** private notes — visa status, health accommodations.
- **Scene:** Alice's file spans pay, home moves, and health notes.

**Key point (red-left-border box):** **Risk:** the folder describes the whole workforce, not one person — a single theft covers everyone.

### Visualization (canvas `c1`, 720×300)

Fan-out schematic: one folder icon on the left, lines fanning to rows of record types, each row with a sensitivity pill.

- **Title (bold 16px `#37474f`, top center):** "One folder fans out into every kind of private record".
- **Folder icon:** tab = stroked rect 30×10 at (60, 118); body = rect 80×54 at (60, 128), fill `#37474f` at 0.12 alpha, stroke `#37474f` width 2; label "HR folder" bold 14px `#37474f` centered at (100, 150); sub-line "full workforce" 12px `#666` centered at (100, 168).
- **Fan lines:** solid `#999` width 1.5 from (140, 155) to (240, row y+9) for each row.
- **Rows** at y = 52, 88, 124, 160, 196 (item text 14px `#2c3e50` left-aligned at x=250, baseline row y+16; pill = rect at x=560, width 110, height 24 at row y, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (615, row y+16)):
  | Item | pill text | color |
  |---|---|---|
  | Salary and bonus history | money | #c62828 |
  | Bank details for each paycheck | money | #c62828 |
  | Addresses and court records | history | #4527a0 |
  | Past employers and references | history | #4527a0 |
  | Visa status, health notes | personal | #ef6c00 |
- **Bottom line (bold 14px `#ef6c00`, centered, y=262):** "One folder, every employee — the fan covers the whole workforce."
- **Caption (bottom center, 13px `#999`, y=285):** "Each row alone is private; together they describe a person fully."

## 2. How it gets taken and used

Copies of the folder live in more places than the HR office.

- **Fact:** payroll vendors hold a copy outside the employer.
- **Fact:** screening firms keep the files they compiled.
- **Mechanism:** a note posing as Bob asks to reroute his pay (payroll diversion).
- **Risk:** the next paycheck lands wherever the record points.
- **Risk:** a salary list makes fake bonus notices believable.
- **Risk:** background files surface facts never made public.

**Key point:** **Risk:** the reroute needs no break-in — one convincing request to a busy inbox is enough.

### Visualization (canvas `c2`, 720×300)

Flow diagram: a fake "update my bank details" request enters the payroll system, and on payday the paycheck detours away from the real account.

- **Title (bold 16px `#37474f`, top center):** "One convincing request reroutes a paycheck".
- **Top-row boxes** 170×55 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+22; sub-line 12px `#666` centered at box y+40):
  | Title | sub-line | color | x | y |
  |---|---|---|---|---|
  | Request | poses as Bob, new account | #c62828 | 30 | 70 |
  | Payroll system | record updated as asked | #37474f | 275 | 70 |
  | Payday run | pays whatever is on file | #546e7a | 520 | 70 |
- **Top-row arrows:** width-1.5 horizontal arrows with filled triangular heads; `#c62828` from (200, 97) to (273, 97); `#37474f` from (445, 97) to (518, 97).
- **Outcome boxes** 170×50 (same fill/stroke pattern; title bold 14px at box y+21, except titles longer than 20 characters — "Account from the request" — drawn at bold 13px so they fit the 170px box; sub-line 12px `#666` at box y+38):
  | Title | sub-line | color | x | y |
  |---|---|---|---|---|
  | Bob's real account | no longer on file | #1b5e20 | 150 | 190 |
  | Account from the request | next paycheck lands here | #c62828 | 520 | 190 |
- **Expected link:** dashed `#999` width-1.5 line (dash [5,4]) from (545, 125) to (250, 188); label "expected" 13px `#999` at (380, 150).
- **Actual link:** solid `#c62828` width-2 arrow with filled head from (605, 125) to (605, 188); label "actual" bold 13px `#c62828` at (640, 160).
- **Bottom line (bold 14px `#ef6c00`, centered, y=268):** "The payday run worked perfectly — the record it read was the lie."
- **Caption (bottom center, 13px `#999`, y=288):** "No system broke; one field changed on one convincing request."

## 3. What limits the damage

Each control removes one link the reroute or the bulk theft needs.

- **Defense:** bank changes confirmed on a second channel (call-back).
- **Defense:** the number called is on file, not in the request.
- **Defense:** access limited to the few roles that truly need it.
- **Defense:** screening vendors held to the employer's own bar.
- **Defense:** alerts fire when someone pulls records in bulk.
- **Fact:** fewer stored copies means fewer doors to guard.

**Key point:** **Win:** a two-minute call-back on a known number defeats the paycheck reroute outright.

### Visualization (canvas `c3`, 720×300)

Copies diagram: employer, payroll vendor, and screening firm each hold a full copy of the folder; the weakest guard sets the risk.

- **Title (bold 16px `#37474f`, top center):** "Three full copies — the weakest guard sets the risk".
- **Holder boxes** 190×110 at y=55, x = 35 / 265 / 495 (centers cx = 130 / 360 / 590; fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at (cx, 77); sub-line 12px `#666` centered at (cx, 94)):
  | Title | sub-line | color | pill text | pill color |
  |---|---|---|---|---|
  | Employer | runs the HR system | #37474f | guarded | #1b5e20 |
  | Payroll vendor | Vendor A — pays wages | #546e7a | guarded | #1b5e20 |
  | Screening firm | Vendor B — ran the checks | #c62828 | weak guard | #c62828 |
- **Mini folder icon per box:** tab = stroked rect 12×6 at (cx-17, 102); body = stroked rect 34×24 at (cx-17, 108); stroke = box color, width 1.5.
- **Pills** 130×24 at (cx-65, 140): fill = pill color at 0.12 alpha, stroke = pill color width 2, text bold 13px in pill color centered at (cx, 156).
- **Connectors:** dashed `#999` width-1.5 lines (dash [5,4]) from (225, 110) to (265, 110) and from (455, 110) to (495, 110); labels "same folder" 12px `#999` centered at (245, 102) and (475, 102).
- **Risk arrow:** solid `#c62828` width-2 vertical arrow with filled head from (590, 235) to (590, 172); label "risk enters at the weakest copy" bold 13px `#c62828` centered at (590, 252).
- **Bottom line (bold 14px `#37474f`, centered, y=272):** "Guarding one copy well does not guard the folder — every copy counts."
- **Caption (bottom center, 13px `#999`, y=290):** "The vendors' guard, not the office's, often decides the outcome."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #546e7a`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#37474f` blue = mechanism/fact (Fact, Mechanism); `#1b5e20` green = defense/win (Defense, Win); `#c62828` red = risk/loss (Risk, Seen); `#ef6c00` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#37474f`; h2 1.3rem `#37474f`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #546e7a`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #c62828`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each canvas is drawn by a named function (`drawC1`, `drawC2`, `drawC3`); a `renderAll()` runs them on load and again on window resize (debounced 150 ms) so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#37474f`, green `#1b5e20`, red `#c62828`, orange `#ef6c00`, plus `#546e7a`, `#4527a0`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes. Fictional naming only: Alice/Bob for people, "Vendor A"/"Vendor B" for firms. Each technical term (direct deposit, payroll diversion, call-back) appears at most once, in parentheses. No realistic credentials, account numbers, or ID numbers anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
