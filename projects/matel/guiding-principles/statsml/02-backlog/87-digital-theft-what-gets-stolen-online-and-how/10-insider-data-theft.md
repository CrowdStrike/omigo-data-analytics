# Insider Data Theft

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Insider Data Theft

**Subtitle:** A departing employee downloads the customer list on the way out — logged in, permitted, and gone; the theft uses the front door, not a break-in.

**Intro callout (purple-left-border box):** The hardest copy to spot is the one made of allowed actions — an insider's download looks exactly like the work the access was granted for (insider threat).

## 1. The front door, not a break-in

The insider already holds a key the building handed out.

- **Fact:** the employee's login already opens every needed door.
- **Mechanism:** each copy is an action the system was built to allow.
- **Mechanism:** logs record the downloads as ordinary work.
- **Fact:** no alarm separates the last copy from daily duties.
- **Risk:** a resignation changes the motive, not the access.
- **Scene:** Alice exports the customer list in her final week.

**Key point (red-left-border box):** **Risk:** the theft looks identical to the job — permitted, logged, and unremarkable.

### Visualization (canvas `c1`, 720×300)

Two-door schematic: a blocked, alarmed break-in door on the left and an open, logged-in front door on the right; the actor walks the second.

- **Title (bold 16px `#6a1b9a`, top center):** "Two ways in — the alarm only watches one".
- **Door labels** (bold 14px, centered at y=48): "Break-in" in `#c62828` at (170, 48); "Front door" in `#2e7d32` at (550, 48).
- **Door rects** 100×120 at y=55 (fill = door color at 0.12 alpha, stroke = door color width 2): break-in door at x=120 (`#c62828`); front door at x=500 (`#2e7d32`).
- **Break-in door interior:** two diagonal `#c62828` width-1.5 lines corner to corner (an X): (120,55)-(220,175) and (220,55)-(120,175).
- **Alarm arcs:** three upward `#c62828` width-1.5 arcs, radii 12/20/28, centered (170, 55), from 1.25π to 1.75π.
- **Break-in sub-line** (12px `#666`, centered): "blocked, alarmed" at (170, 190).
- **Front-door open leaf:** `#2e7d32` width-1.5 stroked path (500,55) → (472,70) → (472,160) → (500,175); filled `#2e7d32` doorknob circle radius 2.5 at (478, 118).
- **Front-door sub-line** (12px `#666`, centered): "open, logged in" at (550, 190).
- **Actor box** 220×50 at x=250, y=200, color `#8e24aa` (fill 0.12 alpha, stroke width 2): title "Alice, departing employee" bold 14px centered at (360, 221); sub-line "holds a valid login" 12px `#666` at (360, 238).
- **Links:** dashed `#999` width-1.5 line from (300, 200) to (190, 178), label "never tried" 13px `#999` at (225, 192); solid `#c62828` width-2 arrow with filled head from (420, 200) to (530, 178), label "walks through" bold 13px `#c62828` at (455, 170).
- **Bottom line (bold 14px `#e65100`, centered, y=268):** "No forced lock, no alarm — the copy rides permissions already granted."
- **Caption (bottom center, 13px `#999`, y=286):** "Departure changed the motive; the access stayed exactly the same."

## 2. The final-weeks pattern

The copying tends to cluster in the weeks before a goodbye.

- **Seen:** download volume climbs quietly before a resignation.
- **Seen:** customer lists, price sheets, and designs move out.
- **Mechanism:** personal drives and email carry the copies out.
- **Risk:** the copy resurfaces later at a competitor (Vendor A).
- **Risk:** a new venture can open its doors on the departed file.
- **Fact:** the employer often learns from lost deals, not logs.

**Key point:** **Risk:** by the time a lost deal points back, the copy has been out for months.

### Visualization (canvas `c2`, 720×300)

Weekly bar series for one account: six ordinary weeks, then a sharp rise in the last two bars before departure.

- **Title (bold 16px `#6a1b9a`, top center):** "Weekly downloads by one account — the last two weeks stand out".
- **Axes:** `#999` width-1 lines, y-axis (70,45)→(70,240), x-axis (70,240)→(660,240).
- **Bars:** width 50, base y=240, at x = 90, 160, 230, 300, 370, 440, 510, 580; values (files) = 12, 10, 14, 11, 13, 12, 46, 68; height = value × 2.5 px. First six bars fill `rgba(106,27,154,0.35)` stroke `#6a1b9a` width 1; last two bars fill `rgba(198,40,40,0.35)` stroke `#c62828` width 2.
- **Value labels:** 12px centered above each bar at bar-top − 6; `#6a1b9a` for the first six, bold `#c62828` for the last two.
- **Week labels** (12px `#666`, centered at y=256): "W1" … "W8" under the bar centers.
- **Baseline:** dashed `#2e7d32` [5,4] width-1 line at y=210 from x=80 to x=650; label "own baseline (~12/wk)" 12px `#2e7d32` left-aligned at (82, 203).
- **Final-weeks marker:** bold 13px `#c62828` centered label "final weeks" at (550, 44); thin `#c62828` width-1 bracket line from (510, 50) to (630, 50).
- **Bottom line (bold 14px `#e65100`, centered, y=272):** "The spike is only visible against the account's own history."
- **Caption (bottom center, 13px `#999`, y=289):** "Each bar is permitted activity; only the change in volume tells a story."

## 3. What limits the damage

Each control shrinks what a departing login can quietly take.

- **Defense:** access reviews trim rights a role no longer needs.
- **Defense:** an alert flags a spike over one's own usual volume.
- **Defense:** a leaver checklist cuts access the day notice lands.
- **Defense:** agreements make later use of the copy legally costly.
- **Fact:** no single control sees the whole pattern by itself.
- **Scene:** Bob's bulk export trips the alert before the file leaves.

**Key point:** **Win:** layered controls turn a quiet exit copy into a visible, costly event.

### Visualization (canvas `c3`, 720×300)

Defense-row diagram: one row per control, each with a short description and a reduces/blocks status pill.

- **Title (bold 16px `#6a1b9a`, top center):** "Four controls — what each one does to the exit copy".
- **Rows** at y = 48, 98, 148, 198 (control name bold 14px `#2e7d32` left-aligned at x=40, baseline y+14; description 13px `#666` left-aligned at x=40, baseline y+32; status pill = rect at x=540, width 140, height 26 at y+6, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (610, y+23)):
  | Control | description | pill text | pill color |
  |---|---|---|---|
  | Access review | rights trimmed when the role changes | reduces | #e65100 |
  | Volume alert | spike over one's own baseline gets flagged | reduces | #e65100 |
  | Leaver checklist | access cut the day notice is given | blocks | #2e7d32 |
  | Legal agreement | using the copy later carries a cost | reduces | #e65100 |
- **Bottom line (bold 14px `#6a1b9a`, centered, y=262):** "One control blocks; the others shrink the window and raise the price."
- **Caption (bottom center, 13px `#999`, y=285):** "Layered together, the quiet exit copy becomes visible and costly."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #8e24aa`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also purple-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#6a1b9a` purple = mechanism/fact (Fact, Mechanism); `#2e7d32` green = defense/win (Defense, Win); `#c62828` red = risk/loss (Risk, Seen); `#e65100` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#6a1b9a`; h2 1.3rem `#6a1b9a`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #8e24aa`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #c62828`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All chart data is literal hardcoded coordinates — no randomness, no dates. Drawing code lives in named functions `drawC1`/`drawC2`/`drawC3` plus a debounced (150ms) render-on-resize (`renderAll`) so canvases stay sharp.
- **Palette:** primary purple `#6a1b9a`, green `#2e7d32`, red `#c62828`, orange `#e65100`, plus `#8e24aa`, `#37474f`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("often", not "always"). Each technical term (insider threat) appears at most once, in parentheses. Fictional people only (Alice, Bob) and "Vendor A"-style companies; no realistic credentials anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
