# Ransomware & Data Extortion

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Ransomware & Data Extortion

**Subtitle:** Modern ransomware copies the data out before locking it — a backup restores the files, but nothing restores the secrecy of the stolen copy.

**Intro callout (blue-left-border box):** The lock is the visible half of the scheme; the quiet copy taken beforehand is the half that no payment, key, or backup can undo.

## 1. Copy first, lock second

The loud part of the scheme is the last part.

- **Mechanism:** intruders often sit inside a network for weeks.
- **Mechanism:** data flows out quietly before anything is locked.
- **Fact:** the quiet copy phase leaves little visible change.
- **Mechanism:** files are then scrambled and a demand appears (ransomware).
- **Scene:** tills, schedules, and records all freeze at once.
- **Risk:** by the time alarms ring, the copy is already out.

**Key point (red-left-border box):** **Risk:** detection usually starts at the lock — after the theft has already finished.

### Visualization (canvas `c1`, 720×300)

Two-phase timeline: a long quiet copy-out band, then a short loud lock band, with a "detection usually here" marker at the lock.

- **Title (bold 16px `#3e2723`, top center):** "Two phases: quiet copy-out, then the visible lock".
- **Phase bands** at y=60, height 80 (fill = band color at 0.12 alpha, stroke = band color width 2; phase title bold 14px in band color centered at y=85; two sub-lines 12px `#666` centered at y=105 and y=122):
  | Phase title | sub-line 1 | sub-line 2 | color | x | width |
  |---|---|---|---|---|---|
  | Quiet copy-out (weeks) | data flows out, screens look normal | operations keep running | #ef6c00 | 60 | 360 |
  | Lock + demand (hours) | files scrambled, note appears | operations freeze | #c62828 | 420 | 240 |
- **Time axis:** horizontal `#999` width-1.5 arrow from (60, 165) to (660, 165) with filled head; tick verticals `#999` from y=158 to y=172 at x=60 and x=420; tick labels 12px `#666` centered: "break-in" at (60, 188); "lock switch thrown" at (420, 188).
- **Detection marker:** filled `#c62828` circle radius 4 at (440, 165); vertical `#c62828` width-1.5 arrow from (440, 215) to (440, 173); label bold 13px `#c62828` centered "detection usually here" at (440, 232).
- **Bottom line (bold 14px `#ef6c00`, centered, y=265):** "The alarm rings at the lock — the copy left weeks earlier."
- **Caption (bottom center, 13px `#999`, y=285):** "Restoring files rewinds phase two; phase one has no rewind."

## 2. Two ransoms, one theft

One break-in produces two separate demands.

- **Mechanism:** payment one buys the key that unscrambles files.
- **Risk:** the key may not work, or only partly work.
- **Mechanism:** payment two buys a promise to delete the copy (double extortion).
- **Fact:** a promise from a thief cannot be verified, ever.
- **Risk:** leak sites publish samples to pressure the victim.
- **Risk:** customers and partners in the data become the lever.

**Key point:** **Risk:** the second payment buys only words — no test can confirm a deletion.

### Visualization (canvas `c2`, 720×300)

Two-ransom diagram: one victim box on the left with two payment arrows to two demand boxes — a testable unlock key and an unverifiable deletion promise.

- **Title (bold 16px `#3e2723`, top center):** "One theft, two separate payment demands".
- **Victim box** 160×60 at x=60, y=120, color `#3e2723` (fill = color at 0.12 alpha, stroke = color width 2): title "A retail company" bold 14px centered at (140, 144); sub-line 12px `#666` "systems locked, data taken" centered at (140, 162); pressure note 12px `#c62828` centered at (140, 205): "leak samples raise the pressure".
- **Demand boxes** 210×60 (same fill/stroke pattern; title bold 14px in box color centered at box y+24; sub-line 12px `#666` centered at box y+42):
  | Title | sub-line | color | x | y |
  |---|---|---|---|---|
  | Payment 1: unlock key | works or fails — testable | #c62828 | 460 | 55 |
  | Payment 2: deletion promise | cannot be verified, ever | #4527a0 | 460 | 185 |
- **Payment arrows** (width 2, filled heads): `#c62828` from (225, 135) to (455, 90), label bold 13px `#c62828` "pay to restore access" at (330, 95); `#4527a0` from (225, 170) to (455, 210), label bold 13px `#4527a0` "pay for a promise" at (330, 205).
- **Bottom line (bold 14px `#ef6c00`, centered, y=265):** "The key can be tested; the deletion promise never can."
- **Caption (bottom center, 13px `#999`, y=285):** "Paying twice still leaves the stolen copy in unknown hands."

## 3. What limits the damage

Different defenses reach different halves of the harm.

- **Defense:** offline backups make the lock survivable without paying.
- **Defense:** internal walls keep one entry local (network segmentation).
- **Defense:** alerts on unusual outbound volume can catch the copy.
- **Fact:** the stolen copy is permanent once it leaves.
- **Win:** prevention is the only cure for the copied data.
- **Fact:** disclosure duties apply whether or not a ransom is paid.

**Key point:** **Win:** backups turn the lock into downtime; only prevention addresses the copy.

### Visualization (canvas `c3`, 720×300)

Row comparison: what a restore from backup fixes versus what it cannot, one item per row with a check or cross icon and a status pill.

- **Title (bold 16px `#3e2723`, top center):** "What a backup restores — and what it cannot".
- **Rows** at y = 55, 100, 145, 190 (item text 14px `#2c3e50` left-aligned at x=100, baseline row y+22; status pill = rect at x=480, width 170, height 26 at row y+2, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered):
  | Item | pill text | color | icon |
  |---|---|---|---|
  | locked files on servers | restored | #2e7d32 | check |
  | halted daily operations | restarted | #2e7d32 | check |
  | the copy already taken | still out there | #c62828 | cross |
  | secrecy of customer records | gone for good | #c62828 | cross |
- **Check icon:** stroked `#2e7d32` width-2.5 polyline from (58, row y+14) to (65, row y+21) to (78, row y+6).
- **Cross icon:** stroked `#c62828` width-2.5 lines from (60, row y+6) to (76, row y+21) and from (76, row y+6) to (60, row y+21).
- **Bottom line (bold 14px `#3e2723`, centered, y=255):** "A backup rewinds the lock; nothing rewinds the copy."
- **Caption (bottom center, 13px `#999`, y=285):** "Prevention is the only defense that reaches the red rows."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #6d4c41`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#3e2723` blue = mechanism/fact (Fact, Mechanism); `#2e7d32` green = defense/win (Defense, Win); `#c62828` red = risk/loss (Risk, Seen); `#ef6c00` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#3e2723`; h2 1.3rem `#3e2723`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #6d4c41`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #c62828`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is a named `drawC1()`/`drawC2()`/`drawC3()` function; a `renderAll()` call runs them once and again on window resize (debounced 150ms) so canvases stay sharp after resize. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#3e2723`, green `#2e7d32`, red `#c62828`, orange `#ef6c00`, plus `#6d4c41`, `#4527a0`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the scheme's economics, no drama words, no attributed intent, no absolutes ("usually", not "always"). Strictly conceptual — no operational or technical how-to detail. Each technical term (ransomware, double extortion, network segmentation) appears at most once, in parentheses. Fictional naming only ("a retail company" style); no realistic credentials anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
