# Health Records Theft

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Health Records Theft

**Subtitle:** A stolen card is cancelled in a phone call — a stolen medical file describes a person for life, and a stranger's treatments can be written into the victim's own chart.

**Intro callout (crimson-left-border box):** The record has no reissue button: the same file that pays for a stranger's care can quietly rewrite the victim's blood type and allergies.

## 1. Why health files are prized

One medical file bundles more of a person than any single card.

- **Fact:** one file bundles identity, insurance, and diagnoses.
- **Fact:** billing and contact details ride along in the same file.
- **Fact:** a card can be cancelled; a diagnosis cannot.
- **Mechanism:** stolen files stay usable for years, not weeks.
- **Fact:** the long shelf life earns records a resale premium.
- **Scene:** clinics, insurers, labs, and app vendors hold copies.

**Key point (red-left-border box):** **Risk:** the file describes a person, not an account — there is no reissue button.

### Visualization (canvas `c1`, 720×300)

Shelf-life comparison: one horizontal bar per stolen item, bar length showing how long the item stays usable before it can be replaced.

- **Title (bold 16px `#880e4f`, top center, y=20):** "Shelf life of stolen data — cards get replaced, histories do not".
- **Rows** at y = 52, 98, 144, 190 (item text 14px `#2c3e50` left-aligned at x=40, baseline row y+16; bar = rect at x=220, row y, height 22, fill = bar color at 0.12 alpha, stroke = bar color width 2; note text 13px in bar color, left-aligned at bar end + 10, baseline row y+16):
  | Item | bar width | color | note |
  |---|---|---|---|
  | account password | 40 | #00695c | reset in minutes |
  | card number | 90 | #00695c | reissued in days |
  | insurance plan ID | 170 | #4527a0 | replaced in months |
  | medical history | 340 | #bf360c | cannot be reissued |
- **Bottom line (bold 14px `#4527a0`, centered, y=250):** "The longer a stolen item stays valid, the more it is worth."
- **Caption (bottom center, 13px `#999`, y=285):** "Replaceable items lose value at cancellation; a medical history never reaches that point."

## 2. What the thief does with it

The file lets a stranger receive care and coverage as someone else.

- **Mechanism:** Bob's care is billed to Alice's plan (medical identity theft).
- **Mechanism:** prescriptions get filled under Alice's coverage.
- **Risk:** Bob's treatment notes can enter Alice's chart.
- **Risk:** her chart can now show a wrong blood type or allergy.
- **Loss:** coverage limits drain against care she never received.
- **Risk:** exposed diagnoses can affect jobs and premiums.
- **Risk:** conditions she never chose to share become visible.

**Key point:** **Risk:** the polluted chart follows Alice into her own next visit.

### Visualization (canvas `c2`, 720×300)

Flow diagram: a stranger visits a clinic under borrowed coverage; the visit fans out into a bill for the insurer and a chart entry in the victim's record.

- **Title (bold 16px `#880e4f`, top center, y=20):** "One clinic visit under borrowed coverage — where the paperwork lands".
- **Boxes** 150×50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at box y+21; sub-line 12px `#666` centered at box y+38):
  | Title | sub-line | color | x | y |
  |---|---|---|---|---|
  | Bob (stranger) | walks in as "Alice" | #bf360c | 40 | 115 |
  | Clinic visit | care given, notes taken | #37474f | 270 | 115 |
  | Bill | sent to Alice's insurer | #4527a0 | 520 | 55 |
  | Chart entry | written into Alice's record | #bf360c | 520 | 175 |
- **Arrows:** horizontal width-1.5 arrow with filled triangular head in `#bf360c` from (190, 140) to (268, 140); diagonal width-1.5 arrows with filled heads — `#4527a0` from (420, 130) to (518, 85), `#bf360c` from (420, 150) to (518, 195).
- **Bottom line (bold 14px `#4527a0`, centered, y=262):** "Alice was never there — yet both the bill and the notes carry her name."
- **Caption (bottom center, 13px `#999`, y=285):** "A wrong blood type or allergy in those notes can misdirect Alice's own future care."

## 3. What limits the damage

Health paperwork rewards the same habits as bank paperwork.

- **Defense:** read insurance statements like bank statements.
- **Defense:** question visits and dates you do not recognize.
- **Defense:** treat a provider's breach notice as a task, not mail.
- **Defense:** ask for your own chart and read the entries.
- **Defense:** request corrections for entries that are not yours.
- **Defense:** providers verifying more than a name and birth date.

**Key point:** **Win:** an unfamiliar line caught early stops the chart pollution too.

### Visualization (canvas `c3`, 720×300)

Holder diagram: one record fans out to four holders, each keeping a full copy; the holder with the lightest safeguards sets the overall risk.

- **Title (bold 16px `#880e4f`, top center, y=20):** "Four holders, four full copies — the weakest one sets the risk".
- **Record label (bold 14px `#880e4f`, centered):** "Alice's health record" at (360, 52).
- **Fan-out links:** dashed `#999` width-1.5 lines (dash 5,4) from (360, 60) to the top center of each holder box: (115, 110), (290, 110), (465, 110), (640, 110).
- **Holder boxes** 150×64 at y=110 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at y=132; sub-line 1 "full copy" 12px `#666` centered at y=148; sub-line 2 12px in box color centered at y=164):
  | Title | sub-line 2 | color | x |
  |---|---|---|---|
  | Clinic | strong safeguards | #00695c | 40 |
  | Insurer | strong safeguards | #00695c | 215 |
  | Lab | mixed safeguards | #4527a0 | 390 |
  | App vendor | lightest safeguards | #bf360c | 565 |
- **Entry-point label (bold 13px `#bf360c`, centered):** "entry point" at (640, 192).
- **Bottom line (bold 14px `#4527a0`, centered, y=250):** "A copy is a copy — stealing from the lightest holder yields the full file."
- **Caption (bottom center, 13px `#999`, y=285):** "The record's safety equals the weakest safeguard among everyone holding it."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #c2185b`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also crimson-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#880e4f` magenta = mechanism/fact (Fact, Mechanism); `#00695c` teal = defense/win (Defense, Win); `#bf360c` red = risk/loss (Risk, Seen, Loss); `#4527a0` purple = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#880e4f`; h2 1.3rem `#880e4f`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #c2185b`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #bf360c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is drawn by a named function (`drawC1`..`drawC3`); a `renderAll()` runs once on load and again on window resize (debounced 150ms) so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary magenta `#880e4f`, teal `#00695c`, red `#bf360c`, purple `#4527a0`, plus `#c2185b`, `#37474f`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes. Each technical term (medical identity theft) appears at most once, in parentheses. Fictional people only (Alice, Bob); no real company names. No realistic credential strings or ID numbers anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
