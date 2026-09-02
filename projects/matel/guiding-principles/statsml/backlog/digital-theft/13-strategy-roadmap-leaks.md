# Strategy & Roadmap Leaks

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Strategy & Roadmap Leaks

**Subtitle:** Release plans, pricing moves, and strategy decks lose their value the moment a rival reads them — this theft steals timing, not property.

**Intro callout (blue-left-border box):** Nothing goes missing and nothing breaks; the only loss is that a rival's calendar now matches yours.

## 1. Information whose value is timing

Some documents are valuable only while nobody else has read them.

- **Fact:** a launch date matters only while it is unknown.
- **Fact:** pricing moves and deal talks work by surprise.
- **Fact:** investment moves pay only if placed before others.
- **Mechanism:** an early reader gains time, not the file itself.
- **Fact:** the deck is near worthless the day after launch.
- **Risk:** strategy files are small and easy to forward.
- **Scene:** a deck drafted by five people reaches fifty.

**Key point (red-left-border box):** **Risk:** the theft steals timing, not property — the file left behind looks untouched.

### Visualization (canvas `c1`, 720×300)

Value-over-time line: a plateau of value before the announcement mark, dropping to near zero after it.

- **Title (bold 16px `#00838f`, top center):** "Value of a launch plan over time — the secret expires on stage".
- **Axes** (`#999`, lineWidth 1): y-axis from (60, 45) to (60, 235); x-axis from (60, 235) to (680, 235).
- **Axis labels** (13px `#666`): "value" left-aligned at (24, 40); "time" centered at (660, 252).
- **Pre-announcement fill:** `#00838f` at 0.10 alpha, rect (60, 70, 340, 165).
- **Value line** (`#00838f`, lineWidth 2.5) through the points: (60, 70) → (400, 70) → (415, 115) → (432, 190) → (450, 222) → (680, 226).
- **Region label** (bold 13px `#00838f`, centered): "value lives here" at (230, 150).
- **Announcement mark:** dashed `#c62828` width-1.5 vertical line from (400, 50) to (400, 235); label bold 13px `#c62828` "announcement" centered at (400, 44).
- **After label** (13px `#999`, centered): "near zero after" at (560, 210).
- **Bottom line (bold 14px `#e65100`, centered, y=268):** "The same file, one day apart — full value, then almost none."
- **Caption (bottom center, 13px `#999`, y=288):** "The document is unchanged; only its head start expired."

## 2. What a rival does with an early read

An early read turns your calendar into the rival's calendar.

- **Risk:** a counter-launch lands the same week as yours.
- **Risk:** pre-set pricing blunts your opening move.
- **Risk:** a bidder who knows your ceiling stops just under it.
- **Risk:** planned trades get traded against before placement.
- **Mechanism:** nothing breaks — deals just close worse than hoped.
- **Scene:** Alice's launch meets a rival ad campaign on day one.

**Key point:** **Risk:** the damage shows up as ordinary bad luck — deals and launches that quietly underperform.

### Visualization (canvas `c2`, 720×300)

Two-lane timeline: your planned launch on the top lane; the rival's counter-launch on the bottom lane, pulled earlier by a leak arrow.

- **Title (bold 16px `#00838f`, top center):** "One leak pulls the rival's counter-launch next to yours".
- **Lane labels** (bold 14px, left-aligned at x=40): "Your plan" in `#00838f` at y=104; "Rival" in `#c62828` at y=204.
- **Lane lines** (`#999`, lineWidth 1.5): top lane from (140, 100) to (660, 100); bottom lane from (140, 200) to (660, 200).
- **Top-lane markers:** filled `#00838f` circle radius 5 at (200, 100), label 12px `#666` "plan written" centered at (200, 122); filled `#558b2f` circle radius 6 at (540, 100), label bold 13px `#558b2f` "your launch" centered at (540, 80).
- **Leak arrow:** dashed `#c62828` width-1.5 arrow with filled head from (280, 107) to (280, 192); label bold 13px `#c62828` left-aligned at (295, 150): "leak — rival reads the plan".
- **Bottom-lane markers:** hollow circle (stroke `#999` width 2, radius 6) at (620, 200), label 12px `#999` "without the leak" centered at (648, 178); filled `#c62828` circle radius 6 at (555, 200), label bold 13px `#c62828` "counter-launch, same week" centered at (490, 178).
- **Pull arrow:** solid `#c62828` width-1.5 arrow with filled head from (612, 216) to (568, 216); label 12px `#c62828` "pulled earlier" centered at (590, 236).
- **Bottom line (bold 14px `#e65100`, centered, y=268):** "Your opening week arrives with company — the surprise is spent."
- **Caption (bottom center, 13px `#999`, y=288):** "Nothing was taken; the rival simply started earlier than you assumed."

## 3. What limits the damage

The defenses shrink the audience, mark the copies, and shorten the clock.

- **Defense:** the smallest reader circle until launch (need-to-know).
- **Defense:** each copy carries its recipient's mark (watermarking).
- **Win:** a leaked page then points back to one desk.
- **Defense:** big moves split so no file holds the whole plan.
- **Defense:** time-box secrecy — plan as if it leaks eventually.
- **Win:** acting first spends the secret before a reader can.
- **Fact:** a secret needed for weeks is easier than one for years.

**Key point:** **Win:** the goal is not a leak-proof plan — it is a leak that arrives late, traced, and incomplete.

### Visualization (canvas `c3`, 720×300)

Defense rows: one row per habit, each with a colored bullet dot and a status pill naming what the habit does.

- **Title (bold 16px `#00838f`, top center):** "Four habits that limit what an early reader gains".
- **Rows** at y = 58, 103, 148, 193 (bullet dot = filled circle radius 4 in pill color at (68, row y+15); item text 14px `#2c3e50` left-aligned at x=88, baseline row y+20; status pill = rect at x=500, width 150, height 26 at row y+2, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (575, row y+19)):
  | Item | pill text | color |
  |---|---|---|
  | smallest reader circle until the announcement | shrinks the read | #558b2f |
  | every copy carries its recipient's mark | traces the copy | #0097a7 |
  | the plan split across separate documents | shrinks the read | #558b2f |
  | act before the plan has time to travel | outruns the leak | #e65100 |
- **Bottom line (bold 14px `#00838f`, centered, y=255):** "Each habit shortens the window an early reader can use."
- **Caption (bottom center, 13px `#999`, y=285):** "The plan may still leak; these decide how late, how traced, and how partial."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #0097a7`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#00838f` blue = mechanism/fact (Fact, Mechanism); `#558b2f` green = defense/win (Defense, Win); `#c62828` red = risk/loss (Risk, Seen); `#e65100` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#00838f`; h2 1.3rem `#00838f`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #0097a7`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #c62828`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly; each chart is a named `drawC1()`/`drawC2()`/`drawC3()` function and a `renderAll()` runs them once at load and again on window resize (debounced ~150ms) so canvases stay sharp after resizing. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary blue `#00838f`, green `#558b2f`, red `#c62828`, orange `#e65100`, plus `#0097a7`, `#6a1b9a`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("near worthless", not "worthless"). Each technical term (need-to-know, watermarking) appears at most once, in parentheses. Fictional naming only (Alice); no real company names. No realistic credentials or secrets anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
