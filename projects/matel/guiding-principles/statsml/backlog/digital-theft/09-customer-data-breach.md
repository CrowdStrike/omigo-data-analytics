# Customer Data Breach

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Customer Data Breach

**Subtitle:** One copied database exposes millions of people at once — digital theft scales with the size of the container, not the effort of the thief.

**Intro callout (teal-left-border box):** Nothing is broken and nothing goes missing; a copy leaves through one door, and everyone whose record sat behind that door is exposed together.

## 1. Theft that scales with the container

Copying a database costs about the same at any size.

- **Scene:** a retail company keeps every customer in one table.
- **Fact:** copying ten million rows costs barely more than one.
- **Fact:** the original stays in place, so nothing looks missing.
- **Mechanism:** one weak entry point sits in front of every record.
- **Mechanism:** centralizing data concentrates value and risk together.
- **Fact:** a physical thief carries loot; a copy weighs nothing.

**Key point (deep-orange-left-border box):** **Risk:** the container sets the loss — one door, and everything behind it leaves at once.

### Visualization (canvas `c1`, 720×300)

Two bar groups: effort to copy (nearly equal bars) beside value exposed (hugely unequal bars).

- **Title (bold 16px `#006064`, top center):** "Effort to copy vs value exposed — one record or ten million".
- **Baseline:** `#ccc` width-1 line from (60, 225) to (660, 225).
- **Group headers** (bold 14px, centered): "Effort to copy" in `#00838f` at (200, 40); "Value exposed" in `#d84315` at (520, 40).
- **Bars** (fill = bar color at 0.35 alpha, stroke = bar color width 2, all width 80, bottoms on the baseline):
  | Group | bar | x | top y | height | color |
  |---|---|---|---|---|---|
  | effort | 1 record | 110 | 165 | 60 | #00838f |
  | effort | 10 million | 210 | 158 | 67 | #00838f |
  | value | 1 record | 430 | 219 | 6 | #d84315 |
  | value | 10 million | 530 | 64 | 161 | #d84315 |
- **Under-bar labels** (13px `#666`, centered, y=242): "1 record" at x=150 and x=470; "10 million" at x=250 and x=570.
- **Over-bar notes** (12px `#666`, centered): "minutes" at (150, 158); "still minutes" at (250, 151); "one person" at (470, 212); "millions of people" at (570, 56).
- **Bottom line (bold 14px `#8e24aa`, centered, y=265):** "The copy costs about the same — the exposure scales with the container."
- **Caption (bottom center, 13px `#999`, y=285):** "Nothing goes missing; the original stays exactly where it was."

## 2. From copy to consequence

The copy starts moving long before anyone notices.

- **Mechanism:** stolen sets are resold and passed along quietly.
- **Mechanism:** buyers merge new sets with older leaked ones.
- **Risk:** merged sets fill in the blanks person by person.
- **Risk:** a fuller profile enables convincing impersonation.
- **Loss:** customers bear the fallout; the company held the data.
- **Scene:** Alice learns of the copy from a letter months later.

**Key point:** **Risk:** the harm lands on people who never chose how their records were stored.

### Visualization (canvas `c2`, 720×300)

Left-to-right flow: copied set → resale → merge → fuller profile, with two older leaks feeding the merge step from below.

- **Title (bold 16px `#006064`, top center):** "From one copy to fuller profiles, step by step".
- **Main boxes** 140×54 at y=70 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at y=92; sub-line 12px `#666` centered at y=110):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Copied set | leaves in one transfer | #d84315 | 30 |
  | Resale | changes hands quietly | #5d4037 | 205 |
  | Merge | joined with older sets | #8e24aa | 380 |
  | Fuller profile | blanks filled in | #d84315 | 555 |
- **Main arrows:** width-1.5 horizontal `#999` arrows with filled triangular heads at y=97: from (170, 97) to (203, 97); from (345, 97) to (378, 97); from (520, 97) to (553, 97).
- **Feeder boxes** 130×40 at y=165 (same fill/stroke pattern; title bold 13px in box color centered at y=182; sub-line 12px `#666` centered at y=197):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Older leak A | emails, cities | #999 | 310 |
  | Older leak B | phone numbers | #999 | 460 |
- **Feeder arrows:** width-1.5 `#999` arrows with filled heads from (375, 165) to (430, 127) and from (525, 165) to (470, 127).
- **Bottom line (bold 14px `#8e24aa`, centered, y=262):** "Disclosure letters arrive long after the copy has traveled."
- **Caption (bottom center, 13px `#999`, y=285):** "Each merge fills more blanks; the person notices only at the end."

## 3. What limits the damage

No single measure works; layers shrink what a copy is worth.

- **Defense:** keep only what the business needs (data minimization).
- **Defense:** scrambled storage makes the copy unreadable (encryption).
- **Defense:** one door opens one room, not the building (segmentation).
- **Fact:** fewer stored fields mean less to lose in any copy.
- **Defense:** for customers, assume exposure has already happened.
- **Defense:** a unique password per account contains the spread.

**Key point:** **Win:** each layer cuts what one successful copy is worth — smaller haul, unreadable contents, one room only.

### Visualization (canvas `c3`, 720×300)

Defense rows grouped by side, each with a colored effect pill (reduces / blocks / contains).

- **Title (bold 16px `#006064`, top center):** "Layers of defense: what each one does to a stolen copy".
- **Group headers** (bold 14px `#006064`, left-aligned at x=40): "Company side" at y=45; "Customer side" at y=185.
- **Rows** at y = 55, 95, 135 (company) and y = 195 (customer). Per row: filled circle radius 5 in pill color at (70, y+12); item text 14px `#2c3e50` left-aligned at x=90, baseline y+17; effect pill = rect at x=520, width 130, height 24 at row y, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (585, y+16):
  | Row y | Item | pill text | color |
  |---|---|---|---|
  | 55 | keep only the fields the business truly needs | reduces | #00838f |
  | 95 | store records scrambled, so the copy is gibberish | blocks | #33691e |
  | 135 | split systems so one opened door reaches one room | reduces | #00838f |
  | 195 | a different password on every account | contains | #5d4037 |
- **Bottom line (bold 14px `#33691e`, centered, y=258):** "Each layer shrinks what one successful copy is worth."
- **Caption (bottom center, 13px `#999`, y=285):** "No single measure covers everything — the layers multiply."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #00838f`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also teal-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#006064` teal = mechanism/fact (Fact, Mechanism); `#33691e` green = defense/win (Defense, Win); `#d84315` deep orange = risk/loss (Risk, Loss); `#8e24aa` purple = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#006064`; h2 1.3rem `#006064`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #00838f`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #d84315`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each canvas is drawn by a named function (`drawC1`–`drawC3`) and a debounced (150ms) resize listener calls `renderAll()` so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary teal `#006064`, green `#33691e`, deep orange `#d84315`, purple `#8e24aa`, plus `#00838f`, `#5d4037`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("about the same", not "identical"). Each technical term (data minimization, encryption, segmentation) appears at most once, in parentheses. Fictional naming only (Alice, "a retail company"); no real company names. No realistic credentials anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
