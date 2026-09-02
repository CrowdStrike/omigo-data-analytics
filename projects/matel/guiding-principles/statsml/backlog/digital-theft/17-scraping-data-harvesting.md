# Scraping & Data Harvesting

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Scraping & Data Harvesting

**Subtitle:** Each profile is public on its own — a million of them gathered into one file becomes a searchable product nobody agreed to.

**Intro callout (green-left-border box):** Nothing is broken into and no password is stolen; the collector only reads what anyone could read — the change happens when the reading is total.

## 1. Public one at a time

Profiles, listings, and posts are readable by anyone, by design.

- **Fact:** profiles and listings are readable by anyone, by design.
- **Mechanism:** a program reads pages the way a browser does (scraper).
- **Mechanism:** it never tires, never sleeps, never skips a page.
- **Fact:** what a person reads in years, it reads in days.
- **Fact:** each page view looks like any ordinary visit.
- **Scene:** Alice's profile logs one more view among thousands.

**Key point (red-left-border box):** **Risk:** no single view crosses a line — the change happens only in the total.

### Visualization (canvas `c1`, 720×300)

Flow schematic: a grid of small page icons on the left, arrows converging into one large "Searchable file" box on the right.

- **Title (bold 16px `#2e7d32`, top center, y=20):** "One by one vs gathered — the same pages, a different thing".
- **Page icons:** twelve rects 34×44, stroke `#2e7d32` width 1.5, fill `#2e7d32` at 0.06 alpha; inside each, three horizontal "text" lines stroked `#999` width 1 from icon x+6 to x+28 at icon y+10, y+18, y+26. Grid: columns x = 50, 105, 160, 215; rows y = 55, 115, 175.
- **Group label (13px `#666`, centered at (142, 245)):** "read one page at a time".
- **Arrows:** three solid `#c62828` width-1.5 arrows with filled triangular heads from (265, 77) to (450, 140), from (265, 137) to (450, 150), and from (265, 197) to (450, 160).
- **File box:** 220×120 at x=455, y=90, color `#c62828` (fill at 0.12 alpha, stroke width 2); title bold 16px `#c62828` "Searchable file" centered at (565, 135); sub-lines 12px `#666` centered: "every profile in one place" at (565, 155), "any name, one query away" at (565, 172).
- **Group label (13px `#666`, centered at (565, 245)):** "read all at once".
- **Bottom line (bold 14px `#8e24aa`, centered, y=265):** "Each page view is ordinary — the collection is not."
- **Caption (bottom center, 13px `#999`, y=285):** "No single visit changed anything; the gathering did."

## 2. Aggregation changes the thing

A searchable pile of profiles answers questions no single page can.

- **Mechanism:** a million pages in one file becomes a lookup tool.
- **Mechanism:** joined with other piles, names link to phones and faces.
- **Risk:** the collection is resold, matched, and reused downstream.
- **Risk:** bulk photos can train face matching (facial recognition).
- **Fact:** the people described never dealt with the collector.
- **Scene:** Bob's old posts resurface in a stranger's search.
- **Risk:** "it was public" skips the question of what it became.

**Key point:** **Risk:** consent to be seen page by page is not consent to be compiled into a product.

### Visualization (canvas `c2`, 720×300)

Join diagram: three separate pile boxes on top merging by arrows into one "Linked records" box below.

- **Title (bold 16px `#2e7d32`, top center, y=20):** "Three separate piles become one set of linked records".
- **Pile boxes** 180×55 at y=50 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 14px in box color centered at y=72; sub-line 12px `#666` centered at y=92):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Profiles | names, jobs, cities | #2e7d32 | 40 |
  | Phone lists | numbers with names | #37474f | 270 |
  | Photos | faces with captions | #8e24aa | 500 |
- **Arrows:** three solid `#c62828` width-2 arrows with filled triangular heads from (130, 108) to (300, 158), from (360, 108) to (360, 158), and from (590, 108) to (420, 158).
- **Linked box:** 280×62 at x=220, y=160, color `#c62828` (fill at 0.12 alpha, stroke width 2); title bold 14px `#c62828` "Linked records" centered at (360, 184); sub-line 12px `#666` "name + phone + face, one row per person" centered at (360, 204).
- **Downstream label (13px `#c62828`, centered at (360, 238)):** "resold, matched, used to train models".
- **Bottom line (bold 14px `#8e24aa`, centered, y=265):** "No pile alone links a face to a phone — the join does."
- **Caption (bottom center, 13px `#999`, y=285):** "The people in the rows never dealt with the collector."

## 3. What limits the damage

No single control stops bulk reading; each one slows or shrinks it.

- **Defense:** pacing rules slow bulk reading (rate limits).
- **Fact:** slowing raises the cost; patient readers still finish.
- **Defense:** puzzles that people pass easily filter machines (CAPTCHA).
- **Defense:** showing less to signed-out visitors shrinks the harvest.
- **Defense:** publish only the fields that truly need to be public.
- **Fact:** legal boundaries are still drawn court by court.
- **Defense:** for individuals: assume any public field gets collected.

**Key point:** **Win:** what is never published can never be harvested — the smallest public surface is the surest limit.

### Visualization (canvas `c3`, 720×300)

Defense table: one row per control, each with a colored dot, a plain-language item, and a "slows" or "shrinks" status pill.

- **Title (bold 16px `#2e7d32`, top center, y=20):** "What limits the harvest — some steps slow it, some shrink it".
- **Rows** at y = 58, 103, 148, 193 (dot = filled circle radius 5 in row color at (70, row y+15); item text 14px `#2c3e50` left-aligned at x=100, baseline row y+22; status pill = rect at x=500, width 140, height 26 at row y+2, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (570, row y+19)):
  | Item | pill text | color |
  |---|---|---|
  | pacing rules on repeated page requests | slows | #43a047 |
  | puzzles shown before more pages load | slows | #43a047 |
  | less shown to signed-out visitors | shrinks | #00838f |
  | only necessary fields made public | shrinks | #00838f |
- **Bottom line (bold 14px `#2e7d32`, centered, y=255):** "Slowing raises the cost; shrinking removes the prize."
- **Caption (bottom center, 13px `#999`, y=285):** "A field never published is the only field that cannot be gathered."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #43a047`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also green-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#2e7d32` green = mechanism/fact (Fact, Mechanism); `#00838f` teal = defense/win (Defense, Win); `#c62828` red = risk/loss (Risk, Seen); `#8e24aa` purple = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#2e7d32`; h2 1.3rem `#2e7d32`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #43a047`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #c62828`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each canvas is drawn by a named function (`drawC1`, `drawC2`, `drawC3`) and a `renderAll()` call, with a debounced (150ms) window-resize listener re-running `renderAll` so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary green `#2e7d32`, teal `#00838f`, red `#c62828`, purple `#8e24aa`, plus `#43a047`, `#37474f`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes; conceptual only, no how-to detail for building collectors or getting past limits. Each technical term (scraper, facial recognition, rate limits, CAPTCHA) appears at most once, in parentheses. Fictional names only (Alice, Bob); no realistic credentials anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
