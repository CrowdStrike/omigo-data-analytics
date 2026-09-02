# Accidental Data Leakage

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Accidental Data Leakage

**Subtitle:** No thief is required — a storage folder left open, an "anyone with the link" share, or a misdirected email publishes data by mistake, and automated readers find it fast.

**Intro callout (purple-left-border box):** The most common data leak has no attacker at the start: the door was opened from the inside, and the internet noticed before the owner did.

## 1. Publishing by mistake

No one has to break in when a setting opens the door.

- **Fact:** one settings toggle separates private from public.
- **Mechanism:** "anyone with the link" means anyone, not recipients.
- **Scene:** an autocomplete picks the wrong recipient for a file.
- **Risk:** dashboards and test copies quietly face the internet.
- **Mechanism:** a test copy carries the same data as the original.
- **Fact:** nobody broke in — the door was left standing open.

**Key point (red-left-border box):** **Risk:** the leak needs no thief — one toggle publishes the whole folder to everyone.

### Visualization (canvas `c1`, 720×300)

Toggle diagram: the same folder shown twice, once behind a private (green) switch and once behind a public (red) switch.

- **Title (bold 16px `#7b1fa2`, top center):** "One toggle — the same folder, two different audiences".
- **Panels** 300×180 at y=50 (fill = panel color at 0.06 alpha, stroke = panel color width 2; title bold 14px in panel color centered at y=72):
  | Title | color | x |
  |---|---|---|
  | Private (toggle off) | #33691e | 40 |
  | Public (toggle on) | #d84315 | 380 |
- **Folder icon** (identical in both panels, centered on panel center x = panel x+150): tab rect 26×10 at (panel x+115, 85) and body rect 70×44 at (panel x+115, 95), both stroked `#666` width 1.5, body filled `#f4f4f4`; label 12px `#666` centered at (panel x+150, 122): "same data".
- **Toggle switch** per panel: track rect 56×22 at (panel x+122, 155), fill = panel color at 0.12 alpha, stroke = panel color width 2; filled knob circle radius 8 in panel color — private knob at (panel x+135, 166) (left side), public knob at (panel x+165, 166) (right side).
- **Reader lines** (13px `#666`, centered at panel x+150, y=205): private — "readable by: invited people only"; public — "readable by: anyone who finds the address".
- **Verdict lines** (bold 13px, centered at panel x+150, y=222): private — "the door stays shut" in `#33691e`; public — "the door stands open" in `#d84315`.
- **Bottom line (bold 14px `#0277bd`, centered, y=262):** "The folder never moved — only the audience changed."
- **Caption (bottom center, 13px `#999`, y=285):** "No break-in occurred; one setting made the copy public."

## 2. Someone is always checking the doors

Open storage does not sit unnoticed for long.

- **Mechanism:** automated programs sweep public addresses nonstop.
- **Seen:** an open folder is often found within hours, not months.
- **Fact:** once copied, closing the folder recalls nothing.
- **Mechanism:** open addresses get shared around once discovered.
- **Risk:** the owner rarely knows how many readers came first.
- **Risk:** "no evidence of access" can mean "we kept no logs".

**Key point:** **Risk:** the honest question is not "was it read" but "who read it, and how often".

### Visualization (canvas `c2`, 720×300)

Timeline: the folder is set public at the left, automated readers arrive within hours, the owner notices far later; the whole span is the exposure window.

- **Title (bold 16px `#7b1fa2`, top center):** "Open folder timeline — automated readers arrive first, the owner last".
- **Axis:** solid `#999` line width 1.5 from (60, 180) to (682, 180) with a filled `#999` triangular arrowhead ending at (690, 180) (points (682,176), (690,180), (682,184)).
- **Opening event:** filled `#0277bd` circle radius 5 at (90, 180); vertical `#0277bd` width-1 line from (90, 150) to (90, 175); label bold 13px `#0277bd` centered at (90, 122): "folder set"; second line "public" at (90, 139).
- **Scanner hits:** filled `#d84315` circles radius 4 at (170, 180), (205, 180), (235, 180), (260, 180); bracket above in `#d84315` width 1 — horizontal line from (160, 148) to (270, 148) with short ticks down to y=158 at both ends; label bold 13px `#d84315` centered at (215, 122): "automated readers arrive"; second line "(within hours)" 12px `#d84315` at (215, 139).
- **Copy note:** 13px `#d84315` centered at (215, 205): "each visit can copy everything".
- **Owner event:** filled `#9c27b0` circle radius 5 at (560, 180); vertical `#9c27b0` width-1 line from (560, 150) to (560, 175); label bold 13px `#9c27b0` centered at (560, 122): "owner notices, closes it"; second line "(often weeks later)" 12px `#666` at (560, 139).
- **Exposure window:** double-headed `#d84315` width-1.5 arrow at y=228 from (90, 228) to (560, 228) with filled triangular heads at both ends; label bold 13px `#d84315` centered at (325, 248): "exposure window — closing it does not recall the copies".
- **Bottom line (bold 14px `#0277bd`, centered, y=268):** "By the time the door is shut, the number of readers is unknown."
- **Caption (bottom center, 13px `#999`, y=288):** "Without records, 'no evidence of access' only means nothing was written down."

## 3. What limits the damage

Good defaults prevent; short windows and records contain.

- **Defense:** private by default, public only by deliberate choice.
- **Defense:** regular audits list what outsiders can actually reach.
- **Defense:** links that expire limit how long a mistake stays live.
- **Defense:** access records turn "maybe" into an answer (logs).
- **Defense:** drills treat every open door as certain exposure.
- **Fact:** prevention beats detection; detection beats guessing.

**Key point:** **Win:** with records and expiring links, a mistake has a known size and a short life.

### Visualization (canvas `c3`, 720×300)

Defense table: one row per habit, each with a role pill — prevents (green), shortens (purple), or answers (brown).

- **Title (bold 16px `#7b1fa2`, top center):** "Four habits — what each one does for an open-door mistake".
- **Rows** at y = 50, 100, 150, 200 (habit name bold 14px `#7b1fa2` left-aligned at x=40, baseline row y+16; sub-line 12px `#666` left-aligned at x=40, baseline row y+32; separator line `#e0e0e0` width 1 from (40, row y+44) to (680, row y+44) under the first three rows only; pill rect at x=540, width 140, height 26 at row y+2, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (610, row y+19)):
  | Habit | sub-line | pill text | pill color |
  |---|---|---|---|
  | Private by default | public only by deliberate choice | prevents | #33691e |
  | Regular outside audits | list what is actually reachable | shortens | #9c27b0 |
  | Expiring links | a mistake stops working on its own | shortens | #9c27b0 |
  | Access records | who read it, and when | answers | #5d4037 |
- **Bottom line (bold 14px `#7b1fa2`, centered, y=262):** "Prevent first; then shorten the window; then be able to answer."
- **Caption (bottom center, 13px `#999`, y=285):** "A drill that treats every open door as certain exposure keeps these habits honest."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #9c27b0`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also purple-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#7b1fa2` purple = mechanism/fact (Fact, Mechanism); `#33691e` green = defense/win (Defense, Win); `#d84315` red = risk/loss (Risk, Seen); `#0277bd` blue = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#7b1fa2`; h2 1.3rem `#7b1fa2`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #9c27b0`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #d84315`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each canvas is drawn by a named function (`drawC1`, `drawC2`, `drawC3`) and a debounced (150ms) resize listener calls `renderAll()` so canvases stay sharp. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary purple `#7b1fa2`, green `#33691e`, red `#d84315`, blue `#0277bd`, plus `#9c27b0`, `#5d4037`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("often found within hours", not "always"). Each technical term (storage bucket, scanners, logs) appears at most once, in parentheses. Fictional naming only — no real company names. No realistic credentials or file addresses anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
