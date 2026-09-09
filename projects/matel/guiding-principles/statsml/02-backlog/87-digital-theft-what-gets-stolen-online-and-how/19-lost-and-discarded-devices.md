# Lost & Discarded Devices

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Lost & Discarded Devices

**Subtitle:** Phones, laptops, USB sticks, and old drives walk away with everything on them — and deleted files come back in a lab.

**Intro callout (blue-left-border box):** No password is guessed and no network is breached — the storage simply travels, and whoever holds it next holds the data.

## 1. The loss: data leaves on hardware

The device is the breach; losing it is the whole attack.

- **Scene:** a phone stays in the taxi, a stick on the table.
- **Fact:** portable media is small, unlabeled, rarely missed.
- **Fact:** a work laptop carries cached mail and saved sessions.
- **Mechanism:** old drives go out with office clean-ups and moves.
- **Risk:** a resale passes the disk along with the device.

**Key point (red-left-border box):** **Risk:** the loss is silent — nothing alerts anyone until the finder acts, if ever.

### Visualization (canvas `c1`, 720×300)

A row of four device boxes, each with a simple stroked icon, the device name, and a sub-caption of where it goes missing; colors follow the page palette roles.

- **Title (bold 16px `#455a64`, top center, y=20):** "Four ways the storage walks away".
- **Boxes** 130×84 at y=56 (fill = box color at 0.10 alpha, stroke = box color width 2; device name bold 14px in box color centered at (cx, 126); sub-caption 12px `#666` centered at (cx, 158) below the box; cx = x + 65):
  | Device | sub-caption | color | x | cx |
  |---|---|---|---|---|
  | Phone | left in a taxi | #455a64 | 40 | 105 |
  | Laptop | left at a cafe | #4527a0 | 215 | 280 |
  | USB stick | left in a meeting room | #f57c00 | 390 | 455 |
  | Old drive | dumped or resold | #bf360c | 565 | 630 |
- **Icons** (stroke = box color, width 2, drawn about each cx):
  - Phone: strokeRect(cx−13, 66, 26, 40); home-button line from (cx−4, 101) to (cx+4, 101).
  - Laptop: screen strokeRect(cx−21, 68, 42, 26); base line width 3 from (cx−28, 100) to (cx+28, 100).
  - USB stick: plug strokeRect(cx−6, 64, 12, 10); body strokeRect(cx−10, 74, 20, 32).
  - Old drive: case strokeRect(cx−22, 68, 44, 34); platter circle radius 9 stroked at (cx−3, 85); filled spindle dot radius 2 at (cx−3, 85).
- **Mid label (13px `#999`, centered at (360, 192)):** "each one leaves with files, mail, and saved sessions aboard".
- **Bottom line (bold 14px `#f57c00`, centered, y=252):** "Nothing is hacked — the hardware itself changes hands."
- **Caption (bottom center, 13px `#999`, y=285):** "The smaller the media, the easier the loss — a stick is gone before it is missed."

## 2. Deleted isn't erased

Removing a file removes its entry, not its bytes.

- **Mechanism:** deleting a file removes the label, not the contents.
- **Fact:** a quick format leaves nearly everything recoverable.
- **Fact:** free recovery tools read an "emptied" drive in minutes.
- **Risk:** even damaged drives yield data to lab equipment.
- **Risk:** a dumped drive has no lock screen at all.

**Key point:** **Risk:** the trash can is an unencrypted archive — a drive that spins up once gives up years of files.

### Visualization (canvas `c2`, 720×300)

Two-bar contrast: what deleting appears to remove (full bar) versus what remains recoverable on the disk (nearly the same height), with a small annotated recovery arrow.

- **Title (bold 16px `#455a64`, top center, y=20):** "Deleting a file: what disappears vs. what stays".
- **Bar labels (bold 14px in bar color, centered above each bar, y=60):** "Appears removed" at (175, 60) in `#455a64`; "Still recoverable" at (405, 60) in `#bf360c`.
- **Bar 1:** rect at (100, 70) size 150×160, fill `#455a64` at 0.15 alpha, stroke `#455a64` width 2; inside text 12px `#666` centered: "the whole file" at (175, 148), "vanishes from view" at (175, 166).
- **Bar 2:** rect at (330, 82) size 150×148, fill `#bf360c` at 0.15 alpha, stroke `#bf360c` width 2; inside text bold 13px `#bf360c` centered "nearly all of it" at (405, 148); 12px `#666` centered "still on the platters" at (405, 166).
- **Baseline:** `#999` width-1 line from (80, 230) to (500, 230).
- **Under-bar sub-lines (12px `#666`, centered, y=248):** "what the folder view shows" at (175, 248); "what a reader finds" at (405, 248).
- **Recovery arrow:** dashed `#bf360c` width-1.5 horizontal arrow with filled triangular head from (485, 150) to (550, 150); label 13px `#bf360c` left-aligned: "recovery tool" at (558, 143), "reads it back" at (558, 160).
- **Bottom line (bold 14px `#f57c00`, centered, y=266):** "Deletion edits the index, not the shelf — the contents wait to be read."
- **Caption (bottom center, 13px `#999`, y=288):** "A quick format is the same edit at drive scale; labs read even damaged platters."

## 3. What limits the damage: assume the hardware will wander

The strongest preparation treats every device as already lost.

- **Defense:** full-disk encryption makes a lost device a locked box.
- **Fact:** the finder holds ciphertext, not files.
- **Defense:** screen lock plus short auto-lock covers grab-and-go.
- **Defense:** remote locate and wipe shorten the exposure window.
- **Defense:** encrypted USB sticks protect the pocket-sized media.
- **Defense:** a real wipe (overwrite or secure erase) precedes resale.
- **Defense:** dead drives get physical destruction, not the trash.

**Key point:** **Win:** encryption moves the loss from "all the data" to "one replaceable object".

### Visualization (canvas `c3`, 720×300)

Layered-checks rows: one row per preparation, item text and sub-line on the left, outcome pill on the right colored by how the found hardware fares.

- **Title (bold 16px `#455a64`, top center, y=20):** "The same lost drive, four preparations".
- **Rows** at y = 52, 100, 148, 196 (item text bold 14px `#2c3e50` left-aligned at (50, y+10); sub-line 12px `#666` left-aligned at (50, y+27); outcome pill = rect at x=510, width 160, height 26 at row y, fill = pill color at 0.12 alpha, stroke = pill color width 2, pill text bold 13px in pill color centered at (590, y+17)):
  | Item | sub-line | pill text | color |
  |---|---|---|---|
  | Unencrypted drive found | every file opens on the first try | readable | #bf360c |
  | Screen lock only | the disk reads out in another machine | slows, doesn't stop | #f57c00 |
  | Full-disk encryption | the finder holds a locked box | locked box | #00796b |
  | Wiped before resale | overwritten sectors have no past | nothing to find | #00796b |
- **Bottom line (bold 14px `#455a64`, centered, y=252):** "Encryption plans for the loss — the hardware wanders, the data does not."
- **Caption (bottom center, 13px `#999`, y=285):** "A remote wipe shortens the window; a real wipe or destruction closes it before resale."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #607d8b`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also underlined in `#607d8b`). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#455a64` primary = mechanism/fact (Fact, Mechanism); `#00796b` green = defense/win (Defense, Win); `#bf360c` red = risk/loss (Risk); `#f57c00` orange = scene/context (Scene). Key-point boxes open with the same colored bold lead word (Risk, Win) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#455a64`; h2 1.3rem `#455a64`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #607d8b`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #bf360c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. Each chart is a named `drawC1()`/`drawC2()`/`drawC3()` function; a `renderAll()` call runs them once and again on window resize (debounced 150ms) so canvases stay sharp after resize. All chart data is literal hardcoded coordinates — no randomness, no dates.
- **Palette:** primary `#455a64`, secondary `#607d8b`, red `#bf360c`, green `#00796b`, orange `#f57c00`, plus `#4527a0` as an occasional fourth box color, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral — describe the mechanism, no drama words, no attributed intent, no absolutes ("nearly everything", not "everything"). Each technical term (secure erase, ciphertext) appears at most once, in parentheses or plainly. Fictional naming only if a person is needed (Alice/Bob); no real company names; no realistic credential strings anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
