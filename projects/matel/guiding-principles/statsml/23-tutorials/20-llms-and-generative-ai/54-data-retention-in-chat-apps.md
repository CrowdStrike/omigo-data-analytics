# Data Retention in Chat Apps

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Data Retention in Chat Apps

**Card description (one line):** Deleting a conversation ends your access to it, not the provider's — retention runs on separate clocks you don't control.

**Subtitle:** Deleting a conversation ends your access to it, not the provider's — how long data lives, where the copies sit, and what "delete" actually completes

## Retention Is a Separate Clock from Training

**Tags:** `core idea` (blue), `two promises` (green)

- **The setup** — "we don't train on your data" is commonly read as "so you don't keep my data"
- **Two promises** — training use and storage duration live in separate clauses with separate answers
- **Common pattern 1** — business endpoints can keep content for a short abuse-review window, commonly quoted as around 30 days
- **Common pattern 2** — consumer accounts may keep conversation history indefinitely until the user deletes it
- **Common pattern 3** — zero or shortened retention is sometimes offered to larger customers as a contract term
- **Both true at once** — a provider can honestly never train on content and still store it for a month
- **Who sets the clock** — the purpose the data is kept for sets the window, and on a business plan the customer can specify it
- **Where to check** — the retention clause and data-processing terms, not the model card or the marketing page

*Example (italic):* A provider can truthfully state that API traffic never trains a model and equally truthfully store it for a stated abuse-review window.

**Key point:** "Not used for training" and "not stored" are two different promises — assume you have only the one that is written down. It is common to assume deleting removes data; the terms of service can decide otherwise.

### Visualization (canvas `c1`, 720×300)

Two independent clock bars over one day axis: the training answer is flat, the storage answer runs a window.

- **Title (bold 15px, `#1a5276`, top center):** "Two Promises, Two Independent Clocks".
- **Constant:** `RET = 30` (days). Day axis maps days 0–40 to x 100–660 via `dayToX(d) = 100 + d / 40 * 560`; so day 30 → x 520.
- **Lane A label (bold 12px green `#008300`, left-aligned at (24, 64)):** "promise 1 — is it used for training?".
- **Lane A bar:** rounded rect x=100, y=70, 560×34, fill `rgba(0,131,0,0.05)`, 2px **dashed** `#008300` border; bold 12px `#008300` centered at (380, 92): "no, on every single day — flat zero across the window".
- **Lane B label (bold 12px magenta `#d55181`, left-aligned at (24, 128)):** "promise 2 — is it stored, and for how long?".
- **Lane B bar:** rounded rect x=100, y=134, width `dayToX(RET) − 100` (=420) ×34, fill `rgba(213,81,129,0.12)`, 2px `#d55181` border; bold 12px `#d55181` centered at (310, 156): text built at render time as `"stored for " + RET + " days"`.
- **Lane B tail:** rounded rect from x=`dayToX(RET)` to 660 (width 140), y=134, h=34, fill `rgba(229,233,239,0.55)`, 1px **dashed** `#6b7280`; 11px `#6b7280` centered at (590, 156): "deleted".
- **Window marker:** 1.5px dashed `#1a5276` vertical line at x=`dayToX(RET)` from y=134 to y=216; bold 11px `#1a5276` centered at (`dayToX(RET)`, 190): "retention window ends".
- **Axis:** 1px `#999` line from (100, 216) to (660, 216); ticks at days `[0, 10, 20, 30, 40]` (x = 100, 240, 380, 520, 660), 11px `#444` centered labels at y=234: "day 0", "10", "20", "30", "40"; 12px `#444` axis title centered at (380, 252): "days since the conversation happened".
- **Annotation (bold 12px orange `#d95926`, centered at y=274):** "both promises can be true at the same time — never trained on, still stored".
- **Caption (11px `#444`, bottom right, y=294):** "commonly published patterns, simplified; check your own terms".

## Where the Copies Actually Live

**Tags:** `worked example` (blue), `many stores` (orange)

- **Store 1: chat history** — the transcript that renders your sidebar, kept until you delete the thread
- **Store 2: memory / personalization** — extracted facts about you that commonly survive deleting the chat
- **Store 3: safety and abuse logs** — sampled or flagged content held on its own fixed window
- **Store 4: files and images** — uploads and generated images sit in object storage with their own lifecycle
- **Store 5: request logs** — API-layer metadata, sometimes with content snippets, kept for days to weeks
- **Stores 6 and 7: backups and vendors** — snapshots lag live deletion; subprocessors clear on their own schedules
- **Store 8: your own exports** — anything in your logging is your retention problem, and where you should audit from
- **Check the scope** — confirm which of these stores the commitment covers, and which regions, if regulation applies

*Example (italic):* An assistant can still greet a user with a nickname learned in a deleted chat — a different store on a different clock.

**Key point:** A conversation is not one copy in one database — deletion has to propagate across stores that each clear on their own schedule.

### Visualization (canvas `c2`, 720×300)

Fan-out panel grid: one conversation, eight stores, each panel carrying its own clock.

- **Title (bold 15px, `#1a5276`, top center):** "One Conversation, Eight Stores, Eight Clocks".
- **Subhead (11px `#6b7280`, centered at (360, 44)):** "deletion must reach every panel below — they do not share a schedule".
- **Grid:** 8 rounded rects 144×88, radius 6; column lefts `[24, 200, 376, 552]`, row tops `[56, 154]`; each with a 2px colored border and the same hue at 0.06 alpha as fill.
- **Panel text (all centered on the panel's center x):** bold 11px store name in the panel color at top+20; 11px `#2c3e50` clock phrase at top+44; bold 11px panel color at top+70 for the day figure.
- **Panels (row-major):**
  1. blue `#2a78d6` — "chat history" / "kept until you delete" / "no set end"
  2. violet `#4a3aa7` — "memory / profile" / "survives chat delete" / "no set end"
  3. orange `#d95926` — "safety & abuse logs" / "own fixed window" / "about 30 days"
  4. magenta `#d55181` — "files & images" / "object storage rules" / "tied to the chat"
  5. aqua `#199e70` — "request logs" / "API-layer metadata" / "days to weeks"
  6. yellow `#c98500` — "backups & snapshots" / "lags live deletion" / "weeks after"
  7. green `#008300` — "subprocessors" / "hosting & moderation" / "their schedule"
  8. ink `#1a5276` — "your own exports" / "sits in your logging" / "you decide"
- **Annotation (bold 12px orange `#d95926`, centered at y=268):** "people picture one database; deletion has to travel through all eight".
- **Caption (11px `#444`, bottom right, y=292):** "commonly published patterns, simplified; check your own terms".

## What Delete Actually Does — and How to Verify It

**Tags:** `common mistake` (red), `deletion mechanics` (orange), `what to do` (blue)

- **Step 1: immediate** — the thread leaves your view at once and is flagged rather than erased in place
- **Step 2: the window** — backend deletion can take up to a stated period, commonly quoted as around 30 days
- **Step 3: backups** — encrypted snapshots age out on their own rotation, typically weeks behind the live copy
- **Chat vs account** — deleting one thread leaves memory, files, and logs that account deletion may cover
- **Shared workspace** — if an admin export already captured the thread, your deletion cannot reach that copy
- **Already trained on** — a shipped model cannot be untrained; deletion removes the stored copy, nothing more
- **Preservation hold** — a formal request to preserve records can suspend deletion until the hold is lifted
- **Someone else's data** — pasting a third party's records makes your own organisation responsible for them too
- **Verify, then weigh it** — export after a deletion request; zero retention also erases your own incident trail

*Example (italic):* After a delete at noon, the user's view clears at once, the primary store clears within the stated window, and snapshots clear later still.

**Common mistake:** Reading "deleted" as "unrecoverable everywhere, right now" — it means "gone from your view, and scheduled everywhere else".

### Visualization (canvas `c3`, 720×300)

Four-lane deletion timeline; each lane clears at a different day and the lag between the first and last is computed.

- **Title (bold 15px, `#1a5276`, top center):** "Delete Pressed at Day 0: Four Lanes, Four Endings".
- **Axis mapping:** days 0–120 to x 130–660 via `dayToX(d) = 130 + d / 120 * 530`; day 30 → x 262.5, day 90 → x 527.5.
- **Lane data (hardcoded):** `[{name:'your view', end:0, col:green, note:'clears immediately'}, {name:'primary store', end:30, col:blue, note:'backend deletion done'}, {name:'backups', end:90, col:magenta, note:'snapshots rotated'}, {name:'preservation hold', end:null, col:orange, note:'suspended while the hold is active'}]`.
- **Lanes:** rows of height 26 at tops `[62, 102, 142, 182]`; bold 11px lane name in the lane color, left-aligned at x=20, baseline top+18.
- **Held bar:** rounded rect from x=130 to `dayToX(end)` (minimum 6px wide so the day-0 lane still shows a stub), fill lane color at 0.15 alpha, 2px lane-color border. For `end === null` the bar spans the full axis to x=660 and is over-drawn with 1px diagonal orange hatch lines every 8px.
- **Cleared region:** from the bar's right edge to x=660, fill `rgba(229,233,239,0.5)`, 1px dashed `#6b7280` (omitted for the preservation-hold lane).
- **Lane note:** 11px `#2c3e50`, left-aligned at `max(barEnd + 8, 140)`, baseline top+18; for the preservation-hold lane the note is drawn inside the bar at x=142 in bold 11px `#d95926`.
- **Axis:** 1px `#999` line from (130, 222) to (660, 222); ticks at days `[0, 30, 60, 90, 120]`, 11px `#444` centered labels at y=240: "day 0", "30", "60", "90", "120"; 12px `#444` axis title centered at (395, 258): "days after you pressed delete".
- **Computed lag (bold 12px orange `#d95926`, centered at (360, 278)):** built at render time from the lanes with a non-null `end` — `lag = max(ends) − min(ends)` = 90 — printed as `"first lane clears at day " + min + ", last at day " + max + " — a " + lag + "-day spread"`. No day figure in this string is a literal.
- **Caption (11px `#444`, bottom right, y=294):** "commonly published patterns, simplified; check your own terms".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), copied verbatim from `48-copyright-complications.html`. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** three canvases, ids `c1`, `c2`, `c3`, intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundRect` and `arrowHead` helpers as in the reference page. No font below 11px.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data integrity:** no `Math.random()` anywhere; every chart uses hardcoded literal arrays. Day counts are internally consistent across canvases — 30 days is the abuse/backend window in c1, c2, and c3; 90 days is the backup rotation in c3. Every statistic printed beside plotted data is computed at render time: c1's window text from `RET`, and c3's min/max/lag from the lane `end` values.
- **Scope discipline:** this page covers **retention only** — how long data lives, where the copies are, and what deletion completes. Training defaults per tier and leakage via the wrong setting belong to sibling pages and are only referenced in passing (the "already trained on" bullet).
- **Content discipline:** **no vendor or product is named anywhere** — only "a consumer chat app", "the business tier", "negotiated zero retention", and "Vendor A" if a placeholder is needed. No named-actor scenarios. Patterns are always described as commonly published industry patterns, never as a specific company's policy. No legal conclusions and no realistic credential or token strings. Unsourced figures carry an "illustrative" label; every policy canvas carries a "commonly published patterns, simplified; check your own terms" or "illustrative" caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
