# Cloud Storage & Share Links

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cloud Storage &amp; Share Links

**Subtitle:** The file stops moving — one copy lives in the cloud, and what travels is a URL plus the permission to open it

## One Copy in the Cloud: the Link Travels, Not the File

**Tags:** `core idea` (blue), `share links` (green), `one canonical copy` (orange)

- **The running example** — Alice drops a 2 GB dataset in a cloud folder and sends Bob a short link
- **The inversion** — earlier methods pushed the bytes to you; a share link brings you to the bytes
- **One canonical copy** — everyone opens the same file; "which attachment is latest?" disappears
- **What travels** — a short URL plus the permission to open what it points to; the file stays put
- **Always current** — Alice fixes a typo once and Bob's link shows the fix; nothing is re-sent

*Example (italic):* Bob clicks the link on Tuesday and sees the row Alice added on Monday — nobody mailed a new version.

**Key point:** The file stops moving — it lives in one place, and what travels is a URL plus permission to reach it.

### Visualization (canvas `c1`, 720×300)

Two-panel schematic contrasting attachments (bytes fan out to people) with a share link (people converge on one copy).

- **Title (bold 15px, ink `#1a5276`, top center, y=24):** "Five Attachments vs One Link: Where the Bytes Live".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=360 from y=40 to y=248.
- **Panel headings (bold 13px, centered, y=56):** "attachments: the bytes travel" in orange `#d95926` at x=180; "share link: the URL travels" in green `#008300` at x=540.
- **Left panel:** Alice node — circle radius 13 at (70,150), fill blue `#2a78d6`, white bold 11px "A". Five recipient circles radius 9, fill `#9aa2ad`, at (295,75) / (295,113) / (295,150) / (295,187) / (295,225). A 1.5px orange `#d95926` arrow from Alice to each recipient, with a 12×9 orange-filled file square (`rgba(217,89,38,0.5)`, 1px `#d95926` border) riding each arrow at its midpoint. Label 11px `#6b7280` centered at (180,262): "5 × 2 GB leave Alice's machine".
- **Right panel:** one cloud copy — rounded rect 84×46 at (498,78) (radius 8), fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border, bold 11px blue centered label "one copy" (two short lines: "one copy" / "2 GB"). Five person circles radius 9, fill aqua `#199e70`, along y=228 at x=440 / 490 / 540 / 590 / 640. From each person a dashed 1.2px aqua line up to the bottom edge of the copy rect. Bold 11px aqua label centered at (540,196): "each holds only the URL". Label 11px `#6b7280` centered at (540,262): "the 2 GB never leave the cloud unrequested".
- **Annotation (bold 13px orange `#d95926`, centered, y=284):** "the bytes stay put — people come to the file".
- **Caption:** none beyond the annotation; the panel labels carry the schematic framing.

## A 2 GB Dataset for 5 Teammates: Attachments vs One Link

**Tags:** `worked example` (blue), `share links` (green)

- **The attachment route** — 5 recipients × 2 GB = 10 GB up Alice's line, 10 GB down into inboxes
- **The link route** — one 2 GB upload; the 3 teammates who open it download 3 × 2 = 6 GB
- **Total moved** — attachments 20 GB, link 8 GB; the gap widens with every extra recipient
- **Alice's uplink** — the slow home uplink carries 2 GB instead of 10 GB; one upload serves all five
- **Free riders** — the 2 teammates who never open the link cost zero bytes moved
- **Copies left behind** — attachments scatter 6 separate copies; the link leaves exactly 1

*Example (italic):* Alice uploads once and closes her laptop; Bob's midnight download comes from the cloud copy, not from her.

**Key point:** One upload plus downloads on demand — bytes move only when someone actually needs them.

### Visualization (canvas `c2`, 720×300)

Two panels: stacked horizontal bars of GB moved per route (upload + downloads), and a small bar pair of copies left behind.

- **Title (bold 15px, ink `#1a5276`, top center, y=24):** "2 GB File, 5 Teammates: Bytes Moved and Copies Left".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=470 from y=40 to y=252.
- **Left panel heading (bold 13px ink, centered at (275,52)):** "GB moved (illustrative: 3 of 5 open it)".
- **Left panel axis:** 0–20 GB mapped to x=120 (0) through x=420 (20), 15 px/GB; 1px `#999` baseline at y=220; `#e5e9ef` gridlines at 0/5/10/15/20 from y=80 to y=220 with 12px `#444` tick labels at y=238; axis caption "GB moved" 12px `#444` centered at (270,258).
- **Bars (34px tall, 12px `#444` route labels right-aligned at x=112):**
  - "attachments" (bar y=92..126): upload segment 0→10 GB (x=120..270), fill `rgba(217,89,38,0.35)`, 2px `#d95926` border, bold 12px `#d95926` inside label "up 10"; download segment 10→20 GB (x=270..420), fill `rgba(25,158,112,0.35)`, 2px `#199e70` border, bold 12px `#199e70` inside label "down 10"; total bold 13px `#2c3e50` "20 GB" above the bar's right end (y=86).
  - "one link" (bar y=160..194): upload segment 0→2 GB (x=120..150), orange as above, bold 12px `#d95926` label "up 2" placed above the segment (y=154) since the segment is narrow; download segment 2→8 GB (x=150..240), aqua as above, bold 12px `#199e70` inside label "down 6"; total bold 13px green `#008300` "8 GB" right of the bar end (x=248, y=182).
- **Right panel heading (bold 13px ink, centered at (595,52)):** "copies that can drift".
- **Right panel bars (vertical, baseline y=220, scale 6 copies = 140px):** "attachments" bar x=525 w=55 h=140 (y=80..220), fill `rgba(217,89,38,0.35)`, 2px `#d95926` border, bold 14px `#d95926` value "6" above (y=72); "one link" bar x=620 w=55 h≈23 (y=197..220), fill `rgba(0,131,0,0.2)`, 2px `#008300` border, bold 14px `#008300` value "1" above (y=189). Labels 12px `#444` centered under baseline (y=238): "attach" / "link".
- **Annotation (bold 13px green `#008300`, centered, y=284):** "8 GB vs 20 GB moved — and only one version exists".

## The Link Is a Permission Slip: Who It Lets In

**Tags:** `permissions travel` (orange), `where it's used` (blue)

- **A link is a capability** — the URL is a key the server checks before it serves a single byte
- **Named people** — only Bob's signed-in account opens it; forwarded, it shows a stranger nothing
- **Anyone with the link** — the URL alone unlocks it, so forwarding it forwards the permission
- **Expiring links** — the permission dies on a set date; the day-8 click gets a refusal, not data
- **Bucket version** — a pre-signed URL on an object storage bucket is the industrial share link
- **Built-in expiry** — a pre-signed URL carries its permission and its deadline inside the URL itself

*Example (italic):* Alice shares a link that expires in 7 days; on day 8 Bob's click opens an "access expired" page, not the data.

**Key point:** Sharing a link shares a permission, not a file — choose who the permission answers to and how long it lives.

### Visualization (canvas `c3`, 720×300)

Three mini-panels, one per link type, each showing the same cloud file and who gets through to it.

- **Title (bold 15px, ink `#1a5276`, top center, y=24):** "Three Kinds of Link: Who Gets In".
- **Panels centered at x=125 / 360 / 595.** Panel headings bold 13px centered at y=56: "named people" in blue `#2a78d6`, "anyone with link" in orange `#d95926`, "expires in 7 days" in violet `#4a3aa7`.
- **File per panel:** rounded rect 54×36 at (cx−27, 74) (radius 6), fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border, 11px blue centered label "file".
- **Requesters per panel:** two circles radius 12 at (cx−45,180) and (cx+45,180), each with a white bold 11px letter and a 1.6px line up to the file rect.
  - Panel 1: left = Bob, fill green `#008300`, "B", solid green line, bold 12px green label "Bob ✓" at (cx−45,214); right = stranger with the forwarded URL, fill `#9aa2ad`, "S", dashed gray line with a bold 13px red `#e74c3c` "✕" at its midpoint, 12px `#6b7280` label "forwarded ✕" at (cx+45,214).
  - Panel 2: both circles orange `#d95926` ("B" and "S"), both solid orange lines, bold 12px orange labels "Bob ✓" / "stranger ✓" at y=214; 11px `#6b7280` note centered at (cx,240): "the URL alone unlocks".
  - Panel 3: same person on two days — both circles violet `#4a3aa7` ("B"), left line solid with bold 12px green "day 3 ✓" at (cx−45,214), right line dashed with red midpoint "✕" and bold 12px red `#e74c3c` "day 8 ✕" at (cx+45,214).
- **Annotation (bold 13px orange `#d95926`, centered, y=268):** "forwarding the URL forwards whatever permission rides on it".
- **Caption (11px `#6b7280`, bottom center, y=290):** "pre-signed bucket URLs work the same way — permission and expiry ride in the link".

## The Synced Folder: Saving — and Deleting — Quietly Travel

**Tags:** `common mistake` (red), `sync folders` (green)

- **Saving became transferring** — save into a synced folder and the client uploads in the background
- **Changed pieces only** — edit one sheet of a 2 GB file and only changed blocks go up, not all 2 GB
- **The mirror illusion** — the local file looks ordinary, but it is one end of a live two-way sync
- **The common mistake** — deleting inside the synced folder deletes the cloud original for everyone
- **The safe move** — "remove download" drops only the local copy and keeps the cloud original
- **Undo window** — trash and version history usually make a propagated delete recoverable

*Example (italic):* Alice tidies her laptop and deletes the synced project folder; an hour later Bob's copy is gone too.

**Key point (Common mistake label):** Inside a synced folder, a delete travels like any other edit — use "remove download" when you only want local space back.

### Visualization (canvas `c4`, 720×300)

Two-panel diagram: the same laptop–cloud–laptop sync chain under "delete inside the folder" (everything goes) vs "remove download" (only local space freed).

- **Title (bold 15px, ink `#1a5276`, top center, y=24):** "Two Deletes That Look the Same — and Are Not".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=360 from y=40 to y=250.
- **Panel headings (bold 13px, centered, y=56):** "delete inside the synced folder" in red `#e74c3c` at x=180; "remove download only" in green `#008300` at x=540.
- **Chain per panel:** three boxes 92×44 (radius 6) at y=104, box x-lefts 22 / 134 / 246 (left panel) and 382 / 494 / 606 (right panel); bold 11px `#2c3e50` centered labels "Alice's laptop" / "cloud original" / "Bob's laptop". Between adjacent boxes, double-headed 2px `#9aa2ad` sync arrows at y=126 with small filled arrowheads both ends.
  - Left panel: all three boxes fill `#f5f6f8` with 2px `#6b7280` border, and each gets a bold 20px red `#e74c3c` "✕" over its center (y=132). Bold 12px red label centered at (180,186): "the delete syncs to all three". 11px `#6b7280` note centered at (180,208): "cloud original gone — Bob loses it too".
  - Right panel: Alice's box dashed 2px `#9aa2ad` border, fill `#fbfbfc`, 11px `#6b7280` inner label gains second line "(freed)"; cloud and Bob boxes fill `rgba(0,131,0,0.08)` with 2px green `#008300` border and a bold 16px green "✓" over each center (y=132). Bold 12px green label centered at (540,186): "local space freed, cloud copy intact". 11px `#6b7280` note centered at (540,208): "trash / version history can still restore mistakes".
- **Annotation (bold 13px orange `#d95926`, centered, y=262):** "sync carries a delete exactly like it carried the save".
- **Caption (11px `#6b7280`, bottom center, y=288):** "schematic — a synced folder is one end of a live mirror".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" in sections 1–3, "Common mistake:" in section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` only for genuine denial/loss states: the blocked stranger and expired click in c3, the propagated delete in c4.
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). The worked example is labeled illustrative: 2 GB file, 5 teammates, 3 of whom open the link. Arithmetic that must stay consistent between text and charts: attachments 5 × 2 = 10 GB up and 10 GB down (total 20 GB, 6 copies counting Alice's); link 2 GB up + 3 × 2 = 6 GB down (total 8 GB, 1 canonical copy). Expiry example: link valid 7 days, day 3 succeeds, day 8 refused. Companies are unnamed ("a cloud drive", "object storage bucket"); people are Alice and Bob.
- This page has no links.
