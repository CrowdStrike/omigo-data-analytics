# Email Attachments & FTP

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Email Attachments &amp; FTP

**Subtitle:** The internet's original file movers — email re-spells a file as text and grows it by a third; FTP just moves the bytes

## An Attachment Is a File Dressed Up as Text

**Tags:** `core idea` (blue), `attachments` (orange), `encoding overhead` (green)

- **The running example** — Alice attaches an 18 MB CSV for Bob; email itself only carries text
- **Built for text** — email predates attachments; a message body is just lines of characters
- **Base64** — the file's bytes are re-spelled using 64 safe characters so they survive as text
- **3 bytes → 4 chars** — every 24 bits are regrouped into four 6-bit characters: a third bigger
- **"CSV" → "Q1NW"** — three real bytes become four characters; same information, 4/3 the size
- **Decoded on arrival** — Bob's mail app reverses the spelling; users never see the bloat

*Example (italic):* Alice's 18 MB CSV travels as roughly 24 MB of letters, digits and symbols inside the message body.

**Key point:** An email attachment is not sent as a file — it is re-spelled as text, and the text spelling is a third larger than the file.

### Visualization (canvas `c1`, 720×300)

Three-row encoding diagram tracing the literal bytes of the string "CSV" through base64: byte boxes with binary, a 24-bit strip regrouped into four 6-bit groups, and the four output characters.

- **Title (bold 15px, `#1a5276`, top center, y=24):** 'How "CSV" Becomes "Q1NW": Base64 in One Step'.
- **Row labels (right-aligned 12px `#6b7280` at x=150):** "file bytes (8 bits)" at y=82, "regrouped (6 bits)" at y=153, "base64 text" at y=222.
- **Byte boxes (row 1, y=58, 140×40, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, at x=170 / 325 / 480):** each holds bold 14px `#1a5276` char + 12px `#2c3e50` binary, centered: "C 01000011", "S 01010011", "V 01010110".
- **Down arrow (2px `#6b7280`, x=410, from y=102 to y=126, small arrowhead).**
- **Bit strip (row 2, y=130, height 34, x=170 to x=650, four 120px groups):** fills at 0.15 alpha with 2px borders in blue `#2a78d6`, green `#008300`, violet `#4a3aa7`, aqua `#199e70`; centered bold 12px text in the matching hue: "010000", "110101", "001101", "010110".
- **Down arrow (2px `#6b7280`, x=410, from y=168 to y=192, small arrowhead).**
- **Char boxes (row 3, y=196, 120×44, aligned under the groups at x=170 / 290 / 410 / 530, same hue fills/borders as their group):** bold 15px char at box center y+18 ("Q", "1", "N", "W") and 11px `#6b7280` value at y+34 ("= 16", "= 53", "= 13", "= 22").
- **Annotation (bold 13px orange `#d95926`, centered, y=268):** "3 bytes in, 4 characters out — every attachment grows by ~33%".
- **Caption (11px `#6b7280`, centered, y=290):** 'base64 alphabet: A–Z, a–z, 0–9, +, / — index 16 = "Q", 53 = "1", 13 = "N", 22 = "W"'.
- **Fact status:** the encoding is exact and reproducible by hand — C=0x43, S=0x53, V=0x56; concatenated bits 010000 110101 001101 010110 = 16, 53, 13, 22 = "Q1NW".

## Why 18 MB Bounces Off a 25 MB Limit

**Tags:** `worked example` (blue), `size limits` (orange), `mailbox copies` (green)

- **The math** — encoded size = file size × 4/3; Alice's 18 MB CSV travels as 24 MB of text
- **The limit** — a 25 MB cap is checked against the encoded message, not the file on disk
- **Real ceiling** — 25 × 3/4 ≈ 18.75 MB; a file bigger than that on disk will bounce
- **Just under** — 18 MB encodes to 24 MB and squeaks through; 21 MB becomes 28 MB and bounces
- **Ten recipients** — the 24 MB message is copied to every mailbox: 10 people store ~240 MB
- **No resume** — a failed send restarts from zero; attachments have no partial retry

*Example (italic):* Bob's 21 MB export bounces off the 25 MB limit — encoded, it weighed 28 MB before headers.

**Key point:** The size limit is applied after the one-third bloat — divide the advertised cap by 4/3 to know what actually fits.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: four files (12 / 15 / 18 / 21 MB on disk), each with an on-disk bar and an as-sent bar (× 4/3), against a dashed red line at the 25 MB server limit; only the 21 MB file's encoded bar crosses it.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "On-Disk File Size vs What the Mail Server Sees".
- **Axes:** y = 0–32 MB mapped from baseline y=245 up to y=55; plot x from 70 to 690; 1px `#999` baseline and left axis; `#e5e9ef` gridlines with right-aligned 12px `#444` labels at 8 / 16 / 24 / 32 ("8", "16", "24", "32 MB").
- **Groups (4, centered at x = 155 / 310 / 465 / 620; two 34px bars per group, 6px apart):**
  - on-disk bar: fill `rgba(42,120,214,0.30)`, 2px `#2a78d6` border; heights 12 / 15 / 18 / 21
  - as-sent bar: fill `rgba(217,89,38,0.30)`, 2px `#d95926` border; heights 16 / 20 / 24 / 28 — except the 28 bar, which uses `rgba(231,76,60,0.25)` fill and 2px `#e74c3c` border (genuine rejection state)
  - bold 12px value labels above each bar in the bar's border color: "12"/"16", "15"/"20", "18"/"24", "21"/"28"
  - x labels 12px `#444` under baseline: "12 MB file", "15 MB file", "18 MB file", "21 MB file"; under the last group an extra bold 11px `#e74c3c` line "rejected"
- **Limit line:** dashed 2px `#e74c3c` horizontal at 25 MB (y≈96.6), bold 12px `#e74c3c` right-aligned label "25 MB limit" just above its right end.
- **Legend (top right, 12px):** blue swatch "on disk", orange swatch "as sent (× 4/3)".
- **Annotation (bold 13px green `#008300`, centered near x=250, y=75):** "real ceiling: 25 × ¾ ≈ 18.75 MB on disk".
- **Caption (11px `#6b7280`, bottom right, y=296):** "illustrative sizes; encoded = file × 4/3, before message headers".

## FTP: The Dedicated File Mover, Still on Duty

**Tags:** `FTP and SFTP` (blue), `where it's used` (green), `data pipelines` (orange)

- **FTP** — a 1971 protocol with one job: move files between machines over the network
- **Three verbs** — list what's there, get a file down, put a file up; that is most of daily use
- **No bloat** — FTP ships raw bytes; an 18 MB file travels as 18 MB, with no size wall
- **Daily drops** — vendors and banks still deliver data by putting files on an SFTP server
- **Pull side** — a scheduled job lists the folder, gets new files, and loads the warehouse
- **Real input** — "the CSV lands by SFTP at 2 a.m." is a live dependency in many pipelines

*Example (italic):* Alice's 05:00 job lists /incoming on Vendor A's SFTP server, finds sales_0826.csv, and pulls it.

**Key point:** When a file arrives in your pipeline from an outside company, an SFTP drop folder is very often the doorway it came through.

### Visualization (canvas `c3`, 720×300)

Left-to-right pipeline diagram of a nightly vendor drop: Vendor A puts a file on an SFTP server at 02:00, a scheduled job pulls it at 05:00 and loads the warehouse; the server box shows the /incoming folder listing with the new file highlighted.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "The 2 a.m. Data Drop: How a Vendor File Reaches the Warehouse".
- **Vendor box (x=25, y=95, 130×54, fill `rgba(0,131,0,0.12)`, 2px `#008300` border):** bold 12px `#2c3e50` "Vendor A", 11px `#6b7280` "daily export".
- **Arrow 1 (3px `#008300`, from (155,122) to (250,122), arrowhead):** bold 12px `#008300` label "put sales_0826.csv" above, 11px `#6b7280` "02:00" below.
- **Server box (x=250, y=70, 210×130, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border):** header bold 12px `#1a5276` "SFTP server — /incoming"; then three 12px file lines: "sales_0824.csv" (`#6b7280`), "sales_0825.csv" (`#6b7280`), "sales_0826.csv  ← new" (bold `#008300`).
- **Arrow 2 (3px `#4a3aa7`, from (460,122) to (555,122), arrowhead):** bold 12px `#4a3aa7` label "list + get" above, 11px `#6b7280` "05:00" below.
- **Job box (x=555, y=95, 140×54, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border):** bold 12px "Alice's 05:00 job", 11px `#6b7280` "scheduled pull".
- **Down arrow (3px `#199e70`, from (625,149) to (625,196), arrowhead)** into **warehouse box (x=555, y=200, 140×46, fill `rgba(25,158,112,0.12)`, 2px `#199e70` border):** bold 12px "warehouse table", 11px `#6b7280` "sales_daily".
- **Failure note (bold 11px `#e74c3c`, centered at x=280, y=252):** "real failure modes: the file is late, empty, or renamed".
- **Annotation (bold 13px orange `#d95926`, centered, y=284):** "a protocol from 1971 still feeds this morning's dashboard".
- **Fact status:** FTP's 1971 origin (RFC 114) is documented; the drop schedule and filenames are illustrative.

## FTP vs SFTP: One Letter Is the Whole Difference

**Tags:** `common mistake` (red), `security` (orange), `SFTP` (blue)

- **The confusion** — SFTP is not FTP with an S bolted on; it is a new protocol riding on SSH
- **Plain FTP** — login details and file contents travel readable to anyone on the path
- **SFTP** — the login, the commands, and the file all ride inside an encrypted SSH session
- **FTPS exists too** — old FTP wrapped in TLS; a third thing, often mixed up with SFTP
- **Two connections** — classic FTP opens a second data channel that firewalls often block
- **Default today** — banks and vendors mandate SFTP; plain FTP lingers on public mirrors

*Example (italic):* Bob configures a plain FTP feed for a partner; the partner's security review rejects it the same week.

**Common mistake:** Plain FTP sends login details and data readable in transit — for anything crossing the internet, use SFTP.

### Visualization (canvas `c4`, 720×300)

Two horizontal lanes showing the same transfer on the wire: a plain-FTP lane whose packets carry readable text, and an SFTP lane whose packets are opaque encrypted blocks; each lane notes what an on-path observer sees.

- **Title (bold 15px, `#1a5276`, top center, y=22):** "The Same Transfer on the Wire: Plain FTP vs SFTP".
- **Lane divider:** dashed 1px `#e5e9ef` horizontal line at y=158 from x=20 to x=700.
- **Lane 1 — plain FTP:** bold 13px `#e74c3c` label "plain FTP" at x=30, y=48. Endpoint boxes 90×36 at y=78 (fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border): "Alice" at x=30, "file server" at x=600; 1.5px `#999` wire from (120,96) to (600,96). Three packet boxes 135×30 at y=81, x=150 / 305 / 460: white fill, 1.5px `#e74c3c` border, centered 11px `#e74c3c` text "login — readable", "get sales.csv", "2026-08-25, 41 rows". Observer line bold 11px `#e74c3c` centered at x=360, y=144: "anyone on the path can read the login and the data".
- **Lane 2 — SFTP:** bold 13px `#008300` label "SFTP (over SSH)" at x=30, y=185. Endpoint boxes 90×36 at y=208, same style, "Alice" at x=30, "file server" at x=600; wire from (120,226) to (600,226). Three packet boxes 135×30 at y=211, x=150 / 305 / 460: solid `rgba(74,58,167,0.85)` fill, centered bold 11px `#ffffff` text "▓▓▓▓▓▓", "encrypted", "▓▓▓▓▓▓". Observer line bold 11px `#008300` centered at x=360, y=270: "an observer sees only the endpoints and noise".
- **Annotation (bold 13px orange `#d95926`, centered, y=292):** "same verbs, one letter apart — only SFTP hides the contents".
- **Fact status:** schematic — packet shapes and lane contents illustrate readable-vs-encrypted transit, not real captures; no real credentials shown.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" or "Common mistake:").
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` reserved for genuine failure states (the rejected bar and limit line in c2, the failure-modes note in c3, the readable-FTP lane in c4).
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented facts: base64 maps 3 bytes to 4 characters (~33% growth); "CSV" encodes to "Q1NW" (bytes 0x43 0x53 0x56 → indexes 16, 53, 13, 22); FTP dates to 1971 (RFC 114); SFTP runs over SSH, FTPS is FTP over TLS; classic FTP uses separate control and data connections. Illustrative figures (25 MB limit as a typical cap, the 12/15/18/21 MB file set, drop-folder schedule) are labeled in captions or hedged in text. Arithmetic that must stay consistent between text and charts: encoded = file × 4/3 → 12→16, 15→20, 18→24, 21→28 MB; 25 × 3/4 = 18.75 MB; 10 recipients × 24 MB ≈ 240 MB.
- This page has no links.
