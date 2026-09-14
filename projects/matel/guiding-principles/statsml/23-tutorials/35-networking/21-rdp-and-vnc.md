# RDP & VNC

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** RDP & VNC

**Subtitle:** Remote desktop sends your keystrokes one way and pictures of the screen the other — the program, its files, and its memory never leave the far machine

## The Office Computer at Your Kitchen Table

**Tags:** `core idea` (blue), `remote control` (green), `thin client` (orange)

- **The setup** — the accounting program and its files live on the office PC; the bookkeeper is at home
- **The wire** — her keystrokes and mouse moves travel to the office; pictures of the screen travel back
- **The illusion** — the office desktop appears in a window at home, but nothing runs on her laptop
- **The round trip** — press "k": 25 ms to the office, 2 ms for the app to render, 25 ms for pixels to return
- **The name** — this is remote desktop: RDP (Windows' protocol) and VNC (works on anything) both do it

*Example (italic):* At 9am she opens the office spreadsheet from her kitchen table; every letter she types shows up on her screen 52 ms after she presses the key.

**Key point:** A remote desktop protocol sends input one way and screen images the other — the GUI goes over the wire while the program and its data stay on the far machine.

### Visualization (canvas `c1`, 720×300)

Two-box flow diagram: home laptop on the left, office PC on the right, keystroke arrow going right, screen-update arrow coming back left, with a millisecond timeline underneath.

- **Title (bold 15px, `#1a5276`, top center):** "One Keystroke's Round Trip: Input Goes Over, Pixels Come Back".
- **Boxes:** "home laptop (viewer)" rounded box at x=70, y=105, 165×70, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; "office PC (app + files)" at x=485, y=105, 165×70, fill `rgba(0,131,0,0.12)`, 2px `#008300` border; 12px `#2c3e50` labels.
- **Top arrow (y=122, left to right):** 3px `#2a78d6` with arrowhead, 12px `#2a78d6` label above: "keystroke 'k' — 30 bytes, 25 ms".
- **Bottom arrow (y=160, right to left):** 3px `#008300` with arrowhead, 12px `#008300` label below: "screen update — 1,152 bytes, 25 ms".
- **Timeline:** 2px `#999` baseline at y=245 from x=70 to x=650; ticks proportional to time (11.15 px/ms) with 12px `#444` labels at "0 ms press" (x=70), "25 ms arrives" (x=349), "27 ms rendered" (x=371, label dropped to a second row at y=282), "52 ms on screen" (x=650).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=210):** "the app runs there — only pictures travel".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Sending 6 MB of Pixels vs a 30-Byte Draw Command

**Tags:** `worked example` (blue), `bandwidth math` (green), `RDP vs VNC` (orange)

- **Raw pixels** — 1920×1080 = 2,073,600 pixels; at 3 bytes each, one full frame is 6,220,800 bytes (~6.2 MB)
- **Raw video** — 30 frames/s of that is ~187 MB/s, roughly 1.5 Gbps — almost no home link carries that
- **VNC's trick** — send only changed rectangles: one typed letter alters a 16×24 cell = 1,152 bytes
- **RDP's trick** — send the drawing instruction instead: "glyph k at (312, 88)" is about 30 bytes
- **The ratio** — 6,220,800 / 1,152 ≈ 5,400× saved by diffing; 1,152 / 30 ≈ 38× more by commands

*Example (italic):* One keystroke costs 6.2 MB as a raw frame, 1,152 bytes as a VNC changed rectangle, and about 30 bytes as an RDP draw command.

**Key point:** VNC watches the framebuffer and ships the pixel rectangles that changed; RDP understands the GUI and ships drawing commands — same screen, wildly different byte counts.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: bytes needed to show one typed letter under the three strategies, log-feel widths hardcoded in pixels.

- **Title (bold 15px, `#1a5276`, top center):** "Bytes to Show One Typed Letter: Raw Frame vs VNC Diff vs RDP Command".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 90, 150, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "raw full frame": blue `#2a78d6` bar width 440, 11px `#444` end label "6,220,800 B"
  - "VNC changed rectangle": aqua `#199e70` bar width 210, end label "1,152 B"
  - "RDP draw command": green `#008300` bar width 60, end label "30 B"
- **Bar style:** 22px tall, solid fills, 4px corner radius.
- **Annotation (bold 13px magenta `#d55181`, right side near y=260):** "5,400× smaller by sending only what changed".
- **Caption (12px `#444`, bottom right):** "widths schematic (log feel); frame and rectangle byte counts exact, RDP command size illustrative".

## Jump Boxes, VDI, and Work-From-Home

**Tags:** `where it's used` (blue), `jump box` (green), `VDI` (orange)

- **Work from home** — one office PC per employee, reachable from any laptop that can show a screen
- **Jump boxes** — admins RDP into a hardened middle machine; production servers accept only its address
- **VDI** — a datacenter runs hundreds of virtual desktops; the machines on desks are just viewers
- **Data stays put** — the customer database is viewed through pixels; its rows never cross the wire
- **Old machines** — a 10-year-old laptop can "run" heavy software because it only shows pictures of it

*Example (italic):* An analyst spends all day querying a 10 GB customer table over RDP, yet the table itself never leaves the datacenter — only screenshots of it do.

**Key point:** Remote desktop turns the network into a long keyboard-and-monitor cable — heavy data and risky access stay inside the datacenter while only pictures leave.

### Visualization (canvas `c3`, 720×300)

Three-box flow diagram of the jump-box pattern: laptop, jump box, database server, with pixel/keystroke arrows crossing and the data marked as staying put.

- **Title (bold 15px, `#1a5276`, top center):** "The Jump Box Pattern: Pixels Leave, Data Stays".
- **Boxes (y=115, 60px tall, 8px radius, 12px `#2c3e50` text):** "analyst's laptop" at x=40 width 165, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; "jump box" at x=285 width 150, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border; "database server" at x=515 width 165, fill `rgba(0,131,0,0.12)`, 2px `#008300` border.
- **Arrows between boxes:** paired 2px arrows, upper `#2a78d6` labeled "keystrokes →" (12px), lower `#008300` labeled "← screen pixels" (12px), between laptop–jump box and jump box–server.
- **Data label:** bold 12px green `#008300` under the server box at y=200: "customer DB — 10 GB never crosses".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=255):** "the network becomes a long monitor cable".
- **Caption (12px `#444`, bottom right):** "illustrative".

## The Window Is a Picture, Not the File

**Tags:** `common mistake` (red), `only pixels travel` (orange)

- **The illusion again** — the spreadsheet window at home is a picture; the real file is on the office PC
- **Wi-Fi drops** — at 2:14pm her connection dies with a long report run at 47%; the run keeps going
- **Reconnect** — at 2:20pm she reconnects and finds it at 67% — the server never noticed her absence
- **The flip side** — "saving to my desktop" saves to the office desktop, not the laptop she is touching
- **Not a download** — closing the viewer doesn't close the app; it only stops the picture

*Example (italic):* Six minutes offline cost her nothing: the report climbed from 47% to 67% while her screen sat frozen.

**Common mistake:** Treating the remote window as a local copy. It is a live picture of a program running elsewhere — losing the connection loses the view, not the work, and files that look "here" are actually there.

### Visualization (canvas `c4`, 720×300)

Line chart of report progress over 30 minutes with a shaded disconnect band: the server's progress keeps climbing while the viewer's screen stays frozen.

- **Title (bold 15px, `#1a5276`, top center):** "Wi-Fi Dies at 2:14, Returns at 2:20 — the Report Never Stops".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "2:00" to "2:30" with 12px `#444` tick labels every 10 minutes; y = report progress 0 to 100%, gridlines `#e5e9ef` at 25/50/75.
- **Disconnect band:** fill `rgba(107,114,128,0.15)` from minute 14 to minute 20 spanning full plot height, 12px `#6b7280` label "connection lost" at its top.
- **Server progress line:** green `#008300` 3px line through minutes `[0, 5, 10, 14, 20, 25, 30]`, progress `[0, 17, 33, 47, 67, 83, 100]` — climbs straight through the band.
- **Frozen view line:** dashed blue `#2a78d6` (dash 4/3) 2px horizontal line at 47% across minutes 14–20, 12px `#2a78d6` label "her screen, frozen at 47%".
- **Annotation (bold 13px green `#008300`, near minute 21, y=80):** "reconnects to 67% — nothing lost".
- **Caption (12px `#444`, bottom right):** "progress illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the frame math is exact arithmetic (1920×1080×3 = 6,220,800 bytes; 16×24×3 = 1,152 bytes; ratios ≈5,400× and ≈38×); the 30-byte RDP command, 25 ms latencies, 10 GB database, and report progress percentages are invented and labeled illustrative; RDP-sends-draw-commands vs VNC-sends-framebuffer-diffs is the documented protocol distinction.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
