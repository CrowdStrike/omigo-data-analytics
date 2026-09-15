# MySQL

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** MySQL

**Subtitle:** MySQL is the free relational database that powered the early web — not the fanciest, just free, fast enough, and easy enough that anyone could run it

## The Free Database That Ran the Early Web

**Tags:** `core idea` (blue), `open source` (green), `LAMP` (orange)

- **The stack** — LAMP meant Linux, Apache, MySQL, PHP: a complete website from four free downloads
- **The price** — commercial databases cost thousands per server license; MySQL cost nothing to start
- **The proof** — WordPress, early Facebook, and YouTube all launched on MySQL
- **The engine** — the InnoDB storage engine gave it safe transactions and crash recovery
- **The owners** — Oracle owns MySQL today; MariaDB is the community fork led by its original creator

*Example (italic):* In 2004 a student could rent one cheap shared server, install the LAMP stack, and run a whole business — MySQL was the M that stored everything.

**Key point:** MySQL became the web's default database not by being the most powerful one, but by being free, fast enough, and simple enough to install in an afternoon.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the LAMP stack: a browser on the left sends a request into one server box holding Apache, PHP, and MySQL stacked, with MySQL highlighted as the layer that stores the data.

- **Title (bold 15px, `#1a5276`, top center):** "The LAMP Stack: Four Free Layers, MySQL Holds the Data".
- **Browser box:** rounded rect x=40 y=125 width=130 height=50, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` label "browser".
- **Server outline:** 2px `#6b7280` rounded rect x=280 y=50 width=400 height=215, 12px `#6b7280` label "one Linux server" at its top-left inside edge.
- **Layer boxes (x=320, width=320, height=44, 8px radius, 12px labels):** "Apache — serves the page" at y=70 fill `rgba(42,120,214,0.15)`; "PHP — runs the code" at y=130 fill `rgba(230,126,34,0.15)`; "MySQL — stores the data" at y=190 fill `rgba(0,131,0,0.12)` with 2px `#008300` border.
- **Arrows:** 3px `#2a78d6` arrow from browser (170,150) to server edge (280,150); thin 2px `#6b7280` down-arrows between the three layer boxes.
- **Annotation (bold 13px green `#008300`, x≈330, y=262):** "every request ends at the database".
- **Caption (12px `#444`, bottom right):** "layout schematic".

## A Small Shop's Orders Table

**Tags:** `worked example` (blue), `SQL` (green)

- **The table** — an orders table with columns id, day, item, price holds five rows for the week
- **The insert** — `INSERT INTO orders VALUES (1,'Mon','latte',4.50)` adds one row
- **The query** — `SELECT SUM(price) FROM orders WHERE day='Mon'` asks one question in one line
- **Hand-check** — Monday's rows are 4.50 + 12.00 + 3.75, so the sum is 20.25
- **The index** — an index on day lets MySQL jump straight to Monday's rows instead of scanning all five

*Example (italic):* The shop's Monday total is 20.25 — three rows matched, two were skipped, and the arithmetic is exact.

**Key point:** A relational table plus SQL means you state what you want — Monday's total — and MySQL finds the matching rows and does the arithmetic for you.

### Visualization (canvas `c2`, 720×300)

Left half draws the five-row orders table with the Monday rows highlighted; right half shows the SQL query and its single-number result.

- **Title (bold 15px, `#1a5276`, top center):** "Five Rows In, One Number Out".
- **Table grid (left):** columns id / day / item / price at x = 50, 100, 170, 290 (right-aligned prices at x=350); bold 12px `#1a5276` header row at y=75 with 1px `#e5e9ef` rule under it; data rows at y = 105, 135, 165, 195, 225 in 12px `#2c3e50`.
- **Rows (hardcoded):** `[1,'Mon','latte',4.50]`, `[2,'Mon','beans',12.00]`, `[3,'Tue','mug',7.25]`, `[4,'Mon','scone',3.75]`, `[5,'Tue','gift box',9.00]`.
- **Highlight:** the three Mon rows get a full-width `rgba(0,131,0,0.12)` background band (x=40 to 360, 26px tall, behind the text).
- **Query box (right):** rounded rect x=420 y=90 width=270 height=60, fill `rgba(42,120,214,0.15)`, 12px monospace `#2c3e50` two-line text "SELECT SUM(price) FROM orders" / "WHERE day='Mon'".
- **Result:** 3px `#008300` down-arrow from the query box to a green-bordered rounded rect x=480 y=190 width=150 height=44 holding bold 16px `#008300` text "20.25".
- **Annotation (bold 12px green `#008300`, x≈420, y=262):** "4.50 + 12.00 + 3.75 = 20.25".
- **Caption (12px `#444`, bottom right):** "shop rows illustrative, sum exact".

## One Primary, Many Read Replicas

**Tags:** `where it's used` (blue), `read scaling` (green)

- **The pattern** — a busy site does far more reads (page views) than writes (new orders)
- **The primary** — one MySQL primary takes every write and keeps the authoritative copy
- **The replicas** — replicas replay the primary's change log and serve read-only queries
- **The math** — one box handles 2,000 reads/s; a primary plus three replicas handles 8,000
- **The echo** — early Facebook and YouTube scaled reads exactly this way before anything fancier

*Example (italic):* On a holiday rush the shop's site serves 8,000 reads/s across four boxes — 2,000 each — while all writes still funnel through the one primary.

**Key point:** Replication turned one cheap database into a fleet: writes go to a single primary, reads fan out across copies — the read-scaling recipe of the early web.

### Visualization (canvas `c3`, 720×300)

Flow diagram: a web app sends writes to one primary; the primary streams its change log to three replicas; reads fan out across all four boxes.

- **Title (bold 15px, `#1a5276`, top center):** "One Primary Takes the Writes, Four Boxes Share the Reads".
- **App box:** rounded rect x=40 y=125 width=130 height=50, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` label "web app".
- **Primary box:** rounded rect x=300 y=60 width=150 height=50, fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` border, 12px label "primary".
- **Replica boxes:** rounded rects x=540 width=140 height=40, fill `rgba(0,131,0,0.12)`, at y = 65, 130, 195, 12px labels "replica 1" / "replica 2" / "replica 3".
- **Write arrow:** 3px `#2a78d6` arrow from app (170,140) to primary (300,85), bold 12px `#2a78d6` label "writes 200/s" above it.
- **Replication arrows:** dashed (4/3) 2px `#6b7280` arrows from primary right edge (450,85) to each replica's left edge, one 12px `#6b7280` label "change log" beside the middle arrow.
- **Read arrows:** 2px `#008300` arrows from app bottom (105,175) curving right to each replica's bottom-left corner, single bold 12px `#008300` label "reads 2,000/s per box" at x≈200, y=250; a fourth straight 2px green arrow (170,168)→(300,104) shows the primary serving reads too.
- **Annotation (bold 13px green `#008300`, x≈430, y=278):** "8,000 reads/s total — 4× one box".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Not Every Read Sees Your Last Write

**Tags:** `common mistake` (red), `replication lag` (orange)

- **The catch** — replication is asynchronous: replicas copy changes after the primary commits them
- **The lag** — a replica normally runs a moment behind; under load the lag stretches to seconds
- **The symptom** — a user saves an order, the next page reads a replica, and the order is missing
- **The mistake** — treating a replica as an exact live copy instead of a slightly delayed one
- **The fix** — send reads that must see a fresh write to the primary, or wait out the lag

*Example (italic):* The shop marks order 6 paid at 12:00:00; a replica lagging 2 seconds still answers "unpaid" at 12:00:01.

**Common mistake:** Assuming every copy agrees at every instant. Replication trades a little freshness for a lot of read capacity — reads that must reflect the latest write belong on the primary.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same write read back from a lagging replica (stale) vs from the primary (fresh), shown as boxes with an arrow between them.

- **Title (bold 15px, `#1a5276`, top center):** "Read After Write: the Lagging Replica Answers From the Past".
- **Row 1 (y=95), label 12px `#444` at x=20:** "read from replica"; blue `#2a78d6` rounded box at x=180 labeled "write: order 6 paid @ 12:00:00" (12px), 3px arrow to a red `#e74c3c` box at x=460 labeled "replica, 2s behind: unpaid" with bold 12px red "✗ stale read" beneath it.
- **Row 2 (y=205), label:** "read from primary"; identical blue box "write: order 6 paid @ 12:00:00", 3px arrow to a green `#008300` box at x=460 labeled "primary: paid" with bold 12px green "✓ fresh" beneath it.
- **Box style:** 170–200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the replica is not wrong — it is 2 seconds behind".
- **Caption (12px `#444`, bottom right):** "lag of 2s illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. SQL snippets in bullets render in `<code>` monospace. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness). The shop's order rows, prices, read/write throughputs, and the 2-second lag are invented and labeled illustrative; the Monday sum 4.50 + 12.00 + 3.75 = 20.25 is exact arithmetic. LAMP composition, InnoDB, the WordPress/Facebook/YouTube adopters, Oracle ownership, and the MariaDB fork are public record.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
