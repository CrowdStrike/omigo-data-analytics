# SQLite

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SQLite

**Subtitle:** SQLite is a full SQL database that lives inside your app as a library — the entire database is one ordinary file, and there is no server at all

## The Notes App With No Database Server

**Tags:** `core idea` (blue), `embedded` (green), `one file` (orange)

- **The app** — a phone notes app keeps 1,200 notes: titles, bodies, tags, timestamps
- **The server way** — a classic database is a separate program; the app talks to it over a network socket
- **The embedded way** — SQLite is a library compiled into the app; running SQL is just a function call
- **The file** — tables, indexes, and data all live in one ordinary file on disk: `notes.db`
- **No setup** — no daemon to install, no port to open, no password, nothing to administer

*Example (italic):* The app calls one library function with `INSERT INTO notes ...` and the note lands in `notes.db` on the phone's own storage — no network is involved.

**Key point:** SQLite is a database as a library: the app links the engine in and reads and writes one local file — there is no server process anywhere.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram comparing the two architectures: a client-server database (app → network → server process → data files) vs SQLite (app with the library built in → one file).

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways for an App to Keep Its Data".
- **Row 1 (y=95), label 12px `#444` at x=20:** "server database"; blue `#2a78d6` rounded box at x=150 labeled "notes app" (12px), 3px arrow labeled 11px `#6b7280` "network socket" to a violet `#4a3aa7` box at x=350 labeled "database server process", 3px arrow to a blue box at x=560 labeled "data files".
- **Row 2 (y=205), label:** "SQLite (embedded)"; blue box at x=150 labeled "notes app + SQLite library", 3px arrow labeled "function call" to a green `#008300` box at x=430 labeled "notes.db — one file".
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "no server, no socket — the database is a file".

## Fifty Small Queries, Two Very Different Bills

**Tags:** `worked example` (blue), `in-process` (green)

- **The query** — opening one note runs `SELECT body FROM notes WHERE id = 7` plus its tags and history
- **Server path** — the app serializes the SQL, sends it over a socket, and waits for the reply
- **Embedded path** — the app calls a function; SQLite reads pages straight out of `notes.db`
- **The cost** — say a network round trip is 1 ms and an in-process call is 0.01 ms (illustrative)
- **Hand-check** — opening a note fires 50 small queries: 50 × 1 ms = 50 ms vs 50 × 0.01 ms = 0.5 ms

*Example (italic):* The same 50-query screen load costs 50 ms against a network server but about 0.5 ms in-process — the chatty pattern that hurts a server is free in SQLite.

**Key point:** With the engine in-process, each query skips the network entirely, so many tiny queries cost about the same as one — a pattern server databases punish.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of query latency, one row per case, comparing network vs in-process at 1 query and at 50 queries.

- **Title (bold 15px, `#1a5276`, top center):** "The Network Tax: 1 Query and 50 Queries, Server vs In-Process".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "server, 1 query — 1 ms": blue `#2a78d6` bar width 80
  - "SQLite, 1 query — 0.01 ms": green `#008300` bar width 8
  - "server, 50 queries — 50 ms": blue bar width 440
  - "SQLite, 50 queries — 0.5 ms": green bar width 30
- **Bar style:** 16px tall, blue bars fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge, green bars fill `rgba(0,131,0,0.25)` with 2px `#008300` edge, 11px `#444` millisecond labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "chatty queries are free when the database is in-process".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic, latencies illustrative".

## The Most Deployed Database in the World

**Tags:** `where it's used` (blue), `reliability` (green)

- **Phones** — every Android and iOS device ships SQLite; messages, contacts, and app data sit in `.db` files
- **Browsers** — major browsers store history, cookies, and settings in SQLite files on your disk
- **Scale** — the SQLite project publicly estimates over one trillion databases in active use
- **Reliability** — the project's test code outweighs the engine code by hundreds of times, run before every release
- **Longevity** — the file format is stable and cross-platform; it is a recommended format for archival data

*Example (italic):* Reading this in a browser, you are within arm's reach of dozens of SQLite files right now — your phone alone holds hundreds.

**Key point:** SQLite is not a niche tool — it is the most widely deployed database engine in existence, trusted because of an unusually massive test suite and a frozen, portable file format.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of where SQLite already runs, one row per platform category, bar widths schematic (reach, not counts).

- **Title (bold 15px, `#1a5276`, top center):** "One Engine, Everywhere: Where SQLite Already Runs".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440.
- **Rows (top to bottom at y = 70, 120, 170, 220), each with a left-aligned 12px `#444` label at x=20:**
  - "every smartphone (Android & iOS)": blue `#2a78d6` bar width 440
  - "every major web browser": green `#008300` bar width 380
  - "desktop operating systems": aqua `#199e70` bar width 320
  - "cars, TVs, set-top boxes, apps": orange `#d95926` bar width 400
- **Bar style:** 16px tall, fills at 0.30 alpha of each color with a 2px solid edge of the same color.
- **Annotation (bold 13px violet `#4a3aa7`, right side near y=255):** "sqlite.org estimates 1+ trillion databases in active use".
- **Caption (12px `#444`, bottom right):** "bar widths schematic — reach is documented, widths are not counts".

## Not a Shrunken Server Database

**Tags:** `common mistake` (red), `single writer` (orange)

- **The mistake** — dropping SQLite behind a busy multi-machine website as if it were a small MySQL
- **Single writer** — SQLite allows one writer at a time; a second writer waits for the lock
- **Readers are fine** — many readers can run at once, and in WAL mode readers run alongside the writer
- **The flat line** — at 10 concurrent writers SQLite still does ~1,940 writes/s (illustrative) — same as at 1
- **The fit** — app and data on one machine: excellent; many writers across a network: wrong tool

*Example (italic):* Ten web workers all insert on every request; nine sit waiting on the write lock while one proceeds, so adding workers adds no write throughput.

**Common mistake:** Choosing SQLite vs a server database by data size. The real question is who writes, from where — SQLite shines when the app and its database share one machine, not when many machines need to write over a network.

### Visualization (canvas `c4`, 720×300)

Line chart of write throughput vs concurrent writers: a server database scales up while SQLite stays flat because writes serialize behind one lock.

- **Title (bold 15px, `#1a5276`, top center):** "Adding Writers: Server DB Scales, SQLite Serializes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = concurrent writers 1 to 10 with 12px `#444` tick labels at 1/2/4/6/8/10; y = writes per second 0 to 12000, gridlines `#e5e9ef` at 3000/6000/9000 with 12px `#444` labels.
- **Server line:** blue `#2a78d6` 3px line with 4px dots through writers `[1, 2, 4, 6, 8, 10]`, writes/s `[1800, 3400, 6200, 8400, 9800, 10600]` — rising curve.
- **SQLite line:** orange `#d95926` 3px line with 4px dots through the same writer grid, writes/s `[2000, 2000, 1980, 1960, 1950, 1940]` — flat.
- **Labels:** bold 12px blue `#2a78d6` "server DB — scales with writers" near (x=6.5 writers, y above its line); bold 12px orange `#d95926` "SQLite — one writer at a time" near (x=6.5 writers, y below its line).
- **Annotation (bold 13px orange `#d95926`, near x=8 writers, y=205, just above the flat SQLite line):** "extra writers just wait for the lock".
- **Caption (12px `#444`, bottom right):** "throughput numbers illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); query latencies and write-throughput curves are invented and labeled illustrative; the embedded architecture, single-writer model, one-file format, and the one-trillion-databases estimate are publicly documented facts from the SQLite project.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
