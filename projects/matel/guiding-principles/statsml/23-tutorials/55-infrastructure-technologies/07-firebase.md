# Firebase

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Firebase

**Subtitle:** Firebase (acquired by Google in 2014) lets an app skip the backend entirely — the client SDK talks straight to managed services, and the server code you would have written simply doesn't exist

## A Chat App With No Server

**Tags:** `core idea` (blue), `no backend` (green), `Firebase` (orange)

- **The app** — a small team builds a group chat: rooms, messages, and every device updating live
- **The old way** — client → your API server → database: auth endpoints, websockets, deploys, on-call
- **The Firebase way** — the client SDK writes messages straight into Firestore, a managed database
- **The push** — every device holds a listener on the room; Firestore pushes each new message to all of them
- **The guard** — security rules run inside Firestore, replacing the authorization code a server would hold

*Example (italic):* Priya's phone writes a message document into `rooms/lunch/messages`; Ben's phone and Ana's laptop see it appear — and no server the team wrote ever touched it.

**Key point:** Firebase's pitch is "apps without servers": the client talks directly to managed services, and the middle tier — the part that pages you at 3am — is Google's problem, not yours.

### Visualization (canvas `c1`, 720×300)

Two-row architecture diagram: the same chat app as a classic three-tier stack (top) vs the Firebase two-piece version (bottom), showing the missing middle tier.

- **Title (bold 15px, `#1a5276`, top center):** "Same Chat App: Three Tiers vs Two".
- **Row 1 (boxes centered on y=100), label 12px `#444` at x=20:** "hand-rolled"; blue `#2a78d6` rounded box at x=130 labeled "chat client" (12px), 3px `#6b7280` arrow to a violet `#4a3aa7` box at x=320 labeled "your API server — auth, sockets, deploys", 3px arrow to a blue box at x=545 labeled "database".
- **Row 2 (boxes centered on y=210), label:** "Firebase"; blue box at x=130 labeled "chat client (SDK)", single 3px green `#008300` arrow straight to a green box at x=430 labeled "Firestore (managed)"; orange `#d95926` 12px label above the arrow's midpoint: "security rules check every read/write".
- **Box style:** 140–200px wide, 42px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=272):** "the middle tier is gone — the SDK talks straight to the database".

## One Write, Every Screen Updates

**Tags:** `worked example` (blue), `live listeners` (green)

- **The write** — at 12:01:30 Priya sends "lunch?"; the SDK commits the document in about 80 ms
- **The fan-out** — Ben's open listener fires at +150 ms, Ana's at +210 ms; neither device asked or polled
- **The offline case** — Sam's phone has no signal; his SDK caches the room and queues his own writes
- **The sync** — 40 seconds later Sam reconnects; the cached listener replays and his screen catches up
- **Hand-check** — one write, two live listeners → two pushes now, one replay later: three screens, zero requests

*Example (italic):* The 12:01:30 message reaches Ben at +150 ms and Ana at +210 ms; Sam, offline, gets it 40 seconds later on reconnect — the team wrote no delivery code at all.

**Key point:** Listeners invert the usual flow — clients don't ask the database for changes, the database pushes snapshots to every subscribed device, online or catching up from offline cache.

### Visualization (canvas `c2`, 720×300)

Swimlane timeline of one message fanning out: Priya's write at t=0, delivery markers on two live devices, and an offline lane that syncs later.

- **Title (bold 15px, `#1a5276`, top center):** "12:01:30 — One Write Fans Out to Every Listener".
- **Axes:** origin x=60, plot width 600; x = milliseconds after send, 0 to 300, 12px `#444` tick labels every 100 ms along a 2px `#999` baseline at y=245.
- **Lanes (horizontal 1px `#e5e9ef` lines, 12px `#444` lane labels at x=62 above each):** Priya y=90, Ben y=135, Ana y=180, Sam y=225.
- **Write marker:** filled blue `#2a78d6` 6px dot on Priya's lane at t=0, bold 12px blue label "send"; hollow blue dot at t=80 ms labeled "commit 80ms".
- **Delivery markers:** filled green `#008300` 6px dots on Ben's lane at t=150 ms ("+150ms") and Ana's lane at t=210 ms ("+210ms"), each with a dashed `#6b7280` (dash 4/3) drop-line from Priya's write point.
- **Offline lane:** Sam's lane drawn dashed orange `#d95926` across the full width, bold 12px orange label at its right end: "offline — cached, syncs +40s".
- **Annotation (bold 13px violet `#4a3aa7`, near x=380, y=60):** "no polling, no delivery code — Firestore pushes the snapshot".
- **Caption (12px `#444`, bottom right):** "latencies illustrative".

## Why Prototypes and Mobile Apps Love It

**Tags:** `where it's used` (blue), `speed to ship` (green)

- **The suite** — Authentication for logins, Cloud Functions for the few server-side bits, Hosting for the web app
- **The audience** — hackathons, prototypes, and mobile apps where one developer is the whole team
- **The math** — the chat backend is ~2.5 dev-days of Firebase config vs ~16 dev-days hand-rolled (illustrative)
- **Offline free** — the SDK's local cache gives mobile apps offline reads and queued writes for nothing
- **The trade** — vendor lock-in and per-read pricing: costs and migration pain grow with the app

*Example (italic):* A solo developer ships the whole chat app in a weekend — login, live sync, and hosting — because the 16 days of backend work shrank to 2.5 days of configuration (illustrative).

**Key point:** Firebase trades control for speed: for a prototype that may not survive the month, renting Google's backend beats spending weeks building one you might throw away.

### Visualization (canvas `c3`, 720×300)

Horizontal paired-bar chart: dev-days per backend job, hand-rolled (blue) vs the Firebase service that replaces it (green).

- **Title (bold 15px, `#1a5276`, top center):** "The Backend You Didn't Write: Dev-Days per Job".
- **Axis:** bars extend right from a 2px `#999` baseline at x=250, scale 70 px per dev-day, max width 440; no gridlines.
- **Rows (paired bars 12px tall, blue above green with 4px gap, pairs centered at y = 70, 115, 160, 205), each with a left-aligned 12px `#444` label at x=20:**
  - "login & accounts / Authentication": blue `rgba(42,120,214,0.30)` bar width 350 (5 d), green `#008300` bar width 35 (0.5 d)
  - "live sync via websockets / Firestore listeners": blue bar width 420 (6 d), green bar width 35 (0.5 d)
  - "authorization middleware / security rules": blue bar width 210 (3 d), green bar width 70 (1 d)
  - "server hosting & deploys / Hosting + Functions": blue bar width 140 (2 d), green bar width 35 (0.5 d)
- **Bar labels:** 11px `#444` day counts at each bar's right end ("5d", "0.5d", "6d", "0.5d", "3d", "1d", "2d", "0.5d").
- **Annotation (bold 13px green `#008300`, right side near y=250):** "16 dev-days shrink to 2.5".
- **Caption (12px `#444`, bottom right):** "dev-days illustrative".

## The Join That Firestore Won't Do

**Tags:** `common mistake` (red), `NoSQL limits` (orange)

- **The confusion** — treating Firestore like SQL with live sync; it is a NoSQL store with strict query limits
- **No joins** — a query reads one collection; "messages joined with sender profiles" is not a query it can run
- **Index rule** — every compound filter needs a pre-built composite index; ad-hoc exploration isn't the model
- **The drift** — month-1 queries all fit; month-8 product asks are relational, and the schema wasn't shaped for them
- **The fix** — denormalize at write time (copy the sender's name into each message) or export to a real warehouse

*Example (italic):* In month 8 product asks for "rooms where a user was mentioned, with each mention's sender profile" — a one-line SQL join, but in Firestore a restructure of how messages are written.

**Common mistake:** Designing documents around month-1 screens and assuming relational queries will come later. In Firestore you shape the data for the queries up front — the joins you skip at write time become migrations, not WHERE clauses.

### Visualization (canvas `c4`, 720×300)

Query ladder: four real product asks from month 1 to month 8 of the chat project, each with a colored verdict pill showing where Firestore's limits bite.

- **Title (bold 15px, `#1a5276`, top center):** "Month 8: The Query the Prototype Never Planned For".
- **Timeline spine:** vertical 2px `#e5e9ef` line at x=95 from y=60 to y=235, filled `#1a5276` 5px dots at each row.
- **Rows (y = 75, 125, 175, 225), each: bold 12px `#1a5276` month label at x=20, 12px `#2c3e50` query text at x=115, and a rounded verdict pill (12px text, 8px radius, ~180px wide) right-aligned at x=520:**
  - "month 1 — last 50 messages in a room": green pill `rgba(0,131,0,0.12)` / `#008300`: "✓ one where + orderBy"
  - "month 3 — one user's messages, all rooms": green pill: "✓ needs a composite index"
  - "month 6 — unread count per room": orange pill `rgba(217,89,38,0.12)` / `#d95926`: "△ keep your own counter"
  - "month 8 — mentions joined with profiles": red pill `rgba(231,76,60,0.12)` / `#e74c3c`: "✗ no joins — restructure"
- **Annotation (bold 13px magenta `#d55181`, centered near y=270):** "denormalize on day one, or pay for it as a migration in month eight".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); delivery latencies (80/150/210 ms, 40 s), dev-day counts (5/6/3/2 vs 0.5/0.5/1/0.5, totals 16 vs 2.5), and the month-1-to-8 query ladder are invented and labeled illustrative; the 2014 Google acquisition, service names (Firestore, Realtime Database, Authentication, Cloud Functions, Hosting, security rules), and the query limits (no joins, composite-index requirement, offline cache) are publicly documented facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
