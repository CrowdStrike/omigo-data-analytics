# The Actor Model & Erlang

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Actor Model & Erlang

**Subtitle:** Instead of workers sharing one memory and fighting over locks, every worker keeps its own private state and talks only by sending messages — share nothing, and running on ten machines looks the same as running on one

## A Coffee Shop With No Shared Whiteboard

**Tags:** `core idea` (blue), `share nothing` (green), `Erlang` (orange)

- **The shared way** — three baristas write every order on one whiteboard, so only one can write at a time
- **The lock** — whoever holds the marker blocks the other two; a dropped marker freezes the whole shop
- **The actor way** — each barista keeps a private notebook nobody else can read or touch
- **The mailbox** — orders arrive as paper slips in each barista's tray, handled one slip at a time
- **The name** — each barista is an *actor*: private state, a mailbox, and behavior for each message
- **Erlang** — the language built at Ericsson where actors (processes) are the only way to share work

*Example (italic):* Barista A never peeks at barista B's notebook — if A needs B's tab total, A drops a slip in B's tray and B replies with a slip.

**Key point:** An actor owns its state outright and communicates only by asynchronous messages — with nothing shared, there is nothing to lock and nothing two workers can corrupt at once.

### Visualization (canvas `c1`, 720×300)

Two-panel flow diagram: left panel shows three baristas contending for one shared whiteboard behind a lock; right panel shows three baristas with private notebooks and individual mailboxes passing message slips.

- **Title (bold 15px, `#1a5276`, top center):** "Three Baristas: One Locked Whiteboard vs Three Private Mailboxes".
- **Left panel (x 20–340), label 13px `#6b7280` at (30, 60):** "shared memory + lock"; orange `#d95926` rounded box (110×46, fill `rgba(217,89,38,0.12)`) at center (x=190, y=150) labeled "whiteboard 🔒" (12px `#2c3e50`); three blue `#2a78d6` boxes (86×34, fill `rgba(42,120,214,0.15)`) at (40, 92) / (40, 152) / (40, 212) labeled "barista A/B/C"; 2px arrows from each to the whiteboard; solid arrow from A, dashed `#e74c3c` arrows from B and C with bold 12px red `#e74c3c` label "waiting" at (150, 235).
- **Right panel (x 380–700), label 13px `#6b7280` at (390, 60):** "actors: share nothing"; three blue boxes (86×34) at (400, 92) / (400, 152) / (400, 212) labeled "barista A/B/C", each with a small mute 11px `#6b7280` note "own notebook" beneath; beside each, a green `#008300` mailbox drawn as three stacked 14×10 squares (fill `rgba(0,131,0,0.15)`) at x=560; 2px `#199e70` aqua arrows carrying 11px labels "slip" from A's box to B's mailbox and from B's box to C's mailbox.
- **Divider:** vertical 1px `#e5e9ef` line at x=360 from y=50 to y=270.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=282):** "no locks anywhere — only slips landing in mailboxes".

## One Overloaded Espresso Mailbox

**Tags:** `worked example` (blue), `backlog math` (green)

- **The setup** — the espresso actor receives 8 order slips per minute but brews only 6 per minute
- **The growth** — the mailbox backlog grows by 8 − 6 = 2 slips every minute
- **Hand-check** — after 5 minutes the backlog is 5 × 2 = 10 slips waiting in the tray
- **The fix** — at minute 5, spawn a second espresso actor; a router splits arrivals 4/min to each
- **The drain** — combined brewing is 12/min against 8/min arriving, so the backlog shrinks 4/min
- **Hand-check again** — 10 slips ÷ 4 per minute = 2.5 minutes; the backlog hits zero at minute 7.5

*Example (italic):* At minute 6 the backlog is 10 − 4 = 6 slips; at minute 7 it is 2; halfway through minute 8 the tray is empty.

**Key point:** Because actors share nothing, scaling is arithmetic, not surgery — you spawn another actor and split the mail instead of adding locks around shared state.

### Visualization (canvas `c2`, 720×300)

Line chart of mailbox backlog over 8 minutes: blue rising segment (one actor), spawn marker at minute 5, green falling segment (two actors) reaching zero at minute 7.5.

- **Title (bold 15px, `#1a5276`, top center):** "Mailbox Backlog: +2 Slips/min Alone, −4 Slips/min After the Spawn".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 8 with 12px `#444` tick labels every minute; y = slips waiting 0 to 12, gridlines `#e5e9ef` at 3/6/9/12 with 12px `#444` labels.
- **Rising line:** blue `#2a78d6` 3px line through (minute, backlog) points `[0, 1, 2, 3, 4, 5]`, backlog `[0, 2, 4, 6, 8, 10]`, with 4px blue dots at each point.
- **Falling line:** green `#008300` 3px line through minutes `[5, 6, 7, 7.5]`, backlog `[10, 6, 2, 0]`, with 4px green dots.
- **Spawn marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 5 from baseline to y=70, 12px `#6b7280` label "second actor spawned" at its top.
- **Point labels:** 11px `#2c3e50` backlog values "2, 4, 6, 8, 10" above the blue dots and "6, 2, 0" above the green dots.
- **Annotation (bold 13px green `#008300`, near minute 6.2, y=95):** "spawn, don't lock — empty by 7.5 min".
- **Caption (12px `#444`, bottom right):** "arrival and brew rates illustrative".

## Crashes Stay Small and Machines Look Alike

**Tags:** `where it's used` (blue), `let it crash` (green), `distribution` (orange)

- **Let it crash** — an Erlang actor that hits a bug simply dies; its private state dies with it alone
- **The supervisor** — a parent actor watches its workers and restarts a dead one with fresh state
- **No contagion** — with nothing shared, a crashed actor cannot corrupt what other actors hold
- **Distribution by default** — sending to an actor on another machine is the same send call as local
- **The heritage** — Ericsson built Erlang for telecom switches; nine-nines uptime was once reported
- **Where you meet it** — streaming pipelines, chat backends, per-user session workers, job queues

*Example (italic):* The tab actor for one table crashes at 14:03:07 and is restarted by its supervisor a second later — every other table's tab never notices.

**Key point:** Share-nothing message passing buys two things at once: a crash is contained to one mailbox, and because messages already cross actor boundaries, crossing machine boundaries needs no new code.

### Visualization (canvas `c3`, 720×300)

Supervisor tree diagram: one supervisor over three workers; the middle worker crashes and is restarted, while the third worker sits on a second machine reached by the identical message send.

- **Title (bold 15px, `#1a5276`, top center):** "One Supervisor, Three Workers: a Crash Restarts, a Remote Send Looks Local".
- **Supervisor:** violet `#4a3aa7` rounded box (150×40, fill `rgba(74,58,167,0.12)`) centered at (310, 75) labeled "supervisor" (12px `#2c3e50`); 2px `#6b7280` lines down to each worker.
- **Workers (y=175, boxes 140×44):** worker 1 blue `#2a78d6` (fill `rgba(42,120,214,0.15)`) at x=60 labeled "tab actor: table 4"; worker 2 red `#e74c3c` border (fill `rgba(231,76,60,0.12)`) at x=240 labeled "crashed 14:03:07" with bold 12px red "✗" at its corner; worker 3 blue at x=500 labeled "tab actor: table 9".
- **Restart:** green `#008300` rounded box (140×34, fill `rgba(0,131,0,0.12)`) at (240, 240) labeled "restarted 14:03:08" with bold 12px green `#008300` "✓ fresh state", curved 2px green arrow from supervisor to it.
- **Machine boundary:** dashed `#e5e9ef` rectangle around worker 3 (x 485–660, y 150–235), 11px `#6b7280` label "machine B" at its top; 2px aqua `#199e70` arrow from worker 1 to worker 3 labeled 11px "same send call" crossing the dashed border.
- **Annotation (bold 13px magenta `#d55181`, near (60, 275)):** "failure is contained to one mailbox — not the whole shop".
- **Caption (12px `#444`, bottom right):** "timestamps illustrative".

## A Message Is a Copy, Not a Shared Pointer

**Tags:** `common mistake` (red), `message copies` (orange)

- **The confusion** — newcomers treat a sent message like a shared reference both sides can edit
- **The truth** — the receiver gets its own copy; editing your original changes nothing they hold
- **The symptom** — you update "the" order after sending it and the barista still brews the old one
- **The cost** — copying means huge messages are expensive; send ids or deltas, not whole tables
- **The fix** — to change remote state, send another message; there is nothing shared to edit
- **The relapse** — routing everything through one giant actor quietly rebuilds the single lock

*Example (italic):* The customer scribbles "decaf" on their own copy of the slip after sending it — the barista's copy still says latte, so a second slip "change to decaf" is the only real fix.

**Common mistake:** Expecting actor messages to behave like shared memory. A message is a snapshot; the only way to affect another actor's state is to send it a new message it chooses to act on.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: editing your own copy after sending (barista never sees it) vs sending a follow-up message (barista updates), shown as order slips flowing into a mailbox.

- **Title (bold 15px, `#1a5276`, top center):** "Editing Your Copy Doesn't Edit Theirs — Send a New Message".
- **Row 1 (y=105), label 12px `#444` at x=20:** "expects shared memory"; blue `#2a78d6` rounded box at x=170 labeled "my slip: latte" (12px), 3px arrow labeled 11px `#6b7280` "send (copied)" to a blue box at x=390 labeled "barista's copy: latte"; below the first box a yellow `#c98500` box (fill `rgba(201,133,0,0.12)`) at (170, 140) labeled "I edit mine: decaf"; bold 12px red `#e74c3c` at x=560: "✗ still brews latte".
- **Row 2 (y=225), label:** "send a new message"; green `#008300` rounded box at x=170 labeled "slip 2: change to decaf" (fill `rgba(0,131,0,0.12)`), 3px green arrow to a green box at x=420 labeled "mailbox → decaf"; bold 12px green `#008300` at x=590: "✓ brews decaf".
- **Box style:** 150–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(201,133,0,0.12)`, 12px `#2c3e50` text.
- **Divider:** horizontal 1px `#e5e9ef` line at y=170 from x=20 to x=700.
- **Annotation (bold 13px orange `#d95926`, centered near y=285):** "state changes travel as messages — never as edits in place".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the backlog series is exactly minutes `[0,1,2,3,4,5]` → `[0,2,4,6,8,10]` then `[5,6,7,7.5]` → `[10,6,2,0]`, derived from illustrative rates of 8 slips/min arriving vs 6 brewed/min (then 12/min after the spawn); crash/restart timestamps in c3 are illustrative; diagram box positions are the pixel coordinates given per chart.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
