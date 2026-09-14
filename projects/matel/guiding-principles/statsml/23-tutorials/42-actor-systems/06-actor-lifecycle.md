# Actor Lifecycle

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Actor Lifecycle

**Subtitle:** An actor is born, works, crashes, and is reborn at the same address — its mailbox survives the restart, its in-memory state does not

## A Cart Actor Is Born, Works, and Dies

**Tags:** `core idea` (blue), `lifecycle hooks` (green), `restart` (orange)

- **The actor** — an online store spawns one shopping-cart actor per shopper the moment they add a first item
- **preStart** — runs once at birth: the cart opens an empty item list and logs "cart created"
- **The loop** — the actor then does one thing forever: take the next message from its mailbox, handle it
- **The crash** — a malformed message throws; the supervisor replaces the actor with a fresh instance
- **postStop** — runs at the true end (checkout or timeout): release resources, log "cart closed"

*Example (italic):* Shopper #4127 adds an item at 2:01pm — preStart fires, the cart actor lives at `/user/cart-4127` and starts handling AddItem messages one at a time.

**Key point:** An actor's life is a fixed script — preStart, a receive loop, optional crash-and-restart cycles, postStop — and the restart cycle is the part everyone underestimates.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the lifecycle: a left-to-right main row (preStart → receive → postStop) with a crash/restart loop arcing above the receive box.

- **Title (bold 15px, `#1a5276`, top center, y=25):** "One Actor's Life: preStart → receive → (crash, restart) → postStop".
- **Main row (boxes 150×46, 8px radius, 12px `#2c3e50` two-line labels, y=170):** green-fill `rgba(0,131,0,0.12)` box at x=50 "preStart — cart created"; blue-fill `rgba(42,120,214,0.15)` box at x=285 "receive — handles messages"; grey-fill `rgba(107,114,128,0.12)` box at x=520 "postStop — cart closed".
- **Main arrows:** 3px `#2c3e50` horizontal arrows between the three boxes.
- **Crash loop above:** red 3px arrow from the receive box top up-left to a red-fill `rgba(231,76,60,0.12)` box (150×40) at x=140, y=70 labeled "crash on a message" (12px red `#e74c3c` word "crash" beside the arrow); 3px arrow right to a green-fill box (170×40) at x=420, y=70 labeled "restart: fresh instance"; 3px green arrow back down into the receive box top.
- **Annotation (bold 13px orange `#d95926`, centered, y=280):** "the mailbox lives outside this loop — it survives the restart".
- **Caption (12px `#444`, bottom right):** "hook names from Akka; the pattern is generic".

## The Crash at Message Five

**Tags:** `worked example` (blue), `state loss` (red)

- **The stream** — shopper #4127 fires 8 AddItem messages in a burst; all 8 land in the mailbox
- **Messages 1–4** — each adds one item; after message 4 the in-memory cart holds 4 items
- **Message 5** — malformed payload, the handler throws, the supervisor restarts the actor
- **The wipe** — the fresh instance starts with an empty list: the 4 items existed only in memory
- **Messages 6–8** — still queued in the surviving mailbox, they process normally: cart ends at 3 items

*Example (italic):* 8 messages sent, 7 processed, message 5 dropped — the cart ends the burst holding 3 items, not the 7 the shopper added.

**Key point:** The restart wiped 4 items of in-memory state but lost zero queued messages — the mailbox belongs to the actor's address, not to the crashed instance.

### Visualization (canvas `c2`, 720×300)

Step line chart of items in the cart across the 8-message burst: blue steps up to 4, a red crash cliff to 0 at message 5, green steps back up to 3.

- **Title (bold 15px, `#1a5276`, top center):** "Message 5 Crashes the Cart: Items Reset to 0, Mailbox Loses Nothing".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = message number 1 to 8 with 12px `#444` tick labels at every message; y = items in cart 0 to 5, gridlines `#e5e9ef` at 1/2/3/4.
- **Before-crash line:** blue `#2a78d6` 3px step line through messages `[1, 2, 3, 4]`, items `[1, 2, 3, 4]`.
- **Crash marker:** vertical dashed `#e74c3c` (dash 4/3) line at message 5 from items 4 down to 0, red 14px "✗" at its top, bold 12px red label "crash — restart, state wiped" beside it.
- **After-restart line:** green `#008300` 3px step line through messages `[6, 7, 8]`, items `[1, 2, 3]`.
- **Annotation (bold 13px green `#008300`, near message 6.5, y=70):** "messages 6–8 were waiting in the mailbox — none lost".
- **Annotation (bold 12px red `#e74c3c`, below the crash line, y=225):** "message 5 itself is dropped".
- **Caption (12px `#444`, bottom right):** "item counts illustrative".

## Same Address, Fresh Memory

**Tags:** `where it's used` (blue), `supervision` (green)

- **The design** — supervisors restart a broken actor instead of letting one bad message kill the app
- **Stable address** — the path `/user/cart-4127` and every ActorRef held by senders keep working
- **Invisible to senders** — other actors keep sending to the same ref; they never learn a crash happened
- **The rule** — anything worth keeping across a restart must live outside the actor: a DB, an event log
- **Where you meet it** — streaming pipeline workers, game session actors, per-device IoT handlers

*Example (italic):* The checkout actor keeps messaging `/user/cart-4127` all afternoon; across two silent restarts, its reference never went stale.

**Key point:** A restart swaps the instance behind an address, not the address itself — the identity, mailbox, and supervision links persist while the memory starts over.

### Visualization (canvas `c3`, 720×300)

Two-column comparison diagram: green boxes listing what survives a restart, red boxes listing what is gone with the old instance.

- **Title (bold 15px, `#1a5276`, top center):** "After a Restart: What Survives vs What Is Gone".
- **Column headers (bold 13px, y=60):** green `#008300` "survives the restart" centered at x=190; red `#e74c3c` "gone with the old instance" centered at x=530.
- **Left column (boxes 280×38, 8px radius, x=50, y = 85 / 140 / 195, fill `rgba(0,131,0,0.12)`, 12px `#2c3e50` text):** "actor path — /user/cart-4127"; "the mailbox and every queued message"; "the ActorRef held by senders".
- **Right column (boxes 280×38, x=400, same y values, fill `rgba(231,76,60,0.12)`):** "in-memory state — the 4 cart items"; "the message that caused the crash"; "running timers and scheduled ticks".
- **Annotation (bold 13px violet `#4a3aa7`, centered, y=265):** "same address, same queue — brand-new memory".
- **Caption (12px `#444`, bottom right):** "cart example illustrative".

## The Restart Is Not a Rewind

**Tags:** `common mistake` (red), `lost message` (orange)

- **The assumption** — people expect the restarted actor to retry message 5 and keep its 4 items
- **Neither happens** — the failing message is dropped by default, and the item list is rebuilt empty
- **preRestart / postRestart** — hooks on the old and new instance for cleanup and re-initialization
- **The fix** — persist each add as an event and replay in postRestart, or park bad messages aside
- **The tell** — "counts drift down after deploys or error spikes" is often silent restart state loss

*Example (italic):* A dashboard shows carts mysteriously shrinking during an error spike — each restart quietly rebuilt a cart from nothing while shoppers kept clicking.

**Common mistake:** Treating a restart as a rewind. The mailbox picks up where it left off — the state does not, and the failing message is dropped, not retried; anything only in memory must be persisted or replayed.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the expected restart behavior (retry message 5, keep 4 items — marked as wrong) vs the actual behavior (drop message 5, fresh empty cart, resume at message 6).

- **Title (bold 15px, `#1a5276`, top center):** "The Restart Illusion: Message 5 Is Not Retried, State Is Not Restored".
- **Row 1 (y=95), label 12px `#444` at x=20:** "what people expect"; blue-fill `rgba(42,120,214,0.15)` rounded box (150×40) at x=160 labeled "cart: 4 items" (12px), 3px arrow to a grey-fill `rgba(107,114,128,0.12)` box (170×40) at x=350 labeled "retry msg 5, keep items", bold 12px red `#e74c3c` "✗ neither happens" at x=560.
- **Row 2 (y=205), label:** "what happens"; red-fill `rgba(231,76,60,0.12)` box (150×40) at x=160 labeled "crash on msg 5 — dropped", 3px arrow to a green-fill `rgba(0,131,0,0.12)` box (150×40) at x=350 labeled "fresh cart: 0 items", 3px arrow to a blue-fill box (160×40) at x=540 labeled "resumes at msg 6" with bold 12px green `#008300` "✓" beside it.
- **Box style:** 8px radius, 12px `#2c3e50` text, arrows 3px `#2c3e50`.
- **Annotation (bold 13px orange `#d95926`, centered, y=272):** "if state only lived in memory, the restart erased it — persist or replay".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 8-message burst, item counts (1–4 before the crash, 1–3 after) and the crash at message 5 are invented and labeled illustrative; lifecycle hook names (preStart, preRestart, postRestart, postStop) follow Akka's classic API, and the survives/lost split (mailbox and path survive, in-memory state and the failing message do not) matches its documented default restart semantics.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
