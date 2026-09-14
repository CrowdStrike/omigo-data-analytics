# Optimistic Updates

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Optimistic Updates

**Subtitle:** The UI lies first and confirms later — the screen shows the result of your click immediately, while the real request is still traveling to the server

## The Like Button That Doesn't Wait

**Tags:** `core idea` (blue), `instant feedback` (green), `in flight` (orange)

- **The tap** — you tap the heart on a photo; the heart fills red and the count ticks 41 → 42 instantly
- **The truth** — at that moment the server has no idea you liked anything; the request just left
- **The lie** — the UI showed the outcome it *expects*, not the outcome that *happened*
- **The bet** — likes succeed almost every time, so pretending success is usually safe
- **The settle** — when the server's reply arrives, the screen either stays put (confirm) or snaps back (reject)

*Example (italic):* The heart fills the instant your finger lifts, even though the server won't record the like for another 400ms.

**Key point:** An optimistic update applies the expected result to the screen immediately and treats the server's reply as a later confirmation — not as the trigger for the change.

### Visualization (canvas `c1`, 720×300)

Two-lane timeline of the same tap: an optimistic UI fills the heart at 16ms, a wait-for-server UI fills it at 400ms, with the request/response arrows drawn between the lanes.

- **Title (bold 15px, `#1a5276`, top center):** "One Tap, Two UIs: Show It Now vs Show It When the Server Says So".
- **Axes:** horizontal time axis, 2px `#999` baseline at y=250, from x=60 to x=660 mapping 0ms → 500ms; 12px `#444` tick labels at 0 / 100 / 200 / 300 / 400 / 500ms; vertical gridlines `#e5e9ef` at each tick.
- **Lane 1 (y=110), label bold 12px `#2a78d6` at x=20:** "optimistic"; hollow heart glyph (2px `#6b7280` outline) at x for 0ms, filled heart (solid `#d55181`) drawn at x for 16ms with 12px `#d55181` label "filled at 16ms"; lane line 3px `#2a78d6` from 16ms to 500ms.
- **Lane 2 (y=190), label bold 12px `#6b7280` at x=20:** "wait for server"; hollow heart at 0ms, greyed dashed 2px `#6b7280` lane line (dash 4/3) from 0 to 400ms, filled heart (solid `#d55181`) at 400ms with 12px `#6b7280` label "filled at 400ms".
- **Request arrow:** 2px `#c98500` arrow from (16ms, y=125) down-right to (400ms, y=240) labeled 12px `#c98500` "request in flight"; response tick bold 12px `#008300` "✓ server confirms" at (400ms, y=245).
- **Annotation (bold 13px green `#008300`, near x=200ms, y=70):** "the screen is 384ms ahead of the truth".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## The 400 Milliseconds Nobody Waits For

**Tags:** `worked example` (blue), `timeline` (green), `rollback` (red)

- **t=0ms** — finger lifts off the heart; the click handler runs
- **t=16ms** — next frame paints: heart filled, count 41 → 42, and the POST request goes out
- **t=400ms** — happy path: server replies 200 OK; the screen already matches, nothing changes
- **Reject path** — server replies 403 at 400ms (post was deleted); the UI must undo its own lie
- **t=416ms** — rollback frame paints: heart empties, count 42 → 41, a small "couldn't like" notice shows
- **Hand-check** — waiting-first feedback: 400ms; optimistic feedback: 16ms — a 25× faster response

*Example (italic):* On the happy path the confirm at 400ms is invisible; on the reject path the heart is red for 400ms and then snaps back to 41.

**Key point:** Optimistic code always has two endings — confirm (do nothing, screen already right) and reject (roll the screen back to the saved pre-click state).

### Visualization (canvas `c2`, 720×300)

Step chart of the on-screen like count over time, success path vs reject path, both stepping 41 → 42 at 16ms — only the reject path steps back down at 416ms.

- **Title (bold 15px, `#1a5276`, top center):** "What the Count Shows: Confirm Keeps 42, Reject Rolls Back to 41".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = 0 to 600ms, 12px `#444` tick labels every 100ms; y = like count 40 to 43, gridlines `#e5e9ef` at 41 / 42.
- **Success path:** green `#008300` 3px step line through (ms, count) points `[0, 16, 400, 600]` → `[41, 42, 42, 42]` (vertical step at 16ms), 12px `#008300` label "confirmed — stays 42" near x=480, on the 42 level.
- **Reject path:** orange `#d95926` 3px step line, drawn 3px below the success line where they overlap, through points `[0, 16, 416, 600]` → `[41, 42, 41, 41]` (step up at 16ms, step down at 416ms), 12px `#d95926` label "rolled back to 41" near x=490 on the 41 level.
- **Event markers:** vertical dashed `#6b7280` lines (dash 4/3) at 16ms and 400ms, 12px `#6b7280` labels "UI updates (16ms)" and "server replies (400ms)" at their tops.
- **Annotation (bold 13px violet `#4a3aa7`, near x=200ms, y=75):** "for 400ms both paths look identical".
- **Caption (12px `#444`, bottom right):** "counts and timings illustrative".

## Why Apps Feel Fast (Even on Slow Networks)

**Tags:** `where it's used` (blue), `perceived speed` (green), `offline-first` (orange)

- **The threshold** — feedback within ~100ms feels instant to people; a network round trip rarely fits in that
- **The trick** — optimistic UIs pin feedback at one frame (~16ms) no matter how slow the network is
- **Slow networks** — on a weak connection the gap grows: the server takes 2500ms but the heart still fills at 16ms
- **Offline-first** — with no connection at all, the tap is queued locally and synced later; the UI never blocks
- **Everywhere** — likes, upvotes, to-do checkboxes, drag-and-drop reorders, chat "sent" bubbles all do this

*Example (italic):* On a weak hotel connection the like takes 2500ms to reach the server, yet the heart fills in 16ms — the app feels exactly as fast as it does at home.

**Key point:** Optimism decouples felt speed from network speed — the UI's response time is one frame, and only the (usually invisible) confirmation rides the network.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: time until the user sees feedback, wait-for-server vs optimistic, across four network conditions.

- **Title (bold 15px, `#1a5276`, top center):** "Time Until the Heart Fills: Waiting Scales With the Network, Optimism Doesn't".
- **Axis:** bars start at x=210, max width 430 mapping 0 → 2500ms linearly; 2px `#999` vertical baseline at x=210; 12px `#444` scale labels 0 / 1000 / 2000ms along y=260.
- **Rows (label 12px `#444` at x=20; two 14px-tall bars per row, wait bar on top, optimistic bar 18px below):**
  - "home wifi" (y=70): wait bar blue `#2a78d6` width for 400ms (≈69px), 11px label "400ms"; optimistic bar green `#008300` width for 16ms (min 4px), 11px label "16ms"
  - "mobile 4G" (y=118): wait bar blue for 900ms (≈155px) "900ms"; optimistic green 16ms "16ms"
  - "weak signal" (y=166): wait bar orange `#d95926` for 2500ms (430px) "2500ms"; optimistic green 16ms "16ms"
  - "offline" (y=214): wait bar hatched/dashed outline `#6b7280` full 430px width, 11px `#6b7280` label "blocked — never fills"; optimistic green 16ms with 11px `#008300` label "16ms, queued to sync"
- **Annotation (bold 13px green `#008300`, right side near y=118):** "optimistic feedback is 16ms on every row".
- **Caption (12px `#444`, bottom right):** "round-trip times illustrative".

## Forgetting That the Server Can Say No

**Tags:** `common mistake` (red), `rollback` (orange), `drift` (red)

- **The shortcut** — the happy path is coded, the reject path is skipped: no saved pre-click state, no undo
- **Silent failure** — a like fails server-side but the heart stays red; the user believes a lie forever
- **Drift** — each unhandled failure leaves the client count one ahead; errors accumulate, never cancel
- **The refresh tell** — reloading the page snaps the count to server truth and the user sees it "lose" likes
- **The fix** — snapshot state before applying, roll back on error, and reconcile with the server's count on response
- **Double-tap trap** — optimistic toggles also need in-flight guards, or two quick taps race each other

*Example (italic):* After 10 optimistic taps of which 2 were rejected with no rollback, the screen says 50 while the server says 48 — the refresh reveals the gap.

**Common mistake:** Shipping only the optimistic half. Without a rollback path and reconciliation, every server rejection becomes permanent on-screen fiction that a refresh embarrassingly corrects.

### Visualization (canvas `c4`, 720×300)

Step chart of client count vs server count across 10 taps where taps 4 and 8 are rejected and never rolled back — the lines drift 2 apart.

- **Title (bold 15px, `#1a5276`, top center):** "No Rollback: Client and Server Drift Apart One Failure at a Time".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = tap number 1 to 10, 12px `#444` tick labels at each tap; y = like count 40 to 51, gridlines `#e5e9ef` at 42 / 44 / 46 / 48 / 50.
- **Client line:** blue `#2a78d6` 3px step line through taps `[0,1,2,3,4,5,6,7,8,9,10]`, counts `[40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50]` — climbs every tap, 12px `#2a78d6` label "screen shows 50" at the right end.
- **Server line:** green `#008300` 3px step line, counts `[40, 41, 42, 43, 43, 44, 45, 46, 46, 47, 48]` — flat at taps 4 and 8, 12px `#008300` label "server has 48" at the right end.
- **Failure markers:** red `#e74c3c` × marks (12px, 2px stroke) on the server line at taps 4 and 8, bold 11px `#e74c3c` labels "rejected, not rolled back".
- **Annotation (bold 13px red `#e74c3c`, near tap 9, y=80):** "refresh will snap 50 → 48".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the running example's timings (click 0ms, paint 16ms, server reply 400ms, rollback 416ms), the per-network feedback times (400 / 900 / 2500ms vs 16ms), and the drift counts (client `[40..50]`, server `[40,41,42,43,43,44,45,46,46,47,48]` with failures at taps 4 and 8) are invented and labeled illustrative; the ~100ms "feels instant" threshold and 16ms frame time are standard UI figures.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
