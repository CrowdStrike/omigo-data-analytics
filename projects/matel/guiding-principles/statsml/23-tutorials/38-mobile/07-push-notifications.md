# Push Notifications

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Push Notifications

**Subtitle:** Your phone isn't listening to every app's server — Apple and Google are, and they relay each message over the one connection the phone already keeps open

## The Order-Shipped Message's Three-Hop Journey

**Tags:** `core idea` (blue), `push path` (green), `APNs / FCM` (orange)

- **The order** — a coffee-gear shop ships Maya's grinder at 3:12pm and wants her phone to say so
- **The wrong guess** — the shop's app isn't running and isn't polling; the phone never asks the shop anything
- **Hop 1** — the shop's server posts the message to Apple's APNs (or Google's FCM for Android phones)
- **Hop 2** — APNs/FCM routes it down the single always-on connection the phone's OS keeps open
- **Hop 3** — the OS itself draws the banner and plays the sound; the shop's app stays asleep throughout

*Example (italic):* At 3:12pm the shop's server sends one HTTPS request to APNs; two seconds later Maya's lock screen reads "Your grinder shipped" — and the shop's app never ran.

**Key point:** A push travels shop server → Apple/Google's push service → the phone's one always-on connection → the OS-drawn banner. The app did nothing; the platform relayed everything.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram of the three hops, with a greyed-out "the app" box underneath showing it stays asleep.

- **Title (bold 15px, `#1a5276`, top center):** "One Push, Three Hops: Server → APNs/FCM → OS → Lock Screen".
- **Boxes (y=120, 44px tall, 8px radius, 12px `#2c3e50` text, fills `rgba(42,120,214,0.15)` with 2px `#2a78d6` borders):** "shop server" at x=30 w=130; "APNs / FCM" at x=215 w=130; "phone OS (one open connection)" at x=400 w=150; "lock-screen banner" at x=605 w=95 with green `#008300` border.
- **Arrows:** 3px `#1a5276` arrows between consecutive boxes; 11px `#6b7280` labels above each: "HTTPS + device token", "the always-on pipe", "OS draws it".
- **Asleep box:** dashed 1px `#6b7280` box at x=400 y=200 w=150 h=36 labeled "the shop's app — asleep" in 12px `#6b7280`.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "the app slept through all three hops".
- **Caption (12px `#444`, bottom right):** "APNs/FCM relay path is documented platform behavior".

## The Token, the Payload, and the Battery Math

**Tags:** `worked example` (blue), `device token` (green), `battery` (orange)

- **The token** — at install, the OS asks APNs/FCM for a device token, an opaque address (~64 hex characters on APNs)
- **The handoff** — the app uploads that token to the shop's server, which files it under Maya's account
- **The payload** — at ship time the server sends the token plus a small JSON body, capped at 4 KB by the platforms
- **One pipe** — the OS holds ONE persistent connection for all apps; every app's pushes arrive through it
- **The math** — 40 apps each keeping their own socket means 40 radio wake-up schedules; sharing cuts it to 1
- **The cost** — the shared pipe idles at ~2% battery a day; 40 separate keep-alives would burn ~30% (illustrative)

*Example (italic):* payload: `{"title": "Order shipped", "body": "Grinder on the way", "badge": 1}` — about 70 bytes of the 4 KB allowance, addressed to Maya's 64-character token.

**Key point:** The token is the address, the payload is the letter, and the shared OS connection is why the whole system exists — one radio pipe for every app instead of forty.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart comparing daily battery cost: one shared OS connection vs 40 per-app connections, at a shared linear scale (14px per battery %).

- **Title (bold 15px, `#1a5276`, top center):** "One Shared Connection vs 40 Per-App Connections (battery per day)".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420.
- **Rows (bar height 22px), each with a left-aligned 12px `#444` label at x=20:**
  - y=110 "1 shared OS connection (1 socket)": green `#008300` bar width 28 (= 2%), 12px green label "~2%/day" at bar end
  - y=190 "40 per-app connections (40 sockets)": orange `#d95926` bar width 420 (= 30%), 12px orange label "~30%/day" right-aligned inside the bar end
- **Bar fills:** `rgba(0,131,0,0.30)` and `rgba(217,89,38,0.30)` with solid 2px borders in the row color.
- **Annotation (bold 13px blue `#2a78d6`, near x=300, y=70):** "this is why Apple and Google sit in the middle".
- **Caption (12px `#444`, bottom right):** "battery percentages illustrative; the 4 KB payload cap is the documented limit".

## From 100 Sends to 9 Taps: Best Effort, Not a Promise

**Tags:** `where it's used` (blue), `engagement` (green), `best effort` (orange)

- **Reach** — a push is the shop's only channel to someone who hasn't opened the app in weeks
- **No promise** — Apple and Google document delivery as best-effort: offline phones, stale tokens, opt-outs
- **The funnel** — of 100 "order shipped" pushes, 94 are accepted, 86 reach a phone, 62 are seen, 9 are tapped
- **Only-latest** — for an offline phone, APNs stores just the most recent push per app; older ones are dropped
- **Design rule** — never send anything critical solely as a push; pair it with in-app state or email

*Example (italic):* Of the shop's 100 pushes one morning, 6 bounce on stale tokens, 8 more never reach a switched-off phone, and only 9 end in a tap — and no step past "sent" was guaranteed.

**Key point:** Push is the highest-reach, lowest-guarantee channel a product has — treat every hop after "sent" as probabilistic and keep a second channel for anything that must arrive.

### Visualization (canvas `c3`, 720×300)

Horizontal funnel of the 100 pushes, one bar per stage, widths at 4.4px per push.

- **Title (bold 15px, `#1a5276`, top center):** "The Shop's 100 Pushes: the Funnel Nobody Guarantees".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; bar height 20px.
- **Rows (top to bottom at y = 70, 110, 150, 190, 230), each with a left-aligned 12px `#444` label at x=20 and an 11px count label at the bar end:**
  - "sent by the server": blue `#2a78d6` bar width 440 — "100"
  - "accepted by APNs/FCM": aqua `#199e70` bar width 414 — "94 (6 stale tokens)"
  - "delivered to a phone": green `#008300` bar width 378 — "86 (8 offline)"
  - "seen on the lock screen": yellow `#c98500` bar width 273 — "62"
  - "tapped": magenta `#d55181` bar width 40 — "9"
- **Bar fills:** row color at 0.30 alpha with a solid 2px border in the row color.
- **Annotation (bold 13px orange `#d95926`, near x=300, y=262):** "no hop past 'sent' is guaranteed".
- **Caption (12px `#444`, bottom right):** "counts illustrative; best-effort delivery is the documented contract".

## A Push Is Not Your App Running

**Tags:** `common mistake` (red), `silent push` (orange)

- **The confusion** — people picture the app "listening in the background"; the OS listens, once, for everyone
- **The banner** — a normal push is drawn by the OS; the app's code runs only if the user taps it
- **Silent pushes** — a payload can wake the app briefly with no banner, but the OS rations these strictly
- **The throttle** — Apple documents background pushes as budgeted; the OS defers or drops the excess
- **The mistake** — building "real-time" sync on silent pushes, then wondering why updates arrive hours late

*Example (italic):* The shop sends a silent push per price change — 30 an hour; the OS wakes the app for a handful and defers the rest, so carts sync stale all afternoon.

**Common mistake:** Treating a push as remote code execution. The OS shows the banner itself, wakes the app only on tap or within a strict silent-push budget, and quietly defers everything beyond it.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: what people imagine a push does vs what actually happens.

- **Title (bold 15px, `#1a5276`, top center):** "The Banner Is the OS's Work, Not the App's".
- **Row 1 (y=95), label 12px `#444` at x=20:** "what people imagine"; blue `#2a78d6` rounded box at x=170 labeled "push arrives" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "app wakes, runs code freely" with bold 12px red "✗ the app is asleep".
- **Row 2 (y=205), label:** "what actually happens"; blue box "push arrives" at x=170, 3px arrow to a green `#008300` box at x=360 labeled "OS draws the banner", then arrow to a green box at x=555 labeled "tap → app launches" with bold 12px green "✓".
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=272):** "silent pushes can wake the app — but the OS budgets them and defers the excess".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); battery percentages (2% / 30%) and the funnel counts (100 / 94 / 86 / 62 / 9) are invented and labeled illustrative; the 4 KB payload cap, the ~64-hex-character APNs token, the store-only-the-latest-push behavior, and silent-push budgeting are documented platform facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
