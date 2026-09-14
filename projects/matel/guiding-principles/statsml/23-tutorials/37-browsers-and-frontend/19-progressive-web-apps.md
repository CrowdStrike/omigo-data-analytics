# Progressive Web Apps

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Progressive Web Apps

**Subtitle:** A progressive web app is a website that installs to the home screen and keeps working offline — the web acting as a third mobile platform beside the two native app stores

## The Coffee App That Skipped the App Store

**Tags:** `core idea` (blue), `installable web` (green), `offline` (orange)

- **The chain** — a coffee chain's ordering site gains a home-screen icon and an offline mode
- **The install** — a commuter taps "Add to Home Screen"; no store search, no download bar
- **The tunnel** — the subway loses signal from 8:03 to 8:07; the menu still opens instantly
- **The trick** — a service worker script answers requests from a local cache when offline
- **The platform** — the same site now launches like an app on phones and desktops alike

*Example (italic):* The tunnel kills the signal at 8:03; the plain website shows an error page, while the installed coffee app shows the full menu.

**Key point:** A PWA is an ordinary website plus a manifest and a service worker — installed, it launches from the home screen and keeps working with no connection.

### Visualization (canvas `c1`, 720×300)

Timeline chart of one commute: percentage of screen taps that load, plain website vs installed PWA, with a shaded no-signal tunnel from 8:03 to 8:07.

- **Title (bold 15px, `#1a5276`, top center):** "The 8:03 Tunnel: the Website Goes Dark, the PWA Doesn't Notice".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = time "8:00" to "8:10" with 12px `#444` tick labels every 2 minutes; y = taps that load 0 to 100%, gridlines `#e5e9ef` at 25/50/75.
- **Tunnel band:** grey fill `rgba(107,114,128,0.12)` from minute 3 to minute 7, 12px `#6b7280` label "no signal (tunnel)" at its top.
- **Plain website line:** red `#e74c3c` 3px line through minutes `[0, 2, 3, 3.2, 4, 5, 6, 7, 8, 10]`, load success `[100, 100, 100, 0, 0, 0, 0, 100, 100, 100]` — vertical cliff to 0 entering the tunnel, back to 100 on exit.
- **PWA line:** green `#008300` 3px line through the same minute grid, load success `[100, 100, 100, 100, 100, 100, 100, 100, 100, 100]` — flat.
- **Annotation (bold 13px green `#008300`, near minute 5, y=90):** "cached menu loads all the way through the tunnel".
- **Caption (12px `#444`, bottom right):** "load success illustrative".

## Manifest, Service Worker, Cache: Tracing an Offline Latte

**Tags:** `worked example` (blue), `three pieces` (green)

- **The manifest** — a small JSON file (name, icon, colors) is what makes the site installable
- **The service worker** — a short script that sits between the app and the network on every request
- **The cache** — on first visit the worker stores 12 files (about 1.2 MB): app shell, menu, images
- **The offline order** — at 8:07 a latte order is written to a local outbox instead of being sent
- **The sync** — at 8:14, signal back, the worker replays the outbox and the order arrives exactly once

*Example (italic):* First visit caches 12 files (about 1.2 MB); every later launch — online or not — draws the menu from that cache in under a second.

**Key point:** Each piece adds one power — the manifest makes it installable, the service worker intercepts requests, the cache answers them; together that is the whole PWA recipe.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram: the same "order a latte" tap taken online (straight to the server) vs offline (cache serves the menu, order parked in an outbox, synced later).

- **Title (bold 15px, `#1a5276`, top center):** "One Tap, Two Paths: the Service Worker Decides".
- **Row 1 (y=95), label 12px `#444` at x=20:** "online, 8:01"; blue `#2a78d6` rounded box at x=130 labeled "tap Order" (12px), 3px arrow to a violet `#4a3aa7` box at x=310 labeled "service worker", 3px arrow to a green `#008300` box at x=510 labeled "network → server ✓"; small 11px `#6b7280` note under the violet box: "copy of response saved to cache".
- **Row 2 (y=205), label:** "offline, 8:07"; blue box "tap Order", arrow to violet box "service worker", arrow to an orange `#d95926` box at x=470 labeled "cache serves menu; order → outbox", then a dashed 2px `#008300` arrow curving to 12px green text "8:14 sync replays outbox ✓" near (x=560, y=260).
- **Box style:** 120–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(74,58,167,0.12)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=30 under the title):** "the app never talks to the network directly — the worker always answers".
- **Caption (12px `#444`, bottom right):** "12 cached files, ~1.2 MB — illustrative".

## One Codebase, No Gatekeeper, No Download

**Tags:** `where it's used` (blue), `distribution` (green), `third platform` (orange)

- **No gatekeeper** — publishing is a deploy, not a store review; a fix ships in minutes, not days
- **One codebase** — the same HTML and JS serves every phone and desktop; no second native team
- **No download** — the coffee PWA caches 1.2 MB vs a 48 MB native app install
- **The funnel** — each install step loses roughly 20% of users; the store route has four drop-off points
- **The reach** — a link in a text message is the entire distribution channel

*Example (italic):* Of 100 customers who tap "get the app", about 41 finish the app-store route while about 95 are ordering on the PWA within a minute.

**Key point:** The web becomes the third mobile platform when a URL delivers what once required a store — an icon, offline behavior, push — with almost none of the install friction.

### Visualization (canvas `c3`, 720×300)

Horizontal funnel bars: 100 customers entering each install route, showing how many survive every step of the app-store path vs the two-step PWA path.

- **Title (bold 15px, `#1a5276`, top center):** "100 Customers Tap 'Get the App': Who Reaches the First Order?".
- **Axis:** bars start at x=230, extend right, max width 440 (= 100 customers); left-aligned 12px `#444` step labels at x=20; 11px `#444` count labels at bar ends.
- **App-store route (rows at y = 55, 82, 109, 136, 163), blue `#2a78d6` bars, fill `rgba(42,120,214,0.30)`, 14px tall:**
  - "store listing opens": width 440 (100)
  - "download starts": width 352 (80)
  - "install finishes": width 282 (64)
  - "app opened": width 224 (51)
  - "account created": width 180 (41), this bar's edge stroked red `#e74c3c`
- **PWA route (rows at y = 215, 242), green `#008300` bars, fill `rgba(0,131,0,0.30)`, 14px tall:**
  - "link opens": width 440 (100)
  - "ordering on the site": width 418 (95)
- **Group labels (bold 12px `#1a5276`):** "via app store" at (x=20, y=40); "via PWA link" at (x=20, y=200).
- **Annotation (bold 13px magenta `#d55181`, right side near y=190):** "the store route loses 59 of 100 before the first order".
- **Caption (12px `#444`, bottom right):** "~20% loss per step, counts illustrative".

## A Responsive Site Is Not a PWA

**Tags:** `common mistake` (red), `iOS limits` (orange)

- **The confusion** — a responsive site resizes for phones; a PWA installs and runs offline
- **The test** — switch on airplane mode: a responsive-only site shows the browser error page
- **Missing pieces** — no manifest means no install prompt; no service worker means no offline
- **The iOS catch** — iOS runs installed PWAs but omits background sync and trims push and storage
- **The mistake** — shipping a "PWA" that is really a mobile-friendly page behind an icon

*Example (italic):* The coffee chain's first "app" was the responsive site bookmarked to the home screen — in the tunnel it showed the same error page as the browser.

**Common mistake:** Calling any mobile-friendly site a PWA. Without a manifest and a service worker there is no install and no offline — and even a real PWA gets a reduced feature set on iOS.

### Visualization (canvas `c4`, 720×300)

Capability matrix: four PWA capabilities down the side, three columns (responsive site, PWA on Android, PWA on iOS), with check / limited / cross marks.

- **Title (bold 15px, `#1a5276`, top center):** "Same Site, Three Very Different Feature Sets".
- **Column headers (bold 12px `#1a5276`, centered at x = 300, 460, 620, y=65):** "responsive site", "PWA (Android)", "PWA (iOS)".
- **Row labels (12px `#2c3e50`, left-aligned at x=20, rows at y = 105, 145, 185, 225):** "home-screen install", "works offline", "push notifications", "background sync".
- **Marks (bold 16px, centered on each column x at each row y):**
  - home-screen install: red `#e74c3c` "✗" (bookmark only), green `#008300` "✓", green "✓"
  - works offline: red "✗", green "✓", green "✓"
  - push notifications: red "✗", green "✓", orange `#d95926` "△ installed only"  (the "△" 16px, the note 11px beside it)
  - background sync: red "✗", green "✓", red "✗"
- **Grid:** light `#e5e9ef` 1px horizontal rules between rows; faint `rgba(42,120,214,0.06)` fill behind the "PWA (Android)" column.
- **Annotation (bold 13px orange `#d95926`, centered near y=265):** "iOS ships the core, trims the edges — design for the smallest set".
- **Caption (12px `#444`, bottom right):** "capability support as documented per platform".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); commute load-success percentages, the cache figures (12 files / ~1.2 MB vs a 48 MB native install), and the funnel counts (100/80/64/51/41 vs 100/95, ~20% loss per step) are invented and labeled illustrative; the c4 capability matrix reflects documented platform support (no background sync on iOS; push for installed web apps only).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
