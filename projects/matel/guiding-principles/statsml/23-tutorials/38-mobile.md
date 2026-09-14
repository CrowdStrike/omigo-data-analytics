# Mobile

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Mobile

**Subtitle:** How phone apps get built, behave on the device, and ship through the app stores — and why mobile plays by different rules than the web.

## Cards

Each card links to a topic page under `mobile/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | BUILDING APPS | Native, Web, Hybrid | [38-mobile/01-native-web-hybrid.md](38-mobile/01-native-web-hybrid.md) | Three ways to build the same app — write it twice for each phone, once for the browser, or once for a tool that runs on both. | native, web app, hybrid |
| 2 | BUILDING APPS | iPhone App 101 | [38-mobile/02-iphone-app-101.md](38-mobile/02-iphone-app-101.md) | With Swift and Xcode, one screen — a working tip calculator — goes from empty project to running app in an afternoon. | Swift, Xcode, first app |
| 3 | BUILDING APPS | Android App 101 | [38-mobile/03-android-app-101.md](38-mobile/03-android-app-101.md) | Your first Android app is one Kotlin file describing one screen — Android Studio builds it, and the same screen later runs on a billion different phones. | Kotlin, Android Studio, emulator |
| 4 | BUILDING APPS | The App Lifecycle | [38-mobile/04-the-app-lifecycle.md](38-mobile/04-the-app-lifecycle.md) | The operating system can kill an app any moment it is off screen — good apps save their work as if every switch away were the last. | foreground, background, state saving |
| 5 | BUILDING APPS | Cross-Platform Frameworks | [38-mobile/05-cross-platform-frameworks.md](38-mobile/05-cross-platform-frameworks.md) | React Native and Flutter let one team write an app once and ship it to both app stores — one codebase, two phones. | React Native, Flutter, one codebase |
| 6 | ON THE DEVICE | The Permissions Model | [38-mobile/06-the-permissions-model.md](38-mobile/06-the-permissions-model.md) | Phones make every app ask before touching the camera, mic, or location — and when the app asks decides whether the user says yes. | runtime prompts, camera, location |
| 7 | ON THE DEVICE | Push Notifications | [38-mobile/07-push-notifications.md](38-mobile/07-push-notifications.md) | Your phone isn't listening to every app's server — Apple and Google relay each message over the one connection the phone already keeps open. | APNs, FCM, relay |
| 8 | ON THE DEVICE | Offline-First & Sync | [38-mobile/08-offline-first-and-sync.md](38-mobile/08-offline-first-and-sync.md) | An offline-first app treats the phone's local database as the real one — every edit saves instantly, and a sync engine reconciles with the server later. | local database, sync engine, conflicts |
| 9 | ON THE DEVICE | Mobile Constraints | [38-mobile/09-mobile-constraints.md](38-mobile/09-mobile-constraints.md) | A phone runs on a battery, a radio, and a network that comes and goes — so good mobile code does less, later, in batches. | battery, radio, batching |
| 10 | SHIPPING & STORES | Code Signing & Provisioning | [38-mobile/10-code-signing-and-provisioning.md](38-mobile/10-code-signing-and-provisioning.md) | A signed app carries cryptographic proof of who built it — the phone checks that proof before it will install anything. | certificates, provisioning, identity |
| 11 | SHIPPING & STORES | App Store Submission & Review | [38-mobile/11-app-store-submission-and-review.md](38-mobile/11-app-store-submission-and-review.md) | Every mobile app update passes through a human review gate before users can get it — so releases take days, not the minutes a web deploy takes. | review gate, days not minutes, rejections |
| 12 | SHIPPING & STORES | Beta Channels | [38-mobile/12-beta-channels.md](38-mobile/12-beta-channels.md) | Before an update reaches every phone, it flows through widening rings of testers — TestFlight on iOS, testing tracks on Google Play — so bugs hit dozens instead of millions. | TestFlight, Play tracks, tester rings |
| 13 | SHIPPING & STORES | Releases & Phased Rollout | [38-mobile/13-releases-and-phased-rollout.md](38-mobile/13-releases-and-phased-rollout.md) | A mobile update ships to 1% of users first and ramps up over a week — because once a binary is on someone's phone, you can never take it back. | staged rollout, 1% first, halt switch |
| 14 | SHIPPING & STORES | The Update Long Tail | [38-mobile/14-the-update-long-tail.md](38-mobile/14-the-update-long-tail.md) | Shipping v5.0 does not replace v4.x — for weeks your app runs as five versions at once, and your servers and analytics have to serve them all. | version spread, old clients, compatibility |
| 15 | SHIPPING & STORES | The Mobile Duopoly | [38-mobile/15-the-mobile-duopoly.md](38-mobile/15-the-mobile-duopoly.md) | Nearly every phone runs one of two operating systems, and each store charges a commission on digital sales — up to $3 of a $10 subscription goes to the store. | two stores, commission, platform rules |
| 16 | THE NETWORK | Cellular Generations | [38-mobile/16-cellular-generations.md](38-mobile/16-cellular-generations.md) | Each "G" is a generation of radio technology roughly 10× faster than the last — GSM and CDMA split 2G, LTE paved the road to 4G, and 5G shrinks the wait. | GSM vs CDMA, LTE, bandwidth & latency |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "BUILDING APPS" `#2980b9`, "ON THE DEVICE" `#27ae60`, "SHIPPING & STORES" `#8e44ad`, "THE NETWORK" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
