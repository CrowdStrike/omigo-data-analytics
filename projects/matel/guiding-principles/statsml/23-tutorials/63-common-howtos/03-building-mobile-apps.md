# Building Mobile Apps

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Building Mobile Apps

**Subtitle:** What a phone app is actually made of, the iPhone-and-Android choice, the Apple and Google paperwork, and the path from paper sketch to the store

## What an App Is Made Of

**Tags:** `big picture` (blue), `four parts` (green), `offline rule` (orange)

- **The screens** — an app is a stack of screens; each is a layout of buttons, lists, and text
- **The logic** — what happens on a tap: check the input, save it, fetch, move to the next screen
- **The device data** — small storage on the phone itself: settings, the signed-in user, cached lists
- **The server calls** — anything shared lives on a backend; the app asks for it over the internet
- **The offline rule** — phones lose signal; good apps show what they cached and sync up later

*Example (italic):* A to-do app: a list screen, an add screen, items saved on the phone, and a nightly sync to a backend.

**Key point:** Strip any app down to four parts — screens, tap logic, on-device data, server calls — and the hard part is rarely the screens.

### Visualization (canvas `c1`, 720×300)

A workshop exploded view: a phone on the workbench with its parts pulled out beside it — screens inside, a gear of tap logic, a parts drawer of on-device data, a cloud backend, and a second phone sharing through it.

- **Title (bold 17px, ink-brown `#78350f`, top center at y=26):** "Inside the Phone — an Exploded View".
- **Workbench:** brown `#78350f` 3px line from x=40 to x=680 at y=252; two legs 3px from y=252 down to y=272 at x=90 and x=630.
- **Phone (big, standing on the bench):** rounded rect x=70, y=60, w=115, h=190, fill `#ffffff`, border brown `#78350f` 2.5px; speaker slot `fillRect(112,68,32,4)` brown; home bar `fillRect(115,238,26,4)` brown; three stacked screens inside (x=82, w=91, h=38, 12px text, at y=84/130/176): "login" / "list" / "detail", all fill `rgba(225,29,72,0.10)`, border rose `#e11d48`; 12px `#6b7280` "a stack of screens" centered at x=127, y=268.
- **Tap-logic gear:** gear pictogram (8 teeth + hub dot) at cx=285, cy=95, r=18, burnt orange `#c2410c`; dashed 1.5px `#6b7280` leader from x=185,y=97 to x=263,y=95; bold 13px burnt "tap logic" centered at x=285, y=140.
- **Parts drawer (on-device data):** box x=255, y=170, w=60, h=34, fill `rgba(21,128,61,0.12)`, border forest `#15803d` 2px, knob = filled forest circle r=3 at (285,187); dashed leader from x=185,y=187 to x=255,y=187; bold 13px forest "on-device data" centered at x=285, y=224; 12px `#6b7280` "settings · cache" at x=285, y=240.
- **Backend cloud:** cloud pictogram (three bumps: arcs r=22 at (455,102), r=28 at (483,92), r=20 at (512,104), flat base y=118), fill `rgba(225,29,72,0.08)`, stroke rose `#e11d48` 2.5px; bold 13px rose "backend" centered at x=483, y=107; 12px `#6b7280` "over the internet" at x=483, y=152; solid burnt 2px arrow from gear edge x=305,y=95 to cloud left x=430,y=100.
- **Other phone:** small rounded rect x=590, y=150, w=40, h=70, fill `#ffffff`, border forest 2px, screen `fillRect(596,160,28,44)` `rgba(21,128,61,0.12)`; 12px `#6b7280` "shared data" centered at x=610, y=140; bold 13px forest "other phones" at x=610, y=240; solid forest 2px arrow from cloud right x=535,y=105 to x=588,y=155.
- **Annotation (bold 14px burnt `#c2410c`, centered at y=292):** "no signal? show what's cached, sync later".

## One App, Two Phone Worlds

**Tags:** `iPhone vs Android` (blue), `three paths` (green), `pick one` (orange)

- **Two platforms** — Apple's iPhone (iOS) and Google's Android: different languages, stores, rules
- **Native** — build twice: Swift for iPhone, Kotlin for Android; the best feel, twice the work
- **Cross-platform** — build once (Flutter, React Native); one codebase runs on both phones
- **Web app** — a phone-friendly website: no store, no install, but fewer phone features
- **The default** — start cross-platform or web; go native when the app is the product itself

*Example (italic):* A solo builder ships one Flutter codebase to both stores; a bank builds its app twice, natively, on purpose.

**Key point:** iPhone and Android are separate worlds with separate paperwork — cross-platform tools bridge the code, but never the stores.

### Visualization (canvas `c2`, 720×300)

Two storefronts with awnings — the App Store and the Play Store — plus a browser signpost, with three roads arriving from the builder's three choices below.

- **Title (bold 17px, ink-brown `#78350f`, top center at y=26):** "Two Storefronts, Three Roads Up".
- **App Store storefront:** storefront pictogram at x=120, y=52, w=150, h=64, rose `#e11d48`: building rect (x, y+20, w, h-20) white fill rose 2px border, striped awning across (x-4, y, w+8, 20) alternating rose / `rgba(225,29,72,0.12)` stripes with scalloped arc bottoms, door rect (w 22, h 24) at bottom center; bold 13px rose "App Store · iPhone" centered at x=195, y=134.
- **Play Store storefront:** same pictogram at x=340, y=52, w=150, h=64, forest `#15803d`; bold 13px forest "Play Store · Android" centered at x=415, y=134.
- **Browser signpost:** rounded rect x=575, y=62, w=120, h=34, fill `rgba(120,53,15,0.10)`, border brown `#78350f` 2px, text "any browser" 13px; pole 3px brown from (635,96) to (635,150); 12px `#6b7280` "no store gate" centered at x=635, y=166.
- **Three source boxes (h=32 at y=205, 13px text):** "Swift + Kotlin — build twice" x=40, w=180, fill `rgba(194,65,12,0.12)`, border burnt `#c2410c`; "one codebase (Flutter / RN)" x=260, w=180, fill `rgba(225,29,72,0.10)`, border rose 2px; "phone-friendly website" x=490, w=170, fill `rgba(120,53,15,0.10)`, border brown.
- **Roads (arrows):** native, burnt 2px: from (100,205) to (185,122) and from (170,205) to (400,122); cross-platform, rose 2.5px: from (330,205) to (225,122) and from (370,205) to (430,122), bold 12px rose "one build, both stores" centered at (350,172); web, brown 2px: from (600,205) to (632,152).
- **Verdicts (12px, centered at y=254):** burnt "two builds · best feel" at x=130; bold rose "the usual first pick" at x=350; brown "no install · fewer features" at x=575.
- **Annotation (bold 14px burnt `#c2410c`, centered at y=290):** "cross-platform bridges the code — never the stores".

## The Paperwork: Accounts, Signing, Review

**Tags:** `provisioning` (blue), `store review` (green), `IT steps, not code` (orange)

- **The accounts** — Apple ($99/year) and Google ($25 once): forms, identity checks, tax details
- **The signing** — apps are stamped with your developer identity; unstamped apps won't install
- **The test ring** — share early builds with real testers: TestFlight (Apple), internal testing (Play)
- **The review** — people and robots check every submission; health or payments draw extra questions
- **The updates** — every fix goes through review again, so a day-one bug can take days to fix

*Example (italic):* The first screen took an afternoon; the accounts, signing, and first review took two weeks.

**Key point:** Shipping to phones means passing Apple's and Google's walls — budget as much patience for the paperwork as for the building.

### Visualization (canvas `c3`, 720×300)

A road to the store blocked by a brick wall with one review gate: signposts for the account, signing, and test-ring steps, a guard booth at the gate, and the storefront on the far side.

- **Title (bold 17px, ink-brown `#78350f`, top center at y=26):** "The Road to the Store Passes One Gate".
- **Road:** band from x=20 to x=700, y=112 to y=140, fill `rgba(120,53,15,0.10)`; dashed 1.5px brown `#78350f` center line at y=126 (dash [8,6]).
- **Signposts (sign rbox h=30 at y=58, 13px text, pole 2.5px brown from sign bottom to y=112):** "developer account" x=35, w=130, fill `rgba(225,29,72,0.10)`, border rose `#e11d48`, pole at x=100; "app signing" x=195, w=105, fill `rgba(194,65,12,0.12)`, border burnt `#c2410c`, pole at x=247; "test ring" x=330, w=95, fill `rgba(21,128,61,0.12)`, border forest `#15803d`, pole at x=377.
- **Sub-labels (12px `#6b7280`, centered at y=162):** "Apple $99/yr · Google $25 once" at x=110; "your identity, stamped on" at x=260; "TestFlight · Play internal" at x=390.
- **Brick wall with gate:** two wall segments fillRect `rgba(194,65,12,0.15)` with burnt 1.5px outline: (472,48) 36×58 and (472,148) 36×62; brick pattern = horizontal burnt 1px lines every 12px plus offset vertical ticks; arch over the road gap: burnt 2.5px arc centered (490,112), r=24, from π to 2π; bold 13px burnt "store review" centered at x=490, y=40; burnt 2.5px arrow crossing the gate from (440,126) to (540,126); 12px burnt "days, not minutes" centered at x=490, y=182.
- **Guard booth:** rect (522,78) 34×34, white fill, burnt 2px border, window `fillRect(530,86,18,12)` `rgba(194,65,12,0.15)`; 12px `#6b7280` "people + robots check" centered at x=545, y=68.
- **The store (far side):** storefront pictogram at x=590, y=58, w=110, h=54, forest `#15803d` (awning + door as in c2); bold 13px forest "the store" centered at x=645, y=134; 12px `#6b7280` "updates re-reviewed too" at x=625, y=162.
- **Extra-scrutiny strip:** rounded rect x=60, y=210, 600×44, fill `rgba(194,65,12,0.08)`, border burnt; bold 13px ink-brown `#78350f` centered at y=228: "health data, payments, kids' apps — extra questions"; 12px `#6b7280` centered at y=246: "plan the answers before submitting".
- **Annotation (bold 14px burnt `#c2410c`, centered at y=288):** "the paperwork takes longer than the first screen".

## The Backend Behind the App

**Tags:** `backend` (blue), `push` (green), `rent, don't build` (orange)

- **Why a backend** — anything shared between users or phones needs a server: accounts, sync, feeds
- **The service** — the app asks a web service over the internet, exactly like a dashboard would
- **Push** — servers can't call phones directly; a push relay (APNs / FCM) taps the phone instead
- **Rent the pieces** — login, database, and push come ready-made; wire them, don't rewrite them
- **Start without** — a first version can live on the device alone; add the backend when sharing appears

*Example (italic):* The to-do app v1 saves only on the phone; v2 adds a backend so two people share a list — and push pings the other phone.

**Key point:** The phone is always the asker, never the server — shared state lives on a backend, and the push relay is how news reaches a phone.

### Visualization (canvas `c4`, 720×300)

A neighborhood scene: two phones as houses on a street, the backend as the post-office building between them, and a bell — the push relay — that rings the friend's phone when the list changes.

- **Title (bold 17px, ink-brown `#78350f`, top center at y=26):** "The Backend Is the Post Office — Push Is the Bell".
- **Ground line:** brown `#78350f` 2.5px from x=30 to x=690 at y=214.
- **Your phone (left):** phone pictogram x=60, y=94, w=70, h=120: white fill, rose `#e11d48` 2.5px border rounded, speaker `fillRect(85,102,20,3)`, screen `fillRect(68,112,54,84)` fill `rgba(225,29,72,0.10)`, home bar `fillRect(87,204,16,3)`; bold 13px rose "your phone" centered at x=95, y=236.
- **Post office (center):** building rect (290,120) 140×94, white fill, forest `#15803d` 2.5px border; peaked roof triangle (282,120)–(360,84)–(438,120), fill `rgba(21,128,61,0.12)`, forest stroke; door rect (345,178) 30×36 forest 2px; bold 13px forest "backend service" centered at x=360, y=150; 12px `#6b7280` "accounts · sync · feeds" at x=360, y=168.
- **Friend's phone (right):** same phone pictogram at x=590, y=94, w=70, h=120, rose; bold 13px rose "friend's phone" centered at x=625, y=236.
- **Ask/answer arrows:** rose 2.5px arrow from (130,122) to (288,128), 12px rose "adds an item" centered at (208,114); forest 2.5px arrow from (288,152) to (132,158), 12px forest "saved ✓" centered at (208,174).
- **Push bell:** bell pictogram at cx=505, cy=176: dome arc r=16 (π to 2π) burnt `#c2410c` fill `rgba(194,65,12,0.15)` with 2.5px stroke, flared base line (487,176)–(523,176), clapper dot r=3 at (505,182); two ring arcs burnt 1.5px at r=22 and r=28 on the right side (-0.5 to 0.5 rad); burnt 2px arrow from (438,148) to (486,166); burnt 2px arrow from (524,166) to (588,142); bold 12px burnt "push relay (APNs / FCM)" centered at x=505, y=234; 12px burnt "the list changed — take a look" centered at x=505, y=252.
- **Annotation (bold 14px burnt `#c2410c`, centered at y=288):** "a phone is never the server — news arrives by push".

## The Recipe: Idea to the Store

**Tags:** `step by step` (blue), `checklist` (green), `start small` (orange)

- **Step 1 — sketch** — draw every screen on paper first; five boxes and arrows save five rewrites
- **Step 2 — pick the path** — cross-platform by default; native only for a reason you can name
- **Step 3 — build one screen** — end to end with real data; an assistant drafts, you run and judge
- **Step 4 — live on your phone** — use it daily for a week; what annoys you will annoy users
- **Step 5 — testers** — a handful of real people via TestFlight / Play internal testing
- **Step 6 — the review** — submit, answer the questions, fix, resubmit — then it's in the store

*Example (italic):* One screen working end to end on your own phone teaches more than five screens half-built in a simulator.

**Key point:** Sketch, pick a path, build one screen, live with it, test with real people, pass review — the store is the finish line of a paperwork race, not a coding race.

### Visualization (canvas `c5`, 720×300)

A winding trail from the workbench to the storefront, with six numbered pennant flags marking the steps along the path.

- **Title (bold 17px, ink-brown `#78350f`, top center at y=26):** "From Workbench to Launch — Six Flags on the Trail".
- **Trail:** polyline through (80,235) → (190,215) → (300,230) → (410,195) → (510,205) → (600,160) → (640,120), stroked twice: 12px `rgba(120,53,15,0.15)` underneath, then dashed 1.5px brown `#78350f` center line on top (dash [7,6]).
- **Workbench (start):** bench top `fillRect(38,242,70,7)` brown `#78350f`, legs 3px at x=46 and x=100 from y=249 to y=268; a paper sheet `fillRect(56,232,26,10)` white with brown 1.5px outline; 12px `#6b7280` "the sketch" centered at x=73, y=284.
- **Pennant flags (at the six trail points, pole 2.5px brown 30px tall rising from the point, pennant = filled triangle 20×12 pointing right from the pole top, number bold 13px `#78350f` just left of the pole top):** flag 1 at (80,235) rose `#e11d48`; flag 2 at (190,215) burnt `#c2410c`; flag 3 at (300,230) forest `#15803d`; flag 4 at (410,195) rose; flag 5 at (510,205) burnt; flag 6 at (600,160) forest.
- **Step labels (12px `#2c3e50`, centered on the pole x):** below the point for flags 1/3/5/6 (y = point+22), above the pennant for flags 2/4 (y = point−44): "sketch on paper" (80); "pick the path" (190); "build one screen" (300); "live on your phone" (410); "real testers" (510); "store review" (600).
- **Storefront (finish):** storefront pictogram at x=600, y=60, w=100, h=52, forest `#15803d` (awning + door as in c2), with a forest flag (pole + pennant) on the roof at x=685; bold 13px forest "v1 in both stores" centered at x=640, y=140.
- **Annotation (bold 14px burnt `#c2410c`, centered at y=290):** "one screen end to end beats five screens half-done".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then five `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper multiplies the backing store by `window.devicePixelRatio` and calls `ctx.scale(dpr,dpr)`; charts are static (no animation), redrawn once on debounced resize.
- **Chart palette object (workshop / storefront theme, per `common-howtos/CLAUDE.md`):** rose `#e11d48`, burnt `#c2410c`, forest `#15803d`, brown `#78350f`, ink `#78350f`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Fills use theme hues at 0.08–0.15 alpha. Site palette for page chrome: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Viz style:** pictorial scenes, not diagram templates — script defines pictogram helpers `gear`, `cloud`, `storefront`, `phoneShape`, `pennant` beside `rbox`/`arrowTo`; every canvas is a small illustrated scene (exploded phone on a workbench, storefronts with awnings, a brick review gate, a post-office street, a flag trail).
- **Fonts:** titles bold 17px, primary labels 13px, secondary/mute captions 12px, insight annotations bold 14px; nothing below 12px.
- **Data:** all positions are the hardcoded literals above (no randomness); the two-week paperwork anecdote and the to-do app are invented and labeled illustrative; Apple's $99/year and Google Play's $25 one-time developer fees, Swift/Kotlin, Flutter/React Native, TestFlight/Play internal testing, and APNs/FCM are named only for their documented public roles; review-duration wording stays qualitative ("days, not minutes").
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
