# Getting Data From Wearable Devices

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Getting Data From Wearable Devices

**Subtitle:** Your ring measured last night's sleep — every path that score can take to a table you own, and why none of them go straight to your phone

## The Journey of One Sleep Score

**Tags:** `core idea` (blue), `sync chain` (green), `not a live stream` (orange)

- **The ring** — Bluetooth and a battery, no internet; it cannot call any server, ever
- **The courier** — the phone app collects the night over Bluetooth and uploads it at the next sync
- **The cloud copy** — the ring vendor's cloud holds the only internet-visible copy of your score
- **The data service** — the ring vendor's cloud also answers programs: show your token, get score 82
- **The two ways out** — keep asking on a timer, or leave your address to be notified (a "webhook")

*Example (italic):* You wake at 6:30; the phone syncs at 7:12; only then can any server — theirs or yours — see the 82.

**Key point:** A wearable webhook fires when the phone syncs, not when the event happens — "realtime" here means minutes-to-hours fresh, never a live stream.

### Visualization (canvas `c1`, 720×300)

A night-to-morning panorama: the ring and phone on a nightstand under the moon, the score arcing up to the vendor's cloud at sync time, then down to your receiver's house as the sun rises.

- **Title (bold 17px, ink `#3730a3`, top center at y=26):** "One Sleep Score's Night Journey".
- **Sky:** `fillRect(20,38,680,180)` fill `rgba(79,70,229,0.06)`; crescent moon at (70,70) r=16 amber `#d97706` (full circle filled, offset circle at (76,66) r=14 knocked out in white); three small 4-point stars, violet `#7c3aed`, at (150,60), (250,52), (560,55).
- **Ground line:** indigo `#4f46e5` 2.5px from x=20 to x=700 at y=218.
- **Nightstand (left, on the ground):** table top `fillRect(50,192,120,7)` indigo, legs 2.5px at x=58 and x=155 from y=199 to y=218; ring = torus (circle r=10, lineWidth 5, violet `#7c3aed`) at (85,175); phone = rounded rect (120,146,32,46), white fill, indigo 2.5px border, screen `fillRect(125,154,22,30)` `rgba(79,70,229,0.10)`; 12px `#6b7280` "Bluetooth · seconds" centered at (110,138); bold 13px violet "ring + phone" centered at (110,240); 12px `#444` "6:30 · sleep ends" at (110,258).
- **Sync arc:** dashed 2px amber `#d97706` quadratic curve from the phone top (140,146) through control point (250,55) to the cloud's left edge (378,90), arrowhead at the end; the score dot rides it — r=6 filled amber circle with a 40%-alpha r=11 halo at (255,87), "82" bold 13px amber above at (255,71); bold 12px amber "sync upload · minutes–hours" centered at (255,132).
- **Vendor cloud:** cloud pictogram (three arc bumps, flat base) centered (430,95), fill `rgba(162,28,175,0.08)`, stroke plum `#a21caf` 2.5px; bold 13px plum "ring vendor cloud" centered at (430,145); 12px `#444` "7:12 · phone syncs" at (430,163).
- **Receiver house (right, on the ground):** body `rect(560,160,80,58)` white fill indigo 2.5px border, roof triangle (552,160)–(600,128)–(648,160) fill `rgba(79,70,229,0.10)` indigo stroke, door `rect(590,190,20,28)` indigo 2px; violet 2.5px arrow from cloud right (482,102) to roof left (556,140), 12px violet "notification · ~1 s" centered at (530,118); bold 13px indigo "your receiver" centered at (600,240); 12px `#444` "7:12:02 · notification arrives" at (600,258).
- **Rising sun:** amber half-disc r=18 at (680,218) (arc π→2π), fill `rgba(217,119,6,0.15)`, amber 2.5px stroke, three short rays above.
- **Caption (12px `#444`, right-aligned at (700,50)):** "times illustrative".
- **Annotation (bold 14px amber `#d97706`, centered at y=290):** "the slow hop is the phone sync — not your code".

## Asking vs Being Told: Two Ways to Collect

**Tags:** `architecture choice` (blue), `pull vs push` (green), `dev vs deploy` (orange)

- **Pull (ask)** — your program asks the vendor's data service and reads back what's new
- **Push (be told)** — you leave your address once; a notification arrives the moment new data syncs
- **Pull's trade** — nothing to host and easy to try by hand, but something must keep asking
- **Push's trade** — hands-free once set up, but needs the always-on receiver and all of its duties
- **The menu** — data services differ by ring vendor: some only answer asks, some also notify
- **Richer asks** — many also take a search — all sleep between two dates — or one big export
- **In practice** — pull while developing to learn the data's shape; push when it runs on its own

*Example (italic):* You pull yesterday's sleep by hand and study its fields; months later the deployed app is told at 7:12:02 — no one at the keyboard.

**Key point:** Develop with pull, deploy with push — both start from the same one-time "Allow" click and token, and the ring vendor's menu decides which options you even have.

### Visualization (canvas `c2`, 720×300)

Two half-scenes split by a center line: pull is a person walking over to check a mailbox; push is the vendor's cloud dropping an envelope at a house whose doorbell rings. Pros/cons under each, vendor-menu strip below.

- **Title (bold 17px, ink `#3730a3`, top center at y=26):** "The Mailbox or the Doorbell".
- **Divider:** dashed 1.5px `#c8cdd4` vertical line at x=360 from y=40 to y=230.
- **Headers (bold 14px, centered at y=56):** "pull — you ask" indigo `#4f46e5` at x=190; "push — you are told" plum `#a21caf` at x=530.
- **Pull scene (left):** stick person at x=80 — head circle r=8 at (80,92), body line (80,100)–(80,128), legs to (72,145) and (88,145), arm (80,110)–(96,116), all indigo 2.5px; mailbox at x=235 — box rounded rect (205,92,60,30) fill `rgba(79,70,229,0.10)` border indigo 2px with 12px text "data service", post 3px indigo from (235,122) to (235,152), flag line indigo 2px from (265,92) to (275,82); dashed indigo 2px arrow from (96,112) to (200,105); 12px `#6b7280` "asks · anytime" centered at (150,158).
- **Push scene (right):** small cloud pictogram centered (450,90), fill `rgba(162,28,175,0.08)`, plum `#a21caf` 2.5px stroke; house at x=595 — body `rect(560,100,70,50)` white fill plum 2.5px border, roof triangle (553,100)–(595,74)–(637,100) fill `rgba(162,28,175,0.10)` plum stroke, door `rect(585,124,18,26)` plum 2px with amber `#d97706` doorbell dot r=3 at (608,132) and two amber 1.5px ring arcs (r=7 and r=11, −0.6→0.6 rad) around it; plum 2.5px arrow from cloud right (492,95) to roof left (556,90); envelope at the arrow midpoint — white `fillRect(515,84,16,11)` with plum 1.5px outline and flap lines from the top corners to the center; 12px `#6b7280` "notifies · on sync" centered at (530,158).
- **Pros/cons lines (12px left-aligned, four per side at y=176/192/208/224; "✓" indigo `#4f46e5`, "✗" red `#e74c3c`, text `#2c3e50`):** pull at x=50: "✓ nothing to host — no public address"; "✓ try it by hand, see the data's shape"; "✗ something must keep asking"; "✗ fresh only as often as you ask". push at x=390: "✓ hands-free — new nights just arrive"; "✓ fresh the moment the phone syncs"; "✗ needs a public receiver + duties"; "✗ more setup before the first row".
- **Menu strip (rounded rect x=30, y=238, 660×36, fill `rgba(217,119,6,0.10)`, border amber `#d97706`):** bold 13px ink `#3730a3` centered at y=253: "what's on offer differs by ring vendor"; 12px `#6b7280` centered at y=268: "asks only · notifications too · date-range search · bulk export".
- **Annotation (bold 14px amber `#d97706`, centered at y=292):** "develop with pull — deploy with push".

## Handing Over Your Address

**Tags:** `worked example` (blue), `handshake` (green), `security` (red)

- **The registration** — you tell the ring vendor once: notify this web address of new sleep data
- **The check** — a test message comes right away; answering it proves your address really works
- **The notification** — at 7:12:02 a message arrives: `usr_114` has new sleep data — no numbers in it
- **The fetch** — your receiver then asks the data service for the full record, showing its token
- **The fake message** — anyone on the internet can send to a public address; check the signature first
- **The duplicate** — missed notifications are re-sent and can arrive twice; skip repeats by their id

*Example (italic):* The 7:12:02 message names usr_114's night; the follow-up request brings the full record, and score 82 becomes a row.

**Key point:** The receiving end is one small program with four duties — answer the test message, check the signature, skip repeats, and say "got it" fast.

### Visualization (canvas `c3`, 720×300)

Sequence of envelopes between two pictograms — your receiver's house and the vendor's cloud: the register-and-verify handshake, a moonlit "night passes" gap, then the notification and the full-record fetch.

- **Title (bold 17px, ink `#3730a3`, top center at y=26):** "The Handshake, Then the Notification".
- **Anchors (replacing header boxes):** house pictogram centered on x=190 — body `rect(160,52,60,34)` white fill indigo `#4f46e5` 2.5px border, roof triangle (154,52)–(190,34)–(226,52) fill `rgba(79,70,229,0.10)`, bold 13px indigo "your receiver" centered at (190,100); small cloud pictogram centered (530,62), fill `rgba(162,28,175,0.08)`, plum `#a21caf` 2.5px stroke, bold 13px plum "ring vendor cloud" centered at (530,100).
- **Lifelines:** dashed 1.5px `#c8cdd4` vertical lines at x=190 and x=530 from y=108 to y=285.
- **Messages (2px arrows between lifelines, each with a small envelope at its midpoint — white `fillRect` 16×11 with 1.5px outline and flap lines in the arrow's color — and a 12px label above):**
  - y=124, right (you→ring vendor), indigo `#4f46e5`: "please notify https://you.example/hook of new sleep data"
  - y=150, left (ring vendor→you), plum `#a21caf`: "test message — are you really there? (code ch_492)"
  - y=176, right, indigo: "yes — ch_492 ✓"
- **Night gap (no arrow, centered between lifelines at y=196–206):** small amber `#d97706` crescent moon r=8 at (330,200); italic 12px violet `#7c3aed` "night passes, phone syncs at 7:12" left-aligned at (346,205).
  - y=224, left, plum: "notification evt_7a2c: usr_114 has new sleep data"
  - y=250, right, indigo: "may I have the full record? (shows token)"
  - y=276, left, amber `#d97706`: "full record: score 82"
- **End marker:** bold 13px indigo `#4f46e5` "saved as a row ✓" centered at (100,290).
- **Caption (12px `#444`, right-aligned at (700,292)):** "ids and times illustrative".

## A Public Address, Without Building a Webapp

**Tags:** `public URL` (blue), `where it runs` (green), `phones can't serve` (orange)

- **The listed address** — a server is just a computer with a public, stable address that never sleeps
- **The apartment problem** — laptops and phones sit behind shared addresses: they call out, nothing calls in
- **Rent** — a tiny receiver in the cloud (a "cloud function"): no website, a few lines, pennies a month
- **Forward** — a tunnel (e.g., ngrok) lends your laptop a temporary public address — first tests only
- **Skip** — the ring app writes to the phone's health store (Apple Health / Health Connect); no server
- **Real apps** — their backend takes the notifications; a push relay (APNs / FCM) alerts the phone

*Example (italic):* Production apps hide all of this: an aggregator or their own backend receives the notifications, and the phone just gets a push.

**Key point:** "Do I need a public webapp?" — no; one rented receiver is enough, and a phone is never the receiver — real apps push to it from a backend, on iPhone and Android alike.

### Visualization (canvas `c4`, 720×300)

Four lanes, each starting from a pictogram (a cloud or a phone) and flowing to its destination: rent, forward, skip, and how production apps do it.

- **Title (bold 17px, ink `#3730a3`, top center at y=26):** "Four Paths From the Data to You".
- **Lane labels (bold 13px, x=15, left-aligned):** "rent" indigo `#4f46e5` y=78; "forward" amber `#d97706` y=133; "skip" violet `#7c3aed` y=188; "real apps" plum `#a21caf` y=243.
- **Lane sources (pictograms instead of source boxes):** lanes rent/forward/real-apps each start with a small cloud pictogram centered (128, laneY), fill `rgba(162,28,175,0.08)`, plum 2px stroke, 12px `#6b7280` "vendor cloud" centered beneath at (128, laneY+26); lane skip starts with a small phone pictogram — rounded rect (114, laneY−20, 26, 40), white fill, violet 2.5px border, screen fill `rgba(124,58,237,0.10)` — with 12px `#6b7280` "ring app" at (128, laneY+34).
- **Lane flows (boxes 26px tall centered on laneY, 8px radius, 12px `#2c3e50` text, 1.5px `#6b7280` arrows from the source pictogram and between boxes):**
  - rent (laneY=74): → "rented receiver" x=250 w=115 (fill `rgba(79,70,229,0.10)`, border indigo) → "your table" x=420 w=100 (fill `rgba(79,70,229,0.06)`, border indigo); bold 12px indigo "the standard answer" left-aligned at x=545.
  - forward (laneY=129): → "tunnel URL" x=250 w=115 (fill `rgba(217,119,6,0.12)`, border amber) → "your laptop" x=420 w=100 (fill `rgba(217,119,6,0.06)`, border amber); 12px amber "dev & first tests" at x=545.
  - skip (laneY=184): → "health store" x=250 w=115 (fill `rgba(124,58,237,0.10)`, border violet) → "your app" x=420 w=100 (fill `rgba(124,58,237,0.06)`, border violet); 12px violet "Apple Health / Health Connect" at x=535.
  - real apps (laneY=239): → "app backend" x=230 w=105 (fill `rgba(162,28,175,0.10)`, border plum) → "push relay" x=370 w=100 (fill `rgba(162,28,175,0.06)`, border plum) → "your phone" x=505 w=95 (fill `rgba(162,28,175,0.06)`, border plum); 12px plum "push, not webhook" at x=615.
- **Annotation (bold 14px amber `#d97706`, centered at y=288):** "a phone is never the server — it gets pushed to".

## The Paperwork Before the Plumbing

**Tags:** `provisioning` (blue), `OAuth consent` (green), `IT steps, not code` (orange)

- **The developer account** — register with the ring vendor first: a form, email verification, terms to accept
- **The app registration** — create an "app" record; you receive a client id + secret, your code's identity
- **The consent** — each user (even just you) clicks "Allow" once; that grant is what becomes a token
- **The scopes** — the grant lists what the token can read (sleep, not heart rate); ask for the minimum
- **The approval wall** — sandbox = you + a few test users; going public needs the ring vendor's review
- **The lifecycle** — access tokens expire and users can revoke; the refresh token keeps access alive
- **The second wall** — a phone app adds Apple/Google paperwork: health permissions, store review

*Example (italic):* The afternoon's slowest step is often not code at all — it's the form, the consent click, and the approval email.

**Key point:** An app is an identity (client id + secret), a user's consent is a scoped grant, and a token is that grant made portable — all of it provisioned by clicking, not coding.

### Visualization (canvas `c5`, 720×300)

A desk of paperwork becoming a key: a form, an id card, a consent sheet, then the scoped token drawn as a key — above a fenced sandbox yard whose only way out is the ring-vendor review gate.

- **Title (bold 17px, ink `#3730a3`, top center at y=26):** "From Form to Token — All Clicks, No Code".
- **Paper 1 (developer account):** sheet `rect(60,58,70,54)`, white fill, indigo `#4f46e5` 2px border, folded corner triangle (112,58)–(130,58)–(130,76) fill `rgba(79,70,229,0.10)`; three 1.5px indigo text-lines inside at y=78/88/98 (x=68 to x=118); bold 13px indigo "developer account" centered at (95,128).
- **Paper 2 (app identity):** id card `rect(210,66,86,46)`, white fill, violet `#7c3aed` 2px border, photo square `fillRect(218,74,20,20)` `rgba(124,58,237,0.15)`, two 1.5px violet lines at y=80/90 (x=246 to x=286); bold 13px violet "app: id + secret" centered at (253,128).
- **Paper 3 (consent):** sheet `rect(370,58,70,54)`, white fill, plum `#a21caf` 2px border, checkbox `rect(378,68,12,12)` with plum "✓" (bold 12px), 12px plum text "Allow?" at (418,79), one 1.5px plum line at y=98 (x=378 to x=428); bold 13px plum "consent: Allow?" centered at (405,128).
- **Key (scoped token):** amber `#d97706` key at (540,85) — bow: circle r=13 stroke 3px centered (548,85); shaft: 3px line (561,85)–(612,85); two teeth 3px lines down 8px at x=598 and x=608; bold 13px amber "scoped token" centered at (580,128).
- **Arrows (2px `#6b7280` at y=85):** (130,85)→(210,85); (296,85)→(370,85); (440,85)→(533,85).
- **Sub-labels (12px `#6b7280`, centered at y=146):** "scopes: sleep only" at x=405; "expires · refresh · revoke" at x=580.
- **Second-wall note (12px violet `#7c3aed`, centered at (360,168)):** "your own phone app adds a 2nd wall (Apple / Google) — the webhook path never meets it".
- **Sandbox fence (left):** two 2px indigo rails at y=204 and y=222 from x=60 to x=330; posts 2.5px indigo every 30px (x=60,90,…,330) from y=196 to y=230; bold 13px indigo "sandbox — you + a few test users" centered at (195,252).
- **Review gate (center):** two amber `#d97706` 3px posts at x=348 and x=372 from y=192 to y=230, amber 2.5px arch arc over them centered (360,192) r=12 (π→2π); bold 13px amber "ring vendor review" centered at (360,180); amber 2px arrow through the gate from (330,214) to (390,214).
- **Production field (right):** open — no fence; three small 4-point stars, plum `#a21caf`, at (450,205), (530,220), (620,200); bold 13px plum "production — any user" centered at (530,252).
- **Annotation (bold 14px amber `#d97706`, centered at y=288):** "forms, consent clicks, approval emails — often slower than the code".

## The Whole Recipe, In One Afternoon

**Tags:** `step by step` (blue), `checklist` (green), `secrets` (red)

- **Step 1 — sign up** — create your app on the ring vendor's site, approve consent, copy your token
- **Step 2 — rent a receiver** — a cloud service runs it nonstop; it comes with a public web address
- **Step 3 — hand over the address** — tell the ring vendor once: notify this URL of new sleep data
- **Step 4 — dry run** — send yourself a practice event and watch it land as one row in your table
- **Step 5 — let it run** — every synced night now shows up on its own; repeats are ignored
- **Keep secrets aside** — the token lives in the cloud platform's settings, never pasted into the code

*Example (italic):* The code is boilerplate an assistant can draft in one go; the accounts, consent clicks, token, and deploy button are the only parts that need you.

**Key point:** One afternoon end to end — sign up, rent a receiver, hand over the address, dry run — and from then on every synced night arrives as a row on its own.

### Visualization (canvas `c6`, 720×300)

The five steps as a constellation: five numbered stars joined by dashed lines across the night sky, the last one pointing down to your table's first real row.

- **Title (bold 17px, ink `#3730a3`, top center at y=26):** "A Five-Star Constellation".
- **Sky:** `fillRect(20,38,680,215)` fill `rgba(79,70,229,0.05)`; crescent moon at (660,65) r=14 amber `#d97706` (filled circle with a white knock-out circle at (665,61) r=12); four tiny background dots r=1.5 violet `#7c3aed` at (60,60), (300,55), (500,70), (160,90).
- **Constellation lines:** dashed 1.5px violet `#7c3aed` (dash [5,5]) joining (90,180) → (210,120) → (330,150) → (450,95) → (570,130).
- **Stars (five-point, outer r=12, inner r=5, filled amber `#d97706` with amber 1.5px stroke) at those five points;** step numbers bold 13px indigo `#4f46e5` centered 20px above each star.
- **Step labels (12px `#2c3e50`, two centered lines below each star at y+26 and y+41):** "sign up / + token" (90,180); "receiver / public URL" (210,120); "address / handed over" (330,150); "dry run / one row lands" (450,95); "rows / flow in" (570,130).
- **The table (bottom right):** amber 2px arrow from (585,140) to (620,206); table `rect(560,210,140,40)`, white fill, indigo `#4f46e5` 2px border, two 1.5px indigo inner horizontal lines at y=223 and y=236, bottom row band `fillRect(561,237,138,12)` `rgba(217,119,6,0.18)`; bold 13px amber "score 82 → your table" centered at (630,268).
- **Annotation (bold 14px amber `#d97706`, centered at y=290):** "the token goes in the cloud platform's settings — never inside the pasted code".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; charts are static (no animation), redrawn once on debounced resize.
- **Chart palette object (night sky / sleep theme, per `common-howtos/CLAUDE.md`):** indigo `#4f46e5`, violet `#7c3aed`, plum `#a21caf`, amber `#d97706` (moon, sun, alarms, token), ink `#3730a3`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Fills use theme hues at 0.05–0.18 alpha. Red `#e74c3c` only for ✗ marks. Site palette (page chrome only): primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Viz style:** pictorial scenes per `common-howtos/CLAUDE.md` — pictograms drawn with canvas primitives (a crescent moon, stars, a ring torus, phones, houses, clouds, a mailbox, envelopes, a fence and gate, papers and a key); no two canvases share the same layout skeleton; helper functions (`cloudP`, `houseP`, `phoneP`, `crescent`, `star4`, `star5`, `envelope`) live beside `setup`/`rbox`/`arrowTo`.
- **Fonts:** titles bold 17px, primary labels 13px, secondary/mute captions 12px, insight annotations bold 14px; nothing below 12px.
- **Data:** all positions and values are the hardcoded literals above (no randomness); the score 82, the 6:30 / 7:12 / 7:12:02 clock times, ids `usr_114`, `evt_7a2c`, `ch_492`, and the "~40 lines / pennies a month" figures are invented and labeled illustrative; scope names and sandbox test-user limits are generic vendor patterns, not any specific vendor's terms; the vendor is generic ("the ring vendor"); Apple Health, Health Connect, APNs, FCM, and ngrok are named only for their documented roles.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
