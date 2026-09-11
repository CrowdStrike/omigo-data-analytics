# AirDrop & Nearby Share

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** AirDrop &amp; Nearby Share

**Subtitle:** Bluetooth finds the neighbor, Wi-Fi carries the file — proximity sharing with no cable, no account, and no internet at all

## Alice's Photo Reaches Bob's Phone Without Any Network

**Tags:** `core idea` (blue), `proximity discovery` (green), `two radios` (orange)

- **The running example** — Alice holds her phone near Bob's and a photo hops across; no cable, no login
- **Radio one: Bluetooth** — a low-power beacon murmurs "a phone is here" to anyone a few meters away
- **Radio two: Wi-Fi** — once Bob accepts, the phones form a private two-phone Wi-Fi link on the spot
- **Division of labor** — Bluetooth sips battery and keeps watch; Wi-Fi is fast but wakes only on demand
- **Phone to phone** — the file crosses the room directly; no router, no server, no account involved

*Example (italic):* Alice taps share, Bob's name appears within seconds, and the photo lands before either phone touches the internet.

**Key point:** Two radios split the job — the cheap one keeps watch for neighbors, the fast one wakes only to carry the file.

### Visualization (canvas `c1`, 720×300)

Two-panel schematic of the two-radio handoff: discovery by Bluetooth beacon on the left, on-demand peer Wi-Fi transfer on the right.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Two Radios, Two Jobs: Bluetooth Finds, Wi-Fi Carries".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=360 from y=40 to y=252.
- **Panel headings (bold 13px, centered, y=62):** "1 — discovery" in violet `#4a3aa7` at x=180; "2 — transfer" in blue `#2a78d6` at x=540.
- **Phones:** rounded rects 44×80 (radius 8), fill `#f5f6f8`, 2px `#6b7280` border, small screen line inside; left panel at (60,110) and (256,110), right panel at (420,110) and (616,110). Names 12px `#444` centered under each phone (y=212): "Alice" / "Bob" in both panels.
- **Left panel (discovery):** three dashed concentric arcs centered on Alice's phone center (82,150), radii 40 / 65 / 90, 1.5px violet `#4a3aa7`, alpha fading 0.9 / 0.6 / 0.35. Beacon label bold 12px `#4a3aa7` centered at (180,86): "beacon: 'a phone is here'". Sub-label 11px `#6b7280` centered at (180,232): "trickle of power · a few meters · always ready".
- **Right panel (transfer):** thick 6px blue `#2a78d6` link from (464,150) to (616,150); three 10px green `#008300` file-block squares riding the link at x=495 / 525 / 555 (y=145). Link label bold 12px `#2a78d6` centered at (540,86): "private peer Wi-Fi link, built on demand". Sub-label 11px `#6b7280` centered at (540,232): "fast · direct · torn down when the file lands".
- **Annotation (bold 13px orange `#d95926`, centered, y=268):** "whisper to find, shout to carry — the file goes phone to phone".
- **Caption (11px `#6b7280`, bottom center, y=290):** "schematic — the two-radio handoff".

## Sending a 500 MB Video: Across the Room vs Around the Internet

**Tags:** `worked example` (blue), `peer wifi` (green)

- **Direct peer Wi-Fi** — ~25 MB/s across the room (illustrative): 500 ÷ 25 = 20 seconds, done
- **The chat-app route** — Alice uploads the video to a server first; Bob then downloads it back
- **Trip one: upload** — home uplink ~2.5 MB/s (illustrative): 500 ÷ 2.5 = 200 s the slow way up
- **Trip two: re-download** — downlink ~12.5 MB/s: 500 ÷ 12.5 = 40 s for the file to come back
- **The total** — 200 + 40 = 240 s ≈ 4 min, versus 20 s direct: 12× slower for the person beside you
- **Bonus cost** — many chat apps also recompress video on upload; the direct copy arrives untouched

*Example (italic):* The video crossed two meters of air in 20 seconds; the chat-app copy toured a data center and came back in 4 minutes.

**Key point:** Network distance is what you pay for — the chat route sends the file away and back; peer Wi-Fi sends it two meters.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: time to move a 500 MB video to the person next to you, direct peer Wi-Fi vs upload-then-re-download, shared seconds axis.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "500 MB to the Person Next to You (illustrative rates)".
- **Axis:** seconds 0–240 mapped to px x=165 (0 s) through x=680 (240 s); 1px `#999` baseline at y=212; `#e5e9ef` vertical gridlines with 12px `#444` tick labels at 0 / 60 / 120 / 180 / 240 (y=230); axis caption "seconds" 12px `#444` centered at (422,250).
- **Rows (bars 40px tall, two-line 12px `#444` left labels right-aligned at x=155):**
  - "direct peer Wi-Fi" / "~25 MB/s": bar y=82..122, blue `rgba(42,120,214,0.35)` to 20 s (≈43px), 2px `#2a78d6` border, bold 13px `#2a78d6` value "20 s" right of the bar end
  - "via chat app" / "upload + re-download": bar y=150..190, stacked — orange `rgba(217,89,38,0.35)` segment to 200 s with 2px `#d95926` border and bold 12px `#d95926` inside label "upload 200 s"; aqua `rgba(25,158,112,0.35)` segment for the next 40 s with 2px `#199e70` border and bold 12px `#199e70` label "re-download 40 s" placed above the segment (y=143); total bold 13px `#2c3e50` "240 s ≈ 4 min" under the stack's right end (y=205 area, right-aligned at x=680)
- **Annotation (bold 13px green `#008300`, centered near x=420, y=54):** "same room: 20 s direct vs 4 min around the internet — 12×".
- **Caption (11px `#6b7280`, bottom center, y=286):** "illustrative: peer Wi-Fi 25 MB/s, home uplink 2.5 MB/s, downlink 12.5 MB/s".

## Who Can Find You: the Receiving Setting Is the Privacy Dial

**Tags:** `privacy control` (orange), `where it's used` (blue)

- **The beacon is the exposure** — advertising "I'm here" is exactly what makes discovery work
- **Everyone** — any nearby phone can see yours and offer a file; handy at events, noisy on a train
- **Contacts only** — the beacon is answered only for people already in your address book
- **Receiving off** — the phone stays invisible; you can still send, nobody can offer you files
- **Consent gate** — an incoming offer names the sender and previews the file; nothing lands unaccepted
- **Everyday data use** — a multi-GB dataset reaches the laptop at the next desk with no upload at all

*Example (italic):* On a crowded train Alice switches to contacts only, and her phone vanishes from the strangers' share sheets.

**Key point:** Discovery is the whole exposure — the receiving setting controls who can even see your phone exists, before any file moves.

### Visualization (canvas `c3`, 720×300)

Three mini-panels, one per receiving setting, each showing Alice's phone amid the same five nearby people (2 contacts, 3 strangers) and lines to whoever can discover her.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Same Room, Three Settings: Who Can See Alice's Phone".
- **Panels centered at x=130 / 360 / 590,** people ring centered at (cx,155), radius 58. Panel headings bold 13px centered at y=56: "Everyone" in orange `#d95926`, "Contacts only" in green `#008300`, "Receiving off" in `#6b7280`.
- **Nodes per panel:** Alice at (cx,155), radius 13, fill `#2a78d6`, white bold 11px "A". Five neighbors radius 8 at angles -90° / -18° / 54° / 126° / 198°: indices 0–1 contacts, fill `#008300`; indices 2–4 strangers, fill `#9aa2ad`.
- **Discovery lines (1.8px, drawn under nodes, color of the neighbor node):** panel 1 — all five neighbors to Alice; panel 2 — only the two green contacts; panel 3 — none.
- **Count labels (bold 12px, centered, y=242, colored as the heading):** "all 5 can see her" / "only her 2 contacts" / "nobody".
- **Annotation (bold 12px orange `#d95926`, centered, y=268):** "the setting decides who the beacon answers — before any file is offered".
- **Caption (11px `#6b7280`, bottom center, y=290):** "illustrative — 2 contacts (green) and 3 strangers (gray) nearby".

## No Mobile Data, No Cloud: the File Never Leaves the Room

**Tags:** `common mistake` (red), `no internet needed` (green)

- **The common guess** — people assume the file rides mobile data or "the cloud"; it uses neither
- **What must be on** — the Bluetooth and Wi-Fi radios, not a Wi-Fi network; no router takes part
- **No SIM, no signal** — it works with zero bars and no internet, because the internet is not on the path
- **No data charges** — nothing crosses the carrier network, so nothing counts against a data plan
- **No server copy** — no third copy sits in a data center; the only copies are on the two phones
- **The catch** — proximity is required; the trick stops working the moment Bob leaves the room

*Example (italic):* Two phones in a basement with no signal and no Wi-Fi network still swap the video — the room is the network.

**Key point (Common mistake label):** It uses the Wi-Fi radio, not your Wi-Fi network — a private phone-to-phone link, so no internet, no cloud, no charges.

### Visualization (canvas `c4`, 720×300)

Two-path diagram: the assumed route (phone → router → chat server → phone, crossed out) above the actual route (phone → phone, one hop).

- **Title (bold 15px, `#1a5276`, top center, y=24):** "The Assumed Path vs the Actual Path".
- **Top row (assumed):** four boxes 110×44 at y=68, x=40 / 210 / 380 / 570 (last at 570 with width 110): "Alice's phone", "home router", "chat server", "Bob's phone"; fill `#f5f6f8`, 2px `#6b7280` border, bold 11px `#2c3e50` centered labels ("chat server" gets second line "(data center)" in 10px `#6b7280`). Connecting arrows 2px `#9aa2ad` between boxes at y=90 with small filled arrowheads. Red `#e74c3c` bold 13px label right-aligned near (680,52): "✕ not what happens". Distance note 11px `#6b7280` centered at (360,132): "hundreds of km — up the slow uplink, then back down".
- **Bottom row (actual):** two boxes 110×44 at y=185, x=150 and x=460, same box style, labels "Alice's phone" / "Bob's phone". Thick 6px green `#008300` link from (260,207) to (460,207). Green `#008300` bold 13px label centered at (360,172): "✓ one hop, a few meters of air". Note 11px `#6b7280` centered at (360,252): "no router, no server, no data plan involved".
- **Annotation (bold 13px green `#008300`, centered, y=274):** "the file crosses the room, not the internet — no server copy ever exists".
- **Caption (11px `#6b7280`, bottom center, y=292):** "schematic — the two candidate paths".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" in sections 1–3, "Common mistake:" in section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150 ms).
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Red `#e74c3c` only for the "not what happens" mark in c4.
- **Data:** all chart values are the hardcoded numbers above (no `Math.random`). Documented behavior: AirDrop and Nearby Share (Quick Share) discover peers over Bluetooth Low Energy and transfer over a direct peer Wi-Fi link; receiving visibility settings offer everyone / contacts / off tiers; incoming offers require an explicit accept. Invented figures (peer Wi-Fi 25 MB/s, uplink 2.5 MB/s, downlink 12.5 MB/s, 2 contacts + 3 strangers) are labeled illustrative in each chart. Arithmetic that must stay consistent between text and charts: 500 ÷ 25 = 20 s; 500 ÷ 2.5 = 200 s; 500 ÷ 12.5 = 40 s; 200 + 40 = 240 s ≈ 4 min; 240 ÷ 20 = 12×.
- This page has no links.
