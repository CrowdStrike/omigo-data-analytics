# Off-Grid & Mesh Messaging

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Off-Grid &amp; Mesh Messaging

**Subtitle:** What happens to a message when there is no server to hold it — neighbors relay it, everyone carries it, and delivery becomes a probability instead of a promise

## Loosening the Center: Federation and Peer-to-Peer

**Tags:** `core idea` (blue), `federation` (green), `P2P` (orange)

- **One operator** — Most messengers put exactly one company's server between every pair of phones.
- **Federation** — XMPP (1999) and Matrix (2014) spread that role across many independent servers.
- **Matrix rooms** — Every homeserver in a Matrix room replicates the room's full history.
- **No owner** — Once history is replicated everywhere, no single operator can delete or gatekeep it.
- **Pure P2P** — Removing servers entirely forces phones to discover each other across NATs.
- **Offline hole** — With no server at all, nothing stores a message for a peer who is offline.

*Example (italic):* Alice's homeserver goes down for a day, yet Bob still reads the whole room from his own server's replica.

**Key point:** Federation spreads the center across many servers; pure P2P removes it entirely — and immediately hits two hard problems: finding your peer, and reaching a peer who is offline.

### Visualization (canvas `c1`, 720×300)

Three-topology diagram — centralized, federated, pure P2P — each panel annotated with what breaks.

- **Title (bold 15px, `#1a5276`, top center):** "Three Topologies: Where Does the Message Live?".
- **Dividers:** dashed 1px `#bdc3c7` vertical lines at x=245 and x=485, from y=38 to y=268.
- **Panel 1 (center x=122), label bold 13px `#1a5276` at y=50:** "Centralized". Server: rounded rect 44×30 centered at (122,120), fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 11px "server". Six client dots (r=7, `#6b7280`) on an ellipse around it (rx=78, ry=62), each joined to the server by a 1.2px `#bbb` line. Annotation bold 12px `#d95926` centered at (122,248): "one operator owns everything"; 11px `#6b7280` at (122,265): "server down = everyone down".
- **Panel 2 (center x=365), label bold 13px `#1a5276` at y=50:** "Federated (XMPP, Matrix)". Three homeserver boxes 52×26, fill `rgba(0,131,0,0.10)`, 2px `#008300` border, bold 10px "home- server" text "HS1"/"HS2"/"HS3", centered at (365,92), (310,178), (420,178). Servers pairwise joined by 2.5px `#008300` lines. Each server has two client dots (r=6, `#6b7280`) hanging off it on thin `#bbb` lines (outward positions). Annotation bold 12px `#008300` centered at (365,248): "history replicated on every homeserver"; 11px `#6b7280` at (365,265): "no single owner".
- **Panel 3 (center x=608), label bold 13px `#1a5276` at y=50:** "Pure P2P". Five online peer dots (r=9, `#4a3aa7`) at (545,95), (665,100), (555,175), (660,185), (608,135), joined pairwise (not complete — 6 of the edges) by 1.5px `#4a3aa7` lines at 0.5 alpha. One offline peer: dot r=9 white fill with dashed 2px `#6b7280` ring at (610,228), dashed `#bbb` edges to two nearest peers. Bold 11px `#6b7280` "offline" under it at y=248 — no, place at (652,232) to the side. Annotation bold 12px `#e74c3c` centered at (608,262): "nobody stores the offline peer's message"; 11px `#6b7280` at (608,278): "discovery + NAT traversal unsolved for free".
- **Caption (11px `#6b7280`, bottom left at (30,290)):** "topology sketches; node counts illustrative".

## Phones as Relays: Bluetooth Mesh in a Crowd

**Tags:** `worked example` (blue), `Bluetooth LE` (green), `store-and-forward` (orange)

- **Short hops** — Bluetooth LE covers roughly 10-100 m, so a single phone reaches almost no one.
- **Relaying** — Each phone forwards strangers' messages, so range grows hop by hop through a crowd.
- **Store-and-forward** — A phone carries a message physically until it meets another node to hand it to.
- **Flood control** — A TTL or hop limit stops each message from bouncing through the mesh forever.
- **Density rules** — The mesh works in a stadium or protest crowd and dies on an empty street.
- **The apps** — FireChat (2014, Hong Kong), Bridgefy (2019-20 protests), and bitchat (2025) ran real crowds.

*Example (italic):* Alice's message crosses a plaza in six hops, and one carrier walks a 200 m gap before handing it onward.

**Key point:** Relaying plus store-and-forward means coverage grows with the crowd — and delivery becomes a probability driven by how dense that crowd is.

### Visualization (canvas `c2`, 720×300)

Crowd-hop diagram: a message hops phone-to-phone across a crowd, one carrier physically walks a gap, hop range circles show BLE reach.

- **Title (bold 15px, `#1a5276`, top center):** "One Message Crossing a Crowd, Hop by Hop".
- **Background crowd:** ~14 bystander phone dots (r=4, `rgba(107,114,128,0.45)`) at hardcoded scattered positions, e.g. (95,210), (170,190), (150,240), (240,200), (310,95), (365,110), (400,230), (250,70), (490,205), (555,215), (630,200), (605,75), (330,250), (680,165).
- **Hop range circles:** dashed 1px `rgba(42,120,214,0.5)` circles, r=36, around each relay node on the path (not around the walk segment).
- **Path nodes:** Alice (r=9, `#2a78d6`, bold 11px label "Alice" below) at (60,150); relays R1-R2 (r=7, `#008300`) at (150,105), (240,150); carrier (r=8, `#d95926`, bold 11px `#d95926` label "carrier" below) at (330,155); after the walk the carrier appears again as an open orange circle (r=8, 2px `#d95926` stroke, white fill) at (445,155); relays R4-R5 (r=7, `#008300`) at (510,110), (575,155); Bob (r=9, `#2a78d6`, bold 11px label "Bob" below) at (645,115).
- **Hop arrows:** 2px `#199e70` arrows along consecutive path nodes (radio hops); the carrier segment (330,155)→(445,155) instead drawn as a dashed 2.5px `#d95926` arrow.
- **Annotations:** bold 12px `#2a78d6` "each hop ≤ ~100 m (BLE)" at (165,62); bold 12px `#d95926` two lines centered at (388,200)/(388,216): "no phone in range — the carrier" / "walks the message across the gap"; bold 12px `#008300` "delivered — 6 radio hops + 1 walk" right-aligned near (700,88).
- **Caption (11px `#6b7280`, centered at (360,288)):** "TTL caps hops (e.g. 7) to stop infinite flooding — positions and counts illustrative".

## Kilometer Texting: LoRa Mesh

**Tags:** `LoRa` (blue), `long range` (green), `trade-off` (orange)

- **The trade** — LoRa swaps bandwidth for distance; tiny text messages travel 2-15 km per hop.
- **Free spectrum** — LoRa runs on unlicensed ISM bands, so anyone can put a node on the air.
- **Meshtastic** — A ~$30 LoRa node pairs with a phone over Bluetooth and meshes with its peers.
- **Who uses it** — Hikers, sailors, and disaster-response crews text where no cell tower exists.
- **goTenna** — goTenna sold the same phone-plus-radio pairing commercially for off-grid teams.
- **Pick two** — Range, bandwidth, and battery form a triangle; a radio design only gets two.

*Example (italic):* A hiker two ridgelines from any tower texts base camp through three Meshtastic hops on a node that runs for days.

**Key point:** LoRa does not beat Bluetooth — it takes the opposite corner of the triangle: kilometers of range at bytes of bandwidth, on multi-day batteries.

### Visualization (canvas `c3`, 720×300)

Log-log scatter of range per hop vs bandwidth for BLE, WiFi Direct, LoRa, cellular, satellite — realistic magnitudes, labeled illustrative.

- **Title (bold 15px, `#1a5276`, top center):** "Range vs Bandwidth: Every Radio Picks a Corner (log-log, illustrative)".
- **Axes:** origin x=80, baseline y=240, plot width 600, plot height 185; both axes 1px `#999`. X = log10 range in meters, domain log 1 to 7, ticks 12px `#444` at "10 m", "100 m", "1 km", "10 km", "100 km", "1000 km", "10⁴ km" (logs 1..7). Y = log10 bandwidth in bps, domain log 2 to 9, gridlines `#e5e9ef` and 12px `#444` tick labels at "1 kbps" (3), "1 Mbps" (6), "1 Gbps" (9). Axis titles 12px `#444`: "range per hop" centered below, "bandwidth" rotated on the left.
- **Points (log10 range, log10 bps):** BLE (1.7, 6.0) `#2a78d6`; WiFi Direct (2.2, 8.0) `#199e70`; LoRa (3.9, 3.0) `#d95926` drawn larger (r=8 vs r=6) with a 2px orange halo ring; cellular LTE (4.0, 7.7) `#6b7280`; satellite text (6.3, 3.7) `#4a3aa7`. Each point gets a bold 12px label in its own color next to it; cellular and satellite labels append "(needs tower)" / "(needs constellation)" in 11px `#6b7280` on a second line.
- **Annotation (bold 13px `#d95926`, near (330,215) pointing at LoRa):** "LoRa: kilometers of range, bytes of bandwidth".
- **Annotation (bold 12px `#2a78d6`, near BLE/WiFi cluster (150,80)):** "meters of range, megabits of speed".
- **Caption (11px `#6b7280`, bottom right at (700,292), right-aligned):** "order-of-magnitude values, illustrative".

## What the Mesh Gives Up

**Tags:** `trade-off` (orange), `limits` (red), `encryption` (green)

- **No guarantee** — Without a server of record, a message can simply vanish and nobody is told.
- **No history** — There is no server to sync missed conversations from; time away means gaps.
- **Latency** — Delivery rides on chance encounters, so it can take minutes or even hours.
- **Capacity** — Every message floods many nodes, so total throughput shrinks as traffic grows.
- **Encryption holds** — End-to-end sealing survives; untrusted relays carry unreadable ciphertext.
- **Metadata leaks** — Nearby nodes can tell who is transmitting; the radio itself reveals presence.

*Example (italic):* During an internet shutdown a mesh text reaches Bob in 40 minutes — the same words normally arrive in 200 ms.

**Key point:** Mesh is a complement for when infrastructure fails, not a replacement for it — it wins exactly one column: still working when the internet does not.

### Visualization (canvas `c4`, 720×300)

Comparison strip: server messaging vs mesh messaging across five properties, with mesh winning only the final column.

- **Title (bold 15px, `#1a5276`, top center):** "Server vs Mesh: Mesh Wins Exactly One Column".
- **Column headers (bold 12px `#1a5276`, centered at y=78):** "delivery guarantee", "speed", "capacity", "history sync", "works when internet dies" at x = 218, 330, 435, 540, 655 (last header split over two lines at y=64/78).
- **Highlight:** the last column gets a full-height rounded background rect x=600, y=48, w=110, h=200, fill `rgba(0,131,0,0.08)`, 2px `#008300` border.
- **Row labels (bold 13px, left-aligned at x=30):** "server messaging" in `#2a78d6` at y=125; "mesh messaging" in `#d95926` at y=185. Thin `#e5e9ef` horizontal rule between rows (y=152, from x=25 to x=710) and under the header (y=92).
- **Server row cells (13px, centered on y=125):** "✓ acked" bold `#008300`; "~200 ms" `#2c3e50`; "scales up" `#2c3e50`; "✓ full" bold `#008300`; "✗ dead" bold `#e74c3c`.
- **Mesh row cells (13px, centered on y=185):** "✗ best effort" bold `#d95926`; "min–hours" `#d95926`; "shrinks w/ load" `#d95926`; "✗ none" `#6b7280`; "✓ works" bold 15px `#008300`.
- **Sub-strip (encryption vs metadata), 12px at y=228:** left half — bold `#008300` "encryption still works end-to-end" centered at (218,228) with 11px `#6b7280` "relays carry only ciphertext" at (218,244); right half — bold `#c98500` "metadata gets worse" centered at (500,228) with 11px `#6b7280` "neighbors see who transmits" at (500,244).
- **Annotation (bold 13px `#1a5276`, centered at (360,278)):** "a complement for infrastructure failure — not a replacement for infrastructure".
- **Caption (11px `#6b7280`, bottom right, right-aligned at (710,296)):** "timings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...` written as full sentences, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). No `Math.random()`, no dates — all coordinates and values are the hardcoded literals above.
- **Chart palette object:** `const P = { blue:'#2a78d6', green:'#008300', magenta:'#d55181', yellow:'#c98500', aqua:'#199e70', orange:'#d95926', violet:'#4a3aa7', ink:'#1a5276', text:'#2c3e50', mute:'#6b7280', grid:'#e5e9ef' }`. Navy `#1a5276` is ink for headings, axes, callout borders. Red `#e74c3c` only for genuine failure states (offline message loss, server messaging dead without internet).
- **Data:** all chart values are invented order-of-magnitude illustrations and are labeled "illustrative"; the factual anchors are public tech history — XMPP (1999), Matrix room replication (2014), BLE range ~10-100 m, FireChat (2014, Hong Kong), Bridgefy (2019-20 protests), bitchat (2025), LoRa 2-15 km hops on unlicensed ISM spectrum, Meshtastic (~$30 nodes) and goTenna, and the standard mesh limits (no delivery guarantee, no history sync, flooding capacity, E2E over untrusted relays, radio-presence metadata).
- **Framing:** factual, neutral tech-history tone; protest-era usage described briefly as usage context with no political commentary. Alice/Bob for people.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
