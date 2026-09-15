# iMessage

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** iMessage

**Subtitle:** The messenger hiding inside the push channel — Apple reused the always-on notification connection every iPhone already holds as the pipe that carries the messages themselves

## The Push Channel Is the Message Pipe

**Tags:** `core idea` (blue), `APNs transport` (green), `2011` (orange)

- **One connection** — every Apple device keeps a single persistent connection to APNs for everything
- **Reused pipe** — iMessage (2011) sends its messages through that same channel, not a new socket
- **Free wake-up** — the radio already listens for pushes, so delivery costs no extra battery
- **Store and forward** — messages queue at Apple while the device is offline, then push on reconnect
- **Not a notification** — the push payload is the message itself; the banner is just a side effect

*Example (italic):* Your iPhone learns of a new iMessage exactly the way it learns of a calendar alert — one push on the one pipe it already holds open.

**Key point:** iMessage did not build a delivery network — it rides the push connection every Apple device already keeps open, so the transport was free.

### Visualization (canvas `c1`, 720×300)

Side-by-side architecture comparison: the actual design (iMessage riding the shared APNs pipe) vs a hypothetical app holding its own second connection.

- **Title (bold 15px, `#1a5276`, top center):** "One Pipe for Everything: iMessages Share the APNs Connection".
- **Divider:** dashed 1px `#bdc3c7` vertical line at x=395 from y=35 to y=272.
- **Left panel label (bold 12px green `#008300`, centered at x=195, y=46):** "iMessage (actual): reuse the pipe".
- **Device box:** rounded rect x=25, y=115, w=90, h=70, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 12px "iPhone", 11px `#444` "one socket".
- **Pipe:** two horizontal 2px `#1a5276` lines (115,136)→(195,136) and (115,164)→(195,164); inside, three 6px-radius packet dots at x=130/155/180, y=150 — first two `#6b7280` (notifications), third `#2a78d6` (iMessage); 12px `#444` caption "notif · notif · message" centered at (155, 192).
- **APNs box:** rounded rect x=195, y=105, w=100, h=90, fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; bold 13px "APNs", 11px "push service".
- **Source boxes:** three rounded rects x=310, w=78, h=24 at y=108/140/172, 11px text "mail push", "cal alert", "iMessage"; first two 1.5px `#6b7280` border, third 2px `#2a78d6` border; short arrows from each into the APNs box right edge.
- **Offline note (bold 12px orange `#d95926`, centered at x=245, y=222):** "offline? messages queue at Apple".
- **Right panel label (bold 12px orange `#d95926`, centered at x=555, y=46):** "hypothetical app: own connection".
- **Device box:** same style as left, x=430, y=115, w=90, h=70, "iPhone", "two sockets".
- **Top pipe:** solid 2px `#1a5276` line pair from device to rounded box "APNs" (x=605, y=88, w=85, h=44), 12px `#444` label "notifications" above the pipe.
- **Bottom pipe:** dashed 2px `#d95926` line pair from device to rounded box "app server" (x=605, y=168, w=85, h=44), bold 12px `#d95926` label "second socket" below the pipe.
- **Cost note (bold 12px `#d95926`, centered at x=555, two lines y=245/260):** "extra battery, keepalives," / "reconnects on every network change".
- **Bottom annotation (bold 13px violet `#4a3aa7`, centered at x=360, y=290):** "iMessage added zero new connections — the transport already existed".

## IDS: One Handle, Many Devices, Many Keys

**Tags:** `worked example` (blue), `key directory` (green), `blue vs green` (orange)

- **Handle lookup** — when you type a recipient, Messages asks IDS which devices that handle owns
- **Per-device keys** — IDS returns a public key for each registered device, not one per person
- **N copies** — the sender encrypts a separate copy of the message for every recipient device
- **Everywhere at once** — that is why one text lands on iPhone, iPad, and Mac simultaneously
- **Blue or green** — if IDS finds no registered devices, Messages falls back to SMS or RCS

*Example (italic):* Alice sends Bob one message; her phone encrypts it three times, once each for his iPhone, iPad, and Mac.

**Key point:** The bubble color is an IDS lookup result — registered devices found means iMessage (blue); none found means the carrier path (green).

### Visualization (canvas `c2`, 720×300)

Fan-out flow: sender queries IDS, receives one public key per recipient device, encrypts one copy per key, and APNs relays each copy to its device.

- **Title (bold 15px, `#1a5276`, top center):** "One Send, Three Encrypted Copies: the IDS Fan-Out".
- **IDS box:** rounded rect x=270, y=40, w=200, h=44, fill `rgba(201,133,0,0.10)`, 2px `#c98500` border; bold 12px "IDS directory", 11px "handle → devices + public keys".
- **Sender box:** rounded rect x=25, y=130, w=115, h=60, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 12px "Alice's iPhone", 11px "encrypts 3 copies".
- **Lookup arrows:** 2px `#c98500` line with arrowhead from (110,130) to (268,68), 12px `#c98500` label "1. look up Bob's handle" near (120,88); return arrow 2px `#c98500` dashed from (280,84) to (130,126), 12px label "2. three device keys" near (255,116).
- **Copy boxes:** three rounded rects x=250, w=160, h=34 at y=112/160/208, fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border, 11px text "copy 1 — enc(iPhone key)", "copy 2 — enc(iPad key)", "copy 3 — enc(Mac key)"; 2px `#6b7280` arrows from the sender box right edge fanning to each copy's left edge; bold 12px violet `#4a3aa7` label "3. encrypt per device" centered at (330, 258).
- **APNs relay:** rounded rect x=460, y=104, w=70, h=146, fill `rgba(107,114,128,0.10)`, 2px `#6b7280` border, bold 12px "APNs" + 11px "relay"; horizontal arrows from each copy box into it and out to each device box.
- **Device boxes:** three rounded rects x=580, w=115, h=34 at y=112/160/208, fill `rgba(0,131,0,0.10)`, 2px `#008300` border, 12px text "Bob's iPhone", "Bob's iPad", "Bob's Mac".
- **Bottom annotation (bold 12px green `#008300`, centered at x=360, y=288):** "IDS returns no devices → conversation falls back to SMS / RCS (green bubble)".

## Encrypted Per Device, Relayed as Ciphertext

**Tags:** `encryption` (green), `caveat` (red), `PQ3` (orange)

- **E2E since 2011** — content is encrypted on the sender's device; Apple relays only ciphertext
- **Visible envelope** — Apple still sees who messaged whom, when, and to which registered devices
- **Directory trust** — Apple runs the key directory, so a compromised IDS could add a silent device
- **Backup caveat** — default iCloud Backup historically kept message content Apple could read
- **ADP opt-in** — Advanced Data Protection (2022) extends end-to-end encryption to the backup
- **PQ3 upgrade** — the 2024 protocol added post-quantum key exchange with periodic rekeying

*Example (italic):* A message crosses Apple's servers as unreadable ciphertext, yet its default iCloud backup was readable by Apple until ADP.

**Key point:** The channel is end-to-end encrypted, but the guarantee is only as strong as the key directory and the backup settings around it.

### Visualization (canvas `c3`, 720×300)

Encryption-boundary diagram: plaintext exists only inside the device zones, ciphertext plus metadata in the Apple zone, with a bottom lane for the backup caveat and protocol upgrades.

- **Title (bold 15px, `#1a5276`, top center):** "The Encryption Boundary: What Apple's Servers Can and Cannot See".
- **Sender zone:** rounded rect x=30, y=52, w=170, h=110, fill `rgba(0,131,0,0.08)`, 2px `#008300` border; bold 12px "Alice's device", 12px lines "plaintext" / "encrypt per device key".
- **Apple zone:** rounded rect x=230, y=52, w=260, h=110, fill `rgba(107,114,128,0.10)`, 2px dashed `#1a5276` border; bold 12px "Apple servers (IDS + APNs)"; 11px lines "sees: sender, recipient, time," / "device list — and ciphertext"; bold 12px `#008300` "cannot read content".
- **Recipient zone:** rounded rect x=520, y=52, w=170, h=110, fill `rgba(0,131,0,0.08)`, 2px `#008300` border; bold 12px "Bob's devices", 12px lines "decrypt" / "plaintext".
- **Arrows:** 2px `#6b7280` arrows zone-to-zone at y=107, each with a 11px `#6b7280` label "ciphertext" above.
- **Lane label (bold 12px `#1a5276`, left-aligned at x=30, y=196):** "the backup caveat — and the upgrades".
- **Caveat box:** rounded rect x=30, y=210, w=210, h=56, fill `rgba(217,89,38,0.08)`, 2px `#d95926` border; bold 12px "iCloud Backup (default)", 11px "Apple holds the key — content readable".
- **ADP box:** rounded rect x=262, y=210, w=210, h=56, fill `rgba(0,131,0,0.10)`, 2px `#008300` border; bold 12px "Advanced Data Protection (2022)", 11px "backup becomes end-to-end too".
- **PQ3 box:** rounded rect x=494, y=210, w=200, h=56, fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border; bold 12px "PQ3 (2024)", 11px "post-quantum keys, periodic rekey".
- **Caption (12px `#6b7280`, centered at x=360, y=290):** "opt-in ADP closes the backup gap; PQ3 hardens the channel itself".

## Blue, Green, and the Fallback Ladder

**Tags:** `fallback` (blue), `SMS` (green), `RCS` (orange)

- **Three transports** — the Messages app multiplexes iMessage over APNs, carrier SMS, and RCS
- **Per conversation** — the path comes from the IDS lookup, decided once per conversation
- **Silent fallback** — when data is unavailable, the app drops to the carrier path without asking
- **SMS legacy** — a green SMS is 160-character texting carried on the carrier signaling channel
- **RCS arrives** — iOS 18 (2024) added RCS, bringing receipts and better media to green chats
- **App-level extras** — reactions, edits, and unsend are app messages only the iMessage path carries

*Example (italic):* In a dead-data zone the same thread quietly delivers as green SMS, then returns to blue when the network is back.

**Key point:** Blue versus green is a transport statement, not a style choice — features like edits and reactions are app messages only the iMessage pipe can carry.

### Visualization (canvas `c4`, 720×300)

Left: per-conversation decision flow from recipient lookup down the fallback ladder. Right: capability comparison grid across the three paths.

- **Title (bold 15px, `#1a5276`, top center):** "Pick a Pipe: IDS Decides, the App Falls Back Silently".
- **Flow box 1:** rounded rect x=30, y=44, w=150, h=32, fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border, 12px "type a recipient"; 2px `#6b7280` arrow down to box 2.
- **Flow box 2 (question):** rounded rect x=30, y=100, w=160, h=44, 2px `#c98500` border, no fill, 12px two lines "IDS lookup:" / "devices registered?".
- **Yes branch:** 2px `#6b7280` arrow right with 11px `#008300` label "yes" to rounded rect x=230, y=100, w=115, h=44, fill `rgba(42,120,214,0.18)`, 2px `#2a78d6` border, bold 12px `#2a78d6` "iMessage" + 11px "blue bubble".
- **No branch:** arrow down with 11px `#6b7280` label "no" to question box x=30, y=172, w=160, h=44, 2px `#c98500` border, 12px "carrier + device" / "support RCS?".
- **RCS branch:** arrow right, label "yes", to rounded rect x=230, y=172, w=115, h=44, fill `rgba(25,158,112,0.14)`, 2px `#199e70` border, bold 12px `#199e70` "RCS" + 11px "green bubble".
- **SMS branch:** arrow down, label "no", to rounded rect x=30, y=244, w=160, h=38, fill `rgba(0,131,0,0.10)`, 2px `#008300` border, bold 12px `#008300` "SMS" + 11px "green bubble".
- **Fallback note (bold 12px orange `#d95926`, left-aligned at x=225, y=262, two lines):** "no data mid-thread?" / "drop to carrier path silently".
- **Capability grid (x=375..705):** column headers at y=64 — 12px bold: "iMessage" `#2a78d6` at cx=545, "RCS" `#199e70` at cx=618, "SMS" `#008300` at cx=680; feature labels 12px `#2c3e50` left-aligned at x=378 on rows y=94/122/150/178/206: "end-to-end encrypted", "typing + read receipts", "reactions, edits, unsend", "high-res media", "works without data"; cell marks 13px bold — "✓" `#008300`, "✗" `#6b7280`, "partial" 11px `#c98500`; row values: E2E ✓/✗/✗, receipts ✓/✓/✗, reactions-edits ✓/partial/✗, media ✓/✓/✗, without-data ✗/✗/✓; 1px `#e5e9ef` horizontal rules between rows.
- **Grid caption (11px `#6b7280`, left-aligned at x=378, y=238, two lines):** "RCS features vary by carrier profile;" / "RCS end-to-end encryption depends on profile version".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** the diagrams carry no invented statistics — they encode publicly documented mechanics: APNs as iMessage's transport (2011), IDS handle-to-device-key lookup with one encrypted copy per device, end-to-end encryption with Apple-visible routing metadata, the iCloud Backup readability caveat closed by opt-in Advanced Data Protection (2022), the PQ3 post-quantum protocol (2024), and the iMessage / RCS (iOS 18, 2024) / SMS transport ladder. The capability grid is simplified; RCS behavior varies by carrier profile.
- **Framing:** factual tech-history tone; describe blue/green mechanics as transport facts without editorializing.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
