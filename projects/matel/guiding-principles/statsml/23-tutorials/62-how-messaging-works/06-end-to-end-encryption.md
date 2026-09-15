# End-to-End Encryption

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** End-to-End Encryption

**Subtitle:** Encrypting so the server that delivers your message cannot read it — even though that same server is the only thing connecting you

## The Pipe Is Locked, but the Post Office Reads Your Mail

**Tags:** `core idea` (blue), `TLS vs E2E` (green), `history` (orange)

- **TLS today** — TLS encrypts the pipe to the server, so nothing leaks on the wire.
- **The catch** — The server sits between the two pipes and reads every message in plaintext.
- **End-to-end** — Only the two phones hold decryption keys, so the provider relays sealed bytes.
- **Blind courier** — The server still routes, queues, and delivers, all on ciphertext it cannot open.
- **OTR (2004)** — OTR pioneered forward secrecy for chat, but both parties had to be online at once.
- **Mobile gap** — A recipient's phone is usually asleep, so live handshakes were fatal for mobile.

*Example (italic):* Alice messages Bob over TLS; both pipes are locked, yet the server in the middle reads "meet at 7" in plaintext.

**Key point:** End-to-end encryption moves the trust boundary from the provider's servers to the two devices — the delivery service becomes a blind courier.

### Visualization (canvas `c1`, 720×300)

Two-row relay diagram contrasting TLS (server reads plaintext) with E2E (server passes a locked box through).

- **Title (bold 15px, `#1a5276`, top center):** "Two Locks: TLS Protects the Pipe, E2E Protects the Message".
- **Row 1 (boxes centered on cy=95, 44px tall, 8px radius), 12px `#444` label "TLS only" at x=20, y=58:** green `rgba(0,131,0,0.12)` box x=60 w=140 "Alice's phone / plaintext → pipe"; red `rgba(231,76,60,0.10)` box with `#e74c3c` border x=290 w=160 "server / decrypts, re-encrypts"; green box x=540 w=140 "Bob's phone / pipe → plaintext". Between boxes: 6px `rgba(42,120,214,0.5)` pipe lines at y=95 with a small blue padlock glyph at each midpoint (x=245, x=495) and bold 12px blue "TLS pipe" labels above at y=78. Bold 12px red `#e74c3c` annotation centered at (370, 142): "message readable at the server".
- **Divider:** dashed 1px `#bdc3c7` horizontal line at y=160, x=20..700.
- **Row 2 (boxes centered on cy=215), label "end-to-end" at x=20, y=178:** green box x=60 w=140 "Alice's phone / seals the box"; mute `rgba(107,114,128,0.12)` box x=290 w=160 "server / relays — cannot open"; green box x=540 w=140 "Bob's phone / opens the box". 3px `#6b7280` arrows between boxes at y=215; a small violet `#4a3aa7` locked-box glyph (18×14 rect + padlock) above each arrow at (245, 190) and (495, 190).
- **Annotations:** bold 12px green `#008300` centered at (360, 258): "only the endpoint devices hold keys — the courier is blind"; bold 13px orange `#d95926` centered at (360, 285): "same server, same delivery job — but now it carries a locked box".

## X3DH: Agreeing on a Secret with Someone Who Is Offline

**Tags:** `key exchange` (blue), `async` (green), `X3DH` (orange)

- **Prekeys** — Bob's app uploads a bundle of public prekeys to the server long before anyone writes.
- **The fetch** — Alice downloads Bob's bundle from the server while his phone is still asleep.
- **Combine** — She combines her identity and ephemeral keys with Bob's bundle into a shared secret.
- **Send now** — She encrypts with that secret and sends immediately; no waiting for Bob to appear.
- **Wake up** — Bob combines his matching private keys and derives the exact same secret next morning.
- **Blind server** — The server hands out public prekeys, but it can never compute the secret itself.

*Example (italic):* Alice fetches Bob's prekey bundle on Tuesday, sends an encrypted message, and Bob decrypts it on Wednesday.

**Key point:** X3DH turns key agreement into an async mailbox drop — public prekeys wait on the server so a shared secret can be derived while one side is offline.

### Visualization (canvas `c2`, 720×300)

Protocol ladder with three lifelines (Alice, Server, Bob) showing the prekey upload, fetch, offline queue, and delayed decrypt across four days.

- **Title (bold 15px, `#1a5276`, top center):** "X3DH: The Prekey Mailbox Drop (days apart)".
- **Lifelines:** Alice x=130, Server x=360, Bob x=590; names bold 13px `#1a5276` at y=48; dashed 1px `#6b7280` vertical lines y=58..272.
- **Arrows (3px with filled arrowheads, label 12px in the arrow's color, centered above each arrow):**
  1. y=92, Bob→Server, blue `#2a78d6`, label at (475, 84): "day 0 — upload public prekey bundle".
  2. y=130, Alice→Server, blue, label at (245, 122): "day 3 — request Bob's bundle".
  3. y=160, Server→Alice, aqua `#199e70`, label at (245, 152): "prekey bundle (public keys only)".
  4. y=210, Alice→Server, green `#008300`, label at (245, 202): "encrypted message".
  5. y=250, Server→Bob, green, label at (475, 242): "day 4 — deliver".
- **Side notes:** bold 12px violet `#4a3aa7` two lines centered on Alice's lifeline: "combine keys →" at (130, 182), "shared secret" at (130, 197); bold 12px orange `#d95926` at (360, 228): "queued — Bob offline"; bold 12px green at (590, 270): "same secret derived — decrypts".
- **Annotation (bold 13px orange, centered at (360, 292)):** "the server carries prekeys and ciphertext — it never learns the secret".

## The Double Ratchet: A New Key for Every Message

**Tags:** `forward secrecy` (blue), `self-healing` (green), `Double Ratchet` (orange)

- **Ratchet** — Every message turns the ratchet one click, so every message has its own fresh key.
- **One-way** — Each key is derived from the last by a one-way step; the chain cannot run backward.
- **Forward secrecy** — Stealing today's keys cannot decrypt yesterday's already-delivered traffic.
- **Self-healing** — A fresh key exchange in later replies locks out an attacker who had your keys.
- **Group chats** — Sender keys let one member encrypt a message once instead of once per member.

*Example (italic):* An attacker copies Bob's keys on Tuesday; Monday's chats stay sealed, and by Thursday's replies the attacker is locked out again.

**Key point:** The Double Ratchet limits damage in both directions of time — old messages stay safe forever, and new messages become safe again after one fresh exchange.

### Visualization (canvas `c3`, 720×300)

Ratchet chain of eight message keys with a compromise marker showing which past and future messages remain safe.

- **Title (bold 15px, `#1a5276`, top center):** "One-Way Key Chain: Where a Compromise Stops".
- **Chain:** eight rounded boxes (54×36, 8px radius) labeled K1..K8 (bold 13px), centers at x = 87, 169, 251, 333, 415, 497, 579, 661, all at cy=140; 3px `#6b7280` arrows between consecutive boxes; "msg 1".."msg 8" in 11px `#444` under each box at y=176. Colors: K1–K4 green `rgba(0,131,0,0.12)` fill / `#008300` border (safe past); K5–K6 red `rgba(231,76,60,0.10)` fill / `#e74c3c` border (exposed window); K7–K8 aqua `rgba(25,158,112,0.12)` fill / `#199e70` border (healed).
- **Compromise marker:** bold 13px red `#e74c3c` "attacker copies keys here" centered at (415, 62); 2.5px red arrow from (415, 70) down to (415, 118).
- **Group brackets (horizontal 2px lines at y=192 with 5px end ticks):** green x=60..360, red x=388..524, aqua x=552..688.
- **Bracket labels:** bold 12px green at (210, 210): "past messages: safe", 11px `#444` at (210, 224): "one-way chain cannot run backward"; bold 12px red at (456, 210): "exposed window", 11px `#444` at (456, 224): "until a fresh exchange"; bold 12px aqua at (620, 210): "healed", 11px `#444` at (620, 224): "fresh exchange → locked out".
- **Annotation (bold 13px violet `#4a3aa7`, centered at (360, 268)):** "forward secrecy protects the past; self-healing recovers the future".
- **Caption (12px `#444`, bottom right, y=292):** "Signal's Double Ratchet, simplified".

## Who Adopted It, and the Metadata That Remains

**Tags:** `adoption` (blue), `privacy limit` (red), `backups` (orange)

- **Lineage** — TextSecure (2010) became Signal, and its protocol became the industry standard.
- **WhatsApp** — In 2016 WhatsApp switched about a billion users to the Signal Protocol.
- **Everyone else** — Messenger made E2E the default in 2023, and Google Messages ships it over RCS.
- **iMessage** — Apple built its own per-device end-to-end design rather than adopting Signal's.
- **Metadata** — E2E hides what you said, not who you talk to, when, how often, or in which groups.
- **Backups** — An unencrypted cloud backup quietly undoes E2E by storing plaintext off the device.

*Example (italic):* Alice's words to Bob are unreadable, yet the server still sees a burst of messages between them every night at midnight.

**Key point:** E2E encryption protects the letter, not the mailing list — traffic patterns and careless backups remain the practical leaks.

### Visualization (canvas `c4`, 720×300)

Adoption timeline (2004–2023) above a two-panel strip contrasting what E2E hides with the metadata the server still sees.

- **Title (bold 15px, `#1a5276`, top center):** "From Niche Tool to Default: Twenty Years of E2E".
- **Timeline axis:** 2px `#999` horizontal line at y=112, x=60..660; year mapped x = 70 + (year − 2004) / 19 × 580.
- **Milestones (6px filled dot on the axis; name bold 12px in the dot's color, year 12px `#444`; "above" labels at y=78/93, "below" labels at y=136/151):** OTR 2004 violet `#4a3aa7` above (x=70); TextSecure 2010 blue `#2a78d6` above (x=253); iMessage 2011 yellow `#c98500` below (x=284); Signal Protocol 2013 magenta `#d55181` above (x=345); WhatsApp → 1B users 2016 green `#008300` below (x=436); Google Messages (RCS) 2021 aqua `#199e70` above (x=589); Messenger default 2023 orange `#d95926` below (x=650).
- **Two-panel strip (rounded boxes y=180, h=90, 8px radius):** left green box x=70 w=280, `rgba(0,131,0,0.08)` fill / `#008300` border, header bold 13px green at y=202 "E2E hides (content)", lines 12px `#2c3e50` at y=226/246: "message text and media", "voice and video call content"; right orange box x=380 w=280, `rgba(217,89,38,0.08)` fill / `#d95926` border, header bold 13px orange at y=202 "the server still sees (metadata)", lines 12px at y=226/244/262: "sender, recipient, timestamps", "message frequency and size", "group membership".
- **Annotation (bold 12px violet, centered at (360, 290)):** "sealed sender narrows the metadata — but a courier always sees traffic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared helpers: `roundedRect`, `arrow` (line + filled arrowhead), `lock` (small padlock glyph: 12×9 body + shackle arc).
- **Chart palette object:** `const P` — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for genuine risk states (server reading plaintext, compromised keys).
- **Data:** all positions and dates are the hardcoded literals above (no randomness, no Date calls); protocol dates are public history (OTR 2004, TextSecure 2010, WhatsApp completion 2016, Messenger default 2023); no key material is ever printed — keys are described abstractly (colored boxes, "Bob's bundle").
- **Framing:** conceptual tutorial for newcomers — intuition over math; the only cryptographic operation named is "combine keys"; no equations, no realistic key or token strings anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
