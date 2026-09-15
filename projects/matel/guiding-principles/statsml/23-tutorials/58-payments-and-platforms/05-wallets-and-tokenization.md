# Wallets & Tokenization

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Wallets & Tokenization

**Subtitle:** When you tap your phone to pay, Apple Pay and Google Pay hand the merchant a stand-in number — the real card number never leaves the bank's side of the wall

## The $6 Coffee That Hides Your Card

**Tags:** `core idea` (blue), `network tokenization` (green), `Apple Pay / Google Pay` (orange)

- **The tap** — you buy a $6 coffee by holding your phone to the terminal; Face ID confirms it's you
- **The enrollment** — when the card was added to the wallet, the network replaced the PAN with a token
- **The token** — a device-specific card number (DPAN) stored in the phone's secure element chip
- **The tap payload** — the terminal receives the DPAN plus a one-time cryptogram, never the real PAN
- **The mapping** — only the card network and issuer can translate the DPAN back to the real card

*Example (italic):* Your card is 4111 1111 1111 1111; the coffee shop's terminal only ever sees DPAN 4802 3391 0644 7729 plus a cryptogram valid for this one tap.

**Key point:** Network tokenization swaps the real card number (PAN) for a device-bound token (DPAN) at enrollment — from then on, merchants transact against the stand-in, never the original.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram of the $6 tap: phone secure element → coffee shop terminal → card network → issuing bank, showing where the DPAN travels and where the real PAN lives.

- **Title (bold 15px, `#1a5276`, top center):** "The $6 Tap: the Real Card Number Stops at the Network".
- **Boxes (rounded 8px radius, 40px tall, 12px `#2c3e50` text, centered on y=150):** phone box at x=30 width 140 labeled "phone secure element / DPAN 4802...7729" fill `rgba(42,120,214,0.15)` border 2px `#2a78d6`; terminal box at x=215 width 140 labeled "coffee terminal / sees DPAN only" fill `rgba(0,131,0,0.12)` border 2px `#008300`; network box at x=400 width 140 labeled "card network / token vault" fill `rgba(74,58,167,0.12)` border 2px `#4a3aa7`; issuer box at x=585 width 115 labeled "issuer / real PAN 4111...1111" fill `rgba(217,89,38,0.12)` border 2px `#d95926`.
- **Arrows:** 3px `#6b7280` arrows between consecutive boxes at y=150; 11px `#6b7280` labels above each: "DPAN + cryptogram", "DPAN + cryptogram", "PAN (after de-token)".
- **Wall marker:** vertical dashed `#e74c3c` (dash 5/4) line at x=395 from y=60 to y=250, bold 12px red label "PAN never crosses left of this line" at (x=390, y=52, right-aligned).
- **Annotation (bold 13px blue `#2a78d6`, near x=100, y=230):** "$6 approved — merchant never held the card number".
- **Caption (12px `#444`, bottom right):** "amount illustrative; flow per public network-tokenization specs".

## What the Terminal Receives: Swipe vs Tap

**Tags:** `worked example` (blue), `DPAN + cryptogram` (green)

- **The swipe** — a swiped card hands over the real PAN 4111 1111 1111 1111 and a static expiry
- **The tap** — the same card via wallet hands over DPAN 4802 3391 0644 7729 instead
- **The cryptogram** — each tap adds a one-time code computed in the secure element; it never repeats
- **The biometric** — Face ID or fingerprint approves the tap, replacing the PIN or signature step
- **The authorization** — the network looks up the DPAN in its vault, swaps in the PAN, asks the issuer
- **Hand-check** — replay yesterday's tap data today and the issuer declines: the cryptogram is stale

*Example (italic):* For the $6 coffee, the tap message carries {DPAN 4802...7729, cryptogram A93F...} — the swipe message would have carried {PAN 4111...1111, expiry 12/27} reusable as-is.

**Key point:** A tap sends two things a swipe never had — a stand-in card number and a single-use cryptogram — so the message authorizes exactly one payment and identifies no real card.

### Visualization (canvas `c2`, 720×300)

Side-by-side "what the terminal receives" panels: swiped card payload on the left, wallet tap payload on the right, field by field.

- **Title (bold 15px, `#1a5276`, top center):** "Same $6 Coffee, Two Very Different Payloads".
- **Left panel (swipe):** rounded box x=45 y=60 width 300 height 185, fill `rgba(231,76,60,0.06)`, border 2px `#e74c3c`; bold 13px `#e74c3c` header "SWIPED CARD" at top center of panel; three 12px `#2c3e50` field rows at y = 115, 150, 185: "PAN: 4111 1111 1111 1111", "expiry: 12/27", "cryptogram: none"; bold 12px red footer at y=222 centered: "reusable anywhere".
- **Right panel (tap):** rounded box x=380 y=60 width 300 height 185, fill `rgba(0,131,0,0.06)`, border 2px `#008300`; bold 13px `#008300` header "WALLET TAP" at top center; three field rows at the same y positions: "DPAN: 4802 3391 0644 7729", "expiry: token expiry", "cryptogram: A93F... (one-time)"; bold 12px green footer at y=222 centered: "valid for this tap only".
- **Divider:** 12px `#6b7280` "vs" centered at (360, 155).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "the real PAN appears in the left payload only".
- **Caption (12px `#444`, bottom right):** "card and token digits invented, illustrative".

## When the Merchant Database Gets Stolen

**Tags:** `where it's used` (blue), `breach damage` (green), `card-on-file` (orange)

- **The breach** — a thief dumps a merchant database holding 40,000 stored customer card records
- **Swipe world** — 40,000 real PANs leak; each one works at any other merchant until reissued
- **Token world** — 40,000 DPANs leak; each is bound to one device or merchant and needs a cryptogram
- **Reuse count** — cards usable elsewhere: 40,000 vs 0; the token dump is worthless to the thief
- **Card-on-file** — the same idea protects saved cards at online stores: the site stores a token, not the PAN
- **Cleanup cost** — a PAN breach forces mass reissue; a token breach is closed by revoking tokens

*Example (italic):* After the breach, the 4111...1111 swipe record buys things all over town; the 4802...7729 token record fails everywhere — wrong device, no cryptogram.

**Key point:** Tokenization shrinks the blast radius of a merchant breach from "every stolen card is live" to "nothing stolen is usable" — which is why networks push it for wallets and card-on-file alike.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: of 40,000 stolen records, how many are usable at other merchants — swiped-PAN database vs tokenized database.

- **Title (bold 15px, `#1a5276`, top center):** "Breach of 40,000 Stored Records: What Can the Thief Reuse?".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420 = 40,000 records; x gridlines `#e5e9ef` at 10,000 / 20,000 / 30,000 with 11px `#6b7280` labels along y=255.
- **Row 1 (bar centered y=105), left-aligned 12px `#444` label at x=20:** "stored real PANs (swipe era)"; red `#e74c3c` bar height 34, width 420, bold 12px red value label "40,000 reusable" at bar end.
- **Row 2 (bar centered y=185), label:** "stored network tokens (DPANs)"; green `#008300` bar height 34, width 3 (visual sliver), bold 12px green value label "0 reusable — wrong domain/device, no cryptogram" right of the sliver.
- **Annotation (bold 13px magenta `#d55181`, near x=300, y=245):** "the breach still happens — it just stops mattering".
- **Caption (12px `#444`, bottom right):** "record counts illustrative; reuse logic exact per token binding".

## A Token Is Not an Encrypted Card Number

**Tags:** `common mistake` (red), `tokenization vs encryption` (orange)

- **The confusion** — people assume the DPAN is the PAN scrambled by a key that could be cracked
- **No math link** — a token is a fresh number mapped to the PAN in the network's vault, not a cipher
- **Nothing to crack** — no key exists that turns 4802...7729 back into 4111...1111; only the vault knows
- **Still not enough alone** — a token without its one-time cryptogram is declined even on the right device
- **The mistake** — treating any masked or hashed PAN in your own database as "tokenized like Apple Pay"

*Example (italic):* A thief with infinite computing power gains nothing from 4802...7729 — the mapping to 4111...1111 is a vault row at the network, not an equation to invert.

**Common mistake:** Calling encryption or hashing "tokenization". Encrypted PANs can be decrypted wherever the key leaks; a network token has no key, no formula, and no value outside its device and its vault.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram contrasting encryption (reversible with a key) and tokenization (vault lookup only), each starting from the same real PAN.

- **Title (bold 15px, `#1a5276`, top center):** "Encryption Has a Key. Tokenization Has a Vault.".
- **Row 1 (boxes centered y=100), 12px `#444` row label "encryption" at x=20:** blue `#2a78d6` rounded box at x=130 width 150 labeled "PAN 4111...1111", 3px arrow labeled "encrypt" (11px `#6b7280`) to an orange `#d95926` box at x=340 width 150 labeled "ciphertext 8f2c...", 3px arrow labeled "leaked key" to a red `#e74c3c` box at x=550 width 150 labeled "PAN recovered" with bold 12px red "✗ reversible" beneath it.
- **Row 2 (boxes centered y=210), row label "tokenization":** blue box at x=130 width 150 labeled "PAN 4111...1111", 3px arrow labeled "vault assigns" to a green `#008300` box at x=340 width 150 labeled "token 4802...7729", 3px arrow labeled "no key exists" drawn dashed (dash 4/3) to a green box at x=550 width 150 labeled "dead end for thief" with bold 12px green "✓ not invertible".
- **Box style:** 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "a stolen token is a pointer into a vault you can't reach".
- **Caption (12px `#444`, bottom right):** "digits invented, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the $6 coffee amount, 40,000 breach records, and all card/token/cryptogram digits (4111 1111 1111 1111, 4802 3391 0644 7729, A93F...) are invented and labeled illustrative; the tokenization mechanics (DPAN in the secure element, one-time cryptogram per tap, network/issuer de-tokenization, biometric in place of PIN/signature, token uselessness outside its device) are the publicly documented behavior of network tokenization and are stated as exact.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
