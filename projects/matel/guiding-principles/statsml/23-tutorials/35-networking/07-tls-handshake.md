# TLS Handshake

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** TLS Handshake

**Subtitle:** Two strangers agree on a secret key over a wire everyone can read — the handshake behind every https:// page

## A Secret Agreed Over the Café Wi-Fi

**Tags:** `core idea` (blue), `key exchange` (green), `https` (orange)

- **The café** — Maya opens an online bookstore on the coffee shop Wi-Fi that anyone nearby can sniff
- **The problem** — her card number must cross a wire where every byte is visible to strangers
- **The hello** — her browser sends a ClientHello: "here are the ciphers I speak, and my key share"
- **The reply** — the server answers with its own key share, its certificate, and "let's use this cipher"
- **The trick** — both sides now compute the same secret key, yet the key itself never crossed the wire
- **The padlock** — from the next message on, everything is encrypted with that freshly agreed key

*Example (italic):* The eavesdropper at the next table records every byte of the handshake and still cannot read the order Maya places one second later.

**Key point:** A TLS handshake is the short opening exchange where two parties who have never met agree on a shared secret key in full public view — that is what the browser padlock certifies.

### Visualization (canvas `c1`, 720×300)

Three-lane message flow diagram: Maya's laptop (left), the open café Wi-Fi (middle lane, eavesdropper), the bookstore server (right), with the TLS 1.3 handshake messages as arrows.

- **Title (bold 15px, `#1a5276`, top center):** "One Round Trip in Public, Then Everything Is Locked".
- **Lanes:** vertical 2px `#e5e9ef` lifelines at x=130 ("Maya's laptop"), x=360 ("open Wi-Fi — everyone can read this"), x=590 ("bookstore server"); lane headers bold 12px `#2c3e50` at y=55, the middle header in mute `#6b7280`.
- **Arrow 1 (y=105):** blue `#2a78d6` 3px arrow left→right, 12px blue label above: "ClientHello + key share A".
- **Arrow 2 (y=150):** green `#008300` 3px arrow right→left, 12px green label above: "ServerHello + key share B + certificate".
- **Arrow 3 (y=195):** violet `#4a3aa7` 3px arrow left→right, 12px violet label above: "Finished — encrypted from here on"; from y=195 down, both lifelines drawn 3px solid violet to suggest the locked channel.
- **Eavesdropper marker:** 12px mute `#6b7280` eye symbol/label "sees A, B" beside the middle lane at y=170.
- **Annotation (bold 13px green `#008300`, near x=360, y=240):** "the key never crosses the wire — only the shares do".
- **Caption (12px `#444`, bottom right):** "TLS 1.3 simplified — the certificate actually travels encrypted".

## The Toy Key Exchange: g=5, p=23

**Tags:** `worked example` (blue), `Diffie-Hellman` (green)

- **Public agreement** — both sides openly agree to work with the numbers g=5 and p=23
- **Private picks** — Maya secretly picks a=6; the server secretly picks b=15; these never leave home
- **Public shares** — Maya sends A = 5^6 mod 23 = 8; the server sends B = 5^15 mod 23 = 19
- **Same secret** — Maya computes 19^6 mod 23 = 2; the server computes 8^15 mod 23 = 2 — identical
- **The eavesdropper** — sees 5, 23, 8, 19 but recovering a or b from them is the hard part
- **Real scale** — real TLS does this with numbers hundreds of digits long (or elliptic curves)

*Example (italic):* From the server's public 19 and her private 6, Maya gets 19^6 mod 23 = 2; the server gets the same 2 from Maya's public 8 and its private 15 — and 2 becomes the seed of the session key.

**Key point:** Each side combines its own private number with the other's public share, and modular arithmetic guarantees both mixtures land on the same secret — while undoing the mix from the public values alone is computationally infeasible at real sizes.

### Visualization (canvas `c2`, 720×300)

Two-column mixing diagram: Maya's private column (left), server's private column (right), a shaded public strip down the middle showing exactly what the eavesdropper sees.

- **Title (bold 15px, `#1a5276`, top center):** "Mix in Private, Swap in Public, Land on the Same 2".
- **Public strip:** vertical band from x=280 to x=440, fill `rgba(107,114,128,0.08)`, 12px mute `#6b7280` header "public wire" at y=55; inside it three 12px `#2c3e50` lines: "g=5, p=23" (y=95), "A=8 →" (y=150), "← B=19" (y=185).
- **Maya column (boxes at x=60, width 190, height 34, 8px radius, fill `rgba(42,120,214,0.15)`, 12px text):** "secret a=6" (y=90), "A = 5^6 mod 23 = 8" (y=140), "key = 19^6 mod 23 = 2" (y=210, green fill `rgba(0,131,0,0.12)`).
- **Server column (boxes at x=470, same style):** "secret b=15" (y=90), "B = 5^15 mod 23 = 19" (y=140), "key = 8^15 mod 23 = 2" (y=210, green fill).
- **Arrows:** 2px blue `#2a78d6` arrow from Maya's A box into the strip; 2px green `#008300` arrow from the server's B box into the strip; thin `#e5e9ef` arrows from each side's secret box down to its key box.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=270):** "both reach 2 — the eavesdropper holds 5, 23, 8, 19 and stays locked out".
- **Caption (12px `#444`, bottom right):** "toy numbers for hand-checking; real TLS uses ~2048-bit values or elliptic curves".

## Every https:// Call Pays for This Handshake

**Tags:** `where it's used` (blue), `latency` (green), `1-RTT` (orange)

- **Everywhere** — every https page, API call, database TLS connection, and pip/npm install starts here
- **The cost** — the handshake is round trips: nothing useful moves until the key is agreed
- **TLS 1.2** — needed 2 round trips before data; to a server 50ms away that is 100ms of pure waiting
- **TLS 1.3** — collapses it to 1 round trip: 50ms to the same server, half the wait gone
- **Resumption** — a returning client can reuse a prior secret and send data immediately (0-RTT)
- **The symptom** — a data scientist sees it as the mysterious fixed overhead on every fresh API session

*Example (italic):* A pipeline making 1,000 fresh https connections to a 50ms-away API spends 100 seconds inside TLS 1.2 handshakes but only 50 seconds under TLS 1.3.

**Key point:** The handshake is a fixed latency tax on every new connection — which is why connection reuse, TLS 1.3, and session resumption are the first fixes for "slow API" complaints.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: handshake waiting time before the first byte of data, for a server 50ms round trip away, across three handshake modes.

- **Title (bold 15px, `#1a5276`, top center):** "Handshake Wait Before Any Data Moves (server 50ms away)".
- **Axis:** baseline vertical 2px `#999` line at x=250; bars extend right, scale 4px per ms, max width 400; x gridlines `#e5e9ef` at 50ms (x=450) and 100ms (x=650) with 12px `#444` labels.
- **Rows (bar height 26px, centered at y = 90, 155, 220), each with a right-aligned 12px `#444` label ending at x=240:**
  - "TLS 1.2 — 2 round trips": orange `#d95926` bar width 400 (100ms), 12px orange value label "100ms" at the bar end
  - "TLS 1.3 — 1 round trip": blue `#2a78d6` bar width 200 (50ms), value label "50ms"
  - "TLS 1.3 resumed — 0-RTT": green `#008300` bar width 8 (~0ms), value label "~0ms"
- **Annotation (bold 13px green `#008300`, near x=470, y=225):** "1,000 fresh connections: 100s vs 50s of pure waiting".
- **Caption (12px `#444`, bottom right):** "50ms RTT illustrative; round-trip counts are the TLS 1.2 / 1.3 protocol facts".

## Encrypted Is Not the Same as Authenticated

**Tags:** `common mistake` (red), `certificates` (orange)

- **The gap** — key exchange alone builds a perfectly private channel to whoever answered, friend or fraud
- **The attack** — a man-in-the-middle runs two handshakes: one with Maya, one with the real server
- **Two padlocks** — both channels are genuinely encrypted; Maya's secrets are simply read in the middle
- **The fix** — the certificate: a signature chain proving the key share really belongs to the bookstore
- **The check** — the browser verifies the chain up to a trusted root before trusting the handshake
- **The habit** — clicking through certificate warnings hands the attacker exactly this opening

*Example (italic):* On a rogue café hotspot, Maya's handshake succeeds and the padlock math is flawless — but without the certificate check she has securely encrypted her card number straight to the attacker.

**Common mistake:** Treating encryption as proof of safety. The Diffie-Hellman mixing hides the conversation; only the certificate says who is on the other end — TLS needs both, and a certificate warning means the second half just failed.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: handshake without certificate verification (attacker relays in the middle, reads everything) vs with verification (forged certificate rejected, connection aborted).

- **Title (bold 15px, `#1a5276`, top center):** "Two Perfect Padlocks, Wrong Party: Why the Certificate Matters".
- **Row 1 (y=95), 12px `#444` label at x=20:** "no cert check"; blue `#2a78d6` rounded box at x=140 labeled "Maya" (12px), 3px arrow to a red `#e74c3c` box at x=330 labeled "attacker — reads all", 3px arrow on to a blue box at x=545 labeled "real server"; small mute `#6b7280` padlock glyphs on both arrows; bold 12px red "✗ both links encrypted, secrets exposed" under the row at y=140.
- **Row 2 (y=215), label:** "with cert check"; blue box "Maya" at x=140, 3px arrow to a red box at x=330 labeled "forged certificate", then a 3px green `#008300` arrow stub ending in a bold 14px green "✕ abort" at x=520 with bold 12px green "✓ handshake refused, nothing sent" under the row at y=260.
- **Box style:** 130–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, top right near y=60):** "encryption hides the words; the certificate names the listener".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the toy Diffie-Hellman numbers (g=5, p=23, a=6, b=15, A=8, B=19, shared secret 2) are exact and hand-checkable; the 50ms RTT and the 1,000-connection totals (100s vs 50s) are invented and labeled illustrative; the round-trip counts (TLS 1.2 = 2 RTT, TLS 1.3 = 1 RTT, resumption 0-RTT) are protocol facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
