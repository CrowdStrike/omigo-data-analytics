# Man-in-the-Middle & Replay

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Man-in-the-Middle & Replay

**Subtitle:** An attacker sitting on the wire between you and a server can read, change, or re-send your messages — certificates and nonces are the reasons they usually can't

## The Coffee-Shop Hotspot That Reads Your Mail

**Tags:** `core idea` (blue), `on-path attacker` (red), `defensive` (green)

- **The scene** — a laptop in a coffee shop joins a free Wi-Fi hotspot named after the shop
- **The twist** — the hotspot is a stranger's box in a backpack, not the shop's router
- **The position** — every request to the bank now flows through the attacker's box first
- **The illusion** — the laptop thinks it reached the bank; the bank thinks the laptop called directly
- **The power** — sitting on the wire, the attacker can read every byte and quietly rewrite any of them

*Example (italic):* You type your bank password on "CafeFreeWiFi"; the backpack box forwards it to the real bank so everything works — and keeps a copy.

**Key point:** A man-in-the-middle attack is an attacker positioned on the network path between two parties, relaying traffic so both sides believe they are talking directly — while the middle reads or modifies everything.

### Visualization (canvas `c1`, 720×300)

Flow diagram contrasting the connection both sides believe in (direct) with the actual path through the attacker's hotspot.

- **Title (bold 15px, `#1a5276`, top center):** "What Both Sides Believe vs What Actually Happens".
- **Top row (y=100):** blue `#2a78d6` rounded box at x=60 (140×44) labeled "your laptop" (12px), dashed `#6b7280` (dash 5/4) 2px arrow straight to a blue box at x=520 (140×44) labeled "bank server"; 12px `#6b7280` label "the connection both sides believe in" centered above the arrow at y=78.
- **Bottom row (y=205):** the same laptop and bank boxes at x=60 and x=520, with a red `#e74c3c` rounded box at x=290 (150×44) labeled "hotspot (attacker)" between them; solid 3px `#e74c3c` arrows laptop→attacker and attacker→bank.
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` for endpoints and `rgba(231,76,60,0.12)` for the attacker, 12px `#2c3e50` text.
- **Annotation (bold 13px red `#e74c3c`, centered at y=270):** "the middle box sees every byte — and can change any of them".
- **Caption (12px `#444`, bottom right):** "schematic — defensive illustration".

## Encrypted Twice, Protected Zero Times

**Tags:** `worked example` (blue), `certificates` (green), `authentication` (orange)

- **Step 1** — the laptop opens an encrypted session; the attacker answers, pretending to be the bank
- **Step 2** — the attacker opens a second encrypted session to the real bank, pretending to be the laptop
- **Step 3** — traffic is decrypted in the middle, read, then re-encrypted; both links look secure
- **The gap** — encryption hid the bytes from outsiders, but nobody checked WHO holds the other end
- **The fix** — TLS makes the server prove its name with a certificate signed by a trusted authority
- **The wall** — the attacker cannot forge that signature, so the browser throws a loud warning instead

*Example (italic):* The attacker terminates one encrypted link and starts another, reading the password in between — until the browser demands a certificate chaining to a trusted authority, which the attacker cannot produce.

**Key point:** Encryption without authentication only guarantees privacy from third parties — not that the second party is who you think. Certificates add the identity proof that makes the middle position untenable.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram: encryption alone (attacker bridges two encrypted sessions and reads in the middle) vs TLS with certificate check (forged identity is blocked).

- **Title (bold 15px, `#1a5276`, top center):** "Encryption Alone vs Encryption + Certificate".
- **Row 1 (y=95), label 12px `#444` at x=20:** "encryption only"; blue `#2a78d6` rounded box at x=140 (110×40) labeled "laptop", 3px arrow labeled "encrypted" (11px `#6b7280`) to a red `#e74c3c` box at x=310 (170×40) labeled "attacker: decrypt, read, re-encrypt", 3px arrow labeled "encrypted" to a blue box at x=560 (110×40) labeled "bank"; bold 12px red "✗ password read in the middle" beneath the row at y=140.
- **Row 2 (y=210), label:** "TLS + certificate"; the same laptop box at x=140, arrow to a red box at x=310 (170×40) labeled "attacker: fake certificate, no CA signature", arrow to an orange `#e67e22` box at x=560 (130×40) labeled "browser warning — page blocked"; bold 12px green `#008300` "✓ attack stopped before any data flows" beneath at y=255.
- **Box style:** 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(230,126,34,0.15)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=282):** "the certificate is the identity check encryption never had".

## Replaying Yesterday's Message

**Tags:** `worked example` (blue), `replay attack` (red), `nonces` (green)

- **The lazy cousin** — the attacker can't read the encrypted transfer, so they just record it whole
- **The replay** — the captured "$50 transfer" message is valid and signed, so they send it 4 more times
- **The damage** — 5 accepted requests move $250 (illustrative) when the customer authorized $50 once
- **Defense 1: nonce** — the message carries single-use number 8317; the server accepts 8317 exactly once
- **Defense 2: timestamp** — a 2:00pm stamp is stale by 2:02pm; this only shrinks the replay window
- **Defense 3: sequence** — message #42 can't follow message #45; out-of-order copies are dropped

*Example (italic):* With nonce 8317 attached, the first transfer succeeds and the 4 replays are refused as duplicates — $50 moves, not $250.

**Key point:** A replay attack re-sends a captured valid message; the classic defense trio — nonces, timestamps, and sequence numbers — makes each message acceptable exactly once, so copies are worthless.

### Visualization (canvas `c3`, 720×300)

Timeline of the same 5 transfer requests under two servers: one without replay protection (all accepted) and one checking a nonce (first accepted, replays rejected).

- **Title (bold 15px, `#1a5276`, top center):** "One $50 Transfer, Sent 5 Times: With and Without a Nonce".
- **Axes:** x = arrival time, 5 request slots at x = `[130, 240, 350, 460, 570]` labeled "0s", "30s", "60s", "90s", "120s" (12px `#444` at y=265); two row baselines with left labels 12px `#444` at x=20: "no defense" at y=110 and "nonce 8317" at y=200.
- **No-defense row (y=110):** 5 filled green `#008300` circles (radius 11) at the slot x positions, each with white bold 11px "✓" centered; bold 12px red `#e74c3c` running total "$250 gone" at x=640, y=110.
- **Nonce row (y=200):** 1 filled green circle with "✓" at x=130, then 4 red `#e74c3c` "✗" marks (bold 18px) at x = `[240, 350, 460, 570]`; 11px `#6b7280` label "duplicate nonce — rejected" centered under the ✗ group at y=228; bold 12px green "$50 — as authorized" at x=640, y=200.
- **Gridlines:** vertical `#e5e9ef` 1px lines at each slot x from y=80 to y=245.
- **Annotation (bold 13px `#1a5276`, top left at x=60, y=60):** "same bytes every time — only the first should count".
- **Caption (12px `#444`, bottom right):** "amounts and times illustrative".

## Why "Encrypted" Doesn't Mean "Safe"

**Tags:** `common mistake` (red), `where it's used` (blue)

- **The mistake** — treating the padlock as the whole story and clicking through certificate warnings
- **What a warning means** — the identity check failed; you may be talking to the middle box right now
- **HTTPS everywhere** — every padlocked page you load runs this certificate check silently
- **API signing** — services sign requests with timestamps so a captured call can't be re-sent later
- **The pairing** — encryption stops eavesdropping, certificates stop impersonation, nonces stop replay

*Example (italic):* A user on hostile Wi-Fi clicks "proceed anyway" past a certificate warning — and hands the on-path attacker the exact session TLS was built to protect.

**Common mistake:** Believing one defense covers all three attacks. Each closes a different door — skipping the certificate check (or omitting nonces from an API) reopens its door even with perfect encryption.

### Visualization (canvas `c4`, 720×300)

Coverage matrix: three attacks as rows, three defenses as columns, checkmarks showing which defense stops which attack.

- **Title (bold 15px, `#1a5276`, top center):** "Each Defense Closes Exactly One Door".
- **Column headers (bold 12px `#1a5276`, y=80):** "encryption" at x=300, "certificates" at x=445, "nonce / timestamp" at x=590 (centered).
- **Row labels (12px `#444`, left-aligned at x=30):** "eavesdropping (read traffic)" at y=125, "impersonation (fake server)" at y=180, "replay (re-send message)" at y=235.
- **Cells:** centered at the column x and row y positions — green `#008300` bold 18px "✓" on the diagonal (encryption×eavesdropping, certificates×impersonation, nonce×replay); mute `#6b7280` 14px "—" in the six off-diagonal cells.
- **Grid:** 1px `#e5e9ef` horizontal rules midway between rows (y=152 and y=207) from x=30 to x=660; light `rgba(0,131,0,0.08)` fill behind each ✓ cell (90×34 rounded rect).
- **Annotation (bold 13px magenta `#d55181`, centered at y=278):** "HTTPS ships all three — which is why clicking past a warning removes a whole column".
- **Caption (12px `#444`, bottom right):** "simplified — defensive overview".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all positions, amounts, and counts are the hardcoded values above (no randomness); the $50 transfer, 5 requests, $250 total, nonce 8317, and the 0–120s timeline are invented and labeled illustrative; text numbers match chart numbers exactly.
- **Framing:** defensive/educational only — the page explains attacks to motivate the defenses (TLS certificates, nonces, timestamps, sequence numbers), never as a how-to.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
