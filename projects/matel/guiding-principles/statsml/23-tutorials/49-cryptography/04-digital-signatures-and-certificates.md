# Digital Signatures & Certificates

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Digital Signatures & Certificates

**Subtitle:** How your browser knows it's your bank — sign with the private key, verify with the public key, then follow a chain of signatures up to a root your browser already trusts

## Public-Key Crypto Run in Reverse

**Tags:** `core idea` (blue), `authenticity` (green), `integrity` (orange)

- **The claim** — Maya's browser receives a page that says it came from examplebank.com; anyone can type that
- **The reversal** — encryption locks with the public key; a signature locks with the PRIVATE key instead
- **The proof** — only the private key can produce the signature, yet any copy of the public key verifies it
- **Two guarantees** — a valid signature proves who signed and that not one byte changed since signing
- **The trick** — you sign a short hash of the document, never the multi-megabyte document itself

*Example (italic):* The bank signs the 32-byte hash of a 2 MB statement once; Maya's laptop verifies the signature in under a millisecond.

**Key point:** A digital signature is public-key cryptography run in reverse — sign with the private key, verify with the public one — proving origin and integrity in a single check. (Literally true for RSA; other schemes meet the same sign-private/verify-public contract differently.)

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram contrasting the two directions of the same key pair: encryption (public locks, private opens) vs signature (private signs, anyone verifies).

- **Title (bold 15px, `#1a5276`, top center):** "Same Key Pair, Two Directions: Secrecy vs Proof".
- **Row 1 (y=95), label 12px `#444` at x=20:** "encryption"; blue `#2a78d6` rounded box at x=140 labeled "message" (12px), 3px arrow labeled 12px `#6b7280` "lock with PUBLIC key" to a green `#008300` box at x=470 labeled "only the PRIVATE key can read".
- **Row 2 (y=205), label:** "signature"; blue box at x=140 labeled "hash of message", 3px arrow labeled "sign with PRIVATE key" to a green box at x=470 labeled "ANYONE with the PUBLIC key verifies".
- **Box style:** 170–200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=268):** "secrecy flows toward the key holder; proof flows away from them".

## A Toy Signature You Can Check by Hand

**Tags:** `worked example` (blue), `sign the hash` (green)

- **The toy key** — public key (n=55, e=3), private exponent d=27; real keys use 600-digit numbers
- **The hash** — the order "PAY 100 TO ANA" hashes to 13 under a toy hash (illustrative)
- **The signing** — signature = 13^27 mod 55 = 7; only whoever knows d=27 can compute this
- **The check** — the verifier computes 7^3 mod 55 = 13 and compares it to the hash of what arrived
- **The tamper** — edit the order to "PAY 900 TO ANA" and it hashes to 20; 13 ≠ 20, verification fails

*Example (italic):* Ana verifies with only the public pair (55, 3): 7 cubed mod 55 gives back 13, exactly the hash of the untampered order — so she pays.

**Key point:** The signature is just the hash transformed by the private key; anyone can undo it with the public key and compare — a single edited character breaks the match.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram of the toy RSA check: the untampered order verifying (top) and the tampered order failing against the same signature (bottom).

- **Title (bold 15px, `#1a5276`, top center):** "Toy RSA: n=55, e=3, d=27 — One Edit Breaks the Match".
- **Row 1 (y=95), label 12px `#444` at x=15:** "as sent"; four rounded boxes left to right at x = 90, 250, 400, 550: blue `#2a78d6` "PAY 100 TO ANA", blue "hash = 13", green `#008300` "sign: 13^27 mod 55 = 7", green "verify: 7^3 mod 55 = 13 ✓"; 3px arrows between boxes.
- **Row 2 (y=205), label:** "tampered"; boxes at the same x positions: red `#e74c3c` "PAY 900 TO ANA", red "hash = 20", grey `#6b7280` "same signature: 7", red "7^3 mod 55 = 13 ≠ 20 ✗" with bold 12px red "reject".
- **Box style:** 130–150px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(107,114,128,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, near x=550, y=140):** "public key alone verifies".
- **Caption (12px `#444`, bottom right):** "toy RSA and toy hash values illustrative — real keys are 2048-bit".

## The Chain of Trust Behind the Padlock

**Tags:** `where it's used` (blue), `certificates` (green), `HTTPS` (orange)

- **The gap** — a public key is just numbers; nothing in the math says WHO the key belongs to
- **The certificate** — a certificate authority (CA) signs a statement binding "this public key ↔ examplebank.com"
- **The roots** — Maya's browser ships with roughly 100 root CAs it trusts unconditionally
- **The chain** — the site's leaf certificate is signed by an intermediate, whose certificate a root signed
- **Every visit** — the browser re-verifies every signature in the chain on every HTTPS connection

*Example (italic):* examplebank.com's leaf certificate is signed by "TrustCo Intermediate CA", which is signed by "TrustCo Root" — one of the roots already on Maya's laptop.

**Key point:** Trust never comes from the key itself — it flows down a chain of signatures, from a root CA that shipped with the browser to the certificate the site presents.

### Visualization (canvas `c3`, 720×300)

Left-to-right chain diagram: browser trust store, root, intermediate, and leaf certificate, with "signs" arrows and per-link verification checks.

- **Title (bold 15px, `#1a5276`, top center):** "Leaf ← Intermediate ← Root: the Chain Verified on Every Connection".
- **Trust store bar:** rounded rectangle at x=40, y=60, width 640, height 34, fill `rgba(26,82,118,0.10)`, 12px `#1a5276` text "browser trust store: ~100 root CAs ship pre-installed".
- **Chain row (boxes at y=150, 44px tall, 8px radius):** violet `#4a3aa7` box at x=50 width 180 "TrustCo Root (self-signed)"; blue `#2a78d6` box at x=280 width 180 "TrustCo Intermediate CA"; green `#008300` box at x=510 width 180 "examplebank.com (leaf)".
- **Arrows:** 3px arrows pointing right between boxes, each labeled 12px `#6b7280` "signs"; dashed 2px `#1a5276` connector (dash 4/3) from the trust store bar down to the root box labeled 11px "already trusted".
- **Check row (y=230):** bold 12px green `#008300` "✓ root found in store" under x≈90, "✓ intermediate signature valid" under x≈300, "✓ leaf signature valid" under x≈530.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "break any one link and the padlock never appears".

## What the Padlock Does — and Does Not — Promise

**Tags:** `common mistake` (red), `revocation & expiry` (orange)

- **The promise** — the padlock means you reached whoever holds the private key for this exact domain
- **Not honesty** — a phishing site at examp1ebank.com can get a perfectly valid certificate in minutes
- **Expiry** — certificates expire (after a few months to a year); an expired one fails verification outright
- **Revocation** — a stolen private key must be revoked, but revocation checking is the messiest part in practice
- **The warning** — a certificate warning means chain verification FAILED; clicking through discards every guarantee

*Example (italic):* On hotel Wi-Fi Maya gets a full-screen certificate warning — the chain can't vouch for the key being presented, so she closes the tab instead of clicking "proceed".

**Common mistake:** Reading the padlock as "this site is safe". It only asserts that the connection goes to the domain's key holder — the domain itself can still be a lookalike run by a scammer.

### Visualization (canvas `c4`, 720×300)

Two-panel checklist: what a valid padlock asserts (green checks, left) vs what it does not assert (red crosses, right).

- **Title (bold 15px, `#1a5276`, top center):** "The Padlock: What It Asserts vs What People Assume".
- **Left panel:** rounded rectangle at x=40, y=55, width 310, height 185, fill `rgba(0,131,0,0.06)`, border 2px `#008300`; header bold 13px `#008300` "the padlock ASSERTS" at y=78.
- **Left items (12px `#2c3e50`, green ✓ prefix, at y = 110, 148, 186):** "you reached this domain's key holder"; "traffic is encrypted in transit"; "content wasn't altered on the way".
- **Right panel:** rounded rectangle at x=380, y=55, width 310, height 185, fill `rgba(231,76,60,0.06)`, border 2px `#e74c3c`; header bold 13px `#e74c3c` "it does NOT assert" at y=78.
- **Right items (12px `#2c3e50`, red ✗ prefix, at y = 110, 148, 186):** "the site is honest or safe to pay"; "the domain is the one you meant (examp1ebank)"; "revocation was checked reliably".
- **Annotation (bold 13px red `#e74c3c`, centered near y=272):** "a certificate warning means the chain FAILED — don't click through".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the toy RSA arithmetic is exact and hand-checkable (13^27 mod 55 = 7, 7^3 mod 55 = 13, with n=55, e=3, d=27 from p=5, q=11), while the toy hash values (13, 20) and the "~100 root CAs" store size are illustrative and labeled as such in captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
