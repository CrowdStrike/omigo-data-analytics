# Symmetric vs Asymmetric Encryption

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Symmetric vs Asymmetric Encryption

**Subtitle:** One shared key that both locks and unlocks, or a key pair where anyone can lock but only you can unlock — one key vs two

## The Locker Key and the Mailbox

**Tags:** `core idea` (blue), `two families` (green), `analogy` (orange)

- **The locker** — a gym locker has one key: whoever holds it can both lock the door and open it
- **Symmetric** — one shared secret key encrypts and decrypts; AES is the standard everyone uses
- **The mailbox** — anyone on the street can drop a letter through the slot, but only the owner's key opens the box
- **Asymmetric** — a mathematically linked pair: the public key locks, only the private key unlocks
- **The publish trick** — you can hand the public key to strangers openly; it only lets them lock, never unlock

*Example (italic):* Two coworkers share one locker key and swap files with AES; a stranger who has never met you drops a message into your "mailbox" using your published public key.

**Key point:** Symmetric = one key shared by both sides; asymmetric = a public/private pair where locking and unlocking are different keys — that single difference drives everything else.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: symmetric on top (same key on both ends), asymmetric below (public key locks, private key unlocks).

- **Title (bold 15px, `#1a5276`, top center):** "One Key vs Two: the Locker and the Mailbox".
- **Row 1 (y=100), label 12px `#444` at x=20:** "symmetric (AES)"; blue `#2a78d6` rounded box at x=150 labeled "sender — key K" (12px), 3px blue arrow labeled "ciphertext" (11px `#6b7280`) to a blue box at x=460 labeled "receiver — key K"; bold 12px blue "same key K on both ends" centered at y=60.
- **Row 2 (y=215), label:** "asymmetric (RSA)"; green `#008300` rounded box at x=150 labeled "anyone — public key" (12px), 3px green arrow labeled "ciphertext" to a violet `#4a3aa7` box at x=460 labeled "owner — private key"; bold 12px violet "only the private key opens it" centered at y=270.
- **Box style:** 170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(74,58,167,0.12)`, 12px `#2c3e50` text.
- **Key icons:** a small 10px key glyph (circle + shaft drawn with 2px strokes) beside each box label, blue on row 1 boxes, green/violet on row 2 boxes.

## Why Strangers Can't Just Share a Secret

**Tags:** `worked example` (blue), `key distribution` (green)

- **The problem** — a symmetric key must reach the other side without an eavesdropper seeing it
- **The chicken-and-egg** — to send the key safely you'd need... an already-shared secret key
- **The count** — every pair of users needs its own key: n users need n(n-1)/2 shared secrets
- **Hand-check** — 10 users need 45 pairwise keys; 100 need 4,950; 1,000 need 499,500
- **The fix** — with key pairs, 1,000 users publish 1,000 public keys and the problem disappears

*Example (italic):* A 1,000-person company doing pairwise symmetric crypto must somehow distribute 499,500 secret keys; with public keys it posts 1,000 keys on a directory page.

**Key point:** Asymmetric encryption exists to solve key distribution — publishing a public key openly replaces half a million secret handshakes with one directory lookup.

### Visualization (canvas `c2`, 720×300)

Line chart of keys needed vs number of users: pairwise symmetric keys (exploding) vs published key pairs (linear), log-feel y placement via hardcoded pixel points.

- **Title (bold 15px, `#1a5276`, top center):** "Keys Needed for n Users: n(n-1)/2 Secrets vs n Public Keys".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = users at ticks `[2, 10, 100, 1000]` spaced evenly (12px `#444` labels); y unlabeled "keys needed (log scale)" 12px `#6b7280` rotated left; gridlines `#e5e9ef` at y=200/155/110/65.
- **Symmetric line:** red `#e74c3c` 3px line through hardcoded pixel points x=`[70, 263, 457, 650]`, y=`[243, 199, 145, 92]` for key counts `[1, 45, 4950, 499500]`; 11px red value labels "1", "45", "4,950", "499,500" above each point.
- **Asymmetric line:** green `#008300` 3px line through the same x positions, y=`[240, 222, 196, 170]` for key counts `[2, 10, 100, 1000]`; 11px green labels "2", "10", "100", "1,000" below each point.
- **Annotation (bold 13px red `#e74c3c`, near x=430, y=70):** "1,000 users → 499,500 secrets to smuggle".
- **Annotation (bold 12px green `#008300`, near x=470, y=205):** "or 1,000 keys published openly".
- **Caption (12px `#444`, bottom right):** "key counts exact, log placement schematic".

## The Hybrid Trick Behind Every HTTPS Connection

**Tags:** `worked example` (blue), `where it's used` (green), `TLS` (orange)

- **The speeds** — AES with hardware support moves about 2 GB/s; RSA is roughly 1,000× slower
- **The size limit** — one RSA-2048 operation encrypts only ~190 bytes, not a file
- **The timing** — a 2 GB backup: AES alone ≈ 1 second; pushing it through RSA-style crypto ≈ 1,000 seconds (~17 minutes)
- **The hybrid** — use RSA once to deliver a fresh 32-byte AES session key (~1 ms), then AES for the 2 GB
- **The everyday case** — TLS/HTTPS does exactly this on every connection: asymmetric handshake, symmetric session

*Example (italic):* Sending a 2 GB backup: hybrid costs ~1 ms of RSA on the 32-byte key plus ~1 second of AES on the data — versus ~17 minutes of pure asymmetric crypto.

**Key point:** Neither family wins alone — asymmetric solves the introduction, symmetric does the heavy lifting; hybrid encryption is why the "slow" family costs you nothing in practice.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time to protect a 2 GB file under three schemes, log-feel via hardcoded pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "Encrypting a 2 GB File: Symmetric, Asymmetric, Hybrid".
- **Axis:** vertical 2px `#999` baseline at x=240, bars extend right, max width 430; left-aligned 12px `#444` scheme labels at x=20.
- **Rows (bar centers at y = 85, 150, 215), 26px tall bars:**
  - "AES only (needs shared key)": green `#008300` bar width 150, 12px label "≈ 1 s" at bar end
  - "RSA-style only (~1000× slower)": red `#e74c3c` bar width 430, 12px label "≈ 1,000 s (~17 min)" inside the bar in white
  - "Hybrid: RSA on 32-byte key + AES on data": blue `#2a78d6` bar width 152, 12px label "≈ 1.001 s" at bar end
- **Annotation (bold 13px blue `#2a78d6`, near x=420, y=245):** "hybrid ≈ AES speed with public-key convenience — this is TLS".
- **Caption (12px `#444`, bottom right):** "timings illustrative (2 GB/s AES, 1000× ratio); bar widths schematic".

## Key Sizes Lie Across Families — and Encryption Isn't Everything

**Tags:** `common mistake` (red), `key sizes` (orange)

- **The trap** — "2048 bits must be stronger than 256 bits" compares numbers from two different games
- **Why** — symmetric keys are attacked by brute force; RSA keys by factoring, which has far better shortcuts
- **The table** — NIST rates RSA-2048 ≈ a 112-bit symmetric key; matching AES-128 takes RSA-3072
- **The extreme** — matching AES-256 would take RSA-15360, which is why RSA stops around 4096 in practice
- **The other trap** — encryption alone hides content but does not prove integrity or who sent it; MACs and signatures are separate mechanisms

*Example (italic):* An attacker can flip bits in an encrypted message without the key — the receiver decrypts garbage or, worse, an altered amount — which is why AES is paired with a MAC (as in AES-GCM).

**Common mistake:** Reading key length across families as one scale, and assuming "encrypted" means "tamper-proof and authenticated" — confidentiality, integrity, and authentication are three separate guarantees.

### Visualization (canvas `c4`, 720×300)

Paired horizontal bar chart: symmetric key bits vs the RSA modulus bits NIST rates as equivalent strength.

- **Title (bold 15px, `#1a5276`, top center):** "Equal Strength, Very Different Bit Counts (NIST equivalences)".
- **Axis:** vertical 2px `#999` baseline at x=210, bars extend right, scale hardcoded so 15,360 bits = width 430; left-aligned 12px `#444` strength labels at x=20.
- **Rows (three groups at y = 70, 140, 210; each group = two 16px bars 6px apart):**
  - "112-bit security": green `#008300` bar width 4 labeled "sym 112", blue `#2a78d6` bar width 57 labeled "RSA 2048"
  - "128-bit security (AES-128)": green bar width 4 labeled "sym 128", blue bar width 86 labeled "RSA 3072"
  - "256-bit security (AES-256)": green bar width 8 labeled "sym 256", blue bar width 430 labeled "RSA 15360"
- **Bar labels:** 11px, green `#008300` / blue `#2a78d6`, drawn just past each bar end.
- **Annotation (bold 13px orange `#d95926`, near x=380, y=110):** "same strength, ~18–60× the bits — never compare raw lengths".
- **Annotation (bold 12px red `#e74c3c`, near x=380, y=265):** "and none of these bits give integrity or authentication".
- **Caption (12px `#444`, bottom right):** "bit equivalences per NIST SP 800-57; bar widths to scale".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); key counts (1 / 45 / 4,950 / 499,500 from n(n-1)/2) and NIST bit equivalences (112→2048, 128→3072, 256→15360) are exact; throughput and timing numbers (2 GB/s AES, 1000× ratio, 1 s / 1,000 s / 1.001 s) are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
