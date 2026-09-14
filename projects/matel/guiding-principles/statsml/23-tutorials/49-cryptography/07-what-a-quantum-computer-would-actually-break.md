# What a Quantum Computer Would Actually Break

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** What a Quantum Computer Would Actually Break

**Subtitle:** Not all encryption — the damage is specific, and the asymmetric half takes it

## Alice Opens One Connection, and It Has Two Halves

**Tags:** `core idea` (blue), `running example` (green), `two halves` (orange)

- **The connection** — Alice's browser opens an HTTPS-style session to Bob's server and sends a 2 GB upload
- **Half one** — an asymmetric key agreement runs first, so both sides end up holding the same fresh session key
- **Half two** — a symmetric cipher (AES) then encrypts the actual 2 GB using that agreed session key
- **Shor's algorithm** — factors integers and computes discrete logarithms in polynomial time, so half one is broken outright
- **Grover's algorithm** — searches an unstructured space of size N in about √N steps, so half two only loses half its bits
- **The asymmetry** — half two survives a parameter change; half one needs mathematics that does not exist in it today

*Example (italic):* On the same connection, a large quantum computer recovers the agreed session key from the handshake — it never has to attack the AES stream at all.

**Key point:** One connection uses both families, and a quantum computer treats them completely differently: the handshake is broken, the bulk encryption is merely weakened.

### Visualization (canvas `c1`, 720×300)

Timeline of one connection split into its asymmetric handshake and symmetric bulk-transfer halves, with the two quantum algorithms labelled on the correct halves.

- **Title (bold 15px, `#1a5276`, top center):** "One Connection, Two Halves — Two Very Different Verdicts".
- **Endpoints:** 12px `#2c3e50` labels "Alice (browser)" at x=20,y=52 and "Bob (server)" at x=20,y=200; a 1px `#e5e9ef` horizontal rail at y=150 from x=150 to x=690.
- **Half one box:** magenta `#d55181` 2px rounded rect (8px radius) x=150,y=95,w=230,h=110, fill `rgba(213,81,113,0.10)`; inside, bold 13px magenta "1. key agreement" at center x=265,y=120; 12px `#2c3e50` lines "asymmetric" (y=142) and "RSA / DH / elliptic curve" (y=160); 12px `#6b7280` "output: one shared session key" (y=186).
- **Half two box:** yellow `#c98500` 2px rounded rect x=410,y=95,w=280,h=110, fill `rgba(201,133,0,0.10)`; inside, bold 13px yellow "2. bulk encryption of 2 GB" at center x=550,y=120; 12px `#2c3e50` lines "symmetric" (y=142) and "AES with the agreed session key" (y=160); 12px `#6b7280` "output: the ciphertext stream" (y=186).
- **Arrow:** 3px `#6b7280` arrow from (382,150) to (406,150).
- **Verdict labels (bold 13px, above each box):** magenta "Shor: broken outright" centered at (265,80); yellow "Grover: 128-bit key → 64-bit strength" centered at (550,80).
- **Verdict footers (bold 12px, below each box):** magenta "needs new mathematics" centered at (265,228); green `#008300` "needs a longer key" centered at (550,228).
- **Caption (12px `#444`, bottom right):** "structure of a hybrid TLS-style session; verdicts per Shor / Grover".

## Doing the Grover Arithmetic on Alice's Session Key

**Tags:** `worked example` (blue), `rule of thumb` (green), `log scale` (orange)

- **The rule** — Grover turns a brute-force search of 2^n keys into about 2^(n/2) quantum steps: the exponent halves
- **AES-128** — 2^128 drops to 2^64 = 18,446,744,073,709,551,616 steps, so the 128-bit margin becomes a 64-bit one
- **The clock** — at an illustrative 10^9 steps/second that is 1.84 × 10^10 seconds ÷ 31,557,600 = about 585 years
- **AES-256** — 2^256 drops to 2^128 ≈ 3.40 × 10^38 steps, about 1.1 × 10^22 years — roughly 780 billion times the age of the universe
- **The fix** — doubling the symmetric key from 128 to 256 bits restores the original 128-bit margin: a config change
- **The honest caveat** — Grover parallelises badly (m machines buy only √m), and error correction inflates every step, so these are lower bounds on difficulty, not forecasts
- **Shor by contrast** — its cost is polynomial in key length, so doubling an RSA modulus adds a small polynomial cost, not an exponential wall

*Example (italic):* Alice's AES-256 stream still needs about 2^128 quantum steps to crack, while the RSA handshake that delivered its key has no exponential barrier left at all.

**Key point:** Grover is a square root, and a square root is defeated by doubling the exponent — Shor is a change of complexity class, and no key length defeats that.

### Visualization (canvas `c2`, 720×300)

Grouped horizontal bar chart of work factors on a log2 (exponent) axis: classical vs quantum for each primitive, with a feasibility threshold line.

- **Title (bold 15px, `#1a5276`, top center):** "Work Factor Before and After (log₂ scale — bar length = exponent)".
- **Axis:** bars start at x=200 and extend right; exponent 0→260 maps to 460px (1.769 px per unit); baseline 1px `#999` vertical at x=200 from y=46 to y=252; x ticks at exponents 0, 64, 128, 192, 256 with 12px `#444` labels "2^0", "2^64", "2^128", "2^192", "2^256" at y=272; 12px `#6b7280` "quantum steps required (exponent of 2)" centered at (450,292)… drawn as the caption line instead if crowded.
- **Row labels (12px `#444`, left-aligned at x=20):** "RSA-2048 handshake", "AES-128", "AES-256", "SHA-256 preimage".
- **Groups (y = 55, 105, 155, 205), each two 15px bars 5px apart:** upper bar = classical (blue `#2a78d6`), lower bar = quantum (magenta `#d55181` for the Shor row, yellow `#c98500` for the Grover rows).
  - RSA-2048: classical exponent 112 (label "≈2^112"); quantum bar drawn as a **dashed 2px magenta outline** to exponent 30 with bold 12px magenta label "polynomial — no exponential barrier (bar schematic)".
  - AES-128: classical 128 ("2^128"), quantum 64 ("2^64 = 1.8×10^19 → ≈585 yr").
  - AES-256: classical 256 ("2^256"), quantum 128 ("2^128 ≈ 1.1×10^22 yr").
  - SHA-256 preimage: classical 256 ("2^256"), quantum 128 ("2^128 — still infeasible").
- **Bar-end labels:** 12px, coloured to match the bar, drawn 6px past each bar end.
- **Threshold line:** 2px dashed `#d95926` vertical at exponent 80 from y=46 to y=252; bold 12px orange rotated-free label "2^80: no longer a comfortable margin" placed at (355,40) centered.
- **Caption (12px `#444`, bottom right):** "rate of 10^9 quantum steps/s is illustrative; RSA bar length schematic".

## What the Asymmetric Half Is Actually Wired Into

**Tags:** `where it's used` (blue), `migration` (green), `hybrid` (orange)

- **Certificates** — every certificate chain Alice's browser validates is a stack of asymmetric signatures
- **Firmware and code signing** — devices refuse unsigned updates, so the verifying key is burned into hardware
- **Protocol handshakes** — key agreement sits at the start of TLS, SSH, VPNs, and most secure messaging
- **Long-lived devices** — meters, vehicles, and industrial controllers ship with a signature scheme for a 15-year life
- **Harvest now, decrypt later** — recorded handshakes can be stored today and opened once the machine exists, so long-confidentiality traffic is already exposed
- **The replacements** — post-quantum families rest on problems believed hard for quantum machines: lattice-based and hash-based constructions are the main lines
- **Hybrid mode** — run a classical and a post-quantum key agreement together and combine both outputs, so the session holds if either survives

*Example (italic):* Swapping AES-128 for AES-256 is a configuration line; swapping the signature scheme in a shipped meter's boot ROM is a hardware refresh cycle.

**Key point:** The symmetric migration is a setting; the asymmetric migration touches everything that verifies anything — which is why lead time matters more than a date.

### Visualization (canvas `c3`, 720×300)

Two-column scope panel: deployed components that contain asymmetric cryptography vs the one that does not, sized by how hard each is to replace.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Broken Half Is Deployed (replacement difficulty)".
- **Column headers (bold 13px, y=52):** magenta `#d55181` "contains asymmetric crypto — needs new mathematics" centered at x=250; green `#008300` "symmetric only" centered at x=580.
- **Left column rows (x=40, w=420, h=30, 6px radius, 4px gaps starting y=68):** "TLS / SSH / VPN key agreement", "X.509 certificate chains", "code and firmware signing keys", "software update signatures", "embedded devices with a 15-year life". Fill opacity ramps with difficulty: `rgba(213,81,113,0.10)` → `0.14` → `0.18` → `0.22` → `0.28`; 2px magenta border; 12px `#2c3e50` label at x=52, vertically centred.
- **Difficulty ticks:** to the right of each left-column row (x=470), 12px `#6b7280` text "protocol", "PKI", "build system", "release pipeline", "hardware refresh".
- **Right column box (x=560,y=68,w=140,h=64, 6px radius):** fill `rgba(0,131,0,0.12)`, 2px green border, bold 12px green "AES bulk encryption" centered, plus 12px `#2c3e50` "128 → 256 bits" below it.
- **Right column note (12px `#6b7280`, wrapped at x=560, starting y=160, 16px line height):** "one config value", "no protocol change", "no new mathematics".
- **Annotation (bold 12px orange `#d95926`, centered at (250,290)):** "recorded traffic today can be decrypted later — lead time, not a date".
- **Caption (12px `#444`, bottom right):** "illustrative inventory".

## "Quantum Breaks All Encryption" — and the Opposite Error

**Tags:** `common mistake` (red), `reframing` (green)

- **Where the slogan fails** — it fails on the symmetric half, where the honest answer is "use a 256-bit key"
- **The better sentence** — quantum computing threatens key agreement and signatures, not bulk encryption
- **Hashes ride with symmetric** — Grover cuts SHA-256 preimage search to about 2^128, which is still out of reach
- **Collisions, modestly** — quantum collision search improves less than early claims suggested and is not treated as a practical break
- **The opposite error** — dismissing it entirely ignores that signing keys in shipped hardware and certificate roots cannot be swapped quickly
- **No date here** — credible estimates of when a capable machine arrives vary by decades, and this page deliberately forecasts none
- **New code is newer code** — post-quantum schemes are less battle-tested than RSA, which is exactly why hybrid deployment is the cautious path

*Example (italic):* Two wrong summaries of the same fact: "AES is finished" (it is not — double the key) and "nothing to do yet" (a meter shipping today outlives the debate).

**Common mistake:** Treating "encryption" as one thing. The correct claim is narrower and more urgent than the slogan: the part that agrees keys and proves identity is the part at risk.

### Visualization (canvas `c4`, 720×300)

Inventory table drawn on canvas: each primitive with its classical and post-quantum effective security level and a verdict, split into "needs new mathematics" and "needs a bigger parameter".

- **Title (bold 15px, `#1a5276`, top center):** "What Breaks, What Survives — Effective Security Level".
- **Column headers (bold 12px `#1a5276`, y=52):** "primitive" at x=30 (left), "classical" at x=330 (center), "post-quantum" at x=460 (center), "verdict" at x=560 (left).
- **Header rule:** 1px `#e5e9ef` line y=60, x=25→700.
- **Rows (h=34, first row top y=66, 12px `#2c3e50` primitive names, 12px centered level text, bold 12px verdict):**
  - "RSA-2048 / Diffie-Hellman / ECC" — "2^112" — "broken" — magenta `#d55181` "new mathematics"
  - "digital signatures (same families)" — "2^112" — "broken" — magenta "new mathematics"
  - "AES-128" — "2^128" — "2^64" — orange `#d95926` "margin too thin"
  - "AES-256" — "2^256" — "2^128" — green `#008300` "fine"
  - "SHA-256 preimage" — "2^256" — "2^128" — green "fine"
  - "SHA-256 collision" — "2^128" — "≈2^128" — green "no practical gain"
- **Row banding:** even rows filled `#f8f9fa` across x=25→700.
- **Group brace:** 3px magenta vertical line at x=20 spanning rows 1–2; 3px green vertical line at x=20 spanning rows 3–6.
- **Footer annotations (bold 12px, y=284 and y=268):** magenta at x=30 "wrong: 'all encryption breaks'"; green at x=380 "also wrong: 'nothing to do yet'".
- **Caption (12px `#444`, bottom right):** "levels are standard security-strength estimates".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), skeleton copied from `01-symmetric-vs-asymmetric-encryption.html`. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers: `roundRect`, `arrow`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data (all hardcoded, no randomness):**
  - Grover exponent rule n → n/2 is exact. 2^64 = 18,446,744,073,709,551,616 (exact). 2^64 / 10^9 = 1.8446744 × 10^10 s; ÷ 31,557,600 s/yr = 584.5 → stated as "about 585 years".
  - 2^128 ≈ 3.402823669 × 10^38; ÷ 10^9 = 3.4028 × 10^29 s; ÷ 31,557,600 = 1.078 × 10^22 yr → stated as "about 1.1 × 10^22 years"; ÷ 1.38 × 10^10 yr (age of universe) = 7.8 × 10^11 → "roughly 780 billion times".
  - The 10^9 quantum-steps/second rate is an invented illustrative assumption and is labelled as such in both text and chart caption.
  - RSA-2048 classical strength shown as 2^112 (standard security-strength estimate). The Shor bar length is schematic and labelled schematic, because Shor's cost is polynomial and has no 2^k form.
  - No qubit counts, no dates, no vendor or product names appear anywhere on the page.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
