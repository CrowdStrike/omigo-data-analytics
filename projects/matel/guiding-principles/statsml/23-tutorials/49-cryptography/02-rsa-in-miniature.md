# RSA in Miniature

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** RSA in Miniature

**Subtitle:** Encrypt a number by hand with two tiny primes — the whole RSA machine fits on a napkin when n = 33

## A Padlock Anyone Can Snap Shut

**Tags:** `core idea` (blue), `public key` (green), `tiny primes` (orange)

- **Two primes** — Alice picks p = 3 and q = 11 and multiplies them: n = 3 × 11 = 33
- **The secret clock** — she computes φ(n) = (3 − 1) · (11 − 1) = 2 · 10 = 20 from the primes
- **Public lock** — she picks e = 3 (it shares no factor with 20) and publishes the pair (n = 33, e = 3)
- **Private key** — she solves 3 · d ≡ 1 (mod 20), gets d = 7, and tells absolutely no one
- **Anyone encrypts** — Bob turns his message m = 4 into c = 4³ mod 33 = 31 using only the public pair

*Example (italic):* Bob sends 31 over an open channel; anyone in the world can lock a message with (33, 3), but only Alice's d = 7 unlocks it.

**Key point:** This is public-key encryption: the locking key (n, e) is published for everyone, and only the secret exponent d — computable only from the primes p and q — can unlock.

### Visualization (canvas `c1`, 720×300)

Flow diagram: message 4 locked with the public pair, ciphertext 31 crossing the wire, unlocked with the private d = 7; an eavesdropper sees everything public and still cannot read it.

- **Title (bold 15px, `#1a5276`, top center):** "Public Pair (33, 3) Locks It; Secret d = 7 Unlocks It".
- **Public-key banner:** rounded box (8px radius) centered at x=290–430, y=48, height 34, fill `rgba(26,82,118,0.12)`, 12px bold `#1a5276` text "public: n = 33, e = 3".
- **Main row (boxes 140×54, y=118, 8px radius, 12px `#2c3e50` two-line text):** blue box (fill `rgba(42,120,214,0.15)`) at x=30 "Bob / message m = 4"; 3px `#2a78d6` arrow to orange box (fill `rgba(217,89,38,0.12)`) at x=290 "on the wire / c = 31"; 3px `#008300` arrow to green box (fill `rgba(0,131,0,0.12)`) at x=550 "Alice / recovers m = 4".
- **Arrow labels (bold 12px, above each arrow):** blue "c = 4³ mod 33 = 31" over the first, green "m = 31⁷ mod 33 = 4" over the second.
- **Eavesdropper:** red-bordered box (fill `rgba(231,76,60,0.12)`) 260×44 at x=230, y=220, 12px text "Eve sees (33, 3) and 31 — she must factor n to get d"; dashed `#6b7280` (dash 4/3) arrow from the wire box down to it.
- **Caption (12px `#444`, bottom right):** "all arithmetic exact".

## Every Step by Hand: Keys, Encrypt, Decrypt

**Tags:** `worked example` (blue), `modular arithmetic` (green)

- **Find d** — try multiples: 3 · 7 = 21 = 20 + 1, so 3 · 7 ≡ 1 (mod 20) and d = 7
- **Encrypt** — c = 4³ mod 33: 4³ = 64, and 64 − 33 = 31, so c = 31
- **Decrypt, square once** — 31² = 961 = 29 · 33 + 4, so 31² ≡ 4 (mod 33)
- **Square again** — 31⁴ ≡ 4² = 16 (mod 33)
- **Combine** — 31⁷ = 31⁴ · 31² · 31 ≡ 16 · 4 · 31 = 1984 = 60 · 33 + 4 ≡ 4 (mod 33): the message is back

*Example (italic):* You never touch the 11-digit number 31⁷ = 27,512,614,111 — two squarings and one multiply, each reduced mod 33, do the whole decryption.

**Key point:** Decryption works because e · d = 21 = 20 + 1: raising to the 21st power runs one full lap of the size-20 clock and lands back on m — that lap size, φ(n) = 20, is exactly what the primes reveal.

### Visualization (canvas `c2`, 720×300)

Needle (lollipop) chart of the encryption map c = m³ mod 33 for m = 1..10, showing how cubing scrambles neighboring messages; the worked value 4 → 31 highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Cubing mod 33 Scrambles the Messages: m → m³ mod 33".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = m from 1 to 10, 12px `#444` tick labels under each needle; y = ciphertext 0 to 33, gridlines `#e5e9ef` at 11 and 22, 12px `#444` labels.
- **Needles:** vertical 3px blue `#2a78d6` stems from the baseline to c values, 6px-radius filled dots on top; hardcoded pairs m `[1,2,3,4,5,6,7,8,9,10]`, c `[1, 8, 27, 31, 26, 18, 13, 17, 3, 10]`.
- **Highlight:** the m=4 stem and dot in green `#008300`, bold 13px green label "4 → 31" beside its dot.
- **Value labels:** 12px `#444` c value above every dot.
- **Annotation (bold 12px violet `#4a3aa7`, upper left, y=45):** "neighbors 3, 4, 5 land at 27, 31, 26 — no visible order".
- **Caption (12px `#444`, bottom right):** "all values exact: c = m³ mod 33".

## Easy Forward, Hard Backward

**Tags:** `why it matters` (blue), `one-way trapdoor` (orange)

- **Forward is cheap** — cubing mod n is a handful of multiplications, even when n has 617 digits
- **Backward needs d** — undoing the cube requires d, and d comes from φ(n) = (p − 1)(q − 1)
- **φ needs the factors** — the only known road to φ(n) is factoring n back into p and q
- **Toy vs real** — 33 = 3 × 11 falls at a glance; a 2048-bit n (617 digits) resists every known algorithm
- **The trapdoor** — multiplying primes is a one-way street whose secret side door is the factorization

*Example (italic):* Eve holds (33, 3) and c = 31 — for n = 33 she factors instantly, but scale n to 2048 bits and the identical attack outlasts the age of the universe.

**Key point:** RSA's entire security is one gap: multiplying two primes is instant, recovering them from the product is infeasible at 2048 bits — easy forward, hard backward is the trapdoor.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: factoring effort as n grows from the toy 33 to a real 2048-bit modulus, bar lengths schematic, milestone facts exact.

- **Title (bold 15px, `#1a5276`, top center):** "Multiplying Is Instant; Factoring Hits a Wall".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440.
- **Rows (14px-tall bars at y = 78, 128, 178, 228, each with a left-aligned 12px `#444` label at x=20):**
  - "n = 33 (2 digits)": green `#008300` bar width 4, 11px label "instant by hand"
  - "n ≈ 50 digits": blue `#2a78d6` bar width 60, 11px label "seconds on a laptop"
  - "n = 232 digits (RSA-768)": orange `#d95926` bar width 220, 11px label "~2,000 CPU-years (2009)"
  - "n = 617 digits (2048-bit)": red `#e74c3c` bar width 440 with arrowhead cut off at the right edge, bold 12px red label "beyond all known computing"
- **Bar style:** fills solid for green/orange/red rows, blue row fill `rgba(42,120,214,0.30)`.
- **Annotation (bold 13px red `#e74c3c`, right side near y=262):** "the private key hides behind this wall".
- **Caption (12px `#444`, bottom right):** "bar lengths schematic; digit counts and RSA-768 effort exact".

## Textbook RSA Is a Toy — Real RSA Adds Armor

**Tags:** `common mistake` (red), `padding` (orange), `where it's used` (blue)

- **Deterministic** — textbook RSA maps m = 4 to 31 every single time; there is no randomness anywhere
- **Codebook attack** — with only 33 possible messages, Eve cubes all of them and reads the table backward
- **Padding fixes it** — real RSA (OAEP) mixes random bytes into m, so equal messages encrypt differently
- **Huge random primes** — real p and q are ~1024 bits each and freshly random, never from a textbook
- **Where RSA sits today** — mainly signatures and legacy key exchange; elliptic curves give equal strength with far smaller keys

*Example (italic):* Eve sees c = 31, computes m³ mod 33 for every m from 0 to 32 in seconds, finds 4³ ≡ 31, and recovers the message without factoring anything.

**Common mistake:** Shipping textbook RSA. The arithmetic above is the genuine core of the real algorithm, but without padding and huge random primes it is a puzzle, not a cipher.

### Visualization (canvas `c4`, 720×300)

Codebook-attack diagram: Eve rebuilds the full encryption table from the public key alone and looks the intercepted ciphertext up backward; the padded fix shown below.

- **Title (bold 15px, `#1a5276`, top center):** "No Randomness Means Eve Builds the Whole Codebook".
- **Codebook table (left):** rounded box 200×150 at x=40, y=55, fill `rgba(42,120,214,0.10)`, 12px bold `#1a5276` header "Eve computes m³ mod 33", then 12px `#2c3e50` rows at 22px spacing: "2 → 8", "3 → 27", "4 → 31" (bold red `#e74c3c`), "5 → 26", "6 → 18".
- **Intercept box:** red-bordered box (fill `rgba(231,76,60,0.12)`) 180×44 at x=400, y=80, 12px text "intercepted: c = 31"; 3px red `#e74c3c` arrow from it to the "4 → 31" row, bold 13px red label "match: m = 4" at the arrow midpoint.
- **Fix box (bottom):** green box (fill `rgba(0,131,0,0.12)`) 560×44 centered at y=222, 12px `#2c3e50` text "fix: OAEP padding — random bytes make the same m encrypt differently every time", bold 12px green `#008300` "✓" at its left edge.
- **Annotation (bold 12px orange `#d95926`, under the intercept box, y=150):** "no factoring needed — determinism alone leaks the message".
- **Caption (12px `#444`, bottom right):** "table values exact: c = m³ mod 33".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness). Every modular-arithmetic value is exact and must stay exact: p=3, q=11, n=33, φ(n)=20, e=3, d=7 (3·7=21≡1 mod 20); c = 4³ mod 33 = 31; 31² = 961 = 29·33 + 4 ≡ 4; 31⁴ ≡ 16; 31⁷ ≡ 16·4·31 = 1984 = 60·33 + 4 ≡ 4; 31⁷ = 27,512,614,111; the c2 map c = [1, 8, 27, 31, 26, 18, 13, 17, 3, 10] for m = 1..10; a 2048-bit n has 617 decimal digits; RSA-768 has 232 digits and cost ~2,000 CPU-years (2009). Only the c3 bar lengths are schematic and labeled so.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
