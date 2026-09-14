# Homomorphic Encryption

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Homomorphic Encryption

**Subtitle:** Encryption where computing on the ciphertexts produces the encryption of the answer — the data is never decrypted while being worked on

## A Glovebox for Numbers

**Tags:** `core idea` (blue), `sealed computation` (green), `encrypted in use` (orange)

- **The box** — a clinic locks its patient numbers in a sealed box and ships the box to a cloud service
- **The gloves** — the cloud works on the contents through built-in gloves, without ever opening the box
- **The key** — only the clinic's key opens the box, and inside sits the finished answer, already computed
- **The third gap** — encrypted at rest and in transit, data is normally decrypted to be computed on
- **The fix** — homomorphic encryption keeps the numbers encrypted even while they are worked on

*Example (italic):* The clinic sends sealed cholesterol readings; the cloud averages them through the gloves and returns a sealed average only the clinic can open.

**Key point:** Homomorphic encryption lets someone compute on ciphertexts so the result decrypts to the computed answer — the party doing the work never sees the data.

### Visualization (canvas `c1`, 720×300)

Three-stage pipeline — clinic encrypts, cloud computes on the sealed box (keyhole crossed out), clinic decrypts — with an at-rest / in-transit / in-use coverage strip underneath showing which stage HE adds.

- **Title (bold 15px, `#1a5276`, top center):** "The Sealed Glovebox: Encrypt, Compute Sealed, Decrypt".
- **Stage boxes (180×62 at y=54, 8px radius, bold 12px `#2c3e50` first line at box-y+24, plain 12px second line at box-y+44, both centered):** at x=40 fill `rgba(42,120,214,0.15)` stroke `#2a78d6`, "clinic encrypts" / "2, 7 → sealed box"; at x=270 fill `rgba(74,58,167,0.12)` stroke `#4a3aa7`, "cloud computes" / "box never opened"; at x=500 fill `rgba(0,131,0,0.12)` stroke `#008300`, "clinic decrypts" / "opens answer: 14".
- **Stage arrows (3px, at y=85):** blue `#2a78d6` from x=222 to arrowhead at x=266; green `#008300` from x=452 to arrowhead at x=496.
- **Crossed-out keyhole (top-right of the cloud box):** 2px `#4a3aa7` circle radius 5 at (432, 66) with a stem line from (432, 70) to (432, 78); a 2.5px red `#e74c3c` slash from (423, 58) to (441, 82).
- **Annotation (bold 13px violet `#4a3aa7`, centered at x=360, y=148):** "sealed even while in use — the cloud never opens the box".
- **Coverage strip (three 196×32 rounded segments at y=180, 5px radius, label centered at y=200):** at x=60 and x=262 fill `rgba(0,131,0,0.12)` stroke `#008300`, 12px green labels "at rest ✓" and "in transit ✓"; at x=464 fill `rgba(74,58,167,0.15)` stroke `#4a3aa7`, bold 12px violet label "in use — HE adds this".
- **Strip sub-labels (y=234, centered):** 12px green `#008300` "ordinary encryption already covers these" at x=259; bold 12px violet `#4a3aa7` "the third gap, closed" at x=562.
- **Caption (12px `#444`, bottom right):** "clinic example — numbers match the worked example below".

## Multiplying 2 by 7 Without Seeing Either

**Tags:** `worked example` (blue), `textbook RSA` (green), `mod 33` (orange)

- **The scheme** — textbook RSA with n = 33 and e = 3 encrypts by cubing: E(m) = m³ mod 33
- **Encrypt 2 and 7** — E(2) = 2³ mod 33 = 8, and E(7) = 7³ mod 33 = 343 mod 33 = 13
- **Cloud multiplies** — it multiplies the two ciphertexts: 8 × 13 = 104, and 104 mod 33 = 5
- **Check directly** — encrypt the true product: E(2 × 7) = E(14) = 2744 mod 33 = 5 — identical
- **What the cloud saw** — only 8, 13, 104, and 5: never 2, never 7, never the answer 14
- **Why it works** — (a·b)³ = a³·b³, so multiplying ciphertexts multiplies the hidden plaintexts

*Example (italic):* Every number is hand-checkable: 343 = 10·33 + 13, 104 = 3·33 + 5, 2744 = 83·33 + 5.

**Key point:** Teaching-sized example — real keys are enormous and real schemes add randomness — but the homomorphic property is genuine: the ciphertext product decrypts to the plaintext product.

### Visualization (canvas `c2`, 720×300)

Two-path flow diagram: plaintexts 2 and 7 multiply to 14 and encrypt to 5 (top), ciphertexts 8 and 13 multiply to 104 ≡ 5 mod 33 (bottom), the matching 5s highlighted at right.

- **Title (bold 15px, `#1a5276`, top center):** "Two Paths to 5: Multiply then Encrypt vs Multiply Ciphertexts".
- **Lane labels (11px `#6b7280`, left-aligned at x=30):** "multiply first, then encrypt" at y=55; "encrypt first, then multiply" at y=285. Lane centerlines: top path y=95, bottom path y=225.
- **Nodes:** circles radius 18, white fill, 2.5px stroke, bold 13px stroke-colored label centered. Top path: "2" at (140, 95) and "7" at (215, 95), both blue `#2a78d6`; result "5" at (585, 95), green `#008300`. Bottom path: "8" at (140, 225) and "13" at (215, 225), both violet `#4a3aa7`; result "5" at (585, 225), green.
- **Encrypt arrows (drawn behind the nodes):** 2px dashed (dash 5/4) violet `#4a3aa7` vertical arrows from (140, 115) and (215, 115) down to arrowheads at y=205; bold 12px violet two-line label centered at x=178: "encrypt:" (y=152) / "m³ mod 33" (y=168).
- **Top operation box (165×48 at x=285, y=71, 8px radius):** fill `rgba(42,120,214,0.12)` stroke `#2a78d6`, bold 12px `#2c3e50` centered text "2 × 7 = 14"; 3px blue arrow from x=237 to x=281 (into the box), along y=95; then a 3px dashed (dash 5/4) violet `#4a3aa7` encrypt arrow from x=450 to x=559 (to the result circle) along y=95, with a bold 12px violet two-line label centered at x=502: "encrypt" (y=69) / "E(14) = 2744 mod 33" (y=83).
- **Bottom operation box (165×62 at x=285, y=193, 8px radius):** fill `rgba(74,58,167,0.10)` stroke `#4a3aa7`, two centered `#2c3e50` lines: bold 12px "8 × 13 = 104" (y=217), plain 12px "104 mod 33 = 5" (y=239); 3px violet arrows from x=237 to x=281 and x=450 to x=559, along y=225.
- **Match highlight:** 2px green `#008300` halo circles radius 24 around both result "5" nodes; 2px dashed (dash 5/4) green vertical line joining them from (585, 121) to (585, 199); bold 13px green label "identical" left-aligned at (620, 165).
- **Annotation (bold 12px orange `#d95926`, centered at x=367, y=280):** "the cloud saw only 8, 13, 104, 5 — never 2, 7, or 14".
- **Caption (12px `#444`, bottom right):** "all arithmetic exact — hand-checkable".

## One Operation or Any Operation

**Tags:** `where it's used` (blue), `partial vs full` (green), `cost` (orange)

- **Partially homomorphic** — supports one operation only: RSA multiplies, Paillier adds
- **Additive at work** — additive schemes tally encrypted votes and sum encrypted salaries
- **Fully homomorphic** — FHE (Gentry, 2009) runs any computation directly on ciphertexts
- **Bootstrapping** — Gentry's trick: the scheme refreshes its own noise so computation can go on
- **The price** — FHE runs orders of magnitude slower than the same computation on plain data
- **Today** — deployed systems mostly use the partial schemes; FHE fills high-stakes niches

*Example (italic):* A hospital consortium runs encrypted medical analytics, and a bank scores a model on sealed inputs — FHE makes both possible.

**Key point:** Partial schemes give one operation cheaply; FHE gives any operation at a heavy slowdown — which is why most systems in production today are partial.

### Visualization (canvas `c3`, 720×300)

Capability-vs-cost bar chart across the scheme spectrum: plain computation, partially homomorphic, fully homomorphic — allowed operations labeled under each bar, bar heights the relative slowdown on a log scale.

- **Title (bold 15px, `#1a5276`, top center):** "Capability vs Cost: Plain, Partially Homomorphic, FHE".
- **Axis:** horizontal 2px `#999` baseline at y=235 from x=70 to x=670.
- **Bars (width 110, centered at x = 170, 380, 590):** heights `[14, 100, 185]` px above the baseline (log-scale schematic for 1×, ~1,000×, ~1,000,000×); fills `rgba(0,131,0,0.30)` / `rgba(42,120,214,0.30)` / `rgba(74,58,167,0.30)` with 2px borders `#008300` / `#2a78d6` / `#4a3aa7`.
- **Slowdown labels (bold 13px, border-colored, centered 8px above each bar top):** "1×", "~1,000× slower", "~1,000,000× slower".
- **Name labels (bold 12px `#1a5276`, centered at y=255):** "plain computation", "partially homomorphic", "fully homomorphic (FHE)".
- **Operation labels (12px `#444`, centered at y=273):** "any op — data exposed", "one op (× or +) — sealed", "any op — sealed".
- **Annotation (bold 12px orange `#d95926`, centered at x=380, y=72):** "the price of 'any operation'", with a 2px dashed (dash 4/3) orange pointer line from (455, 68) to an arrowhead at (532, 54) aimed at the FHE bar.
- **Scale note (11px `#6b7280`, left-aligned at x=70, y=55):** "bar heights on a log scale".
- **Caption (12px `#444`, bottom right):** "slowdowns illustrative — real figures vary by scheme and workload".

## What the Glovebox Does Not Hide

**Tags:** `common mistake` (red), `threat model` (orange)

- **The scope** — HE hides data from the party computing on it, and nothing more than that
- **The key holder** — whoever holds the private key still sees the decrypted answer in full
- **Still visible** — the cloud learns which program it ran and roughly how big the data was
- **Not faster** — "homomorphic" does not mean faster or free; the gloves slow every move down
- **The mental model** — a very safe but very slow glovebox, chosen when exposure is unacceptable

*Example (italic):* If the clinic mails the key to a partner, the partner reads the answer — the glovebox only ever blinded the cloud.

**Common mistake:** Treating HE as total secrecy. It blinds only the computing party — key holders see answers, and the cloud still sees the program and the data's size.

### Visualization (canvas `c4`, 720×300)

Who-sees-what matrix: rows of secrets, check/cross cells for clinic, cloud, and key holder — the cloud column highlighted to show exactly what HE does and does not hide.

- **Title (bold 15px, `#1a5276`, top center):** "Who Sees What: HE Blinds Only the Computing Party".
- **Column band:** light violet rectangle `rgba(74,58,167,0.08)` behind the cloud column, x=432 to x=508, y=48 to y=244.
- **Column headers (centered at x = 330, 470, 610):** bold 13px `#1a5276` "clinic", "cloud", "key holder" at y=66; 11px `#6b7280` "(data owner)", "(computes)", "(decrypts)" at y=82.
- **Rows (baseline text y = 110, 150, 190, 230; a 1px `#e5e9ef` gridline 12px below each):** left-aligned 12px `#444` labels at x=30: "raw numbers (2, 7)", "the program that ran", "the data's rough size", "the final answer (14)".
- **Cells (bold 16px, centered on the column x positions, at row y + 5):** green `#008300` ✓ / red `#e74c3c` ✗, row by row (clinic, cloud, key holder): raw numbers ✓ ✗ ✗; the program ✓ ✓ ✗; rough size ✓ ✓ ✗; final answer ✓ ✗ ✓.
- **Annotation (bold 12px orange `#d95926`, centered at x=360, y=268):** "the cloud still learns the program and the size — HE is not total secrecy".
- **Caption (12px `#444`, bottom right):** "in the clinic example the clinic also holds the key".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; shared `roundBox(ctx,x,y,w,h,r)` and `arrowHead(ctx,x,y,angle,color)` helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness). The modular arithmetic is exact and must stay exact everywhere it appears (text, `c1`, `c2`): n=33, e=3; E(2) = 2³ mod 33 = 8; E(7) = 7³ mod 33 = 343 mod 33 = 13 (343 = 10·33 + 13); ciphertext product 8 × 13 = 104, 104 mod 33 = 5 (104 = 3·33 + 5); direct check E(14) = 2744 mod 33 = 5 (2744 = 83·33 + 5). Only the `c3` bar heights `[14, 100, 185]` and slowdown magnitudes (1×, ~1,000×, ~1,000,000×) are illustrative and labeled so. The `c4` check/cross matrix is the four rows given above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
