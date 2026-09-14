# When Yesterday's Cipher Breaks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** When Yesterday's Cipher Breaks

**Subtitle:** "Secure" has an expiry date — and it expires without any new machine being invented: cheaper compute and better mathematics retire ciphers on their own

## The Fingerprint That Stopped Proving Anything

**Tags:** `core idea` (blue), `collisions` (orange), `integrity` (green)

- **The system** — a document archive stores an MD5 fingerprint of every file so any later edit is detectable
- **A responsible choice** — when it was built, MD5 was the mainstream fingerprint everyone used
- **What changed** — an attacker can now construct two different documents sharing one MD5 fingerprint
- **The check today** — the archive compares fingerprints, they match, and it reports "unchanged" — wrongly
- **Collision break** — find any two inputs with one fingerprint; this is what kills signatures and integrity checks
- **Preimage break** — recover an input from its fingerprint; this is what protects a stored hashed value
- **MD5's status** — collisions are trivial, so every use that relies on uniqueness of the fingerprint is dead

*Example (italic):* Bob's archive holds an approved invoice; Alice supplies a rewritten invoice built to share the same MD5 fingerprint, and the integrity check waves it through.

**Key point:** A fingerprint only proves "unchanged" while nobody can build a second document with the same fingerprint — MD5 lost that property, so the check still runs and no longer means anything.

### Visualization (canvas `c1`, 720×300)

Flow diagram: two different documents feed one MD5 function and emerge with a single identical fingerprint, which the verifier accepts.

- **Title (bold 16px, `#1a5276`, top center):** "Two Different Documents, One MD5 Fingerprint".
- **Document boxes (left, x=25, 175×54, 8px radius, centered 12px `#2c3e50` text over two lines):** at y=62 fill `rgba(0,131,0,0.12)` border 2px `#008300`, text "Doc A" / "the approved invoice"; at y=186 fill `rgba(217,89,38,0.14)` border 2px `#d95926`, text "Doc B" / "Alice's rewritten invoice".
- **MD5 box:** grey rounded box x=255, y=115, 150×62, fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border, 13px text "MD5" bold on the first line and 11px "fingerprint function" on the second.
- **Arrows in:** 3px lines from each document box right edge (x=200) to the MD5 box left edge (x=251), green `#008300` from Doc A, orange `#d95926` from Doc B, each with a 10px filled arrowhead.
- **Fingerprint box:** x=470, y=112, 205×68, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px "same fingerprint" and 11px `#6b7280` "128 bits — digest not shown"; 3px `#2a78d6` arrow from the MD5 box into it.
- **Verifier note (12px `#6b7280`, centered at x=572, y=205):** "verifier reports: unchanged".
- **Annotation (bold 13px `#d95926`, centered near x=360, y=272):** "the check runs, passes, and proves nothing".
- **Caption (12px `#444`, bottom right):** "schematic; no digests shown".

## Cheaper Compute, Better Mathematics — Same Direction

**Tags:** `core idea` (blue), `secure has an expiry` (orange), `one-way ratchet` (green)

- **Force one: price** — the key space never grew, but the cost of searching all of it keeps falling
- **DES is the case** — a 56-bit key was impractical to search in the 1970s and is searchable in days now
- **Force two: mathematics** — published cryptanalysis finds structure that beats brute force outright
- **MD5 and SHA-1** — both fell to collision attacks costing far less than their advertised strength
- **No new machine** — neither force needs a quantum computer; both happened with ordinary hardware
- **One-way ratchet** — an algorithm never gets stronger; every result about it only lowers the attack cost
- **Judged later** — an artifact encrypted today is attacked on tomorrow's cost curve, not today's

*Example (italic):* Bob's archive was built when DES-class effort meant decades of machine time; the same effort is now a rented cluster running for a week.

**Key point:** Two independent forces retire cryptography — compute gets cheaper (a slope) and mathematics gets better (a cliff) — and both push the attacker's cost in the same direction, downward.

### Visualization (canvas `c2`, 720×300)

Line chart: attacker cost over time falls on a gentle slope from cheaper compute, then drops off a cliff when cryptanalysis lands, crossing an "affordable" threshold.

- **Title (bold 16px, `#1a5276`, top center):** "Attacker Cost Falls Two Ways: A Slope and A Cliff".
- **Axes:** origin x=80, baseline y=252, plot right edge x=690, plot top y=52; 2px `#999` x-axis and y-axis; 12px `#444` x label "time →" at (385, 284); rotated-free 12px `#444` y label drawn as two stacked lines at x=14 is not used — instead a 12px `#6b7280` line "cost of a successful attack (log, illustrative)" at (85, 44), left-aligned.
- **Curve (3px, hardcoded polyline, drawn in two colored runs):** blue `#2a78d6` through `[80,80] [150,92] [220,104] [290,116] [330,122]`, then magenta `#d55181` for the cliff `[330,122] [344,182]`, then blue again through `[344,182] [420,192] [500,201] [580,210] [690,222]`.
- **Threshold line:** dashed (dash 6/5) 2px `#d95926` horizontal line at y=170 from x=80 to x=690, with 12px `#d95926` label "affordable to an attacker" at (86, 162).
- **Slope annotation (bold 13px `#2a78d6`, left-aligned at (140, 74)):** "cheaper compute: steady slope".
- **Cliff annotation (bold 13px `#d55181`, left-aligned at (356, 146)):** "better mathematics: a cliff".
- **Cliff arrow:** 3px `#d55181` vertical arrow at x=330 from y=126 to y=178 with a 10px arrowhead pointing down.
- **Below-line note (12px `#6b7280`, left-aligned at (420, 240)):** "same algorithm, now within reach".
- **Caption (12px `#444`, bottom right):** "shape illustrative".

## Eight Days for DES, 10^20 Years for AES-128

**Tags:** `worked example` (blue), `rule of thumb` (green), `birthday bound` (orange)

- **The key space** — 2^56 = 72,057,594,037,927,936 possible DES keys, a fixed number that never changed
- **A stated rate** — assume a modest cluster tries 100 billion (10^11) keys per second (illustrative)
- **The division** — 72,057,594,037,927,936 / 10^11 = 720,576 seconds of searching
- **In days** — 720,576 / 86,400 = 8.34 days, so one week of rented time exhausts the whole key space
- **AES-128 compared** — 2^128 / 10^11 ≈ 3.4 × 10^27 seconds ≈ 1.1 × 10^20 years at the same rate
- **Why that matters** — that is ~8 billion times the age of the universe, so key length is not the weak point
- **Birthday bound** — collisions on an n-bit hash cost about 2^(n/2): 2^64 for MD5, 2^80 for SHA-1, 2^128 for SHA-256
- **Real cost was lower** — MD5 collisions run on a laptop in seconds and SHA-1 fell near 2^63, not 2^80

*Example (italic):* The generic bound said SHA-1 collisions should cost 2^80 work; the published attack needed roughly 2^63, an industrial-scale but affordable computation.

**Key point:** The birthday bound is a ceiling on the attacker's cost, never a floor — the advertised strength is the best case for the defender, and cryptanalysis only ever moves the real number down.

### Visualization (canvas `c3`, 720×300)

Log-scale bar chart of work factors, with an illustrative "affordable today" threshold line showing which bars fall below it.

- **Title (bold 16px, `#1a5276`, top center):** "Work Factors on a Log Scale, and What Is Affordable".
- **Axes:** origin x=90, baseline y=250, plot right edge x=675, plot height 195; y is log10 of the operation count from 10^15 (baseline) to 10^40 (top), i.e. `y = 250 - (log10(work) - 15) / 25 * 195`.
- **Gridlines:** `#e5e9ef` 1px at 10^20, 10^25, 10^30, 10^35, 10^40 with right-aligned 12px `#444` labels "10^20" … "10^40" at x=84, plus "10^15" at the baseline.
- **Bars (70px wide, centers at x = 148, 265, 382, 499, 616), heights from log10 values `[16.858, 18.965, 19.266, 24.082, 38.532]`** giving pixel heights `[14.5, 30.9, 33.3, 70.8, 183.6]`:
  - x=148 `2^56` DES key search — fill `rgba(217,89,38,0.35)`, 2px `#d95926`
  - x=265 `2^63` SHA-1 collision, actual attack — fill `rgba(213,81,129,0.35)`, 2px `#d55181`
  - x=382 `2^64` MD5 collision, generic bound — fill `rgba(213,81,129,0.35)`, 2px `#d55181`
  - x=499 `2^80` SHA-1 collision, generic bound — fill `rgba(42,120,214,0.35)`, 2px `#2a78d6`
  - x=616 `2^128` AES-128 key search — fill `rgba(0,131,0,0.35)`, 2px `#008300`
- **Bar labels:** bold 12px in the bar's border color centered above each bar top ("2^56", "2^63", "2^64", "2^80", "2^128"); two-line 12px `#444` names below the baseline at y=268 and y=283 ("DES key" / "search", "SHA-1" / "actual", "MD5" / "generic", "SHA-1" / "generic", "AES-128" / "key").
- **Threshold line:** dashed (dash 6/5) 2px `#d95926` horizontal line at the log10 of 2^70 (21.072 → y ≈ 202.6) across x=90 to x=675, label 12px `#d95926` "affordable today (~2^70, illustrative)" left-aligned at x=96, 6px above the line.
- **Annotation (bold 13px `#d55181`, left-aligned at (150, 120)):** "cryptanalysis moved SHA-1 from 2^80 to 2^63".
- **Second annotation (bold 12px `#008300`, right-aligned at (700, 52)):** "2^128 stays out of reach".
- **Caption (12px `#444`, bottom right):** "rate assumption illustrative".

## "We Use Encryption" Is Not a Security Property

**Tags:** `common mistake` (red), `migration lag` (orange), `crypto-agility` (green)

- **The empty claim** — "we use encryption" says nothing; the algorithm, key length, mode, and year all matter
- **No announcement arrives** — the mathematics gets published, software defaults change, and nobody tells the old system
- **The lag is the incident** — a primitive stays known-weak for years while deployed code keeps verifying with it
- **Why migration stalls** — every producer and every verifier must switch together, so nobody moves first
- **Legacy interoperability** — a downgrade path that still accepts the old algorithm keeps the weakness fully alive
- **Crypto-agility** — store an algorithm identifier beside every artifact so the swap is configuration, not archaeology
- **Hardcoded means stuck** — a system that bakes in one algorithm name has no upgrade path at all
- **Not everything migrates** — hashes used for sharding, cache keys, or deduplication carry no security claim

*Example (italic):* Bob's archive records only a fingerprint column, so nobody can tell which rows are MD5 and which are SHA-256 — the migration becomes a forensic exercise instead of a config change.

**Common mistake:** Assuming a break reaches the people running old systems. It rarely does — the paper is published, the defaults quietly change, and an undisturbed legacy verifier keeps trusting a broken primitive for a decade.

### Visualization (canvas `c4`, 720×300)

Timeline: for each primitive, the span from publication to the demonstrated break (blue) and the span from break to deprecation or continued deployment (orange), showing the lag.

- **Title (bold 16px, `#1a5276`, top center):** "The Gap Between 'Break Published' and 'Still Deployed'".
- **Time axis:** x maps years 1975 → 2030 onto x=110 → 690, i.e. `x = 110 + (year - 1975) / 55 * 580`; 2px `#999` axis line at y=252 with 12px `#444` tick labels at 1980, 1990, 2000, 2010, 2020, 2030 and 1px `#e5e9ef` vertical gridlines up to y=60.
- **Rows (bar height 20, right-aligned 13px `#2c3e50` name ending at x=100):** y=78 "DES", y=133 "MD5", y=188 "SHA-1".
- **Blue segments (`rgba(42,120,214,0.35)` fill, 2px `#2a78d6` border) — published until break demonstrated:** DES 1977→1999, MD5 1992→2004, SHA-1 1995→2017.
- **Orange segments (`rgba(217,89,38,0.30)` fill, 2px `#d95926` border) — break until withdrawal or continued use:** DES 1999→2005 (withdrawn as a standard), MD5 2004→2026 (still present in legacy integrity checks), SHA-1 2017→2026 (still present in legacy artifacts).
- **Segment labels (11px `#444`):** "published &lt;year&gt;" left-aligned at each blue segment's left edge 5px above the bar, "break &lt;year&gt;" centered at the segment boundary 14px below the bar, and a bold 12px `#d95926` label 6px past each orange segment's right end: "withdrawn 2005", "still deployed", "still deployed".
- **Gap annotation (bold 13px `#d95926`, left-aligned at (300, 118)):** "MD5 collisions public since 2004".
- **Annotation (bold 12px `#6b7280`, left-aligned at (300, 232)):** "dates approximate; deployment persistence illustrative".
- **Caption (12px `#444`, bottom right):** "timeline schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data:** no randomness anywhere — all chart data is hardcoded literal arrays. The work-factor bars are computed from exact exponents via `Math.log10(Math.pow(2, k))` for k = 56, 63, 64, 80, 128, and the threshold line from k = 70. Arithmetic that must hold to the digit: 2^56 = 72,057,594,037,927,936; 72,057,594,037,927,936 / 10^11 = 720,576 s; 720,576 / 86,400 = 8.34 days; 2^128 / 10^11 ≈ 3.4 × 10^27 s ≈ 1.1 × 10^20 years. The 10^11 keys/second rate and the ~2^70 "affordable today" threshold are stated as illustrative assumptions; historical dates are approximate and the "still deployed" spans are labeled illustrative.
- **Scope boundary:** this page is about cipher and hash primitives reaching end of life — key-space economics, collision versus preimage breaks, and migration lag. Password storage, salting, and deliberately slow hashing belong to the password-hashing and rainbow-table pages and are not repeated here.
- **Framing:** defensive/educational; algorithm names (MD5, SHA-1, SHA-256, DES, AES) are open standards, not products, and no vendor or product is named. No specimen digests, keys, or credential strings appear anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
