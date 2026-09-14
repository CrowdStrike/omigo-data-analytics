# The Economics of Cracking Hashes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Economics of Cracking Hashes

**Subtitle:** The same stolen table is safe or lost depending only on which hash was used — and the dividing line moves every hardware generation

## Nobody Is Serving the Guesses Anymore

**Tags:** `core idea` (blue), `offline cracking` (orange), `cost per guess` (green)

- **The theft** — one website's user table walks out the door: 100,000 records, one stored hash per user
- **Fixed attacker** — one rented GPU and one wordlist; the only variable is which hash the site chose
- **No rate limit** — offline there is no server in the loop, so the attacker's own hardware sets the pace
- **Not a login** — online guessing meets throttling and lockout after a few tries; a stolen table meets neither
- **The only lever** — with attempt speed uncapped, the sole remaining defense is the cost of one guess
- **Priced, not graded** — the margin is measured in GPU-hours and dollars rather than in "strength"

*Example (italic):* The stolen table is byte-for-byte the same in all three cases — a hash choice made years earlier decides whether cracking it costs the price of a coffee or a quarter's budget.

**Key point:** Offline cracking removes the rate limit that protects a login page, so a password hash's job is to be deliberately expensive per guess — and the security margin is a currency amount, not an adjective.

### Visualization (canvas `c1`, 720×300)

Two-band schematic: the online path is throttled by the server, the offline path is throttled by nothing, and both converge on a single remaining defense.

- **Title (bold 15px, `#1a5276`, top center):** "Online Guessing Is Throttled; Offline Cracking Is Not".
- **Box style:** rounded 8px radius, 12px `#2c3e50` centered text (two lines where noted), 2px borders, 3px `#6b7280` arrows with arrowheads between boxes.
- **Band 1 (boxes 44px tall, top edge y=68), divider `#e5e9ef` line across at y=135:** blue `rgba(42,120,214,0.15)` / `#2a78d6` box at x=30, w=170, two lines "attacker at the" / "login page" → arrow → green `rgba(0,131,0,0.12)` / `#008300` box at x=250, w=250, two lines "the server sets the pace:" / "100 tries/hour, then lockout" → bold 13px green `#008300` text "✓ rate limited" left-aligned at x=520.
- **Band 2 (boxes 44px tall, top edge y=158):** blue box at x=30, w=170, two lines "attacker holds the" / "stolen table" → arrow → orange `rgba(217,89,38,0.15)` / `#d95926` box at x=250, w=250, two lines "no server in the loop:" / "the GPU sets the pace" → bold 13px red `#e74c3c` text "✗ no rate limit" left-aligned at x=520.
- **Bottom callout box:** violet `rgba(74,58,167,0.10)` fill, 2px `#4a3aa7` border, x=150, y=238, 420×42, bold 13px `#4a3aa7` centered text "the only defense left: cost per guess".
- **Caption (12px `#444`, bottom right):** "schematic".

## Doing the Division: 10 Seconds, 578.7 Days, 15.9 Years

**Tags:** `worked example` (blue), `guesses per second` (orange), `cost per password` (green)

- **The candidate space** — a wordlist plus mutation rules enumerates 1,000,000,000,000 (1e12) candidates
- **Illustrative rates** — one GPU: fast general-purpose hash 100,000,000,000/s, bcrypt 20,000/s, memory-hard 2,000/s
- **Fast hash** — 1e12 ÷ 100,000,000,000 = 10 seconds for the entire candidate space against the stolen table
- **bcrypt** — 1e12 ÷ 20,000 = 50,000,000 seconds; ÷ 86,400 = 578.7 days of continuous GPU time
- **Memory-hard** — 1e12 ÷ 2,000 = 500,000,000 s = 5,787.0 days; ÷ 365 = 15.9 years for one sweep
- **The ratio** — 100,000,000,000 ÷ 2,000 = 50,000,000× more cost per guess, same table and same hardware
- **In money** — at $1.00 per GPU-hour: 50,000,000 ÷ 3,600 = 13,889 GPU-hours ≈ $13,889 for the bcrypt sweep
- **The contrast** — the fast-hash sweep is 10 ÷ 3,600 = 0.0028 GPU-hours ≈ $0.003, a rounding error

*Example (italic):* Not one character of any password changed between those rows — same table, same GPU, same wordlist — yet the full sweep costs $0.003 under one hash and $13,889 under another.

**Key point:** One stored hash turns a 10-second job into a 15.9-year one. The fast-hash sweep is loose change; the bcrypt sweep is a budget decision an attacker has to justify.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart on a true log axis: guesses per second for the three storage choices, each labeled with its computed 1e12 sweep time and dollar cost.

- **Title (bold 15px, `#1a5276`, top center):** "Guesses per Second (log scale) and the Cost of One Full Sweep".
- **Axis:** horizontal 2px `#999` line at y=238 from x=250 to x=670; log10 mapping `x = 250 + (log10(v) - 3) * (420 / 9)` covering 1e3 to 1e12; 12px `#444` tick labels centered at x = 250, 390, 530, 670 reading "1 thousand", "1 million", "1 billion", "1 trillion" at y=255.
- **Rows (bars 18px tall, top edges y = 68, 128, 188), each with two 12px `#444` left-aligned label lines at x=20 (first at barTop+2, second at barTop+18):**
  - y=68 "fast general-purpose hash" / "100,000,000,000 /s": fill `rgba(231,76,60,0.30)`, 2px `#e74c3c` border, bar width `(11 - 3) * 420/9 = 373.3`; bold 12px `#e74c3c` label right-aligned at barEnd − 8 reading "10 s ≈ $0.003".
  - y=128 "bcrypt, moderate work factor" / "20,000 /s": fill `rgba(25,158,112,0.30)`, 2px `#199e70` border, bar width `(4.301 - 3) * 420/9 = 60.7`; bold 12px `#199e70` label left-aligned at barEnd + 8 reading "578.7 days ≈ $13,889".
  - y=188 "memory-hard, moderate setting" / "2,000 /s": fill `rgba(0,131,0,0.30)`, 2px `#008300` border, bar width `(3.301 - 3) * 420/9 = 14.0`; bold 12px `#008300` label left-aligned at barEnd + 8 reading "5,787.0 days = 15.9 years".
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned at x=255, y=228):** "50,000,000× more cost per guess".
- **Caption (12px `#444`, bottom right, y=294):** "rates illustrative for one modern GPU; every division exact".
- **Axis note (12px `#6b7280`, centered at x=440, y=275):** "guesses per second".

## The Weak Passwords Fall First, Under Any Hash

**Tags:** `honest caveat` (red), `likelihood ordering` (orange), `long tail` (green)

- **Not a sweep** — attackers never run the whole space; the wordlist is ordered by likelihood, best guesses first
- **The assumption** — 30,000 of the 100,000 users picked a password inside the first 1,000,000 candidates
- **Hand-check** — 1,000,000 ÷ 20,000 per second = 50 seconds, and that is *with* bcrypt doing the hashing
- **The share** — 30,000 ÷ 100,000 = 30% of accounts open inside the first minute of a well-chosen hash
- **What slowness buys** — the long tail: only 61,000 of 100,000 fall in the whole run, so 39,000 stay unreached
- **The ceiling** — 1e9 ÷ 20,000 = 50,000 s = 13.9 hours reaches 51,000 accounts; the next 578 days add only 10,000
- **The honest limit** — a slow hash protects the average password, never the guessable one
- **Still invalidate** — after a breach you cannot know whose password was weak, so reset every credential

*Example (italic):* 30,000 accounts open in the first 50 seconds and 51,000 within 13.9 hours; grinding out the remaining 578 days adds only 10,000 more (cracked counts illustrative).

**Key point:** A slow hash does not save a weak password — likelihood-ordered guessing finds it in the opening seconds. What slowness actually protects is the long tail of decent passwords behind it.

### Visualization (canvas `c3`, 720×300)

Cumulative curve on a log-scale x axis: accounts cracked out of 100,000 as the bcrypt run progresses, steep in the first seconds then nearly flat for the remaining 578 days.

- **Title (bold 15px, `#1a5276`, top center):** "30% Fall in the First Minute; the Next 578 Days Add 31,000".
- **Axes:** origin x=70, baseline y=240, plot width 600, plot height 175 (top y=65); x = candidates tried, log10 from 3 to 12 via `x = 70 + (log10(v) - 3) * (600/9)`; y = accounts cracked 0 to 100,000 via `y = 240 - c/100000*175`; gridlines `#e5e9ef` at 25,000 / 50,000 / 75,000 / 100,000 with 12px `#444` right-aligned labels ending at x=64; x-axis 2px `#999`.
- **X tick labels (12px `#444`, centered, y=258) at x = 70, 270, 470, 670:** "1 thousand", "1 million", "1 billion", "1 trillion"; second row (12px `#6b7280`, y=276) at the same x positions: "0.05 s", "50 s", "13.9 hours", "578.7 days".
- **Curve:** 3px `#2a78d6` line through the hardcoded pairs (candidates, cracked) `[(1e3, 2000), (1e4, 8000), (1e5, 18000), (1e6, 30000), (1e7, 38000), (1e8, 45000), (1e9, 51000), (1e10, 55000), (1e11, 58000), (1e12, 61000)]`, 4px `#2a78d6` dots at each point.
- **Marker at the 1e6 point (x=270, y=187.5):** 6px `#e74c3c` dot, dashed `#e74c3c` (dash 4/3) vertical line from that point down to the baseline, bold 12px `#e74c3c` label left-aligned at (280, 182) reading "30,000 accounts (30%) in 50 s".
- **End label:** bold 12px `#2a78d6` right-aligned at (664, 124) reading "61,000"; 12px `#6b7280` right-aligned at (664, 142) reading "39,000 never reached".
- **Annotation (bold 13px green `#008300`, left-aligned at x=300, y=100):** "slowness protects the tail, not the weak".
- **Caption (12px `#444`, bottom right, y=294):** "cracked counts illustrative; x axis log10, times at 20,000 guesses/s".

## Salt, Peppers, and the Line That Keeps Moving

**Tags:** `moving line` (blue), `work factor` (orange), `common mistake` (red)

- **Salt** — a per-user random value defeats precomputed tables and forces the attacker to work per user
- **Not slower** — salt multiplies total cost by the user count but adds nothing to the cost of one guess
- **Work factor** — a tunable dial that must be raised as hardware improves, rehashing at each next login
- **The drift** — unchanged code and setting: 20,000 guesses/s becomes 320,000 after four hardware doublings
- **Verify** — 1e12 ÷ 320,000 = 3,125,000 s; ÷ 86,400 = 36.2 days, down from the original 578.7 days
- **Memory-hard** — GPUs parallelize arithmetic far more cheaply than memory, which flattens that advantage
- **Pepper** — a secret held outside the database means a database-only breach yields nothing crackable
- **The confusion** — "our passwords were hashed" names neither hash nor work factor, so it claims nothing

*Example (italic):* A work factor chosen when a GPU managed 20,000 guesses/s permits 320,000 today — the sweep fell from 578.7 days to 36.2 days while not one line of code changed.

**Common mistake:** "They were hashed, so they're safe" is not a claim until you say which hash at which work factor — the same sentence covers a 10-second sweep and a 15.9-year one. Its mirror is equally wrong: a strong hash is no substitute for password strength, because likelihood-ordered guessing still finds weak passwords in the opening seconds under any hash.

### Visualization (canvas `c4`, 720×300)

Log-scale bar chart: guesses per second achievable against one unchanged work factor across four hardware doublings, with the computed 1e12 sweep time under each bar.

- **Title (bold 15px, `#1a5276`, top center):** "A Fixed Work Factor Loses Ground Every Hardware Doubling".
- **Axes:** baseline 2px `#999` at y=240 from x=75 to x=675; log10 y mapping `barHeight = (log10(v) - 4) * 87.5` (plot height 175, 1e4 at the baseline, 1e6 at y=65); gridlines `#e5e9ef` at 1e4 (y=240), 1e5 (y=152.5), 1e6 (y=65) with 12px `#444` right-aligned labels "10,000", "100,000", "1,000,000" ending at x=68.
- **Bars (60px wide, centered at x = 135, 245, 355, 465, 575), hardcoded guesses/s `[20000, 40000, 80000, 160000, 320000]` giving heights `[26.3, 52.7, 79.0, 105.4, 131.7]`:** fills/borders in order green `rgba(0,131,0,0.30)`/`#008300`, aqua `rgba(25,158,112,0.30)`/`#199e70`, yellow `rgba(201,133,0,0.30)`/`#c98500`, orange `rgba(217,89,38,0.30)`/`#d95926`, red `rgba(231,76,60,0.30)`/`#e74c3c`.
- **Bar labels:** bold 12px in each bar's border colour, centered 8px above the bar top: "20,000", "40,000", "80,000", "160,000", "320,000".
- **Below-axis labels (centered on each bar):** 12px `#444` at y=258 reading "today", "+1×", "+2×", "+3×", "+4×"; 12px `#6b7280` at y=276 reading "578.7 d", "289.4 d", "144.7 d", "72.3 d", "36.2 d"; row captions right-aligned at x=68, 12px `#6b7280`: "hardware:" at y=258 and "1e12 sweep:" at y=276.
- **Y-axis note (12px `#6b7280`, left-aligned at x=75, y=52):** "guesses per second (log scale)".
- **Annotations:** bold 13px `#d95926` at (150, 92) "same code, same setting: 16× cheaper to crack"; 12px `#008300` at (150, 110) "the fix is raising the work factor, then rehash at login".
- **Caption (12px `#444`, bottom right, y=294):** "doubling cadence illustrative; every division exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** no randomness anywhere — every series is a hardcoded literal array. The illustrative inputs are the three guess rates (1e11, 2e4, 2e3 per second on one GPU), the 1e12 candidate space, the $1.00 per GPU-hour rental price, the 100,000-record table, the cracked-account curve, and the two-year-per-doubling cadence. Every derived figure is exact and must match between prose and chart: 1e12 ÷ 1e11 = 10 s; 1e12 ÷ 2e4 = 5e7 s ÷ 86,400 = 578.7 days; 1e12 ÷ 2e3 = 5e8 s = 5,787.0 days ÷ 365 = 15.9 years; 1e11 ÷ 2e3 = 50,000,000×; 5e7 ÷ 3,600 = 13,889 GPU-hours ≈ $13,889; 10 ÷ 3,600 = 0.0028 GPU-hours ≈ $0.003; 1e6 ÷ 2e4 = 50 s; 30,000 ÷ 100,000 = 30%; 1e9 ÷ 2e4 = 50,000 s = 13.9 hours; 61,000 − 51,000 = 10,000, 61,000 − 30,000 = 31,000, and 100,000 − 61,000 = 39,000; 1e12 ÷ 320,000 = 3,125,000 s ÷ 86,400 = 36.2 days, and the doubling ladder 578.7 / 289.4 / 144.7 / 72.3 / 36.2 days.
- **Scope boundaries:** this page owns the *economics* only — what a hash costs per guess and how that cost decays. It does not re-teach what a hash is or why passwords are never stored (that is the password-hashing page), and it does not cover online guessing against a login page beyond the one contrast that motivates the offline setting (that is the brute-force page).
- **Framing:** defensive/educational. Algorithm names (bcrypt, scrypt, Argon2, SHA-256, MD5) are open standards and may be named; no vendors, no cracking tools, no products. No credential strings, specimen digests, or realistic example passwords appear anywhere on the page.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
