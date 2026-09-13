# Euclid's GCD

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Euclid's GCD

**Subtitle:** To find the biggest length that measures two numbers exactly, keep replacing the bigger number with the remainder of dividing it by the smaller — the last nonzero remainder is the answer, and this trick has worked for 2,300 years

## Two Ribbon Rolls, One Cutting Length

**Tags:** `core idea` (blue), `running example` (green), `greatest common divisor` (orange)

- **The florist** — two ribbon rolls, 84 cm and 60 cm, must be cut into equal pieces with zero waste
- **The question** — what is the longest piece length that divides both rolls exactly?
- **A near miss** — 15 cm fits the 60 cm roll perfectly but leaves 9 cm of the 84 cm roll on the floor
- **The winner** — 12 cm works on both: the 84 cm roll gives 7 pieces, the 60 cm roll gives 5, no scrap
- **The name** — that 12 is the greatest common divisor (GCD): the largest number dividing both exactly

*Example (italic):* Any cut longer than 12 cm wastes ribbon on at least one roll; shorter common cuts like 6 or 4 cm also waste nothing but give smaller pieces — 12 is the greatest.

**Key point:** The GCD of two numbers is the longest measuring stick that fits both exactly — here gcd(84, 60) = 12.

### Visualization (canvas `c1`, 720×300)

Two horizontal ribbon bars drawn to scale (7 px per cm), each sliced into 12 cm pieces, showing that the same piece length exhausts both rolls with nothing left over.

- **Title (bold 15px, `#1a5276`, top center):** "Cutting 84 cm and 60 cm Rolls into Equal 12 cm Pieces".
- **Roll A:** bold 13px `#1a5276` label at x=90, y=95: "roll A — 84 cm (7 pieces)"; bar from x=90 to x=678 (588px = 84 cm × 7 px/cm), y=105 to y=131 (26px tall), fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border; 2px white separators every 84px (one per 12 cm piece); bold 12px `#1a5276` "12" centered in each of the 7 segments.
- **Roll B:** bold 13px `#008300` label at x=90, y=165: "roll B — 60 cm (5 pieces)"; bar from x=90 to x=510 (420px), y=175 to y=201, fill `rgba(0,131,0,0.30)`, 2px `#008300` border; same 84px separators; "12" in each of the 5 segments.
- **Ruler:** 1px `#999` axis line at y=235 from x=90 to x=678, tick labels "0", "12", "24", "36", "48", "60", "72", "84" every 84px (12px `#444`).
- **Annotation (bold 13px orange `#d95926`, centered at x=360, y=218, one line):** "12 cm fits both rolls exactly — the greatest common divisor".
- **Caption (12px `#444`, bottom right):** "illustrative florist example — a 15 cm cut would leave 9 cm of roll A as waste".

## Divide, Keep the Remainder, Repeat

**Tags:** `worked example` (blue), `remainders` (green)

- **The trick** — divide the bigger number by the smaller one and keep only the remainder
- **Step 1** — 84 ÷ 60 = 1 remainder 24, so the pair (84, 60) shrinks to (60, 24)
- **Step 2** — 60 ÷ 24 = 2 remainder 12, and the pair shrinks again to (24, 12)
- **Step 3** — 24 ÷ 12 = 2 remainder 0; a zero remainder means stop
- **The answer** — the last nonzero remainder, 12, is the GCD — the same 12 cm the florist found
- **Why it works** — anything that measures both 84 and 60 must also measure their leftover 24, so no candidate is ever lost

*Example (italic):* Three little divisions replace testing 60 candidate lengths one by one — and the same three-line recipe works on numbers with hundreds of digits.

**Key point:** Replace (big, small) with (small, remainder) until the remainder hits 0; the last nonzero remainder is the GCD.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart of the shrinking pair across the three division steps: each group shows the current (bigger, smaller) pair, with the remainder chain 24 → 12 → 0 marching toward the answer.

- **Title (bold 15px, `#1a5276`, top center):** "Three Divisions: (84, 60) → (60, 24) → (24, 12) → stop".
- **Axes:** origin x=60, baseline y=235, plot width 610, plot height 175; y = 0 to 90 with light `#e5e9ef` gridlines at 20, 40, 60, 80 and 12px `#444` tick labels.
- **Groups (centers at x = 145, 295, 445, 595):** two bars each, 38px wide with a 6px gap — left bar is the bigger number (fill `rgba(42,120,214,0.35)`, 2px `#2a78d6` border), right bar is the smaller/remainder (fill `rgba(217,89,38,0.25)`, 2px `#d95926` border). Pair values, hardcoded: `[84, 60]`, `[60, 24]`, `[24, 12]`, `[12, 0]`; the final 0 draws no bar — instead bold 12px `#008300` "0 — stop" sits at the baseline.
- **Value labels:** bold 12px above each bar in its border color ("84", "60", "60", "24", "24", "12", "12").
- **Equation labels (11px `#6b7280`, centered under each group at y=262):** "84 = 1×60 + 24", "60 = 2×24 + 12", "24 = 2×12 + 0", "gcd = 12".
- **Flow arrows:** dashed `#6b7280` (dash 4/3) arrows from the top of each group's smaller bar to the top of the next group's bigger bar — the small number is promoted, the remainder joins it.
- **Annotation (bold 13px green `#008300`, near x=505, y=90):** "last nonzero remainder = 12 → the GCD".

## Twenty-Three Centuries of Service

**Tags:** `where it's used` (blue), `history` (orange), `everyday tech` (green)

- **The record** — written in Euclid's Elements around 300 BC, it is the oldest algorithm still in daily use
- **Fractions** — reducing 84/60 means dividing both by their GCD 12, giving the simplest form 7/5
- **Screens** — gcd(1920, 1080) = 120, which is exactly how a monitor earns its familiar "16:9" label
- **Cryptography** — making an RSA key starts with a GCD check; Euclid-style inverses power https signatures
- **Built in** — the algorithm ships inside every phone's math and crypto libraries, unchanged at heart

*Example (italic):* The gcd(1920, 1080) ladder — remainders 840, 240, 120, 0 — takes four divisions, and 1920/120 : 1080/120 is the 16:9 on the spec sheet.

**Key point:** Euclid's GCD predates paper books, algebra, and computers — and still runs billions of times a day inside ordinary devices.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline with four milestone dots from Euclid's Elements to today, making the "oldest algorithm still in service" claim visible at a glance.

- **Title (bold 15px, `#1a5276`, top center):** "The Oldest Algorithm Still in Service".
- **Baseline:** 2px `#999` horizontal line at y=175 from x=70 to x=690, small arrowhead at the right end.
- **Milestone dots (8px radius, on the line), year labels bold 13px `#1a5276` centered above each dot at y=150, descriptions 11px `#444` centered below in two lines at y=200 and y=214:**
  - x=120, blue `#2a78d6`: "~300 BC" — "Euclid's Elements," / "Book VII"
  - x=300, aqua `#199e70`: "1482" — "first printed" / "edition, Venice"
  - x=480, orange `#d95926`: "1977" — "RSA encryption" / "built on GCD checks"
  - x=640, green `#008300`: "2026" — "in every phone's" / "crypto library"
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=380, y=90):** "≈2,300 years in service — billions of runs a day".
- **Caption (12px `#444`, bottom right):** "timeline schematic — spacing not to scale".

## No Prime Factoring Required

**Tags:** `common mistake` (red), `no factoring` (orange)

- **The instinct** — most people find a GCD by factoring both numbers into primes and collecting shared ones
- **Fine for small** — 84 = 2·2·3·7 and 60 = 2·2·3·5 share 2·2·3 = 12, the same ribbon answer
- **Breaks at scale** — factoring grinds to a halt on huge numbers; that very hardness is what guards crypto keys
- **Euclid skips it** — gcd(1071, 462) falls in 3 divisions (remainders 147, 21, 0) with no primes in sight
- **Brute force** — testing every candidate length from 462 down would take up to 462 checks to reach 21

*Example (italic):* Two students race on gcd(1071, 462): one starts building factor trees, the other writes three remainder lines and announces 21 first.

**Common mistake:** Treating GCD as a factoring problem. Euclid's algorithm finds the answer without ever learning the primes — which is exactly why it stays fast at sizes where factoring is hopeless.

### Visualization (canvas `c4`, 720×300)

Two horizontal bars comparing step counts for the same problem — brute-force candidate checking versus Euclid's three divisions — with the actual remainder ladder printed beneath.

- **Title (bold 15px, `#1a5276`, top center):** "Same Answer for gcd(1071, 462): 3 Divisions vs 462 Checks".
- **Layout:** row labels 12px `#444` left-aligned at x=20; bars start at x=235 with max width 445 (to x=680), 26px tall.
- **Row 1 (y=105):** label "brute force — try every length 462 → 1"; bar width 445px representing 462 checks, fill `rgba(213,81,129,0.30)`, 2px `#d55181` border; bold 12px `#d55181` label just left of the bar's right end: "462 checks".
- **Row 2 (y=165):** label "Euclid — divide, keep remainder"; bar width 6px (true scale ≈3px, drawn at minimum visible width), fill `#008300`; bold 12px `#008300` label right of the bar: "3 divisions".
- **Ladder text (11px `#6b7280`, at x=235, y=212, three lines 14px apart):** "1071 = 2×462 + 147" / "462 = 3×147 + 21" / "147 = 7×21 + 0 → gcd = 21".
- **Annotation (bold 13px green `#008300`, near x=430, y=270):** "no factoring, no search — 154× fewer steps".
- **Caption (12px `#444`, bottom right):** "illustrative step counts — brute force tests each candidate length once".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar lengths, pair values, ladder equations, and milestone positions are the hardcoded literals above (no randomness); the arithmetic is exact — gcd(84, 60) = 12, gcd(1920, 1080) = 120, gcd(1071, 462) = 21 — while step-count comparisons and the timeline spacing are labeled illustrative/schematic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
