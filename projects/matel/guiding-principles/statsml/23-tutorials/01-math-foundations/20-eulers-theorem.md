# Euler's Theorem

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Euler's Theorem

**Subtitle:** Raise any number coprime to n to the power φ(n) and the remainder is 1 — the one-line fact that guarantees RSA decryption undoes encryption for messages coprime to n

## The Last Digit of 7^100

**Tags:** `core idea` (blue), `repeating pattern` (green)

- **The puzzle** — what is the last digit of 7^100? The number itself is far too big to print.
- **Just watch** — 7, 49, 343, 2401 end in 7, 9, 3, 1 — and then the pattern starts over.
- **Cycle of 4** — every 4th power of 7 ends in 1, so only the exponent mod 4 matters.
- **The answer** — 100 = 4 × 25, so 7^100 lands where 7^4 does: last digit 1.
- **Last digit = mod 10** — "last digit" is just the remainder after dividing by 10.

*Example (italic):* 7^100 has 85 digits, yet its last digit takes ten seconds of arithmetic to find: it is 1.

**Key point:** Powers cycle in modular arithmetic. Euler's theorem names the exponent that is guaranteed to bring the remainder back to 1.

### Visualization (canvas `c1`, 720×300)

Line chart of the last digits of 7^1..7^12, showing the 7, 9, 3, 1 pattern repeating every 4 steps, with dashed vertical lines at each cycle boundary.

- **Title (bold 15px, `#1a5276`, top center):** "Last Digit of 7^k: the Pattern Repeats Every 4 Steps".
- **Data:** digits `[7, 9, 3, 1, 7, 9, 3, 1, 7, 9, 3, 1]` for exponents k = 1..12.
- **Axes:** origin x=60, width 600, baseline y=245, chart height 180, y scale 0–10; 1px `#999` L-shaped axis.
- **Cycle boundaries:** dashed `#bdc3c7` (dash 4/3) vertical lines at k = 4, 8, 12 (x = lx + (k−1)/11 × 600), from top of chart to baseline.
- **Line:** blue `#2a78d6` 2px polyline through all 12 points.
- **Points:** 5px dots — green `#008300` where the digit is 1, blue otherwise; each point labeled with its digit (bold 12px, green if 1, else `#444`) 10px above; x-axis labels "7^1".."7^12" (11px `#444`) 16px below baseline.
- **Annotations:** orange `#d95926` bold 13px centered at y=46: "7, 9, 3, 1 — then over again: cycle length 4"; green bold 13px centered at h−8: "100 = 4 × 25 → 7^100 ends in 1".

## Counting the Coprimes: φ(n)

**Tags:** `definition` (blue), `worked example` (green), `totient` (orange)

- **Coprime** — a number is coprime to 10 if it shares no factor with 10 (no 2s, no 5s).
- **The totient** — φ(10) counts the coprimes among 1..10: only {1, 3, 7, 9}, so φ(10) = 4.
- **Euler's theorem** — if a is coprime to n, then a^φ(n) leaves remainder 1 mod n.
- **Our case** — 7 is coprime to 10 and φ(10) = 4, so 7^4 ≡ 1 (mod 10): the cycle explained.
- **Why (the shuffle)** — multiplying {1, 3, 7, 9} by 7 mod 10 just reshuffles the same set.

*Example (italic):* ×7 mod 10 sends 1→7, 7→9, 9→3, 3→1 — same four numbers in a new order, back home in 4 steps.

**Key point:** Euler's theorem: a^φ(n) ≡ 1 (mod n) whenever gcd(a, n) = 1. The magic exponent is not n — it is φ(n), the count of coprimes below n.

### Visualization (canvas `c2`, 720×300)

Dual-panel diagram: a 2×5 grid of the numbers 1..10 with coprimes highlighted (left) and a 4-node cycle diagram of the ×7 mod 10 shuffle on {1, 3, 7, 9} (right), split by a dashed divider at x=355.

- **Title (bold 15px, `#1a5276`, top center):** "φ(10) = 4 Coprimes, and Why ×7 Brings Them Home in 4 Steps".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=355 from y=40 to h−12.
- **Left panel (grid of boxes):** heading bold 12px `#444` left-aligned at (45, 66): "numbers 1..10 — green shares no factor with 10"; ten 50×50 boxes (8px gap) in 2 rows of 5 starting at (45, 80); coprimes {1, 3, 7, 9} fill `rgba(0,131,0,0.18)` with 2px green `#008300` border and bold 16px green number; the rest fill `rgba(107,114,128,0.12)` with 1px `#bbb` border and bold 16px mute `#6b7280` number.
- **Left captions:** green bold 14px "φ(10) = 4  →  {1, 3, 7, 9}"; mute 12px "gray numbers share a 2 or a 5 with 10" — both left-aligned at x=45, below the grid (y = 80 + 2×58 + 24 and + 44).
- **Right panel (cycle):** heading bold 12px `#444` centered at (540, 56): "multiply by 7, keep the last digit"; four nodes at radius 72 around center (540, 168) — 1 (top), 7 (right), 9 (bottom), 3 (left); each node a white-filled circle radius 20 with 2px green stroke and bold 16px green number; violet `#4a3aa7` 2px arrows (9px filled arrowheads) connecting 1→7→9→3→1, trimmed 22px from each node center.
- **Right annotations:** violet bold 12px "1→7→9→3→1" at the circle's center; violet bold 13px centered under the cycle on two lines (h−26 and h−10): "same set reshuffled — 4 steps return to start," / "so 7^4 ≡ 1 (mod 10)".

## A Toy RSA Round Trip

**Tags:** `where it's used` (blue), `RSA` (green), `worked example` (orange)

- **Setup** — pick primes p = 3, q = 11; publish n = 33, keep φ(33) = 2 × 10 = 20 secret.
- **Key pair** — pick e = 3, then d = 7, because 3 × 7 = 21 leaves remainder 1 mod 20.
- **Encrypt** — message m = 4 becomes c = 4^3 mod 33 = 64 mod 33 = 31.
- **Decrypt** — c^d = 31^7 mod 33 = 4; the original message comes back exactly.
- **Why it works** — m^21 = (m^20) × m ≡ 1 × m by Euler, since 20 = φ(33).
- **The security** — everyone sees n = 33, but finding φ(n) means factoring n.

*Example (italic):* With a 600-digit n, factoring takes longer than the age of the universe — so φ(n) stays secret.

**Key point:** RSA chooses e × d ≡ 1 (mod φ(n)) so that m^(ed) ≡ m (mod n). Euler's theorem is the one line that guarantees decryption undoes encryption for messages coprime to n.

### Visualization (canvas `c3`, 720×300)

Flow diagram of the toy RSA round trip: two key boxes on top, a five-box encrypt/decrypt pipeline with arrows, and three annotation lines beneath.

- **Title (bold 15px, `#1a5276`, top center):** "Toy RSA with n = 33: Encrypt with e = 3, Decrypt with d = 7".
- **Key boxes (y=42, 240×40, 2px stroke, bold 13px centered label):** blue `#2a78d6` box at x=90 labeled "public: n = 33, e = 3"; magenta `#d55181` box at x=390 labeled "secret: φ(33) = 20, d = 7".
- **Flow boxes (116×52 at y=118, x positions `[22, 162, 302, 442, 582]`, 2px stroke in the box color, bold 12px top label + bold 14px value):** "message / m = 4" (green `#008300`), "encrypt / 4^3 mod 33" (blue), "cipher / c = 31" (orange `#d95926`), "decrypt / 31^7 mod 33" (magenta), "back to / m = 4" (green); gray `#888` 2px arrows with filled arrowheads between consecutive boxes.
- **Sub-captions (12px `#444`, 18px below the boxes):** "64 − 33 = 31" under the encrypt box; "sent in the open" under the cipher box.
- **Bottom annotations (centered):** green bold 14px at y=232: "why: m^(3×7) = m^21 = (m^20) × m ≡ 1 × m   because m^φ(33) ≡ 1 (Euler)"; orange bold 13px at y=262: "an eavesdropper needs φ(33) = 20 — and finding it means factoring 33 = 3 × 11"; mute `#6b7280` 12px at y=286: "toy-sized numbers, illustrative — real RSA uses 600-digit n".

## Where the Shortcut Breaks

**Tags:** `common mistake` (red), `rule of thumb` (green)

- **Coprime or bust** — 2^4 mod 10 = 6, not 1; Euler promises nothing when a shares a factor with n.
- **Test every digit** — a^4 mod 10 = 1 only for a = 1, 3, 7, 9 — exactly the coprimes to 10.
- **Shrink mod φ(n)** — reduce exponents mod φ(10) = 4, never mod 10 itself.
- **Wrong route** — 7^13 ends in 7 (13 mod 4 = 1), not in 3 (the wrong 13 mod 10 = 3 route).
- **Fermat's case** — when n is a prime p, φ(p) = p − 1 and Euler becomes Fermat's little theorem.

*Example (italic):* Marking 2^4 mod 10 as 1 is the classic slip — 16 leaves remainder 6, because 2 divides 10.

**Common mistake:** Applying a^φ(n) ≡ 1 without checking gcd(a, n) = 1. Verify coprimality first, then shrink exponents mod φ(n) — never mod n.

### Visualization (canvas `c4`, 720×300)

Bar chart of a^4 mod 10 for a = 1..9, coprime bars in green and non-coprime bars in orange, with a dashed "Euler's promise" line at remainder 1.

- **Title (bold 15px, `#1a5276`, top center):** "a^4 mod 10 for a = 1..9: Only Coprimes Land on 1".
- **Data:** values `[1, 6, 1, 6, 5, 6, 1, 6, 1]` for a = 1..9; coprime flags `[true, false, true, false, false, false, true, false, true]`.
- **Axes:** origin x=60, width 600, baseline y=240, chart height 170, y scale max 7; 1px `#999` L-shaped axis.
- **Promise line:** dashed green `#008300` (dash 5/4, 1.5px) horizontal line at remainder 1, labeled bold 12px green left-aligned 8px in and 8px above the line: "Euler's promise: remainder 1".
- **Bars:** width 600/9 with 8px insets (bar body = bw − 16); coprime bars fill `rgba(0,131,0,0.4)` with 1.5px green stroke, others fill `rgba(217,89,38,0.4)` with 1.5px orange `#d95926` stroke; value labeled bold 13px (green or orange) 8px above each bar; x labels "1^4".."9^4" (12px `#444`) 18px below baseline.
- **Annotations:** orange bold 13px centered at chart top (y = baseY − chH + 6): "2^4 = 16 → remainder 6: shares a factor with 10, no promise"; green bold 13px centered at h−12: "green a = 1, 3, 7, 9 are the coprimes to 10 — all hit 1".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. A shared `arrow(ctx, x1, y1, x2, y2, color, lw)` helper draws a line plus a 9px filled arrowhead. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
