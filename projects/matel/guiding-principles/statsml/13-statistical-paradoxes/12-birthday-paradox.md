# Birthday Paradox: Your ID Space Runs Out Long Before You Think It Does

**Subtitle:** Duplicates arrive when the number of pairs gets large, not when the number of items does

Four `.card-section` blocks, canvases `c1`..`c4`, 50/50 columns.

---

## Section 1 — Pairs Multiply Much Faster Than Items Do

**Tags:** blue "core idea" · green "counting pairs" · orange "n squared"

**Bullets**
- **The question people answer** — they estimate the chance that one particular item finds a twin.
- **The question that matters** — a system breaks if *any* two items anywhere match each other.
- **Pair count** — with n items there are n(n−1)/2 distinct pairs, each its own chance to match.
- **Quadratic growth** — double the items and you nearly quadruple the pairs that could clash.
- **Calendar check** — 23 people make 253 pairs, and the chance some pair matches is 50.7%.
- **One specific person** — with those same 23 people, the chance anyone matches Alice is 5.9%.
- **The gap widens** — 50 people reach 97.0% for some pair, still only 12.6% for Alice.
- **Near certainty** — by 70 people a match is 99.9% likely, yet Alice is still under one in five.

**Example:** It takes 254 people before someone probably shares Alice's date, but only 23 before some pair shares a date.

**Key point:** Intuition budgets for one item's risk; the system pays for every pair's risk at once.

**Chart c1 — collision curve against group size**
- X axis: number of people, 1 to 70. Y axis: probability 0 to 100%.
- Magenta curve: chance some pair matches, exact product 1 − ∏(365−k)/365, computed in the draw loop.
- Mute/blue shallow reference curve: chance someone matches one specific person, 1 − (364/365)^(n−1).
- Dashed orange guides at the 50% line and at the group size where the magenta curve first crosses it; label prints that size and the computed percentage.
- Small gray parenthetical near the shallow curve giving its value at the same group size.
- Caption: some pair matching is the common case long before your item does.

---

## Section 2 — A 32-Bit Random ID Duplicates After Seventy-Seven Thousand Rows

**Tags:** blue "hashes and IDs" · green "sizing rule" · red "32-bit is not enough"

**Bullets**
- **The reassuring number** — a 32-bit space holds about 4.29 billion values, which sounds inexhaustible.
- **The real number** — random 32-bit IDs hit a 50% chance of a duplicate after about 77,163 of them.
- **Already visible earlier** — at just 10,000 such IDs the chance of a duplicate is already 1.2%.
- **The half-way rule** — the coin-flip point sits near √(2·N·ln 2), which is about 1.1774·√N.
- **Square-root scaling** — every extra bit of ID width buys only about 41% more safe items.
- **48-bit** — safe to roughly 19.8 million items, which many production tables pass in a month.
- **64-bit** — coin-flip point about 5.06 billion, so a billion-row table already carries 2.7% risk.
- **128-bit** — coin-flip point past twenty quintillion items, which no ordinary table approaches.

**Example:** A billion random IDs collide with probability 2.7% at 64 bits and about six in a trillion at 96 bits.

**Key point:** Judge an ID width against the square root of its space, never against the space itself.

**Chart c2 — safe item count by ID width**
- Horizontal bars for 32, 48, 64 and 128 bits; bar length = log2 of the computed coin-flip point.
- Value computed in the draw function as Math.sqrt(2 * Math.pow(2, b) * Math.LN2), printed beside each bar in plain units (thousand / million / billion / scientific).
- Vertical violet reference line at a billion items (log2 ≈ 29.9), labelled.
- Bars left of the line drawn magenta (unsafe at that scale), bars right of it green.
- Caption: the axis is doubling-steps, so a bar twice as long means a space vastly larger.

---

## Section 3 — Two Different PDFs, One SHA-1 Fingerprint

**Tags:** blue "documented case" · orange "cryptographic hashes" · red "2017"

**Bullets**
- **What happened** — researchers built two different PDF files sharing one SHA-1 fingerprint.
- **Who** — Stevens, Bursztein, Karpman, Albertini and Markov at Google and CWI Amsterdam, 2017.
- **The output width** — SHA-1 emits 160 bits, so its pair-counting ceiling sits near 2^80 tries.
- **What the ceiling means** — 2^80 is the cost of finding a clash by blind search, nothing better.
- **What they paid** — a structural shortcut in SHA-1 found one for about 2^63.1 of work.
- **The margin lost** — that is roughly 122,000 times cheaper than the pair-counting ceiling.
- **The honest reading** — the pair-counting bound is an upper limit on safety, not a promise of it.
- **The consequence** — Git began moving object naming off SHA-1 once cheap clashes were real.

**Example:** A 160-bit hash promises about 2^80 work to break by luck alone; SHAttered got there for 2^63.1.

**Key point:** Pair counting tells you the best case for an attacker who has no insight; real breaks come in under it.

**Source note:** Stevens, Bursztein, Karpman, Albertini, Markov, "The first collision for full SHA-1", CRYPTO 2017 (shattered.io).

**Chart c3 — work required, in doubling steps**
- Horizontal axis of doublings of work, 40 to 170.
- Magenta band left of 2^63.1 = the cost the attack actually paid; green band from 2^63.1 to the ceiling = the margin the design implied; mute band from the ceiling to 2^160 = out of reach for anyone.
- Three staggered ticks: attained cost (2^63.1), pair-counting ceiling (computed as 160/2), full blind search (2^160).
- Orange bracket between attained and ceiling printing the computed ratio 2^(80−63.1) ≈ 122,294×.
- Caption: the ceiling was the best case for luck alone; insight came in far under it.

---

## Section 4 — A Counter Never Collides, and Enough Bits Never Will

**Tags:** blue "the boundary" · green "when it is safe" · orange "coordinated allocation"

**Bullets**
- **The trap needs randomness** — items must be thrown independently into a space nobody coordinates.
- **A counter is immune** — a single sequence generator hands out each value once, so pairs never clash.
- **Any coordinator works** — a central allocator, or per-writer ranges, removes the guesswork entirely.
- **Uniqueness checks work** — a database constraint converts a silent duplicate into a loud retry.
- **Non-uniform spaces are worse** — a skewed hash concentrates items, so clashes come even sooner.
- **The sizing rule** — for n items and a target risk p, you need a space of about n²/(2p) values.
- **Worked size** — a billion rows at one-in-a-billion risk needs about 5×10^26 values, near 89 bits.
- **What to pick** — 96 or 128 bits clears that comfortably; 64 bits does not.

**Example:** A billion-row table wanting one-in-a-billion duplicate risk needs about 89 bits, so 96 is the smallest sane width.

**Key point:** Either coordinate the allocation and the risk is zero, or size the space from n²/(2p) and stop guessing.

**Chart c4 — bits required for a billion items**
- X axis: target risk on a log scale from 1e-2 down to 1e-18.
- Aqua curve: bits needed = log2(n²/(2p)) with n = 1e9, computed in the draw loop.
- Horizontal dashed reference lines at 64, 96 and 128 bits, labelled.
- Marker where the curve crosses one-in-a-billion risk; label prints the computed bit count.
- Mute annotation: a counter sits off this chart entirely, needing only enough bits to count to n.
- Caption: risk falls fast in bits, so buying width is cheap and guessing is not.

**Source note:** Illustrative Example (the sizing figures are computed, not measured).
