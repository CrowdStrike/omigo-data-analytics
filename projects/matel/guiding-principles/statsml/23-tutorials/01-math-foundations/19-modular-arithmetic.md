# Modular Arithmetic

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Modular Arithmetic

**Subtitle:** Clock math: count past the end of the dial and you wrap back to the start — the remainder is the answer, and it powers hashing, sharding, and crypto

## A 5-Hour Job Starting at 9 O'Clock

**Tags:** `core idea` (blue), `wrap-around` (green), `remainder` (orange)

- **The clock** — a job starts at 9 and takes 5 hours; it ends at 2, not at 14, because the dial wraps at 12
- **The wrap** — whenever a count passes 12 it starts over from 0, so only the leftover past full laps matters
- **The name** — "14 mod 12 = 2" just says: divide 14 by 12, throw away the laps, keep the remainder 2
- **Same spot** — 14, 26, and 38 all leave remainder 2 when divided by 12, so all three land on 2
- **Finite world** — mod 12 squeezes every whole number, however big, onto just 12 dial positions

*Example (italic):* A 14:00 departure reads as 2 pm — you already compute mod 12 every time you read a train timetable.

**Key point:** "x mod n" is the remainder after dividing x by n — clock arithmetic on a dial with n positions. Everything else on this page is just choosing a useful n.

### Visualization (canvas `c1`, 720×300)

Clock face (left) showing 9 + 5 wrapping past 12 to land on 2, with the remainder arithmetic written out as a text column on the right.

- **Title (bold 15px, `#1a5276`, top center):** "9 + 5 = 14, but the Dial Says 2".
- **Dial:** circle center (185, 172), radius 100, `#999` 2px stroke; hour numbers 1–12 placed at radius r−18 (12px `#666`, except 9 in bold 14px blue `#2a78d6` and 2 in bold 14px green `#008300`); short tick marks `#bbb` 1px from radius r−6 to r at each hour.
- **Start/end dots:** 6px filled dots at radius r−42 — blue `#2a78d6` at the 9 o'clock angle (π), green `#008300` at the 2 o'clock angle (11π/6).
- **Wrap arc:** orange `#d95926` 3px clockwise arc at radius r−42 from 9 around past 12 to 2, ending in a filled orange arrowhead (built from the tangent at the 2 o'clock end, ~12px long).
- **Dial labels (bold 12px, centered):** blue "start: 9" at (cx−r−2, cy−14); green "end: 2" at (cx+r−12, cy−44); orange "+5 hours, wrapping past 12" at (cx, cy+r+20).
- **Right column (left-aligned at x=400):** heading bold 13px `#444` "the arithmetic" (y=70); 14px `#2c3e50` lines "9 + 5 = 14   (runs past the dial)" (y=100) and "14 = 1 × 12 + 2   (one lap, 2 left over)" (y=128); green bold 17px "14 mod 12 = 2" (y=162); bold 13px `#444` "same remainder, same spot:" (y=205); aqua `#199e70` bold 14px "14, 26, 38 all mod 12 = 2" (y=232); mute `#6b7280` 12px "every whole number lands on one of 12 spots" (y=258).

## Dealing Order IDs Onto 4 Shards

**Tags:** `worked example` (blue), `sharding` (green)

- **The setup** — a store splits its orders table across 4 database shards, numbered 0 to 3
- **The rule** — shard = order_id mod 4, so ID 17 goes to shard 1 because 17 = 4×4 + 1
- **Check by hand** — 22 mod 4 = 2, 35 mod 4 = 3, 40 mod 4 = 0; the remainder is the address
- **No lookup table** — any machine recomputes the shard from the ID alone, no directory needed
- **Even spread** — the 8 sample IDs land exactly 2 per shard; remainders balance load for free

*Example (italic):* Order 53 arrives: 53 = 13×4 + 1, remainder 1 — write it to shard 1, no coordination needed.

**Key point:** The remainder is a free, deterministic address: same ID in, same shard out, on any machine — for as long as the divisor n never changes.

### Visualization (canvas `c2`, 720×300)

Flow diagram: a left-hand list of 8 order IDs with their mod-4 remainders, connected by colored lines to 4 shard boxes on the right.

- **Title (bold 15px, `#1a5276`, top center):** "shard = order_id mod 4".
- **Data:** ids `[17, 22, 35, 40, 53, 66, 71, 84]`; remainders `[1, 2, 3, 0, 1, 2, 3, 0]`; shard colors `[#2a78d6 (blue), #008300 (green), #d95926 (orange), #4a3aa7 (violet)]` indexed by shard number; shard contents `[[40, 84], [17, 53], [22, 66], [35, 71]]` for shards 0–3.
- **Left list (8 rows at y = 62 + i×27):** ID bold 13px `#2c3e50` right-aligned at x=105; "mod 4 =" 12px mute `#6b7280` at x=115; remainder digit bold 13px in its shard's color at x=172.
- **Shard boxes (right):** four 200×44 rectangles at x=470, vertical centers y = 82 + shard×56; 2px stroke in the shard color; inside, "shard N" bold 13px in the shard color and "IDs a, b" 13px `#2c3e50`.
- **Connecting lines:** from (185, row y) to (470, shard center y), 1.5px in the shard color at globalAlpha 0.55.
- **Takeaway (bold 13px green `#008300`, centered at y=290):** "8 IDs, 2 per shard — the remainder is the address".

## The Same Trick Behind Hash Tables and Crypto

**Tags:** `where it's used` (blue), `hashing` (green), `crypto` (orange)

- **Hash buckets** — a hash turns "alice@ex.com" into 2,183,479; mod 10 maps it to bucket 9
- **Last digit** — mod 10 is literally the last digit, which is why bucket 9 is instant to read off
- **Hash % N** — every hash table you have used stores keys at index hash(key) mod capacity
- **Checksums** — card numbers and ISBNs validate themselves with a mod-10 or mod-11 check digit
- **Crypto** — RSA multiplies huge numbers mod n; the wrap-around is what makes undoing it hard

*Example (italic):* hash("bob@ex.com") = 907,215 → bucket 5; hash("carol@ex.com") = 448,802 → bucket 2 (illustrative hashes).

**Key point:** Mod turns an unbounded number into a bounded address. Hashing, sharding, and much of cryptography are that one move applied at different scales.

### Visualization (canvas `c3`, 720×300)

Pipeline table (key → hash → mod 10 → bucket) above a row of 10 bucket boxes, with the three occupied buckets highlighted in each key's color.

- **Title (bold 15px, `#1a5276`, top center):** "bucket = hash(key) mod 10 — the Last Digit Is the Address".
- **Data:** keys `['alice@ex.com', 'bob@ex.com', 'carol@ex.com']`; hash heads `['2,183,47', '907,21', '448,80']` with highlighted last digits `['9', '5', '2']`; buckets `[9, 5, 2]`; row colors `[#2a78d6 (blue), #008300 (green), #d95926 (orange)]`.
- **Column headers (bold 12px mute `#6b7280`, y=52, left-aligned):** "key" at x=60, "hash(key)" at x=290, "mod 10" at x=460, "bucket" at x=560.
- **Rows (y = 82 + i×34):** key 13px `#2c3e50` at x=60; mute "→" at x=250; hash head 13px `#2c3e50` at x=290 with the last digit appended immediately after (measured via `measureText`) in bold 14px of the row color; mute "→" at x=470; bucket digit bold 15px in the row color at x=575.
- **Bucket row:** 10 boxes 56px wide × 40px tall with 4px gaps, starting x=60, y=200; occupied buckets get a 0.2-alpha fill and 2px stroke in the owner's color plus the key's local part (before the @) in bold 11px centered; empty buckets a plain 1px `#ccc` stroke; bucket digits 0–9 in 12px `#666` centered 18px below each box.
- **Caption (12px mute `#6b7280`, centered at y=290):** "hash values illustrative — mod 10 keeps only the last digit".

## Change n and Everything Moves

**Tags:** `common mistake` (red), `resharding` (orange)

- **Adding a shard** — switching from mod 4 to mod 5 reassigns 6 of the 8 sample IDs
- **Why** — remainders by 4 and by 5 are unrelated: 17 mod 4 = 1 but 17 mod 5 = 2
- **The cost** — naive hash % N resharding forces a near-total data migration overnight
- **The fix** — consistent hashing exists precisely to avoid re-dealing every key on resize
- **Negatives** — many languages return -7 % 12 = -7, but clock math says 5; guard your indexes

*Example (italic):* A team grew a cache from 4 to 5 nodes using plain hash % N and watched the hit rate collapse overnight.

**Common mistake:** Assuming mod addresses are stable when n changes. They are stable only for a fixed n — change the divisor and almost every key gets a new home.

### Visualization (canvas `c4`, 720×300)

Two rows of 8 ID chips — the same IDs assigned by mod 4 (top) and mod 5 (bottom) — with "moved" flags under every chip whose shard changed.

- **Title (bold 15px, `#1a5276`, top center):** "Growing 4 Shards to 5: Almost Every Key Moves".
- **Data:** ids `[17, 22, 35, 40, 53, 66, 71, 84]`; mod-4 remainders `[1, 2, 3, 0, 1, 2, 3, 0]`; mod-5 remainders `[2, 2, 0, 0, 3, 1, 1, 4]`; shard colors `[#2a78d6, #008300, #d95926, #4a3aa7, #d55181]` (blue, green, orange, violet, magenta); matching chip fills `['rgba(42,120,214,0.16)', 'rgba(0,131,0,0.13)', 'rgba(217,89,38,0.13)', 'rgba(74,58,167,0.13)', 'rgba(213,81,129,0.13)']`.
- **Chips:** 70×44 rectangles at x = 45 + i×80; fill and 2px stroke in the assigned shard's color; ID bold 13px `#2c3e50` centered at chip y+19; shard tag "sN" bold 12px in the shard color at chip y+36.
- **Top row:** heading bold 13px `#444` left-aligned at (45, 58) "4 shards:  shard = id mod 4"; chips at y=68, no moved flags.
- **Bottom row:** heading at (45, 158) "5 shards:  shard = id mod 5"; chips at y=168; every chip where rem4 differs from rem5 gets red `#e74c3c` bold 11px "moved" centered at chip y+60.
- **Takeaway (bold 13px red `#e74c3c`, centered at y=262):** "6 of 8 keys changed shards — only 22 and 40 stayed put".
- **Caption (12px mute `#6b7280`, centered at y=285):** "remainders by 4 and by 5 are unrelated: 17 mod 4 = 1 but 17 mod 5 = 2".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
