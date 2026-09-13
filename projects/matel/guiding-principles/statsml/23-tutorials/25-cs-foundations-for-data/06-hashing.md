# Hashing

**Page type:** detail page (tutorial page: 4 card-sections, each an h2 + two-column layout table, text left 50%, canvas right 50%)
**HTML title tag:** Hashing

**Subtitle:** A function turns "alice@example.com" into bucket 7 — the same bucket every single time — and that one trick powers instant lookup, dedup, and fair A/B splits

## One Email In, Bucket 7 Out — Every Time

Tags: `core idea` (blue), `running example` (green)

- **The trick** — a hash function turns any input into a number in a fixed range
- **Our example** — h("alice@example.com") = bucket 7, today, tomorrow, on any machine
- **Deterministic** — same input, same output; the function remembers nothing
- **Spread** — different inputs scatter roughly evenly across the buckets
- **Fast** — one pass over the characters; no searching through stored data

*Example:* Feed it "alice@example.com" a million times and it answers 7 a million times.

**Key point:** A hash function is a deterministic scrambler — same input, same bucket, every time. That one property powers everything below.

### Visualization (canvas `c1`, 720×300)

Flow diagram: five emails on the left flow through a central "h( )" function box into ten buckets on the right.

- **Title (bold 15px, ink `#1a5276`, top center):** "One function, ten buckets: h(email) → bucket".
- **Function box:** at x=290, y=118, 90×54, fill rgba(42,120,214,0.10), stroke ink `#1a5276` width 2, bold 16px centered label "h( )".
- **Buckets:** ten 100×20 boxes stacked at x=590 from y=48 with 4px gaps, each with right-aligned 12px label "bucket 0" … "bucket 9". Bucket 7 highlighted: fill rgba(42,120,214,0.18), blue `#2a78d6` stroke width 2. Occupied buckets 3, 4, 6 filled rgba(107,114,128,0.10); empty buckets white with `#c8ced6` stroke.
- **Emails (left, monospace 12px, listed at 40px vertical spacing):** "alice@example.com" (bold, blue `#2a78d6`, → bucket 7), "bob@example.com" (mute `#6b7280`, → 4), "carol@example.com" (mute, → 6), "dave@example.com" (mute, → 3), "erin@example.com" (bold, magenta `#d55181`, → 7). Each email connects with a two-segment line (email → function box → its bucket) in its own color; bold entries use line width 2.5, others 1.2.
- **Annotations:** blue bold 13px centered at (335, 288): "same input → same bucket, every time"; magenta 12px left-aligned at (590, 288): "alice + erin share 7".

## A Toy Hash You Can Compute by Hand

Tags: `worked example` (green)

- **The recipe** — add up the character codes, divide by 10, keep the remainder
- **alice@example.com** — codes sum to 1687; 1687 mod 10 = 7 → bucket 7
- **The others** — bob sums to 1484 → 4; carol 1706 → 6; dave 1593 → 3
- **erin@example.com** — sums to 1607 → bucket 7: the same bucket as alice
- **Collision** — two inputs sharing a bucket is normal; each bucket keeps a short list

*Example:* With 5 emails and 10 buckets a shared bucket is expected — the table just checks the short list inside.

**Key point:** Collisions are not bugs — they are planned for. Real hash functions just make them rare and spread them evenly.

### Visualization (canvas `c2`, 720×300)

Arithmetic table plus a bucket strip.

- **Title (bold 15px, `#1a5276`, top center):** "Toy hash: sum of character codes, keep the last digit".
- **Table:** header row (bold 12px mute `#6b7280`) with columns "email" (left at x=40), "sum of codes" (right-aligned at x=430), "mod 10" (right-aligned at x=540), underlined by a grid-gray `#e5e9ef` rule. Five monospace 12px rows (row colors: alice blue `#2a78d6`, bob/carol/dave mute `#6b7280`, erin magenta `#d55181`); mod-10 result in bold 13px monospace:
  - alice@example.com — 1687 — 7
  - bob@example.com — 1484 — 4
  - carol@example.com — 1706 — 6
  - dave@example.com — 1593 — 3
  - erin@example.com — 1607 — 7
- **Bucket strip (bottom, y=218, ten 60×44 cells from x=40 at 64px pitch):** cell index 0–9 labeled above in mute 12px; occupants in 11px text — bucket 3: "dave"; bucket 4: "bob"; bucket 6: "carol"; bucket 7: "alice", "erin". Bucket 7 highlighted magenta (fill rgba(213,81,129,0.12), magenta stroke width 2); other occupied cells fill rgba(42,120,214,0.10); empty cells white, stroke `#c8ced6`.
- **Caption (magenta bold 13px, bottom center):** "1687 mod 10 = 7 and 1607 mod 10 = 7 — a collision, held as a short list".

## Where a Data Scientist Meets It Daily

Tags: `where it's used` (blue), `rule of thumb` (green)

- **Dictionary lookup** — a python dict hashes the key straight to its bucket, no scan
- **Dedup** — hash every row; equal hashes flag likely duplicates without all-pairs compares
- **A/B assignment** — `hash(user_id) mod 100 < 50` → variant A; fair and sticky
- **Sharding** — `hash(key) mod #servers` decides which machine stores the row
- **Stickiness** — same group on every visit and server — if the hash is deterministic (md5/murmur)

*Example:* hash("user_4217") mod 100 = 32, so user 4217 sees variant A on every page load, forever (illustrative).

**Key point:** Hashing gives a random-looking but perfectly repeatable split — that is why experiments assign users by hash, not by coin flip. Use a deterministic hash like md5 — Python's built-in `hash()` is randomized per process.

### Visualization (canvas `c3`, 720×300)

A/B assignment band: a 0–100 hash axis split at 50 with user dots scattered on it.

- **Title (bold 15px, `#1a5276`, top center):** "hash(user_id) mod 100 — below 50 → variant A".
- **Band:** rectangle at x=60, y=150, width 600, height 44; left half filled rgba(42,120,214,0.14), right half rgba(0,131,0,0.12), outer stroke `#c8ced6`. Below the halves, bold 13px centered labels: "variant A" (blue `#2a78d6`) and "variant B" (green `#008300`).
- **Threshold:** vertical dashed (5/4) ink `#1a5276` line width 2 at the midpoint, labeled bold 12px "cut at 50" above.
- **Axis ticks:** mute 12px labels 0, 25, 50, 75, 100 under the band.
- **User dots (positions illustrative), 5px radius, id labels 11px alternating above/below the band:** u1: 32 (highlighted orange `#d95926`, 7px radius), u2: 87, u3: 5, u4: 64, u5: 71, u6: 18, u7: 49, u8: 93, u9: 56, u10: 11. Dots below 50 blue, at/above 50 green.
- **Annotations:** orange bold 13px centered: "user_4217 → 32 → A on every visit, on every server"; mute 11px centered below: "user positions illustrative".

## The Confusion: Hashing Is Not Encryption

Tags: `common mistake` (orange)

- **One-way** — a hash has no key and no decoder; you cannot compute the email back from 7
- **But guessable** — anyone can hash a list of candidate emails and match your value
- **Not anonymization** — hashed emails are pseudonyms, not erased identities
- **Encryption** — two-way by design: a key locks, the key unlocks, the original returns
- **Rule** — hash to look things up or split traffic; encrypt to hide content

*Example:* A "hashed" email column can be re-identified by hashing a public email list and matching values.

**Key point:** Hashes are one-way street signs, not locked boxes — treat hashed IDs as identifiers, never as anonymized data.

### Visualization (canvas `c4`, 720×300)

Split-panel diagram: one-way hashing on the left vs two-way encryption on the right; vertical dashed gray divider (`#bdc3c7`, dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One-way street vs locked box".
- **Left panel** — heading bold 13px blue `#2a78d6` centered at x=180: "Hashing: one-way". Two boxes (fill `#f8f9fa`, 12px monospace centered labels): "alice@example.com" (blue stroke, 175×32) and "a9f3…07" (violet `#4a3aa7` stroke, 90×32), connected by a blue forward arrow. Below: a mute reverse arrow crossed out with a magenta `#d55181` X (width 2.5), captioned mute 11px "no key, no way back". Orange `#d95926` 11px two-line note: "but: hash a guess list and compare —" / "hashed IDs are pseudonyms, not anonymous".
- **Right panel** — heading bold 13px green `#008300` centered at x=540: "Encryption: two-way with a key". Two boxes: "meet at 6pm" (green stroke, 110×32) and "Xk29…Qz" (violet stroke, 110×32), with a green forward arrow labeled 11px green "+ key locks" and an aqua `#199e70` return arrow labeled "+ key unlocks". Text 11px `#2c3e50`: "the original comes back exactly".
- **Bottom line (ink bold 13px centered):** "hash = street sign you cannot walk backwards; encryption = box you can reopen".

## Regeneration instructions

- **Template/layout:** tutorial detail page (per `tutorials/CLAUDE.md`, social-graph reference skeleton). Page = `<h1>` + `.subtitle` paragraph, then 4 `.card-section` blocks. Each `.card-section` has an `<h2>` (1.3rem, `#1a5276`, bottom border `2px solid #2980b9`) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `<td class="text-col">` (50%) and right `<td class="viz-col">` (50%), cell padding 12px, vertical-align top.
- **Text column structure:** `.tags` pill row first, then a `<ul>` of 5 bullets each opening with `<b>bold term</b>` (bold terms colored `#1a5276`), one italic `.example` paragraph (`#555`, 0.9rem), one `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem) with a `<strong>` "Key point:" prefix.
- **Tag pills:** inline-block, 0.72rem, weight 600, padding 2px 10px, radius 10px. Colors — blue: bg rgba(26,82,118,0.12) / `#1a5276`; green: bg rgba(39,174,96,0.15) / `#27ae60`; red: bg rgba(231,76,60,0.12) / `#e74c3c`; orange: bg rgba(230,126,34,0.15) / `#e67e22`.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `2px solid #2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<code>` in ui-monospace on `#f4f6f8`. No nav bar, no back/home links.
- **Canvases:** 720×300 intrinsic, CSS `width:100%`, border `1px solid #e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- Grid cards elsewhere linking here use `.html` extensions in regenerated HTML.
