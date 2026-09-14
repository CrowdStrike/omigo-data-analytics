# How One Breach Becomes Every Breach

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** How One Breach Becomes Every Breach

**Subtitle:** Follow one stolen customer table all the way to somebody's email account — copied out, cracked, merged into a shared list, then replayed at sites that were never breached

## A Shop's Customer Table Walks Out the Door

**Tags:** `core idea` (blue), `the reuse chain` (orange), `defensive` (green)

- **The copy** — an online shop's customer table is copied out: an email column and a stored password field
- **The crack** — offline guessing turns that stored password field back into usable passwords for most rows
- **The merge** — recovered pairs are pooled with other breaches into one file, sold and reshared for years
- **The replay** — because people reuse passwords, those pairs get tried at sites that were never breached
- **The prize** — an email inbox is the top target, because it can reset the password on every other account
- **No relationship** — the shop and the inbox share no vendor and no system, only one reused password

*Example (italic):* Alice bought something from the shop once, years ago; the chain ends with a stranger reading her inbox, and nothing about the two accounts was ever connected except her password.

**Key point:** A breach travels in four hops — copy, crack, merge, replay. Company Y gets compromised because its customers reused the password that leaked at company X.

### Visualization (canvas `c1`, 720×300)

Two-row snake flow diagram: the five stages of the chain, from the stolen table to the opened inbox.

- **Title (bold 15px, `#1a5276`, top center):** "One Stolen Table, Four Hops, Somebody Else's Inbox".
- **Box style:** rounded 8px radius, 2px border, centered two-line 12px `#2c3e50` text at `y+22` and `y+40`.
- **Row 1 (y=68, height 56):** x=25 w=195 orange fill `rgba(217,89,38,0.15)` border `#d95926` — "1. shop's customer table" / "copied out"; x=262 w=195 yellow fill `rgba(201,133,0,0.15)` border `#c98500` — "2. stored password field" / "cracked offline"; x=499 w=196 violet fill `rgba(74,58,167,0.13)` border `#4a3aa7` — "3. merged with other" / "breaches into one list".
- **Row 2 (y=196, height 56):** x=262 w=195 blue fill `rgba(42,120,214,0.15)` border `#2a78d6` — "4. pairs replayed at" / "sites never breached"; x=499 w=196 magenta fill `rgba(213,81,129,0.15)` border `#d55181` — "5. email inbox opened" / "resets everything else".
- **Arrows (3px `#6b7280`, 10px heads):** (220,96)→(258,96); (457,96)→(495,96); (457,224)→(495,224).
- **Elbow arrow (3px `#6b7280`):** (597,124) down to (597,160), left to (359,160), down to (359,192) with a downward head.
- **Annotation (bold 13px violet `#4a3aa7`, left-aligned):** "no shared vendor," at (25,215) and "only a shared password" at (25,233).
- **Caption (12px `#444`, bottom right):** "chain schematic".

## 500,000 Rows and Three Ways They Were Stored

**Tags:** `worked example` (blue), `crack rates` (orange), `storage choice` (green)

- **The table** — 500,000 rows are copied out, each row an email address and a stored password field
- **Plaintext** — nothing to crack: 500,000 of 500,000 rows, 100%, are usable the moment the file lands
- **Fast unsalted hash** — a dictionary run recovers 425,000 of 500,000 rows, which is 85% of the table
- **Slow salted hash** — the same run recovers only 15,000 of 500,000 rows, which is 3% of the table
- **Reuse step** — take the fast-hash case: if 30% reused that password elsewhere, 425,000 × 0.30 = 127,500
- **Inbox step** — if 10% of those 127,500 used it at their email provider, that is 12,750 inboxes reachable
- **The lever** — storage choice alone moves recoverable rows from 500,000 down to 15,000, a 33-fold cut

*Example (italic):* The same theft either hands over 500,000 working passwords or 15,000 — and 12,750 reachable inboxes is 2.55% of the original 500,000 rows (crack and reuse rates illustrative, arithmetic exact).

**Key point:** What the stolen table looks like decides the damage: plaintext is instantly usable, a fast unsalted hash is mostly cracked, a slow salted hash mostly resists — then reuse rates turn survivors into accounts elsewhere.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: usable passwords recovered from the same 500,000-row table under three storage choices.

- **Title (bold 15px, `#1a5276`, top center):** "500,000 Stolen Rows: How Many Become Usable Passwords".
- **Baseline:** vertical 2px `#999` line at x=250 from y=70 to y=250; bars start at x=250 and 420px = 500,000 rows.
- **Rows (bar height 26, top edges y = 85, 150, 215), right-aligned 12px `#444` labels ending at x=240:** "plaintext field", "fast unsalted hash", "slow salted hash".
- **Bars from hardcoded values `[500000, 425000, 15000]` mapped to pixel widths `[420, 357, 13]`:** fills orange `rgba(217,89,38,0.35)`, yellow `rgba(201,133,0,0.35)`, green `rgba(0,131,0,0.50)` with 2px borders `#d95926`, `#c98500`, `#008300`.
- **Value labels (bold 12px in the border colour):** "500,000 (100%)" and "425,000 (85%)" right-aligned *inside* their bars ending at bar-end − 8; "15,000 (3%)" left-aligned outside at bar-end + 8.
- **Annotation (bold 13px ink `#1a5276`, x=250, y=258):** "×30% reuse = 127,500 pairs that open other sites".
- **Caption (12px `#444`, bottom right at y=290):** "crack and reuse rates illustrative; arithmetic exact".

## Four Dumps Merged Into One File That Never Expires

**Tags:** `combolists` (blue), `aggregation` (orange), `rule of thumb` (green)

- **The pooling** — the shop's 425,000 cracked pairs are appended to three other dumps of 310,000, 190,000, 75,000
- **The total** — 425,000 + 310,000 + 190,000 + 75,000 = 1,000,000 rows sitting in one merged file
- **Deduplication** — 820,000 distinct email addresses remain, so 180,000 rows repeated an address already present
- **The definition** — that merged, deduplicated file of email-and-password pairs is what is called a combolist
- **No expiry date** — it is copied endlessly, so closing the account at the shop does not remove the row
- **It only grows** — every later breach is appended, making the list larger and more accurate over time
- **The rule** — a password that leaked once must be treated as public forever, at every site it was used

*Example (italic):* Alice's row from the shop is still being traded years later, now sitting beside rows from three breaches she has never heard of (dump sizes illustrative).

**Key point:** A combolist aggregates many breaches into one searchable file. Breach data does not expire, it accumulates — which is why one leaked password stays usable indefinitely.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: four source dumps above a divider, the merged total and the deduplicated unique-email count below it.

- **Title (bold 15px, `#1a5276`, top center):** "Four Dumps Merged: 1,000,000 Rows, 820,000 Unique Addresses".
- **Scale:** bars start at x=200, bar height 18, and 400px = 1,000,000 rows.
- **Source rows (top edges y = 52, 87, 122, 157) from hardcoded values `[425000, 310000, 190000, 75000]` mapped to widths `[170, 124, 76, 30]`:** fills yellow `rgba(201,133,0,0.35)`, blue `rgba(42,120,214,0.35)`, aqua `rgba(25,158,112,0.35)`, violet `rgba(74,58,167,0.30)` with 2px borders `#c98500`, `#2a78d6`, `#199e70`, `#4a3aa7`; right-aligned 12px `#444` labels ending at x=190: "the shop (cracked)", "forum dump", "game site dump", "streaming dump".
- **Divider:** 1px `#e5e9ef` line from x=60 to x=680 at y=185.
- **Total rows (top edges y = 200, 235) from `[1000000, 820000]` mapped to widths `[400, 328]`:** merged bar fill `rgba(26,82,118,0.35)` border 2px `#1a5276`, label "merged combolist"; unique bar fill `rgba(213,81,129,0.30)` border 2px `#d55181`, label "unique email addresses".
- **Value labels (bold 12px in each bar's border colour, left-aligned at bar-end + 8):** "425,000", "310,000", "190,000", "75,000", "1,000,000", "820,000".
- **Annotation (bold 13px magenta `#d55181`, x=210, y=275):** "1,000,000 − 820,000 = 180,000 duplicate rows removed".
- **Caption (12px `#444`, bottom right at y=292):** "dump sizes illustrative".
- **Note:** the four source widths (170+124+76+30 = 400) sum exactly to the merged bar's width, matching the arithmetic in the text.

## "I Wasn't Breached, So I'm Fine"

**Tags:** `common mistake` (red), `the root account` (orange), `what to do` (green)

- **The claim** — "I was never breached, so I'm fine": the breach happens at a company, not at a person
- **Forgotten signup** — it is often a shop used once years ago that the customer no longer remembers joining
- **The partial fix** — changing the password at the breached shop closes that single door and nothing else
- **Still live** — the same password is still accepted at every other site where it was reused, unchanged
- **The reset tree** — the email inbox can trigger a password reset on nearly every account beneath it
- **Root first** — so the inbox deserves the strongest and most unique password, plus a second factor
- **What to do** — change the reused password everywhere it was used, starting with the email account

*Example (italic):* Alice resets her password at the shop that leaked and feels safe, while the old password still opens her forum, her store account, and — worst of all — her inbox.

**Common mistake:** Treating a breach as the breached company's problem. The exposed asset is the password itself, so the fix is every site that shares it — and the email account, which can reset all the others, comes first.

### Visualization (canvas `c4`, 720×300)

Tree diagram: the email inbox at the root, five accounts beneath it, with reset arrows and a per-account status row showing only the breached shop was fixed.

- **Title (bold 15px, `#1a5276`, top center):** "The Inbox Is the Root of the Reset Tree".
- **Root box:** x=260, y=48, 200×44, 8px radius, fill `rgba(213,81,129,0.15)`, 2px `#d55181` border, bold 13px `#2c3e50` centered text "email inbox".
- **Child boxes (y=175, height 44, width 118, at x = 21, 161, 301, 441, 581):** fill `rgba(42,120,214,0.13)`, 2px `#2a78d6` border, 8px radius, centered 12px `#2c3e50` labels "a bank", "the shop", "cloud files", "a forum", "a store".
- **Arrows:** 2px `#6b7280` straight lines from the root box bottom centre (360,92) to each child's top centre (x+59, 171), each with a 9px head; 11px `#6b7280` label "password reset link" left-aligned at (368,130).
- **Status row (bold 11px, centered under each child at y=235):** "still live" in `#e74c3c` for "a bank", "cloud files", "a forum", "a store"; "changed ✓" in `#008300` for "the shop".
- **Annotation (bold 13px ink `#1a5276`, centered at x=360, y=265):** "12,750 inboxes reachable — one reset opens the rest".
- **Caption (12px `#444`, bottom right at y=291):** "illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is used only for the "still live" alarm status.
- **Data:** every value is a hardcoded literal (no randomness anywhere). The 500,000-row table, the crack rates (100% / 85% / 3%), the 30% reuse rate, the 10% email-reuse rate, and the four dump sizes are invented and labeled illustrative; all arithmetic on top of them is exact and text numbers match chart numbers to the digit: 500,000 × 0.85 = 425,000; 500,000 × 0.03 = 15,000; 425,000 × 0.30 = 127,500; 127,500 × 0.10 = 12,750; 12,750 ÷ 500,000 = 2.55%; 500,000 ÷ 15,000 ≈ 33; 425,000 + 310,000 + 190,000 + 75,000 = 1,000,000; 1,000,000 − 820,000 = 180,000.
- **Scope:** this page is the mechanism of the whole chain — it deliberately does not re-teach the final replay hop (credential stuffing) or how password hashing works; the stored password field is described only by what a thief can do with it.
- **Credential hygiene:** no sample rows, no example password or hash strings, no `key=value` credential syntax — the stolen table is described by its columns ("email column", "stored password field") only. People are Alice/Bob; companies are generic ("an online shop", "a forum").
- **Framing:** defensive/educational — the chain is explained so a reader understands why an unrelated site's breach reaches their inbox, and what to change first; no operational attack guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
