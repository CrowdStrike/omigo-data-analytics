# Rainbow Tables

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Rainbow Tables

**Subtitle:** An attacker can compute hash→password answers once and reuse them against every breach forever — a random per-user salt is the defense that makes all that precomputed work worthless

## The Table Built Before the Breach

**Tags:** `core idea` (blue), `precomputation` (orange), `defensive` (green)

- **The breach** — an attacker steals a password database and finds hashes, not passwords
- **The giveaway** — the same password always hashes to the same value, in every unsalted database
- **The shortcut** — so hash→password answers can be computed once, offline, before any breach
- **The reuse** — one precomputed table then cracks breach after breach with instant lookups
- **The name** — rainbow tables are the classic space-saving form of this precomputation

*Example (italic):* An attacker spends 20 hours building a table once; every unsalted breach after that falls in about 4 seconds per lookup.

**Key point:** A precomputation attack moves the expensive work to before the breach — pay the hashing cost once, and every stolen unsalted database becomes a lookup, not a computation.

### Visualization (canvas `c1`, 720×300)

Bar chart of attacker effort: one tall one-time "build the table" bar, then three near-zero bars for lookups against three separate breaches.

- **Title (bold 15px, `#1a5276`, top center):** "Precompute Once, Crack Forever: Attacker Effort Per Breach".
- **Axes:** baseline 2px `#999` at y=245 from x=60 to x=680; no y gridlines (heights carry labels).
- **Bars (90px wide, bottoms on the baseline), left to right:**
  - x=110: orange `#d95926` bar height 170, bold 12px label "20 hrs" above, 12px `#444` label "build table (once)" below
  - x=260: red `#e74c3c` bar height 6, bold 12px red label "~4 sec" above, label "breach A" below
  - x=410: red bar height 6, "~4 sec", "breach B"
  - x=560: red bar height 6, "~4 sec", "breach C"
- **Annotation (bold 13px red `#e74c3c`, centered near y=110 over the three lookup bars):** "the same table, every breach, forever".
- **Caption (12px `#444`, bottom right):** "hours and seconds illustrative".

## A Million Passwords in a Thousand Rows

**Tags:** `worked example` (blue), `hash/reduce chains` (green)

- **The chain** — start from a password, hash it, then a "reduce" step maps the hash to a new password
- **The walk** — hash, reduce, hash, reduce: 1,000 steps make a chain that touches 1,000 passwords
- **The trick** — store only each chain's first and last entry; the middle can be recomputed on demand
- **The count** — 1,000 stored chains × 1,000 steps each ≈ 1,000,000 passwords covered in 1,000 rows
- **The rainbow twist** — a different reduce step per column keeps chains from merging mid-walk
- **The trade** — a lookup recomputes chains, up to ~500,000 hash steps: time traded for storage

*Example (italic):* A full lookup table for 1,000,000 passwords needs 1,000,000 rows; the rainbow table covers the same space in 1,000 rows — a 1,000× storage saving.

**Key point:** A rainbow table is a compressed precomputed dictionary — chains of hash/reduce steps keep two entries per 1,000 passwords, trading lookup time for storage so one table spans a huge password space.

### Visualization (canvas `c2`, 720×300)

Top half: flow diagram of one hash/reduce chain with only its endpoints stored. Bottom half: two schematic bars comparing full-table rows vs rainbow-table rows.

- **Title (bold 15px, `#1a5276`, top center):** "One Chain, 1,000 Steps — Store Only the Ends".
- **Chain row (boxes centered on y=95):** rounded boxes 34px tall, 8px radius, 12px `#2c3e50` text, connected by 2px `#6b7280` arrows labeled 11px "H" or "R" above:
  - green `rgba(0,131,0,0.12)` box at x=15 width 110 "example-string" (stored), arrow "H" to blue `rgba(42,120,214,0.15)` box at x=165 width 60 "7f3a…", arrow "R" to blue box at x=265 width 120 "example-string-2", arrow "H" to blue box at x=425 width 60 "c91d…", dashed `#6b7280` arrow labeled "… 1,000 steps …" to green box at x=575 width 120 "example-string-N" (stored)
  - bold 11px green `#008300` "stored" under the first and last boxes; 11px `#6b7280` "recomputed at lookup" under the middle boxes
- **Storage bars (14px tall, left labels 12px `#444` at x=20):**
  - y=200: "full table — 1,000,000 rows": blue fill `rgba(42,120,214,0.30)` bar from x=230 width 440, 11px width label at bar end
  - y=240: "rainbow table — 1,000 rows": green `#008300` solid bar from x=230 width 40, 11px label "1,000× smaller" at bar end
- **Caption (12px `#444`, bottom right):** "hash values invented, pixel widths schematic — row counts exact for the example".

## Two Users, One Password, Two Hashes

**Tags:** `where it's used` (blue), `the salt defense` (green)

- **The salt** — a random per-user value, generated at signup and stored right beside the hash
- **The mix** — the system hashes salt+password together, so the salt changes the output completely
- **Two users** — alice and bob both pick "example-string", but their salts differ, so their hashes differ
- **The kill** — the attacker's table was built without any salt, so it holds no matching entries
- **Built in** — modern password hashes (bcrypt, argon2) generate and embed a salt automatically

*Example (italic):* Unsalted, alice and bob both store c4d1…9e and one lookup cracks both; with salts a91f and 7c3e they store 72e8…41 and d05b…c7 — unrelated values.

**Key point:** A salt makes every user's hash unique even for identical passwords, so precomputed tables become useless and each account must be attacked separately, from scratch.

### Visualization (canvas `c3`, 720×300)

Two-band flow diagram: the same password "example-string" for alice and bob, hashed without salt (identical outputs, cracked) vs with per-user salts (unrelated outputs, table useless).

- **Title (bold 15px, `#1a5276`, top center):** "Same Password, With and Without Salt".
- **Band labels (12px `#444` at x=20):** "no salt" centered on y=95; "per-user salt" centered on y=215; thin `#e5e9ef` divider line across at y=155.
- **Box style:** rounded 8px radius, 30px tall, 12px `#2c3e50` text; password boxes blue `rgba(42,120,214,0.15)`, hash boxes red `rgba(231,76,60,0.12)` in band 1 and green `rgba(0,131,0,0.12)` in band 2; 2px `#6b7280` arrows between.
- **Band 1 (no salt):** alice row y=75: box at x=130 "alice · example-string" → box at x=400 "c4d1…9e"; bob row y=118: box "bob · example-string" → box "c4d1…9e"; bold 12px red `#e74c3c` at x=560, y=95: "identical — one table lookup cracks both".
- **Band 2 (salted):** alice row y=195: box at x=130 "alice · salt a91f" → box at x=400 "72e8…41"; bob row y=238: box "bob · salt 7c3e" → box at x=400 "d05b…c7"; bold 12px green `#008300` at x=560, y=215: "unrelated — table has no entry".
- **Caption (12px `#444`, bottom right):** "hash values are made-up placeholders".

## The Salt Sits in Plain Sight

**Tags:** `common mistake` (red), `salt ≠ secret` (orange)

- **The confusion** — assuming a salt must be hidden like a key; it is stored in plaintext next to the hash
- **The job** — a salt's job is uniqueness, not secrecy: it only has to differ for every user
- **Visible and fine** — a bcrypt output string literally contains its salt, readable by anyone
- **The real slip** — one shared salt for all users recreates the problem: one rebuilt table cracks everyone
- **The pairing** — salts defeat precomputation; slow hashes (bcrypt/argon2 cost factors) defeat per-account guessing

*Example (italic):* An engineer hides one site-wide salt in a config file; an attacker who reads it builds a single new table and cracks all 10,000 accounts at once.

**Common mistake:** Treating the salt as a secret — and then "protecting" it with shortcuts like a single global salt. A salt can sit in plain sight; what it must be is different for every user.

### Visualization (canvas `c4`, 720×300)

Top strip: a mock database row showing the salt stored openly. Below: horizontal bars comparing how many separate attacks a 10,000-account database forces under each salting choice.

- **Title (bold 15px, `#1a5276`, top center):** "Separate Attacks Forced on a 10,000-Account Database".
- **DB row strip (y=60):** three rounded boxes side by side starting at x=150, 30px tall, 12px `#2c3e50` text: blue `rgba(42,120,214,0.15)` "user: alice", orange `rgba(230,126,34,0.15)` "salt: a91f (plaintext)", blue "hash: 72e8…41"; 11px `#6b7280` label "stored side by side — and that's fine" to the right.
- **Bars (14px tall, baseline x=230, left labels 12px `#444` at x=20):**
  - y=140: "no salt": red `#e74c3c` bar width 8, 11px red label "1 old table cracks all"
  - y=190: "one shared salt": red bar width 8, 11px red label "1 rebuilt table cracks all"
  - y=240: "per-user salts": green `#008300` bar width 440, 11px green label "10,000 separate attacks"
- **Annotation (bold 13px green `#008300`, right side near y=115):** "per-user salts multiply attacker work 10,000×".
- **Caption (12px `#444`, bottom right):** "pixel widths schematic, account count illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points and box texts are the hardcoded literals above (no randomness); build hours, lookup seconds, hash strings, salts, and account counts are invented and labeled illustrative; the chain arithmetic (1,000 chains × 1,000 steps ≈ 1,000,000 passwords, 1,000× storage saving) is exact for the stated example, and text numbers match chart numbers.
- **Framing:** defensive/educational — the page explains precomputation attacks only to motivate why salts exist and how modern password hashing (bcrypt/argon2) builds the defense in.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: session ids, cookie values, tracking ids, token parts, and example passwords on this page are made-up placeholders — for illustration only, and to avoid false positives from secret scanners."
