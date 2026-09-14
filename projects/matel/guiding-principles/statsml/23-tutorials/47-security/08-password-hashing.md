# Password Hashing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Password Hashing

**Subtitle:** Never store a password — store a one-way hash of it, and make that hash deliberately slow: bcrypt's sluggishness is the feature that turns an attacker's 1-second crack into a 3-year slog

## The Site That Never Keeps the Password

**Tags:** `core idea` (blue), `one-way hash` (green), `defensive` (green)

- **The signup** — a cooking site must remember something about alice's password to check it later
- **Plaintext** — storing "example-string" directly means one stolen file exposes every account instantly
- **Encryption** — reversible storage needs a key, usually on the same server; a full breach reveals it
- **The hash** — store hash("example-string") = "9c2f…" instead; nothing on the server can reverse it
- **The login** — alice types her password, the server hashes it and compares hashes: match means login

*Example (italic):* At every login the site hashes what alice typed and compares strings — her actual password never touches the disk, so a breach hands out puzzles, not passwords.

**Key point:** Store a one-way hash and compare hashes at login — never plaintext, never reversible encryption. A thief with the database gets values that must be guessed, not read.

### Visualization (canvas `c1`, 720×300)

Three-band flow diagram: the same stolen database row under plaintext, reversible encryption, and one-way hashing, each flowing to what the thief gets.

- **Title (bold 15px, `#1a5276`, top center):** "Three Ways to Store a Password — Only One Survives a Breach".
- **Band labels (12px `#444` at x=20):** "plaintext" centered on y=85, "encrypted" on y=160, "hashed" on y=235; thin `#e5e9ef` divider lines across at y=122 and y=197.
- **Box style:** rounded 8px radius, 32px tall, 12px `#2c3e50` text, 2px `#6b7280` arrows between; stored-row boxes blue `rgba(42,120,214,0.15)` at x=110, outcome boxes at x=390.
- **Band 1 (y=85):** blue box "alice · example-string" → red `rgba(231,76,60,0.12)` box "thief reads every password" with bold 12px red `#e74c3c` "✗ instant" at its right.
- **Band 2 (y=160):** blue box "alice · X7f2…= (key on server)" → orange `rgba(230,126,34,0.15)` box "key stolen too — decrypts all" with bold 12px orange `#d95926` "✗ instant" at its right.
- **Band 3 (y=235):** blue box "alice · 9c2f… (one-way)" → green `rgba(0,131,0,0.12)` box "thief must guess, one try at a time" with bold 12px green `#008300` "✓ work" at its right.
- **Caption (12px `#444`, bottom right):** "stored values are made-up placeholders".

## One Billion Guesses: 1 Second vs 3.2 Core-Years

**Tags:** `worked example` (blue), `the arithmetic of slowness` (green)

- **The theft** — the attacker steals the hash file and guesses offline on his own GPUs, no rate limits
- **Fast hashes** — a GPU computes about 1 billion SHA-256 guesses per second, 10 billion for MD5
- **The budget** — 1 billion guesses covers most real-world passwords: at 1 billion/s that is 1 second
- **The slow hash** — bcrypt tuned to 100 ms per hash allows just 10 guesses per second per core
- **Hand-check** — 1,000,000,000 ÷ 10 per second = 100,000,000 s ≈ 3.2 core-years for that budget
- **The user** — a real login pays the 100 ms exactly once; nobody notices a tenth of a second

*Example (italic):* The same billion-guess dictionary takes 1 second against SHA-256 but about 3.2 core-years against 100 ms bcrypt — while alice's login gets 0.1 s slower.

**Key point:** Password hashing is asymmetric arithmetic — the defender pays the slow hash once per login, the attacker pays it once per guess, and a billion guesses turns 100 ms into years.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: time to run 1 billion guesses against one stolen hash under MD5, SHA-256, and 100 ms bcrypt — schematic widths, exact time labels.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Try 1 Billion Guesses Against One Account".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 420; widths are hardcoded schematic pixels, not a real time axis.
- **Rows (14px tall bars, left-aligned 12px `#444` labels at x=20):**
  - y=90: "MD5 — 10 billion/s": red `#e74c3c` bar width 6, bold 11px red label "0.1 s" at bar end
  - y=155: "SHA-256 — 1 billion/s": red bar width 12, bold 11px red label "1 s" at bar end
  - y=220: "bcrypt 100 ms — 10/s per core": green `#008300` bar width 360, bold 11px green label "≈ 3.2 core-years" at bar end
- **Annotation (bold 13px green `#008300`, near x=300, y=190):** "same guesses, same attacker — 100,000,000× longer".
- **Caption (12px `#444`, bottom right):** "guess rates illustrative, arithmetic exact; pixel widths schematic".

## A Dial That Doubles the Work

**Tags:** `where it's used` (blue), `cost factor` (orange), `memory-hard` (green)

- **The dial** — bcrypt, scrypt, and argon2 take a cost factor; each +1 doubles the hashing work
- **The ratchet** — 100 ms today; when GPUs get 2× faster, raise the cost by 1 and rehash at next login
- **Memory-hard** — argon2 and scrypt also demand RAM per guess (say 64 MB), not just CPU time
- **The GPU wall** — a GPU with 8 GB of RAM fits 8192 ÷ 64 = 128 concurrent guesses, not thousands
- **Built-in salt** — these functions generate and embed a per-user salt automatically in their output

*Example (italic):* Raising bcrypt's cost from 10 to 12 turns 100 ms into 400 ms per hash — the attacker's 3.2 core-years become about 12.7, and a login still finishes in under half a second.

**Key point:** A password hash is tunable slowness — a cost factor that ratchets up as hardware improves, plus memory demands that stop GPUs from running thousands of guesses in parallel.

### Visualization (canvas `c3`, 720×300)

Bar chart of milliseconds per bcrypt hash across cost factors 8 through 14, each bar double the last.

- **Title (bold 15px, `#1a5276`, top center):** "The Cost Factor: +1 Doubles the Work".
- **Axes:** baseline 2px `#999` at y=245 from x=60 to x=680; no y gridlines (heights carry labels).
- **Bars (55px wide, bottoms on the baseline), cost factors and ms `[8, 9, 10, 11, 12, 13, 14]` / `[25, 50, 100, 200, 400, 800, 1600]`, pixel heights `[5, 10, 20, 40, 80, 160, 170]` (the last bar clipped, drawn with a jagged top edge to show it runs off scale):**
  - bars at x = 90, 175, 260, 345, 430, 515, 600; fills blue `rgba(42,120,214,0.30)` with 2px `#2a78d6` top edge, except cost 10 solid green `#008300` (today's setting) and cost 14 orange `rgba(217,89,38,0.35)` with 2px `#d95926` edge
  - bold 12px labels above each bar: "25", "50", "100 ms", "200", "400", "800", "1600 ms"; 12px `#444` cost labels below the baseline: "cost 8" … "cost 14"
- **Annotation (bold 13px violet `#4a3aa7`, near x=340, y=60):** "GPUs got 2× faster? turn the dial +1".
- **Marker:** bold 11px green `#008300` label "today" under the cost-10 bar's cost label.
- **Caption (12px `#444`, bottom right):** "cost 10 = 100 ms illustrative; the doubling is exact".

## A Salt Alone Doesn't Save a Fast Hash

**Tags:** `common mistake` (red), `salt ≠ slow` (orange), `pepper` (green)

- **The confusion** — "we salt our SHA-256 hashes, so we're safe": salt and slowness stop different attacks
- **What salt does** — defeats precomputed tables and forces per-account work; it adds no time per guess
- **Still fast** — salted SHA-256 still yields the 1 billion guesses per second from the arithmetic above
- **The pepper** — a server-side secret mixed into every hash, kept out of the database entirely
- **The layer** — a thief holding only the database can't even start guessing without the pepper

*Example (italic):* A team salts its MD5 hashes and calls the job done; the stolen file still falls at billions of guesses per second — the salt only stopped the ready-made tables.

**Common mistake:** Believing a salt makes a fast hash safe. Salt defeats precomputation, deliberate slowness defeats brute-force guessing, and a pepper blocks offline cracking altogether — they are three separate layers, and a strong scheme uses all three.

### Visualization (canvas `c4`, 720×300)

Three-row layer diagram: each defense layer on the left paired with the one attack it actually blocks on the right, with a crossed-out red attack box.

- **Title (bold 15px, `#1a5276`, top center):** "Three Layers, Three Different Attacks".
- **Rows (centered on y = 90, 165, 240), thin `#e5e9ef` divider lines at y=127 and y=202:**
  - y=90: green `rgba(0,131,0,0.12)` rounded box at x=60 "salt — per-user, stored in the open", 2px `#6b7280` arrow labeled 11px "blocks" to red `rgba(231,76,60,0.12)` box at x=420 "precomputed tables" with bold 13px red `#e74c3c` "✗" at its right
  - y=165: green box "slow + memory-hard hash (bcrypt/argon2)", arrow "blocks" to red box "GPU brute force" with red "✗"
  - y=240: green box "pepper — server-side secret, not in the DB", arrow "blocks" to red box "offline guessing from the file alone" with red "✗"
- **Box style:** 34px tall, 8px radius, 12px `#2c3e50` text, left boxes ~320px wide, right boxes ~220px wide.
- **Annotation (bold 12px ink `#1a5276`, top right near y=55):** "hash( pepper + salt + password ), slowly".
- **Caption (12px `#444`, bottom right):** "schematic — each layer answers one attack".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points, box texts, and bar widths are the hardcoded literals above (no randomness); guess rates (10 billion MD5/s, 1 billion SHA-256/s), the 100 ms bcrypt timing, and the 64 MB argon2 memory setting are invented and labeled illustrative; the arithmetic on top of them is exact and text numbers match chart numbers (1e9 ÷ 10/s = 1e8 s ≈ 3.2 core-years; cost-factor doubling 25→50→100→200→400→800→1600 ms; 4× cost-10→12 turns 3.2 core-years into ≈12.7; 8192 MB ÷ 64 MB = 128 concurrent guesses).
- **Framing:** defensive/educational — attacker guess rates appear only to motivate why password hashes are deliberately slow, memory-hard, salted, and optionally peppered.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: session ids, cookie values, tracking ids, token parts, and example passwords on this page are made-up placeholders — for illustration only, and to avoid false positives from secret scanners."
