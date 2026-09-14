# Brute Force & Dictionary Attacks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Brute Force & Dictionary Attacks

**Subtitle:** A password is one point hidden in a search space you get to size — understanding how attackers shrink that space is how defenders keep it huge

## The Padlock With 209 Billion Combinations

**Tags:** `core idea` (blue), `search space` (green), `entropy` (orange)

- **The forum** — a small web forum's admin audits how guessable her 1,000 users' passwords are
- **One user** — Maya's password is 8 lowercase letters; an attacker must find it among 26^8 options
- **The count** — 26 choices per position, 8 positions: 26^8 = 208,827,064,576 possibilities (exact)
- **Bigger alphabet** — allow all 95 keyboard characters: 95^8 = 6,634,204,312,890,625 (exact)
- **Length wins** — each added character multiplies the space ×26; 26^12 = 95,428,956,661,682,176
- **Entropy** — "bits of entropy" is just log2 of this count: 26^8 ≈ 37.6 bits, 95^8 ≈ 52.6 bits

*Example (italic):* Maya adds a single ninth lowercase letter and her space jumps 26-fold, from 209 billion to 26^9 = 5,429,503,678,976 — each added letter multiplies ×26, while widening one position to mixed case only doubles it.

**Key point:** A password's strength is the size of the space an attacker must search — width of the alphabet raises the base, but length raises the exponent, and exponents win.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of search-space sizes for four password recipes; log-feel achieved by hardcoded pixel widths, not a real log axis.

- **Title (bold 15px, `#1a5276`, top center):** "Search Space: Length Beats a Fancier Alphabet".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 440.
- **Rows (top to bottom at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20:**
  - "8 lowercase — 26^8 = 209 billion": blue `#2a78d6` bar width 105
  - "8 letters+digits — 62^8 = 218 trillion": aqua `#199e70` bar width 195
  - "8 of all 95 chars — 95^8 = 6.6 quadrillion": orange `#d95926` bar width 240
  - "12 lowercase — 26^12 = 95 quadrillion": green `#008300` bar width 275
- **Bar style:** 16px tall, fills at 0.30 alpha of each color with a 2px solid edge, 11px `#444` count labels at bar ends.
- **Annotation (bold 13px green `#008300`, right side near y=255):** "12 plain letters beat 8 of everything".
- **Caption (12px `#444`, bottom right):** "bar widths log-scaled schematic, counts exact".

## Why Attackers Guess Words, Not Letters

**Tags:** `worked example` (blue), `dictionary attack` (red), `human bias` (orange)

- **The shortcut** — humans don't pick uniformly; attackers try likely passwords first, not aaaaaaaa
- **The wordlist** — 100,000 common words and past-breach passwords, tried before any raw brute force
- **Mangling rules** — each word spawns variants: capitalize, append digits, swap a→@, o→0, e→3
- **0r@nge!** — passes every complexity rule, yet it's just "orange" plus three standard rules
- **The tally** — against the forum's 1,000 hashes: top 100 guesses crack 40 accounts, 100 million crack 350
- **The scale** — 100 million guesses is 1/66,000,000th of the 95^8 space, yet it takes 35% of accounts

*Example (italic):* The attacker's 12th mangled candidate for "orange" is 0r@nge! — an account using it falls in under a second, while a truly random 8-character password would sit among 6.6 quadrillion equals.

**Key point:** Dictionary attacks work because real passwords cluster in a tiny, predictable corner of the space — complexity rules reshape that corner (0r@nge!) without moving anyone out of it.

### Visualization (canvas `c2`, 720×300)

Step-style cumulative line chart: forum accounts cracked (of 1,000) as the attacker's guess count grows, on a hardcoded log-feel x axis.

- **Title (bold 15px, `#1a5276`, top center):** "350 of 1,000 Accounts Fall to a Sliver of the Space".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = guesses tried with 12px `#444` tick labels "100", "10k", "1M", "100M" at x = 160, 300, 440, 580 (log-feel, hardcoded); y = accounts cracked 0 to 500, gridlines `#e5e9ef` at 125/250/375.
- **Cracked line:** red `#e74c3c` 3px line through points (x, accounts): `[(60,0), (160,40), (300,180), (440,290), (580,350)]`, 5px dots at each point, 12px `#444` value labels "40", "180", "290", "350" above the dots.
- **Plateau marker:** horizontal dashed `#6b7280` (dash 4/3) line at the y of 350 from x=580 to x=660, 12px `#6b7280` label "the rest are near-random" at its right end.
- **Annotation (bold 13px red `#e74c3c`, near x=300, y=80):** "100M guesses = 1/66,000,000th of 95^8".
- **Caption (12px `#444`, bottom right):** "cracked counts illustrative; space fraction exact".

## Online vs Offline: Who Sets the Guessing Speed

**Tags:** `where it's used` (blue), `rate limiting` (green), `stolen hashes` (red)

- **Online attack** — guesses go through the login page, so the server sets the pace and sees every try
- **The defense** — lockouts and throttling: at 100 tries/hour, exhausting 26^8 takes ~238,000 years
- **Offline attack** — the attacker stole the hash file and guesses on their own hardware, unseen
- **Fast hash** — GPUs test ~10 billion MD5-style guesses/second: all of 26^8 in about 21 seconds
- **Slow hash** — bcrypt/Argon2 cost factors cut that to ~100,000/second: the same sweep takes ~24 days
- **The lesson** — rate limits defend logins; only slow hashing and bigger spaces defend stolen files

*Example (italic):* The same 209 billion-guess sweep takes 238,000 years against a throttled login page but 21 seconds against a stolen fast-hash file — the defense must assume the file walks out the door.

**Key point:** Online attacks are slow because the defender controls the clock; offline attacks run at hardware speed, so the password's search space and the hash's slowness are all that's left.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time to try all 208,827,064,576 lowercase-8 passwords at four guessing speeds; log-feel via hardcoded pixel widths.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Sweep 26^8 = 209 Billion Guesses".
- **Axis:** vertical 2px `#999` baseline at x=265, bars extend right, max width 420.
- **Rows (top to bottom at y = 75, 125, 175, 225), each with a left-aligned 12px `#444` label at x=20:**
  - "online, throttled 100/hr — 238,000 years": green `#008300` bar width 420
  - "online, no throttle 1,000/s — 6.6 years": aqua `#199e70` bar width 250
  - "offline, bcrypt 100k/s — 24 days": orange `#d95926` bar width 165
  - "offline, fast hash 10B/s — 21 seconds": red `#e74c3c` bar width 40
- **Bar style:** 16px tall, fills at 0.30 alpha with 2px solid edge, 11px `#444` time labels at bar ends.
- **Annotation (bold 13px red `#e74c3c`, right side near y=255):** "a stolen fast-hash file turns years into seconds".
- **Caption (12px `#444`, bottom right):** "widths log-scaled schematic; times from the stated rates, guess count exact".

## The Complexity-Rule Trap

**Tags:** `common mistake` (red), `defenses ranked` (green), `MFA` (orange)

- **The mistake** — believing "must include a symbol and a digit" makes passwords hard to guess
- **Why it fails** — users satisfy the rule the same way (0r@nge!1), which the mangling rules expect
- **Length instead** — 5 random common words: 7776^5 = 28,430,288,029,929,701,376 ≈ 28 quintillion (exact)
- **The sweep** — at 10 billion guesses/second, that passphrase space takes ~90 years to exhaust
- **Best defenses** — in order: long random passphrases, a password manager, slow server-side hashing
- **MFA on top** — a second factor means even a correctly guessed password can't log in alone

*Example (italic):* "chair-planet-mango-violin-tape" breaks every complexity checklist yet sits in a 28-quintillion space, while rule-compliant 0r@nge!1 falls inside the attacker's first few thousand mangled guesses.

**Common mistake:** Grading passwords by which character classes they contain. Attackers grade them by how early they appear in a wordlist-plus-rules run — length and randomness move that rank; a mandatory `@` does not.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a rule-compliant password falling to a dictionary run vs a plain 5-word passphrase plus MFA holding, shown as guess boxes flowing toward the account.

- **Title (bold 15px, `#1a5276`, top center):** "Complexity Checkbox vs Length + MFA".
- **Row 1 (y=95), label 12px `#444` at x=20:** "0r@nge!1 ✓rules"; blue `#2a78d6` rounded box at x=175 labeled "wordlist + rules run" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "hit in first ~10k guesses" with bold 12px red "✗ account taken".
- **Row 2 (y=205), label:** "5 random words"; blue box at x=175 labeled "space 7776^5 ≈ 28 quintillion", 3px arrow to a green `#008300` box at x=400 labeled "~90 yrs at 10B/s", then arrow to a green box at x=580 labeled "+ MFA" with bold 12px green "✓ guess alone fails".
- **Box style:** 140–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "defenses stack: length sets the space, slow hashing sets the speed, MFA makes the guess insufficient".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); combinatorial counts (26^8 = 208,827,064,576; 62^8 = 218,340,105,584,896; 95^8 = 6,634,204,312,890,625; 26^12 = 95,428,956,661,682,176; 26^9 = 5,429,503,678,976; 7776^5 = 28,430,288,029,929,701,376) are exact; sweep times follow exactly from the stated guess rates; the forum's cracked-account counts (40/180/290/350 of 1,000) are invented and labeled illustrative.
- **Framing:** defensive/educational — the page explains attacker economics so defenders can size spaces, throttle logins, hash slowly, and deploy MFA; no operational attack guidance beyond the arithmetic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: realistic credential strings on this page were converted to generic placeholders — for illustration only, and to avoid false positives from secret scanners."
