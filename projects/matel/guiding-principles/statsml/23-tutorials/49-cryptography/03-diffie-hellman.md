# Diffie-Hellman

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Diffie-Hellman

**Subtitle:** Two people shout messages across a crowded room and still end up with a shared secret nobody in the room can figure out

## Mixing Paint in Front of Everyone

**Tags:** `core idea` (blue), `paint analogy` (green), `public exchange` (orange)

- **The common paint** — Alice and Bob agree, out loud, on a public starting color: a can of yellow
- **The secret drops** — each privately picks a secret color; Alice adds her red, Bob adds his blue
- **The swap** — they trade the mixtures in public: Alice hands over orange, Bob hands over green
- **The finish** — each adds their own secret to the other's mixture; both arrive at the same muddy brown
- **The stuck spy** — Eve saw yellow, orange, and green go past, but paint can't be unmixed into its parts

*Example (italic):* Yellow + red + blue gives the same brown whether the red went in first or last — so Alice and Bob match, and Eve holds only mixtures she cannot separate.

**Key point:** Diffie-Hellman lets two parties build one shared secret using only public messages — the trick is a mixing step that is easy to do and practically impossible to undo.

### Visualization (canvas `c1`, 720×300)

Two-lane flow diagram of the paint protocol: Alice's lane on top, Bob's lane on bottom, a public middle strip showing what Eve sees, colored boxes flowing left to right into the same final color.

- **Title (bold 15px, `#1a5276`, top center):** "Same Brown on Both Sides — Eve Only Ever Sees Mixtures".
- **Public strip:** horizontal band y=130..170 across the plot, fill `rgba(107,114,128,0.08)`, 12px `#6b7280` label "public — Eve sees all of this" at x=70, y=163.
- **Alice lane (y=75):** rounded box at x=70 fill `#f5d547` labeled "yellow (public)" (12px `#2c3e50`), 3px `#6b7280` arrow to box at x=230 fill `#e8956b` labeled "+ secret red → orange", arrow dipping into the public strip toward Bob.
- **Bob lane (y=225):** rounded box at x=70 fill `#f5d547` labeled "yellow (public)", arrow to box at x=230 fill `#7fb069` labeled "+ secret blue → green", arrow rising into the public strip toward Alice.
- **Final boxes (x=520):** Alice lane box fill `#8b6f47` labeled "green + my red = brown"; Bob lane box fill `#8b6f47` labeled "orange + my blue = brown"; both 12px white text.
- **Crossing arrows:** the orange box's arrow crosses down through the strip to Bob's final box; the green box's arrow crosses up to Alice's final box; small 11px `#6b7280` labels "sent in public" on each crossing.
- **Box style:** 130–170px wide, 38px tall, 8px radius, 1px `#999` border.
- **Annotation (bold 13px green `#008300`, right edge x≈560, y=150):** "identical secret, never transmitted".
- **Caption (12px `#444`, bottom right):** "paint colors schematic".

## The Same Trick with p=23 and g=5

**Tags:** `worked example` (blue), `exact numbers` (green)

- **Public setup** — everyone, Eve included, knows the modulus p=23 and the base g=5
- **Alice's turn** — secret a=6; she sends A = 5⁶ mod 23 = 15625 mod 23 = 8
- **Bob's turn** — secret b=15; he sends B = 5¹⁵ mod 23 = 19 (squares: 5²≡2, 5⁴≡4, 5⁸≡16; 16·4·2·5=640≡19)
- **Alice finishes** — B^a = 19⁶ mod 23: 19²≡16, 19⁴≡3, so 3·16 = 48 ≡ 2
- **Bob finishes** — A^b = 8¹⁵ mod 23: 8²≡18, 8⁴≡2, 8⁸≡4, so 4·2·18·8 = 1152 ≡ 2
- **Why it matches** — both computed g^(ab): (g^b)^a = (g^a)^b, mixing order doesn't matter

*Example (italic):* The wire carried only 23, 5, 8, and 19 — yet both ends now hold the shared secret 2, which was never sent.

**Key point:** Each side raises the other's public number to its own secret exponent; exponentiation commutes, so both land on g^(ab) mod p — here exactly 2.

### Visualization (canvas `c2`, 720×300)

Two-column ladder diagram: Alice's computations on the left, Bob's on the right, a shaded public channel in the middle carrying the four public numbers, both columns converging on the boxed shared secret 2.

- **Title (bold 15px, `#1a5276`, top center):** "p=23, g=5: Only 8 and 19 Cross the Wire — Both Sides Compute 2".
- **Channel:** vertical band x=300..420, fill `rgba(107,114,128,0.08)`, 12px `#6b7280` rotated or top label "public wire" at y=55; 13px `#2c3e50` text "p = 23, g = 5" centered at y=75.
- **Alice column (boxes at x=40, width 230, 34px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text), top to bottom at y = 95, 145, 195:** "secret a = 6", "A = 5⁶ mod 23 = 8", "B⁶ = 19⁶ mod 23 = 2".
- **Bob column (boxes at x=450, width 230, same style, fill `rgba(0,131,0,0.12)`), same rows:** "secret b = 15", "B = 5¹⁵ mod 23 = 19", "A¹⁵ = 8¹⁵ mod 23 = 2".
- **Crossing arrows (3px):** blue `#2a78d6` arrow from Alice's row-2 box through the channel to Bob's row-3 box, 12px blue label "8 →" at midpoint y≈150; green `#008300` arrow from Bob's row-2 box to Alice's row-3 box, 12px green label "← 19".
- **Shared-secret box:** centered at x=310..410, y=245, 34px tall, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, bold 13px `#4a3aa7` text "shared secret = 2"; thin dashed `#4a3aa7` connectors from both row-3 boxes.
- **Secret shading:** small 11px `#d95926` "never sent" tags beside "secret a = 6", "secret b = 15", and the shared-secret box.
- **Caption (12px `#444`, bottom right):** "all values exact".

## Why Watching the Wire Gets Eve Nowhere

**Tags:** `where it's used` (blue), `discrete log` (orange), `forward secrecy` (green)

- **Eve's problem** — she knows p=23, g=5, and saw A=8; to proceed she must find a with 5^a ≡ 8
- **No shortcut** — the powers of 5 mod 23 bounce around with no visible order; only trial gives a=6
- **Discrete logarithm** — undoing g^a mod p is the discrete-log problem; at toy size it's 22 guesses
- **Real sizes** — real DH uses ~2048-bit p (or elliptic curves), putting trial far beyond any computer
- **Forward secrecy** — TLS makes fresh DH secrets per session, then discards them after use
- **Recorded traffic stays dark** — stealing a server's long-term key later unlocks none of the old sessions

*Example (italic):* An eavesdropper who taped a year of TLS traffic and then stole the server's key still can't read the tapes — each session's DH exponents died with the session.

**Key point:** DH's safety rests on the discrete-log problem being one-way at scale, and its ephemeral use buys forward secrecy — past sessions stay sealed even after future key theft.

### Visualization (canvas `c3`, 720×300)

Scatter plot of 5^x mod 23 for x = 1..22: the full, exact power table, showing values scattered with no usable pattern; the two exchanged points highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "5^x mod 23 Jumps Around — Seeing 8 Doesn't Point at x = 6".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 185; x = exponent 1 to 22, 12px `#444` tick labels at 1, 6, 11, 15, 22; y = result 0 to 23, gridlines `#e5e9ef` at 8 and 19 with 12px `#444` labels "8" and "19" at left.
- **Points:** 6px filled circles `#2a78d6` at (x, 5^x mod 23) for x=1..22, exact values in order: `[5, 2, 10, 4, 20, 8, 17, 16, 11, 9, 22, 18, 21, 13, 19, 3, 15, 6, 7, 12, 14, 1]`.
- **Highlights:** the point (6, 8) drawn 8px in orange `#d95926` with bold 12px orange label "Alice sent 8 (a=6)"; the point (15, 19) drawn 8px in green `#008300` with bold 12px green label "Bob sent 19 (b=15)".
- **Dashed guides:** horizontal dashed `#d95926` (dash 4/3) line at y-value 8 from the axis to the point, showing Eve's view: she knows the height, not the x.
- **Annotation (bold 13px violet `#4a3aa7`, upper left near x=2, y=60):** "22 guesses here — astronomically many in real DH".
- **Caption (12px `#444`, bottom right):** "all 22 points exact; 5 is a generator mod 23".

## The Handshake That Trusts a Stranger

**Tags:** `common mistake` (red), `man in the middle` (orange)

- **The gap** — DH proves two parties share a secret; it never says who the other party is
- **Mallory's move** — sitting on the wire, she runs one DH with Alice and a second DH with Bob
- **Two secrets** — Alice shares key K1 with Mallory, Bob shares K2 with Mallory; nobody notices
- **The relay** — Mallory decrypts each message with one key, reads it, re-encrypts with the other
- **The fix** — TLS signs the DH values with a certificate, so Alice knows the far end is really Bob

*Example (italic):* Alice's "secure" channel terminates at Mallory's laptop, which politely forwards everything to Bob — both ends see a working encrypted connection.

**Common mistake:** Treating key exchange as authentication. Unauthenticated DH secures you to *someone* — certificates (or another identity check) are what make that someone the right person.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the exchange Alice and Bob believe is happening (one shared key) vs the real topology with Mallory in the middle (two keys, one relay).

- **Title (bold 15px, `#1a5276`, top center):** "One Handshake or Two? Alice Can't Tell Without Authentication".
- **Row 1 (y=95), label 12px `#444` at x=20:** "what Alice thinks"; blue `#2a78d6` rounded box at x=150 labeled "Alice", 3px green `#008300` double-headed arrow labeled "shared key K" (bold 12px green) to a blue box at x=520 labeled "Bob".
- **Row 2 (y=210), label:** "what's really there"; blue box "Alice" at x=110, 3px orange `#d95926` double-headed arrow labeled "K1" to a red `#e74c3c` box at x=330 labeled "Mallory — reads everything", then 3px orange double-headed arrow labeled "K2" to a blue box "Bob" at x=550.
- **Box style:** 110–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` for Alice/Bob, `rgba(231,76,60,0.12)` for Mallory, 12px `#2c3e50` text; bold 12px red "✗ two separate DH runs" under the Mallory box at y≈250.
- **Divider:** dashed `#e5e9ef` horizontal line at y=150 between the rows.
- **Annotation (bold 13px magenta `#d55181`, right side near y=270):** "certificates pin the far end — that's TLS's job, not DH's".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Paint-analogy fills in c1 (`#f5d547` yellow, `#e8956b` orange, `#7fb069` green, `#8b6f47` brown) are one-off schematic colors for that diagram only.
- **Data:** all numbers are exact and hardcoded (no randomness). The DH run is p=23, g=5, a=6, b=15, A=8, B=19, shared secret 2 — every modular step in the text and in c2 must reproduce exactly. The c3 scatter uses the exact power table `[5, 2, 10, 4, 20, 8, 17, 16, 11, 9, 22, 18, 21, 13, 19, 3, 15, 6, 7, 12, 14, 1]` for 5^1..5^22 mod 23. Only the paint colors (c1) and box positions are schematic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
