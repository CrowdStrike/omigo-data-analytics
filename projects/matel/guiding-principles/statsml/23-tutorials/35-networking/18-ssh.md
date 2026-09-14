# SSH

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SSH

**Subtitle:** One command opens an encrypted terminal, file copy, or tunnel into any machine you hold a key for — the encrypted tunnel to everywhere

## A Terminal Inside a Machine 3,000 Miles Away

**Tags:** `core idea` (blue), `remote shell` (green), `encryption` (orange)

- **The server** — Priya's model trains on the team's GPU box in a far-away data center, not on her laptop
- **The command** — `ssh priya@gpu-server` opens a shell on that machine as if she sat at its keyboard
- **The wire** — every keystroke she types and every line printed back crosses the café Wi-Fi encrypted
- **The ancestor** — telnet did the same remote-terminal job but sent the password in readable plaintext
- **The name** — Secure SHell: the same old remote terminal, wrapped in an encrypted channel

*Example (italic):* Priya types `nvidia-smi` in the café; the command runs in the data center and the GPU stats travel back — the sniffer at the next table captures only gibberish.

**Key point:** SSH gives you a shell on a remote machine over an encrypted channel — everything you type and everything printed back is unreadable to anyone on the path between you and the server.

### Visualization (canvas `c1`, 720×300)

Two-row comparison diagram: the same login crossing the café Wi-Fi over telnet (readable) vs over SSH (gibberish), each row laptop box → public wire band → server box.

- **Title (bold 15px, `#1a5276`, top center):** "Same Login, Same Wi-Fi: What the Sniffer at the Next Table Sees".
- **Wire band:** vertical strip from x=250 to x=470, fill `rgba(107,114,128,0.08)`, 12px mute `#6b7280` header "open café Wi-Fi" at y=55.
- **Row 1 (y=110), 12px `#444` label "telnet (1980s)" at x=20:** blue `#2a78d6` rounded box at x=95 labeled "Priya's laptop" (12px), 3px red `#e74c3c` arrow through the strip to a blue box at x=505 labeled "gpu-server"; inside the strip, bold 13px red monospace "plaintext credentials" at y=100 with 11px red label "readable" under it.
- **Row 2 (y=210), label "ssh":** same laptop and server boxes, 3px green `#008300` arrow through the strip; inside the strip, bold 13px green monospace "x9$k?…Vq2#" at y=200 with 11px green label "encrypted" under it.
- **Box style:** 130px wide, 40px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "the terminal is remote; the secrecy is end to end".
- **Caption (12px `#444`, bottom right):** "password and ciphertext strings illustrative".

## Proving It's Priya Without Sending a Password

**Tags:** `worked example` (blue), `key pairs` (green)

- **Two keys** — Priya keeps a private key on her laptop; the server stores only the matching public key
- **The challenge** — at login the server in effect picks a fresh random number and says "sign this"
- **Toy keys** — private key (7, 33), public key (3, 33); the server's challenge is the number 4
- **The signature** — Priya's laptop computes 4^7 mod 33 = 16 and sends 16 back
- **The check** — the server computes 16^3 mod 33 = 4; the challenge comes back, so the key matches
- **No secret sent** — the private 7 never leaves the laptop; a sniffer sees only the 4 and the 16

*Example (italic):* Recording the login is useless for replay — next time the server picks a different challenge, and only the private key can sign that one too.

**Key point:** Key-pair login is a challenge-signature round: the server verifies the signature with the stored public key, so it confirms who you are while no password and no private key ever cross the wire.

### Visualization (canvas `c2`, 720×300)

Two-lane message flow with the toy-RSA arithmetic: laptop lane (left) and server lane (right), challenge going left, signature going right, verification box at the end.

- **Title (bold 15px, `#1a5276`, top center):** "Sign My Random Number: 4 Goes Out, 16 Comes Back, 4 Reappears".
- **Lanes:** vertical 2px `#e5e9ef` lifelines at x=150 ("Priya's laptop — private key (7, 33)") and x=570 ("gpu-server — public key (3, 33)"); lane headers bold 12px `#2c3e50` at y=55.
- **Arrow 1 (y=110):** blue `#2a78d6` 3px arrow right→left, 12px blue label above: "challenge: sign 4".
- **Sign box:** rounded box at x=80, y=135, width 220, height 34, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text "4^7 mod 33 = 16".
- **Arrow 2 (y=200):** green `#008300` 3px arrow left→right, 12px green label above: "signature: 16".
- **Verify box:** rounded box at x=450, y=225, width 240, height 34, fill `rgba(0,131,0,0.12)`, 12px text "16^3 mod 33 = 4 ✓ matches".
- **Sniffer marker:** 12px mute `#6b7280` label "wire sees only 4 and 16" centered at x=360, y=170.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "the private 7 never leaves the laptop".
- **Caption (12px `#444`, bottom right):** "toy numbers for hand-checking; real keys are ~256-bit curves or 2048-bit RSA".

## One Key, Four Doors: Shells, Copies, Git, Tunnels

**Tags:** `where it's used` (blue), `tunnels` (green), `git` (orange)

- **Remote shell** — `ssh` itself: run commands, edit files, babysit long training jobs from anywhere
- **File copy** — `scp gpu-server:results.csv .` pulls files home through the same encrypted channel
- **Git** — `git push` to a `git@...` address logs in over SSH with the same key pair, no password
- **Tunnels** — `ssh -L 5433:db:5432` makes a private far-away database appear on Priya's own laptop
- **Under the hood** — rsync, remote notebooks, and deploy scripts all ride on plain SSH underneath

*Example (italic):* Priya's notebook connects to localhost:5433 on her laptop, and SSH quietly ferries every query to the database's port 5432 inside the data center.

**Key point:** SSH is a general encrypted pipe with a login attached; a shell, a file copy, a git push, and a database tunnel are just different traffic sent through the same pipe with the same key.

### Visualization (canvas `c3`, 720×300)

Hub-and-spoke diagram: one central SSH box, four spoke boxes for the four everyday uses, each spoke labeled with its command.

- **Title (bold 15px, `#1a5276`, top center):** "One Encrypted Pipe, Four Everyday Jobs".
- **Hub:** rounded box centered at (360, 165), width 200, height 52, fill `rgba(26,82,118,0.12)`, 2px `#1a5276` border, bold 13px `#1a5276` text "SSH — encrypted pipe + key login".
- **Spokes (2px lines from hub edge to each box, 11px mute `#6b7280` command label on each line):**
  - top-left box at (60, 75): blue `#2a78d6` border, fill `rgba(42,120,214,0.15)`, 12px text "remote shell"; line label "ssh priya@gpu-server"
  - top-right box at (520, 75): green `#008300` border, fill `rgba(0,131,0,0.12)`, 12px text "file copy"; line label "scp results.csv"
  - bottom-left box at (60, 235): violet `#4a3aa7` border, fill `rgba(74,58,167,0.12)`, 12px text "code push"; line label "git push (git@…)"
  - bottom-right box at (520, 235): aqua `#199e70` border, fill `rgba(25,158,112,0.12)`, 12px text "database tunnel"; line label "-L 5433:db:5432"
- **Box style (spokes):** 150px wide, 40px tall, 8px radius, 12px `#2c3e50` text.
- **Annotation (bold 13px aqua `#199e70`, bottom center near y=290):** "the tunnel makes localhost:5433 secretly mean the data center's 5432".
- **Caption (12px `#444`, bottom right):** "commands abbreviated; layout schematic".

## The Fingerprint Prompt Everyone Types yes To

**Tags:** `common mistake` (red), `host keys` (orange)

- **The prompt** — first connect: "authenticity can't be established, fingerprint SHA256:9pT4… — continue?"
- **What it asks** — SSH wants you to confirm this public key really belongs to your server, not a fake
- **The habit** — most people type yes unread; an impostor on rogue Wi-Fi passes exactly this way
- **The memory** — after one yes the key is saved in known_hosts, and later mismatches raise loud alarms
- **The loud one** — "REMOTE HOST IDENTIFICATION HAS CHANGED" means the key differs — stop, don't delete
- **The fix** — compare the fingerprint against one published out-of-band, or pre-install known_hosts

*Example (italic):* Priya hits the CHANGED warning at the café, deletes the known_hosts line to "make it work", and every command and file in her session now flows through an impostor's relay.

**Common mistake:** Treating the host-key prompt as noise. Encryption to the wrong machine protects nothing — the fingerprint check is the only step where SSH learns the far end is your server and not a relay.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: typing yes blindly (impostor relays the whole session) vs checking the fingerprint (mismatch caught, connection refused).

- **Title (bold 15px, `#1a5276`, top center):** "The yes That Lets an Impostor In — and the Check That Doesn't".
- **Row 1 (y=95), 12px `#444` label at x=20:** "yes, unread"; blue `#2a78d6` rounded box at x=125 labeled "Priya" (12px), 3px arrow to a red `#e74c3c` box at x=300 labeled "impostor SHA256:Xw2R…", 3px arrow on to a blue box at x=535 labeled "gpu-server"; bold 12px red "✗ session relayed and read in the middle" under the row at y=140.
- **Row 2 (y=215), label:** "fingerprint checked"; blue box "Priya" at x=125, 3px arrow to a red box at x=300 labeled "Xw2R… ≠ published 9pT4…", then a 3px green `#008300` arrow stub ending in bold 14px green "✕ abort" at x=505, with bold 12px green "✓ mismatch caught, nothing sent" under the row at y=260.
- **Box style:** 130–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px magenta `#d55181`, top right near y=60):** "the padlock says private; the fingerprint says to whom".
- **Caption (12px `#444`, bottom right):** "fingerprint strings illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the toy key-pair numbers (private key (7, 33), public key (3, 33), challenge 4, signature 4^7 mod 33 = 16, verification 16^3 mod 33 = 4) are exact and hand-checkable; the tunnel ports 5433:db:5432 match between text and chart c3; the plaintext-credentials label, ciphertext "x9$k?…Vq2#", and fingerprints SHA256:9pT4… / SHA256:Xw2R… are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: realistic credential strings on this page were converted to generic placeholders — for illustration only, and to avoid false positives from secret scanners."
