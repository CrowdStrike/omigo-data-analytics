# ZFS

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** ZFS

**Subtitle:** ZFS checksums every block it writes and re-checks it on every read — the filesystem that assumes the disk is lying until proven otherwise

## The Ledger That Rotted on the Shelf

**Tags:** `core idea` (blue), `end-to-end checksums` (green), `bit rot` (orange)

- **The archive** — a coffee shop chain keeps five years of daily sales files on one storage box
- **The rot** — one night a disk quietly flips a single bit inside a three-year-old sales file
- **The old way** — an ordinary filesystem returns the corrupted block as if nothing happened
- **The distrust** — ZFS stores a checksum for every block and verifies it on every single read
- **The parent** — the checksum lives in the parent block pointer, not next to the data it guards
- **The chain** — parents are checksummed by their parents up to the root, a merkle-style tree

*Example (italic):* The analyst opens the 2023 file in 2026; ZFS re-checks its blocks on the way up and flags the flipped bit before a single wrong number reaches the spreadsheet.

**Key point:** ZFS (shipped in Solaris, 2005) treats the disk as an unreliable witness — every block read is checked against a checksum stored one level above it, so corruption cannot hide.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: a read on an ordinary filesystem (corrupt block passes through unnoticed) vs a read on ZFS (checksum in the parent pointer catches it).

- **Title (bold 15px, `#1a5276`, top center):** "Same Flipped Bit, Two Filesystems: Only One Notices".
- **Row 1 (y=95), label 12px `#444` at x=20:** "ordinary FS"; blue `#2a78d6` rounded box at x=150 labeled "disk block (1 bit flipped)" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "returned as-is to the app" with bold 12px red "✗ silent corruption".
- **Row 2 (y=205), label:** "ZFS"; blue box at x=150 "disk block (1 bit flipped)", 3px arrow to a green `#008300` box at x=360 labeled "verify vs parent checksum", then arrow to a green box at x=560 labeled "mismatch — flagged" with bold 12px green "✓ caught".
- **Box style:** 150–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the checksum lives in the parent pointer — bad data can't vouch for itself".
- **Caption (12px `#444`, bottom right):** "flow schematic".

## Catching a Flipped Bit with One Addition

**Tags:** `worked example` (blue), `checksum` (green)

- **The block** — one block holds four daily totals for the week: 120, 95, 140, 88
- **The stored check** — at write time the parent pointer records their sum: 120+95+140+88 = 443
- **The flip** — months later bit 7 of the last value flips on disk: 88 becomes 216 (+128)
- **The re-check** — the read-back sum is 120+95+140+216 = 571, which does not equal 443
- **The repair** — ZFS fetches the mirror's copy, verifies it sums to 443, and rewrites the bad block
- **The real thing** — production ZFS uses fletcher4 or SHA-256, not a sum; the logic is identical

*Example (italic):* Read back [120, 95, 140, 216], sum to 571, compare with the stored 443 — one addition exposes the flip, and the mirror copy that sums to 443 replaces it.

**Key point:** Verification is just "recompute and compare": stored checksum 443 vs read-back 571 means the disk lied, and a redundant copy that matches 443 is trusted instead.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart of the four daily totals: values as written (blue) vs values as read back after the bit flip (orange, with day 4 corrupted), checksums compared on the right.

- **Title (bold 15px, `#1a5276`, top center):** "One Bit Flips: 88 Becomes 216 and the Sum Betrays It".
- **Axes:** origin x=60, baseline y=245, plot width 480, plot height 180; y = value 0 to 250, gridlines `#e5e9ef` at 50/100/150/200; x = four day groups "Mon"–"Thu" with 12px `#444` labels.
- **Written bars:** blue `#2a78d6` fill `rgba(42,120,214,0.30)` with solid 2px edge, heights for values `[120, 95, 140, 88]`, 12px value labels on top.
- **Read-back bars:** beside each written bar, aqua `#199e70` for the matching days `[120, 95, 140]`, but day 4 drawn red `#e74c3c` at value `216` with bold 12px red label "216 (bit 7 flipped)".
- **Checksum panel (right, x=570–710):** 12px `#2c3e50` lines "stored: 443" and "read: 571", separated by a 2px `#999` rule, bold 13px red "443 ≠ 571".
- **Annotation (bold 13px green `#008300`, near x=300, y=60):** "mirror copy sums to 443 — bad block rewritten".
- **Caption (12px `#444`, bottom right):** "totals illustrative; sum stands in for fletcher4/SHA-256".

## Scrubbing Years of Data Before It Rots

**Tags:** `where it's used` (blue), `scrub` (green), `data science` (orange)

- **The threat** — flaky cables, firmware bugs, and misdirected writes corrupt data without any error
- **The scrub** — a ZFS scrub walks every block in the pool, re-checks it, and repairs from redundancy
- **The schedule** — run monthly, corruption is caught while a good copy still exists to heal from
- **RAID-Z** — ZFS's own parity RAID, so every repair has checksummed parity blocks to rebuild from
- **Copy-on-write** — ZFS never overwrites a live block in place, so a crash can't half-write one
- **The stakes** — a silently corrupted training file poisons every model retrained from it

*Example (italic):* A monthly scrub of the archive quietly repairs a handful of bad blocks a year; the same blocks on a plain filesystem would sit corrupted until someone noticed odd numbers.

**Key point:** Checksums only help if something reads the data — scrub is the routine that reads everything on purpose, turning silent decade-long rot into a monthly repair log line.

### Visualization (canvas `c3`, 720×300)

Line chart over 36 months: cumulative silently-corrupted blocks on a checksum-less filesystem (growing) vs a ZFS pool with monthly scrubs (held at zero), on a shared time axis.

- **Title (bold 15px, `#1a5276`, top center):** "36 Months of Bit Rot: Unchecked It Accumulates, Scrubbed It Stays Zero".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 36 with 12px `#444` tick labels every 6 months; y = cumulative bad blocks 0 to 10, gridlines `#e5e9ef` at 2/4/6/8.
- **Plain FS line:** red `#e74c3c` 3px line through months `[0, 6, 12, 18, 24, 30, 36]`, bad blocks `[0, 1, 3, 4, 6, 8, 9]` — a slow staircase up.
- **ZFS line:** green `#008300` 3px line through the same months, bad blocks `[0, 0, 0, 0, 0, 0, 0]` — flat on the baseline.
- **Scrub ticks:** small vertical `#6b7280` dashes (dash 3/3) just above the green line every month (months 1-36), one 12px `#6b7280` label "scrub repairs, count resets" near month 12.
- **Annotation (bold 13px red `#e74c3c`, near month 26, y=95):** "9 corrupt blocks and nobody knows".
- **Caption (12px `#444`, bottom right):** "block counts illustrative".

## RAID Alone Can't Tell Which Copy Is Right

**Tags:** `common mistake` (red), `mirrors` (orange)

- **The confusion** — "I have RAID mirrors, so I'm already protected" — RAID copies, it doesn't verify
- **The tie** — when two mirror copies disagree, plain RAID has no way to know which one is correct
- **The coin flip** — many RAID setups just return whichever copy answered first, right or wrong
- **The referee** — ZFS holds the checksum in the parent, an outside witness neither copy can forge
- **Self-healing** — the copy matching the checksum wins, and ZFS rewrites the losing copy from it
- **Disk ECC too** — a drive's own ECC misses misdirected and phantom writes; end-to-end checks don't

*Example (italic):* Mirror A returns a block summing to 443 and mirror B one summing to 571; plain RAID may serve B without blinking, while ZFS picks A and heals B on the spot.

**Common mistake:** Treating redundancy as integrity. Two disagreeing copies with no checksum is a tie you can lose; the parent-stored checksum is what turns redundancy into repair.

### Visualization (canvas `c4`, 720×300)

Two-row decision diagram: disagreeing mirror copies resolved by plain RAID (arbitrary pick) vs by ZFS (parent checksum picks the verified copy and heals the other).

- **Title (bold 15px, `#1a5276`, top center):** "Mirrors Disagree: Coin Flip vs Checksum Referee".
- **Row 1 (y=95), label 12px `#444` at x=20:** "plain RAID"; two blue `#2a78d6` rounded boxes at x=150 and x=150,y+? stacked compactly labeled "copy A: sum 443" and "copy B: sum 571" (12px), 3px arrows converging to a red `#e74c3c` box at x=440 labeled "serves whichever answers first" with bold 12px red "✗ may return 571".
- **Row 2 (y=205), label:** "ZFS"; same two blue boxes "copy A: sum 443" / "copy B: sum 571", arrows into a green `#008300` box at x=380 labeled "parent checksum = 443", then arrow to a green box at x=580 labeled "serve A, rewrite B" with bold 12px green "✓ self-heal".
- **Box style:** 140–180px wide, 36px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "redundancy gives you a second copy; the checksum tells you which copy to trust".
- **Caption (12px `#444`, bottom right):** "sums carried over from the worked example, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); daily totals `[120, 95, 140, 88]`, checksums 443 / 571, the flipped value 216, and the 36-month bad-block counts `[0, 1, 3, 4, 6, 8, 9]` vs `[0, 0, 0, 0, 0, 0, 0]` are invented and labeled illustrative; ZFS facts (2005 Solaris debut, checksums in parent block pointers forming a merkle-style tree, scrub, RAID-Z, copy-on-write, fletcher4/SHA-256) are documented public behavior.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
