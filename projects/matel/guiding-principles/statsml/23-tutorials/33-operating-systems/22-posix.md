# POSIX

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** POSIX

**Subtitle:** POSIX is the written contract between a program and any Unix-like operating system — code written to the contract runs unchanged on Linux, macOS, and BSD

## The Nightly Sales Script That Runs on Three Machines

**Tags:** `core idea` (blue), `portability` (green), `IEEE 1003` (orange)

- **The script** — a coffee-shop chain's `sales_report.sh` totals last night's orders from a CSV
- **Three machines** — written on a Linux server, checked on a macOS laptop, rerun in a BSD container
- **Same answer** — all three print the same total, 2,847 orders, without changing a single line
- **The reason** — each OS honors one shared rulebook for the shell, the utilities, and the C calls
- **The rulebook** — IEEE standard 1003, first published in 1988, is that rulebook: POSIX
- **The scope** — it pins down the shell grammar, about 160 utilities, and C APIs like `open` and `fork`

*Example (italic):* The analyst emails `sales_report.sh` to a franchise running FreeBSD; it prints 2,847 orders there too, because both sides wrote to the same contract.

**Key point:** POSIX (Portable Operating System Interface) is a standard, not an operating system — a program written to it runs on any system that implements the contract.

### Visualization (canvas `c1`, 720×300)

Flow diagram: one script box on the left, a POSIX contract band in the middle, three OS boxes on the right each emitting the same output.

- **Title (bold 15px, `#1a5276`, top center):** "One Script, One Contract, Three Operating Systems".
- **Script box:** blue `#2a78d6` rounded box (150×46, 8px radius, fill `rgba(42,120,214,0.15)`) at x=30, y=140, 12px `#2c3e50` label "sales_report.sh".
- **Contract band:** vertical violet `#4a3aa7` rounded rectangle (110×190, fill `rgba(74,58,167,0.10)`) centered at x=300, y=55; rotated or stacked 12px violet label "POSIX contract" with 11px `#6b7280` sublabels "shell grammar / utilities / C calls".
- **OS boxes:** three green-edged boxes (170×42, fill `rgba(0,131,0,0.10)`, 12px `#2c3e50` text) at x=430, y = 65 / 140 / 215, labeled "Linux server", "macOS laptop", "FreeBSD container".
- **Arrows:** 3px `#6b7280` arrows from script box into the band, then from the band to each OS box.
- **Output labels:** bold 12px green `#008300` "2,847 orders" to the right of each OS box (x=615).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=285):** "the script talks to the contract, not to any one OS".
- **Caption (12px `#444`, bottom right):** "order count illustrative".

## Auditing the Script: 8 Portable Lines, 4 That Break

**Tags:** `worked example` (blue), `hand check` (green), `GNU extensions` (orange)

- **The audit** — the script calls 12 command-line tools; check each against the POSIX spec by hand
- **The portable 8** — `sort`, `uniq -c`, `cut -d`, `grep`, `awk`, `tr`, `wc -l`, `head` are all in the standard
- **The risky 4** — `sed -i`, `grep -P`, `date -d`, `cp --reflink` are GNU extensions, not POSIX
- **On Linux** — all 12 of 12 lines work, because GNU tools accept their own extensions
- **On macOS** — only 8 of 12 work; the 4 extension lines fail — illegal option or misparsed args
- **The score** — a 12-line script that "works fine" was really 8 portable lines plus 4 Linux-only ones

*Example (italic):* The laptop test fails on line 5 (`date -d yesterday`) — the flag exists in GNU date but nowhere in the POSIX date spec, so BSD-based macOS rejects it.

**Key point:** Portability is checkable line by line: a command is safe exactly when the flag you use appears in the POSIX spec, not merely when it works on your machine.

### Visualization (canvas `c2`, 720×300)

Compatibility matrix: 12 command rows, two OS columns (Linux with GNU tools, macOS with BSD tools), green checks and red crosses.

- **Title (bold 15px, `#1a5276`, top center):** "12 Commands, 2 Machines: Where the Script Breaks".
- **Layout:** column headers bold 12px `#1a5276` "Linux (GNU)" at x=430 and "macOS (BSD)" at x=570, y=52; 12 rows at y = 70 + i*18 for i in 0..11; command names 12px monospace `#2c3e50` at x=40.
- **Rows (name, linux, macos):** hardcoded array `[["sort","ok","ok"],["uniq -c","ok","ok"],["cut -d","ok","ok"],["grep","ok","ok"],["awk","ok","ok"],["tr","ok","ok"],["wc -l","ok","ok"],["head","ok","ok"],["sed -i","ok","fail"],["grep -P","ok","fail"],["date -d","ok","fail"],["cp --reflink","ok","fail"]]`.
- **Marks:** "ok" drawn as bold 13px green `#008300` "✓" ; "fail" as bold 13px red `#e74c3c` "✗", centered under each header.
- **Divider:** 1px `#e5e9ef` horizontal line between row 8 and row 9, 11px `#6b7280` label "POSIX above, GNU-only below" at x=40 on the divider.
- **Annotation (bold 13px red `#e74c3c`, right side near y=270):** "4 of 12 lines break on the second machine".
- **Caption (12px `#444`, bottom right):** "flag support per published GNU and POSIX docs; script contents illustrative".

## Why a Standard From 1988 Still Decides Where Your Code Runs

**Tags:** `where it's used` (blue), `data pipelines` (green), `history` (orange)

- **Cron and CI** — nightly jobs, Docker builds, and CI runners execute shell scripts on machines you didn't pick
- **The `#!/bin/sh` line** — that shebang promises a POSIX shell, which may be dash or ash, not bash
- **Longevity** — POSIX.1 (1988) covered C system calls; POSIX.2 (1992) added the shell and utilities
- **One spec today** — since 2001 POSIX and the Single UNIX Specification share one merged text
- **Still current** — the standard keeps being revised (2001, 2008, 2017, 2024 editions) rather than replaced
- **The payoff** — a pipeline written to the spec outlives laptops, distros, and cloud vendors

*Example (italic):* A data scientist's cleanup script from a 2010 Linux box still runs in a 2026 Alpine container because both implement the same 1992 shell-and-utilities spec.

**Key point:** Every place a data scientist's code runs unattended — cron, containers, CI — is a machine chosen by someone else; POSIX is what makes "it ran on my laptop" transfer.

### Visualization (canvas `c3`, 720×300)

Timeline of the standard's editions on a horizontal axis from 1985 to 2025, with milestone markers and labels.

- **Title (bold 15px, `#1a5276`, top center):** "One Contract, Revised for Nearly Four Decades".
- **Axis:** 2px `#999` horizontal line at y=170 from x=60 to x=660; x maps years 1985–2025 linearly (15 px/yr); 12px `#444` tick labels at 1985, 1995, 2005, 2015, 2025.
- **Milestones:** hardcoded array of (year, label, color): `[1988, "POSIX.1 — C system calls", blue #2a78d6]`, `[1992, "POSIX.2 — shell & utilities", green #008300]`, `[2001, "merged with Single UNIX Spec", violet #4a3aa7]`, `[2008, "major revision", aqua #199e70]`, `[2024, "current edition", orange #d95926]`.
- **Marker style:** 7px-radius filled circles on the axis; 12px labels alternating above (y=120) and below (y=205) the line with 1px `#6b7280` leader lines.
- **Annotation (bold 13px ink `#1a5276`, centered near y=60):** "scripts written to the 1992 spec still run today".
- **Caption (12px `#444`, bottom right):** "edition years per IEEE 1003 publication history".

## Linux Is Not POSIX, and Bash Is Not sh

**Tags:** `common mistake` (red), `bashisms` (orange), `spec vs system` (blue)

- **The confusion** — treating "runs on my Linux box" as proof of POSIX compliance; Linux is not certified
- **The irony** — macOS is a certified UNIX, so BSD-flavored macOS is often closer to the spec than GNU
- **Bashisms** — `[[ ]]`, arrays, and `==` inside `[ ]` are bash features absent from the POSIX shell
- **The silent trap** — `#!/bin/sh` on Debian and Alpine runs dash or ash, where bashisms fail at runtime
- **The fix** — either write pure POSIX sh, or say `#!/bin/bash` honestly and require bash everywhere

*Example (italic):* A deploy script using `[[ -f config ]]` passes on the laptop's bash but dies in the Alpine container's ash with "[[: not found" — the shebang said `sh`, the code said bash.

**Common mistake:** Assuming your daily environment is the standard. POSIX is a document; Linux, GNU, and bash each add extras on top, and the extras are exactly what fails to travel.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: an in-place edit written with a GNU extension (crashes on macOS) vs the POSIX-portable rewrite (runs everywhere).

- **Title (bold 15px, `#1a5276`, top center):** "The sed -i Trap: Extension vs Contract".
- **Row 1 (y=95), label 12px `#444` at x=20:** "GNU extension"; blue `#2a78d6` rounded box at x=150 labeled "sed -i 's/,/;/' f" (12px monospace), 3px arrow to a green `#008300` box at x=360 labeled "Linux: edits f", 3px arrow to a red `#e74c3c` box at x=545 labeled "macOS: misparsed args" with bold 12px red "✗ cron job dies".
- **Row 2 (y=205), label:** "POSIX rewrite"; blue box at x=150 labeled "sed 's/,/;/' f > t && mv t f", 3px arrow to a green box at x=400 labeled "Linux: edits f", arrow to a green box at x=560 labeled "macOS: edits f" with bold 12px green "✓".
- **Box style:** 140–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the extension is convenient; the contract is what travels".
- **Caption (12px `#444`, bottom right):** "sed -i is GNU/BSD-divergent; the redirect+mv form is pure POSIX".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 2,847 order count and the 12-command script are invented and labeled illustrative; the 8-portable / 4-GNU split matches published GNU vs POSIX flag documentation; timeline years (1988 / 1992 / 2001 / 2008 / 2024) are the actual IEEE 1003 edition dates; macOS UNIX certification and Linux's non-certified status are documented public facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
