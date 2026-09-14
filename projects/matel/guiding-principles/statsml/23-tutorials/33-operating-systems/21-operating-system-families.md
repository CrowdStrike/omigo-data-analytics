# Operating System Families

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Operating System Families

**Subtitle:** Every operating system you will meet belongs to one of a handful of families — here is each family's root, its major members, and what actually separates them

## Unix: The Single Ancestor

**Tags:** `core idea` (blue), `lineage` (green), `1969` (orange)

- **Unix (1969)** — Thompson and Ritchie built it at Bell Labs on a spare minicomputer
- **Rewritten in C (1973)** — the rewrite let one OS move to new hardware instead of dying with it
- **Version 6 (1975)** — shipped to universities with source, which is how the ideas spread
- **The split** — AT&T's commercial System V and Berkeley's academic BSD diverged in the 1980s
- **The commercial branch** — Solaris, AIX and HP-UX each grew from System V for one vendor's hardware
- **POSIX (1988)** — a written contract that let code compile on every branch at once
- **Linux is not on this tree** — it copies Unix's interfaces but shares none of its source code

*Example (italic):* A shell pipeline typed on a Mac in 2026 uses the same syntax a Bell Labs researcher typed in 1975.

**Key point:** Nearly every server, phone and Mac in use descends from one 1969 codebase — and the one major exception, Linux, reimplemented its interfaces from scratch rather than inheriting them.

### Visualization (canvas `c1`, 720×340)

Left-to-right branching tree from a single Unix root, drawn with organic bezier branches, plus one detached dashed node for Linux.

- **Title (bold 15px, `#1a5276`, top center):** "One 1969 Root, Every Branch After It".
- **Root node (rounded 8px, x=30 y=150 w=150 h=46):** fill `rgba(26,82,118,0.15)`, 2px `#1a5276` border; bold 13px "Unix" and 11px "Bell Labs, 1969".
- **Level-2 nodes (rounded 8px, x=260 w=200 h=34, at y = 58, 106, 154, 202):** "Research Unix — V6, 1975" (mute), "BSD — Berkeley, 1977" (aqua `#199e70`), "System V — AT&T, 1983" (violet `#4a3aa7`), "Solaris, AIX, HP-UX" (mute).
- **Level-3 nodes (rounded 8px, x=510 w=190 h=34):** "macOS, iOS — via BSD + Mach" at y=106 in aqua; "POSIX contract, 1988" at y=154 in violet.
- **Branches:** 2px bezier curves from the root's right edge to each level-2 node's left edge, and from BSD/System V to their level-3 nodes, coloured to the destination node.
- **Detached Linux node (rounded 8px, x=260 y=270 w=380 h=34):** white fill, dashed 2px `#2a78d6` border; bold 12px `#2a78d6` label "Linux, 1991 — Unix-like, but zero Unix source code".
- **Annotation (bold 12px magenta `#d55181`, x=470 y=250, centered):** "one codebase under nearly every server and phone".
- **Caption (12px `#444`, bottom right):** "founding years documented; branch positions schematic".

## BSD: One Berkeley Source, Three Philosophies

**Tags:** `worked example` (blue), `forks` (green), `licence` (orange)

- **Berkeley (1977)** — Berkeley added to AT&T's source, then spent years removing it to ship freely
- **The lawsuit** — AT&T sued in 1992 over that code; the settlement forced a clean release
- **4.4BSD-Lite (1994)** — the last Berkeley version, and the root of every BSD alive now
- **FreeBSD (1993)** — optimises for performance and server throughput on commodity hardware
- **OpenBSD (1995)** — forked from NetBSD choosing audited correctness over speed or features
- **NetBSD (1993)** — optimises for portability, running on dozens of processor architectures
- **Whole-system tree** — a BSD ships kernel and userland as one versioned unit, unlike Linux distros
- **The licence** — permissive terms let Apple and Sony fold BSD code into closed products

*Example (italic):* The same network stack lineage runs a FreeBSD file server, an OpenBSD firewall, and a NetBSD board no other OS supports.

**Key point:** Three BSDs from one source split on a single question each — speed, safety, or portability — which is why choosing between them is choosing a priority, not a feature list.

### Visualization (canvas `c2`, 720×340)

Trunk-and-three-branches tree: the Berkeley trunk narrows through the 1994 clean release, then splits into three labelled branches with their chosen priority.

- **Title (bold 15px, `#1a5276`, top center):** "One Trunk, Three Priorities".
- **Trunk node (rounded 8px, x=30 y=148 w=190 h=46):** fill `rgba(25,158,112,0.15)`, 2px `#199e70`; bold 13px "Berkeley (BSD)" and 11px "first release 1977".
- **Waist node (rounded 8px, x=265 y=153 w=185 h=36):** fill `rgba(107,114,128,0.12)`, 2px `#6b7280`; 12px "4.4BSD-Lite, 1994 — clean release".
- **Branch nodes (rounded 8px, x=505 w=185 h=44, at y = 62, 148, 234):** "FreeBSD — 1993" filled `rgba(42,120,214,0.12)` border `#2a78d6`; "OpenBSD — 1995" filled `rgba(231,76,60,0.10)` border `#e74c3c`; "NetBSD — 1993" filled `rgba(201,133,0,0.12)` border `#c98500`; each with a bold 12px name and an 11px priority line: "performance", "audited security", "portability".
- **Branches:** 3px bezier curves trunk → waist → each branch node, coloured to the destination.
- **Lawsuit marker:** a small 11px `#e74c3c` label "AT&T lawsuit, 1992–94" with a short bracket under the trunk→waist segment at y=210.
- **Annotation (bold 12px violet `#4a3aa7`, x=360 y=300, centered):** "same source, one question each: fast, safe, or everywhere".
- **Caption (12px `#444`, bottom right):** "fork years and priorities documented".

## Linux: A Kernel, Then a Thousand Distros

**Tags:** `worked example` (blue), `distros` (green), `support tiers` (orange)

- **The kernel (1991)** — Torvalds wrote only the core; it cannot boot into anything usable alone
- **The distro** — a distro is that kernel plus GNU tools, an installer, and a package manager
- **Debian (1993)** — volunteer-run and the base under Ubuntu, Mint, and Raspberry Pi OS
- **Ubuntu (2004)** — a Debian rebuild on a fixed release clock, aimed at desktops and cloud images
- **Red Hat (1994)** — Fedora leads, RHEL freezes for a decade, Rocky and Alma rebuild it free
- **SUSE (1994)** — the European enterprise line, with openSUSE as its community edition
- **Arch and Alpine** — Arch ships continuously; Alpine strips to about 5 MB and dominates containers
- **What the tier decides** — the same kernel, frozen at a different age, decides what breaks

*Example (italic):* A container image built on Alpine and the same service on RHEL run one kernel lineage, but their libraries are years apart.

**Key point:** "Linux" names only the kernel — every argument about Linux is really an argument about a distro's packaging, release clock, and freeze policy.

### Visualization (canvas `c3`, 720×340)

Branching distro tree: the kernel at the root, the "kernel + userland" waist, then family branches fanning to their derivative distros.

- **Title (bold 15px, `#1a5276`, top center):** "One Kernel, Many Packagings".
- **Root node (rounded 8px, x=20 y=148 w=155 h=46):** fill `rgba(230,126,34,0.15)`, 2px `#d95926`; bold 13px "Linux kernel" and 11px "1991".
- **Waist node (rounded 8px, x=205 y=153 w=175 h=36):** fill `rgba(107,114,128,0.12)`, 2px `#6b7280`; 11px "+ GNU userland = usable OS".
- **Family nodes (rounded 8px, x=420 w=130 h=32):** "Debian 1993" at y=58, "Red Hat 1994" at y=124, "SUSE 1994" at y=196, "Arch / Alpine" at y=262; fills at 0.12 alpha of blue, red `#e74c3c`, green `#008300`, violet `#4a3aa7` respectively with matching 2px borders.
- **Leaf nodes (rounded 8px, x=580 w=120 h=28, 11px labels):** "Ubuntu 2004" at y=58 (from Debian); "Fedora" at y=104 and "RHEL, Rocky" at y=146 (both from Red Hat); "openSUSE" at y=198 (from SUSE).
- **Branches:** 2px beziers root → waist → each family node, and family → each leaf, coloured to the destination.
- **Annotation (bold 12px `#d95926`, x=360 y=312, centered):** "the freeze date, not the kernel, decides what breaks".
- **Caption (12px `#444`, bottom right):** "distro founding years documented; Alpine size approximate".

## Windows: Two Lineages That Merged at XP

**Tags:** `worked example` (blue), `two lines` (green), `NT` (orange)

- **MS-DOS (1981)** — a single-user command interpreter with no memory protection at all
- **Windows 1.0–3.1** — a graphical program running on top of DOS, not an OS in its own right
- **The 9x line** — 95, 98 and ME kept the DOS underpinnings and their crash behaviour
- **Windows NT (1993)** — Dave Cutler's team built a real kernel with protection and multiprocessing
- **The VMS inheritance** — Cutler came from DEC, and NT's design shows VMS ideas, not Unix ones
- **XP (2001)** — the two lines merged: consumers finally got the NT kernel underneath
- **The moat** — every modern Windows is NT, and its promise to run old software constrains it

*Example (italic):* A 1998 home PC and a 1998 office PC both said "Windows" on the box while running two entirely different kernels.

**Key point:** Windows was two operating systems for a decade — a DOS shell for homes and a real kernel for offices — and everything since XP is the office one.

### Visualization (canvas `c4`, 720×340)

Two-lane timeline tree, 1981 to 2026: the DOS lane ends in a stop marker while the NT lane continues, with a merge arrow at 2001.

- **Title (bold 15px, `#1a5276`, top center):** "Two Lines, One Survivor".
- **X scale:** `xOf(year) = 70 + (year - 1981) / (2026 - 1981) * 590`; gridlines 1px `#e5e9ef` with 12px `#444` labels at 1981, 1993, 2001, 2015, 2026 along y=316.
- **DOS lane (y=100, red `#e74c3c`, 3px):** runs 1981 to 2001, ending in a 10px red "✕"; filled dots with 11px labels above at MS-DOS 1981, Windows 3.1 1992, Windows 95, Windows ME 2000; bold 12px lane label "DOS line — no memory protection".
- **NT lane (y=225, blue `#2a78d6`, 3px):** runs 1993 to 2026; filled dots with 11px labels below at NT 3.1 1993, Windows 2000, XP 2001, Windows 10, Windows 11; bold 12px lane label "NT line — a real kernel".
- **Merge arrow:** a 2px `#4a3aa7` arrow from the DOS stop marker at (xOf(2001), 112) down to the NT lane at (xOf(2001), 213), with a bold 11px `#4a3aa7` label "consumers move to NT".
- **VMS input:** a small dashed 1px `#6b7280` arrow entering the NT lane start from above-left, labelled 11px "VMS ideas via Cutler".
- **Annotation (bold 12px magenta `#d55181`, x=430 y=160, centered):** "every Windows since 2001 is the NT line".
- **Caption (12px `#444`, bottom right):** "release years documented; lane positions schematic".

## Apple: Two Inputs Converged, Then Fanned Out

**Tags:** `worked example` (blue), `Darwin` (green), `convergence` (orange)

- **Classic Mac OS (1984)** — the original Mac line had no Unix in it and no memory protection
- **NeXTSTEP (1989)** — Jobs's NeXT combined a Mach kernel with BSD userland and Objective-C
- **The acquisition (1997)** — Apple bought NeXT and made NeXTSTEP the base of its next system
- **Darwin and XNU** — the open-source core; XNU joins Mach with BSD code in one kernel
- **Mac OS X (2001)** — Darwin under a new interface, which is why Terminal behaves like Unix
- **iOS (2007)** — the phone runs the same Darwin core with a different interface layer
- **One core, four products** — macOS, iOS, watchOS and tvOS are all Darwin with different shells

*Example (italic):* The same `ls` and `grep` a server admin types work unchanged in a Mac Terminal, because both sit on BSD userland.

**Key point:** Apple's current systems are not descended from the original Macintosh — the 1984 line was a dead end, and everything since 2001 grows from NeXT's Mach-plus-BSD core.

### Visualization (canvas `c5`, 720×340)

Converge-then-fan tree: two inputs merge into NeXTSTEP, narrow through Darwin, then fan into four products, with the Classic Mac OS branch drawn as a dead end.

- **Title (bold 15px, `#1a5276`, top center):** "Two Inputs, One Core, Four Products".
- **Input nodes (rounded 8px, x=20 w=150 h=32, 11px labels):** "Mach kernel — CMU" at y=76 border violet `#4a3aa7`; "BSD userland" at y=140 border aqua `#199e70`.
- **NeXTSTEP node (rounded 8px, x=215 y=92 w=150 h=44):** fill `rgba(42,120,214,0.12)`, 2px `#2a78d6`; bold 12px "NeXTSTEP" and 11px "1989 · bought 1997".
- **Darwin node (rounded 8px, x=405 y=92 w=140 h=44):** fill `rgba(26,82,118,0.15)`, 2px `#1a5276`; bold 12px "Darwin / XNU" and 11px "one kernel".
- **Product leaves (rounded 8px, x=585 w=115 h=26, 11px):** "macOS 2001" at y=48, "iOS 2007" at y=84, "watchOS" at y=120, "tvOS, iPadOS" at y=156.
- **Branches:** 2px beziers inputs → NeXTSTEP → Darwin → each leaf, coloured to the destination.
- **Dead-end branch (y=250):** a dashed 2px `#e74c3c` line from x=60 to x=430 with a 10px red "✕" at its end; 12px `#e74c3c` label "Classic Mac OS, 1984–2001 — not Unix, no protected memory"; a small 11px `#6b7280` note "shared no code with what replaced it".
- **Annotation (bold 12px green `#008300`, x=400 y=296, centered):** "the Mac's software ancestor is NeXT, not the 1984 Mac".
- **Caption (12px `#444`, bottom right):** "acquisition and release years documented".

## Android and ChromeOS: Linux Without the Distro

**Tags:** `common mistake` (red), `Linux kernel` (orange), `not a distro` (blue)

- **Both start at Linux** — each uses the Linux kernel, so both are Linux by the strictest reading
- **Neither is a distro** — a Debian program will not run on either without being rebuilt
- **Android (2008)** — Google bought Android Inc in 2005 and shipped on phones three years later
- **Bionic, not GNU** — Android swapped GNU's C library for its own, breaking binary compatibility
- **The app runtime** — Android apps target a managed runtime, not the kernel's own interfaces
- **ChromeOS (2011)** — a Linux machine whose one real application is the browser
- **Verified boot** — ChromeOS checks the system's signature at every start and self-heals if it fails
- **A/B partitions** — an update writes to the spare half, so a bad update reboots into the old one

*Example (italic):* A compiled command-line tool that runs on any Ubuntu server fails on an Android phone, despite both running a Linux kernel.

**Common mistake:** Reading "runs the Linux kernel" as "runs Linux software". Android and ChromeOS kept the kernel and replaced almost everything above it, so kernel lineage predicts nothing about whether a program will run.

### Visualization (canvas `c6`, 720×340)

Root-and-two-branches tree with a kept-versus-replaced comparison: the Linux kernel at the left, Android branching up and ChromeOS down, each annotated with what it discarded.

- **Title (bold 15px, `#1a5276`, top center):** "Same Kernel, Different Everything Else".
- **Root node (rounded 8px, x=20 y=148 w=150 h=46):** fill `rgba(230,126,34,0.15)`, 2px `#d95926`; bold 13px "Linux kernel" and 11px "shared by both".
- **Android branch node (rounded 8px, x=250 y=58 w=160 h=40):** fill `rgba(0,131,0,0.12)`, 2px `#008300`; bold 12px "Android — 2008".
- **ChromeOS branch node (rounded 8px, x=250 y=240 w=160 h=40):** fill `rgba(42,120,214,0.12)`, 2px `#2a78d6`; bold 12px "ChromeOS — 2011".
- **Branches:** 3px beziers from the root to each branch node, coloured to the destination.
- **Per-branch detail lists (11px, x=440, items at 18px spacing):** under Android at y=54: green `#008300` "keeps: kernel, drivers" then red `#e74c3c` "replaces: GNU libc → Bionic", "replaces: apps → managed runtime"; under ChromeOS at y=236: green "keeps: kernel, Gentoo build" then blue `#2a78d6` "adds: verified boot", "adds: A/B partition updates".
- **Divider:** dashed 1px `#e5e9ef` horizontal line at y=170 spanning x=200 to x=700.
- **Annotation (bold 12px `#e74c3c`, x=430 y=178, centered):** "kernel lineage does not mean a program will run".
- **Caption (12px `#444`, bottom right):** "release years and component swaps documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then six `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%). Six sections exceeds the usual 3–4 because the page's purpose is one section per OS family.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...` (all ≤105 characters so they do not wrap at the 50/50 split), one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** all six are intrinsic 720×340. The shared `setup(id)` helper reads each canvas's logical size from its `width`/`height` attributes once and caches it in `dataset` (the attributes get overwritten with device pixels, so they cannot be re-read on resize), then sizes the backing store to the rendered width × `window.devicePixelRatio` and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Shared drawing helpers:** `roundedBox(ctx,x,y,w,h,r,fill,border,bw)`; `branch(ctx,x1,y1,x2,y2,color,width)` drawing a horizontal-control-point bezier so every tree edge curves organically; `arrowLine` for the Windows merge arrow; `xMark(ctx,x,y,color)` for the dead-end stop markers.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** no random generation and no quantities to compute — every chart is a lineage diagram of hardcoded literal node lists. Documented facts: Unix at Bell Labs 1969 by Thompson and Ritchie, the 1973 C rewrite, Version 6's 1975 university distribution, the System V (1983) and BSD (from 1977) divergence, POSIX 1988, Linux's 1991 start as an independent reimplementation sharing no Unix source; the 1992–94 AT&T/BSDi litigation and 4.4BSD-Lite in 1994; FreeBSD and NetBSD 1993, OpenBSD forked from NetBSD 1995; Debian and Slackware 1993, Red Hat and SUSE 1994, Ubuntu 2004; MS-DOS 1981, Windows 3.1 1992, the 95/98/ME line, Windows NT 3.1 in 1993 under Dave Cutler (previously of DEC, VMS), XP in 2001 merging the consumer line onto NT; Classic Mac OS 1984–2001, NeXTSTEP from 1989, Apple's NeXT acquisition completed 1997, Mac OS X 10.0 in 2001, XNU combining Mach and BSD, iPhone OS 2007; Android Inc acquired 2005 and first shipped 2008 with Bionic in place of GNU libc, ChromeOS shipping 2011 with verified boot and A/B partition updates. Alpine's ~5 MB base size is stated as approximate.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
