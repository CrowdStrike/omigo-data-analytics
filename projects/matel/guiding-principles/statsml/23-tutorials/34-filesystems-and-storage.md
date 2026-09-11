# Filesystems & Storage

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Filesystems & Storage

**Subtitle:** How a disk full of numbered blocks becomes named files in folders — and how the major filesystems keep those books honest through power cuts, lying disks, and networks.

## Cards

Each card links to a topic page under `filesystems/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | FOUNDATIONS | What a Filesystem Does | [34-filesystems-and-storage/01-what-a-filesystem-does.md](34-filesystems-and-storage/01-what-a-filesystem-does.md) | A disk is just millions of numbered storage blocks — the filesystem is the bookkeeping layer that turns them into named files inside folders. | blocks, metadata, bookkeeping |
| 2 | FAT FAMILY | FAT — The DOS Legacy | [34-filesystems-and-storage/02-fat-the-dos-legacy.md](34-filesystems-and-storage/02-fat-the-dos-legacy.md) | One big lookup table where entry N says which piece of a file comes after piece N — the whole filesystem is a game of follow-the-chain. | lookup table, cluster chains, DOS legacy |
| 3 | FAT FAMILY | exFAT | [34-filesystems-and-storage/03-exfat.md](34-filesystems-and-storage/03-exfat.md) | Microsoft stretched the old FAT design so flash cards could hold giant files — and it became the one format every gadget can read. | flash cards, big files, universal format |
| 4 | MODERN LOCAL | NTFS | [34-filesystems-and-storage/04-ntfs.md](34-filesystems-and-storage/04-ntfs.md) | Windows' filesystem keeps one master table where every file is a row — with a journal that survives power cuts and a permission list on every file. | master file table, journaling, permissions |
| 5 | MODERN LOCAL | ext4 | [34-filesystems-and-storage/05-ext4.md](34-filesystems-and-storage/05-ext4.md) | The unglamorous filesystem behind most Linux machines — a careful clerk that records where every file lives and keeps a journal so a power cut can't scramble the books. | linux default, inodes, journal |
| 6 | MODERN LOCAL | APFS | [34-filesystems-and-storage/06-apfs.md](34-filesystems-and-storage/06-apfs.md) | Apple's copy-on-write filesystem never copies data it can share — a duplicate is just a second name pointing at the same blocks. | copy-on-write, clones, snapshots |
| 7 | MODERN LOCAL | ZFS | [34-filesystems-and-storage/07-zfs.md](34-filesystems-and-storage/07-zfs.md) | Checksums every block it writes and re-checks it on every read — the filesystem that assumes the disk is lying until proven otherwise. | checksums, bit rot, self-healing |
| 8 | BEYOND ONE DISK | SMB & NFS | [34-filesystems-and-storage/08-smb-and-nfs.md](34-filesystems-and-storage/08-smb-and-nfs.md) | Make a folder on another machine look like a local folder — every file operation quietly becomes a message across the network. | network shares, remote folders, protocols |
| 9 | BEYOND ONE DISK | Filesystem Comparison | [34-filesystems-and-storage/09-filesystem-comparison.md](34-filesystems-and-storage/09-filesystem-comparison.md) | The major filesystems on one scorecard — journaling, checksums, snapshots, file-size limits, and which computers can even read the drive. | scorecard, trade-offs, compatibility |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "FOUNDATIONS" `#2980b9`, "FAT FAMILY" `#27ae60`, "MODERN LOCAL" `#8e44ad`, "BEYOND ONE DISK" `#e67e22`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#e67e22`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
