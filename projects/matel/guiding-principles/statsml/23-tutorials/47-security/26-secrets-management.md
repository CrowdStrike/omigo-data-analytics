# Secrets Management

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Secrets Management

**Subtitle:** Committing a password to git is like shouting it in a crowded room — deleting the file later doesn't make anyone forget it

## The Password That Can't Live in Git

**Tags:** `core idea` (blue), `git history` (orange), `leaks` (red)

- **The shortcut** — a developer hardcodes the database password as a plain string in config.py to get a demo working
- **The push** — the commit goes to the shared repo; CI clones it, three teammates pull it
- **The forever part** — deleting the file adds a new commit; the old commit with the password stays in history
- **The spread** — every clone, fork, and CI cache now carries the secret; there is no recall button
- **The scanners** — bots crawl public commits and flag exposed keys within minutes, a publicly documented pattern

*Example (italic):* The password is "removed" on Friday, but six full copies of the repo — history included — still hold it, and a fork made Thursday keeps it forever.

**Key point:** A secret committed to source control is copied to every clone and preserved in every commit — deleting the file changes the working tree, not the history.

### Visualization (canvas `c1`, 720×300)

Step chart of how many copies of the secret exist as repo events happen — the line only ever goes up, including after the file is deleted.

- **Title (bold 15px, `#1a5276`, top center):** "Copies of the Secret Only Go Up — Even After You Delete the File".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = six event labels `["commit", "push", "CI clone", "3 pulls", "file deleted", "fork"]` evenly spaced, 12px `#444`, angled or shortened to fit; y = copies 0 to 8, gridlines `#e5e9ef` at 2/4/6.
- **Step line:** blue `#2a78d6` 3px step-after line through copies `[1, 2, 3, 6, 6, 7]` at the six events, 4px round dots at each step.
- **Delete marker:** vertical dashed `#6b7280` (dash 4/3) line at the "file deleted" event, 12px `#6b7280` label "delete commit lands" at its top.
- **Annotation (bold 13px red `#e74c3c`, above the flat segment at "file deleted", y≈95):** "still 6 copies — history keeps the password".
- **Caption (12px `#444`, bottom right):** "copy counts illustrative".

## One Leaked Key: 3,120 Minutes vs 31

**Tags:** `worked example` (blue), `static vs dynamic` (green)

- **The leak** — a key is pushed to a public repo Monday 10:00; abuse starts 10:09, nine minutes later
- **Static key** — nobody notices until an odd bill; it is revoked by hand Wednesday 14:09
- **The window** — Monday 10:09 to Wednesday 14:09 is exactly 52 hours = 3,120 minutes of access
- **Dynamic credential** — issued 09:40 with a 60-minute lifetime, it expires at 10:40 on its own
- **Hand-check** — abuse at 10:09, expiry at 10:40: the attacker window is 31 minutes, ~100× smaller

*Example (italic):* The same leak costs 3,120 minutes of database access with a static key, but only 31 minutes with a one-hour dynamic credential — and no human had to notice.

**Key point:** A short-lived credential turns "we must detect and revoke" into "it expires by itself" — the blast radius shrinks from days to the credential's remaining lifetime.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart comparing the attacker's access window for the same 10:00 leak: static key vs one-hour dynamic credential.

- **Title (bold 15px, `#1a5276`, top center):** "Same Leak at 10:00 Monday: How Long the Thief Keeps Access".
- **Axis:** horizontal 2px `#999` baseline at x=250, bars extend right, max width 420; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (bars 22px tall, at y = 105 and y = 195), each with a left-aligned 12px `#444` two-line label ending at x=240:**
  - "static key — revoked by hand Wed 14:09": red `#e74c3c` bar width 420, bold 12px red end label "3,120 min (52 h)"
  - "dynamic 1-hour credential — expires 10:40": green `#008300` bar width 34, bold 12px green end label "31 min"
- **Leak marker:** small vertical dashed `#6b7280` tick at x=250 spanning both rows, 11px `#6b7280` label "abuse starts 10:09 (leak 10:00)" beneath the baseline.
- **Annotation (bold 13px green `#008300`, centered near y=250):** "~100× smaller window — and it closes with no human in the loop".
- **Caption (12px `#444`, bottom right):** "timeline illustrative; bar widths schematic, minutes exact".

## The Escalation Ladder: From Env Vars to No Secret at All

**Tags:** `defense in depth` (green), `where it's used` (blue), `rotation` (orange)

- **Env vars** — better than code: the secret leaves the repo, but sits plaintext on disk and in process listings
- **Secret manager** — one encrypted store; apps fetch at startup and every read is access-controlled and audited
- **Dynamic credentials** — the manager issues a per-instance database credential valid for an hour
- **Workload identity** — the platform attests who the app is, so there is no bootstrap secret to steal at all
- **Rotation** — replacing secrets on a schedule is the discipline that catches you when prevention fails

*Example (italic):* An app on the top rung starts with zero stored secrets: the platform vouches for its identity, and it receives a fresh one-hour database credential nobody ever typed.

**Key point:** Each rung removes a place the secret can leak from — code, then disk, then long-lived validity, then finally the existence of a stored secret at all.

### Visualization (canvas `c3`, 720×300)

Ascending staircase of five rounded boxes, one per rung, each with a one-line residual-risk note beneath it — risk color cools as the ladder climbs.

- **Title (bold 15px, `#1a5276`, top center):** "Five Rungs: What Can Still Leak at Each Step".
- **Boxes (130px wide, 44px tall, 8px radius, 12px `#2c3e50` bold text, centers ascending left to right):** at (x=25, y=200), (x=163, y=165), (x=301, y=130), (x=439, y=95), (x=577, y=60) — x is the box's left edge, y its top edge.
  - "hardcoded in git" — fill `rgba(231,76,60,0.15)`, border 2px `#e74c3c`
  - "environment variable" — fill `rgba(217,89,38,0.15)`, border 2px `#d95926`
  - "secret manager" — fill `rgba(42,120,214,0.15)`, border 2px `#2a78d6`
  - "dynamic 1-h credential" — fill `rgba(0,131,0,0.12)`, border 2px `#008300`
  - "workload identity" — fill `rgba(25,158,112,0.15)`, border 2px `#199e70`
- **Risk notes (11px `#6b7280`, centered under each box, 6px gap):** "in history forever" / "plaintext on disk, in ps" / "encrypted, audited reads" / "leak expires in ≤60 min" / "no stored secret to steal".
- **Arrows:** 2px `#6b7280` arrows from each box's right edge to the next box's left edge, stepping upward.
- **Annotation (bold 13px violet `#4a3aa7`, lower right near y=255):** "every rung removes a place the secret can leak from".

## Deleting the File Doesn't Revoke the Key

**Tags:** `common mistake` (red), `rotation` (orange)

- **The mistake** — the leak is found, the file is deleted, history is force-push scrubbed: "fixed"
- **Still valid** — the key itself was never rotated; the copy the scanner grabbed at 10:09 still works
- **Right order** — rotate the credential first so the leaked value is dead, then clean the repo
- **The routine** — scheduled rotation means even an unnoticed leak dies at the next rotation
- **The tripwire** — a secret scanner in CI rejects the commit before it ever reaches the shared repo

*Example (italic):* A team scrubs the repo Tuesday and relaxes; the attacker's saved copy of the key keeps working until the key itself is rotated on Friday.

**Common mistake:** Treating the repo as the thing to fix. The repo is the billboard; the key is the lock — once a secret is exposed, rotate it immediately and treat the cleanup as cosmetic.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: responding to a leak by scrubbing the repo (key still works) vs rotating the key first (old key dead), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Leak Response: Scrub the Repo vs Rotate the Key".
- **Row 1 (boxes at y=85), label 12px `#444` at x=20:** "scrub only"; orange `#d95926` rounded box at x=150 labeled "delete file, rewrite history" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "attacker's copy still works" with bold 12px red "✗ access continues".
- **Row 2 (boxes at y=200), label:** "rotate first"; green `#008300` rounded box at x=150 labeled "rotate key — old value dead", 3px arrow to a green box at x=420 labeled "then scrub as cleanup" with bold 12px green "✓ access ends now".
- **Box style:** 200–220px wide, 44px tall, 8px radius, fills `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Tripwire strip:** thin dashed `#2a78d6` vertical line at x=120 spanning both rows, 11px `#2a78d6` rotated or top label "CI secret scan blocks the commit here next time".
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the repo is the billboard; the key is the lock — change the lock first".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); copy counts and the leak timeline are invented and labeled illustrative; the 3,120 / 31 minute windows and the ~100× ratio follow exactly from the stated clock times (abuse Mon 10:09 → revoked Wed 14:09; abuse 10:09 → 10:40 expiry) and must match between text and chart.
- **Framing:** defensive/educational — the page teaches how to protect credentials, not how to exploit leaked ones; no real service names, no real key formats.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: realistic credential strings on this page were converted to generic placeholders — for illustration only, and to avoid false positives from secret scanners."
