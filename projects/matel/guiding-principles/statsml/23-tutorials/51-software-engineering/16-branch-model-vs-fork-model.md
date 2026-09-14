# Branch Model vs Fork Model

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Branch Model vs Fork Model

**Subtitle:** Both end in a pull request — the real choice between sharing one repo and forking personal copies is who you trust with write access

## Two Ways to Get the Same Fix In

**Tags:** `core idea` (blue), `trust` (green), `collaboration` (orange)

- **The branch model** — six teammates share one repository; each pushes feature branches straight into it
- **The fork model** — contributors get no write access; each forks a personal copy and pushes there instead
- **The proposal** — a fork's change arrives as a pull request from the fork repo into the canonical repo
- **The variable** — the models differ in trust, not tooling: who is allowed to write to the canonical repo
- **The numbers** — 6 teammates all hold write; 2,300 contributors, but only 3 maintainers hold write

*Example (italic):* The same one-line fix ships both ways — a teammate pushes branch `fix-rounding` into the shared team repo; a stranger pushes it to her personal fork and opens a pull request across repos.

**Key point:** The branch model assumes everyone is already trusted (the standard company setup); the fork model was invented for open source, where thousands of strangers must contribute without ever being trusted with write access.

### Visualization (canvas `c1`, 720×300)

Side-by-side topology diagram: branch model (one shared repo, all contributors push) vs fork model (canonical repo, personal forks, pull requests crossing between repos).

- **Title (bold 15px, `#1a5276`, top center):** "One Repo Everyone Writes vs Forks Nobody Needs Write For".
- **Divider:** vertical dashed `#6b7280` (dash 4/3) line at x=360 from y=45 to y=270; 12px `#6b7280` panel labels "branch model" at x=180 and "fork model" at x=540, y=52.
- **Left panel:** blue `#2a78d6` rounded box (150×44, 8px radius, fill `rgba(42,120,214,0.15)`) centered at (185, 95) labeled "shared repo" (12px `#2c3e50`); six 11px-radius `#2a78d6` circles along y=215 at x = 65, 113, 161, 209, 257, 305; 2px blue double-headed arrows from each circle up to the repo box; 12px `#444` label at (185, 250) "6 contributors — all can push (write ✓)".
- **Right panel:** blue rounded box (170×44) centered at (540, 88) labeled "canonical repo — write: 3" (12px); three violet `#4a3aa7` rounded boxes (90×36, fill `rgba(74,58,167,0.12)`) centered at y=185, x = 435, 540, 645, each labeled "fork"; grey `#6b7280` 2px downward arrows from canonical box to each fork labeled "fork/clone" (11px, once, near x=470); green `#008300` 2px upward arrows from each fork back to the canonical box labeled "PR" (bold 12px green, once, near x=610); 12px `#444` label at (540, 250) "2,300 contributors — write ✗".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=282):** "the topology encodes the trust boundary".
- **Caption (12px `#444`, bottom right):** "contributor counts illustrative".

## origin, upstream, and Keeping a Fork Alive

**Tags:** `worked example` (blue), `remotes` (green)

- **Two remotes** — the local clone calls your fork `origin` and the canonical repo `upstream`
- **Push vs propose** — you push branches to `origin`; only a pull request can reach `upstream`
- **The drift** — a fork never syncs itself: while your PR waits, `upstream` keeps gaining commits
- **Hand-check** — at 2 commits landing upstream per day, 7 days of waiting = 14 commits behind
- **The sync** — `git fetch upstream` then rebase onto `upstream/main` brings the drift back to 0

*Example (italic):* Day 7: your fork reads "14 commits behind"; one `git fetch upstream && git rebase upstream/main` plus `git push origin`, and it reads 0 behind again.

**Key point:** A fork is a real, separate repository that never updates itself — the `upstream` remote plus a fetch-and-rebase habit is the only thing keeping it from silently going stale.

### Visualization (canvas `c2`, 720×300)

Top half: three-box remote flow diagram (upstream, fork, laptop). Bottom half: mini line chart of the fork's commits-behind count over the 7-day wait, dropping to 0 at the sync.

- **Title (bold 15px, `#1a5276`, top center):** "Three Repos, Two Remotes, One Pull Request".
- **Boxes (rounded 8px, 42px tall, 12px `#2c3e50` text):** blue `#2a78d6` fill `rgba(42,120,214,0.15)` box (190 wide) at x=55, y=55 labeled "upstream — canonical"; violet `#4a3aa7` fill `rgba(74,58,167,0.12)` box (190 wide) at x=475, y=55 labeled "origin — your fork"; blue box (170 wide) at x=275, y=125 labeled "laptop clone".
- **Arrows (2px):** green `#008300` arrow from origin box left edge to upstream box right edge along y=76, bold 12px green label "pull request" above it; blue `#2a78d6` arrow from laptop clone up-right into origin box, 12px blue label "git push origin"; grey `#6b7280` dashed (4/3) arrow from upstream box down-right into laptop clone, 12px `#6b7280` label "git fetch upstream".
- **Mini chart:** baseline 2px `#999` at y=265 from x=60 to x=660; y-axis 0 to 14 commits mapped to y=265 up to y=195, gridline `#e5e9ef` at 7; x = days 0 to 7, 12px `#444` tick labels "d0"–"d7" every day, x step 80px starting at x=60.
- **Drift line:** orange `#d95926` 3px line through days `[0, 1, 2, 3, 4, 5, 6, 7]`, commits behind `[0, 2, 4, 6, 8, 10, 12, 14]`; green `#008300` 3px vertical drop at day 7 from 14 to 0 with a filled green dot at (d7, 0).
- **Annotation (bold 12px green `#008300`, near x=520, y=200):** "fetch + rebase → 0 behind".
- **Caption (12px `#444`, bottom right):** "2 commits/day illustrative".

## Whose Code Does Your CI Just Run?

**Tags:** `where it's used` (blue), `security` (red)

- **Branch CI** — a teammate's branch runs CI with the repo's secrets: deploy keys, tokens, caches
- **Fork CI** — a stranger's PR is arbitrary code; running it with secrets lets them be exfiltrated
- **The sandbox** — fork PRs get a read-only token and no secrets until a maintainer approves the run
- **Real stakes** — CI tokens leaked via fork PRs are a publicly discussed supply-chain attack path
- **The gate** — maintainer review is the moment untrusted code crosses into the trusted side

*Example (italic):* A malicious "fix" adds one line that prints the CI's deploy token into the build log — harmless on a sandboxed fork runner, catastrophic on a trusted one.

**Key point:** The fork model moves the trust question into CI: untrusted pull-request code must run with no secrets and no write token until a human decides to trust it.

### Visualization (canvas `c3`, 720×300)

Two-row flow diagram: a teammate's branch flowing through trusted CI vs a stranger's fork PR flowing through a sandboxed runner and a human approval gate.

- **Title (bold 15px, `#1a5276`, top center):** "Same Pull Request, Two CI Trust Levels".
- **Row 1 (y=95), label 12px `#444` at x=20:** "teammate branch"; blue `#2a78d6` rounded box at x=150 labeled "branch push" (12px), 3px arrow to a green `#008300` box at x=340 labeled "trusted CI — secrets ✓ write ✓", 3px arrow to a green box at x=590 labeled "merge" with bold 12px green "✓".
- **Row 2 (y=205), label:** "stranger's fork PR"; blue box at x=150 labeled "fork PR", 3px arrow to an orange `#d95926` box at x=340 labeled "sandbox CI — secrets ✗ read-only", 3px arrow to a green box at x=590 labeled "review → merge".
- **Box style:** 130–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Danger callout (bold 12px red `#e74c3c`, at x=340, y=150, between the rows):** "secrets + stranger's code = exfiltration".
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "the sandbox exists because the PR author is untrusted by design".

## Picking a Model Is Picking a Trust Boundary

**Tags:** `common mistake` (red), `decision rule` (orange)

- **The mistake** — choosing by size or habit ("we're a big project, so forks") instead of by trust
- **The rule** — same trust domain → branch model; crossing a trust boundary → fork model
- **Inner source** — companies use forks so any team can propose changes to a central platform repo
- **The hybrid** — the platform team branches among themselves; outside teams contribute via forks
- **Same interface** — either way the change lands as a pull request, so review tooling is shared

*Example (italic):* A 40-person company forcing forks between desk-mates adds sync friction for nothing; an open-source repo handing write access to drive-by contributors deletes its only gate.

**Common mistake:** Treating forks as "the serious way" or branches as "the simple way." The two models are trust topologies — pick the one matching who must be kept out of the repo, not the size of the project.

### Visualization (canvas `c4`, 720×300)

Three-row decision diagram mapping "who is contributing" to the collaboration model, with a dashed trust-boundary line that the fork rows must cross.

- **Title (bold 15px, `#1a5276`, top center):** "The Decision Rule: Match the Model to the Trust Boundary".
- **Trust boundary:** vertical dashed `#6b7280` (dash 4/3) line at x=355 from y=55 to y=265; 12px `#6b7280` label "trust boundary" rotated or placed at (355, 48).
- **Left boxes (230×40, rounded 8px, at x=40, 12px `#2c3e50` text), rows at y = 80, 155, 230:** green `#008300` fill `rgba(0,131,0,0.12)` "your own team — trusted"; orange `#d95926` fill `rgba(217,89,38,0.12)` "another team, same company"; red `#e74c3c` fill `rgba(231,76,60,0.12)` "strangers on the internet".
- **Right boxes (250×40, at x=430), same rows:** blue `#2a78d6` fill `rgba(42,120,214,0.15)` "branch model — shared repo"; violet `#4a3aa7` fill `rgba(74,58,167,0.12)` "fork model — inner source"; violet "fork model + sandboxed CI".
- **Arrows (3px `#2c3e50`)** from each left box to its right box; 11px `#6b7280` label "same domain" on the row-1 arrow, "crosses boundary" on the row-2 and row-3 arrows.
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "branches for the trusted, forks for everyone else".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); contributor counts (6 / 2,300 / 3) and the commit-drift series (2 per day, 14 by day 7) are invented and labeled illustrative; the fork/branch mechanics and CI sandbox behavior are factual, not invented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
