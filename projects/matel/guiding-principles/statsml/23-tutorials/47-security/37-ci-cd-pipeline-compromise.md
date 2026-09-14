# CI/CD Pipeline Compromise

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** CI/CD Pipeline Compromise

**Subtitle:** The build server holds more secrets than any developer, and runs whatever the config file says — including a line added last night

## One Pipeline, Three Kinds of Keys

**Tags:** `core idea` (blue), `build-server secrets` (orange), `defensive` (green)

- **The pipeline** — a build runs on every push: it compiles, tests, deploys, publishes, and signs the release
- **Why it holds keys** — it cannot deploy without deploy credentials, publish without a publishing token, or sign without the signing key
- **The config file** — what the build does is defined by a file inside the repository, so changing it is an ordinary code change
- **The inversion** — the most privileged component the team runs is also the one whose instructions are easiest to edit
- **Effective privilege** — anyone who can merge that file can, in effect, run code as the build with all of its credentials
- **Secrets by design** — steps read secrets to work, so a step that prints or transmits one exfiltrates it, and build logs are widely readable
- **Review asymmetry** — reviewers read application code line by line and skim build configuration
- **Cache carry-over** — a tampered cache entry or artifact persists, so one bad build can influence later clean ones

*Example (italic):* Alice opens a routine pull request; the only file it touches is the pipeline config, and it clears review in two minutes (illustrative).

**Key point:** The pipeline's effective privilege is the union of everything it can deploy to, publish to, and sign — while its access control is whatever governs a routine merge.

### Visualization (canvas `c1`, 720×300)

Privilege-union diagram: two individual developers holding one capability each on the left, the build pipeline at the centre, and the six capabilities it holds fanned out on the right.

- **Title (bold 15px, `#1a5276`, top center):** "The Pipeline Holds 6 Capabilities; The Top Developer Holds 1".
- **Left header (bold 12px `#1a5276`, left-aligned at x=30, y=48):** "individual developers".
- **Developer boxes (x=30, w=160, h=44, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, centered 12px `#2c3e50` text on two lines):** at y=60 — "Alice" / "deploy env 1 (1)"; at y=118 — "Bob" / "deploy env 2 (1)".
- **Left annotation (bold 13px magenta `#d55181`, left-aligned at x=30):** "max per person: 1" at y=198, "6 / 1 = 6x" at y=218.
- **Centre pipeline box:** x=225, y=115, 140×70, 8px radius, fill `rgba(74,58,167,0.15)`, 2px `#4a3aa7` border; bold 13px `#4a3aa7` "the build" at y=143 and "pipeline" at y=161, centered at x=295; 12px `#6b7280` caption "runs on every push" centered at (295, 104).
- **Capability boxes (x=420, w=270, h=28, 6px radius, centered 12px `#2c3e50` text at boxY+18), at y = 42, 76, 110, 144, 178, 212:**
  - "deploy → environment 1", "deploy → environment 2", "deploy → environment 3" — fill `rgba(0,131,0,0.13)`, 2px `#008300` border
  - "publish → package registry 1", "publish → package registry 2" — fill `rgba(217,89,38,0.13)`, 2px `#d95926` border
  - "sign → release signing key" — fill `rgba(213,81,129,0.13)`, 2px `#d55181` border
- **Arrows:** 2px `#4a3aa7` lines with 8px arrowheads from the pipeline's right edge (365, 150) to each capability box's left edge (416, boxY+14).
- **Right annotation (bold 13px `#1a5276`, centered at (555, 262)):** "union = 6 privileged capabilities".
- **Caption (12px `#444`, bottom right):** "capability counts illustrative".

## Counting the Union Against the Reviewers

**Tags:** `worked example` (blue), `privilege union` (orange), `rule of thumb` (green)

- **The holdings** — credentials for 3 deploy environments, 2 package registries, and 1 signing key: 3 + 2 + 1 = 6 capabilities
- **The best-placed human** — the most privileged individual developer holds credentials for 1 environment, so 6 / 1 = 6x
- **Who can change it** — 45 engineers can merge to the repository, so 45 people can alter what the pipeline runs
- **Who holds production** — only 3 engineers hold production deploy credentials directly: 45 / 3 = 15x more people with effective reach
- **The share** — direct holders are 3 / 45 = 6.7% of the population that can change the build's behaviour
- **Reach of one artifact** — the 2 registries feed 30 internal services, so 1 poisoned build reaches 30 services: 30 / 1
- **Why it is supply-chain scale** — the fan-out, not the single break-in, turns one bad build into a fleet-wide event
- **Assumptions** — one team, each capability counted once, and every merger assumed able to touch the config file

*Example (italic):* Alice's team restricts production deploys to 3 people, yet 45 people can edit the file that tells the build how to deploy (counts illustrative).

**Key point:** 6 capabilities sit behind a routine merge that 45 people can perform — the privilege is centralized, the authority to redirect it is not.

### Visualization (canvas `c2`, 720×300)

Bar chart comparing the population that can change what the pipeline runs against the population holding production deploy credentials directly.

- **Title (bold 15px, `#1a5276`, top center):** "45 Can Change What the Build Runs; 3 Hold Deploy Credentials".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = engineers 0 to 50, gridlines `#e5e9ef` at 10/20/30/40/50 with 12px `#444` right-aligned tick labels at x=54; x-axis 2px `#999`.
- **Bars (90px wide, centered at x=210 and x=450), values `[45, 3]`, pixel heights computed at render as `value / 50 * 180` (= 162 and 10.8):** first bar fill `rgba(213,81,129,0.30)` with 2px `#d55181` border; second bar fill `rgba(0,131,0,0.35)` with 2px `#008300` border.
- **Value labels (bold 13px in the bar's border color, centered above each bar top):** "45 engineers", "3 engineers".
- **X labels (12px `#444`, centered under each bar, two lines at baseline+18 and baseline+34):** "can merge to the repo" / "(= can change the pipeline)"; "hold production deploy" / "credentials directly".
- **Annotations (bold 13px, left-aligned at x=300):** magenta `#d55181` "45 / 3 = 15x more people" at y=80; `#1a5276` "3 / 45 = 6.7% hold it directly" at y=102.
- **Caption (12px `#444`, bottom right):** "headcounts illustrative".

## Sealing the Untrusted Trigger, Scoping the Rest

**Tags:** `where it's used` (blue), `defenses` (green), `least privilege` (orange)

- **Split the triggers** — builds from an outside fork's pull request run with no secrets; only trusted branches get privileged builds
- **Why that is sharpest** — otherwise an outsider's code executes in a context holding all 6 capabilities
- **Scope per step** — give each job only the one environment it needs instead of exposing the full set to every job
- **Short-lived credentials** — a credential issued for one build and expiring with it beats a long-lived stored secret
- **Guard the config** — the pipeline file is the highest-privilege file in the repository, so require review by a designated group
- **Ephemeral runners** — single-use machines, so no cache, checkout, or leftover state survives a job for the next one
- **Pin build plugins** — the actions and plugins a build imports execute inside the privileged context too
- **Log the access** — alert on secret reads and on any change to the pipeline definition

*Example (italic):* A fork's pull request is still compiled and tested — it just runs sealed, with nothing available to steal (illustrative).

**Key point:** Keep the pipeline's own authority narrower than the union of what the team can do, and never hand secrets to a build triggered by code nobody has reviewed.

### Visualization (canvas `c3`, 720×300)

Two-lane flow diagram: the trusted-branch lane receives scoped secrets and may deploy, while the fork lane runs with none and returns only test results.

- **Title (bold 15px, `#1a5276`, top center):** "Separate the Triggers: Untrusted Builds Get No Secrets".
- **Lane 1 label (bold 12px `#008300`, x=30, y=52):** "trusted branch — privileged build".
- **Lane 1 boxes (y=60, h=52, 8px radius, fill `rgba(0,131,0,0.13)`, 2px `#008300` border, centered 12px `#2c3e50` text, two lines where noted):** x=30 w=170 "push to a trusted" / "branch"; x=250 w=200 "build with scoped," / "short-lived secrets"; x=490 w=200 "may deploy, publish," / "and sign".
- **Lane 2 label (bold 12px `#2a78d6`, x=30, y=178):** "fork pull request — sealed build".
- **Lane 2 boxes (y=186, h=52, same geometry, fill `rgba(42,120,214,0.13)`, 2px `#2a78d6` border):** x=30 w=170 "pull request from" / "an outside fork"; x=250 w=200 "build with NO" / "secrets attached"; x=490 w=200 "test results only," / "no deploy, no publish".
- **Arrows:** 2px lines with 8px arrowheads in the lane's border color, from x=200→246 and x=450→486 at each lane's vertical centre (y=86 and y=212).
- **Divider:** dashed `#6b7280` (dash 5/4) horizontal line at y=150 from x=20 to x=700.
- **Annotation (bold 13px `#1a5276`, centered at (360, 272)):** "same repository, two privilege classes".
- **Caption (12px `#444`, bottom right):** "illustrative configuration".

## A Signature Names the Producer, Not Its Integrity

**Tags:** `common mistake` (red), `signing` (orange)

- **The habit** — production servers are hardened and audited; the build system is filed under developer tooling
- **The contradiction** — that tooling writes to production by design, so it sits inside the trust boundary, not beside it
- **The signing error** — "our releases are signed, so they are trustworthy" confuses the producer with the producer's integrity
- **What a signature proves** — this artifact came from our pipeline; it says nothing about what the pipeline was told to do
- **The consequence** — a subverted pipeline emits validly signed, verifiably wrong artifacts, and every signature check passes
- **The fan-out** — one such artifact travels through 2 registries into 30 consuming services before anyone looks twice
- **The better question** — not "is it signed?" but "was the build that signed it doing what we intended?"

*Example (italic):* Every consuming service verifies the signature, every verification succeeds, and all 30 install the same wrong artifact (illustrative).

**Common mistake:** Treating the build system as developer tooling and a valid signature as proof of integrity. Signing attests to the producer; if the pipeline was subverted, every artifact it emits is validly signed and wrong.

### Visualization (canvas `c4`, 720×300)

Fan-out diagram: one subverted build publishes through 2 registries into 30 consuming services, with the "validly signed" label attached to the bad artifact.

- **Title (bold 15px, `#1a5276`, top center):** "One Subverted Build → 2 Registries → 30 Services, All Validly Signed".
- **Build box:** x=30, y=125, 150×56, 8px radius, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border; centered 12px `#2c3e50` text "1 subverted build" at y=148 and bold 12px `#e74c3c` "validly signed" at y=167.
- **Registry boxes (x=250, w=150, h=44, 8px radius, fill `rgba(217,89,38,0.13)`, 2px `#d95926` border, centered 12px `#2c3e50` text at boxY+27):** y=80 "package registry 1"; y=160 "package registry 2".
- **Service dots:** 30 filled circles, radius 6, fill `rgba(42,120,214,0.75)` with 1px `#2a78d6` stroke, in a 6×5 grid at x = 470 + col×40 (col 0–5) and y = 70 + row×38 (row 0–4); 12px `#444` label "30 consuming services" centered at (570, 258).
- **Arrows:** 2px `#e74c3c` from the build box right edge (180, 153) to each registry box left edge (246, 102) and (246, 182); 2px `#d95926` from each registry box right edge (400, 102) and (400, 182) to (452, 140) and (452, 160), 8px arrowheads.
- **Annotations (bold 12px red `#e74c3c`, left-aligned at x=30):** "the signature proves WHO built it —" at y=248, "not whether the build did what you intended" at y=266.
- **Caption (12px `#444`, bottom right):** "service counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` used only for the subverted-build alarm state.
- **Data:** all values are hardcoded literal arrays — no randomness anywhere. The counts (3 environments + 2 registries + 1 signing key = 6 capabilities; 45 mergers vs 3 direct credential holders; 30 consuming services) are invented for one illustrative team and labeled illustrative. Derived figures must be reproducible by hand and match the charts to the digit: 3 + 2 + 1 = 6, 6 / 1 = 6x, 45 / 3 = 15x, 3 / 45 = 6.7%, 30 / 1. Bar pixel heights in `c2` are computed at render from the values (`v / 50 * 180`), not hardcoded.
- **Scope boundary:** this page owns the build system itself as the target. Dependency-trust threats (typosquatting, hijacked maintainers, lockfiles, SBOMs) belong to the software-supply-chain page and are not re-taught here.
- **Framing:** defensive/educational throughout — mechanisms and defenses only, no operational attack guidance. No real CI/CD products, hosting platforms, or vendors are named; people are Alice and Bob; registries and environments are numbered generically. No credential strings, tokens, or key=value credential syntax appear anywhere on the page.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
