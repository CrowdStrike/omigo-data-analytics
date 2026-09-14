# Threat Modeling & Attack Surface

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Threat Modeling & Attack Surface

**Subtitle:** Security as systematic pessimism — before shipping, draw the system, list every way it can go wrong, and count every door an attacker could try

## Four Questions Before the First Line of Code

**Tags:** `core idea` (blue), `systematic pessimism` (orange), `trust boundaries` (green)

- **The app** — a small web app: a login page, a file-upload feature, and an admin panel
- **Question 1** — what are we building? Diagram the parts and where untrusted data crosses in
- **Question 2** — what can go wrong? Walk each part with the STRIDE checklist, not gut feeling
- **Question 3** — what are we doing about each threat? Every item gets a mitigation or an accept
- **Question 4** — did we do enough? Re-check the diagram whenever a new feature adds a door
- **Trust boundary** — any line where data moves from a less-trusted zone to a more-trusted one

*Example (italic):* The team spends one whiteboard hour drawing browser → login → app → database, and finds two trust boundaries before writing any defense code.

**Key point:** Threat modeling replaces "be secure" with four answerable questions — what are we building, what can go wrong, what are we doing about it, and did we do enough.

### Visualization (canvas `c1`, 720×300)

System diagram of the example web app with dashed trust-boundary lines separating the internet, the application zone, and the admin zone.

- **Title (bold 15px, `#1a5276`, top center):** "Draw It First: One Web App, Two Trust Boundaries".
- **Zones:** three background bands — internet zone x 20–230 fill `rgba(231,76,60,0.06)`, app zone x 250–540 fill `rgba(42,120,214,0.06)`, data/admin zone x 560–700 fill `rgba(0,131,0,0.06)`; 11px `#6b7280` zone labels at each band top ("untrusted internet", "application", "data & admin").
- **Boxes (rounded 8px, 130×40, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text):** "browser" at (50, 130); "login endpoint" at (280, 60); "file upload" at (280, 130); "admin panel" at (280, 200); "database" at (580, 90); "file store" at (580, 180).
- **Arrows:** 3px `#2a78d6` arrows browser→login, browser→upload, browser→admin; 2px `#199e70` arrows login→database, upload→file store, admin→database.
- **Trust boundaries:** vertical dashed `#e74c3c` (dash 6/4) lines at x=240 and x=550, bold 12px `#e74c3c` labels "boundary 1: internet → app" (top of x=240) and "boundary 2: app → data" (top of x=550).
- **Annotation (bold 13px orange `#d95926`, near (60, 260)):** "every arrow crossing a dashed line is a place to ask: what can go wrong here?".
- **Caption (12px `#444`, bottom right):** "system schematic, illustrative".

## Running STRIDE on the Login, the Upload, and the Admin Panel

**Tags:** `worked example` (blue), `STRIDE` (green), `likelihood × impact` (orange)

- **The checklist** — STRIDE: Spoofing, Tampering, Repudiation, Information disclosure, DoS, Elevation
- **Spoofing** — attacker guesses the admin password: likelihood 4, impact 5, risk 4×5 = 20
- **Info disclosure** — uploaded-file URLs are guessable, anyone can download: 4×4 = 16
- **Elevation** — a normal user types /admin and it loads: 3×5 = 15; tampering via upload: 2×5 = 10
- **DoS and repudiation** — 1 GB uploads fill the disk: 3×3 = 9; no audit log on deletes: 2×3 = 6
- **The ranking** — sort by likelihood × impact; the top three (20, 16, 15) get fixed this sprint

*Example (italic):* Six threats scored by hand in an afternoon: 20, 16, 15, 10, 9, 6 — the guessable admin password outranks the flashy upload exploit.

**Key point:** STRIDE turns "what can go wrong" into a repeatable sweep, and likelihood × impact turns the resulting list into an ordered work queue instead of a panic.

### Visualization (canvas `c2`, 720×300)

Risk-matrix scatter: the six scored threats plotted on a likelihood (x) vs impact (y) grid, with shaded high-risk corner and each point labeled by its STRIDE letter.

- **Title (bold 15px, `#1a5276`, top center):** "Six Threats, One Grid: Likelihood × Impact Decides the Order".
- **Axes:** origin x=70, baseline y=245, plot width 560, plot height 180; x = likelihood 1 to 5, y = impact 1 to 5, 12px `#444` tick labels at each integer, gridlines `#e5e9ef` at each integer; axis titles 12px `#444` "likelihood →" (bottom center) and "impact →" (rotated, left).
- **High-risk shading:** rectangle covering likelihood ≥3 and impact ≥4 filled `rgba(231,76,60,0.08)`.
- **Points (10px filled circles, bold 12px labels beside each):** red `#e74c3c` "S: admin password guessed (20)" at (4,5); red `#e74c3c` "I: guessable file URLs (16)" at (4,4); orange `#d95926` "E: /admin open to users (15)" at (3,5); orange `#d95926` "T: upload overwrites code (10)" at (2,5); blue `#2a78d6` "D: disk-filling uploads (9)" at (3,3); blue `#2a78d6` "R: no delete audit log (6)" at (2,3).
- **Annotation (bold 13px red `#e74c3c`, inside the shaded corner near (3.1, 4.6) in data coords):** "fix this corner first".
- **Caption (12px `#444`, bottom right):** "scores 1–5 illustrative; risk = likelihood × impact".

## Fewer Doors to Guard

**Tags:** `where it's used` (blue), `attack surface` (orange), `rule of thumb` (green)

- **Attack surface** — every point where an attacker can interact: endpoints, uploads, vendors, people
- **The count** — the app exposes 40 public endpoints, 20 accepted upload types, 8 vendor integrations
- **The people door** — 12 employees hold admin access; each account is part of the surface too
- **The shrink** — after review: 12 endpoints, 3 upload types (png/jpg/pdf), 3 vendors, 2 admins
- **The rule** — you cannot patch a door that does not exist; removal beats hardening
- **Assets first** — list what is worth stealing (password table, uploaded contracts) to aim the effort

*Example (italic):* Dropping 28 unused endpoints and 17 upload types removed more risk in a day than a month of hardening the 40 originals would have.

**Key point:** A smaller attack surface means fewer doors to guard — enumerate every interaction point, cut what you can, and harden only what must stay open.

### Visualization (canvas `c3`, 720×300)

Grouped horizontal bar chart: attack-surface counts before vs after reduction, four categories.

- **Title (bold 15px, `#1a5276`, top center):** "Shrinking the Surface: Doors Before vs After the Review".
- **Layout:** category labels 12px `#444` left-aligned at x=20; bars start at x=210, max width 440 scaled so 40 units = 440px (11px/unit); each category is a pair of 16px-tall bars (before on top, after below, 6px gap), pairs centered at y = 75, 130, 185, 240.
- **Bars (before fill `rgba(42,120,214,0.30)` with 2px `#2a78d6` edge; after solid green `#008300`):** "public endpoints": before 40 (440px), after 12 (132px); "upload file types": before 20 (220px), after 3 (33px); "vendor integrations": before 8 (88px), after 3 (33px); "admin accounts": before 12 (132px), after 2 (22px).
- **Value labels:** 11px `#444` counts at each bar end (40/12, 20/3, 8/3, 12/2).
- **Annotation (bold 13px green `#008300`, right side near y=100):** "28 endpoints deleted = 28 doors that can never be picked".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## The Model That Only Fears Hackers — Once

**Tags:** `common mistake` (red), `stale model` (orange)

- **Mistake 1** — profiling only the hoodie hacker; insiders and careless vendors are attackers too
- **Mistake 2** — treating the model as a launch document; every new feature adds unmodeled doors
- **The drift** — the team ships a password-reset flow six months later and never re-runs STRIDE
- **The hole** — the reset token is guessable — spoofing, the exact top threat from the first model
- **The habit** — a threat model is a living artifact: re-ask the four questions at each design review

*Example (italic):* The launch-day model scored admin spoofing 20 and it was fixed — then an unmodeled reset flow reopened the same door with a 6-digit token.

**Common mistake:** Doing the threat model once and filing it away. The attack surface changes with every feature, so the pessimism has to be re-run, not remembered.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a stale one-time model letting a new feature ship unreviewed (breach) vs a living model catching the flaw at design time.

- **Title (bold 15px, `#1a5276`, top center):** "One-Time Model vs Living Model: the Password-Reset Flow".
- **Row 1 (y=95), label 12px `#444` at x=20:** "model at launch only"; blue `#2a78d6` rounded box at x=180 labeled "reset flow shipped, no review" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "guessable 6-digit token" with bold 12px red "✗ spoofing door reopened".
- **Row 2 (y=205), label:** "re-model each feature"; blue box at x=180 "reset flow at design review", 3px arrow to a green `#008300` box at x=380 labeled "STRIDE pass: spoofing flagged", then arrow to a green box at x=575 labeled "long random token" with bold 12px green "✓".
- **Box style:** 150–175px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the checklist is cheap; re-running it is the discipline".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); threat scores (4×5=20, 4×4=16, 3×5=15, 2×5=10, 3×3=9, 2×3=6) and surface counts (40→12, 20→3, 8→3, 12→2) are invented and labeled illustrative; STRIDE letters and the four threat-modeling questions are the standard Shostack formulation.
- Framing is strictly defensive: the page teaches how defenders enumerate and rank threats, never how to execute an attack.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
