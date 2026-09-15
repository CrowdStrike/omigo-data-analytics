# IAM Policy Evaluation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** IAM Policy Evaluation

**Subtitle:** Several separate layers judge one read of one object — everything starts denied, one explicit deny is final, and an allow you can see is never proof the read will work

## One Read, and Everything That Gets a Say

**Tags:** `core idea` (blue), `who is attached where` (green), `guardrails` (orange)

- **The request** — a team's analytics role asks to read one object from one bucket
- **The role's own policy** — lists which actions on which buckets this role may use
- **The bucket's own policy** — lists who may do what to it, even callers outside your account
- **The org-wide guardrail** — sits above the account and can only take permissions away
- **The role's ceiling** — a cap on the role and on its session, and never a grant of its own
- **The public-access switch** — an account and bucket setting that blocks public reads
- **The old per-object grants** — switched off on modern buckets, so you can ignore them
- **No single answer** — every one of these sees the same read, and each can stop it

*Example (italic):* The role's own policy allows the read, and the read can still fail — several other layers are looking at the same request.

**Key point:** Permission is not a property of one document. It is the result of several separately owned documents judged together against one request.

### Visualization (canvas `c1`, 720×300)

Gate stack: the single read travels down a spine on the left and must pass six labelled gates, each naming what it is attached to and what it can do.

- **Title (bold 15px, `#1a5276`, top center at y=22):** "One Read Must Survive Every Layer".
- **Spine:** 2.5px `#1a5276` vertical line at x=104 from y=44 to y=272, arrowhead at the bottom; bold 12px `#1a5276` label "one read" centered at (104, 38).
- **Six gate bars:** x=120, width 572, height 30, 6px radius, row tops derived as `y = 46 + i × 38` (46, 84, 122, 160, 198, 236). Each bar fill is its colour at 0.12 alpha, border 2px in the same colour; bold 12.5px colour text for the name at x=132 on baseline y+20, then `· ` plus the description in 12px `#2c3e50` starting at 132 + measured name width + 8.
  - Row 1 blue `#2a78d6` — "the role's policy" · "may allow or deny this read"
  - Row 2 green `#008300` — "the bucket's policy" · "may allow or deny this read"
  - Row 3 orange `#d95926` — "org-wide guardrail" · "can only take away, never grant"
  - Row 4 violet `#4a3aa7` — "the role's ceiling" · "a cap on what the role may have"
  - Row 5 magenta `#d55181` — "public-access switch" · "blocks a read by anyone at all"
  - Row 6 mute `#6b7280` — "old per-object grants" · "switched off — ignore them"
- **Connector stubs:** 2px line in each row's colour from x=104 to x=116 on the row's centre line (y+15), with a 7px arrowhead into the bar.
- **Annotation (bold 12.5px orange `#d95926`, centered at (406, 278)):** "the guardrail never grants — it can only take away".
- **Caption (12px `#444`, right-aligned at (w−12, 294)):** "illustrative names; the layers and where they attach are documented".

## The Order: Start Denied, and a Deny Always Wins

**Tags:** `how it works` (blue), `evaluation order` (green), `deny wins` (orange)

- **Start denied** — with no policy anywhere, every action on every object is refused
- **A deny is final** — one explicit deny anywhere ends it, and no allow overrides it
- **Either side may grant** — an allow on the role or on the bucket is enough alone
- **The guardrail only filters** — it never grants, it only removes what was granted
- **Ceilings cap the role** — the role's cap and its session cap each trim what it can do
- **Every layer must agree** — the read has to survive the guardrail and both ceilings
- **The public switch is last** — it blocks a public read after the policies have spoken
- **A read from another account** — that one request is judged twice, once on each side

*Example (italic):* Widening the role's policy from one folder to the whole bucket changes nothing if the org-wide guardrail never allowed that read.

**Key point:** What is permitted is every ceiling overlapped, minus every explicit deny. "Either side may grant" applies only between a role's policy and a bucket's policy inside one account.

### Visualization (canvas `c2`, 720×300)

Flowchart of the order: default deny at the start, the explicit-deny short circuit exiting immediately to the right, then the either-side grant, the ceilings, and the public-access switch last.

- **Title (bold 15px, `#1a5276`, top center at y=20):** "Judging One Read, In Order".
- **Box helper:** 6px radius rounded rect, fill = colour at 0.12 alpha, 2px border in the colour, bold 12px label (13.5px when `big`) drawn as centred lines, 15px line height.
- **Row A (y=40, height 44):** `#6b7280` "a read of / one object" at x=24 w=126 · `#1a5276` "start: / default DENY" at x=166 w=156 · `#d95926` "explicit deny / anywhere?" at x=338 w=204 · red `#e74c3c` "DENIED / — final" at x=560 w=132.
- **Row A arrows (2px `#6b7280`, 7px head):** x=150→166 and x=322→338 on centre line y=62; the deny exit x=542→560 drawn 2.5px red `#e74c3c` with a bold 11.5px red "yes" centered above at (551, 52).
- **Elbow from Row A to Row B (2px `#6b7280`, dashed 4/3):** from (440, 84) down to y=104, left to x=115, down to y=140 with an arrowhead; bold 11.5px `#6b7280` "no" left-aligned at (452, 100).
- **Row B (y=140, height 46):** `#2a78d6` "role allow OR / bucket allow?" at x=24 w=182 · `#4a3aa7` "inside the guardrail / and the ceilings?" at x=218 w=190 · `#d55181` "public-access / switch blocks it?" at x=420 w=150 · green `#008300` "ALLOWED" (bold 13.5px) at x=582 w=110.
- **Row B arrows (2px `#6b7280`):** x=206→218 and x=408→420 on centre line y=163, each with a bold 11.5px `#008300` "yes" centered above at (212, 134) and (414, 134); x=570→582 in 2.5px `#008300` with bold 11.5px `#008300` "no" centered above at (576, 134).
- **No union note.** The earlier draft printed "OR applies only here" at (24, 206), where the label ran straight through the dashed drop line at x=115. The Row B box already reads "role allow OR bucket allow?", so the label is dropped rather than relocated.
- **Deny bar:** x=24, y=232, width 546, height 34, 6px radius, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, bold 12.5px `#e74c3c` centred at x=297, y=253: "DENIED — nothing allowed it, or a layer took it away".
- **Drop arrows into the deny bar (2px `#e74c3c`, dashed 3/3, 7px head):** from the bottom centre of each Row B box, x = 115, 313 and 495, from y=186 into the bar at y=232; bold 11.5px `#e74c3c` labels 9px to the right of each at y=212: "no", "no", "yes".
- **Caption (12px `#444`, right-aligned at (w−12, 294)):** "documented order; one request inside one account".

## Eight Combinations, One Read

**Tags:** `worked example` (blue), `every row checked` (green), `what usually fails` (orange)

- **Nothing mentions it** — neither the role nor the bucket allows the read, so it fails
- **Only the role allows** — the bucket's policy stays silent and the read succeeds
- **Only the bucket allows** — inside one account, an allow from either side is enough
- **Both allow** — the read works, and two allows are no stronger than a single one
- **The guardrail omits it** — the read is gone from the whole account, role or not
- **The bucket denies it** — that deny beats the role's allow, which is still visible
- **The ceiling omits it** — the cap sits below the role's own policy, so the read fails
- **A public allow, blocked** — with the switch on, a read by anyone at all is refused

*Example (italic):* Rows two and three differ only in which document holds the allow, and both succeed — the one place where two documents add up.

**Key point:** Five of these eight fail, and only one of those five involves an actual deny. Most failures are something missing from a ceiling, not something forbidden.

### Visualization (canvas `c3`, 720×300)

Truth-table grid: eight scenarios down the side, the five layers that can act across, and a coloured verdict column.

- **Title (bold 15px, `#1a5276`, top center at y=20):** "Same Read, Eight Combinations".
- **Columns (x, width, header):** scenario (14, 190), role (208, 62), bucket (272, 68), guardrail (342, 72), ceiling (416, 62), public (480, 56), verdict (538, 172). No two columns overlap and the last ends at x=710.
- **Header row (bold 11.5px `#1a5276`, baseline y=50; scenario left-aligned at x+4, the rest centered in their column):** "scenario", "role", "bucket", "guardrail", "ceiling", "public", "verdict"; a 1.5px `#e5e9ef` rule at y=56 from x=14 to x=710.
- **Eight rows, height 25, tops derived as `58 + i × 25` (58…233).** Even-index rows get a `#f7f9fb` band across x=14..710. Scenario text 11.5px `#2c3e50` left-aligned at x=18, baseline top+17; layer cells 11.5px centered — `—` in `#6b7280`, `allow` in `#2a78d6`, `deny` in `#e74c3c` bold, `ok` in `#199e70`, `no read` in `#d95926` bold, `off` and `n/a` in `#6b7280`, `on` in `#d55181` bold.
- **Rows (scenario · role · bucket · guardrail · ceiling · public · verdict):**
  1. "nothing mentions the read" · — · — · ok · ok · off · **DENIED — default deny** (red)
  2. "only the role allows" · allow · — · ok · ok · off · **ALLOWED** (green)
  3. "only the bucket allows" · — · allow · ok · ok · off · **ALLOWED** (green)
  4. "both allow" · allow · allow · ok · ok · off · **ALLOWED** (green)
  5. "the guardrail omits the read" · allow · — · no read · ok · off · **DENIED — the guardrail** (red)
  6. "the bucket policy denies it" · allow · deny · ok · ok · off · **DENIED — a deny wins** (red)
  7. "the role's ceiling omits it" · allow · — · ok · no read · off · **DENIED — the ceiling** (red)
  8. "a public allow, no role" · — · allow \* · ok · n/a · on · **DENIED — public blocked** (red)
- **Verdict cell style:** rounded rect at the verdict column x, width 172, height 19, at row top +3, 4px radius; fill `rgba(0,131,0,0.14)` + 1.5px `#008300` border for ALLOWED, `rgba(231,76,60,0.12)` + 1.5px `#e74c3c` border for every DENIED; text bold 11.5px in the border colour, centered.
- **Legend (11.5px `#6b7280`, left-aligned at (14, 272)):** "ok = that layer permits the read · — = silent · allow \* = granted to anyone".
- **Annotation (bold 12px violet `#4a3aa7`, left-aligned at (14, 290)):** built at render time as "only N of the M failures is an actual deny", where M counts the rows whose verdict is a denial and N counts the rows containing a `deny` cell — the literal rows give N=1, M=5, matching the key point.
- **Caption (12px `#444`, right-aligned at (w−12, 294)):** "combinations illustrative; every verdict follows the documented order".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). All three callouts use the label "Key point:" — there is no "Common mistake" section on this page any more.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead`, `line` and `rgba` as in page 11.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for genuine deny states: the DENIED boxes and bar in `c2`, the `deny` cells and every DENIED verdict in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split while still carrying a full clause. Do **not** clip them to ~55–70 character stubs — that pass was made and rejected for compromising quality — and do **not** pad them back up to the folder default of ~90–100. Eight bullets per section.
- **Register and vocabulary.** Plain technical English for a first encounter, no analogies and no invented scenes. Less product surface than the earlier draft: say "a read of one object", not GET/PUT/LIST/HEAD; say "the org-wide guardrail", not SCP or Organizations; say "the role's ceiling", not permission boundary or session policy by name; say "the public-access switch", not Block Public Access or BPA; say "the old per-object grants, switched off on modern buckets", not object ACLs and Bucket Owner Enforced; describe policies as documents that list who may do what rather than naming policy elements or JSON keys. Keep the three mechanics unmistakable: everything starts denied, an explicit deny always wins, and an allow is needed from every layer that has a say.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No fourth section.** A fourth section ("Why the New Allow Changed Nothing") walked the condition keys for transport, source address, org membership, prefix and upload encryption, plus the policy simulator and the audit trail, over a `c4` nested-set diagram with a formula strip. It was cut as reference material: it restated section two's mechanics in set notation and carried the heaviest product jargon on the page. Condition keys belong on the bucket-policy / access-control page; the simulator and the audit trail belong on an auditing page.
  - **No formula strip.** `effective = (identity ∪ resource) ∩ … − deny` is notation for a reference doc, not a tutorial; the prose form ("every ceiling overlapped, minus every explicit deny") stays in section two's key point.
  - **No cross-account walkthrough.** It stays a single bullet; cross-account access has its own page.
  - **Three sections, one canvas each (`c1`, `c2`, `c3`), eight bullets each.**
- **Data:** all values are hardcoded literals; no randomness anywhere. No bucket ARNs, account identifiers, keys, or key-value credential syntax — the caller is "a team's analytics role" and the resource is "one object in one bucket".
  - **Documented behaviour the page stands on:** an implicit default deny; an explicit deny taking precedence over any allow; inside one account an allow in either the role's policy or the bucket's policy being sufficient; the org-wide guardrail and the role's ceilings acting only as maximum-permission filters that never grant; a session carrying its own additional ceiling; per-object grants being disabled on buckets that enforce owner-only ownership; the public-access switch existing at both account and bucket level and overriding a public grant; a cross-account request being judged in both accounts.
  - **Derived, not measured:** the eight-row table. Row 1 follows from the default deny; rows 2–4 from either side being sufficient in one account (and from allows not accumulating); row 5 from a guardrail that never permits the read removing it regardless of the role's allow; row 6 from an explicit deny ending the evaluation; row 7 from a ceiling capping what a role's policy can grant; row 8 from the public-access switch overriding a grant to everyone, where the caller has no role so the ceiling column reads `n/a`. Five rows deny; exactly one of those five contains a deny statement, and both counts are computed from the rows array at render time rather than written into the annotation.
  - **Computed geometry and collision notes:** `c1` derives its six row tops from `46 + i × 38` and starts each description at the measured width of the name, so no label can collide; its annotation sits at y=282 centred at x=406 while the caption is right-aligned at y=296, keeping the two clear of each other (the earlier draft had them 6px apart and overlapping). `c2`'s union label was removed rather than moved, since at y=206 it crossed the dashed drop line at x=115. `c3` derives row tops from `58 + i × 25` and centres every cell on its own column midpoint.
