# IAM Policy Blindspots

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** IAM Policy Blindspots

**Subtitle:** Allows, denies, wildcards, and trust policies compose into an answer nobody wrote down — and role-assumption chains reach further than the policy reads

## One Account, Four Documents, One Answer

**Tags:** `core idea` (blue), `allow vs deny` (orange), `evaluated not read` (green)

- **The account** — an analytics service account should read one folder of one storage container, nothing else
- **Four authors** — an identity policy, a resource policy, an inherited organization policy, and a trust policy
- **Default deny** — with no document mentioning the account at all, the answer is no access
- **Allow anywhere** — a single explicit allow in any one of those documents is enough to grant the action
- **Deny wins** — one explicit deny anywhere overrides every allow, so a visible allow can be dead
- **Conditions** — a grant may be live only from an internal network or on a tagged resource, so context decides
- **Evaluated, not read** — access is a function of several documents; no single one of them holds the answer

*Example (italic):* The identity policy plainly says allow read on `<container>/<folder>/*`, yet an organization-level deny on that container makes the effective answer no access (illustrative).

**Key point:** Effective access is computed by evaluating every attached document together — default deny, any allow grants, any deny overrides — so reading one policy never answers "what can this account do?"

### Visualization (canvas `c1`, 720×300)

Evaluation-pipeline diagram: four independently-authored documents feed one evaluator, which emits a single effective decision; the deny path is highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Four Documents, One Evaluator, One Effective Answer".
- **Input boxes (x=30, width 200, height 42, 8px radius, centered 12px bold first line at y+18 and 11px `#2c3e50` second line at y+34):**
  - y=52, blue: fill `rgba(42,120,214,0.15)`, border `#2a78d6` — "identity policy" / "allow read <container>/*"
  - y=104, aqua: fill `rgba(25,158,112,0.15)`, border `#199e70` — "resource policy" / "allow read (analytics)"
  - y=156, orange: fill `rgba(217,89,38,0.18)`, border `#d95926` — "organization policy" / "DENY read on <container>"
  - y=208, yellow: fill `rgba(201,133,0,0.15)`, border `#c98500` — "condition on the allow" / "only from internal network"
- **Evaluator box:** violet rounded box x=286, y=100, 132×102, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` "evaluate" at (352, 124); three 11px `#2c3e50` lines at (352, 146/164/182): "default: no access", "any allow → grant", "any deny → override".
- **Arrows:** 2px `#6b7280` lines with 8px arrowheads from each input box's right edge (x=230, y = box y+21) to the evaluator's left edge (x=282, y = 151); the organization-policy arrow is 3px `#d95926` instead.
- **Decision box:** x=500, y=126, 180×50, 8px radius, fill `rgba(217,89,38,0.18)`, 2px `#d95926` border; bold 13px `#d95926` "effective: NO ACCESS" at (590, 148); 11px `#6b7280` "for this action, today" at (590, 165). 3px `#d95926` arrow from x=418 to x=496 at y=151.
- **Annotation (bold 13px red `#e74c3c`, left-aligned at x=30, y=268):** "the allow in the identity policy is dead".
- **Caption (12px `#444`, bottom right):** "generic pseudo-policy fields; illustrative".

## Counting What the Wildcard Granted

**Tags:** `worked example` (blue), `wildcards` (orange), `rule of thumb` (green)

- **The container** — 5,000 objects spread across 12 folders sit inside the one storage container
- **The need** — the account legitimately reads a single folder holding 400 of those objects
- **The shortcut** — the policy names the resource `<container>/*` instead of `<container>/<folder>/*`
- **The over-grant** — that wildcard grants all 5,000 objects: 5,000 / 400 = 12.5x more than needed
- **The needed share** — the legitimate scope is 400 / 5,000 = 8% of what the document actually granted
- **Same length** — the wildcard policy is not one character longer than the narrow one would be
- **The fix** — list resources explicitly; where a wildcard is unavoidable, pair it with a condition

*Example (italic):* Widening one path segment from `<container>/<folder>/*` to `<container>/*` moves the granted set from 400 objects to 5,000 (illustrative figures).

**Key point:** A short policy is not a narrow policy — wildcards grow the granted set without growing the document, so breadth is invisible in the text a reviewer is reading.

### Visualization (canvas `c2`, 720×300)

Horizontal bar comparison: the 400 objects the account needs against the 5,000 a container-level wildcard grants, on one shared scale.

- **Title (bold 15px, `#1a5276`, top center):** "One Folder Needed (400) vs One Wildcard Granted (5,000)".
- **Scale:** bars start at x=200, plot width 440 maps 0 to 5,000 objects. Baseline axis 2px `#999` at y=222 from x=200 to x=640; ticks at 0 / 2,500 / 5,000 (x = 200 / 420 / 640) with 3px stubs and 12px `#444` labels at y=240; 12px `#444` axis caption "objects in the container" centered at (420, 262).
- **Rows (bar height 34, hardcoded values `[400, 5000]` → pixel widths `[35, 440]`):**
  - y=96, needed: fill `rgba(0,131,0,0.35)`, 2px `#008300` border; right-aligned 12px `#444` label "needs: 1 folder" ending at x=190; bold 12px `#008300` value "400 objects" at (243, 117).
  - y=158, granted: fill `rgba(217,89,38,0.35)`, 2px `#d95926` border; right-aligned 12px `#444` label "granted: <container>/*" ending at x=190; bold 12px `#7a3208` value "5,000 objects" at (500, 179), drawn inside the bar for contrast.
- **Gridline:** 1px `#e5e9ef` vertical at x=420 from y=88 to y=222 (the 2,500 tick), drawn before the bars.
- **Annotations (exactly two, left-aligned):** bold 13px `#d95926` at (200, 68) "5,000 / 400 = 12.5x over-grant"; 12px `#6b7280` at (200, 86) "needed share = 400 / 5,000 = 8%".
- **Length note (bold 12px `#1a5276`, left at x=200, y=44):** "both policies are one line long".
- **Caption (12px `#444`, bottom right):** "object counts illustrative".

## Who May Become This Role

**Tags:** `assume-role chains` (blue), `trust policy` (magenta), `where it's used` (green)

- **A separate question** — the trust policy decides who may assume the role, not what the role may do
- **Narrow but open** — a tightly scoped role that almost anyone may assume is not a tightly scoped grant
- **The graph** — 8 roles, with edges A→B, A→C, B→D, C→D, D→E, E→F, plus a separate G→H
- **Direct reach** — A's own trust and permission documents show exactly 2 edges out of A: to B and C
- **Transitive reach** — walking those edges, A reaches B, C, D, E, F: 5 roles, and 5 / 2 = 2.5x the 2 read
- **Self-escalation** — a permission to attach policies or pass a role is a permission to grant more access
- **Enumerate, don't read** — treat assume-role as a graph problem and compute reachability, not grants
- **Audit both sides** — review who may assume each role as carefully as what each role may do

*Example (italic):* No document mentions A and F together, yet A → C → D → E → F is four hops of individually reasonable trust (illustrative graph).

**Key point:** Assume-role chains compose transitively, so a role's real blast radius is its reachable set in the trust graph — 5 roles here — not the 2 named in its own documents.

### Visualization (canvas `c3`, 720×300)

Directed role graph: the two direct edges out of A in blue, the further transitively-reachable edges in magenta, and a separate two-node island in grey.

- **Title (bold 15px, `#1a5276`, top center):** "A's Own Documents Show 2 Roles; A Can Reach 5".
- **Nodes (circles, radius 22, 2px border, bold 15px letter centered, 11px `#6b7280` role caption below):** A (70, 155) violet fill `rgba(74,58,167,0.18)` border `#4a3aa7`; B (180, 100), C (180, 210), D (300, 155), E (410, 155), F (520, 155) all magenta fill `rgba(213,81,129,0.18)` border `#d55181`; G (630, 105), H (630, 215) grey fill `rgba(107,114,128,0.12)` border `#6b7280`.
- **Edges (drawn from circle edge to circle edge with an 9px arrowhead):** A→B and A→C in 3px `#2a78d6`; B→D, C→D, D→E, E→F in 2.5px `#d55181`; G→H in 2px `#6b7280`.
- **Legend / annotations:** bold 13px `#2a78d6` left-aligned at (250, 48) "direct edges from A: 2 (B, C)"; bold 13px `#d55181` left-aligned at (250, 68) "reachable from A: 5 — B, C, D, E, F"; bold 12px `#4a3aa7` left-aligned at (250, 88) "5 / 2 = 2.5x what A's documents show".
- **Island label (12px `#6b7280`, centered at (630, 165)):** "not reachable".
- **Node captions (11px `#6b7280`, centered 34px below each centre):** A "start", F "reached in 4 hops", others omitted.
- **Caption (12px `#444`, bottom right):** "role graph illustrative".

## Reading One Policy Is Not Reviewing the Access

**Tags:** `common mistake` (red), `review practice` (orange), `defensive` (green)

- **The confusion** — a reviewer reads the identity policy and believes the access has now been reviewed
- **One term of five** — identity, resource, inherited deny, conditions and trust are all terms: 1 / 5 = 20%
- **Wrong signal** — a short policy feels safe because it is short, and wildcard breadth never shows up
- **Inverted length** — the one-line wildcard grants 5,000 objects; the longer explicit list grants 400
- **Use the simulator** — an effective-access analyzer composes what a human cannot hold in their head
- **Deny high** — put organization-level denies on what must never happen, since deny-wins makes it stick
- **Watch grant powers** — alert specifically on attaching policies and on passing roles to services

*Example (italic):* A reviewer approves a four-line identity policy, unaware that a resource policy widens it and that three teams may assume the role.

**Common mistake:** Approving the identity policy and calling the access reviewed. It is one term in an expression — ask the evaluator, list the reachable roles, and check who may assume the role before signing off.

### Visualization (canvas `c4`, 720×300)

Two panels: the five terms of the effective-access expression with only one marked reviewed, and the inverted signal where the shorter policy grants far more.

- **Title (bold 15px, `#1a5276`, top center):** "One Term Reviewed, Five Terms Decide".
- **Top panel — five tiles (y=52, height 46, width 120, 8px radius, x = 40, 170, 300, 430, 560):** tile 1 solid blue fill `rgba(42,120,214,0.18)` with 2px `#2a78d6` border, bold 12px `#2a78d6` "identity policy" at y+20 and 11px `#6b7280` "reviewed" at y+36; tiles 2–5 fill `rgba(107,114,128,0.08)` with 2px dashed (`[5,4]`) `#6b7280` border, bold 12px `#6b7280` labels "resource policy", "inherited deny", "conditions", "trust policy" at y+20 and 11px `#6b7280` "not reviewed" at y+36.
- **Top annotation (bold 13px red `#e74c3c`, centered at (360, 126)):** "1 of 5 terms reviewed = 20% of the expression".
- **Bottom panel — two bars (bar height 20, start x=260, hardcoded values `[5000, 400]` → widths `[360, 29]`):** y=178 orange fill `rgba(217,89,38,0.35)` border `#d95926`, right-aligned 12px `#444` label "one-line wildcard" ending at x=250, bold 12px `#d95926` value "5,000 objects" at (628, 193); y=228 green fill `rgba(0,131,0,0.35)` border `#008300`, right-aligned 12px `#444` label "longer explicit list" ending at x=250, bold 12px `#008300` value "400 objects" at (297, 243).
- **Bottom annotation (bold 12px `#1a5276`, left at (260, 158)):** "document length points the wrong way".
- **Caption (12px `#444`, bottom right):** "object counts illustrative".

## Footnote

A `.footnote` paragraph after the last section (0.82rem, `#6b7280`, top border 1px `#e0e0e0`, 16px padding-top):
"Policy fields (effect / action / resource), paths such as `<container>/<folder>/*`, and single-letter role names are generic placeholders written for illustration — they are not any real provider's schema, service, or action names, and no credential-like strings appear on this page."

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%). One `.footnote` paragraph closes the page.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`, magenta `rgba(213,81,129,0.15)`/`#d55181`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers: `roundBoxTL` (rounded rect with fill + 2px stroke) and `arrowHead`.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for the two genuine alarm annotations (a dead allow, an unreviewed expression).
- **Data (all hardcoded, no randomness, arithmetic verified):**
  - Wildcard breadth: container of 5,000 objects across 12 folders; needed folder holds 400. Over-grant 5,000 / 400 = 12.5x. Needed share 400 / 5,000 = 8%. Bar widths 440 and 35 px on a 0–5,000 scale (35 / 440 = 8.0%).
  - Role graph: nodes A–H, directed edges A→B, A→C, B→D, C→D, D→E, E→F, G→H. Out-edges of A = 2 (B, C). Transitive closure from A = {B, C, D, E, F} = 5 roles. Ratio 5 / 2 = 2.5x. Longest path A→C→D→E→F = 4 hops. G and H are unreachable from A. The drawn graph must be exactly these seven edges.
  - Review coverage: 5 terms in the expression, 1 reviewed → 1 / 5 = 20%.
  - Every number in the prose appears identically in the chart it belongs to.
- **Provider neutrality:** no real cloud provider, service, API, or policy-schema name; pseudo-policy fields (effect / action / resource) only, described in the page as generic. No ARNs, account identifiers, tokens, or key=value credential syntax; paths are `<container>/<folder>/*` placeholders and the footnote states this.
- **Scope boundary:** this page owns the technical composition side (evaluation rules, wildcard breadth, trust policies, assume-role reachability, permission-granting permissions). The organizational side — permission ratchets, unused grants nobody removes, permission boundaries as process — belongs to the least-privilege page and is deliberately absent here.
- **Framing:** defensive/educational; the page teaches how to reason about and audit effective access, with no operational attack guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
