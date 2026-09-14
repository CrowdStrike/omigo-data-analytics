# Build Provenance &amp; Artifact Signing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Build Provenance &amp; Artifact Signing

**Subtitle:** An SBOM lists what is inside a release; provenance says which build produced it — and that is the part an attacker has to forge

## Three Questions About One Downloaded Release

**Tags:** `core idea` (blue), `what vs which build` (orange), `verifiable claims` (green)

- **The artifact** — Alice's team publishes one release file; Bob downloads it and has to decide whether to run it
- **Integrity** — a hash answers "are these the same bytes the publisher produced?" and catches corruption in transit
- **Its limit** — a hash says nothing about who produced the bytes, and an attacker who replaces file and hash together passes
- **Authenticity** — a signature answers "was this signed by a key belonging to the publisher?" and catches an impostor
- **Its limit** — a signature is a statement about a key holder, never about the process that key holder actually ran
- **Provenance** — a signed attestation from the build platform names the source commit, the builder, and the build's inputs
- **The difference** — signing proves WHO; provenance proves HOW, so provenance can be compared against expectations
- **The forgery target** — an attacker inside the pipeline gets valid signatures for free; a truthful build description is the hard part

*Example (italic):* Bob verifies the hash, verifies the signature, and still cannot tell whether the file was built from Alice's reviewed branch or on somebody's laptop.

**Key point:** A hash, a signature, and provenance answer three different questions — the bytes, the key holder, and the process — and only the third is a claim a policy can check.

### Visualization (canvas `c1`, 720×300)

Three-rung ladder read bottom-to-top: integrity, authenticity, provenance, each with its own precise limitation printed to the right.

- **Title (bold 15px, `#1a5276`, top center):** "Three Questions, Three Different Answers".
- **Rung boxes (x=45, w=270, h=50, 8px radius, centered text at x=180):** bottom rung y=200 fill `rgba(42,120,214,0.13)` 2px `#2a78d6` border, bold 13px `#2a78d6` "1. Integrity — same bytes?" at y=224 and 12px `#2c3e50` "answered by a hash" at y=242; middle rung y=130 fill `rgba(201,133,0,0.13)` 2px `#c98500` border, bold 13px `#c98500` "2. Authenticity — whose key?" at y=154 and 12px "answered by a signature" at y=172; top rung y=60 fill `rgba(0,131,0,0.13)` 2px `#008300` border, bold 13px `#008300` "3. Provenance — which build?" at y=84 and 12px "signed attestation from the platform" at y=102.
- **Upward arrows (2px `#6b7280`, 8px arrowhead):** from (180, 198) to (180, 184) and from (180, 128) to (180, 114).
- **Limitation text (12px `#6b7280`, left-aligned at x=335, two lines each):** beside the bottom rung "says nothing about who" at y=218 / "produced the bytes" at y=236; beside the middle rung "says nothing about what" at y=148 / "the build actually did" at y=166; beside the top rung "checkable against policy —" at y=78 / "still no claim about intent" at y=96.
- **Annotation (bold 12px violet `#4a3aa7`, left-aligned at x=45, y=278):** "each rung adds a claim the rung below cannot make".
- **Caption (12px `#444`, bottom right):** "three distinct questions".

## Counting What Each Control Actually Catches

**Tags:** `worked example` (blue), `coverage matrix` (orange), `honest accounting` (green)

- **Five scenarios** — (a) bytes altered in transit, (b) an impostor publishes as Alice's team, (c) her signing key is stolen
- **The hard two** — (d) the pipeline is subverted and builds bad code from the real repository, (e) a malicious commit is merged
- **Hash** — detects (a) only: 1 of 5 = 20%, because every other scenario produces bytes the publisher really did emit
- **Signature** — detects (a) and (b): 2 of 5 = 40%; the impostor lacks a key, but the real pipeline signs faithfully
- **Provenance with policy** — detects (a), (b), (c): 3 of 5 = 60%, since a stolen key cannot name the expected builder and branch
- **Not (d)** — a subverted build platform can emit valid provenance for its own malicious build, so the metadata agrees
- **Not (e)** — the process was followed exactly and the source itself was malicious, which is a code-review problem
- **Assumptions** — one artifact, one policy checking repository plus branch plus builder, each scenario counted once

*Example (italic):* Provenance moves coverage from 2 of 5 to 3 of 5 — a real gain, with 2 scenarios that no metadata check will ever see (illustrative scenario set).

**Key point:** 20% → 40% → 60% across hash, signature, and provenance — provenance raises the floor and closes no ceiling, because no metadata control validates intent.

### Visualization (canvas `c2`, 720×300)

Coverage matrix: 3 control rows × 5 attack-scenario columns, detected cells filled green, with the computed row totals printed on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Detection Coverage: 1 of 5, 2 of 5, 3 of 5".
- **Grid geometry:** columns start x=175, cell width 75 (5 columns, right edge x=550); rows start y=90, cell height 48 (3 rows, bottom edge y=234).
- **Column headers:** 12px `#6b7280` "attack scenario" centered at (362, 58); bold 13px `#1a5276` letters "(a)", "(b)", "(c)", "(d)", "(e)" centered in each column at y=82.
- **Row labels (12px `#2c3e50`, right-aligned ending at x=168, at each row's vertical centre + 4):** "hash / checksum", "signature", "provenance + policy".
- **Cells, from the hardcoded matrix `[[1,0,0,0,0],[1,1,0,0,0],[1,1,1,0,0]]`:** detected cells fill `rgba(0,131,0,0.18)` with a bold 16px `#008300` "✓" centered; undetected cells fill `rgba(229,233,239,0.55)` with a 14px `#6b7280` "—" centered; every cell outlined 1px `#e5e9ef`.
- **Row totals (bold 13px, left-aligned at x=562, at each row's vertical centre + 4), computed at render from the matrix row sums:** "1 / 5 = 20%" in `#2a78d6`, "2 / 5 = 40%" in `#c98500`, "3 / 5 = 60%" in `#008300`.
- **Annotation (bold 12px magenta `#d55181`, left-aligned at x=175, y=262):** "(d) and (e) survive all three — no metadata control validates intent".
- **Caption (12px `#444`, bottom right):** "illustrative scenario set".

## Rejecting a Correctly Signed Artifact

**Tags:** `where it's used` (blue), `verify at install` (green), `rule of thumb` (orange)

- **The policy** — Bob requires the attestation to name an expected source repository, an expected builder, and a protected branch
- **The rejection** — an artifact built from a fork branch fails that policy despite carrying a perfectly valid signature
- **Platform-generated** — the attestation must come from the build platform, not from the build's own steps, or the build can lie about itself
- **Verify at consumption** — the check belongs where the artifact is installed and must fail closed; unverified metadata is decoration
- **Short-lived signing** — credentials issued to the build's workload identity and expiring with it remove the long-lived stolen-key problem
- **Public log** — recording signatures in a public transparency log makes a surprise release discoverable by anyone watching
- **Maturity ladder** — scripted build, then a hosted platform that generates provenance, then a hardened platform the build cannot forge
- **Strongest check** — a reproducible build lets a third party rebuild the source and confirm the bytes without trusting the builder at all

*Example (italic):* The signature verifies, the policy sees a fork branch where a protected branch was required, and the install stops (illustrative policy).

**Key point:** Provenance becomes a control only when the consumer enforces it at install time and rejects mismatches — the check, not the metadata, is the defense.

### Visualization (canvas `c3`, 720×300)

Policy-verification flow: one artifact with a valid signature passes two attestation checks, fails the branch check, and is rejected.

- **Title (bold 15px, `#1a5276`, top center):** "Valid Signature, Rejected by Policy".
- **Artifact box (x=25, y=105, w=175, h=76, 8px radius, fill `rgba(42,120,214,0.13)`, 2px `#2a78d6` border, centered at x=112):** 12px `#2c3e50` "release artifact" at y=130; bold 12px `#008300` "signature: valid" at y=150; 12px `#6b7280` "attestation attached" at y=170.
- **Check boxes (x=250, w=255, 8px radius, 12px `#2c3e50` label left-aligned at x=262, bold 15px status glyph right-aligned at x=493):** y=52 h=46 fill `rgba(0,131,0,0.13)` 2px `#008300` border, label "source repository = expected" at y=80, green "✓"; y=112 h=46 same green styling, label "builder identity = expected" at y=140, green "✓"; y=172 h=56 fill `rgba(231,76,60,0.12)` 2px `#e74c3c` border, labels "branch = a fork branch" at y=194 and "expected: a protected branch" at y=212, red "✗" at y=203.
- **Reject box (x=540, y=126, w=155, h=64, 8px radius, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border, centered at x=617):** bold 13px `#e74c3c` "REJECTED" at y=152; 12px `#2c3e50` "fail closed" at y=172.
- **Arrows (2px, 8px arrowheads):** `#2a78d6` from (200, 143) to (246, 75), (246, 135), (246, 200); `#6b7280` from (509, 75) and (509, 135) plus `#e74c3c` from (509, 200), all to (536, 158).
- **Annotations (bold 13px red `#e74c3c`, left-aligned at x=25):** "the signature was fine —" at y=252; "the attestation named an unexpected branch" at y=270.
- **Caption (12px `#444`, bottom right):** "attestation fields are placeholders".

## "It Is Signed, So It Is Safe"

**Tags:** `common mistake` (red), `SBOM vs provenance` (orange)

- **The collapse** — "it is signed" gets treated as one answer to all three questions when it answers authenticity alone
- **What it proves** — a key holder asserted this artifact; nothing about the source, the builder, or what the build did
- **Why it feels sufficient** — the verification succeeds, so the check looks complete, but success only means the key matched
- **The SBOM confusion** — an SBOM is often requested as though it were a security control when it is an inventory of contents
- **What an SBOM is for** — answering "am I affected?" on the day a vulnerability in some component is newly published
- **What it cannot do** — a malicious build can ship an identical component list, so the inventory looks entirely unchanged
- **Complements** — contents versus origin: the SBOM says what is inside, provenance says which build produced it
- **Better question** — not "is it signed?" but "does its attestation match the build we expected to have run?"

*Example (italic):* A malicious build of the same release ships the same component list, so the SBOM diff is empty while the provenance mismatch is obvious.

**Common mistake:** Reading a valid signature as proof of safety, and an SBOM as a detection control. The signature answers who; the SBOM answers what is inside; only provenance answers which build produced these bytes.

### Visualization (canvas `c4`, 720×300)

Two side-by-side cards contrasting what an SBOM answers with what provenance answers, each listing its question, its best use, and its blind spot.

- **Title (bold 15px, `#1a5276`, top center):** "SBOM Answers 'What Is Inside'; Provenance Answers 'Which Build'".
- **Left card (x=30, y=60, w=310, h=190, 8px radius, fill `rgba(42,120,214,0.10)`, 2px `#2a78d6` border):** bold 13px `#2a78d6` header "SBOM — an inventory" at (45, 84); 12px `#2c3e50` lines left-aligned at x=45 — "question: what components are" y=112, "inside this release?" y=130, "good for: am I affected by a new" y=158, "vulnerability disclosure?" y=176, "blind to: a malicious build with" y=204, "an identical component list" y=222.
- **Right card (x=380, y=60, w=310, h=190, 8px radius, fill `rgba(0,131,0,0.10)`, 2px `#008300` border):** bold 13px `#008300` header "Provenance — an origin claim" at (395, 84); 12px `#2c3e50` lines left-aligned at x=395 — "question: which build produced" y=112, "these exact bytes?" y=130, "good for: policy on repository," y=158, "branch, and builder identity" y=176, "blind to: malicious code that was" y=204, "reviewed and merged normally" y=222.
- **Label emphasis:** the leading words "question:", "good for:", and "blind to:" are drawn in bold 12px (`#1a5276` on the left card, `#1a5276` on the right card) and the remainder of each line in regular 12px `#2c3e50`, by measuring the bold prefix width and offsetting the rest.
- **Annotation (bold 13px magenta `#d55181`, centered at (360, 276)):** "complements, not substitutes: contents vs origin".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). A final `.footnote` line (0.8rem `#6b7280`) after the last section states that attestation field values on the page are placeholders so security scanners do not flag them.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` only for the rejection/failure state in `c3`.
- **Data:** no randomness anywhere. The only quantitative object is the coverage matrix, hardcoded as `[[1,0,0,0,0],[1,1,0,0,0],[1,1,1,0,0]]` (rows: hash, signature, provenance + policy; columns: scenarios a–e). Row totals and percentages are computed at render from that matrix (`sum`, `sum/5*100`), so the chart cannot disagree with the prose: 1/5 = 20%, 2/5 = 40%, 3/5 = 60%. The scenario set and the coverage judgements are illustrative and labeled as such.
- **Coverage rationale (must not be inflated on regeneration):** provenance detects (c) because a stolen signing key alone cannot produce an attestation naming the expected builder and protected branch; it does not detect (d) because a subverted build platform can emit valid provenance for its own malicious build; it does not detect (e) at all because the process was followed and the source was malicious.
- **Scope boundary:** SBOMs at an introductory level, typosquatting, and lockfiles belong to the software-supply-chain page; the build system as an attack target and pipeline attack mechanics belong to the CI/CD-pipeline-compromise page. This page starts from the fact that a compromised pipeline emits a validly signed artifact and asks which claim would have caught it.
- **Framing:** defensive/educational only. No real vendors, tools, signing utilities, transparency logs, build platforms, or frameworks are named, and no framework level numbering is claimed — the maturity ladder is described generically as increasing rigour. People are Alice and Bob. No credential strings, keys, hashes, tokens, or key=value credential syntax appear anywhere; attestation fields are shown as descriptive placeholders.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
