# Cross-Account Access

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cross-Account Access

**Subtitle:** When the reader and the bucket sit in different accounts both accounts have to allow the call — and a file uploaded from outside can leave the bucket's own owner unable to read it

## Both Accounts Have to Allow the Read

**Tags:** `core idea` (blue), `both sides must allow` (green), `two owners` (orange)

- **The setup** — account A owns the bucket, account B wants to read one file in it
- **Same account** — one permission grant on either side is enough to allow the read
- **Different accounts** — both the reader's side and the bucket's side must allow it
- **Neither alone** — the bucket owner cannot grant what account B never permitted
- **The usual failure** — account B grants itself read but the bucket never names it
- **The mirror failure** — the bucket names account B, but B never allowed the call
- **Only one difference** — everything else in the permission check works the same
- **Two owners** — the two grants live in two accounts, so two separate teams must agree

*Example (illustrative):* Account B's reader has permission to read any bucket and is still refused, because account A's bucket policy never mentions it.

**Key point:** Inside one account permissions add up; across accounts they have to overlap. A cross-account grant is always two edits made by two different owners.

### Visualization (canvas `c1`, 720×300)

Two side-by-side panels with the same two policy inputs and opposite answers: one grant is enough inside an account, both are needed across two.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Same Policies, Opposite Answers".
- **Panels:** `panelX(i) = 30 + i × 350`, so x = 30 and 380, each 310 wide, y=44 h=206, 8px radius, fill `#fbfcfd`, 1.5px `#e5e9ef` border. Panel centre `cx = panelX(i) + 155`.
- **Panel headers (bold 12.5px, centered at cx, y=64):** left "both in one account" in `#008300`, right "in two different accounts" in `#d95926`.
- **Policy boxes (two per panel, 270 wide × 34 tall at x = panelX+20, y=80 and y=126, 6px radius):**
  - Box 1, both panels: "account B allows the read" — fill blue at 0.14 alpha, 2px `#2a78d6` border, 12px `#2c3e50` centered.
  - Box 2, both panels: "the bucket policy says nothing" — fill `#f4f6f9`, 1.5px `#6b7280` border, 12px `#6b7280` centered.
- **Connectors:** 1.5px `#6b7280` — vertical from (cx−70, 114) down to y=168, vertical from (cx+70, 160) down to y=168, joined by a horizontal line at y=168.
- **Gate label (bold 13px, centered at cx, y=184):** left "one grant is enough" in `#008300`, right "both sides must allow" in `#e74c3c`.
- **Arrow:** 1.5px `#6b7280` from (cx, 188) to (cx, 196) with a 6px arrowhead into the result box.
- **Result box (200 wide × 38 tall at x = panelX+55, y=200, 6px radius):** left "ALLOWED" fill `rgba(0,131,0,0.16)`, 2.5px `#008300` border, bold 14px `#008300`; right "REFUSED" fill `rgba(231,76,60,0.14)`, 2.5px `#e74c3c` border, bold 14px `#e74c3c`.
- **Annotation (bold 13px violet `#4a3aa7`, centered at y=268):** "a bucket policy that says nothing is harmless in one account, fatal across two".
- **Caption (12px `#444`, right-aligned at y=294):** "accounts illustrative; both sides having to allow a cross-account call is documented".

## Three Ways to Hand Out Access

**Tags:** `worked example` (blue), `borrowed role preferred` (green), `entry points` (orange)

- **Name the reader** — account A lists account B's reader in the bucket's own policy
- **Its cost** — that policy grows into a long list of outsiders somebody must audit
- **Long-lived keys** — the outside reader keeps its own credentials that nobody in A rotates
- **Borrowed role** — account A keeps a role that account B is trusted to step into
- **Why it wins** — the borrowed credentials expire, and A can withdraw the role at any time
- **Own front door** — each reader gets a named entry point with its own small policy
- **It can only narrow** — an entry point never grants more than the bucket allows
- **Choosing** — a one-off share suits naming, a nightly pipeline suits a borrowed role

*Example (illustrative):* Switching account B from a name in the bucket policy to a borrowed role turns a standing grant into a session account A can end by editing one role.

**Key point:** All three finish with the same two-sided check. What changes is where the permission sits and how long the credential stays valid.

### Visualization (canvas `c2`, 720×300)

Three small flows in three columns, each three nodes deep with its trade-off written underneath; the borrowed-role column is highlighted as the recommended one.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Three Ways to Grant It, and What Each Costs".
- **Columns:** `colX(i) = 30 + i × 230`, so x = 30, 260, 490. Nodes 170 wide at `nx = colX + 15`; node centre `cx = nx + 85`.
- **Column headers (bold 12.5px, centered at cx, y=52):** "1. name the reader" `#2a78d6`, "2. borrowed role" `#008300`, "3. entry point" `#d55181`.
- **Nodes (170 × 30, 6px radius, 12px centered text, y = 66, 116, 166):**
  - Column 1: "the reader" → "named in the bucket policy" → "account A's bucket".
  - Column 2: "the reader" → "borrows a temporary role" → "account A's bucket".
  - Column 3: "the reader" → "its own entry point" → "account A's bucket".
- **Node style:** first and last node fill `#f4f6f9`, `#2c3e50` text; the middle node takes the column colour at 0.15 alpha with a 2px border in that colour. Outer-node borders are `#6b7280` at 1.5px, except in the highlighted column where all three borders are 2.5px in the column colour.
- **Highlight backing:** `rgba(0,131,0,0.06)` rect x=252, y=44, w=216, h=206, 8px radius, 1.5px `#008300` border, drawn before the columns.
- **Arrows:** 1.5px column-coloured vertical arrows (96 → 116 and 146 → 166) at the node centre, 6px arrowheads.
- **Trade-off text (12px, two centered lines per column at y=222 and y=238):**
  - Column 1 `#6b7280`: "simplest; the policy grows into" / "a long list of outsiders".
  - Column 2 `#008300`: "credentials expire and A can" / "withdraw the role — recommended".
  - Column 3 `#6b7280`: "one small policy per reader;" / "never more than the bucket".
- **Annotation (bold 13px `#4a3aa7`, centered at y=270):** "whichever you pick, both accounts still have to allow the call".
- **Caption (12px `#444`, right-aligned at y=294):** "arrangement illustrative; an entry point never exceeding the bucket is documented".

## The Object the Bucket Owner Could Not Read

**Tags:** `common mistake` (red), `who owns the file` (green), `mostly historical` (orange)

- **The upload** — a writer in account B uploads one file into account A's bucket
- **The old rule** — the file belonged to the account that uploaded it, not to its owner
- **The odd result** — account A paid for the bytes while its own read was refused
- **It could still delete** — the owner could remove the file it was not allowed to read
- **First fix** — ask every writer to hand ownership over on upload, which one job forgot
- **Second fix** — a bucket setting decides ownership instead of each writer choosing
- **Owner enforced** — the bucket's owner owns every file that lands in it, whoever wrote it
- **Mostly historical** — the default since April 2023, so only older buckets are exposed

*Example (illustrative):* A nightly job in account B wrote a month of files into account A's bucket; every read by account A was refused while the storage bill kept growing.

**Common mistake:** Do not assume the bucket's owner owns whatever lands in it. On an older bucket it does not — turn on the owner-enforced setting instead of trusting every writer to hand ownership over.

### Visualization (canvas `c3`, 720×300)

The same upload under the two ownership settings: the older one ends in a refused read, the newer one in a read that works.

- **Title (bold 15px, `#1a5276`, centered at y=22):** "Same Upload, Two Ownership Settings".
- **Step boxes:** `bx(i) = 40 + i × 165`, so x = 40, 205, 370, 535; each 140 wide, 44 tall, 6px radius. Two 12px centered text lines per box at y+18 and y+34. Last box ends at x=675, inside the canvas.
- **Arrows:** 1.5px `#6b7280` horizontal arrows from `bx(i)+142` to `bx(i)+163` on the row's centre line (y+22), 6px arrowheads — the arrow stops at the next box's left edge, it does not run into it.
- **Row 1 label (bold 12.5px `#d95926`, left-aligned at x=40, y=52):** "the older setting — the writer keeps ownership".
- **Row 1 boxes (y=60):**
  - "account B" / "uploads a file" — fill blue at 0.14, 2px `#2a78d6`.
  - "the file belongs" / "to account B" — fill orange at 0.14, 2px `#d95926`.
  - "account A reads it" / "→ REFUSED" — fill red at 0.14, 2.5px `#e74c3c`, second line bold `#e74c3c`.
  - "account A still" / "pays for the bytes" — fill yellow at 0.14, 2px `#c98500`.
- **Row 2 label (bold 12.5px `#008300`, left-aligned at x=40, y=150):** "owner enforced — the default since April 2023".
- **Row 2 boxes (y=158):**
  - "account B" / "uploads a file" — fill blue at 0.14, 2px `#2a78d6`.
  - "the file belongs" / "to account A" — fill green at 0.14, 2px `#008300`.
  - "account A reads it" / "→ WORKS" — fill green at 0.18, 2.5px `#008300`, second line bold `#008300`.
  - "no writer can" / "opt out of this" — fill aqua at 0.14, 2px `#199e70`.
- **Annotation (bold 13px magenta `#d55181`, left-aligned at x=40, y=232):** "paying for bytes you cannot read is the mark of the old ownership rule".
- **Note (12px `#6b7280`, left-aligned at x=40, y=256):** "the owner could always delete the file — only reading it was refused".
- **Caption (12px `#444`, right-aligned at y=294):** "sequence illustrative; uploader ownership and the newer default are documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `23-tutorials/CLAUDE.md`), matching the sibling `11-cross-region-replication.html` in this folder. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Section three's callout uses the label "Common mistake:".
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers `roundRect`, `arrowHead`, `arrow` and `rgba` as in page 11.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` is reserved for the refusals: the cross-account gate and REFUSED result in `c1` and the refused read in `c3`.
- **Bullet length — this is the deliberate calibration of the page.** Bullets run roughly 75–90 characters including the bold lead term, so each holds one line at the 50/50 split without wrapping while still carrying a full clause. Do **not** clip them to ~55–70 characters — that pass was made and rejected for compromising quality. Do **not** pad them back to the folder default of ~90–100 either; this page is deliberately shorter. Eight bullets per section.
- **Register and vocabulary.** Plain technical English for a first reader, no analogies and no invented scenes. Fewer product and API names than the earlier draft: say "account B allows the read", not an identity policy naming `s3:GetObject`; say "a role account B is trusted to step into", not a trust policy plus `AssumeRole`; say "a named entry point", not an access point ARN; say "the owner-enforced setting", not an ACL mode. Accounts are only ever "account A" (owns the bucket) and "account B" (uploads and reads) — no account numbers, no resource identifiers, no key names, no credential-shaped strings anywhere on the page.
- **Deliberate omissions — do not reintroduce on regeneration.**
  - **No encryption-key-gate section.** A fourth section listed four gates on an encrypted read (identity policy, bucket policy, encryption key policy, public-access blocking), with a `c4` gate diagram, the Decrypt-versus-GenerateDataKey split, whole-organisation grants and the confused-deputy conditions. It was cut for turning a tutorial into a reference sheet. Key-policy grants belong with the encryption page; public-access blocking belongs with the access-control page.
  - **No organisation-wide grant or confused-deputy bullets.** They travelled with that section and are configuration detail at this level.
  - **No three-account cast.** The page uses two accounts only; a separate producer account added a name without adding an idea.
  - **Three sections, eight short bullets each, one canvas each (`c1`, `c2`, `c3`).**
- **Data:** every chart is a diagram built from hardcoded literal labels — no datasets, no randomness, no `Math.random()`.
  - **Documented behaviour the page stands on:** a cross-account request needs an allow on the caller's side *and* on the bucket's side, while a same-account request needs only one of the two; a bucket owner cannot grant more than the caller's own account permits; the three grant shapes are naming the outside reader in the bucket policy, letting it borrow a role in the bucket's account, and giving it its own named entry point; an entry point can only narrow what the bucket policy already allows; borrowed-role credentials are short-lived; a file uploaded by a principal in another account was historically owned by that uploading account, so the bucket owner could be billed for a file it could not read but could delete; the successive fixes were handing ownership over on upload, then a bucket-level ownership setting, then the owner-enforced setting that switches off per-file permission lists and gives the bucket owner every file; owner-enforced is the default for buckets created since April 2023.
  - **Illustrative and labelled as such:** the two policy inputs in `c1`, the arrangement in `c2`, the four-step sequences in `c3`, and the nightly-job incident in section three.
  - **Computed geometry:** `c1` places panels with `panelX(i) = 30 + i × 350` and centres everything on `panelX + 155`; `c2` places columns with `colX(i) = 30 + i × 230`, nodes at `colX + 15` and node centres at `colX + 100`; `c3` places boxes with `bx(i) = 40 + i × 165`, box width 140, so each arrow spans the 25 px gap from `bx(i)+142` to `bx(i)+163` instead of overshooting into the next box.
