# The Confused Deputy: Agent Tool Permissions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Confused Deputy: Agent Tool Permissions

**Subtitle:** A program holds authority for one party and takes instructions from another — the deputy is not malicious and not broken, it simply cannot tell that the two should never be combined

## The Compiler That Deleted the Billing File

**Tags:** `core idea` (blue), `1988 problem` (orange), `authorization design` (green)

- **The service** — a shared compiler appends to the system's billing file, so it holds write access to that file
- **The user** — Alice invokes the compiler and names where her output should be written; it writes where told
- **The collision** — Alice names the billing file as her output target, and the compiler dutifully overwrites it
- **Two sources** — the authority came from the compiler's standing grant, the target came from Alice's argument
- **Nothing was hacked** — no bug, no stolen password; each half of the decision was legitimate on its own
- **The name** — a 1988 paper called this the *confused deputy*: a program confused about who is asking
- **The signature** — "holds authority for A" plus "takes instructions from B" is the whole pattern

*Example (italic):* Alice cannot write the billing file and the compiler has no reason to want it gone — yet the compiler, acting with its authority on her choice of target, destroys it.

**Key point:** The failure is not in either party. Authority and intent arrived from different sources and the deputy had no way to see that combining them was never authorized.

### Visualization (canvas `c1`, 720×300)

Diagram: two arrows arrive at the deputy from different sources — intent from the user, authority from the system grant — and the deputy cannot distinguish them.

- **Title (bold 15px, `#1a5276`, top center):** "Authority and Intent Arrive From Different Sources".
- **Alice box:** violet-tinted rounded box at x=30, y=58, 175×52, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, centered 12px `#2c3e50` two-line text "Alice (user)" / "names the output file".
- **Grant box:** aqua-tinted rounded box at x=30, y=188, 175×52, fill `rgba(25,158,112,0.12)`, 2px `#199e70` border, two-line text "system grant" / "may write the billing file".
- **Deputy box:** blue rounded box at x=288, y=112, 150×74, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, two-line text "compiler service" / "(the deputy)".
- **Outcome box:** orange rounded box at x=510, y=123, 180×52, fill `rgba(217,89,38,0.13)`, 2px `#d95926` border, two-line text "billing file" / "overwritten".
- **Arrows (3px, arrowheads 10px):** violet `#4a3aa7` from (205,84) to (284,138); aqua `#199e70` from (205,214) to (284,162); blue `#2a78d6` from (438,149) to (506,149).
- **Arrow labels (12px):** violet "intent: which file" centered at (246,98); aqua "authority: may write it" centered at (246,232).
- **Deputy note (11px `#6b7280`, centered at (363,205)):** "cannot tell them apart".
- **Annotation (bold 13px magenta `#d55181`, centered at x=360, y=282):** "neither half was illegitimate on its own".
- **Caption (12px `#444`, bottom right, y=h-10):** none — annotation occupies the bottom line.

## The Same Shape in Four Familiar Places

**Tags:** `where it's used` (blue), `pattern recognition` (orange)

- **Browser (CSRF)** — it holds your session cookie and is told what to request by a page you did not write
- **Server (SSRF)** — it holds a position inside the network and is told which URL to fetch by request input
- **Build system** — it holds deploy credentials and executes a config file that arrived in a pull request
- **Assistant** — it holds Alice's mailbox and file access and takes direction from the text it just read
- **Same signature** — in all four the authority is standing and ambient while the intent arrives from outside
- **Why it recurs** — ambient authority is convenient: the deputy never has to be told what it may touch
- **How to spot it** — list what a component holds, list who can talk to it; an overlap means expect this bug

*Example (italic):* A pipeline with deploy credentials that runs a build file from an untrusted branch is the 1988 compiler with newer vocabulary.

**Key point:** The AI agent is the newest instance, not a new species — it is simply an unusually capable deputy that reads unusually untrusted input.

### Visualization (canvas `c2`, 720×300)

Four side-by-side panels: the same "holds / instructed by / result" structure filled in for the compiler, the browser, the server, and the assistant.

- **Title (bold 15px, `#1a5276`, top center):** "One Pattern, Four Deputies".
- **Panels:** four rounded boxes, y=52, height 196, width 162, at x = 25, 197, 369, 541; 8px radius, 2px border, 12% tinted fill. Colors in order: violet `#4a3aa7`, blue `#2a78d6`, aqua `#199e70`, orange `#d95926`.
- **Panel headers (bold 13px in the panel's border color, centered):** "compiler, 1988", "browser (CSRF)", "server (SSRF)", "assistant".
- **Row labels (11px `#6b7280`, centered):** "holds" at y=108, "instructed by" at y=162.
- **"holds" values (12px `#2c3e50`, centered, two lines at y=126 and y=141):** compiler "write access to" / "the billing file"; browser "your session" / "cookie"; server "a position inside" / "the network"; assistant "the mailbox and" / "the file store".
- **"instructed by" values (12px `#2c3e50`, centered, two lines at y=180 and y=195):** compiler "the user's output" / "filename"; browser "a form on a" / "foreign page"; server "a URL in the" / "request body"; assistant "the text it" / "just read".
- **Result footers (bold 12px in the panel's border color, centered, two lines at y=222 and y=237):** compiler "billing file" / "overwritten"; browser "a request you" / "never chose"; server "internal service" / "reached"; assistant "an action Alice" / "never asked for".
- **Divider:** 1px `#e5e9ef` horizontal line inside each panel at y=152, inset 10px from the panel edges.
- **Caption (12px `#444`, bottom right):** "same structure, four eras".

## Counting the Over-Grant: 15,000 Items Down to 2

**Tags:** `worked example` (blue), `least privilege` (green), `capabilities` (orange)

- **The grant** — an assistant is configured with a mailbox of 12,000 messages, 3,000 files, and send-to-anyone
- **The task** — Alice asks it to summarize 1 document and reply to 1 thread, so it needs 1 file, 1 recipient
- **Over-grant on files** — 3,000 available ÷ 1 needed = 3,000x more document reach than the task requires
- **Over-grant on messages** — 12,000 ÷ 1 = 12,000x, and outbound send is unbounded rather than merely 12,000x
- **Standing reach** — 12,000 + 3,000 = 15,000 items that any instruction the deputy reads can act upon
- **Scoped reach** — grant only the named file and the named recipient: 2 items, and 2 ÷ 15,000 = 0.013%
- **The mechanism** — a capability names the specific object and action, so ambient identity stops deciding
- **Short-lived** — issue it per request and expire it with the request; standing credentials are the root cause

*Example (italic):* Same task, same assistant, same answer for Alice — but a misdirected instruction can reach 2 objects instead of 15,000 (counts illustrative).

**Key point:** 2 ÷ 15,000 = 0.013% is the quantitative case for per-request scoping: the useful work is unchanged and the reachable surface falls by roughly four orders of magnitude.

### Visualization (canvas `c3`, 720×300)

Horizontal comparison: a full-width stacked bar for the standing grant (12,000 messages + 3,000 files = 15,000) against a near-invisible sliver for the request-scoped grant (2).

- **Title (bold 15px, `#1a5276`, top center):** "Reachable Items: Standing Grant vs Request-Scoped".
- **Standing-grant bar:** 30px tall at y=88, from x=200, total width 460px, split into 368px messages (`rgba(42,120,214,0.35)` fill, 2px `#2a78d6` border) then 92px files (`rgba(217,89,38,0.30)` fill, 2px `#d95926` border); widths are 460 × 12,000/15,000 = 368 and 460 × 3,000/15,000 = 92.
- **Segment labels (11px `#2c3e50`, centered in each segment at y=107):** "12,000 messages", "3,000 files".
- **Scoped bar:** 30px tall at y=168, from x=200, width 3px (460 × 2/15,000 rounds below 1px, drawn at a 3px minimum), fill `rgba(0,131,0,0.65)`, 2px `#008300` border.
- **Row labels (12px `#444`, right-aligned ending at x=190, two lines each):** "standing grant" / "(configured once)" at y=100 and y=116; "request-scoped" / "(1 file + 1 recipient)" at y=180 and y=196.
- **Value labels (bold 12px):** `#1a5276` "15,000" at (668, 107); `#008300` "2  —  too small to see at this scale" at (212, 187).
- **Annotation (bold 13px green `#008300`, x=200, y=232):** "2 / 15,000 = 0.013% of the standing reach".
- **Note (12px `#6b7280`, x=200, y=254):** "over-grant: 3,000x on files, 12,000x on messages".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## Not an AI Problem — an Authorization Problem

**Tags:** `common mistake` (red), `defensive design` (green), `audit` (orange)

- **The framing error** — this gets filed as an AI problem, so teams go shopping for an AI-specific detector
- **What it actually is** — an authorization-design problem understood since 1988; the agent is one more deputy
- **Wrong variable** — "the assistant is trustworthy" is irrelevant; its trustworthiness was never the failing part
- **Right question** — whose instructions will it act on, and with whose authority attached to those actions
- **Confirmation gates** — human approval on outbound or destructive steps re-attaches a person's intent to the authority
- **Split the roles** — the component that ingests untrusted content should not be the one holding the credentials
- **Log the why** — record the reason the deputy believed it was acting, so an audit separates delegated from injected intent
- **No detector** — none of this spots a hostile instruction; it caps what one can accomplish, which is the correct posture

*Example (italic):* An assistant that reads a shared document and an assistant that sends messages can be the same product but should not be the same credential holder.

**Common mistake:** Debating whether the model is trustworthy. Bind authority to the intent of a specific request rather than to the identity of a standing session, and the deputy's honesty stops being load-bearing.

### Visualization (canvas `c4`, 720×300)

2×2 matrix: authority model (ambient standing vs per-request capability) against instruction source (from Alice vs from ingested content), with the outcome in each cell.

- **Title (bold 15px, `#1a5276`, top center):** "Outcome Depends on Authority Model, Not on Trust".
- **Column headers (bold 12px `#2c3e50`, centered, two lines at y=50 and y=66):** column 1 (x 250–450, center 350) "instructions" / "from Alice"; column 2 (x 460–660, center 560) "instructions from" / "ingested content".
- **Row labels (12px `#444`, right-aligned ending at x=240, two lines each):** "ambient standing" / "authority" at y=118 and y=134; "per-request" / "capability" at y=208 and y=224.
- **Cells:** four rounded boxes 200×90, 8px radius, 2px border, at (250,80), (460,80), (250,180), (460,180).
- **Cell contents (bold 12px in the border color on line 1 at cell y+36, 12px `#2c3e50` on line 2 at cell y+58, both centered):**
  - (ambient, Alice): border `#008300`, fill `rgba(0,131,0,0.10)` — "intended action" / "correct result".
  - (ambient, ingested): border `#e74c3c`, fill `rgba(231,76,60,0.12)` — "confused deputy" / "15,000 items reachable".
  - (per-request, Alice): border `#008300`, fill `rgba(0,131,0,0.10)` — "intended action" / "2 items reachable".
  - (per-request, ingested): border `#199e70`, fill `rgba(25,158,112,0.10)` — "refused" / "outside the 2 named items".
- **Annotation (bold 12px violet `#4a3aa7`, x=30, y=290):** "the deputy's trustworthiness is not an axis on this chart".
- **Caption (12px `#444`, bottom right, y=290):** "illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Helpers: `roundBoxTL` for rounded boxes, `arrowHead` for arrowheads, `lines` for centered multi-line text.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` used only for the genuine failure cell in `c4`.
- **Data:** no randomness anywhere; every figure is a hardcoded literal. The grant (12,000 messages, 3,000 files, 1 file + 1 recipient needed) is invented and labeled illustrative. Derived figures must be recomputed if the grant changes: 3,000 ÷ 1 = 3,000x, 12,000 ÷ 1 = 12,000x, 12,000 + 3,000 = 15,000, 2 ÷ 15,000 = 0.0001333 → 0.013%. Bar segment widths derive from the same totals: 460 × 12,000/15,000 = 368 px and 460 × 3,000/15,000 = 92 px, summing to the 460 px whole. Text numbers must match chart numbers exactly.
- **Framing:** conceptual and defensive throughout. No injection payload, no adversarial instruction text, no credential strings, no named vendors, models, or products. The 1988 confused-deputy formulation is cited as documented computer-science history.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
