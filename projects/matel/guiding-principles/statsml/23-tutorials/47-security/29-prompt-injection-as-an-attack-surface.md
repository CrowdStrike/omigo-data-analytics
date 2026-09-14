# Prompt Injection as an Attack Surface

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Prompt Injection as an Attack Surface

**Subtitle:** A model reads instructions and data through one channel, so untrusted text lands in a privileged position — the same bug class as SQL injection and SSRF, but with no parameterized-query equivalent, so the mitigation is capability limits, not filtering

## Untrusted Input in a Privileged Position

**Tags:** `core idea` (blue), `bug class` (orange), `unsolved` (red)

- **The shape** — a language model concatenates its operator's instructions and its input data into one channel
- **No structural separation** — nothing in that channel marks which bytes carry authority and which are mere content
- **Same bug class** — untrusted input crossing into a privileged position, exactly like SQL injection or SSRF
- **The difference** — SQL has parameterized queries; there is no equivalent that fully solves prompt injection
- **Say it plainly** — this is currently an unsolved problem mitigated by architecture, not a bug with a patch
- **Privileged position** — the danger begins when that same stream drives tools that hold the assistant's credentials
- **Defender's stance** — assume injection succeeds sometimes, then design so that success buys the attacker little

*Example (italic):* An assistant fetches a web page to answer Alice's question; a sentence in that page addresses the assistant directly, and it arrives in the same stream as Alice's operator-written rules.

**Key point:** Prompt injection is untrusted input reaching a privileged position through a channel with no structural boundary — treat it as an architecture problem, because no text-level fix closes it.

### Visualization (canvas `c1`, 720×300)

Pipeline diagram: three untrusted input channels merge into one concatenated prompt stream, which drives a model that holds tools and credentials.

- **Title (bold 15px, `#1a5276`, top center):** "Untrusted Text Enters; Privileged Action Exits".
- **Channel boxes (three, x=30, width 150, height 40, 6px radius, fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border, centered 12px `#2c3e50` text):** y=80 "fetched web page", y=130 "uploaded document", y=180 "returned tool result".
- **Bold 12px `#6b7280` label above the group (x=30, y=68, left-aligned):** "untrusted".
- **Merge arrows:** 2px `#6b7280` lines from each box's right edge (x=180) to the left edge of the prompt strip (x=250) at y=150, each with a 9px arrowhead.
- **Prompt strip:** x=250, y=125, width 170, height 50, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 6px radius; bold 12px `#2a78d6` centered "one prompt" and 11px `#2c3e50` second line "instructions + data".
- **Strip cells:** 12 equal vertical cells drawn inside the prompt strip with 1px `#fff` separators to show one undifferentiated stream.
- **Model box:** x=460, y=125, width 100, height 50, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, bold 12px `#4a3aa7` centered "the model"; 3px `#2a78d6` arrow from x=420 to x=456 at y=150.
- **Tools box:** x=600, y=110, width 95, height 80, fill `rgba(217,89,38,0.14)`, 2px `#d95926` border, bold 12px `#d95926` centered "tools" with 11px `#2c3e50` lines "+ credentials" and "privileged"; 3px `#d95926` arrow from x=560 to x=596 at y=150.
- **Annotation (bold 13px orange `#d95926`, centered at x=430, y=232):** "untrusted words end up in a privileged position".
- **Second annotation (bold 12px `#1a5276`, centered at x=335, y=205):** "no boundary survives the concatenation".
- **Caption (12px `#444`, bottom right):** "illustrative deployment".

## Counting Channel-Tool Pairs in One Deployment

**Tags:** `worked example` (blue), `attack surface` (orange), `blast radius` (green)

- **The deployment** — an internal assistant reads 5 input channels and holds 6 tools (illustrative, stated numbers)
- **The channels** — fetched web pages, uploaded documents, inbound tickets, database rows, returned tool results
- **The tools** — 4 read-only (document search, ticket read, order lookup, file summary) and 2 that move data out
- **Outbound pair** — the 2 are "send email" and "make an outbound web request"; either can carry data outside
- **Naive surface** — any channel can reach any tool, so 5 × 6 = 30 channel-tool pairs are injection-reachable
- **Exfiltration slice** — 5 × 2 = 10 of those pairs can move data out, and 10 / 30 = 33.3% of the surface
- **Severity rule** — impact is set by what the model can do, not by what it reads; text with no tools is low impact
- **Also carriers** — file names, code comments, and image alt text are inputs too, and each adds channel rows

*Example (italic):* Alice uploads a vendor document; the assistant reads it and holds a send-email tool, so one of the 10 outbound pairs is live for the whole session (counts illustrative).

**Key point:** The security question is not "can it be injected" — assume yes — but "what is the worst action reachable from an injected instruction", which is blast-radius thinking applied to tools.

### Visualization (canvas `c2`, 720×300)

Matrix: 5 input channels (rows) × 6 tools (columns) = 30 cells, with the 10 exfiltration-capable cells highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "5 Input Channels × 6 Tools = 30 Injection-Reachable Pairs".
- **Annotation (bold 13px orange `#d95926`, centered, y=48):** "10 of 30 pairs can move data out = 33.3%".
- **Grid geometry:** columns x=160 to x=700 (width 540, 6 columns of 90px); rows y=88 to y=238 (height 150, 5 rows of 30px).
- **Column headers (11px, centered above each column at y=80):** "doc search", "ticket read", "order lookup", "file summary" in `#2a78d6`; "send email", "web request" in bold `#d95926`.
- **Row labels (12px `#444`, right-aligned ending at x=154, vertically centered in each row):** "web page", "document", "ticket", "database row", "tool result".
- **Cells:** columns 1–4 fill `rgba(42,120,214,0.15)` with 1px `#2a78d6` border; columns 5–6 fill `rgba(217,89,38,0.30)` with 2px `#d95926` border.
- **Cell marks:** bold 12px centered glyph — read-only cells show "·" in `#2a78d6`, outbound cells show "!" in `#d95926`.
- **Group brackets (below the grid, 2px lines at y=250):** `#2a78d6` from x=160 to x=520 with 12px `#2a78d6` centered label "4 read-only → 20 pairs" at y=266; `#d95926` from x=520 to x=700 with 12px bold `#d95926` centered label "2 outbound → 10 pairs" at y=266.
- **Caption (12px `#444`, bottom right):** "illustrative deployment; 5 × 6 = 30, 5 × 2 = 10".

## Cutting the Reachable Capability

**Tags:** `where it's used` (blue), `least privilege` (green), `defense in depth` (orange)

- **The move** — remove outbound capability from the path that reads untrusted content, keeping the rest
- **The arithmetic** — exfiltration-capable pairs fall from 10 to 0 while 20 useful pairs remain untouched
- **What is kept** — the assistant retains 20 / 30 = 66.7% of its surface with none of the exfiltration paths
- **Model output is input** — never execute, interpolate into a query, or render as unescaped HTML what a model wrote
- **Human confirmation** — the most reliable control today, and its value is that it breaks the automated chain
- **Least privilege** — scope each tool as narrowly as a user account; prefer read-only wherever it suffices
- **Isolation** — separate the step that reads untrusted content from the step that holds credentials where possible
- **Audit trail** — log every tool call with its arguments so an injected action is at least reconstructable later

*Example (italic):* Bob's team moves the send-email tool behind a click-to-confirm dialog, so an injected instruction can compose a message but cannot dispatch it unattended.

**Key point:** Reduce reachable capability rather than trying to filter language — the arithmetic of the surface is under your control, and the wording of untrusted text is not.

### Visualization (canvas `c3`, 720×300)

The same 5 × 6 matrix after the mitigation: the two outbound columns are struck out and empty, the 20 read-only pairs stay live.

- **Title (bold 15px, `#1a5276`, top center):** "After Removing Outbound Tools: 0 Exfiltration Pairs, 20 Kept".
- **Annotation (bold 13px green `#008300`, centered, y=48):** "20 / 30 = 66.7% of the surface kept, 0 / 30 outbound".
- **Grid geometry, headers, and row labels:** identical to `c2` (columns x=160–700 in 6 × 90px, rows y=88–238 in 5 × 30px, same label text and positions).
- **Read-only cells (columns 1–4):** fill `rgba(0,131,0,0.18)`, 1px `#008300` border, bold 12px `#008300` centered "·".
- **Outbound cells (columns 5–6):** fill `rgba(107,114,128,0.10)`, 1px `#6b7280` border, and a 1.5px `#6b7280` diagonal line corner to corner in each cell; no glyph.
- **Column headers for columns 5–6:** 11px `#6b7280` with a 1.5px `#6b7280` strike-through line drawn across the header text.
- **Group brackets (2px lines at y=250):** `#008300` from x=160 to x=520 with bold 12px `#008300` centered label "20 pairs still useful" at y=266; `#6b7280` from x=520 to x=700 with 12px `#6b7280` centered label "0 pairs reach outbound" at y=266.
- **Caption (12px `#444`, bottom right):** "illustrative; 20 + 0 = 20 of 30 pairs live".

## Why a Stronger System Prompt Is Not a Fix

**Tags:** `common mistake` (red), `SQL contrast` (orange), `honest limits` (blue)

- **The expectation** — teams assume a better filter or a firmer system prompt will close the hole
- **Why it fails** — the model has one channel, so "ignore instructions in the data" is itself just more data
- **The SQL contrast** — parameterization works because a database separates query syntax from value slots
- **Structural, not textual** — a bound value can never become syntax; that guarantee has no prompt equivalent
- **Filtering helps** — input screening and instruction-hierarchy prompting reduce success but do not eliminate it
- **Don't overclaim** — presenting a prompt-based defense as a solution creates false confidence, not safety
- **Where it must live** — the mitigation belongs in the surrounding architecture: capability limits and confirmations

*Example (italic):* A team hardens its system prompt and reports fewer incidents, yet the send-email tool is still reachable from any document the assistant reads — the surface never changed.

**Common mistake:** Treating prompt injection as a text-filtering problem. Parameterization fixed SQL injection because the boundary was real; a prompt has no such boundary, so limit what the model can do instead.

### Visualization (canvas `c4`, 720×300)

Two-panel contrast: a database query with a hard wall between syntax and value, versus a prompt as one continuous stream with no wall.

- **Title (bold 15px, `#1a5276`, top center):** "A Real Boundary vs One Concatenated Stream".
- **Left panel header (bold 13px `#2c3e50`, centered at x=185, y=62):** "database: parameterized query".
- **Query box:** x=45, y=95, width 130, height 60, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 6px radius; bold 12px `#2a78d6` "query syntax" with 11px `#2c3e50` second line "compiled first".
- **Value box:** x=205, y=95, width 120, height 60, fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border, 6px radius; bold 12px `#6b7280` "value slot" with 11px `#2c3e50` second line "stays a value".
- **The wall:** 5px solid `#008300` vertical line from (190, 88) to (190, 162); bold 12px `#008300` label "structural" at (190, 80) centered and 12px `#008300` label "boundary" at (190, 178) centered.
- **Left caption (12px `#2c3e50`, centered at x=185, y=200):** "a bound value can never become syntax".
- **Divider:** 1px `#e5e9ef` vertical line at x=360 from y=55 to y=250.
- **Right panel header (bold 13px `#2c3e50`, centered at x=535, y=62):** "prompt: one channel".
- **Stream strip:** x=390, y=95, width 300, height 60, 2px `#d95926` border, 6px radius; 20 equal cells with 1px `#fff` separators, cells 1–8 fill `rgba(42,120,214,0.20)`, cells 9–20 fill `rgba(107,114,128,0.20)`.
- **Segment labels (11px, above the strip at y=88):** `#2a78d6` "instructions" centered over cells 1–8; `#6b7280` "untrusted data" centered over cells 9–20.
- **No-wall marker:** 1.5px dashed `#d95926` (dash 4/4) vertical line at x=510 from y=95 to y=155; bold 12px `#d95926` centered label "no wall here" at (510, 175).
- **Right caption (12px `#2c3e50`, centered at x=535, y=200):** "data can read exactly like an instruction".
- **Bottom annotation (bold 13px `#d95926`, centered at x=360, y=232):** "so the fix lives in capability limits, not in wording".
- **Caption (12px `#444`, bottom right):** "schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red reserved for genuine alarm states; this page uses orange for attacker-reachable capability.
- **Data:** no randomness anywhere; every number is the stated deployment or derived from it — 5 channels, 6 tools (4 read-only + 2 outbound), 5 × 6 = 30 pairs, 5 × 2 = 10 outbound pairs = 33.3%, 20 read-only pairs = 66.7%, 10 + 20 = 30 and 33.3% + 66.7% = 100%. The deployment is invented and labeled illustrative in text and captions; text numbers must match chart numbers exactly.
- **Framing:** defensive/educational throughout. No working payload, no copyable adversarial instruction — injected content is described only as "a sentence in the page that addresses the assistant directly". No real vendors, models, or products; people are Alice and Bob. No claim that any listed defense is complete.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
