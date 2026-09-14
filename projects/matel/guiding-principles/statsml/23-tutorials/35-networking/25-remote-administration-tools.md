# Remote Administration Tools

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Remote Administration Tools

**Subtitle:** A legitimate tool category that gives a technician full interactive control of a distant machine — built on consent, visible sessions, and audit logs

## Fixing a Laptop in Another City Without Leaving Your Desk

**Tags:** `core idea` (blue), `remote control` (green), `consent` (orange)

- **The ticket** — a sales laptop in another city refuses a patch; the technician sits at headquarters
- **The accept** — the employee clicks "accept" on a prompt; nothing starts until she does
- **The control** — the technician now sees her screen, moves her mouse, and installs the patch
- **The visibility** — the employee watches every click; a banner shows the session is live
- **The record** — the session's start, actions, and disconnect land in an audit log
- **The category** — remote desktop, helpdesk screen-sharing, and RMM fleet agents all work this way

*Example (italic):* At 10:02 the employee clicks "accept"; by 10:14 the patch is installed and the session ends, with both sides having seen everything.

**Key point:** A remote administration tool grants full interactive control of a distant machine — legitimately, because consent, a visible session, and an audit log are built into the workflow.

### Visualization (canvas `c1`, 720×300)

Timeline strip of the helpdesk call: four dated events on a baseline — session request, employee accept, visible session start, logged disconnect — with a shaded green band for the running visible session.

- **Title (bold 15px, `#1a5276`, top center):** "One Helpdesk Call, Start to Logged Finish".
- **Baseline:** 2px `#999` horizontal line at y=185 from x=60 to x=680.
- **Session band:** fill `rgba(0,131,0,0.12)` with 1px `#008300` border from x=340 to x=620, y=160 to y=185 (10:03 to 10:14); bold 12px `#008300` label "visible session — patch installed" centered at (480, 152).
- **Events (each: colored 6px-radius dot on the baseline, dashed 3/3 1.5px vertical connector up to a two-line 12px label in the event color, and a 12px `#444` time label at baseline+24):**
  - x=100, "10:01", green `#008300`, label at y=70: "technician sends" / "a session request"
  - x=220, "10:02", blue `#2a78d6`, label at y=118: "employee clicks accept —" / "nothing starts before that"
  - x=340, "10:03", violet `#4a3aa7`, label at y=70: "control begins — a banner" / "shows the session is live"
  - x=620, "10:14", orange `#d95926`, label at y=118: "disconnect — session" / "written to the audit log"
- **Annotation (bold 13px violet `#4a3aa7`, centered at (360, 248)):** "consent starts it, a banner shows it, the audit log records it".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Two Outbound Calls Meet at the Vendor's Relay

**Tags:** `worked example` (blue), `NAT` (green), `relay` (orange)

- **The blocker** — both machines sit behind routers doing NAT; neither can dial the other directly
- **The workaround** — each machine keeps an outbound connection open to the vendor's relay server
- **The code** — the employee reads a 9-digit session code to the technician: 483 291 657
- **The match** — the technician enters it; the relay pairs the two outbound connections into a session
- **The shortcut** — when the routers allow it, the relay brokers a direct peer-to-peer path instead
- **The close** — on disconnect the relay logs who connected, when, and for how long

*Example (italic):* The laptop dialed out at 10:01, the technician's console at 10:02; the relay matched code 483 291 657 and the encrypted session ran until 10:14.

**Key point:** Outbound connections pass through NAT where inbound ones cannot — so both sides dial out to a relay, and the session code tells the relay which two callers to join.

### Visualization (canvas `c2`, 720×300)

Session diagram: technician's machine and sales laptop at the bottom, each behind its own NAT router, both with outbound arrows up to the vendor's relay at top center; the direct path between the machines is crossed out.

- **Title (bold 15px, `#1a5276`, top center):** "One Session, Three Machines: Both Sides Dial Out to the Relay".
- **Relay box:** x=270, y=48, 180×70, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border; bold 12px `#4a3aa7` label "the vendor's relay" centered at y=74; bold 12px `#4a3aa7` line "session 483 291 657 — matched" centered at (360, 110), inside the box.
- **Machine boxes (y=210, 48px tall, 12px `#2c3e50` two-line labels):** "HQ technician" / "(viewer + control)" at x=40 width 170, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; "sales laptop" / "(employee clicked accept)" at x=510 width 170, fill `rgba(0,131,0,0.12)`, 2px `#008300` border.
- **Router boxes (y=140, 120×30, fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border, 11px `#6b7280` labels):** "office router (NAT)" at x=65; "home router (NAT)" at x=535.
- **Outbound arrows:** left side 2.5px `#2a78d6` — vertical from (125,210) up to (125,174) with arrowhead, then diagonal from (125,140) to (266,84) with arrowhead at the relay's left edge; 12px `#2a78d6` label "outbound" at (170, 105). Right side mirrored in 2.5px `#008300` — (595,210)→(595,174), then (595,140)→(454,84); 12px `#008300` label "outbound" at (595 side, x=550, y=105).
- **Blocked direct path:** dashed (5/4) 1.5px `#6b7280` horizontal line between the machine boxes from (214,234) to (506,234); bold 16px red `#e74c3c` "×" at (360,240); bold 11px red label "direct dial blocked by NAT" at (360, 262).
- **Annotation (bold 13px orange `#d95926`, centered at (360, 190)):** "outbound connections pass NAT; inbound ones don't".
- **Caption (12px `#444`, bottom right):** "session code illustrative".

## From One Helpdesk Ticket to Five Thousand Laptops

**Tags:** `where it's used` (blue), `fleet management` (green), `trade-off` (red)

- **Helpdesk** — support staff shadow a user's screen with consent, fix the issue, disconnect
- **Server rooms** — admins live in SSH and RDP; most servers are managed only remotely
- **Fleets** — unattended RMM agents sit on thousands of machines for patching and monitoring
- **Work from home** — the same category lets an employee reach an office desktop from anywhere
- **The trade-off** — the agent that patches 5,000 laptops overnight is one high-value target
- **The wrapping** — enterprises add MFA, allowlists, and session recording around that agent

*Example (italic):* One console pushes Tuesday night's security patch to 5,000 laptops while their owners sleep — which is exactly why that console sits behind MFA and an allowlist.

**Key point:** Standing remote-admin agents trade convenience for concentration of power: one console reaches every machine, so the console itself must be defended like a vault.

### Visualization (canvas `c3`, 720×300)

Fleet-management fan-out: one RMM console at the top inside a dashed security wrapper, green arrows fanning out to a grid of one hundred small agent squares standing in for 5,000 laptops.

- **Title (bold 15px, `#1a5276`, top center):** "Patch Night: One Console Reaches the Whole Fleet".
- **Security wrapper:** dashed (5/4) 1.5px `#4a3aa7` rectangle at x=255, y=38, 210×56 around the console; 11px `#4a3aa7` two-line label left-aligned at x=480: "MFA + allowlist" (y=58) / "+ session recording" (y=73).
- **Console box:** x=280, y=46, 160×40, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border; bold 12px `#1a5276` label "RMM console" centered at (360, 71).
- **Fan-out arrows:** five 2px `#008300` arrows from (360, 94) down to (x, 144–146) with arrowheads, x targets = `[100, 230, 360, 490, 620]`.
- **Agent grid:** 4 rows × 25 columns of 14×14 squares, 22px pitch, starting at (70, 152); fill `rgba(0,131,0,0.25)` with 1px `#008300` border (patched), except indices `[3, 20, 33, 47, 61, 78, 92]` (row-major) which use fill `rgba(107,114,128,0.25)` with 1px `#6b7280` border (offline).
- **Legend (y=246, 12×12 swatches, 11px `#444` labels):** green swatch at x=150 + "patched overnight"; grey swatch at x=300 + "offline — retried next run".
- **Annotation (bold 12px orange `#d95926`, centered at (300, 280)):** "the same standing agent is a single high-value target".
- **Caption (12px `#444`, bottom right):** "grid is a schematic sample of 5,000 — illustrative".

## One Acronym, Two Very Different RATs

**Tags:** `common mistake` (red), `acronym collision` (orange)

- **The collision** — "RAT" means both Remote Administration Tool and Remote Access Trojan
- **Same capability** — both watch the screen, move the mouse, run commands, and move files
- **The difference** — consent, disclosure, and audit trails separate them; capability alone cannot
- **The abuse** — attackers increasingly install legitimate remote-admin software to blend in
- **The response** — security teams watch even the legitimate category: who installed it, who approved

*Example (italic):* A helpdesk session and a trojan can drive the identical screen-and-mouse channel — one announced itself with an "accept" prompt and a log entry; the other told no one.

**Common mistake:** Judging the software by what it can do. The remote-control channel is not what makes a trojan a trojan — the missing consent, hidden install, and absent logs are.

### Visualization (canvas `c4`, 720×300)

Side-by-side comparison table drawn on canvas: Remote Administration Tool vs Remote Access Trojan across five rows, with the identical capability row highlighted in yellow.

- **Title (bold 15px, `#1a5276`, top center):** "Two RATs: Identical Capability, Opposite Everything Else".
- **Table geometry:** column edges at x = 30 (row labels), 200, 445, 690; header row y=45 to y=75; five body rows of 34px each ending at y=245; all grid lines 1px `#e5e9ef`.
- **Headers:** "Remote Administration Tool" bold 12px `#008300` centered over cell fill `rgba(0,131,0,0.12)`; "Remote Access Trojan" bold 12px `#e74c3c` centered over cell fill `rgba(231,76,60,0.12)`.
- **Rows (label bold 12px `#1a5276` left-aligned at x=40; cell text 12px `#2c3e50` centered per column):**
  - capability — "full remote control of the machine" | "full remote control of the machine" (this row spans all three columns with fill `rgba(201,133,0,0.15)` and a 2px `#c98500` border)
  - consent — "user clicks accept; IT approved" | "none — the user never agreed"
  - visibility — "visible banner during the session" | "hidden; runs silently"
  - installation — "installed and disclosed by IT" | "delivered covertly by malware"
  - logging — "every session in the audit log" | "no logs; hides its tracks"
- **Annotation (bold 12px yellow `#c98500`, centered at (360, 271)):** "the capability row is identical — consent, disclosure, and logs tell them apart".
- **Caption (12px `#444`, bottom right):** "same channel, opposite legitimacy".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates; charts collected in a `__charts` array of functions, all invoked once at load and again on a 150 ms debounced window resize; shared `arrowHead(ctx,x,y,angle,color)` and `box(ctx,x,y,w,h,fill,stroke)` helpers.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red `#e74c3c` appears only for the blocked path and the trojan column (genuine alarm states).
- **Data:** everything is hardcoded (no randomness): the 483 291 657 session code, the 10:01/10:02/10:03/10:14 timeline, the fleet fan-out targets `[100, 230, 360, 490, 620]`, the offline-square indices `[3, 20, 33, 47, 61, 78, 92]`, and every table cell string above; the session code, timings, and fleet grid are invented and labeled illustrative. Vendor kept generic ("the vendor's relay"); TeamViewer-style products are never named, and SSH/RDP/RMM are cited only as category examples with no product-specific behavior claimed. The NAT/outbound-relay/session-code mechanism and the Administration-Tool-vs-Access-Trojan distinction (consent, disclosure, logging — not capability) are the documented industry facts the page teaches.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
