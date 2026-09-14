# SMTP & How Email Works

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SMTP & How Email Works

**Subtitle:** Email still runs on SMTP, a 1982 protocol where any domain's server hands mail directly to any other's — one of the oldest federated systems still running

## A Coffee Receipt Crosses the Internet

**Tags:** `core idea` (blue), `federation` (green), `mail relay` (orange)

- **The receipt** — a coffee shop's register emails a receipt from receipts@beanhouse.example to maya@inboxly.example
- **The handoff** — the register gives the message to beanhouse.example's own outgoing SMTP server
- **The lookup** — that server asks DNS: "who accepts mail for inboxly.example?" — the MX record answers "mx1.inboxly.example"
- **The delivery** — beanhouse's server opens a connection straight to mx1.inboxly.example and pushes the message
- **No middleman** — there is no central email company in the path; the two domains' servers talk directly

*Example (italic):* Maya orders a latte at 9:14am; by 9:15am the receipt sits in a mailbox on mx1.inboxly.example, having passed through exactly two mail servers.

**Key point:** SMTP is store-and-forward between independent servers: the sender's server finds the receiver's server via a DNS MX lookup and delivers directly — federation, not a hub.

### Visualization (canvas `c1`, 720×300)

Flow diagram of one email's hops: register app → beanhouse SMTP server → (DNS MX lookup) → mx1.inboxly.example → Maya's mailbox, left to right on one lane with the DNS box above.

- **Title (bold 15px, `#1a5276`, top center):** "One Receipt, Two Servers, Zero Middlemen".
- **Main lane (boxes at y=170, 44px tall, 8px radius, 12px `#2c3e50` text):** blue `#2a78d6` box at x=25 w=130 "register app"; blue box at x=195 w=160 "beanhouse SMTP server"; green `#008300` box at x=415 w=170 "mx1.inboxly.example"; green box at x=625 w=80 "Maya's mailbox". Box fills `rgba(42,120,214,0.15)` for blue, `rgba(0,131,0,0.12)` for green.
- **Arrows:** 3px `#1a5276` solid arrows between consecutive boxes, 11px `#6b7280` labels under each: "submit", "SMTP push", "stored".
- **DNS box:** aqua `#199e70` rounded box at x=270 y=60 w=220 h=38, fill `rgba(25,158,112,0.12)`, 12px text "DNS: MX inboxly.example → mx1"; dashed `#6b7280` (dash 4/3) up-and-back arrow linking it to the beanhouse box, 11px `#6b7280` label "who accepts mail?".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=260):** "no central hub — any domain's server can deliver to any other's".
- **Caption (12px `#444`, bottom right):** "timing illustrative; hop sequence exact".

## Five Commands, a Message, Seven Replies

**Tags:** `worked example` (blue), `reply codes` (green)

- **The dialogue** — SMTP is plain text: the sender types 5 commands plus the message, the receiver answers with 7 numbered replies
- **The greeting** — the receiver speaks first: "220 mx1.inboxly.example ready" before any command
- **The envelope** — HELO, MAIL FROM, RCPT TO name the sender and recipient before any content is sent
- **The content** — DATA gets reply 354 ("go ahead"); the message ends with a lone "." on its own line
- **The code rule** — first digit tells all: 2 = done, 3 = send more, 4 = try again later, 5 = never retry
- **Hand-check** — count the replies below: four 250s, one 220, one 354, one 221 — every first digit is 2 or 3, so delivery succeeded

*Example (italic):* Beanhouse's server sends HELO, MAIL FROM, RCPT TO, DATA, the message, QUIT — and reads 220, 250, 250, 250, 354, 250, 221 back, all success codes.

**Key point:** An SMTP delivery is a short scripted conversation you can read by eye — the three-digit reply codes, not the prose after them, are what the sending machine acts on.

### Visualization (canvas `c2`, 720×300)

Ladder (sequence) diagram: client lifeline left, server lifeline right, 13 alternating arrows carrying the exact commands and reply codes of one delivery.

- **Title (bold 15px, `#1a5276`, top center):** "The Whole Delivery Is 13 Lines of Text".
- **Lifelines:** vertical 2px `#e5e9ef` lines at x=150 and x=570 from y=62 to y=278; bold 12px `#1a5276` headers above them: "beanhouse server (client)" and "mx1.inboxly (server)".
- **Arrows (2px, arrowheads, one per row at y = 72 + 16·i for i = 0..12, label 11px centered above each arrow):** direction ← means server-to-client (label color green `#008300`, code 354 in yellow `#c98500`), → means client-to-server (blue `#2a78d6`):
  - ← `220 mx1.inboxly.example ready`
  - → `HELO beanhouse.example`
  - ← `250 ok`
  - → `MAIL FROM:<receipts@beanhouse.example>`
  - ← `250 ok`
  - → `RCPT TO:<maya@inboxly.example>`
  - ← `250 ok`
  - → `DATA`
  - ← `354 go ahead, end with "."` (yellow `#c98500`)
  - → `Subject: Your receipt ... "."`
  - ← `250 accepted, queued`
  - → `QUIT`
  - ← `221 bye`
- **Annotation (bold 12px green `#008300`, right margin near y=290):** "every reply starting with 2 = success".
- **Caption (12px `#444`, bottom left):** "dialogue verbatim SMTP; message body shortened".

## Deliverability: Why Authenticated Mail Reaches the Inbox

**Tags:** `where it's used` (blue), `SPF / DKIM` (green), `data pipelines` (orange)

- **The open door** — federation means anyone's server can claim to be beanhouse.example, so receivers verify
- **SPF** — a DNS record listing which server IPs are allowed to send mail for the domain
- **DKIM** — a cryptographic signature in the headers proving the message wasn't altered and came from the domain
- **The scoring** — receiving servers score each message; unauthenticated mail is spam-foldered or refused
- **The pipeline** — receipts, alerts, and reports ride SMTP; the 4xx/5xx bounce codes feed delivery dashboards

*Example (italic):* Of 10,000 receipts sent in a month, 6,200 reach the inbox without SPF/DKIM; with both records set, 9,600 do — same messages, different trust.

**Key point:** Because SMTP itself trusts anyone, deliverability is earned in DNS — SPF and DKIM are the two records that tell the receiving server the sender is who it claims.

### Visualization (canvas `c3`, 720×300)

Two stacked horizontal bars: where 10,000 receipts land without vs with SPF+DKIM, segments for inbox / spam folder / bounced.

- **Title (bold 15px, `#1a5276`, top center):** "10,000 Receipts: Same Mail, Different DNS Records".
- **Geometry:** bars start at x=190, full scale 440px = 10,000 messages, 34px tall; row labels 12px `#444` right-aligned at x=180.
- **Row 1 (y=100), label "no SPF / DKIM":** green `#008300` segment width 273 (6,200 inbox), yellow `#c98500` segment width 128 (2,900 spam folder), red `#e74c3c` segment width 40 (900 bounced); 11px white count labels inside the two wide segments, 11px red "900" beside the bar end.
- **Row 2 (y=185), label "SPF + DKIM set":** green segment width 422 (9,600 inbox), yellow width 13 (300), red width 4 (100); 11px labels "300 / 100" in `#6b7280` beside the bar end.
- **Legend (11px, under title at y=62):** green swatch "inbox", yellow swatch "spam folder", red swatch "bounced".
- **Annotation (bold 13px magenta `#d55181`, centered near y=255):** "+3,400 receipts reach the inbox — two DNS records did that".
- **Caption (12px `#444`, bottom right):** "counts illustrative".

## SMTP Sends; IMAP Reads

**Tags:** `common mistake` (red), `push vs pull` (orange)

- **The confusion** — people say "my email app uses SMTP"; SMTP only pushes mail toward the mailbox
- **Push vs pull** — SMTP is servers pushing to servers; the phone pulls from the mailbox with IMAP or POP
- **The mailbox** — the receipt sits stored on mx1.inboxly.example until Maya's phone asks for it
- **Two addresses** — the envelope's RCPT TO decides delivery; the To: header is just display text
- **The symptom** — an app's "SMTP settings" configure sending only; mail not showing up is an IMAP problem

*Example (italic):* Maya's phone shows no new mail on the subway — the receipt was delivered by SMTP an hour ago and waits on the server until IMAP can pull it.

**Common mistake:** Debugging "email not arriving on my phone" in the SMTP settings. SMTP's job ends at the recipient server's mailbox — everything after that is a retrieval protocol, not SMTP.

### Visualization (canvas `c4`, 720×300)

Two-lane diagram: top lane the SMTP push from beanhouse's server into the mx1 mailbox, bottom lane the IMAP pull from that mailbox to Maya's phone, with the mailbox as the shared box.

- **Title (bold 15px, `#1a5276`, top center):** "SMTP Stops at the Mailbox".
- **Shared box:** ink-bordered `#1a5276` rounded box at x=300 y=125 w=160 h=54, fill `rgba(26,82,118,0.10)`, 12px text "mailbox on mx1.inboxly.example".
- **Top lane (y=95):** blue `#2a78d6` box at x=40 w=180 "beanhouse SMTP server", 3px blue arrow into the mailbox box, bold 12px blue label above the arrow "SMTP push (port 25)".
- **Bottom lane (y=215):** green `#008300` box at x=520 w=160 "Maya's phone", 3px green arrow FROM the mailbox to the phone, bold 12px green label under the arrow "IMAP pull (port 993)".
- **Lane captions (11px `#6b7280`):** "delivery — server to server" under the top lane, "reading — client asks the server" under the bottom lane.
- **Annotation (bold 13px orange `#d95926`, centered near y=275):** "sending and reading are two different protocols".
- **Caption (12px `#444`, bottom right):** "ports are the real defaults; layout schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded literals above (no randomness); the c2 ladder lines and reply codes (220/250/354/221) are real SMTP verbatim; c3 delivery counts (6,200/2,900/900 vs 9,600/300/100 of 10,000) are invented and labeled illustrative, segment widths = count/10,000 × 440px; c4 ports 25 (SMTP) and 993 (IMAPS) are the true defaults. Domains are generic `.example` names.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
