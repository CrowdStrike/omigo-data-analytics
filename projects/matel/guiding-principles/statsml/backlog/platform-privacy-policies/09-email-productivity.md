# Email / Productivity Suite

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Email / Productivity Suite

**Subtitle:** The mailbox is the archive of your life — receipts, travel, finances, relationships — and the scanning that filters spam can also read all of it as features.

**Disclaimer callout:** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** messages you write, contacts, calendar events, documents, spreadsheets, uploaded files.
- **Incidental:** every message you *receive* — a decades-long archive of receipts, tickets, statements, confirmations; attachment scanning (spam/malware pipelines that can also extract content features); read and open timestamps; IP, device, and client telemetry; typing and editing telemetry inside documents; sharing and access logs.
- **Inferred:** purchase history parsed from receipts; travel patterns from bookings; financial picture from statements; your full correspondence graph (who, how often, response times); meeting graph and movements from the calendar; document topics.

**Key point (callout box):** A mailbox is the most complete personal archive that exists — a machine-readable record of what you bought, where you went, and who you know, going back decades. Structured parsers turn receipts and bookings into a purchase-and-travel history automatically; nobody ever typed that history in.

### Visualization (canvas `c1`, 720×420)

Grouped horizontal bar chart: assumed vs realistic collection extent, two bars per row.

- **Title (bold 13px `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (row at y=30, swatches 14×10 starting at x=239 and x=335):** "assumed" — fill `rgba(26,82,118,0.35)`; "realistic extent" — fill `rgba(231,76,60,0.55)`. Legend text 11px `#2c3e50`.
- **Rows** (label, assumed %, actual %): Message contents 70/95; Attachment contents scanned 25/85; Receipts → purchase history 10/80; Calendar as movement log 20/85; Correspondence graph 30/90; Doc contents & edit telemetry 15/75; Read times, IPs, devices 20/85; "Deleted" mail still existing 15/70.
- **Layout:** right-aligned labels at x=225 (11px `#2c3e50`), bars start at x=239, max width 380px (scale 0–100), bar height 12px, assumed bar on top, actual bar 3px below, group spacing 42px, first group at y=52.
- **Caption (bottom center, 11px `#999`):** "Bar lengths are illustrative, not measured percentages."

## How it gets used

- **Provide the service:** deliver mail, sync calendars, edit and share documents.
- **Rank / recommend:** priority inbox, suggested replies, search ranking over your own archive.
- **Feature extraction:** receipts, flights, and package tracking become structured events and reminders.
- **Model training:** spam filters, autocomplete, and assistant features learn from content and behavior (scope varies by tier).
- **Ad targeting (consumer tiers, historically):** purchase and interest signals from mail content and activity.
- **Sharing:** affiliates; add-on developers granted mailbox scopes; workplace administrators, who can read everything in business tiers.

### Visualization (canvas `c2`, 720×340)

Two-column flow diagram: data-category boxes on the left linked by arrows to use boxes on the right.

- **Title (bold 13px `#1a5276`, top center):** "From data category to use".
- **Left boxes (x=40, width 185, height 32 centered on y = 55/110/165/220/275):** Mail contents `#1a5276`; Attachments & receipts `#e67e22`; Calendar events `#2980b9`; Contacts & send patterns `#8e44ad`; Doc edits & access logs `#e74c3c`. Style per box: stroke in its color (1.5px), fill same color at 12% alpha, bold 12px centered label in its color.
- **Right boxes (x=480, width 205, same y positions):** Deliver, sync, edit `#27ae60`; Priority inbox / smart reply `#1a5276`; Purchase / travel extraction `#e67e22`; Interest segments / ads `#e74c3c`; Admins & add-on developers `#8e44ad`.
- **Links (gray `#bbb` lines 1.2px with filled arrowheads), left → right:** Mail contents → Deliver, sync, edit; Mail contents → Priority inbox / smart reply; Attachments & receipts → Purchase / travel extraction; Attachments & receipts → Interest segments / ads; Calendar events → Purchase / travel extraction; Contacts & send patterns → Priority inbox / smart reply; Contacts & send patterns → Interest segments / ads; Doc edits & access logs → Admins & add-on developers.
- **Caption (bottom center, 11px `#999`):** "The spam-scanning pipeline and the feature-extraction pipeline read the same bytes."

## How long it's kept

- **Active mailbox:** forever, by design — permanence is the product.
- **"Deleted" mail:** ~30 days in trash, then weeks to months in backups.
- **The recipient's copy:** forever, entirely outside your control — deleting your side deletes half the conversation.
- **Server logs (IPs, access, sends):** months to years.
- **Document version history:** often the full edit history for the life of the document.
- **Legal hold / audit (business tiers):** indefinite, overriding user deletion.
- **Identifiable vs de-identified:** the longest retention usually applies to copies stripped of direct identifiers, not the originals — raw identifiable records get shorter windows, while de-identified or aggregated versions are kept far longer or indefinitely. The catch: stripping PII does not always prevent re-identification.

### Visualization (canvas `c3`, 720×330)

Horizontal retention-timeline bars per data category with an "account deleted" marker line.

- **Title (bold 13px `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Axis:** bars start at x0=220, max extent xMax=690; account-deleted marker at x=480.
- **Rows** (label, bar end x, color, note): "Deleted" mail in trash 300 `#2980b9` "~30d"; Server / access logs 410 `#2980b9` "months–years"; Mailbox & documents 480 `#e67e22` "life of account"; Backups after deletion 555 `#e67e22` "weeks–months tail"; Legal hold / audit copies 690 `#e74c3c` "indefinite" with right-pointing arrowhead; Recipients' copies of your mail 690 `#e74c3c` "forever, not yours" with right-pointing arrowhead.
- **Bar style:** height 16px, gap 22px, first at y=46; fill in row color at 45% alpha, 1px stroke in row color. Notes in 10px `#666` just right of each bar end (inside near left edge for full-length bars). Labels right-aligned at x=210, 11px `#2c3e50`.
- **Marker:** vertical dashed orange line (`#e67e22`, 2px, dash 5/4) at x=480 spanning the rows, labeled below in bold 11px `#e67e22` centered: "account deleted".
- **Caption (11px `#999`, bottom):** "Bars crossing the marker survive account deletion."

## What you get back

- **A typical export includes:** messages, contacts, calendar, documents and files, settings and filters.
- **Typically excluded:** the derived purchase and travel histories, correspondence-graph metrics, spam and sender-reputation scores, access and login logs, typing/editing telemetry, ad interest segments — and every copy of your words sitting in other people's mailboxes.

**Key point (callout box):** The asymmetry: the export is your filing cabinet. What the platform learned by reading the filing cabinet — who matters to you, what you spend, where you travel — is not in the box.

### Visualization (canvas `c4`, 720×320)

Two side-by-side comparison panels: export contents vs retained data.

- **Title (bold 13px `#1a5276`, top center):** "The export vs what exists".
- **Left panel (x=35, width 310, y=40, height 235, green `#27ae60` — 2px stroke, 8% alpha fill), bold 13px title "IN THE EXPORT",** items (12px `#2c3e50`, centered, 25px spacing): Messages & attachments / Contacts / Calendar events / Documents & files / Settings & filters.
- **Right panel (x=375, width 310, red `#e74c3c`), bold 13px title "EXISTS BUT NOT RETURNED",** items: Derived purchase history; Derived travel patterns; Correspondence-graph metrics; Spam / reputation scores; Access & login logs; Typing / editing telemetry; Copies in recipients' mailboxes.
- **Caption (bottom center, 11px `#999`):** "The filing cabinet comes back. What was learned from reading it does not."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout, right `<td>` (55%, `text-align: center`) holds the canvas. Cell borders `1px solid #e0e0e0`, padding 16px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with 6px bottom margin.
- **Callouts:** `.disclaimer` — background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Canvases are `display: block; margin: 0 auto`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`. No nav bar, no back/home links.
- Note: in regenerated HTML, any card/grid links referencing this page use the `.html` extension.
