# Outlook / Exchange Online

**Page type:** detail page (platform-API layout: h1 + subtitle + "Last verified" badge, one two-column obj-table row — text left 45%, example JSON + canvas right 55% — then an "Official API References" link list)
**HTML title tag:** Outlook / Exchange Online — Platform APIs

**Subtitle:** Read and manage Outlook mailboxes — messages, folders, and inbox rules — through Microsoft Graph.

**Verified badge:** Last verified: August 2026

## Section: What you can get (left column)

- Every message in a mailbox, with sender, recipients, body, and attachments
- The folder tree, with item and unread counts per folder
- An incremental "only what changed" sync so you don't re-download the mailbox
- Each user's inbox rules (auto-forward, auto-move, auto-delete)
- Mailbox settings such as time zone and working hours

### Key-point callout

**App-level permission means every mailbox in the company.** An app granted organization-wide read access can read anyone's mail — including the CEO's — with no per-user consent. Narrowing that requires a separate Exchange-side policy that is invisible from the app's own settings, so teams routinely believe their blast radius is smaller than it is. Verify, don't assume.

## Section: Watch out for (left column)

- Sync bookmarks expire; recovery is a full re-download of the folder, so size your system for that day, not the average day
- A message's ID changes when it moves between folders unless you opt into immutable IDs — stored IDs can silently go stale
- Rate limits are per mailbox and cap parallel requests, so scale by working across many mailboxes, never by hammering one
- The metadata-only permission returns empty message bodies instead of an error, so a text pipeline can quietly produce nothing

## Section: Example: a suspicious inbox rule (right column)

Code block (`pre`, JSON):

```json
{
  "displayName": "",
  "sequence": 2,
  "isEnabled": true,
  "conditions": { "sentToMe": true },
  "actions": {
    "forwardTo": [
      { "emailAddress": { "address": "collector@external.example" } }
    ],
    "markAsRead": true,
    "delete": true
  }
}
```

Caption (italic, gray, 0.85em): Blank name, forward everything externally, mark read, delete — the classic mailbox-compromise signature. It is only visible by checking each mailbox's rules.

### Visualization (canvas `deltaCostChart`, responsive width × 380)

Horizontal bar chart: indicative request cost per sync cycle for one mailbox folder, on an arbitrary common scale (the ratios are the point).

- **Title (bold 14px `#1a5276`, top center):** "Requests consumed per sync cycle, one mailbox folder"
- **Subtitle (italic 10px `#666`):** "indicative ratios on an arbitrary scale — throttling is per mailbox, so this is the budget that matters"
- **Rows (label / value / accent color / note under label):**
  - "Initial full sync" — 100 — `#e74c3c` — "every page of every message"
  - "Token expired — resync" — 100 — `#e74c3c` — "identical cost, unplanned"
  - "Re-poll without delta" — 100 — `#e67e22` — "$filter on receivedDateTime still pages the folder"
  - "Delta + refetch changed" — 22 — `#e67e22` — "delta page, then GET per changed id"
  - "Delta only (projection)" — 9 — `#27ae60` — "one page, reduced properties"
  - "Change notification" — 3 — `#27ae60` — "push; still needs delta to reconcile gaps"
- **Scale:** max 100; vertical gridlines (`#eee`) at 0, 25, 50, 75, 100 with gray `#888` 10px tick labels; the 100 tick is labeled "100 (full)".
- **Bars:** 22px tall, 24px gap, starting y=62; fill `rgba(26,82,118,0.35)` with a 4px-wide colored accent strip at the left edge in the row's color; bold 11px value label like "100x" in the row color to the right of the bar end.
- **Labels:** row label right-aligned bold 11px `#2c3e50` in a label column (min(160, 32% of width)); note below in 9.5px `#888`, truncated with "..." if longer than 46 chars.
- **Footer annotations (centered):** italic 10.5px `#e74c3c`: "An expired delta token collapses the bottom three rows into the top one. Plan capacity for that day."; italic 10px `#666`: "Ceiling to respect: 10,000 requests / 10 min / mailbox, and 4 concurrent requests / mailbox."
- Redraws on window resize; canvas width fills its cell (`getBoundingClientRect`), fixed 380px CSS height.

## Section: Official API References

- [Outlook Mail API Overview](https://learn.microsoft.com/en-us/graph/outlook-mail-concept-overview) — Microsoft Graph mail surface concepts
- [message Resource Type](https://learn.microsoft.com/en-us/graph/api/resources/message) — message object reference with all properties

## Regeneration instructions

- **Layout:** platform-APIs detail page. Body: h1, `.subtitle` paragraph, `.verified` badge span, one `table.obj-table` with a single `<tr>`; left `<td>` (45%) holds `.section-title` headings ("What you can get", "Watch out for") with `<ul>` lists and one `.key-point` callout between them; right `<td>` (55%) holds a `.section-title` example heading, a `<pre>` JSON block with an inline-styled italic gray caption `<p>`, and the canvas. After the table: `<h2>Official API References</h2>` with a `<ul>` of external links.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `table.obj-table` full width, collapsed borders, td padding 16px, first td 45% / last td 55%. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.section-title` bold `#1a5276` 1.05em. `li` 0.93em. Links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="deltaCostChart" height="380">`, CSS `display:block; width:100%`; script sizes backing store to `getBoundingClientRect().width × 380` times `window.devicePixelRatio`, and calls `ctx.scale` so drawing stays in logical coordinates, and redraws on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar fill `rgba(26,82,118,0.35)`, grays `#666`/`#888`/`#eee`.
- In regenerated HTML, any card/grid links pointing to this page use the `.html` extension.
