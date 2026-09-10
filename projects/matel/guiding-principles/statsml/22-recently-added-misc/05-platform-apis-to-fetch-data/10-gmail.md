# Gmail API

**Page type:** detail page (two-column obj-table: text left 45%, code sample + canvas right 55%, one row, bordered cells)
**HTML title tag:** Gmail API — Platform APIs

**Subtitle:** Read, search, organize, and send email in a Gmail mailbox on the account owner's behalf.

**Verified badge:** Last verified: August 2026

## Overview (untitled — this page's obj-table has no h2 above it)

### What you can get

- Every email in a mailbox — messages, conversation threads, drafts, and attachments
- Labels (Gmail's folders and tags) for filtering and organizing
- Near-real-time alerts when the mailbox changes, plus an incremental "what changed since" feed
- The ability to send mail as the user

**Key-point callout:** **Access comes in tiers — ask for the narrowest one that works.** There are separate permission levels for read-only, headers-only (no message body), send-only, and full access. Apps requesting broad access to mail content must pass a formal security review by Google before going live, which is often the slowest part of the whole project.

### Watch out for

- Usage is metered in "quota units" — sending an email costs about 20x more than reading one
- The API accepts messages up to about 35 MB; the familiar 25 MB cap is a Gmail app limit, not an API one
- The "what changed since" bookmark expires — an app that is offline too long must re-scan the mailbox from scratch
- Every user must individually consent (or a Workspace admin must grant domain-wide access)

### Code sample (right column)

Heading: **Example: one message, as the API returns it**

```
{
  "id": "18a3f5c7d8e9b012",
  "threadId": "18a3f5c7d8e9b012",
  "labelIds": ["INBOX", "UNREAD"],
  "snippet": "Hi team, please review the Q3 report...",
  "payload": {
    "headers": [
      { "name": "From", "value": "sender@company.com" },
      { "name": "Subject", "value": "Q3 Report Review" }
    ],
    "parts": [
      { "mimeType": "text/html", "body": { "size": 4521 } },
      { "mimeType": "application/pdf", "filename": "Q3_Report.pdf",
        "body": { "attachmentId": "ANGjdJ...", "size": 2458624 } }
    ]
  }
}
```

### Visualization (canvas `pieChart`, responsive width × 400)

Pie chart of typical email payload composition by size, with leader-line labels and a bottom legend.

- **Title (bold 13px `#1a5276`, top center):** "Typical Email Payload Size Distribution"
- **Slices (label, percent, color):**
  - Headers — 2% — `#1a5276`
  - Text body (plain) — 5% — `#2980b9`
  - HTML body — 15% — `#27ae60`
  - Inline images — 28% — `#e67e22`
  - Attachments — 45% — `#e74c3c`
  - Metadata/envelope — 5% — `#8e44ad`
- **Geometry:** pie centered horizontally at half canvas width, cy=190, radius 120, first slice starts at 12 o'clock (-90°), slices drawn clockwise with 2px white borders between them.
- **Labels:** each slice has a `#999` leader line from radius+5 out to radius+30 then a 20px horizontal extension; label text in 11px `#2c3e50` reading "Label (N%)", left- or right-aligned by side, e.g. "Attachments (45%)".
- **Legend (bottom, y = height−30):** six evenly spaced items across the width, each a 12×12 color swatch plus the slice label in 10px `#2c3e50`.

## Official API References

- [Gmail API Documentation](https://developers.google.com/gmail/api) — API home with guides and concepts
- [Gmail API REST Reference](https://developers.google.com/gmail/api/reference/rest) — users.messages, threads, labels, drafts, history

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, `.verified` badge, then the one-row `.obj-table` directly (no h2 before it): left `<td>` 45% with `.section-header` paragraph headings, bullet lists, and a `.key-point` callout; right `<td>` 55% with a `.section-header`, a `<pre class="payload">` code sample, and the canvas. Then `h2` "Official API References" with a link list. Unlike the sibling pages, this page uses a global `* { margin:0; padding:0; box-sizing:border-box; }` reset and bordered table cells.
- **Page style:** body system sans-serif, `#2c3e50` text, white background, padding 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` inline badge — background `#f0f8ff`, border 1px `#2980b9`, color `#1a5276`, 0.8em, radius 4px, padding 2px 8px; `.obj-table td` bordered `1px solid #e0e0e0`, padding 16px; `.section-header` bold `#1a5276` 0.95em; `pre.payload` background `#f8f9fa`, left border 3px `#1a5276`, monospace 0.78em, padding 16px, radius 4px; `.key-point` background `#f8f9fa`, left border 3px `#e74c3c`, padding 10px 14px, 0.93em; li 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="pieChart" height="400">`, CSS `width: 100%`; redraws on window resize using `getBoundingClientRect()` width; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, grays `#666`/`#999`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
