# WhatsApp Chat Export

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload samples + canvas right 55%)
**HTML title tag:** WhatsApp Chat Export — Data Exports

**Subtitle:** In-app per-chat export — plain text .txt file with timestamps, sender names, and media references.

**Verified line:** Last verified: August 2026

## How to Export

- Open chat → More (⋮) → Export Chat
- Option: "Without Media" (text only) or "Include Media" (attaches photos/videos/audio as separate files)
- **Per-chat only** — no bulk "export all chats" option exists
- Exports as a .txt file (plus media folder if included)

## Format

- Plain text (.txt) with structured timestamps
- Format: `[M/D/YY, HH:MM:SS] Sender: Message`
- Media attachments referenced as `<Media omitted>` (without media) or filename (with media)
- System messages (calls, group changes) included inline

## What's Included

- All messages with timestamps and sender names
- Media filenames (IMG-20260822-WA0001.jpg, etc.)
- Call logs: missed, received, duration
- Group events: member added/removed, subject changes
- Deleted message placeholders ("This message was deleted")

**Key-point callout:** **Not a GDPR export.** This is a user-facing convenience feature, not a data subject access request. Meta offers a separate "Request Account Info" (Settings → Account → Request Account Info) which provides a different JSON-based export with account metadata, contacts, and settings.

## GDPR "Request Account Info" gives you:

- Account creation date, phone number, device info
- Profile photo, about text, settings
- Group membership list
- Blocked contacts
- JSON format, takes ~3 days to generate

## What's Missing (by design)

**Missing callout (red-bordered), titled "What's Missing (by design)":**

- Encryption keys — end-to-end encryption means neither export contains them
- Read receipts metadata (blue ticks timing)
- Online/last-seen status logs
- Typing indicator events
- Message delivery/read timestamps (only send time)
- Other participants' account info

## Right column: payload samples

**Payload note (italic):** Exported .txt format (actual file content):

**Payload block (monospace, verbatim):**

```
[8/22/26, 14:03:21] Alice: Hey, are you coming tonight?
[8/22/26, 14:04:05] Bob: Yeah, running 10 min late
[8/22/26, 14:04:12] Alice: <Media omitted>
[8/22/26, 14:05:30] Bob: Looks great! See you there
[8/22/26, 15:30:00] Missed voice call
[8/22/26, 18:45:12] Alice: Voice call (3 min 42 sec)
[8/22/26, 20:11:03] Bob: IMG-20260822-WA0014.jpg (file attached)
[8/23/26, 09:15:44] Alice: This message was deleted
[8/23/26, 09:16:02] Bob: 👍
[8/23/26, 11:00:00] Alice added Charlie
[8/23/26, 11:00:01] Alice changed the subject to "Weekend Plans"
```

**Payload note (italic):** Separate GDPR export ("Request Account Info") — JSON format:

**Payload block (monospace, verbatim):**

```
{
  "account_info": {
    "phone_number": "+1 (555) 012-3456",
    "creation_date": "2019-03-14T08:22:00Z",
    "platform": "Android",
    "app_version": "2.26.8.12"
  },
  "profile": {
    "display_name": "Alice",
    "about": "Available",
    "photo": "profile_photo.jpg"
  },
  "groups": ["Weekend Plans", "Work Team", "Family"]
}
```

### Visualization (canvas `msgChart`, 100% width × 340px)

Area line chart: daily message counts over ~6 months (~180 days), filled under the line, with annotated spikes.

- **Title (bold 13px, `#1a5276`, top center):** "Daily Message Count Over Chat Lifetime (~6 months)".
- **Data (daily message counts, in order):** `[45, 62, 38, 71, 55, 12, 8, 52, 67, 43, 78, 61, 15, 5, 48, 53, 72, 85, 44, 9, 3, 60, 55, 47, 63, 70, 18, 7, 112, 98, 65, 42, 50, 11, 6, 38, 45, 52, 68, 74, 22, 10, 0, 2, 5, 8, 35, 48, 62, 55, 71, 80, 14, 4, 41, 57, 63, 49, 75, 88, 20, 9, 130, 105, 72, 58, 45, 13, 6, 50, 62, 44, 38, 55, 16, 8, 42, 58, 65, 71, 53, 11, 3, 0, 0, 4, 12, 28, 45, 60, 52, 47, 68, 19, 7, 55, 63, 78, 82, 56, 41, 15, 5, 48, 52, 67, 73, 59, 12, 4, 140, 118, 95, 72, 60, 25, 8, 45, 53, 61, 70, 48, 14, 6, 38, 42, 55, 65, 51, 10, 2, 47, 58, 63, 75, 82, 18, 9, 52, 60, 44, 38, 56, 13, 5, 0, 3, 8, 22, 45, 58, 64, 72, 68, 50, 42, 11, 4, 55, 63, 78, 85, 62, 15, 7, 48, 52, 67, 90, 105, 88, 70, 55, 42, 18, 6]` (comment in source: active days 40-80, quiet days 5-15, spikes 100+, some zeros).
- **Axes:** y from 0 to 150, ticks at 0/30/60/90/120/150 with `#eee` gridlines and `#666` 10px labels; rotated y-axis label "Messages" (`#666`); x-axis line `#999` at bottom; x labels "Mar", "Apr", "May", "Jun", "Jul", "Aug" evenly spaced (`#666` 10px), with "2026" centered below them; margins top 40, bottom 50, left 50, right 20.
- **Series:** connected line `#1a5276` width 1.5; area under the line filled `rgba(26, 82, 118, 0.2)`.
- **Annotations (italic 9px, orange `#e67e22`, left-aligned just above the spike point):** "group event" at index 28 (~112 msgs); "planning weekend" at index 60 (~130 msgs); "birthday party" at index 109 (~140 msgs).

## Regeneration instructions

- **Layout:** single `.obj-table` (full width, collapsed borders) with one `<tr>`: left `<td>` (45%) holds `<strong>` section headings ("How to Export", "Format", "What's Included", "GDPR \"Request Account Info\" gives you:") with bullet lists, a `.key-point` callout, and a `.missing` callout; right `<td>` (55%, text-align center) holds two `.payload-note` + `.payload` blocks and the canvas.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.85em `#888`; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px. `.missing` — background `#fdf2f2`, left border `3px solid #e74c3c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace (ui-monospace/Menlo) 0.78em, white-space pre, left-aligned; `.payload-note` 0.82em `#666` italic left-aligned.
- **Canvas:** `height="340"` attribute, drawn at 100% container width; render via a `draw()` function using `getBoundingClientRect()` width, fixed 340px height, `window.devicePixelRatio` scaling (backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), re-run on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; area fill `rgba(26, 82, 118, 0.2)`. No nav bar, no back/home links; in regenerated HTML any card links use `.html` extensions.
