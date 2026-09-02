# Messaging App

**Page type:** detail page (obj-table layout: one row per section, text left 45%, canvas right 55% centered)
**HTML title tag:** Messaging App — Collect, Use, Keep, Return

**Subtitle:** Content may be end-to-end encrypted, but metadata is not — who, when, how often, from where.

**Disclaimer (callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** profile name, photo, status text, groups you create or join.
- **Not collected (when E2EE):** message content — end-to-end encryption genuinely blocks the operator from reading it in transit.
- **Incidental:** metadata around every message — who, when, how often, message sizes, from which IP and device; online/offline transitions, typing indicators, last-seen timestamps.
- **Incidental (graph):** your uploaded contact list — including people not on the app; group membership links you to everyone in the room.
- **Escapes encryption:** cloud backups are often *not* end-to-end encrypted by default; when a recipient reports a message, its plaintext is forwarded to the operator.
- **Inferred:** your closest contacts, activity rhythms, and social communities — computable from metadata alone.

> **Key point:** Most surprising: "end-to-end encrypted" covers only the message body. The pattern of your relationships — who, when, how often — is fully visible, and often more revealing than the words.

### Visualization (canvas `c1`, 720×460)

Grouped horizontal bar chart: assumed vs realistic collection extent per category.

- **Title (bold 13px `#1a5276`, centered):** "What people assume is collected vs realistic extent (illustrative)"
- **Legend:** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent" (11px `#2c3e50`).
- **Rows** (label, assumed a, realistic b; 0–100 scale, right-aligned 12px labels at x=215, bars start x=225, max width 410, bar height 13, inner gap 3, group gap 15, start y=52; gray `#999` 10px value labels at bar ends):
  - Message content (E2EE): 30 / 10
  - Metadata: who / when / how often: 15 / 90
  - Contact list (incl. non-users): 20 / 85
  - Group memberships: 10 / 80
  - Online / typing / last-seen: 10 / 75
  - Device & network info: 20 / 80
  - Cloud backups (not E2EE): 10 / 70
  - Reported-message plaintext: 5 / 50
  - Profile & status data: 40 / 70
- **Footer caption (gray `#999` 11px, centered):** "Numbers are illustrative. Note the first row: content is the one place reality is LOWER than assumed."

## How it gets used

- **Provide the service:** route messages, sync devices, show delivery and presence states.
- **Spam & abuse detection:** metadata patterns (burst sends, group joins) flag bad actors without reading content.
- **Contact suggestions:** the uploaded contact graph powers "people you may know" style features.
- **Ad targeting via affiliates:** when the operator runs other consumer properties, messaging metadata can inform ad profiles there.
- **Model training & analytics:** usage telemetry tunes features; reported content trains moderation models.

The business insight: the operator never needs your words. The graph plus timing is the product.

### Visualization (canvas `c2`, 720×330)

Flow diagram with a distinctive top lane: E2EE content bypasses the profiling hub (dashed arrow straight across), while metadata categories funnel into the hub and fan out to uses. Boxes have colored 1.5px stroke, 12%-alpha fill, bold 11px centered colored text; arrows are gray `#bbb` 1.5px with filled arrowheads.

- **Title (bold 13px `#1a5276`, centered):** "From data category to use — content bypasses the pipeline, metadata does not"
- **Top lane (E2EE bypass):** green `#27ae60` box (x=20, y=40, 175×46): "Message content" / "(E2EE — unreadable)"; **dashed** arrow straight across to green box (x=530, y=24, 175×42): "Delivered to recipient" / "operator cannot read".
- **Left boxes** (x=20, 175×46):
  - y=110, `#1a5276`: "Metadata" / "who · when · how often"
  - y=180, `#e67e22`: "Contact graph +" / "group membership"
  - y=250, `#2980b9`: "Presence telemetry" / "online · typing · last-seen"
- **Hub box** (x=280, y=160, 165×66, `#8e44ad`): "Relationship graph +" / "activity profile". All three left boxes arrow into the hub.
- **Right boxes** (x=530, 175×42, hub arrows out to each):
  - y=96, `#27ae60`: "Route & sync"
  - y=152, `#1a5276`: "Spam / abuse detection"
  - y=208, `#2980b9`: "Contact suggestions"
  - y=270, `#e74c3c`: "Affiliate ad profiles" / "& analytics"

## How long it's kept

- **Message content:** held on servers only until delivered; undelivered messages typically expire after ~30 days.
- **Metadata / logs:** kept far longer — months to years of who-talked-to-whom records.
- **Cloud backups:** live until overwritten or manually deleted — and may sit under a different provider's policy.
- **Reported content:** plaintext retained for moderation review and appeals.
- **After account deletion:** profile removed on a schedule; your number persists in other users' uploaded contact lists; "as required by law" holds are indefinite.
- **Identifiable vs de-identified:** the longest retention usually applies to copies stripped of direct identifiers, not the originals — records tied to your number get shorter windows, while de-identified or aggregated versions are kept far longer or indefinitely. The catch: stripping the number does not always prevent re-identification — the contact graph itself is a fingerprint.

### Visualization (canvas `c3`, 720×340)

Horizontal retention-timeline bars, one per data category, with a dashed "account deleted" vertical marker.

- **Title (bold 13px `#1a5276`, centered):** "How long each category lives (illustrative)"
- **Plot:** bars start at x=215, plot width 470, first row y=44, bar height 22, gap 16; bars filled at 45% alpha of their color; rows ending at 1.0 get a solid arrowhead (= indefinite); gray `#999` 10px note next to each bar; right-aligned 12px `#2c3e50` row labels.
- **Rows** (label, end fraction, color, note):
  - Message content (server), 0.08, `#27ae60`, "until delivered / ~30d"
  - Metadata / traffic logs, 0.68, `#2980b9`, "months to years"
  - Cloud backups, 0.78, `#e67e22`, "until overwritten"
  - Reported-message copies, 0.72, `#e74c3c`, "moderation review"
  - Your number in others' contacts, 1.0, `#8e44ad`, "not yours to delete"
  - "As required by law" holds, 1.0, `#e74c3c`, "indefinite"
- **Marker:** dashed (5/4) red `#e74c3c` 1.5px vertical line at 50% of plot width, bold 11px red label below: "account deleted". Gray 11px axis labels: "signup" (left), "years / indefinite →" (right).

## What you get back

- **In a typical export:** profile info, settings, contact list as uploaded, group names, sometimes coarse account activity.
- **Not returned:** the metadata graph (who/when/how often), presence and typing telemetry, inferred closest-contacts and community structure, spam/risk scores, your number's appearance in other people's uploaded contacts, reported-message copies.

> **Key point:** The asymmetry: messages live on your device, so the export looks small and harmless — while the relationship graph, the most analyzable asset, exists only server-side and is never returned.

### Visualization (canvas `c4`, 720×340)

Two side-by-side panels comparing export contents vs retained-but-not-returned data. Panels are 320px wide, y=34, height 280, 8%-alpha fill + 2px stroke of panel color, bold 13px colored title, 11px `#2c3e50` item lines at 24px spacing.

- **Title (bold 13px `#1a5276`, centered):** "The export vs what actually exists"
- **Left panel (x=30, green `#27ae60`) "IN THE EXPORT":** Profile name, photo, status / Settings & preferences / Contact list as uploaded / Group names / Coarse account activity
- **Right panel (x=380, red `#e74c3c`) "EXISTS BUT NOT RETURNED":** Who / when / how often graph / Presence & typing telemetry / Inferred closest contacts / Community / cluster structure / Spam & risk scores / Your number in others' uploads / Reported-message copies
- **Footer caption (gray `#999` 11px, centered):** "The export looks empty because your messages live on your phone — the graph lives on theirs."

## Regeneration instructions

- **Template/layout:** platform-privacy-policies detail page. h1, `.subtitle`, one `.disclaimer` callout, then a single `.obj-table` (full-width, border-collapse) with four `<tr>` rows — one per section (collected / used / kept / returned). Left `<td>` (45%) holds `.obj-title` + `<ul>` bullets + optional `.key-point` box and paragraph; right `<td>` (55%, text-align center) holds the canvas.
- **Page CSS:** body -apple-system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#2980b9` secondary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, gray `#999`/`#666`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper. The `c2` flow arrow for E2EE content is dashed (4/4) to signal the bypass.
- In regenerated HTML, any card links use `.html` extensions.
