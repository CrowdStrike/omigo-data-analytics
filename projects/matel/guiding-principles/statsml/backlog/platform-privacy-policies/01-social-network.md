# Social Network / Feed

**Page type:** detail page (obj-table layout: one row per section, text left 45%, canvas right 55% centered)
**HTML title tag:** Social Network / Feed — Collect, Use, Keep, Return

**Subtitle:** The social graph, dwell time per post, drafts you never published — and interests inferred from all three.

**Disclaimer (callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** profile fields, posts, photos, comments, friend/follow list, groups joined, events attended.
- **Incidental:** dwell time per feed item measured in milliseconds; scroll speed and re-reads; drafts and text deleted before posting, sometimes captured as interaction telemetry; device model, IP, precise timing of every session; location extracted from photo metadata and check-ins.
- **Incidental (about others):** contact lists uploaded from phones build shadow profiles of people who never signed up.
- **Off-platform:** browsing on other sites via embedded buttons, pixels, and SDKs — visits register whether or not you click.
- **Inferred:** interest segments, life events (moving, engagement, new job), political and religious affinity categories, face recognition templates — none of it ever typed by you.

> **Key point:** Most surprising: text you typed and deleted before posting can still leave a telemetry trace — the platform can know what you almost said.

### Visualization (canvas `c1`, 720×460)

Grouped horizontal bar chart: assumed vs realistic collection extent per category.

- **Title (bold 13px `#1a5276`, centered):** "What people assume is collected vs realistic extent (illustrative)"
- **Legend:** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent" (11px `#2c3e50`).
- **Rows** (label, assumed a, realistic b; 0–100 scale, right-aligned 12px labels at x=195, bars start x=205, max width 430, bar height 13, inner gap 3, group gap 15, start y=52; small gray `#999` 10px value labels at bar ends):
  - Posts & profile: 90 / 95
  - Likes & comments: 70 / 90
  - Dwell time per post (ms): 10 / 85
  - Deleted drafts: 5 / 45
  - Contacts of non-users: 5 / 70
  - Off-platform browsing: 15 / 80
  - Photo location metadata: 20 / 75
  - Face templates: 10 / 55
  - Inferred traits & affinities: 15 / 90
- **Footer caption (gray `#999` 11px, centered):** "Numbers are illustrative — they show the shape of the gap, not measured values."

## How it gets used

- **Provide the service:** render the feed, deliver notifications, sync across devices.
- **Rank the feed:** dwell time and scroll behavior weigh more than likes — attention is the real vote.
- **Ad targeting & measurement:** inferred affinity segments plus off-platform browsing decide which ads you see and prove which ones "worked".
- **Model training:** your reactions train the recommender that shapes everyone else's feed.
- **Sharing:** measurement partners, affiliated apps under the same parent, and advertisers receive aggregated or matched signals.

The feed you see is the output of a model whose main input is what you lingered on — not what you said you liked.

### Visualization (canvas `c2`, 720×330)

Flow diagram: data-category boxes on the left funnel into a central hub, which fans out to use boxes on the right. Boxes have colored 1.5px stroke, 12%-alpha fill of the same color, and bold 11px centered colored text; arrows are gray `#bbb` 1.5px lines with filled arrowheads.

- **Title (bold 13px `#1a5276`, centered):** "From data category to use"
- **Left boxes** (x=20, 175×46, one at each y with color and lines):
  - y=40, `#1a5276`: "Declared" / "posts · profile · graph"
  - y=110, `#2980b9`: "Telemetry" / "dwell · drafts · device"
  - y=180, `#e67e22`: "Contacts uploads" / "incl. non-users"
  - y=250, `#e74c3c`: "Off-platform" / "pixels · buttons · SDKs"
- **Hub box** (x=280, y=130, 165×66, `#8e44ad`): "Profile +" / "inferred segments". All left boxes arrow into the hub.
- **Right boxes** (x=530, 175×42, hub arrows out to each):
  - y=34, `#27ae60`: "Provide the service"
  - y=96, `#1a5276`: "Feed ranking"
  - y=158, `#e74c3c`: "Ad targeting +" / "measurement"
  - y=226, `#2980b9`: "Model training"
  - y=284, `#e67e22`: "Partners & affiliates"

## How long it's kept

- **Active account:** posts and profile kept as long as the account exists; interaction telemetry typically on rolling multi-month windows.
- **After deletion:** a grace period (often ~30 days to "change your mind"), then deletion from live systems over weeks to months.
- **Backup tail:** copies persist in backups for an additional window after live deletion.
- **Shadow-profile contact data:** came from other people's phones, not your account — deleting your account does not touch it.
- **De-identified aggregates and "as required by law":** effectively indefinite buckets — the longest retention applies to copies stripped of direct identifiers, not the originals. Raw identifiable records get the shorter windows; the catch is that stripping PII does not always prevent re-identification.

### Visualization (canvas `c3`, 720×340)

Horizontal retention-timeline bars, one per data category, with a dashed "account deleted" vertical marker.

- **Title (bold 13px `#1a5276`, centered):** "How long each category lives (illustrative)"
- **Plot:** bars start at x=215, plot width 470, first row y=44, bar height 22, gap 16; bars filled at 45% alpha of their color; rows ending at 1.0 get a solid arrowhead (= indefinite); gray `#999` 10px note next to each bar; right-aligned 12px `#2c3e50` row labels.
- **Rows** (label, end fraction, color, note):
  - Your posts & photos, 0.58, `#27ae60`, "delete + grace"
  - Interaction telemetry / logs, 0.68, `#2980b9`, "rolling windows"
  - Backup copies, 0.78, `#e67e22`, "backup tail"
  - Shadow-profile contact data, 1.0, `#e74c3c`, "not yours to delete"
  - De-identified aggregates, 1.0, `#8e44ad`, "indefinite"
  - "As required by law" holds, 1.0, `#e74c3c`, "indefinite"
- **Marker:** dashed (5/4) red `#e74c3c` 1.5px vertical line at 50% of plot width, bold 11px red label below: "account deleted". Gray 11px axis labels: "signup" (left), "years / indefinite →" (right).

## What you get back

- **In a typical export:** your posts, photos, comments, messages, friend list, settings, sometimes a coarse list of ad topics.
- **Not returned:** per-post dwell telemetry, deleted-draft captures, inferred affinity segments, face recognition templates, off-platform browsing logs, your entry in shadow profiles built from others' contacts, internal ranking and risk scores.

> **Key point:** The asymmetry: the export returns what you gave, not what was learned. The derived layer — the part that actually drives ranking and targeting — is treated as the platform's data, not yours.

### Visualization (canvas `c4`, 720×340)

Two side-by-side panels comparing export contents vs retained-but-not-returned data. Panels are 320px wide, y=34, height 280, 8%-alpha fill + 2px stroke of panel color, bold 13px colored title, 11px `#2c3e50` item lines at 24px spacing.

- **Title (bold 13px `#1a5276`, centered):** "The export vs what actually exists"
- **Left panel (x=30, green `#27ae60`) "IN THE EXPORT":** Posts, photos, comments / Messages / Friend / follow list / Profile & settings / Coarse ad-topic list (sometimes)
- **Right panel (x=380, red `#e74c3c`) "EXISTS BUT NOT RETURNED":** Dwell-time telemetry per post / Deleted-draft captures / Inferred affinity segments / Face recognition templates / Off-platform browsing logs / Shadow-profile data about you / Internal ranking / risk scores
- **Footer caption (gray `#999` 11px, centered):** "What you gave comes back. What was learned about you does not."

## Regeneration instructions

- **Template/layout:** platform-privacy-policies detail page. h1, `.subtitle`, one `.disclaimer` callout, then a single `.obj-table` (full-width, border-collapse) with four `<tr>` rows — one per section (collected / used / kept / returned). Left `<td>` (45%) holds `.obj-title` + `<ul>` bullets + optional `.key-point` box and paragraph; right `<td>` (55%, text-align center) holds the canvas.
- **Page CSS:** body -apple-system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#2980b9` secondary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, gray `#999`/`#666`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper.
- In regenerated HTML, any card links use `.html` extensions.
