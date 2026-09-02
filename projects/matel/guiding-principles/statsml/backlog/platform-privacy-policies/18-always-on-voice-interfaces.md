# Always-On Voice Interfaces

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Always-On Voice Interfaces

**Subtitle:** Assistants that ship enabled by default inside devices bought for another purpose — phones, earbuds, cars, TVs — an always-processing microphone now attached to advanced AI, in places nobody chose to put one.

**Disclaimer callout (orange left-border box):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** the voice commands you deliberately speak to the phone, earbuds, car, or TV remote.
- **Incidental:** a wake-word buffer running by default on every device with a mic — often re-enabled after software updates; in-car cabin audio tied to location, speed, and telematics; TV voice queries captured in the living room alongside whatever else is being said; passenger and bystander speech from people who never touched the device; contacts and messages synced to a car you paired once.
- **Inferred:** a voice print that recognizes you across devices; who is present in the car or room and when; stress and mood from tone; routines stitched from where and when each mic hears you; with modern language models behind the mic, fragments of ambient speech become structured understanding — topics, plans, relationships — not just matched keywords.

**Key-point callout:** Two things make the phone-native assistant a different beast. The mic is always processing in the background by default, and what hears you is no longer a keyword matcher — it is an advanced model that understands what it hears and sits at the OS level, able to join voice with mail, messages, photos, and location in a way no single app can. A smart speaker is a mic you chose to install; this category is intelligence that arrived enabled inside something else — and the passengers, guests, and children in range never agreed to any policy at all.

### Visualization (canvas `c1`, 720×420)

Grouped horizontal bar chart: assumed vs realistic extent of collection.

- **Title (bold 13px, `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (top, after label column):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent".
- **Rows (label, assumed, actual):** Commands you speak 85/95; Wake-word buffer on phone 20/80; Car cabin audio + location 15/78; TV voice queries + room audio 15/65; Passenger / bystander speech 5/60; Contacts synced to a car 10/75; Cross-device voice print 10/80; Presence & routine inference 8/70.
- **Layout:** right-aligned labels at x=225, bars start x=239, max bar width 380px scaled to 0–100, bar height 12px, assumed bar above actual bar (3px gap), row pitch 42px, starting y=52. No numeric value labels on the bars.
- **Bar colors:** assumed `rgba(26,82,118,0.35)`, actual `rgba(231,76,60,0.55)`.
- **Footnote (bottom center, `#999`, 11px):** "Bar lengths are illustrative, not measured percentages."

## How it gets used

- **Provide the service:** transcribe and execute the command on whichever device heard it.
- **Model training:** recordings and transcripts improve wake-word and speech models across the whole device fleet.
- **Personalize:** the cross-device voice print follows you from phone to car to TV, carrying preferences and history along.
- **Ad targeting & measurement:** command topics and viewing/driving context feed interest segments.
- **Ambient understanding:** assistant features increasingly summarize, anticipate, and act unprompted — which requires continuously interpreting context, not just answering commands.
- **Sharing:** in a car, the automaker, the assistant platform, and the telematics vendor can each receive a copy under separate policies; TV platforms share voice-query topics with content and ad partners.

One utterance in a car can land in three different companies' data stores, each with its own retention and its own policy you have never seen.

### Visualization (canvas `c2`, 720×340)

Bipartite flow diagram: five data-source boxes on the left connected to five destination boxes on the right.

- **Title (bold 13px, `#1a5276`, top center):** "One utterance, many destinations".
- **Left boxes** (185×32px at x=40; 12% alpha fill, 1.5px colored stroke, bold 12px label in box color): "Phone / earbud audio" `#1a5276` (y 55), "Car cabin audio" `#e67e22` (y 110), "TV voice queries" `#8e44ad` (y 165), "Synced contacts / messages" `#2980b9` (y 220), "Voice print" `#e74c3c` (y 275).
- **Right boxes** (210×32px at x=475): "Answer the command" `#27ae60` (y 55), "Assistant platform models" `#1a5276` (y 110), "Automaker / telematics vendor" `#e67e22` (y 165), "Interest segments / ads" `#e74c3c` (y 220), "Cross-device personalization" `#2980b9` (y 275).
- **Links (left index → right index):** 0→0, 0→1, 1→1, 1→2, 2→3, 3→2, 4→4 — gray `#bbb` 1.2px lines with rotated solid arrowheads at the right ends.
- **Footnote (bottom center, `#999`, 11px):** "In a car, the assistant platform, automaker, and telematics vendor each hold a copy under separate policies."

## How long it's kept

- **Voice recordings & transcripts:** account-lifetime by default on the assistant side, per device.
- **Car infotainment:** synced contacts, call logs, and voice data stay in the vehicle until manually wiped — surviving your rental return or the car's resale.
- **Telematics / event data:** retained by the automaker for years; crash-related data effectively indefinite.
- **TV voice queries:** months to years of query logs tied to the device and household profile.
- **Bystander speech:** stored under the device owner's account — the person recorded has no account to delete it from.
- **Aggregates & trained models:** the value persists indefinitely after any deletion.
- **Identifiable vs de-identified:** the longest retention usually applies to copies stripped of direct identifiers, not the originals — raw identifiable audio gets shorter windows, while de-identified transcripts and aggregates are kept far longer. The catch: stripping PII does not always prevent re-identification, and a voice recording is itself biometric.

### Visualization (canvas `c3`, 720×330)

Horizontal retention timeline bars with an "account deleted" marker.

- **Title (bold 13px, `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Rows (label, bar end in px from x0=220 toward xMax=690, color, note):** "TV voice-query logs" end 380 `#2980b9` "months–years"; "Voice recordings / transcripts" end 480 `#e67e22` "account lifetime"; "In-vehicle synced data" end 540 `#e67e22` "until manually wiped"; "Bystander speech (owner's account)" end 560 `#e74c3c` "no owner = no delete"; "Telematics / event data" end 620 `#e74c3c` "years"; "Aggregates / trained models" end 690 `#e74c3c` "indefinite" with right-pointing arrowhead.
- **Layout:** right-aligned 11px labels ending at x=210, bar height 16px, gap 22px, starting y=46; bars filled at 45% alpha with 1px solid outline; notes in 10px `#666` after each bar end.
- **Account-deleted marker:** vertical dashed orange line (`#e67e22`, dash 5/4, width 2) at x=480 spanning all rows, with bold 11px centered label "account deleted" below.
- **Footnote (bottom center, `#999`, 11px):** "Bars crossing the marker survive account deletion — the car's copy never saw the account at all."

## What you get back

- **A typical export includes:** your own voice recordings and transcripts from your own account, device settings, paired-device list.
- **Typically excluded:** the cross-device voice print, presence and routine inferences, in-vehicle data held by the automaker and telematics vendor, TV-side household profiles, anything recorded while you were a passenger or guest on someone else's device.

**Key-point callout:** The asymmetry is worst for bystanders: a passenger recorded by a car they don't own, or a guest near a TV remote, has no account, no export path, and no deletion button — their voice lives under someone else's login. And the vehicle's copy answers to the automaker's policy, not the assistant's.

### Visualization (canvas `c4`, 720×320)

Two side-by-side panels comparing the export contents vs what exists.

- **Title (bold 13px, `#1a5276`, top center):** "The export vs what exists".
- **Left panel** (310×235px at x=35 y=40, `#27ae60` 2px stroke, 8% alpha fill): heading bold 13px "IN THE EXPORT"; centered 12px `#2c3e50` items (25px spacing): "Your voice recordings", "Your transcripts", "Device settings", "Paired-device list".
- **Right panel** (310×235px at x=375 y=40, `#e74c3c` 2px stroke, 8% alpha fill): heading bold 13px "EXISTS BUT NOT RETURNED"; centered items: "Cross-device voice print", "Presence / routine inferences", "Automaker & telematics copies", "TV household profile", "Bystander & passenger audio", "Interest / ad segments", "Model training contributions".
- **Footnote (bottom center, `#999`, 11px):** "Your account's export covers your account. The car, the TV, and other people's devices answer to nobody's export."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout or paragraph, right `<td>` (55%, `text-align: center`) holds one canvas. Cell borders `1px solid #e0e0e0`, padding 16px. Page order: h1, `.subtitle`, `.disclaimer`, table.
- **Page CSS:** body system sans-serif stack, line-height 1.6, color `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, text `#7d5a29`, 0.9em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em; canvas `display: block; margin: 0 auto`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fills `rgba(26,82,118,0.35)` and `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
