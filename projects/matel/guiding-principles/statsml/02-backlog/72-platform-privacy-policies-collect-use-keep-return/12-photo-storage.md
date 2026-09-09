# Photo Storage

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Photo Storage

**Subtitle:** Not a shoebox of images — a searchable index of every face, place, object, and document you ever photographed.

**Disclaimer (orange callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** the photos and videos you upload, albums, captions, shares.
- **Incidental:** EXIF metadata inside each file — timestamp, GPS coordinates, device model — which assembles into a timestamped location trail; upload timing and network details; app telemetry; the shared-album participant graph (who shares with whom).
- **Inferred:** face-recognition templates that cluster every recurring person in your library — including friends, children, and strangers who never used the service and never consented; object and scene labels on every image; OCR text extracted from photographed documents, screenshots, and whiteboards; relationships from who appears with you most often; places lived and travel history from the location trail; "memories" and "on this day" features are the visible proof that the whole library is longitudinally indexed.

**Key point (blue-left-border box):** The non-consent surprise: face templates get built for people in your photos who have no account and never agreed to anything — their biometric geometry exists because you pressed upload.

### Visualization (canvas `c1`, 720×400)

Grouped horizontal bar chart: assumed vs realistic extent of collection, per data category.

- **Title (bold 13px `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (top, at x=200 and x=300):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent". Labels in `#2c3e50` 11px.
- **Rows (label, assumed a, realistic b — values on 0–100 scale):**
  - The photos themselves: a=90, b=92
  - EXIF location trail: a=25, b=88
  - Face templates (incl. non-users): a=15, b=90
  - Object / scene labels per image: a=12, b=85
  - OCR of documents / screenshots: a=5, b=78
  - Shared-album social graph: a=18, b=70
  - Inferred relationships + travel: a=5, b=72
- **Geometry:** right-aligned labels at x=225, bars start at x=235, max bar width 395px, bar height 13px, group gap 18px, start y=54. Assumed bar on top (`rgba(26,82,118,0.35)`), realistic bar below (`rgba(231,76,60,0.55)`). Numeric value printed just past each bar end: assumed value in `#999`, realistic value in `#e74c3c`.
- **Caption (bottom center, `#999` 11px):** "Numbers are illustrative relative extents, not measured statistics."

## How it gets used

- **Provide the service:** storage, sync, sharing, cross-device display.
- **Rank and recommend:** search by face, place, or object; auto-generated albums and "memories" surfaced by engagement models.
- **Ad targeting and measurement:** in some models, scene and interest signals (pets, travel, babies) feed the parent company's ad segments.
- **Model training:** vision models for faces, objects, and OCR improve on user libraries under broad license grants.
- **Sharing:** processors and affiliates; hash-matching against abuse databases; legal requests can reach the location trail.

### Visualization (canvas `c2`, 720×340)

Bipartite flow diagram: left column of data categories connected by gray bezier arrows to right column of uses.

- **Title (bold 13px `#1a5276`, top center):** "From data category to use".
- **Left boxes (200×36 at x=30, centered on y; box stroke in its color, fill same color at 0.12 alpha, bold 11px `#2c3e50` label):**
  - Photo + video pixels (y=50, `#1a5276`)
  - EXIF location + time (y=105, `#2980b9`)
  - Face / object / OCR index (y=160, `#8e44ad`)
  - Sharing graph (y=215, `#e67e22`)
  - App telemetry (y=270, `#e74c3c`)
- **Right boxes (200×36 at x=490, same styling):**
  - Store, sync, display (y=50, `#27ae60`)
  - Search + memories ranking (y=105, `#2980b9`)
  - Interest / ad signals (y=160, `#e74c3c`)
  - Vision model training (y=215, `#8e44ad`)
  - Abuse scan + legal access (y=270, `#e67e22`)
- **Arrows (bezier curves, `#bbb` 1px, small filled arrowhead at right end), [left index, right index] pairs:** [0,0],[0,3],[0,4],[1,1],[1,4],[2,1],[2,2],[2,3],[3,1],[3,2],[4,3],[1,2].
- **Caption (bottom center, `#999` 11px):** "The derived index — not the pixels — is what powers ranking, signals, and training."

## How long it's kept

- **Active account:** originals plus every derived index (faces, labels, OCR) for the life of the account.
- **Deleted photos:** sit in trash 30–60 days, still indexed and restorable.
- **Backup tail:** copies persist in server backups for weeks to months after the trash is emptied.
- **Derived indexes:** deleting a photo does not always delete the face cluster or labels built from it.
- **Abuse-scan records:** hash-match logs kept indefinitely, "as required by law".
- **Shared copies:** photos already shared to others' libraries are out of your control entirely.
- **Identifiable vs de-identified:** the longest retention usually applies to copies stripped of direct identifiers, not the originals — raw identifiable records get shorter windows, while de-identified or aggregated versions are kept far longer or indefinitely. The catch: stripping PII does not always prevent re-identification — faces and places in the pixels are identifiers in themselves.

### Visualization (canvas `c3`, 720×360)

Horizontal retention-timeline bar chart with a dashed "account deleted" marker line.

- **Title (bold 13px `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Geometry:** bars start at x0=210, timeline max x1=690; bar height 18px, gap 22px, start y=50. Bars filled at 0.45 alpha of their color plus a 1px solid stroke of the same color. Right-aligned row labels in `#2c3e50` 11px; note text in `#666` 10px to the right of each bar.
- **Rows (label, bar end x, color, note):**
  - App telemetry: end=360, `#27ae60`, "months, rolling"
  - Trash (deleted photos): end=465, `#2980b9`, "30–60 d past delete"
  - Originals + albums: end=500, `#e67e22`, "account life + backup tail"
  - Face / object / OCR index: end=540, `#e67e22`, "may outlive the photo"
  - Abuse-scan hash records: end=620, `#e74c3c`, "legal hold"
  - Copies shared to others: end=690, `#e74c3c`, "out of your control", with a filled arrowhead continuing past the bar end (runs off the timeline).
- **Marker:** vertical dashed red line (`#e74c3c`, dash 6/4, width 2) at x=430 spanning the rows, labeled below in bold red: "account deleted".
- **Caption (bottom center, `#999` 11px):** "time →   (bar lengths illustrative; \"delete\" starts a process, not an event)".

## What you get back

- **In a typical export:** your original files, album structure, captions, some metadata sidecar files.
- **Typically not returned:** face templates and cluster assignments; object and scene labels; the OCR text index; similarity vectors; the inferred relationship and travel profiles; other people's biometric data derived from your photos; abuse-scan records.

**Key point (blue-left-border box):** The asymmetry: the export gives back the pixels you uploaded. The index built on top of the pixels — the part that makes the library searchable and the profile valuable — is not in the archive.

### Visualization (canvas `c4`, 720×400)

Iceberg diagram: export above the waterline, derived index below.

- **Title (bold 13px `#1a5276`, top center):** "The export iceberg: what comes back vs what stays under".
- **Iceberg shape (centered at cx=360, waterline at y=130):** top polygon (above water) — vertices (cx−110, 130), (cx−60, 44), (cx+70, 52), (cx+120, 130) — green `#27ae60` fill at 0.15 alpha with 2px green stroke. Bottom polygon (below water) — vertices (cx−110, 130), (cx+120, 130), (cx+175, 250), (cx+90, 360), (cx−130, 345), (cx−170, 230) — red `#e74c3c` fill at 0.12 alpha with 2px red stroke.
- **Waterline:** horizontal dashed blue line (`#2980b9`, dash 8/5, width 2) from x=30 to x=690 at y=130, labeled above-left in 10px blue: "waterline = export boundary".
- **Above-water labels:** left-aligned at x=40 — bold green 12px "IN THE EXPORT", then items in `#2c3e50` 11px: "Original files", "Albums + captions", "Some metadata". Inside the top iceberg, bold green 11px centered two-line label: "your pixels" / "come back".
- **Below-water labels:** bold red 12px at x=40: "EXISTS BUT NOT RETURNED"; left column items (x=40, `#2c3e50` 11px): "Face templates + clusters", "Object / scene labels", "OCR text index", "Similarity vectors"; right column items (right-aligned at x=680): "Non-user biometric data", "Relationship inferences", "Travel / places-lived profile", "Abuse-scan records". Inside the lower iceberg, bold red 11px centered two-line label: "the index built on" / "your pixels stays".
- **Caption (bottom center, `#999` 11px):** "Proportions illustrative: the derived layer is the larger and more sensitive half."

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` box, right `<td>` (55%, `text-align: center`) holds the canvas. Table cell borders `1px solid #e0e0e0`, padding 16px. Above the table: h1, `.subtitle`, `.disclaimer`.
- **Page CSS:** body system sans-serif (-apple-system stack), `line-height 1.6`, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper; `canvas { display: block; margin: 0 auto; }`.
- Any links in regenerated HTML use `.html` extensions.
