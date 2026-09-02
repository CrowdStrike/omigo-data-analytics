# Google Drive API

**Page type:** detail page (platform-API layout: h1 + subtitle + "Last verified" badge, one two-column obj-table row — text left 45%, example JSON + canvas right 55% — then an "Official API References" link list)
**HTML title tag:** Google Drive API — Platform APIs

**Subtitle:** Read, upload, search, and share the files stored in a Google Drive.

**Verified badge:** Last verified: August 2026

## Section: What you can get (left column)

- Files and folders with names, owners, sharing permissions, and version history
- File downloads, and exports of Google Docs/Sheets/Slides into PDF or Office formats
- Search by name, type, owner — and by words inside the file content
- A "what changed" feed plus push notifications when files change

### Key-point callout

**Google's own file types have no file to download.** A Google Doc, Sheet, or Slide deck is not stored as a normal file — you must export it to a standard format like PDF or DOCX, and exports have their own size limits. Also, anything in a shared drive belongs to the organization, not to the person who created it.

## Section: Watch out for (left column)

- Trashed files are permanently deleted after 30 days
- Version history of uploaded (non-Google) files is pruned after 30 days or 100 versions, unless a version is explicitly flagged to keep forever
- Push-notification channels expire quickly (often about an hour) and must be renewed, or updates silently stop
- Uploads are capped at 750 GB per user per day

## Section: Example: what the API says about one Google Doc (right column)

Code block (`pre.payload`, JSON):

```json
{
  "kind": "drive#file",
  "name": "Q3 Planning Document",
  "mimeType": "application/vnd.google-apps.document",
  "modifiedTime": "2026-08-20T14:32:15.000Z",
  "owners": [{ "displayName": "Jane Smith", "emailAddress": "jane@company.com" }],
  "permissions": [
    { "role": "owner", "type": "user", "emailAddress": "jane@company.com" },
    { "role": "writer", "type": "domain", "domain": "company.com" }
  ],
  "size": null,
  "webViewLink": "https://docs.google.com/document/d/.../edit"
}
```

Caption (gray, 0.88em): `"size": null` — a Google Doc has no binary; only an export does.

**Section head above canvas:** Google Drive File Hierarchy

### Visualization (canvas `treeChart`, responsive width × 420)

Tree diagram of a Drive file hierarchy: rounded-rect nodes connected by elbow lines, with per-file-type icons and a legend.

- **Title (bold 13px `#1a5276`, top left, inside canvas):** "Google Drive File Hierarchy"
- **Node colors:** folders/root `#1a5276`; Google Docs `#4285f4`; Sheets `#0f9d58`; Slides `#f4b400`; other files `#666`; shared drive `#8e44ad`.
- **Tree structure (indented 50px per level, ~38px row gap, starting near top-left):**
  - "My Drive" — root node: solid `#1a5276` filled rounded rect (4px radius, 24px tall), white bold 12px label
    - "Projects" — folder: outlined rounded rect in `#1a5276` with a small folder icon
      - "Q3 Planning.gdoc" — file node with a Docs-blue square icon lettered "D"
      - "Budget.gsheet" — file node with a Sheets-green square icon lettered "S"
      - "Presentation.gslides" — file node with a Slides-yellow square icon lettered "P"
      - "assets/" — folder
        - "logo.png" — file node with a generic gray file icon
        - "report.pdf" — file node with a generic gray file icon
    - "Shared Drive" — shared-drive node: dashed (3/2) `#8e44ad` outlined rounded rect with purple folder icon
      - "Team Wiki.gdoc" — Docs file node
    - "Archive" — folder
    - "Personal" — folder
- **Node style:** non-root nodes are white/`#f8f9fa` rounded rects with 1.5px colored outline (file nodes: `#f8f9fa` fill, `#ddd` border), 11px `#2c3e50` labels, 10px icon left of the text.
- **Connectors:** 1px `#bbb` elbow lines from below-left of each parent (x offset +20) down and across to the child's left edge.
- **Legend (bottom right):** 10×10 color swatches with 10px `#2c3e50` labels: Docs (`#4285f4`), Sheets (`#0f9d58`), Slides (`#f4b400`), Other (`#666`).
- **Notes:** italic 10px `#888` bottom-left: "quotaBytesUsed: 0 for Workspace files (stored differently)"; italic 10px `#8e44ad` near the legend: "Dashed border = Shared Drive (org-owned)".
- Redraws on window resize; backing store scaled by devicePixelRatio (`ctx.scale` back to logical coordinates); canvas CSS height fixed at 420px via stylesheet.

## Section: Official API References

- [Google Drive API Documentation](https://developers.google.com/drive/api) — API home with guides and concepts
- [Drive API v3 REST Reference](https://developers.google.com/drive/api/reference/rest/v3) — files, permissions, revisions, changes, drives resources

## Regeneration instructions

- **Layout:** platform-APIs detail page. Body: h1, `.subtitle` paragraph, `.verified` badge span, one `table.obj-table` with a single `<tr>`; left `<td>` (45%) holds `.section-head` headings ("What you can get", "Watch out for") with `<ul>` lists and one `.key-point` callout between them; right `<td>` (55%) holds a `.section-head` example heading, a `<pre class="payload">` JSON block with an inline-styled gray caption `<p>` (contains a `<code>` span), a second `.section-head` ("Google Drive File Hierarchy"), and the canvas. After the table: `<h2>Official API References</h2>` with a `<ul>` of external links. Uses `<p class="section-head">` (0.95em bold `#1a5276`) and a universal `* { margin:0; padding:0; box-sizing:border-box; }` reset.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 40px, white background. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#f0f8ff`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 8px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `table.obj-table` full width, collapsed borders, td padding 16px with `border: 1px solid #e0e0e0`, first td 45% / last td 55%. `pre.payload` background `#f8f9fa`, `border-left: 3px solid #1a5276`, monospace (`ui-monospace, Menlo`), 0.78em, padding 16px, radius 4px. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.92em. `li` 0.92em. Links `#1a5276`. Canvas CSS: `display:block; width:100%; height:420px`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="treeChart">` (no intrinsic attributes; size comes from CSS); script sizes backing store to `getBoundingClientRect()` width/height times `window.devicePixelRatio`, `ctx.scale` back to logical coordinates, and redraws on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus Google brand colors `#4285f4` (Docs), `#0f9d58` (Sheets), `#f4b400` (Slides), `#8e44ad` (shared drive), grays `#666`/`#888`/`#bbb`/`#ddd`.
- In regenerated HTML, any card/grid links pointing to this page use the `.html` extension.
