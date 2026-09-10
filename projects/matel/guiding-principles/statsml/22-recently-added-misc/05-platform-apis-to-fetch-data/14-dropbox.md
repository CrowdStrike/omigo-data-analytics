# Dropbox

**Page type:** detail page (platform-API layout: h1 + subtitle + "Last verified" badge, one two-column obj-table row — text left 45%, example JSON + canvas right 55% — then an "Official API References" link list)
**HTML title tag:** Dropbox API — Platform APIs

**Subtitle:** List, download, and track changes to the files in a Dropbox account or Business team.

**Verified badge:** Last verified: August 2026

## Section: What you can get (left column)

- File and folder listings, with a bookmark to fetch only what changed since last time
- File content — uploads, downloads, and older versions of the same file
- Shared links and their settings (expiry, who can open them)
- File requests: upload forms that let outsiders send files in
- For Business teams: the member directory and a full audit log of who did what

### Key-point callout

**History outlives the visible file.** Old versions of a file stay downloadable after edits, and the team audit log keeps the file's name, path, and who touched it even after a user "deletes" it. How long depends on plan — about 30 days on Plus, 180 days on Business, and up to 10 years with the Extended Version History add-on — so an access review that only looks at current files misses most of the picture.

## Section: Watch out for (left column)

- "Deleted" rarely means gone — check version history and the audit log, not just the current listing
- A file's path changes when it is moved or renamed; track the permanent file ID instead
- The change notification only says "something changed" — you must ask again to find out what
- Audit and team features require a Business plan; individual accounts cannot get them at any price

## Section: Example: version history of one file (right column)

Code block (`pre`, JSON):

```json
{
  "entries": [
    { ".tag": "file", "name": "Vendor Contract.pdf",
      "id": "id:a4ayc_80_OEAAAAAAAAAXw",
      "rev": "a1c10ce0dd78", "size": 892143,
      "server_modified": "2026-08-19T11:02:51Z" },
    { ".tag": "file", "name": "Vendor Contract.pdf",
      "id": "id:a4ayc_80_OEAAAAAAAAAXw",
      "rev": "9f2b71a4c003", "size": 874902,
      "server_modified": "2026-07-30T09:14:07Z" }
  ]
}
```

Caption (gray, 0.88em): Each older revision remains downloadable for the whole retention window, even if the current file is overwritten or deleted.

**Section head above canvas:** Retention Windows — What Survives a Delete

### Visualization (canvas `retentionChart`, responsive width × 360)

Horizontal bar chart of documented retention windows in days; open-ended windows drawn as dashed bars with arrowheads.

- **Title (bold 13px `#1a5276`, top left):** "Documented retention windows (days)"
- **Rows (label / days / color / value note; `open: true` = open-ended dashed bar):**
  - "Version history — Basic / Plus" — 30 — `#e74c3c` — "30 days"
  - "Deleted file recovery — Basic / Plus" — 30 — `#e74c3c` — "30 days"
  - "Version history — Professional / Business" — 180 — `#1a5276` — "180 days"
  - "Deleted file recovery — Prof. / Business" — 180 — `#1a5276` — "180 days"
  - "Team event log (team_log)" — 200 (open-ended) — `#e67e22` — "plan-tiered, longer"
  - "list_folder cursor validity" — 200 (open-ended) — `#8e44ad` — "no fixed expiry"
- **Scale/axis:** x from 0 to 200 days; vertical gridlines every 30 days from 0 to 180 (`#eee`, the 0 line `#999`), tick labels 10px `#888` below the plot; left label pad 250px, top pad 44px, bottom pad 76px.
- **Bars:** closed bars — fill `rgba(26,82,118,0.35)` with a 1.5px stroke in the row color; open-ended bars — dashed 4/3 stroke in the row color, translucent fill (row color + `33` alpha), and a solid filled arrowhead triangle in the row color at the right end. Bar height min(24, 55% of band).
- **Labels:** row label right-aligned 11px `#2c3e50` left of the plot; value note bold 10px in the row color to the right of each bar.
- **Green markers:** a 3.5px-radius `#27ae60` dot at the midpoint of every bar.
- **Footer annotations (left-aligned, bottom):** bold 10px `#27ae60`: "Green dot = anywhere inside the bar, the bytes are still retrievable by rev."; italic 10px `#888` on two lines: "Dashed / arrowed bars are open-ended: the exact window is set by plan tier and admin policy, not by the API." / "A file removed from the current listing is still reachable by rev, and still named in the audit log."
- Redraws on window resize; backing store scaled by devicePixelRatio via `ctx.setTransform(dpr, 0, 0, dpr, 0, 0)`; fixed 360px CSS height.

## Section: Official API References

- [Dropbox Developers](https://www.dropbox.com/developers) — developer home for apps, docs, and the App Console
- [HTTP API v2 Reference](https://www.dropbox.com/developers/documentation/http/documentation) — files, sharing, file_requests, and users endpoints

## Regeneration instructions

- **Layout:** platform-APIs detail page. Body: h1, `.subtitle` paragraph, `.verified` badge span, one `table.obj-table` with a single `<tr>`; left `<td>` (45%) holds `.section-head` headings ("What you can get", "Watch out for") with `<ul>` lists and one `.key-point` callout between them; right `<td>` (55%) holds a `.section-head` example heading, a `<pre>` JSON block with an inline-styled gray caption `<p>`, a second `.section-head` ("Retention Windows — What Survives a Delete"), and the canvas. After the table: `<h2>Official API References</h2>` with a `<ul>` of external links. Note this page uses `<p class="section-head">` (0.95em bold `#1a5276`) rather than `.section-title`, plus a universal `* { margin:0; padding:0; box-sizing:border-box; }` reset.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #e0e0e0`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `table.obj-table` full width, collapsed borders, td padding 16px, first td 45% / last td 55%. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.8em, monospace (`ui-monospace, Menlo`). `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `p` and `li` 0.93em. Links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="retentionChart" height="360">`, CSS `display:block; width:100%`; script sizes backing store to cell width × 360 times `window.devicePixelRatio` and redraws on resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, grays `#888`/`#999`/`#eee`.
- In regenerated HTML, any card/grid links pointing to this page use the `.html` extension.
