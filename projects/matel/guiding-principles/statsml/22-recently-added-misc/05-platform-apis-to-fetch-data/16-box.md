# Box

**Page type:** detail page (two-column obj-table layout: text left 45%, code sample + canvas right 55%, single row)
**HTML title tag:** Box API — Platform APIs

**Subtitle:** Manage files and folders in Box, along with the governance extras — custom metadata, retention rules, legal holds, and an audit trail.

**Verified badge:** Last verified: August 2026

## What you can get

- Files, folders, and their full version history
- Custom metadata fields attached to files (contract value, counterparty, ...) — and search by those values
- Retention policies and legal holds that override user deletion
- An enterprise audit log of every action: who did what, when, from where
- Watermarks stamped onto previews and downloads

**Key-point callout:** **"Deleted" is not gone — by design.** A file under a retention policy or legal hold cannot be permanently deleted by its owner: the user sees it vanish, while the content stays preserved and discoverable, and the audit log keeps the record of what happened. Box is built for regulated industries where that is the point, not a bug.

## Watch out for

- A new server-to-server app sees an empty root folder — it is its own separate user until content is shared with it or it acts on a real user's behalf
- Many fields are left out of responses unless explicitly requested, so a "missing" value may just be un-asked-for
- Deleting a metadata template deletes its values from every file that used it, with no undo
- Watermarks are applied at view time only — anyone allowed to download still gets a clean copy

## Example: a file with custom metadata attached

Code block (`pre`, JSON):

```
{
  "type": "file",
  "id": "12345678901",
  "name": "MSA - Northwind 2026.pdf",
  "metadata": {
    "enterprise_54321": {
      "contractRecord": {
        "counterparty": "Northwind Traders",
        "contractValue": 480000,
        "effectiveDate": "2026-03-01T00:00:00Z",
        "renewalType": "auto"
      }
    }
  }
}
```

Caption below the code (0.88em, gray `#666`): These typed fields are queryable — "all auto-renewing contracts above $400k" is one API call, no content scan needed.

## Metadata Richness by Object Type

### Visualization (canvas `richnessChart`, width 100% responsive × 380)

Grouped bar chart: qualitative comparison of governance surfaces per Box object type. Three bars per object group (max 18px bar width, 4px gap between bars in a group), y-scale 0–3.

- **Title (bold 13px, `#1a5276`, top left at 12,10):** "Which governance surfaces attach to which object type"
- **Subtitle (italic 10px, `#888`, at 12,28):** "none / partial / full — qualitative, from the documented object schemas"
- **Series (legend at bottom, 11px, filled 11×11 swatches):**
  - Custom metadata templates — `#1a5276`
  - Retention / legal hold — `#8e44ad`
  - Admin event coverage — `#e67e22`
- **Data (object: custom, gov, audit):**
  - file: 3, 3, 3
  - folder: 3, 3, 3
  - file_version: 0, 3, 2
  - web_link: 2, 1, 2
  - task: 0, 0, 2
  - shared_link: 0, 1, 2
  - user / group: 1, 0, 3
- **Y axis:** ticks at 0–3 labeled "none", "partial", "most", "full" (10px, `#888`, right-aligned); horizontal gridlines `#eee` (baseline `#999`). Padding: left 60, right 24, top 48, bottom 92.
- **Bars:** fill is series color at ~33% alpha (blue series uses `rgba(26,82,118,0.35)`, others color + `55` hex alpha), stroke series color at 1.5px. Zero values drawn as a flat `#ccc` tick on the baseline instead of a bar.
- **X labels:** object names rotated -20° (−π/9), 10.5px, `#2c3e50`, right-aligned under each group.
- **Footnote (italic 10px, `#888`, bottom left):** "Note: file_version carries no custom metadata — templates attach to the file, so field history is not versioned with content."

## Official API References

- [Box Developer Documentation](https://developer.box.com/) — developer home for the Box Platform
- [Box API Reference](https://developer.box.com/reference/) — REST reference for files, folders, metadata, events, retention

## Regeneration instructions

- **Layout:** platform-apis-fetch detail page. h1, `.subtitle` paragraph, `.verified` badge span, then one `.obj-table` (full width, border-collapse) with a single `<tr>`: left `<td>` (45%) holds `.section-head` headings + `<ul>` bullets + one `.key-point` callout; right `<td>` (55%) holds a `.section-head` + `<pre>` JSON sample, caption paragraph, another `.section-head` + the canvas. After the table, an h2 "Official API References" with a link list. Note: this page has no "Overview" h2 before the table (unlike siblings 17–20).
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.section-head` bold 0.95em `#1a5276`; `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `pre` — background `#f4f4f4`, padding 14px, radius 6px, 0.8em, ui-monospace; `li`/`p` 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="richnessChart" height="380">`, CSS `display:block; width:100%`; JS resizes on window resize, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), fixes CSS height to 380px, and applies `ctx.setTransform(dpr,0,0,dpr,0,0)` before drawing.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#888`/`#666`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
