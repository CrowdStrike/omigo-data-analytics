# Tracking Data: Shadow Profiles

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Shadow Profiles

**Subtitle:** A record about someone with no account, assembled from what other people uploaded. Because the subject never sees it, its error rate cannot be checked by the one person who would know.

## Section 1: What is it?

**Lede:** A row about a non-user, keyed on a contact detail.

- **Trigger:** a user grants contact-list access, and the upload contains entries for people who are not users
- **Why the row exists:** to suggest "people you may know" later, the platform needs a row keyed on the contact's email or phone
- **It accumulates:** uploads from different users converge on the same key
- **More attaches to it** — photo tags, check-ins mentioning a name, mutual-contact structure

**Callout:** **Every field is second-hand:** a name comes from what a contact typed into their own address book; a relationship comes from co-occurrence in other people's lists. What distinguishes this from an under-populated user profile is the sourcing, not the sparsity.

### Visualization (canvas `c1`, 720×320)

Hub-and-spoke diagram: the row is assembled from uploaders, not from the subject. One hue per uploading account.

- **Title (bold 14px ink `#1a5276`, centered, y=22):** "Every arrow points inward — the subject contributes nothing". Subtitle (12px mute `#6b7280`, y=40): "one hue per uploading account: the row is the union of separate address books".
- **Uploader nodes:** seven filled circles radius 22 labeled "acct 1"–"acct 7" (white 11px text), placed on an ellipse around center (w/2, 156) with radii 255 (x) and 80 (y), starting at −90° and stepping 2π/7; hues in series order: blue `#2a78d6`, green `#008300`, violet `#4a3aa7`, orange `#d95926`, aqua `#199e70`, magenta `#d55181`, yellow `#c98500`.
- **Spokes:** a 1.5px line from each uploader to the center, in that uploader's hue at alpha 0.45.
- **Central node:** white circle radius 38 with dashed ink stroke (dash 5/5, width 2.5 — nothing self-reported), containing a bold 28px ink "?"; 13px ink caption below at center+56: "record about a non-user".
- **Footer bands (two rounded boxes 320×50 r7 at y=256):** left box at x=30, orange tint alpha 0.12: bold 13px orange `#d95926` "Supplied by other accounts", 12px text color "name, label, associates, confidence". Right box at x=370, mute tint alpha 0.10: bold 13px mute "Supplied by the subject", 12px mute "no fields".

## Section 2: What does it collect?

- **Phone numbers and email addresses** from uploaded contact lists
- **Name and label** the uploader had saved for the entry — plus whatever else the contact card carried, such as an employer, birthday, or address
- **Which accounts hold that contact**, and how many
- **Co-occurrence structure** — which other entries appear in the same lists
- **Mentions** in tags, captions, or check-ins made by account holders

**Callout:** **Hashing preserves linkage:** an email address has one canonical form, so its hash is deterministic — every uploader with that address produces the identical key, which is what makes it a usable join key. Anonymisation would break linkage; this keeps it.

**Callout:** **No correction path:** with `subject_notified: false`, a `match_confidence` of 0.63 can never be checked against the one person who would know it was wrong.

### Visualization (canvas `c2`, 720×320)

Pipeline diagram: a deterministic hash is a join key, not anonymisation — three address books, one transform, one key; hue tracks the stage.

- **Title (bold 14px ink, centered, y=24):** "The same address hashes to the same key from every uploader". Subtitle (12px mute, y=44): "three separate address books, one transform, one key — hue tracks the stage".
- **Sources (three rounded boxes 200×34 r6 at x=30, y centers 90/150/210):** "acct 1 address book" (blue `#2a78d6`), "acct 2 address book" (green `#008300`), "acct 3 address book" (violet `#4a3aa7`); each box filled in its hue at alpha 0.13 with a 1.5px stroke, bold 12px label in its hue plus 12px monospace text-color line "name@example.com"; a 2px arrow line in the source's own hue converging on the transform at (314,152).
- **Transform:** solid orange `#d95926` rounded box (320,130) 92×44 r6; white bold 13px "sha256" and 11px "canonical form".
- **Arrow out:** aqua `#199e70` line width 2 with solid aqua arrowhead from (414,152) to (480,152).
- **Result key:** rounded box (486,122) 182×60 r8, aqua fill alpha 0.15, aqua stroke width 2; bold 13px aqua "one identical key", 12px monospace text-color "sha256:9c1e…", 11px mute "all three rows join".
- **Footer bands (two rounded boxes 320×50 r7 at y=236):** left at x=30, aqua tint alpha 0.12: bold 13px aqua "Preserved", 12px text color "exact linkage across every uploader". Right at x=370, mute tint alpha 0.10: bold 13px mute "Removed", 12px mute "the readable form, not the ability to join".
- **Caption (13px mute, centered, y=306):** "Anonymisation would break the join; determinism is what makes the key useful."

**Payload note (right column, below canvas):** Sample payload — illustrative structure, not real captured data.

```
// No schema is published for records about people who
// have no account, so the whole block is reconstruction.
// The shape follows from the constraint: with no login,
// the row has to be keyed on something else.
{
  // ── inferred / plausible ──
  "record_key":    "sha256:9c1e…",   // hash of an email address
  "has_account":   false,
  "identifiers": {
    "email_hash":  "sha256:9c1e…",
    "phone_hash":  "sha256:b704…"
  },
  "arrived_via": [
    { "source": "contact_sync", "uploader": "u-8812…" },
    { "source": "contact_sync", "uploader": "u-4470…" }
  ],
  "inferred_name":     "…",
  "match_confidence":  0.63,
  "subject_notified":  false
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Contact matching** — telling a new user which contacts are already on the platform
- **Unmatched entries are kept** so the suggestion still works if that contact joins later

**Label (effect pill):** Additional consequence

- Those entries constitute a **record about a non-user** — a name, a set of associates, often a confidence score
- It exists **before any relationship** with that person, and shapes **what they are shown** if they sign up

**Callout:** **One row is not one person:** a common name across several address books can merge two people, and one person with two email addresses can split into two rows. A count of rows is therefore not a count of people, and neither error announces itself.

### Visualization (canvas `c3`, 720×320)

Line chart: how much of a row exists depends on who else joined — two non-users with the same real circle of 40 acquaintances, stored associate count vs platform adoption of their circle.

- **Title (bold 14px ink, centered, y=24):** "Two non-users, the same forty acquaintances". Subtitle (12px mute, y=42): "the stored associate count is the part of that circle that joined and uploaded a list".
- **Axes:** ink L-axis; plot area x from 80 to 600, y baseline 236, top 74. Y ticks 0/10/20/30/40 with grid-gray `#e5e9ef` gridlines and 12px mute labels; x ticks 0%/25%/50%/75%/100%.
- **Ceiling line:** dashed violet `#4a3aa7` horizontal line (dash 5/4, width 1.5) at y = 40; bold 12px violet label "forty real acquaintances, for both".
- **Stored-count line:** blue `#2a78d6` diagonal (width 2.5) from (0%, 0) to (100%, 40); bold 12px blue label "associates stored in the row" (right-aligned near 86% of the line).
- **Two subjects (dot radius 5.5 with dashed drop lines to both axes in their own tinted hue):** at 10% adoption, orange `#d95926` — bold 12px "few of their circle joined", 12px "reads as 4 associates"; at 80% adoption, aqua `#199e70` — "most of their circle joined", "reads as 32 associates".
- **X-axis caption (12px mute, centered, baseline+38):** "share of that circle who joined and uploaded a contact list  →".
- **Captions (centered):** italic 12px text color at h−26: "The row on the left is not a quieter life — it is a thinner sample of the same one."; italic 11px mute at h−9: "Illustrative — forty is a stand-in, chosen to make the proportion readable."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede` paragraph, bullets with bolded lead terms (`li b` in `#1a5276`), `.lbl` purpose/effect pills, and `.key-point` callouts; right `<td>` (55%, `text-align: center`) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption plus `.payload` `<pre>` block (both left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em. `.key-point` and `.payload`: background `#f8f9fa`, left border `3px solid #1a5276`; `.payload` ui-monospace 0.78em pre, `.payload-note` 0.82em italic `#666`. `.lbl` pills: uppercase 0.7em bold, `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart (720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts share a rounded-rect path helper (`rr`) and a `tint(hex, alpha)` helper producing rgba fills from palette tokens.
- **Palette:** this page's charts use the tracking categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Site-wide accents remain #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
