# Tracking Data: Share Link Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Share Link Tracking

**Subtitle:** A shared video link carries parameters identifying the sharer, so the platform can attribute the recipient's view back to a specific referral.

## Section 1: What is it?

A share link is not a bare link to the content.

- **Generated URL** carrying the sharing account, source surface, and share channel
- **Attribution:** the recipient's view is credited to a referral, not organic discovery
- **A short redirect** — the pasted link is usually not the link that loads
- **Observed directly on TikTok in the early 2020s:** the copied short URL, opened in a clean incognito tab with no session and no cookies, resolved to several hundred characters and twenty or more parameters. The video ID was the only readable field

**Opaque, not readable:** the values look base64- or hex-shaped, but shape does not establish whether they are encrypted, compressed, or merely encoded — from outside those are indistinguishable. The token-to-account mapping exists only on the platform's servers.

### Visualization (canvas `c1`, 720×320)

Two-stage flow diagram: a short link expanding into a long opaque URL (as observed).

- **Title (bold 17px, `#2a78d6` blue, top center):** "What the Share button copies, and what it resolves to".
- **Stage 1 box:** rounded rect (radius 4) at x=150, y=38, 420×30, fill `#f8f9fa`, stroke green `#008300`, 1.5px. Inside, 15px monospace text `#2c3e50`: `https://short.link/ZM8kQ2r7x/`. Left of the box, bold 14px green label: "on the clipboard" (at x=20, y=58).
- **Arrow:** vertical blue (`#2a78d6`) 2px line from (360,70) to (360,92) with a filled triangular arrowhead ending at y=100; to its right, 14px blue text: "opened in a clean incognito tab".
- **Stage 2 box:** rounded rect at x=60, y=104, 600×78, fill `#f8f9fa`, stroke blue `#2a78d6`, 1.5px. Inside, three lines of 13px monospace `#2c3e50` (20px line spacing starting y=124, left x=72):
  - `platform.com/@creator/video/6829174430?aa=MS4wLjABAAAAqL7Ry9c2`
  - `Bx8fVn0hK…&bb=8f2c1e4b7a90d3e6&cc=1591028417&dd=Y29weV9saW5r`
  - `&ee=… (20+ further parameters, several hundred characters)`
- **Tally line (y=202):** bold 14px, centered — green `#008300` "readable: the video ID" at x=200; orange `#d95926` "opaque: everything else" at x=480.
- **Caption (14px, muted `#6b7280`, centered, y=228):** "Parameter names are placeholders. The real ones carry no hint of meaning."

## Section 2: What does it collect?

- **Sharing account** identifier
- **The item** being shared
- **Surface** it was shared from — feed, profile, search
- **Share channel** selected — which messaging app
- **Recipient's account**, if logged in when they open it — the open itself usually reaches the platform's servers either way, since resolving the short link is a request to them
- **Timestamps** for both the share and the open

**What the observation establishes:** only the first payload block was observed. That the expanded URL is long and opaque is a fact; that any particular field sits in it is not. The parameter count is evidence that more than a video ID is carried, and nothing more.

**Watch history is not in the link:** it does not need to be. The account identifier is enough to join the referral to session data the platform already holds.

### Visualization (canvas `c2`, 720×320)

Four-row readability table chart: who can read what the parameters mean.

- **Title (bold 17px, blue `#2a78d6`, top center):** "Who can read what the parameters mean".
- **Rows** (rounded rects 640×32 at x=40, starting y=52, 40px spacing; who left-aligned at x=54 in 15px `#2c3e50`; capability right-aligned at x=666 in bold 15px):
  1. "You, reading the URL" → "the video ID only" (fill `rgba(42,120,214,0.10)`, stroke blue `#2a78d6`, capability text muted `#6b7280`)
  2. "The recipient" → "the video ID only" (same styling)
  3. "The messaging app" → "the URL as a string" (same styling)
  4. "The platform" → "the full mapping" (fill `rgba(231,126,34,0.18)`, stroke orange `#d95926`, capability text orange)
- **Caption (14px, muted, centered, y=226):** "The tokens are only meaningful to the party holding the lookup table."

### Payload (below canvas c2)

Payload note (italic, above the block): "Sample payload — illustrative structure, not real captured data."

```
// ── observed ──
// Stage 1. What the Share button puts on the clipboard:
https://short.link/ZM8kQ2r7x/

// Stage 2. Where it lands, opened in a clean incognito
// tab. Several hundred characters, 20+ parameters.
// Parameter names below are placeholders: the real ones
// are short, undocumented, and carry no hint of meaning.
platform.com/@creator/video/6829174430
  ?aa=MS4wLjABAAAAqL7Ry9c2Bx8fVn0hK…   // ~90 chars, base64-shaped
  &bb=8f2c1e4b7a90d3e6                 // 32 chars, hex-shaped
  &cc=1591028417                        // 10 digits, epoch-shaped
  &dd=Y29weV9saW5r                      // short, base64-shaped
  … 20+ further parameters

// ── inferred / plausible ──
// What one share plausibly becomes server-side:
{
  "item_id":       "6829174430",
  "sharer_uid":    "6742…",
  "share_link_id": "3f1c9a2e…",
  "channel":       "copy_link",
  "surface":       "fyp",
  "shared_at":     "2020-06-01T14:20:17Z",
  "opened_by":     null,      // set on first open
  "opened_at":     null
}
```

## Section 3: Why is it collected?

**Stated purpose** (label pill)

- **Routing** — send the recipient to the right content
- **Credit** — attribute shared views to the creator

**Additional consequence** (label pill)

- An **edge between two accounts** outside the platform's own follow graph — sharer and recipient **need not follow each other**

**A stronger ranking signal than a like:** the same edge yields a per-item share-through rate. A like costs one tap; a share costs social capital.

**The graph follows the button, not the friendship:** an edge appears only when the platform's own Share button made the link and the recipient opened it while logged in. A link retyped by hand, a screenshot, or a recommendation made out loud leaves no edge — so the graph tracks share-button use and login state, not the relationships it gets read as describing.

### Visualization (canvas `c3`, 720×320)

Horizontal funnel bar chart: how many real recommendations survive to become an edge in the graph. Illustrative funnel from one item recommended 100 times; only the final row exists as an edge.

- **Title (bold 14px, ink `#1a5276`, centered, y=28):** "From a recommendation to an edge in the graph". **Subtitle (12px, muted `#6b7280`, y=47):** "starting from 100 recommendations of the same item".
- **Bars:** left pad 40, start y=66, row height 30, gap 11; full width 300px = 100. Bar width proportional to n; fill is translucent tint of the row hue (alpha 0.22, last row 0.45), stroke solid hue (1.5px, last row 2px). Count centered in bar (bold 13px, row hue); label to the right at x=358 (12px, `#2c3e50`; last row bold in its hue); drop text below label (italic 11px, muted, prefixed "− ").
  1. n=100, blue `#2a78d6`, label "recommended to someone, any way at all", no drop
  2. n=64, aqua `#199e70`, label "sent as a link, not said out loud", drop "told in person, or on a call"
  3. n=41, violet `#4a3aa7`, label "made with the platform's Share button", drop "retyped, screenshotted, copied by hand"
  4. n=27, yellow `#c98500`, label "the recipient opened it", drop "never opened"
  5. n=18, orange `#d95926`, label "and was logged in when they did", drop "opened logged out"
- **Bracket:** orange 2px bracket on the left edge of the last row only, with rotated vertical bold 11px orange label "in the graph".
- **Captions (centered):** italic 12px `#2c3e50` at h−26: "The graph is missing 82 of the 100, and each loss has a reason of its own." Italic 11px muted at h−9: "Illustrative rates — the shape of the funnel, not a measured platform."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Include a rounded-rect path helper and a `tint(hex, alpha)` helper for translucent fills.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Project-wide palette reference: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
