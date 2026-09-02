# Tracking Data: Hidden Page Content

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Hidden Page Content

**Subtitle:** The document a browser downloads and the page a person reads are not the same thing. The difference is written for machines.

## What is it?

Rendered content is a subset of delivered content.

- **Markup arrives complete** — CSS then decides which parts get drawn
- **"Invisible" is a rendering outcome**, not a property of the data
- **Still there:** text set to `display:none`, pushed off-screen, or given zero opacity is in the page source, and often in the accessibility tree
- **Anything reading the document** sees all of it — crawlers, scrapers, preview generators, screen readers, automated agents

**Mostly mundane:** tabs, menus, collapsed panels, and screen-reader labels are hidden because the interface is not showing them yet. The gap is a channel, not a mechanism, and different things use it differently.

### Visualization (canvas `c1`, 720×320)

Two-panel subset diagram: the rendered page versus the delivered document.

- **Title (bold 16px, centered, blue `#2a78d6`, y=16):** "The same page: rendered, and as delivered".
- **Panels:** two blue-stroked rects (290×194, y=26) — left at x=40 titled "What renders" (bold 15px blue) with muted 13px sub "read by a person"; right at x=390 titled "What was delivered" with sub "read by a machine".
- **Visible blocks:** four solid bars (widths 150/240/210/90 px, 12px tall, 17px spacing) in translucent blue `rgba(42,120,214,0.35)`, drawn identically in both panels. Under the left panel's blocks, muted 13px left-aligned text: "nothing else is present to a viewer".
- **Extra blocks (right panel only):** five dashed-orange-outlined boxes (230×13, dash 3/3) with 13px monospace orange labels: "hidden input: session token", "honeypot field, off-screen", "JSON-LD + Open Graph tags", "framework state blob", "ad slot context; watermark".
- **Subset relation:** blue arrow between the panels with bold "⊆" symbol above it.
- **Caption (muted 13px, bottom center):** "Schematic. CSS decides the left panel; the network delivered the right one."

## What does it collect?

Nothing, as a category — the material is too varied for one answer. Separating it is the exercise.

| Hidden material | What it is for | Is it collection? |
|---|---|---|
| `display:none`, `visibility:hidden`, zero opacity, or off-screen | UI state, tabs, menus, accessibility text | Usually not — functional |
| Hidden inputs (`<input type="hidden">`) with session, campaign, form-instance tokens | State the server needs echoed back | Yes — carries identifiers |
| Honeypot fields, invisible to a person, filled in by naive automation | Bot detection | Yes — the signal *is* whether an invisible field was touched |
| Structured data: JSON-LD, Open Graph tags, framework state blobs, comments | Crawlers, link previews, the app itself | Not collection, but exposes fields the interface never shows |
| Contextual keywords and slot metadata around an ad container | Sent to the ad platform to pick an ad | Yes — the ad-network case |
| Zero-width and confusable characters inside visible text | Identifies which copy a leak came from | Yes — a watermark |
| Text placed where only an automated reader will encounter it | Keyword stuffing through to instructions aimed at that reader | Neither — a manipulation channel, not a collection one |

**A honeypot is a measurement, not a field:** it carries no data. What gets recorded is whether an element nobody could see was touched. Field names here are generic, not any vendor's parameters.

### Visualization (canvas `c2`, 720×320)

One-axis spectrum chart: hidden-material categories placed on an axis from "functional only" to "carries an identifier", as outlined chips with stems down to the axis.

- **Title (bold 16px, centered, blue `#2a78d6`, y=20):** "Does the hidden material report anything back?".
- **Axis:** horizontal blue line from x=60 to x=660 at y=200, tick marks at both ends; muted 13px endpoint labels "functional only" (left) and "carries an identifier" (right).
- **Chips (rounded outline, 20px tall, 13px label in chip color, stem in translucent blue down to axis; position t on 0–1 axis, stacking row 0/1/2 at y = 60 + row×34):**
  - t=0.03, row 0, "UI state, menus, tabs" — green `#008300`
  - t=0.10, row 1, "accessibility text" — green `#008300`
  - t=0.34, row 0, "JSON-LD / Open Graph" — orange `#d95926`
  - t=0.42, row 1, "framework state blob" — orange `#d95926`
  - t=0.70, row 2, "ad slot context" — blue `#2a78d6`
  - t=0.82, row 0, "honeypot verdict" — blue `#2a78d6`
  - t=0.90, row 1, "hidden session token" — blue `#2a78d6`
  - t=0.97, row 2, "zero-width watermark" — blue `#2a78d6`
- **Caption (muted 13px, bottom center):** "Schematic ordering, not a measurement. Middle group exposes fields without reporting them."

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, preformatted, verbatim):**

```
<!-- Delivered markup. Only the marked lines reach a screen. -->
<h2>Order summary</h2>                          <!-- seen -->
<button>Place order</button>                    <!-- seen -->
<input type="hidden" name="form_instance" value="fi_9c1d…">
<input type="hidden" name="campaign_ref"  value="cr_2b7f…">
<div style="position:absolute;left:-9999px">
  <input type="text" name="contact_alt_email">  <!-- honeypot -->
</div>
<span style="display:none">layout_variant_b</span>

// The record a collector derives from the same page.
{
  // ── present in the delivered document ──
  "form_instance":  "fi_9c1d…",
  "campaign_ref":   "cr_2b7f…",
  "variant_flag":   "layout_variant_b",

  // ── inferred / plausible ──
  "honeypot_filled":  true,   // an observation, not a field
  "honeypot_verdict": "automation_suspected",
  "bot_score":        0.88
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **Readers other than the person** — crawlers need structured descriptions, link previews need a title and image, screen readers need labels the design conveys visually
- **The page's own machinery** — an app carries its state; an ad container must describe its page before a platform can fill it

**Additional consequence** (label pill, orange)

- The same channel carries **identifiers and page context outward**, and can hold **a watermark** — two copies that look identical but differ in bytes
- **Text aimed at automated readers** becomes a way to steer that reader's output

**Declared, not verified:** a dataset scraped from these fields holds what sites chose to say about themselves for ranking. Nothing checked those claims, and a page that omits the markup is missing from the dataset rather than counted as featureless.

### Visualization (canvas `c3`, 720×320)

Horizontal funnel bar chart: pages remaining after each declared field a dataset requires, with dropped pages shown as faint gray remainder segments. Illustrative counts from 1,000 pages fetched.

- **Title (bold 13px, ink `#1a5276`, centered, y=24):** "Pages left after each field the dataset requires".
- **Subtitle (12px, muted `#6b7280`, centered, y=42):** "the fields are declared by the site, not verified by the crawler".
- **Geometry:** bars start at x=214, max width 452 (scaled to 1,000), 22px tall, rows 34px apart starting y=62; right-aligned 12px row labels at x=202; bold 12px value labels after each bar.
- **Rows (label → count):** "pages fetched" → 1,000; "+ has a title tag" → 940; "+ has a description" → 610; "+ has product data" → 240; "+ names an author" → 95. First four bars blue `#2a78d6` (fill `rgba(42,120,214,0.30)`, 1px stroke); last bar orange `#d95926` (fill `rgba(217,89,38,0.45)`). Behind each bar, the width dropped at that step is filled faint gray `rgba(107,114,128,0.14)`.
- **Legend line (muted 12px, left-aligned below last row):** "grey = pages dropped at that step, not pages without the feature".
- **Caption (italic 12px, `#2c3e50`, centered):** "The rows that survive are the sites that describe themselves most fully."
- **Footnote (italic 11px, muted, bottom center):** "Illustrative counts — the shape of the drop-off, not a measured crawl."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets, `.key-point` callouts, and (row 2) an inner `.cat-table`; right `<td>` (55%, centered) holds the canvas, plus (row 2 only) the `.payload-note` caption and `.payload` pre block, both left-aligned.
- **Inner table:** `.cat-table` — full width, collapsed borders `1px solid #cdd`, 0.86em, cell padding 7px 9px; `th` background `#eef4f8`, color `#1a5276`, left-aligned.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; outer table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `p` 0.95em; `li` 0.93em with bold lead terms `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`; `.lbl` uppercase pill labels 0.7em bold — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` monospace 0.78em, background `#f8f9fa`, left border `3px solid #1a5276`, `white-space: pre`; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×320); a shared `setupCanvas(id)` helper reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helper: `rr(ctx, x, y, w, h, r)` rounded-rect path. All chart data is hardcoded literal arrays — no random values.
- **Palette:** declared once as tokens `P` — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately excluded from the series rotation (reserved for genuine alarm states). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
