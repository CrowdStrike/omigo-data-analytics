# Tracking Data: Cookies

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Cookies

**Subtitle:** A cookie is a small piece of text a site stores in your browser, which the browser sends back every time you return. Who can read it depends on who left it.

## What is it?

A short note a site leaves in your browser, which the browser hands back on every later visit.

- **The site says "keep this"** — your browser stores it and returns it automatically
- **First-party:** left by the site you are actually on
- **Third-party:** left by something embedded in the page, such as an ad

**Same trick either way — only the owner differs:** which is why browsers restrict the third-party kind by default rather than asking site by site.

### Visualization (canvas `c1`, 720×320)

Side-by-side comparison diagram: two rounded rectangles contrasting first-party and third-party cookies.

- **Title (bold 16px, centered, blue `#2a78d6`, y=20):** "Two kinds of cookies — one helpful, one not".
- **Left box:** rounded rect (radius 8) at x=30, y=40, 300×180, filled green `#008300`. White text, centered at x=180: bold 16px "First-Party Cookie" (y=65); 15px "Set by the site you're visiting" (y=88). White 30%-opacity circle (radius 30) at (180,140) containing a cookie emoji (🍪, 20px). Three 14px lines: "Keeps you logged in" (y=185), "Remembers your cart" (y=200), "Saves your language" (y=215).
- **Right box:** rounded rect at x=370, y=40, 320×180, filled magenta `#d55181`. White text centered at x=530: bold 16px "Third-Party Cookie" (y=65); 15px "Set by an AD COMPANY on that page" (y=88). White 30%-opacity circle (radius 30) at (530,140) containing an eye emoji (👁, 20px). Three 14px lines: "Tracks you across the entire web" (y=185), "Builds a profile of everything you do" (y=200), "Sells that profile to advertisers" (y=215).

## What does it collect?

- **A signed-in session**, so you are not asked to log in on every page
- **Preferences** — language, currency, layout
- **A random string** that means nothing on its own but is unique to your browser — one string per browser per device, so one person on a phone and a laptop looks like two visitors

**The cookie is a ticket number, not the file:** what is known about that number sits on a server, not in your browser.

**Clearing cookies throws away the ticket, not the file:** the old record stays and your next visit starts a fresh one. So one person turns into several tickets over time, and "unique visitors" counts too many people.

### Visualization (canvas `c2`, 720×320)

Canvas-drawn comparison table: which data each cookie type stores.

- **Title (bold 16px, centered, blue `#2a78d6`, y=20):** "What each type stores".
- **Table:** starts at x=130, y=40; column widths 220/120/120; row height 28. Header row filled blue `#2a78d6` with white bold 15px labels: "Data Type", "First-Party", "Third-Party".
- **Rows** (zebra striping `#f8f9fa` / `#fff`, text `#2c3e50` 15px), values as (row label, first-party, third-party):
  - Login state: ✓ / —
  - Cart contents: ✓ / —
  - Unique tracking ID: — / ✓
  - Cross-site browsing: — / ✓
  - Ad click history: — / ✓
  - Visit count: ✓ / ✓
- **Marks:** first-party checkmarks green `#008300`, third-party checkmarks magenta `#d55181`, dashes gray `#e5e9ef`. Outer border stroked blue `#2a78d6`, width 1.

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, preformatted, verbatim):**

```
// Set-Cookie response headers. Attribute names below
// are defined by RFC 6265; values are illustrative.

// ── documented / standard ──
// (a) first-party, from the site in the address bar
Set-Cookie: sid=8f2ac41e…; Domain=shop.example.com;
  Path=/; Expires=Fri, 22 Aug 2026 16:00:00 GMT;
  Secure; HttpOnly; SameSite=Lax

// (b) third-party, from a script embedded in the page
Set-Cookie: uid=e1b7d904…; Domain=.adnetwork.example;
  Path=/; Max-Age=31536000;
  Secure; SameSite=None      // None required to send
                             // in a cross-site context

// What the server receives on a later request:
Cookie: sid=8f2ac41e…; uid=e1b7d904…; lang=en-US

// ── inferred / plausible — the server-side record
//    that `uid` is used to look up ──
{
  "uid":          "e1b7d904…",
  "first_seen":   "2025-11-03",
  "domains_seen": ["shop.example.com", "news.example", "…"],
  "segments":     ["in_market_footwear", "…"],
  "sync_ids":     { "dsp_a": "…", "ssp_b": "…" }
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **To remember you between clicks** — carts, logins, settings
- **Nothing else can do it:** the web forgets you between every page unless the browser hands something back

**Additional consequence** (label pill, orange)

- An ad company embedded on **thousands of sites** can read its own cookie on all of them
- Your path across those sites **joins up under one string**

**How long it lasts changes everything:** some cookies vanish when you close the browser; others are set to last a year.

**Fine for a cart, wrong for a headcount:** a cart works whether you hold several cookies or a family shares one computer. Counting people breaks in both cases, and a cookie cannot tell them apart.

### Visualization (canvas `c3`, 720×320)

Forked-path flow diagram: one starting node splitting into a helpful path and a cross-site path.

- **Title (bold 16px, centered, blue `#2a78d6`, y=20):** "Same word "cookie" — two very different things".
- **Start node:** blue `#2a78d6` circle (radius 20) at (360,55) with white bold 14px two-line label "You visit" / "a site".
- **Fork lines:** green `#008300` line (width 2) from (345,72) to (150,105); magenta `#d55181` line from (375,72) to (550,105).
- **Left (good) path:** three green rounded rects (140×28, radius 5) at x=80, starting y=105, spaced 40px, white 15px centered text: "Login saved", "Cart remembered", "Done!"; green connector lines between them at x=150.
- **Right (bad) path:** three magenta rounded rects (160×28, radius 5) at x=480, same y positions, white text: "ID assigned", "Tracked across web", "Profile sold"; magenta connectors at x=560.
- **Path labels (bold 15px, y=230):** "Helpful path" in green centered at x=150; "Cross-site path" in magenta centered at x=560.

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, plus (row 2 only) the `.payload-note` caption and `.payload` pre block, both left-aligned.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with bold lead terms `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`; `.lbl` uppercase pill labels 0.7em bold — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` monospace 0.78em, background `#f8f9fa`, left border `3px solid #1a5276`, `white-space: pre`; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×320); a shared `setupCanvas(id)` helper reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex, a)` for translucent fills, `rr(ctx, x, y, w, h, r)` rounded-rect path.
- **Palette:** declared once as tokens `P` — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately excluded from the series rotation (reserved for genuine alarm states). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
