# Ad Tracking, Cookies & Third-Party Cookies

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Ad Tracking, Cookies & Third-Party Cookies

**Subtitle:** The tracking layer under the web itself — you never signed up for it, it has no product of its own, and it follows you across every site that embeds its pixel.

**Disclaimer callout (orange left-border box):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** nothing. This is the category's defining feature — there is no signup, no account, and usually no visible product. The "consent" is a cookie banner.
- **Incidental:** a *first-party cookie* remembers you on one site; a *third-party cookie* is set by an ad network embedded on thousands of sites, so the same ID sees your visits across all of them. Tracking pixels and script tags report every page view, scroll, and click back to networks the page owner chose; cookie syncing lets different networks match their IDs to each other; fingerprinting (fonts, canvas, GPU, screen) re-identifies you after cookies are cleared.
- **Inferred:** a cross-site browsing profile — interests, income band, health concerns, life events — assembled by companies you have never heard of, keyed to an ID you have never seen.

**Key-point callout:** The wow: a single page load can notify dozens of trackers, and through cookie syncing your visit to one obscure site can be joined to your history everywhere else — by parties with no relationship to you at all.

### Visualization (canvas `c1`, 720×420)

Diagram: first-party vs third-party cookies across three sites feeding one ad network, plus cookie-sync partners.

- **Title (bold 13px, `#1a5276`, top center):** "First-party vs third-party cookies".
- **Three site boxes** (150×90px at y=45, x = 60 / 285 / 510; `#1a5276` 2px stroke, 10% alpha fill, bold 12px heading): "News site", "Shopping site", "Health forum". Each contains two chips (120×20px): a green chip (`#27ae60`, 15% alpha fill, 10px label) "1st-party cookie (own ID)" and a red chip (`#e74c3c`, 15% alpha fill) "embedded tracker pixel".
- **Ad network box** (300×70px at x=210 y=230, `#e74c3c` 2px stroke, 12% alpha fill): bold 13px red "Ad network — ONE 3rd-party cookie ID", 11px `#2c3e50` line "sees all three visits as the same person".
- **Arrows:** dashed red lines (`#e74c3c`, dash 4/3, 1.3px) from each site's tracker chip to the ad network box.
- **Cookie-sync boxes** (140×30px at y=340): "other network" (x≈60) and "data broker" (x≈540), purple `#8e44ad` (12% alpha fill, bold 11px label), connected from the ad network box corners by dashed purple lines; centered purple 10px label "cookie syncing: networks match their IDs to each other".
- **Footnote (bottom center, `#999`, 11px):** "Green stays on one site. Red follows you across all of them. Purple joins the followers together."

## How it gets used

- **Ad targeting:** the cross-site profile decides which ads you see everywhere — including retargeting (the shoes that follow you for weeks).
- **Real-time bidding:** each ad slot triggers an auction where your profile is broadcast to hundreds of bidders in milliseconds — every bidder receives the data whether they win or not.
- **Attribution & measurement:** linking the ad you saw to the purchase you made, across devices, via identity graphs.
- **Identity resolution:** hashed emails and logins stitch cookies, phones, and TVs into one persistent person-level record.
- **Data resale:** segments ("expecting parent", "recently diagnosed", "in financial distress") are packaged and sold to anyone buying ads.

The moment of use is invisible: the auction, the sync, and the profile lookup all happen inside a single page load.

### Visualization (canvas `c2`, 720×340)

Fan-out diagram: one source box on the left connected by arrows to six destination boxes on the right.

- **Title (bold 13px, `#1a5276`, top center):** "One page load: the invisible fan-out".
- **Source box** (150×60px at x=40 y=140, `#1a5276` 2px stroke, 12% alpha fill): bold 12px "You load one page", 11px `#2c3e50` "(~1 visible article)".
- **Right boxes** (255×32px at x=430; 12% alpha fill, 1.5px colored stroke, bold 12px label in box color): "Analytics scripts" `#2980b9` (y 55), "Ad networks (pixels)" `#e74c3c` (y 105), "RTB auction — 100s of bidders" `#e74c3c` (y 155), "Cookie-sync partners" `#8e44ad` (y 205), "Social embed trackers" `#e67e22` (y 255), "Data brokers / identity graphs" `#8e44ad` (y 305).
- **Connectors:** gray `#bbb` 1.2px lines from the source box (x=190, y=170) to each right box, ending in rotated solid arrowheads.
- **Footnote (bottom center, `#999`, 11px):** "Every RTB bidder receives your profile whether it wins the ad slot or not."

## How long it's kept

- **Cookies themselves:** expirations from a session to years — but the cookie is just the key, not the data.
- **The server-side profile:** outlives any cookie; clearing cookies discards your copy of the key, not their copy of the profile — and fingerprinting or the next login re-links you.
- **Bid-stream data:** broadcast to hundreds of parties with no retention control at all once it leaves the auction.
- **Identity graphs:** maintained indefinitely as an asset; they are the product.
- **Third-party cookie phase-out:** the death of the cookie has not meant the death of tracking — server-side tagging, hashed-email IDs, and fingerprinting inherit the job.
- **Identifiable vs de-identified:** the longest-lived copies are usually "de-identified" — cookie IDs and hashed emails instead of names. But a pseudonymous ID that follows one person across years of browsing is not anonymous in any meaningful sense; stripping the name does not strip the identity.

### Visualization (canvas `c3`, 720×330)

Horizontal retention timeline bars with a "cookies cleared" marker.

- **Title (bold 13px, `#1a5276`, top center):** "Retention: the cookie is the key, not the data (illustrative)".
- **Rows (label, bar end in px from x0=235 toward xMax=690, color, note):** "Session cookie" end 300 `#27ae60` "closes with tab"; "Persistent 1st-party cookie" end 420 `#2980b9` "months–years"; "3rd-party cookie" end 420 `#e67e22` "until cleared / blocked"; "Fingerprint re-identification" end 560 `#e74c3c` "re-links after clearing"; "Server-side profile" end 620 `#e74c3c` "unaffected by clearing"; "Identity graph / bid-stream copies" end 690 `#e74c3c` "indefinite" with right-pointing arrowhead.
- **Layout:** right-aligned 11px labels ending at x=225, bar height 16px, gap 22px, starting y=46; bars filled at 45% alpha with 1px solid outline; notes in 10px `#666` after each bar end.
- **Cookies-cleared marker:** vertical dashed orange line (`#e67e22`, dash 5/4, width 2) at x=420 spanning all rows, with bold 11px centered label "cookies cleared" below.
- **Footnote (bottom center, `#999`, 11px):** "Clearing cookies discards your copy of the key — not their copy of the profile."

## What you get back

- **A typical "export":** effectively nothing. With no account, there is no export button. Some networks offer an opaque "why this ad" page or an interest-category list — a fraction of the profile, findable only if you know the network exists.
- **Not returned:** the cross-site browsing history, the bid-stream copies scattered across hundreds of buyers, the identity graph linking your devices, the inferred segments you are sold under.

**Key-point callout:** The asymmetry is total: consumer platforms return your content and keep the inferences; the tracking layer returns nothing, because you were never its customer — you are the inventory. Opt-outs typically stop the *targeting*, not the *collection*.

### Visualization (canvas `c4`, 720×320)

Two side-by-side panels comparing what is retrievable vs what exists.

- **Title (bold 13px, `#1a5276`, top center):** "What you can retrieve vs what exists".
- **Left panel** (310×235px at x=35 y=40, `#27ae60` 2px stroke, 8% alpha fill): heading bold 13px "WHAT YOU CAN RETRIEVE"; centered 12px `#2c3e50` items (28px spacing): "\"Why this ad\" page", "Interest-category list", "Opt-out toggle", "(only if you find the network)".
- **Right panel** (310×235px at x=375 y=40, `#e74c3c` 2px stroke, 8% alpha fill): heading bold 13px "EXISTS BUT NOT RETURNED"; centered items: "Cross-site browsing history", "Identity graph across devices", "Bid-stream copies (100s of buyers)", "Inferred segments you are sold as", "Cookie-sync match tables", "Fingerprint records".
- **Footnote (bottom center, `#999`, 11px):** "No account, no export. You were never the customer — you are the inventory."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout or paragraph, right `<td>` (55%, `text-align: center`) holds one canvas. Cell borders `1px solid #e0e0e0`, padding 16px. Page order: h1, `.subtitle`, `.disclaimer`, table.
- **Page CSS:** body system sans-serif stack, line-height 1.6, color `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, text `#7d5a29`, 0.9em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, 0.93em; canvas `display: block; margin: 0 auto`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fills `rgba(26,82,118,0.35)` and `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
