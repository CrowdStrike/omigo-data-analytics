# Tracking Data: Internet Provider Logging

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Internet Provider Logging

**Subtitle:** Resolving a domain name requires a resolver to be asked. Whoever operates that resolver can log the question.

## Section 1: What is it?

A name has to be turned into an address, and something has to be asked.

- **Mechanism:** a device asks a DNS resolver to turn a name into an address before it can connect
- **Default resolver** is the one the internet provider supplies
- **Question and answer pass through it**, so the operator is in a position to log them
- **Encrypted DNS** changes who is asked, not whether the question is loggable

**Key point (callout):** **Encryption moves the record, not the record-keeping:** the question still has to be answered by somebody, and that somebody is then the party holding the log.

### Visualization (canvas `c1`, 720×320)

Flow diagram: traffic pipe from device through the resolver to websites.

- **Title (bold 16px blue `#2a78d6`, top center):** "The resolver has to be asked before the connection can be made".
- **Device box (left):** solid blue `#2a78d6` rounded rect (100×80 at x=30, y=80, radius 8) with white bold 15px two-line label "Your" / "Device".
- **Pipes (device → resolver):** two tapering quadrilateral bands filled `rgba(42,120,214,0.35)` fanning from the device to the resolver box.
- **Resolver box (center):** solid blue rounded rect (180×120 at x=260, y=60, radius 10); white bold 16px "DNS resolver"; a log-lines glyph of four white horizontal strokes (width 2, last line shorter); white 13px caption inside: "name asked, and when".
- **Pipes (resolver → websites):** two more tapering translucent blue bands to the right column.
- **Website boxes (right):** four rounded rects (130×30, radius 5, stacked at 42px spacing from y=55), fill `rgba(42,120,214,0.35)`, blue stroke, blue 14px labels: "example.com", "cdn.example.net", "api.example.org", "example-clinic.com".
- **Arrow labels (13px `#6b7280`):** "DNS request →" over the left pipe; "← response" over the right pipe.

## Section 2: What does it collect?

- **Domain name** queried — not the page path, which is inside the encrypted connection
- **Query type**, response code, and the answer records
- **Timestamp** of each query
- **Subscriber address** the query came from
- **Device hint**, sometimes, from the local hostname
- **Server names from the traffic itself** — the provider also carries the connection, and a TLS handshake typically names the host in the clear (SNI), so switching resolvers does not hide which domains are visited

**Key point (callout):** **A name, not a page or an action:** the row records that a device asked where a domain lives. Whether the connection followed, what was read, and whether a person or a background app asked are all absent.

**Key point (callout):** **TTL breaks counting:** a repeat visit inside the cache window generates no query at all, so query counts undercount visits by an amount that varies per domain.

### Visualization (canvas `c2`, 720×320)

Terminal-style DNS log mockup.

- **Title (bold 16px blue `#2a78d6`, top center):** "What a resolver log row holds — illustrative".
- **Terminal window:** dark rounded rect `#1a1a2e` (600×200 at x=60, y=30, radius 6) with a header bar `#2d2d44` (22px tall) containing three window dots (magenta `#d55181`, orange `#d95926`, green `#008300`) and a centered 12px monospace gray `#aaa` filename: "isp-dns-log.txt".
- **Log rows (13px monospace, 24px line spacing, illustrative, on reserved example domains):** columns — timestamp in green `#008300` at x=80, domain in white at x=180, device hint in orange `#d95926` at x=420, query type "│ A" in muted `#6b7280` at x=560:
  - 08:01:12 / mail.example.com / phone
  - 08:03:44 / forum.example.net / phone
  - 08:15:02 / health-info.example / laptop
  - 08:22:18 / jobs.example.org / laptop
  - 08:34:55 / cdn.example.net / laptop
  - 08:41:30 / video.example.com / tv
  - 09:02:11 / bank.example.com / phone
- **Caption inside the frame (12px monospace `#6b7280`):** "the row records the name asked — not the page, and not who asked".

### Example payload (below canvas `c2`, right column)

Visible caption above the block (`.payload-note`, italic): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── standard DNS fields (RFC-defined) ──
  "qname":       "www.example-clinic.com",
  "qtype":       "A",           // also AAAA, HTTPS, TXT
  "rcode":       "NOERROR",
  "answer":      ["198.51.100.7"],
  "ttl":         300,           // seconds; a re-visit inside
                                // the TTL produces no query
  "ts":          "2026-08-22T21:33:02Z",

  // ── inferred / plausible, added by the resolver operator ──
  "subscriber_id": "acct-4471…",
  "client_ip":     "203.0.113.42",   // the household, not a person
  "device_hint":   "android-tablet", // from DHCP hostname
  "category":      "health",         // domain classification list
  "transport":     "Do53"            // vs DoH / DoT
}
```

## Section 3: Why is it collected?

**Label (`.lbl-purpose`):** Stated purpose

- **Diagnosing failures** and sizing cache capacity
- **Blocking malware domains** — a blocklist cannot work without inspecting the name; retention for a set period is legally required in some places

**Label (`.lbl-effect`):** Additional consequence

- Classified against a category list, it yields **interest categories per subscriber line**; categorised aggregates are openly sold
- Held by the operator, so it is **reachable by legal process** without the subscriber's involvement

**Key point (callout):** **The trap is the unit:** a row is keyed to a subscriber line, so the unit of observation is a household or an office, not a person. Running a resolver never needed that split, so nothing in the log supports it — a category may belong to a household member other than the one being modelled.

### Visualization (canvas `c3`, 720×320)

Segmented proportion bar plus legend: a day of queries on one subscriber line, split by what sent them. Illustrative shares; the point is that the row carries no such field.

- **Title (bold 14px `#1a5276`, top center):** "One day of queries on one subscriber line".
- **Subtitle (12px `#6b7280`):** "the log records the line — not which of these sent the query".
- **Data (query counts by originator):**
  - "one household member": 1840, blue `#2a78d6`, a person
  - "another member": 1120, violet `#4a3aa7`, a person
  - "a third member": 640, aqua `#199e70`, a person
  - "TV, speaker, thermostat": 980, yellow `#c98500`, not a person
  - "background prefetch and retries": 1420, magenta `#d55181`, not a person
  - Total 6,000; non-person share 2,400 → 40%.
- **Bar:** single horizontal bar at x=42, y=62, width w−84, height 52, segmented proportionally by count; each segment filled with its hue at alpha 0.40, stroked in the hue, with the count in bold 12px centered in its segment; total "6,000" in 12px `#6b7280` centered below the bar.
- **Legend (from y=154, 24px rows):** 13×13 tinted swatch with hue stroke, label in 13px `#2c3e50`, and a right-hand annotation at x=400 — "a person" in muted gray for the three household members, "nobody was at a screen" in orange `#d95926` for the other two.
- **Highlight line (bold 13px orange):** "About 40% of the day had nobody behind it".
- **Bottom captions (centered):** italic 12px `#2c3e50`: "A profile read off this line attributes all five segments to one person."; italic 11px `#6b7280`: "Illustrative shares — the shape, not a measured line."

## Regeneration instructions

- **Layout:** tracking detail-page `.obj-table`: full-width table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets, `.lbl` labels and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600; `.lede` 0.95em.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`.
- **Labels:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** intrinsic 720×320 each; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared palette object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, orange:#d95926, violet:#4a3aa7, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }` and a rounded-rect path helper. Red is reserved for alarm states and not used here. All chart data is hardcoded literal arrays (no Math.random); the c3 percentage is computed in-script from the counts.
- **Project palette reference:** #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Links:** none on this page; in regenerated HTML any card links elsewhere use `.html` extensions.
