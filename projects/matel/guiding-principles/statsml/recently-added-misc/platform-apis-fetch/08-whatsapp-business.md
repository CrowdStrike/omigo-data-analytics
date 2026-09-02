# WhatsApp Business API (Cloud API)

**Page type:** detail page (two-column obj-table: text left 45%, code sample + canvas right 55%, one row)
**HTML title tag:** WhatsApp Business API — Platform APIs

**Subtitle:** Send and receive WhatsApp messages with customers at scale through Meta's hosted Cloud API.

**Verified badge:** Last verified: August 2026

## API Overview & Per-Message Pricing

### What you can get

- Send order updates, appointment reminders, OTP codes and promotions to customers on WhatsApp
- Receive customer replies into your own systems via webhooks
- Chat freely with a customer for 24 hours after they message you
- Reach out first using message templates pre-approved by Meta

**Key-point callout:** **You pay per message, and the clock matters.** Since July 2025, each delivered template message (marketing, utility, authentication) is charged individually. Replies within 24 hours of a customer's last message are free-form and free — outside that window, only paid pre-approved templates can be sent.

### Watch out for

- Customers must explicitly opt in before you can message them
- Templates need Meta approval, which can take up to 24 hours
- Requires a dedicated phone number — a personal WhatsApp can't be used
- How many people you can message per day grows with your quality rating

### Code sample (right column)

Heading: **Example: Send Template Message**

```
POST /v22.0/{phone_number_id}/messages

{
  "messaging_product": "whatsapp",
  "to": "15551234567",
  "type": "template",
  "template": {
    "name": "order_confirmation",
    "language": {"code": "en_US"},
    "components": [{
      "type": "body",
      "parameters": [{"type": "text", "text": "ORDER-12345"}]
    }]
  }
}
```

### Visualization (canvas `pricingChart`, responsive width × 400)

Grouped bar chart: template message pricing per country, four category bars per country group.

- **Title (bold 13px `#1a5276`, top center):** "Template Message Pricing by Country & Category (USD, indicative)"
- **Groups (x-axis, 11px `#2c3e50` labels):** North America, India, Brazil, UK, Indonesia
- **Series/colors:** Marketing `#e74c3c`, Utility `#1a5276`, Authentication `#27ae60`, Service `#bdc3c7`
- **Data (USD per message, [Marketing, Utility, Authentication, Service]):**
  - North America: [0.025, 0.015, 0.0135, 0.00]
  - India: [0.025, 0.004, 0.004, 0.00]
  - Brazil: [0.065, 0.025, 0.0315, 0.00]
  - UK: [0.058, 0.033, 0.031, 0.00]
  - Indonesia: [0.043, 0.018, 0.024, 0.00]
- **Y-axis:** 0 to 0.08 max; tick labels "$0.000", "$0.020", "$0.040", "$0.060", "$0.080" (11px `#666`, right-aligned) with `#eee` horizontal gridlines; `#999` baseline x-axis. Margins: top 50, bottom 70, left 60, right 20.
- **Bars:** group occupies 70% of its slot, 3px gap between bars; nonzero bars get a value label above them like "$0.025" (9px `#333`, centered).
- **Legend (bottom left, 11px):** color swatch + name for Marketing, Utility, Authentication, Service.
- **Footnote (italic 10px `#888`, bottom right):** "Service (free-form in 24hr window): free — see Meta rate card for current per-message rates"

## Official API References

- [Cloud API Overview](https://developers.facebook.com/docs/whatsapp/cloud-api) — architecture, getting started, sending messages
- [Pricing](https://developers.facebook.com/docs/whatsapp/pricing) — current per-message billing model and rate cards

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle`, `.verified` badge, then `h2` "API Overview &amp; Per-Message Pricing" followed by a one-row `.obj-table` (left `<td>` 45% with `<strong>` sub-headings — this page uses plain `<strong>` rather than a `.section-title` div — bullet lists, and a `.key-point` callout; right `<td>` 55% with a `<strong>` heading, a `<pre>` code sample, and the canvas), then `h2` "Official API References" with a link list.
- **Page style:** body system sans-serif, `#2c3e50` text, white background, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` inline badge — background `#eaf2f8`, border 1px `#2980b9`, color `#1a5276`, 0.8em, radius 4px, padding 2px 10px; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border 3px `#e74c3c`, padding 10px 14px, 0.93em; li 0.93em; links `#1a5276`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="pricingChart" height="400">`, CSS `width: 100%`; redraws on window resize using `getBoundingClientRect()` width; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and `ctx.scale` back to logical coordinates.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, neutral gray `#bdc3c7`, grays `#666`/`#888`/`#2c3e50`.
- In regenerated HTML, any card/page links use `.html` extensions.
