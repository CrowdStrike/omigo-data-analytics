# Expensify

**Page type:** detail page (h2 "Overview" then single obj-table row: text left 45%, payload + canvas right 55%; verified badge under subtitle; "Official API References" section below)
**HTML title tag:** Expensify — Platform APIs

**Subtitle:** Lets you pull your company's expense reports, receipts, and approval data out of Expensify.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Expense reports and the individual expenses inside them — merchant, amount, date, category, tags
- Receipt images, plus the fields the receipt scanner read off them automatically
- Company policy setup: category lists, tag lists, employees, and the approval chain
- Policy violations flagged on each expense

**Key point (callout):** The scanned merchant, amount, and date on a receipt are machine-read guesses, not facts. When a person corrects one, Expensify keeps the correction in a separate "modified" field next to the original. Always prefer the corrected value — and note that the corrections themselves are your only measure of how often the scanner gets it wrong.

### Watch out for

- Exports are a two-step file job: one call requests the export, a second call downloads the file. A single call appears to succeed but returns nothing useful.
- A report only shows its current status. A report that was rejected and resubmitted looks identical to one approved first try.
- There is no read-only credential — the same secret that exports data can also create and change expenses. Treat it as a write credential.
- Reports mix currencies, converted at the rate captured at report time. Re-converting later with today's rates will not reconcile.

### Sample payload

**Payload note (italic):** One expense inside an exported report — the scanned merchant name and the user's correction sit side by side.

```json
{
  "reportName": "Client visit — Berlin",
  "status": "APPROVED",
  "submitterEmail": "j.okoro@acme.example",
  "transactionList": [{
    "merchant": "HOTEL ADLON KEMPIN",
    "modifiedMerchant": "Hotel Adlon Kempinski",
    "amount": 92400,
    "modifiedAmount": 0,
    "currency": "EUR",
    "category": "Lodging",
    "receiptObject": { "state": "SCANCOMPLETE" }
  }]
}
```

### Visualization (canvas `approvalChart`, responsive width × 380)

Horizontal funnel bar chart: report status counts through four stages, with dashed red drop-off annotations between consecutive bars.

- **Title (bold 13px `#1a5276`, top center):** "Report Status Funnel — terminal state only, not a transition log".
- **Subtitle (italic 10px `#888`):** "Illustrative volumes. The API reports where each report is now; it does not report how it got there."
- **Stages (label / count / bar color):**
  - "OPEN" / 1000 / `#1a5276`
  - "SUBMITTED" / 942 / `#2980b9`
  - "APPROVED" / 806 / `#27ae60`
  - "REIMBURSED" / 771 / `#8e44ad`
- **Drop-off annotations (dashed `#e74c3c` line segment between each pair of bars, italic 10px red text):** "58 never submitted", "136 rejected or held", "35 pending payment".
- **Layout:** bars 40px high with 30px gaps, alpha 0.85, left margin 105px, right margin 150px; bar widths proportional to count / 1000; stage labels right-aligned 12px `#2c3e50` left of the bars; counts bold 12px right of each bar end.
- **Footer (11px `#666`, two lines, left-aligned):** "A report that was rejected once and resubmitted is indistinguishable" / "from one approved on the first pass. Both read APPROVED."

## Official API References

- [Expensify Integration Server API](https://integrations.expensify.com/Integration-Server/doc/) — the `requestJobDescription` reference: job types, credentials, filters, and the two-step export/download flow
- [Export Report Template Reference](https://integrations.expensify.com/Integration-Server/doc/export_report_template.html) — Freemarker fields available to report and expense export templates

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle`, `.verified` badge span, h2 "Overview", then one `.obj-table` (full width, border-collapse) with a single `<tr>`: left `<td>` (45%) holds two `<span class="section-label">` headings ("What you can get", "Watch out for") with bullet lists and a `.key-point` callout between them; right `<td>` (55%) holds `.payload-note`, `<pre class="payload">`, and `<canvas id="approvalChart" height="380">`. Below the table, an h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif, line-height 1.6, color `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.8em `#888`, 1px `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 `#1a5276` 1.3em with 2px `#2980b9` bottom border; `.section-label` bold `#1a5276`, block, margin-top 16px (0 for first); `.obj-table td` 16px padding, `1px solid #e0e0e0` border, vertical-align top; li 0.93em; p 0.93em; links `#1a5276`; `pre.payload` `#f8f9fa` background, 3px `#1a5276` left border, monospace 0.78em, pre whitespace, radius 4px, left-aligned; `.payload-note` 0.82em `#666` italic; `.key-point` `#f8f9fa` background, 3px `#1a5276` left border, padding 10px 14px, 0.93em; canvas block, `width: 100%`, margin-top 12px.
- **Palette:** `#1a5276` primary blue, `#2980b9` mid blue, `#27ae60` green, `#e74c3c` red, `#8e44ad` purple.
- **Canvas:** responsive — width from `canvas.getBoundingClientRect().width`, fixed 380px CSS height, backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.setTransform` reset then `ctx.scale` back to logical coordinates, redrawn on window resize.
- Note: in regenerated HTML, any card/page links use `.html` extensions.
