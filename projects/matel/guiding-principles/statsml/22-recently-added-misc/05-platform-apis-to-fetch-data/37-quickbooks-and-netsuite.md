# QuickBooks & NetSuite

**Page type:** detail page (platform-API layout: h1 + subtitle + verified badge, one two-column obj-table row — text left 45%, payload + canvas right 55% — then an official-references list)
**HTML title tag:** QuickBooks & NetSuite — Platform APIs

**Subtitle:** Lets you pull the accounting books — invoices, bills, payments, and journal entries — out of QuickBooks Online or NetSuite.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get**

- Invoices, bills, payments, and journal entries, down to the individual line
- The chart of accounts, plus customer and vendor lists
- QuickBooks' own pre-built reports: P&L, balance sheet, aging
- In NetSuite, SQL-style queries over the ledger tables — joins and aggregates included

**Key point (callout):** The ledger is restated. Accountants reclassify, void, and adjust prior months, often weeks after the fact — so an extract of "March" taken in April and one taken in July will legitimately differ. Record the as-of date of every extract, or you will never be able to tell a restatement from a pipeline bug.

**Watch out for**

- No change history. Both systems tell you a record was edited and when — not what the earlier values were. If "why did last quarter move?" matters, snapshot the data yourself from day one.
- QuickBooks queries cannot join or aggregate — you pull raw rows and compute totals yourself.
- Deleted records vanish silently from ordinary queries. QuickBooks reports deletions only through its change-tracking endpoint.
- NetSuite filters rows by the requesting role's permissions, silently. A permissions change looks exactly like a business decline.

### Payload example

**Payload note (italic, above the block):** A QuickBooks invoice. `SyncToken: "3"` means it has been edited three times — but the earlier values are gone.

```
{
  "Invoice": [{
    "Id": "1047",
    "SyncToken": "3",
    "TxnDate": "2026-07-28",
    "DueDate": "2026-08-27",
    "CustomerRef": { "name": "Northwind Retail Ltd" },
    "TotalAmt": 4820.00,
    "Balance": 1820.00,
    "MetaData": {
      "CreateTime": "2026-07-28T09:14:02-07:00",
      "LastUpdatedTime": "2026-08-11T16:40:55-07:00"
    }
  }]
}
```

### Visualization (canvas `restateChart`, responsive width × 380)

Line chart with colored point markers: the same accounting period's revenue as reported at four different extraction dates.

- **Title (bold 13px `#1a5276`, top center):** "Same accounting period, re-extracted at four as-of dates".
- **Subtitle (italic 10px `#888`, centered):** "Illustrative. July revenue as reported by the ledger, queried on four different days."
- **Data (x label, value, point color, italic note under the x label):** Aug 1 $1.842M `#1a5276` "pre-close"; Aug 12 $1.918M `#27ae60` "accruals posted"; Sep 3 $1.887M `#e67e22` "reclass + credit memo"; Nov 15 $1.891M `#8e44ad` "audit adjustment".
- **Axes:** y from $1.78M to $1.96M with 7 gridlines labeled "$X.XXM" (light gray `#eee` lines, labels `#666`); x baseline gray `#999`; margins top 62, bottom 92, left 66, right 26; points centered in four equal slots.
- **Reference line:** horizontal dashed line (rgba(26,82,118,0.35), dash 4/4, width 1.4) at the first reading ($1.842M).
- **Series:** connecting line `#1a5276` width 2.2; 6px-radius dots in the per-point colors; bold value label "$1.842M" etc. in `#2c3e50` above each dot.
- **Annotation (italic 10px red `#e74c3c`, left-aligned just above the reference line):** "+4.1% between the first two reads — no bug, just close" (the percentage is computed from the first two values: (1.918−1.842)/1.842 × 100, rendered to one decimal).
- **Footer (two 11px `#666` lines, left-aligned at bottom):** "None of these readings is wrong. An extract without an as-of date is" / "unfalsifiable — you cannot tell restatement from an ingestion defect."

## Official API References

- [QuickBooks Online API — Get Started](https://developer.intuit.com/app/developer/qbo/docs/get-started) — the Accounting API entry point: entities, query dialect, and CDC
- [NetSuite Documentation (Oracle Help Center)](https://docs.oracle.com/en/cloud/saas/netsuite/) — SuiteTalk REST, SuiteQL, SuiteScript, and authentication guides

## Regeneration instructions

- **Layout:** platform-API detail page: h1, `.subtitle` paragraph, `.verified` inline badge, `<h2>Overview</h2>` with a bottom border, one `.obj-table` row — left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with bullet lists and a `.key-point` callout between them; right `<td>` (55%) holds a `.payload-note` italic paragraph, a `pre.payload` JSON block, and the canvas — then `<h2>Official API References</h2>` with a link list.
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; `.verified` 0.8em `#888` with `1px solid #e0e0e0` border, radius 4px, padding 2px 10px; h2 1.3em `#1a5276` with `2px solid #2980b9` bottom border; `.section-label` bold `#1a5276`; `.obj-table` cells `1px solid #e0e0e0`, padding 16px, vertical-align top; li/p 0.93em; links `#1a5276`; `pre.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, radius 4px; `.key-point` same background/left border, padding 10px 14px, 0.93em; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** `display: block; width: 100%`, height attribute 380; drawn responsively from `getBoundingClientRect().width`, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, gray text `#666`/`#888`/`#2c3e50`, reference line rgba(26,82,118,0.35).
