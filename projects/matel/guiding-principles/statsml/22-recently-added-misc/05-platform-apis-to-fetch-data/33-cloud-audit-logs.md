# Cloud Audit Logs

**Page type:** detail page (single obj-table row: text left 45%, payload + canvas right 55%; verified badge under subtitle; "Official API References" section below)
**HTML title tag:** Cloud Audit Logs — Platform APIs

**Subtitle:** The activity trails AWS, Google Cloud, and Azure keep — a record of who did what in your cloud accounts.

**Verified badge:** Last verified: August 2026

## What you can get

- Who created, changed, or deleted resources and permissions — recorded by default
- Who read or wrote the data itself — recorded only if someone switched it on first
- For every action: the actor, their IP address, the target, the time, and success or failure

**Key point (callout):** An event not being recorded when it happened is **unrecoverable — no provider can reconstruct the period before logging was enabled**. The configuration decision must be made before you know which question you will need to answer, so the safe default is to enable broadly and store cheaply.

## Watch out for

- Data-read logging is off by default on all three clouds — "who read the data" is usually the post-breach question, and usually the unanswered one
- Free retention is short; anything longer needs an export you configure and pay for yourself
- Each cloud uses its own record format — lining them up into one model is most of the real work
- Logs scoped to one region or subscription silently miss activity everywhere else

## Sample payload

**Payload note (italic):** Sample AWS CloudTrail data event (abbreviated) — this record exists only because data-event logging was configured beforehand; by default, the read leaves no trace.

```json
{
  "eventTime": "2026-08-18T13:41:07Z",
  "eventSource": "s3.amazonaws.com",
  "eventName": "GetObject",
  "eventCategory": "Data",
  "sourceIPAddress": "10.42.7.19",
  "userIdentity": {
    "type": "AssumedRole",
    "arn": "arn:aws:sts::111122223333:assumed-role/etl-task-role/etl-worker-7"
  },
  "requestParameters": {
    "bucketName": "acme-warehouse-raw",
    "key": "events/dt=2026-08-18/part-00017.parquet"
  }
}
```

### Visualization (canvas `c1`, responsive width × 380)

Status-dot table: one row per provider log stream, a green/red dot marking default-on vs must-be-enabled, with a colored note per row; rows grouped by provider with a tinted provider band.

- **Title (bold 14px `#1a5276`, top center):** "Default-on vs must-be-enabled, by provider and stream".
- **Subtitle (italic 10px `#666`):** "every red row is a period of history you will not be able to reconstruct".
- **Rows (provider / stream label / dot color / note text):**
  - AWS / "Management events" / green `#27ae60` / "on by default; short free window"
  - AWS / "Data events (S3, Lambda…)" / red `#e74c3c` / "OFF — needs event selectors"
  - AWS / "Insights events" / red `#e74c3c` / "OFF — enabled per trail"
  - GCP / "Admin Activity" / green `#27ae60` / "always on; cannot be disabled"
  - GCP / "System Event" / green `#27ae60` / "always on"
  - GCP / "Data Access" / red `#e74c3c` / "OFF (except BigQuery) — IAM audit config"
  - Azure / "Activity Log" / green `#27ae60` / "on by default; short fixed window"
  - Azure / "Resource / diagnostic logs" / red `#e74c3c` / "OFF — diagnostic setting per resource"
  - Azure / "Entra sign-in export" / red `#e74c3c` / "OFF — diagnostic setting required"
- **Layout:** 30px row height starting at y=56; each provider group gets a full-width background band in the provider color at alpha 0.12 with a bold 11px provider label at the left (provider colors: AWS `#e67e22`, GCP `#1a5276`, Azure `#8e44ad`); stream labels 11.5px `#2c3e50`; status dot radius 6 (green `#27ae60` if on, red `#e74c3c` if off); note text 10.5px in the dot color, bold when off.
- **Caption (italic 11px `#666`, bottom center):** "Green streams also expire — default retention is short. Only an export you configured lasts."

## Official API References

- [AWS CloudTrail User Guide](https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-user-guide.html) — trails, event data stores, management vs data events
- [GCP Cloud Audit Logs](https://cloud.google.com/logging/docs/audit) — Admin Activity, Data Access, System Event and Policy Denied streams

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle`, `.verified` badge span, then one `.obj-table` (full width, border-collapse) with a single `<tr>`: left `<td>` (45%) holds two `.obj-title` blocks ("What you can get", "Watch out for" with `margin-top: 18px`) with bullet lists and a `.key-point` callout between them; right `<td>` (55%, text-align center) holds `.payload-note`, `<pre class="payload">`, and `<canvas id="c1" height="380">`. Below the table, an h2 "Official API References" with a link list.
- **Page CSS:** body system sans-serif, line-height 1.6, color `#2c3e50`, padding 30px 40px, white background; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` 0.8em `#888`, 1px `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 `#1a5276` 1.3em with 2px `#2980b9` bottom border; `.obj-title` bold `#1a5276` 1.1em; `.obj-table td` 16px padding, `1px solid #e0e0e0` border, vertical-align top; li 0.93em; links `#1a5276`; `.payload` `#f8f9fa` background, 3px `#1a5276` left border, monospace 0.78em, pre whitespace, left-aligned; `.payload-note` 0.82em `#666` italic left-aligned; `.key-point` `#f8f9fa` background, 3px `#1a5276` left border, padding 10px 14px, 0.93em; canvas block, `width: 100%`, margin 16px auto 0.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple.
- **Canvas:** responsive — width from `canvas.offsetWidth` (fallback 600), fixed 380px CSS height, backing store sized to rendered width × `window.devicePixelRatio` (display capped via `style.maxWidth`), `ctx.scale` back to logical coordinates, redrawn on window resize.
- Note: in regenerated HTML, any card/page links use `.html` extensions.
