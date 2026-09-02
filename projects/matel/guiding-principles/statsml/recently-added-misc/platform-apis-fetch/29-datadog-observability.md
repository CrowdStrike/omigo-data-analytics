# Datadog & Observability APIs

**Page type:** detail page (single-row two-column obj-table: text left 45%, payload + canvas right 55%; followed by a references list)
**HTML title tag:** Datadog & Observability APIs — Platform APIs

**Subtitle:** Query metrics, logs, traces, and alerts out of Datadog and similar system-monitoring platforms.

**Verified badge:** Last verified: August 2026

## What you can get

- Numeric time series — CPU, latency, request counts — sliced by tag
- Searchable log records
- Events: deploys, alerts, scaling actions
- Per-request timing traces (sampled, not complete)
- Monitor and SLO definitions, states, and history

**Key point (callout box):** What survives is decided **before you ever query**. Sampling, exclusion filters, and aggregation happen at ingest, and older data is rolled up into coarser buckets forever — so the same question about the same week returns a smoother, lower-peaked answer months later. If a number matters long-term, compute and store it yourself while the raw data still exists.

## Watch out for

- The newest few minutes are always incomplete — any query ending at "now" systematically undercounts
- Traces are sampled: span counts are not request counts, and errors are deliberately over-kept
- A log excluded at ingest is unrecoverable unless it was archived first
- Percentiles (p95, p99) cannot be recomputed from rolled-up data

## Payload (right column)

**Payload note (italic):** Sample metric query response (abbreviated).

```json
{
  "status": "ok",
  "query": "avg:system.cpu.user{env:prod}by{host}",
  "series": [
    {
      "metric": "system.cpu.user",
      "scope": "env:prod,host:i-0ab12cd34ef567890",
      "interval": 300,
      "pointlist": [
        [ 1755763200000, 21.4 ],
        [ 1755763500000, 24.9 ],
        [ 1755763800000, 78.1 ]
      ]
    }
  ]
}
```

**Second payload note (italic, below the payload):** `"interval": 300` was chosen by the backend, not requested. Widen the time range and it grows — quietly changing what "max" means.

### Visualization (canvas `c1`, responsive width × 380)

Layered bar chart of log-count per 5-minute bucket showing ingestion delay: a ghost bar for the eventually-settled count under a solid bar for what is visible now; the newest buckets are visibly still filling.

- **Title (bold 14px `#1a5276`, top center):** "Ingestion delay: the newest buckets are still filling".
- **Subtitle (italic 11px `#666`):** "illustrative shape — log events per time bucket, as a query sees them right now".
- **Data:** 18 buckets, newest on the right.
  - Settled counts: `[92, 88, 95, 101, 97, 90, 99, 104, 96, 93, 98, 100, 94, 97, 102, 99, 95, 98]`.
  - Visible fraction now: `[1,1,1,1,1,1,1,1,1,1,1,1,0.99,0.96,0.88,0.66,0.34,0.09]`; visible bar = settled × fraction.
- **Axes:** y from 0 to 120 with gridlines and labels every 30 (`#eee` lines, `#aaa` 9px labels, right-aligned at left edge); plot area: left 46, right w−16, top 62, bottom h−74; gray `#ccc` baseline along the bottom.
- **Bars:** ghost (eventual) fill `rgba(26,82,118,0.35)`; solid (visible-now) fill `#1a5276` when fraction ≥ 0.98, red `#e74c3c` when still filling, drawn at alpha 0.9.
- **Markers:** dashed (4/3) red `#e74c3c` vertical line at the right edge labeled "now" (bold 10px red, below baseline); solid green `#27ae60` vertical line at bucket 12 labeled "safe query horizon (now − lag)" (bold 10px green, right-aligned near the top); incomplete zone between them shaded `rgba(231,76,60,0.07)`.
- **Legend (10px, swatch + `#2c3e50` text):** `rgba(26,82,118,0.35)` "will eventually be indexed"; `#1a5276` "complete now"; `#e74c3c` "still filling — undercounts".
- **Caption (italic 11px `#666`, bottom center):** "A job aggregating up to \"now\" reports a drop that is an artefact of the pipeline, not the system."

## Official API References

- [Datadog API Reference](https://docs.datadoghq.com/api/latest/) — full v1/v2 endpoint reference
- [Metrics API](https://docs.datadoghq.com/api/latest/metrics/) — query, submission and metadata endpoints

## Regeneration instructions

- **Layout:** single-row `.obj-table` (full width, border-collapse): left `<td>` 45% with "What you can get" (`.obj-title` + `<ul>`), a `.key-point` callout, then "Watch out for" (`.obj-title` with `margin-top:18px` + `<ul>`); right `<td>` 55% (text-align center) with `.payload-note` (italic), a `<pre class="payload">` JSON block, a second `.payload-note` (with inline `<code>` for `"interval": 300`), and the canvas. After the table, an `h2` "Official API References" with a plain `<ul>` of external links. Verified badge is a `<span class="verified">`.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` badge — 0.8em `#888`, 1px solid `#ddd` border, inline-block, padding 2px 10px, radius 4px; h2 1.3em `#1a5276` with 2px solid `#2980b9` bottom border; `.obj-title` bold 1.1em `#1a5276`; table cell borders `1px solid #2980b9`, padding 16px; li 0.93em; links `#1a5276`.
- **Payload / key-point style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, white-space pre, left-aligned; `.payload-note` — 0.82em italic `#666`, left-aligned; `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** `<canvas id="c1" height="380">` with CSS `display:block; margin:16px auto 0; width:100%`; width taken from `canvas.offsetWidth` at draw time, backing store scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates), redrawn on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, ghost-bar fill `rgba(26,82,118,0.35)`. No nav bar, no back/home links.
