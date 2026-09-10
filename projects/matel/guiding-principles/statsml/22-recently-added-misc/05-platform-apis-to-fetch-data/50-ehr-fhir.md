# EHR / FHIR

**Page type:** detail page (two-column obj-table layout: text left 45%, payload + canvas right 55%, single Overview row, followed by an API-references list)
**HTML title tag:** EHR / FHIR — Platform APIs

**Subtitle:** Lets approved apps pull patients' medical records — labs, diagnoses, medications — from hospital systems in a standard format.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get** (section label, left column)

- Patient demographics and identifiers
- Lab results and vital signs
- Diagnoses and problem lists
- Prescriptions written (not whether the drug was actually taken)
- Bulk export of a whole patient population, with institutional approval

**Key-point callout (red left border):**

**A lab value exists because a doctor ordered a test.** The sicker the patient, the more measurements — so the record reflects the care process as much as the body, and missing data is never random. Models trained naively on EHR data learn hospital workflow, not physiology.

**Watch out for** (section label, left column)

- FHIR is a spec, not an endpoint — every hospital runs its own server with its own quirks, so what works at one site fails at the next
- Results carry two times: when the sample was taken vs when the result became known — indexing on the first leaks the future into models
- Diagnosis codes follow billing incentives and problem lists go stale — they are not clinical truth
- Access requires per-institution approval and patient-privacy compliance (HIPAA, IRB), not a developer sign-up

**Payload note (right column, inline-styled 0.85em `#555`):** **One lab result** — taken at 22:40, but not known until 03:12 the next day. Models must use the second clock.

Code block (`pre`), verbatim:

```
{
  "resourceType": "Observation",
  "status": "final",
  "code": { "coding": [{
    "system": "http://loinc.org",
    "code": "2160-0",
    "display": "Creatinine" }] },
  "subject": { "reference": "Patient/eXY7q" },
  "effectiveDateTime": "2026-05-18T22:40:00Z",
  "issued":            "2026-05-19T03:12:00Z",
  "valueQuantity": {
    "value": 2.41, "unit": "mg/dL"
  }
}
```

**Chart caption (above canvas):** **Measurements cluster during hospital stays** — how often a value is measured is itself a severity signal.

### Visualization (canvas `fhirMissingnessChart`, responsive width × 380)

Patient timeline over 90 days: an underlying (unobserved) serum-creatinine trajectory (dashed orange), sparse/dense observed points (blue dots) clustering inside a shaded inpatient-encounter band, a reference-limit line, and an observations-per-week bar strip below the main plot.

- **Data model (deterministic, computed):**
  - Underlying trajectory: `1.02 + 0.04*sin(d/9) + 2.05/(1+exp(-(d-43)/2.4)) - 1.62/(1+exp(-(d-54)/4.2))` mg/dL (stable baseline, acute rise on admission, partial recovery).
  - Inpatient encounter window: days 38–55.
  - Observation days: sparse outpatient every 17 days before day 38; daily during days 38–55, plus a second (+0.5 day) sample per day during days 41–50 (twice daily at peak acuity); sparse again every 15 days from day 64 to 90.
  - Observed values: trajectory value plus deterministic pseudo-noise (`sin`-hash jitter, amplitude ±0.07) so the picture is identical across redraws.
  - Upper limit of normal: 1.30 mg/dL.
  - Observations-per-week strip: counts of observations binned into 7-day weeks, bars scaled to the max weekly count.
- **Layout:** height 380; padding left 58, right 20, top 52, bottom 98. Main plot: x maps day 0–90; y maps 0.6–3.5 mg/dL. Below it, a 26px-tall per-week bar strip starting 28px under the plot.
- **Title (top center):** bold 13px `#1a5276` "Measurement frequency is a severity signal, not a nuisance"; italic 10px `#888` sub-line "Illustrative patient timeline — serum creatinine Observations over 90 days."
- **Encounter band:** days 38–55 filled `rgba(26,82,118,0.14)` with `rgba(26,82,118,0.35)` vertical boundary lines; centered bold 10px `#1a5276` label near the top: "inpatient encounter".
- **Gridlines:** `#e8e8e8` horizontal every 0.5 from 1.0 to 3.5, with `#555` 11px right-aligned tick labels (one decimal).
- **Underlying trajectory:** dashed (`[5,4]`) orange `#e67e22`, width 1.6, sampled every 0.5 day.
- **Reference limit:** dashed (`[7,4]`) red `#e74c3c`, width 1.4, horizontal at 1.30; 10px red left-aligned label above it: "upper limit of normal (1.3 mg/dL)".
- **Observed points:** 2.6px-radius filled `#1a5276` dots.
- **Axes:** `#2c3e50` L-shape; x ticks every 15 days labeled 0–90 in `#555` 11px; rotated y-axis title bold 11px `#1a5276` "Serum creatinine (mg/dL)".
- **Per-week strip:** orange `#e67e22` bars (width 68% of each week slot) above a `#2c3e50` baseline; bold 10px orange label above the strip: "observations per week"; bold 11px `#1a5276` centered label below: "Day of patient timeline".
- **Legend (bottom row, 10px, `#555` labels):** blue dot swatch "recorded Observation"; dashed orange line swatch "underlying trajectory (unobserved)"; dashed red line swatch "reference limit".
- Redraws on window resize.

## Official API References

- [HL7 FHIR R4 Specification](https://hl7.org/fhir/R4/) — the resource definitions (Patient, Observation, Condition, MedicationRequest) and REST API
- [SMART App Launch](https://hl7.org/fhir/smart-app-launch/) — the OAuth 2.0 authorization framework for FHIR, including Backend Services and system/ scopes

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" with a single-row `.obj-table` (left `<td>` 45%: section labels + bullet lists + one `.key-point` callout; right `<td>` 55%: payload note + `<pre>` FHIR JSON + chart note + `<canvas>`), then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline badge — background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.section-label` bold `#1a5276` block. Payload/chart notes are inline-styled 0.85em `#555` paragraphs. `li`/`p` 0.93em; links `#1a5276`; `code` background `#f4f4f4`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="fhirMissingnessChart" height="380">`, CSS `display:block; width:100%`; drawing code reads `getBoundingClientRect().width`, sets backing store to `rect.width * dpr` / `380 * dpr` using `window.devicePixelRatio`, fixes CSS height to 380px, `ctx.scale` back to logical coordinates, and re-renders on `resize`.
- **Palette:** primary blue `#1a5276`, green `#27ae60` (unused here), red `#e74c3c`, orange `#e67e22`, band fill `rgba(26,82,118,0.14)`; grid `#e8e8e8`; text `#555`/`#2c3e50`/`#888`.
- In regenerated HTML, any card/page links use `.html` extensions.
