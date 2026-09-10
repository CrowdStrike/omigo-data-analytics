# Include Feature Without Checking Causal Direction

**Page type:** detail page (two card-sections, each an h2 + two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** Include Feature Without Checking Causal Direction

**Subtitle:** Collection calls "predict" default — but calls happen BECAUSE of default

## The Anti-Pattern

"Collection calls made" perfectly predicts loan default. It's a consequence, not a cause — the feature encodes the label.

**Key point (red left border):** A feature that is generated **after** the target event is not a predictor — it's a symptom. Including it guarantees leakage and inflated metrics.

*Other examples of effect-encodes-cause:*

- Treatment prescribed → predicting disease (treatment happens because of diagnosis)
- Support tickets opened → predicting churn (tickets opened because user is already leaving)

### Visualization (canvas `c1`, 720×300)

Timeline diagram: effect used as predictor, with a backward-pointing "prediction" arrow crossed out.

- **Background:** full-canvas `#fafafa` fill.
- **Title (bold 14px, top center, `#1a5276`):** "Causal Direction Violation: Effect Used as Predictor"
- **Timeline:** horizontal line `#2c3e50` width 2 at y=160 from x=60 to x=w-40 with filled right arrowhead; label "Time →" (12px `#666`) near the arrow end at y+25.
- **Event 1 (x=240):** orange circle radius 10, fill `#e67e22`, stroke `#d35400` width 2; above it bold 13px orange "DEFAULT" (y-22) and 11px `#666` "(target event)" (y-38); below, 11px `#666` "t = 0" (y+35).
- **Event 2 (x=480):** red circle radius 10, fill `#e74c3c`, stroke `#c0392b` width 2; above it bold 13px red "COLLECTION CALLS" and 11px `#666` "(consequence)"; below "t = +30 days".
- **Backward arrow:** dashed red (`#e74c3c`, dash 6/4, width 2.5) at y = timeline+65 from the calls circle leftward to the default circle, with a red left-pointing arrowhead; bold 12px red label centered above it: "Feature \"predicts\" target (but happened AFTER!)"; bold 20px red "✗" mark centered below the arrow (y+28).
- **Bottom note (12px `#888`, centered):** "Correlation ≠ Causation — temporal order matters"

## The Design Pattern

Timeline audit: for every feature, draw WHEN it was generated relative to the target event. Rule: if feature_timestamp ≥ target_event_timestamp → it's leakage, remove.

**Key point (red left border):** Automate: `assert max(feature_timestamps) < min(prediction_timestamps)` per entity.

- List all features with their generation timestamps
- Draw a prediction point on the timeline
- Any feature generated at or after prediction point → remove
- Enforce programmatically in pipeline with timestamp assertions

### Visualization (canvas `c2`, 720×300)

Timeline diagram: features placed relative to a prediction point, safe zone left, leakage zone right.

- **Background:** full-canvas `#fafafa` fill.
- **Title (bold 14px, top center, `#1a5276`):** "Timeline Audit: Features Must Precede Prediction Point"
- **Timeline:** horizontal line `#2c3e50` width 2 at y=150 from x=40 to x=w-40 with filled right arrowhead.
- **Prediction point:** vertical dashed green line (`#27ae60`, dash 8/5, width 2.5) at x=370 from y=50 to h-40; bold 13px green label "PREDICTION POINT" centered above at y=46.
- **Valid features (left of line):** green dots (radius 7, `#27ae60`) at x=120, 200, 290 on the timeline, labels 12px `#2c3e50` above ("income", "balance", "tenure"), bold 16px green "✓" below each (y+32).
- **Leaked features (right of line):** red dots (radius 7, `#e74c3c`) at x=460, 545, 630, labels above ("calls", "late_fee", "closed"), bold 16px red "✗" below each.
- **Zone labels:** centered under each half at y+60/y+76 — green bold 12px "✓ SAFE ZONE" with 11px "feature_time < prediction_time"; red bold 12px "✗ LEAKAGE ZONE" with 11px "feature_time ≥ prediction_time".
- **Rule box (bottom):** rectangle x=140, width 440, height 28 near y=h-30, fill `#f0f7f0`, stroke `#27ae60` width 1, containing bold 12px `#1a5276` centered text: "Rule: feature_time < prediction_time".

## Regeneration instructions

- **Layout:** two `.card-section` blocks ("The Anti-Pattern", "The Design Pattern"), each with an `h2` and a `table.layout` (width 100%, border-collapse) containing one row: `td.text-col` (45%) with paragraph + `.key-point` + `.example` + `ul`, `td.viz-col` (55%) with the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; canvas `width: 100%`, 1px `#e0e0e0` border, 4px radius; `.key-point` background `#f8f9fa`, 3px red `#e74c3c` left border, padding 8px 12px, 0.9rem; `.example` italic `#555` 0.9rem; `ul` 0.92rem. The Design Pattern key point contains a `<code>` element and uses HTML entities `&ge;`/`&lt;` in the text. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvas:** intrinsic 720×300, scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; CSS width 100%. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card links use `.html` extensions.
