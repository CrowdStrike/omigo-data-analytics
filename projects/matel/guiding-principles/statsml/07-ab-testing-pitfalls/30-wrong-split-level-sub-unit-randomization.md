# Wrong Split Level (Sub-Unit Randomization)

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section)
**HTML title tag:** Wrong Split Level — A/B Testing Pitfalls

**Subtitle:** Design Flaw — Splitting at a granularity below the natural unit of independence. Outcomes within the cluster are correlated, so individual-level randomization violates independence.

## Section 1: The Problem

- The experiment randomizes at a level more granular than where independence actually lives. Sub-units within a cluster share fate — they are not independent observations.
- **Web:** Splitting by session/GUID/cookie when a single user has multiple devices and sessions. The same person gets both A and B. Their behavior is correlated across sessions — not independent.
- **Health:** Randomizing a hygiene intervention at the individual level when the disease is communicable within households. One treated person in the family reduces exposure for all family members. The household is the natural unit of independence.
- **Education:** Randomizing a teaching method per student within a classroom. Students interact, share notes, compare experiences — the classroom is the independent unit.

**Why it matters:** When sub-units within a cluster are correlated, your effective sample size is the number of clusters, not the number of sub-units. Treating N=5000 sessions as independent when they come from N=800 users inflates precision and produces false positives.

### Visualization (canvas `c1`, 720×340)

Diagram: one user circle fanning out to three device boxes assigned to different arms.

- **Title (bold 15px `#1a5276`, centered, y=20):** "One User — Three Sessions — Split Independently".
- **User node:** circle at (W/2, 110), radius 50, fill `rgba(26,82,118,0.1)`, stroke `#1a5276` width 3; bold 14px `#1a5276` centered label "User #4281".
- **Devices (boxes 120×50 at y=210, connected to the user by thin gray `#999` lines):**
  - x=180: "Phone (cookie A)" — group "Treatment", color `#27ae60`, fill `rgba(39,174,96,0.15)`.
  - x=360: "Laptop (cookie B)" — group "Control", color `#e74c3c`, fill `rgba(231,76,60,0.15)`.
  - x=540: "Tablet (cookie C)" — group "Treatment", color `#27ae60`, fill `rgba(39,174,96,0.15)`.
  - Each box: device label 12px `#333`, group name bold 13px in the group color.
- **Bottom warning (centered):** bold 14px `#e74c3c` "Same person in BOTH arms → independence violated" (y=300); 13px `#666` "Fix: split by user-id, not by cookie/session/device" (y=320).

## Section 2: Domain Examples

- **Web platforms:** User has phone + laptop + tablet. Cookie-based split assigns phone to treatment, laptop to control. Conversion is a user-level decision, not a device-level one. Split by user-id.
- **Epidemiology:** Testing hand-washing promotion to reduce diarrheal disease. Randomizing individuals within a village is meaningless — water source is shared, transmission is household-clustered. Randomize at village or household level.
- **Marketplace:** Splitting buyers individually for a new recommendation algorithm, but buyers in the same household share a delivery address and purchasing patterns. The household is the correlated unit.
- **Mobile apps:** Splitting by app install ID when a user reinstalls or has multiple accounts. The "independent" units are the same person.

**Correct approach:** Identify the level at which outcomes are independent. Randomize at that level. If you must randomize lower, use cluster-robust standard errors or mixed-effects models that account for the intra-cluster correlation (ICC).

**The tell:** Ask "can two units in different arms actually be the same entity, or share the same outcome?" If yes — your split level is too granular.

### Visualization (canvas `c2`, 720×340)

Side-by-side wrong-vs-correct household randomization diagram.

- **Title (bold 15px `#1a5276`, centered, y=20):** "Communicable Disease — Individual vs Household Randomization".
- **Left panel (Wrong):** header bold 13px `#e74c3c` at (180, 50): "✗ Wrong: Split by person". A dashed gray (`#999`, dash 5/3) household box 200×180 at (80,60), captioned "Household" (12px `#666`). Inside, four person circles (radius 20) at (130,110) T green, (230,110) C red, (130,190) C red, (230,190) T green — fill at 20% alpha of `#27ae60`/`#e74c3c`, stroke 2px, bold 14px letter "T"/"C" inside. Dashed orange (`#e67e22`, dash 3/2) transmission lines connect the members, with 11px `#e67e22` label "transmission" in the center.
- **Right panel (Correct):** header bold 13px `#27ae60` at (540, 50): "✓ Correct: Split by household". Two solid household boxes 100×120 at y=60: House A at x=430 stroked `#27ae60`, captioned "House A" / bold "(Treatment)", containing four small circles (radius 15) filled `rgba(39,174,96,0.25)` stroked `#27ae60`; House B at x=560 stroked `#1a5276`, captioned "House B" / bold "(Control)", circles filled `rgba(26,82,118,0.15)` stroked `#1a5276`.
- **Bottom notes (centered):** 13px `#333` "Entire household gets same assignment → no within-cluster contamination" (y=280); 12px `#666` "Effective N = number of households, not number of people" (y=300) and "Use ICC (intra-cluster correlation) to estimate design effect" (y=318).

## Section 3: Real Example: Seller Tool Tested on Buyers

- A large online marketplace built a tool that helped sellers write better product listings, but measured it by splitting buyers into A and B groups. The change lived on the seller side, so once a seller improved a listing, every buyer saw the improvement — including the control group.
- Both arms of the test were looking at the same upgraded listings, so the comparison showed almost no difference and the tool looked useless. The effect was real; it had just been smeared evenly across both groups.
- The right design is to split at the level where the change actually operates — divide sellers (or their listings) into A and B rather than buyers, so the two groups genuinely see different things.

### Visualization (canvas `c3`, 720×300)

Flow diagram: one seller box feeding both buyer groups via dashed red arrows.

- **Title (bold 16px `#2a2a2a`, centered, y=26):** "Seller-Side Change, Buyer-Side Split".
- **Seller box:** 260×54 centered at top (y=50), fill `rgba(39,174,96,0.15)`, stroke `#27ae60` width 2.5; bold 14px `#27ae60` "Seller improves listing" plus 14px "(this is where the change lives)".
- **Buyer group boxes (180×60 at y=170):** at x=110 "Buyer group A \"treatment\"" and x=430 "Buyer group B \"control\"", fill `rgba(26,82,118,0.1)`, stroke `#1a5276`, label bold 14px `#1a5276`; below the label, 14px `#e74c3c` "sees the improved listing". Dashed red (`#e74c3c`, dash 5/3, width 2) arrows with triangle heads run from the seller box to each group box.
- **Center annotation (bold 14px `#e74c3c`, between the group boxes):** "both arms see" / "the SAME change".
- **Verdict (bold 15px `#e74c3c`, centered, y=262):** "A vs B compares two identical experiences — the measured difference is ~zero".
- **Bottom line (15px `#333`, centered, y=H−14):** "Split sellers or listings, not buyers, so the arms actually differ".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table; section 1 is its own `.obj-table`, sections 2 and 3 share a second `.obj-table` (two rows); left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`; `ul` 0.9em `#333`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
