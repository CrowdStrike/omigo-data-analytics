# Location Data / Cell Triangulation

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Location Data / Cell Triangulation — Domain Pitfalls

**Subtitle:** Precision illusions, privacy leakage, granularity mismatches, and the fundamental tension between location utility and individual privacy.

## Precision Varies 1000× By Method — Same Data Format

- **GPS:** ±2-5m outdoor and highest precision, but needs clear sky and fails indoors or in urban canyons.
- **WiFi positioning:** ±15-30m and works indoors, but depends on a known AP database that goes stale.
- **Cell tower triangulation:** ±100-500m urban and ±2-5km rural — always available, but very coarse.
- **The problem:** All three report as (lat, lng) with the same decimal places, so the format hides the method.
- **False precision:** 6 decimal places LOOK like 0.1m accuracy but may be ±2km if the fix came from a cell tower.
- **No metadata indicating method:** Most apps and databases store just the coordinates, with no source field.
- **Undecidable:** "40.7128, -74.0060" may be a GPS fix or triangulation anywhere in lower Manhattan.

**Impact:** Analysis assumes uniform precision. Geofencing (alert when user enters zone) triggers from ±500m away. "User was at the crime scene" might mean "user was within 2km of the crime scene." Precision uncertainty is INVISIBLE in the data.

### Visualization (canvas `c1`, 720×300)

Concentric dashed uncertainty circles around one point, one per positioning method.

- **Title (bold 17px `#1a5276`, centered):** "Same Coordinates — Wildly Different Actual Precision".
- **Center point:** dark `#333` 4px dot at (w/2, 110).
- **Circles (dashed, dash 4/3, width 2; radius = method radius × 0.8 px):** GPS ±5m green `#27ae60`; WiFi ±30m blue `#2980b9`; Cell Tower ±150m red `#e74c3c`.
- **Labels (17px, left-aligned at w/2+170, stacked):** "GPS (±5m → ±5m)", "WiFi (±30m → ±30m)", "Cell Tower (±150m → ±0.2km)" — each in its method color.
- **Bottom caption (bold 17px red, centered):** "All report same (lat,lng) format. Precision is INVISIBLE."

## Tower Handoff = Phantom Movement

- Phone on Tower A is reported at Tower A's position (±300m); handoff to a stronger Tower B jumps it 2km.
- The user never moved — the phone just switched towers, but the data reads "moved 2km in 5 seconds."
- **"Impossible travel" alerts in security:** Login from NYC, then "London" 10 minutes later, flagged as a breach.
- Real cause: a VPN reconnecting to a different exit node, or a tower that was geolocated incorrectly.
- **Oscillation:** A phone bouncing between two towers ping-pongs between points 1km apart, all day, at a desk.
- **Speed calculation from this:** "2km in 5 seconds = 1440 km/h" — supersonic on paper, stationary in reality.

**Impact:** Any analysis using location CHANGES (speed, distance, travel patterns) from cell data is contaminated by phantom movements from tower handoffs. Must filter physically impossible movements before any analysis.

### Visualization (canvas `c2`, 720×300)

Diagram: stationary user between two towers, with a dashed arrow showing the reported "jump".

- **Title (bold 17px `#1a5276`, centered):** "User Stationary — Data Shows 2km "Movement" (Tower Handoff)".
- **Tower A:** blue `#2980b9` filled triangle at x=150 (apex y=50, base y=130), labeled "Tower A".
- **Tower B:** red `#e74c3c` filled triangle at x=550, labeled "Tower B".
- **User:** dark `#333` 8px dot at (350,100), labeled "User (stationary)" above.
- **Reported movement:** dashed orange line (`#e67e22`, dash 5/3, width 2) from Tower A to Tower B at y=100.
- **Bottom caption (bold 17px orange `#e67e22`, centered):** `Data: "moved 2km in 5 seconds" = 1440 km/h`.

## Indoor / Vertical Ambiguity (Which Floor? Which Building?)

- Triangulation gives (lat, lng) — a 2D position. Floor 1 or floor 30? The parking garage below? The roof?
- The same (lat, lng) fits the target's office on floor 15, a coffee shop on floor 1, or a business on floor 8.
- **Geofence warrants:** "All phones in this building" returns everyone in the footprint, within about ±100m.
- **Swept in:** That footprint takes in adjacent buildings, the subway underneath, and the park across the street.
- **Shopping mall:** Triangulation says "in the mall" — not which store, nor first floor versus food court.
- Analytics needs store-level granularity; the data offers "somewhere in this 200m radius," a permanent gap.
- **Z-axis is essentially unmeasured:** Barometric altitude exists in some phones, rarely in location APIs.
- A 50-story building is therefore a single flat point on most maps, with every floor collapsed into it.

**Impact:** "User visited competitor's store" — actually user was in the same mall, different floor. Retail foot traffic analytics based on cell data over-counts by including everyone in the vertical column above/below the store.

### Visualization (canvas `c3`, 720×300)

Building cross-section with a single 2D location point spanning all floors.

- **Title (bold 17px `#1a5276`, centered):** "Same (lat,lng) — Floor 1? Floor 15? Parking Garage? Subway?".
- **Building:** light blue-gray `#f0f4f8` rectangle 200×140 at (250,40) with gray `#999` outline and 7 horizontal floor lines every 20px.
- **Floor labels (17px, left at x=460):** red `#e74c3c` "Floor 6: Target's office"; gray `#666`: "Floor 3: Coffee shop", "Floor 1: Lobby", "B1: Parking".
- **Location point:** red 6px dot at (350,90) with a vertical dashed red line (dash 3/3, width 1.5) running the full building height.
- **Bottom caption (bold 17px red, centered):** "Z-axis unmeasured. "In this building" = 7 possible locations."

## Two Points = Unique Identity (Home + Work De-Anonymization)

- Phone at location X from 10pm-7am nightly = HOME; at location Y from 9am-5pm weekdays = WORK.
- Home address plus work address is a unique identifier for 95% of the US population, with no name needed.
- **"Anonymized" location datasets:** Drop the name and phone number, keep the locations, keep the identity.
- The routine pattern alone still pins the person down, so removal of direct identifiers changes nothing.
- Research (MIT): 4 spatiotemporal points (places plus times) uniquely identify 95% of 1.5 million individuals.
- **Sold by data brokers:** "Anonymized" mobility data sells commercially; journalists keep re-identifying people.
- Named in reports: military personnel at bases, government officials, protesters, abortion clinic visitors.

**Impact:** There is NO meaningful anonymization of location traces. The pattern IS the identity. Removing device IDs doesn't help — the trajectory itself is a fingerprint. "Anonymized location data" is an oxymoron with sufficient temporal resolution.

### Visualization (canvas `c4`, 720×300)

Two labeled circles (home, work) joined by an arrow labeled as a unique identifier.

- **Title (bold 17px `#1a5276`, centered):** "2 Points (Home + Work) = Unique Identity for 95% of People".
- **Home circle:** radius 50 at (180,110), fill `rgba(41,128,185,0.2)`, stroke `#2980b9` width 2; bold blue "HOME" with "10pm-7am" below.
- **Work circle:** radius 50 at (520,110), fill `rgba(39,174,96,0.2)`, stroke `#27ae60` width 2; bold green "WORK" with "9am-5pm" below.
- **Connector:** red `#e74c3c` line (width 2) between circles; bold red label above: "= UNIQUE PERSON (95% of US pop)".
- **Bottom caption (17px `#555`, centered):** ""Anonymized" location data is an oxymoron."

## Urban Density Bias — Rural Populations Are Invisible

- **Urban:** 20 towers per km² gives triangulation precision of ±100m — good enough for block-level analysis.
- **Suburban:** 2-5 towers per km² gives ±500m precision, which is neighborhood-level at best.
- **Rural:** 1 tower per 10-50 km² gives ±2-5km — only useful for "somewhere in this county."
- Quality is INHERENTLY BETTER in wealthy urban areas with more infrastructure, worse in poor and rural ones.
- Any model using location features therefore inherits systematic bias along the same lines as infrastructure.
- **Coverage gaps:** Dead zones with no signal generate ZERO data points for the people living in them.
- Those people are invisible rather than "at home" — truly untrackable, and silently dropped from every count.

**Impact:** Mobility studies, traffic analysis, population density estimates — all biased toward urban populations. "Average travel distance" is measured more precisely for urban residents → appears different even if actual behavior is similar. Precision inequality creates analytical inequality.

### Visualization (canvas `c5`, 720×300)

Three area cards comparing tower density and resulting precision.

- **Title (bold 17px `#1a5276`, centered):** "Tower Density = Precision Inequality".
- **Cards (3 boxes 200×100 at y=50, x = 60 + i·230; fill at 20% alpha, 2px solid border in the area color; bold colored heading, then 17px `#333` lines):**
  - Urban green `#27ae60` — "20 towers/km²" / "Precision: ±100m"
  - Suburban orange `#e67e22` — "4 towers/km²" / "Precision: ±500m"
  - Rural red `#e74c3c` — "0.5 towers/km²" / "Precision: ±5km"
- **Bottom caption (bold 17px red, centered):** "Wealthy areas get better data quality. Systematic bias."

## Granularity Mismatch: Collection vs Purpose vs Consent

- **Collected at:** Every 30 seconds at GPS-level precision, for a weather app that "needs location" to forecast.
- **Consented for:** "Show me nearby restaurants" — a one-time, coarse lookup the user actually asked for.
- **Used for:** Sold to a data broker, then political campaign targeting, then a law enforcement geofence warrant.
- The granularity collected (continuous and precise) vastly exceeds what the stated purpose ever required.
- That excess granularity is what enables the downstream use cases the user never consented to.
- **Temporal granularity:** "Location access: always" reports every minute, 24/7, whether the app is open or not.
- 1440 data points/day × 365 days = 525,600 points/year PER PERSON, stored indefinitely by default.

**Impact:** Data minimization principle violated at collection. Downstream users have access to precision/frequency they shouldn't (and the user wouldn't consent to if asked clearly). "Do you want to share your exact location every 30 seconds with unknown third parties for years?" → nobody says yes. But that's what "Allow location: Always" means.

### Visualization (canvas `c6`, 720×300)

Three shrinking horizontal bars: collected vs consented vs actually needed.

- **Title (bold 17px `#1a5276`, centered):** "Collected vs Consented vs Actually Used".
- **Bars (at x=60, y = 50 + i·50, height 35, width = fraction × 600; fill at 30% alpha with 1.5px solid border; 17px `#333` label inside):**
  - "Collected: every 30s, GPS precision" — 95% width, red `#e74c3c`.
  - "Consented for: "nearby restaurants" (one-time, coarse)" — 40% width, orange `#e67e22`.
  - "Actually needed: city-level (±10km)" — 15% width, green `#27ae60`.
- **Bottom caption (bold 17px red, centered):** "Excess precision collected enables uses user never consented to."

## Stingray / IMSI Catchers — Fake Towers in Your Data

- Law enforcement and criminals deploy fake cell towers ("stingrays") that phones connect to automatically.
- A phone cannot tell a stingray from a real tower: it connects, reports location, and the data is captured.
- **In your location dataset:** Some "tower connections" are to surveillance devices, not real infrastructure.
- Those points come from towers that shouldn't exist in any inventory, and nothing marks them as different.
- Stingrays can force a downgrade from 4G/5G to 2G (no encryption) to intercept communications.
- So the location point doubles as a marker that the phone's security was compromised at that moment.
- **Mass surveillance at events:** A stingray at a protest or rally captures ALL phones within its range.
- Later, "who was at this protest?" gets answered from those fake-tower connection logs.

**Impact:** Data provenance uncertainty — you don't know if a location point came from legitimate infrastructure or a surveillance device. Analyses built on this data may include surveillance artifacts that contaminate mobility patterns.

### Visualization (canvas `c7`, 720×300)

Diagram: two real towers flanking a fake stingray tower that all nearby phones connect to.

- **Title (bold 17px `#1a5276`, centered):** "Fake Cell Tower (Stingray) — Phone Connects Automatically".
- **Real towers:** green `#27ae60` filled triangles at x=100 and x=600 (apex y=60, base y=130), each labeled "Real" (17px, centered).
- **Fake tower:** red `#e74c3c` filled triangle at x=350, labeled bold "STINGRAY".
- **Phones:** five dark `#333` 5px dots in a row at y=170 (x = 250 + i·50), each connected to the stingray by a dashed red line (dash 3/3, width 1.5).
- **Bottom caption (17px red, centered):** "Phone can't distinguish real from fake. Data provenance = uncertain."

## Geofence Warrant Over-Inclusion (Legal Misuse of Imprecise Data)

- A geofence warrant asks for "all device IDs within 100m of this location between 2-3pm."
- Cell triangulation is ±300m, so everyone within 400m is returned — a full city block of innocent people.
- **False inclusion:** The person on the 10th floor across the street, and the person in the subway below.
- Also swept in: a delivery driver passing through, and anyone on a distant tower geolocated with ±500m error.
- Suspect pool from one geofence: 200 devices. Actually at the crime scene: 3. False positive rate: 98.5%.
- All 200 are investigated anyway, since the warrant return carries no way to rank or exclude them.
- **search engine reports:** 11,554 geofence warrants received in 2020 (US), each returning dozens to hundreds.
- Most of those people have NOTHING to do with the investigation they were pulled into.

**Impact:** Location data precision is INSUFFICIENT for the legal standard it's being used at. "Beyond reasonable doubt" requires precision that cell triangulation cannot provide. But the data LOOKS precise (6 decimal places!) so courts accept it.

### Visualization (canvas `c8`, 720×300)

Bullseye diagram: small crime-scene circle inside a much larger dashed sweep radius dotted with innocent bystanders.

- **Title (bold 17px `#1a5276`, centered):** "Geofence Warrant: Request 100m → Get 400m (Due to Precision)".
- **Crime scene:** filled circle radius 20 at (350,110), fill `rgba(231,76,60,0.3)`, stroke `#e74c3c` width 2.
- **Actual sweep radius:** dashed red circle (dash 5/3, width 1.5) radius 80 around the same center.
- **Innocent people:** gray `#999` 4px dots at `[(290,80), (310,140), (380,70), (400,130), (270,110), (420,90), (330,160), (370,50)]`.
- **Legend (17px, left at x=460):** red "● Crime scene (actual)"; gray "● Innocent people swept in".
- **Bottom caption (bold 17px red, centered):** "200 devices returned. 3 relevant. False positive rate: 98.5%."

## Callout (philosophy box)

**The meta-problem:** Location data has a unique property — it's simultaneously the most USEFUL data type (every app wants it) and the most DANGEROUS for privacy (it uniquely identifies individuals, reveals political/religious/health activities, and can't be meaningfully anonymized). Every system that collects, stores, or analyzes location data must answer: "Is the precision we're collecting proportional to the purpose? And who else might access this?"

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then per pitfall an `<h2>` heading (1.4em `#1a5276` with 2px `#2980b9` bottom border) followed by a single-row `.obj-table`: left `<td>` (40%) holds `.obj-title` (repeating the pitfall name) + bullet list (some bullets with bold lead-ins) + a bold "Impact:" paragraph, right `<td>` (60%, centered) holds the canvas. Even table rows get background `#fafcfe`. A final `.philosophy` callout closes the page.
- **Callouts:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; ul 0.9em `#333`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** all canvases 720×300 intrinsic; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart fonts use 17px `-apple-system`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, grays `#333`/`#555`/`#666`/`#999`.
