# Tracking Data Collection Methods

**Page type:** grid page (sectioned card navigation grid, 3 columns, with TOC and per-section blurbs)
**HTML title tag:** Tracking Data Collection Methods

**Subtitle:** Everyday devices produce data as a by-product of working. What each mechanism measures, and what it does not resolve.

## Callout (philosophy box)

**Why this matters:** Most of these records were created to make something work — route a request, bill a call, time a traffic signal. They get reused later as measurements of behaviour, and that reuse is where the statistical problems start: the unit of observation is usually a device or an account rather than a person, values are often model output rather than measurement, and coverage is rarely uniform across the places and people being compared.

## Table of Contents (boxed "Contents" list, anchor links to section ids)

1. Everyday Phone and Desktop Use (`#everyday-phone-and-desktop-use`)
2. Wearables and the Body (`#wearables-and-the-body`)
3. Location and Movement (`#location-and-movement`)
4. Home, Retail and Commerce (`#home-retail-and-commerce`)
5. Infrastructure and Networks (`#infrastructure-and-networks`)
6. Traffic Monitoring (`#traffic-monitoring`)
7. Workplace Measurement (`#workplace-measurement`)
8. Driver Assistance (`#driver-assistance`)
9. Self-Driving Perception (`#self-driving-perception`)

## Section: Everyday Phone and Desktop Use

**Blurb:** Mechanisms that run while someone uses an ordinary phone or browser.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | WEB | Server-Side Logging | [tracking/01-server-side-logging.md](tracking-data-collection-methods/01-server-side-logging.md) | Every request a web server answers is written to a log — source IP, path, timestamp, user agent. This is a byproduct of how HTTP works. | web, no-client-code, always-on |
| 2 | WEB | JavaScript Tags | [tracking/02-javascript-tags.md](tracking-data-collection-methods/02-javascript-tags.md) | Scripts embedded in a page that record interaction events the server never sees — clicks, scroll depth, field focus, time on page. | web, clicks, behavior |
| 3 | WEB | Analytics Platforms | [tracking/03-analytics-platforms.md](tracking-data-collection-methods/03-analytics-platforms.md) | Google Analytics, Mixpanel, Amplitude — hosted products that stitch individual events into sessions, funnels, and cohorts. | web, funnels, sessions |
| 4 | WEB | Email Tracking Pixels | [tracking/04-email-tracking-pixels.md](tracking-data-collection-methods/04-email-tracking-pixels.md) | Remote images in an email report the open time, client, and approximate location when the client fetches them. | email, open-rate, location |
| 5 | WEB | Mouse & Keyboard Activity | [tracking/05-mouse-keyboard-activity.md](tracking-data-collection-methods/05-mouse-keyboard-activity.md) | Input events serve as a presence signal — evidence that a human is at the screen rather than a script or an unattended tab. | web, attention, presence |
| 6 | HIGH-RESOLUTION | Session Replay | [tracking/06-session-replay.md](tracking-data-collection-methods/06-session-replay.md) | Tools like FullStory record DOM changes, scrolls, clicks, and field entries, then reconstruct a replayable video of the session. | web, replay, form-fields |
| 7 | ADVERTISING | Cookies | [tracking/07-cookies.md](tracking-data-collection-methods/07-cookies.md) | Small files a site stores in your browser to recognize return visits. Third-party cookies let one identifier be read across many sites. | web, identity, cross-site |
| 8 | ADVERTISING | Browser Fingerprinting | [tracking/08-browser-fingerprinting.md](tracking-data-collection-methods/08-browser-fingerprinting.md) | Identification without stored state — combining screen size, installed fonts, canvas rendering quirks, and dozens of other attributes into a stable ID. | web, stateless, no-consent |
| 9 | ADVERTISING | Ad Tracking Pixels | [tracking/09-ad-tracking-pixels.md](tracking-data-collection-methods/09-ad-tracking-pixels.md) | One-pixel images or script beacons loaded from Meta, Google, and others that report a page view or purchase back to the ad platform. | ads, conversion, cross-site |
| 10 | ADVERTISING | Cross-Device Tracking | [tracking/10-cross-device-tracking.md](tracking-data-collection-methods/10-cross-device-tracking.md) | Joining phone, laptop, tablet, and TV identifiers into one graph — deterministically via logins, probabilistically via shared IP and timing. | identity, ads, multi-device |
| 11 | LOW-VISIBILITY | Hidden Page Content | [tracking/11-hidden-page-content.md](tracking-data-collection-methods/11-hidden-page-content.md) | The delivered document is larger than the rendered page. Most of the difference is functional; some of it carries identifiers and context. | web, honeypot, watermark |
| 12 | LOW-VISIBILITY | Share Link Tracking | [tracking/12-share-link-tracking.md](tracking-data-collection-methods/12-share-link-tracking.md) | What looks like a plain video link is a short redirect that expands into a long URL of opaque parameters, carrying the sharer's identity to whoever opens it. | mobile, social, referral-graph |
| 13 | LOW-VISIBILITY | Clipboard Access | [tracking/13-clipboard-access.md](tracking-data-collection-methods/13-clipboard-access.md) | Apps can read the clipboard on launch. iOS 14 added a visible paste banner in 2020 — the reads were not new, the instrument that exposed them was. | mobile, low-visibility, sensitive |
| 14 | MOBILE | Mobile App SDKs | [tracking/14-mobile-app-sdks.md](tracking-data-collection-methods/14-mobile-app-sdks.md) | Third-party code libraries bundled into apps that report screen views, taps, and device attributes to analytics and ad platforms. | mobile, apps, third-party |
| 15 | MOBILE | Phone Permission Requests | [tracking/15-phone-permission-requests.md](tracking-data-collection-methods/15-phone-permission-requests.md) | Location, camera, and contacts permissions gate access at the OS level. Once granted, where that data travels is up to the app. | mobile, location, contacts |
| 16 | HIGH-RESOLUTION | Short Video Behavioral Tracking | [tracking/16-short-video-behavioral-tracking.md](tracking-data-collection-methods/16-short-video-behavioral-tracking.md) | Feed apps record watch duration per video, rewatch count, swipe velocity, and pause points — signals far finer than a like or a follow. | mobile, behavior, granular |
| 17 | HIGH-RESOLUTION | Third-Party Keyboard Apps | [tracking/17-third-party-keyboard-apps.md](tracking-data-collection-methods/17-third-party-keyboard-apps.md) | Keyboard apps process every keystroke to power prediction. What stays on-device and what is uploaded depends on settings most users never open. | mobile, text, on-device |
| 18 | HIGH-RESOLUTION | Search History | [tracking/18-search-history.md](tracking-data-collection-methods/18-search-history.md) | Account-linked query logs covering health, legal, and financial questions — a record of intent rather than of action. | web, intent, retention |
| 19 | HIGH-RESOLUTION | AI Chatbot Conversations | [tracking/19-ai-chatbot-conversations.md](tracking-data-collection-methods/19-ai-chatbot-conversations.md) | Conversation logs with ChatGPT, Claude, and Gemini. Users routinely disclose health, legal, and financial detail they would not put in a search box. | AI, conversations, sensitive |
| 20 | LOW-VISIBILITY | Shadow Profiles | [tracking/20-shadow-profiles.md](tracking-data-collection-methods/20-shadow-profiles.md) | Records assembled about people who never signed up, from other users' uploaded contact lists, photo tags, and invitations. | social, no-consent, non-users |
| 21 | AGGREGATION | Aggregation Granularity as a Proxy | [tracking/21-aggregation-granularity-proxy.md](tracking-data-collection-methods/21-aggregation-granularity-proxy.md) | Not a collection mechanism. Aggregating to a household clears the non-PII bar while leaving a group of two or three — anonymous by label, a close proxy in practice. | aggregation, k-anonymity, unit-of-analysis |
| 22 | AI CONTENT | Fingerprinting Generated Content | [tracking/22-fingerprinting-generated-content.md](tracking-data-collection-methods/22-fingerprinting-generated-content.md) | Images carry C2PA provenance metadata and pixel watermarks like SynthID. The same idea now applies to text, where the carrier is word choice itself. | AI, watermark, provenance |

## Section: Wearables and the Body

**Blurb:** Body-worn sensors — from fitness bands to continuous glucose monitors — eye tracking, and biometric matching, where most reported values are model output rather than measurement.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 23 | BODY | Fitness Wearables | [tracking/23-fitness-wearables.md](tracking-data-collection-methods/23-fitness-wearables.md) | Body-worn sensors sampling heart rate, sleep stage, step count, glucose (with a paired CGM), and location continuously, including overnight. | body, health, always-on |
| 24 | BODY | Sports Athlete Tracking | [tracking/24-sports-athlete-tracking.md](tracking-data-collection-methods/24-sports-athlete-tracking.md) | Each sport breaks a different measurement method, so each uses a different sensor. Speed and load are derived, not measured. | sensors, derived-metrics, sampling-rate |
| 25 | BODY | Sport-Specific Wearables | [tracking/25-sport-specific-wearables.md](tracking-data-collection-methods/25-sport-specific-wearables.md) | Where the sensor sits on the athlete — golf glove, shoulder pad, under-shirt vest, pitching sleeve, swim wrist — and what that spot can and cannot capture. | worn-sensors, sport-specific, measured-vs-derived |
| 26 | BODY | Prescribed Medical Monitors | [tracking/26-prescribed-medical-monitors.md](tracking-data-collection-methods/26-prescribed-medical-monitors.md) | Holter and patch ECGs, home sleep studies, ambulatory blood-pressure cuffs — clinician-prescribed recorders that sample a bounded window of a varying life. | medical, episode-bounded, clinical-grade |
| 27 | BIOMETRIC | Biometric Templates and Matching | [tracking/27-biometric-templates-matching.md](tracking-data-collection-methods/27-biometric-templates-matching.md) | No biometric system recognises anyone. It computes a similarity score and compares it to a threshold, so the decision is configuration. | biometrics, base-rate, irrevocable |
| 28 | BIOMETRIC | Genetic Relative Matching | [tracking/28-genetic-relative-matching.md](tracking-data-collection-methods/28-genetic-relative-matching.md) | A profile identifies relatives, not only its owner. Reach into a population grows far faster than enrollment in the database. | genetics, inference, consent |
| 29 | PERMANENT | DNA & Genetic Data | [tracking/29-dna-genetic-data.md](tracking-data-collection-methods/29-dna-genetic-data.md) | Consumer genetics data shared with research partners, licensed to pharma, and searched by police through relatives. DNA cannot be rotated like a password. | body, permanent, law-enforcement |
| 30 | BODY | Phone Motion Sensors | [tracking/30-phone-motion-sensors.md](tracking-data-collection-methods/30-phone-motion-sensors.md) | Gyroscope and accelerometer streams classify walking, driving, and posture. Research has also recovered typed text from them. | mobile, no-permission, behavior |
| 31 | BODY | Eye Tracking | [tracking/31-eye-tracking.md](tracking-data-collection-methods/31-eye-tracking.md) | Cameras estimating gaze position and dwell — which element was fixated, in what order, and for how long. | attention, VR, retail |
| 32 | HIGH-RESOLUTION | Face & Emotion Tracking | [tracking/32-face-emotion-tracking.md](tracking-data-collection-methods/32-face-emotion-tracking.md) | Models that match identity or estimate age, gender, and expression from a camera feed. The inferred attributes are estimates, not facts. | face, inference, retail |
| 33 | BODY | Head-Worn Cameras and Assistants | [tracking/33-head-worn-cameras-and-assistants.md](tracking-data-collection-methods/33-head-worn-cameras-and-assistants.md) | Eyewear with an outward-facing camera. Every other mechanism here records the device owner; this one records whoever is in front of it, and only the wearer has an account. | camera, bystanders, convenience-sample |
| 34 | BODY | Hand Tracking & Gesture Input | [tracking/34-hand-tracking-gesture-input.md](tracking-data-collection-methods/34-hand-tracking-gesture-input.md) | Cameras fit a skeleton to the hand and a classifier turns poses into commands — including holds of several seconds to confirm an action. How a person gestures is close to a signature. | gestures, classifier-output, behavioral-biometric |
| 35 | BODY | Tracked Motion Controllers | [tracking/35-tracked-motion-controllers.md](tracking-data-collection-methods/35-tracked-motion-controllers.md) | A handheld IMU that drifts, pinned to the room by cameras or beacons. The pose is a fusion estimate — motion-capture-grade hand trails, produced by a game accessory. | sensor-fusion, 6DoF, play-space |
| 36 | BODY | Fitness Game Peripherals | [tracking/36-fitness-game-peripherals.md](tracking-data-collection-methods/36-fitness-game-peripherals.md) | A strain-sensing ring and a leg strap turn exercise into a game — and build a workout history under game terms, where reps are threshold decisions and calories are model outputs. | exergaming, session-only, derived-metrics |

## Section: Location and Movement

**Blurb:** Positioning, ranging, and proximity — and why coverage is never uniform across places.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 37 | LOCATION | Indoor Positioning | [tracking/37-indoor-positioning.md](tracking-data-collection-methods/37-indoor-positioning.md) | Indoors a phone asks a service where it is instead of working it out alone, so the method that covers where people actually spend time is the one that leaves a record. | location, sensor-fusion, drift |
| 38 | PHYSICAL | Store WiFi & Bluetooth Beacons | [tracking/38-store-wifi-bluetooth-beacons.md](tracking-data-collection-methods/38-store-wifi-bluetooth-beacons.md) | Sensors in malls and stores detect nearby phone radios to measure foot traffic, dwell time, and aisle-level paths. | physical, retail, passive |
| 39 | PHYSICAL | WiFi Probe Requests | [tracking/39-wifi-probe-requests.md](tracking-data-collection-methods/39-wifi-probe-requests.md) | Phones periodically broadcast probe frames naming known networks. Any receiver in range can log those broadcasts. | physical, phone, passive |
| 40 | HIGH-RESOLUTION | WiFi Signals as Radar | [tracking/40-wifi-signals-radar.md](tracking-data-collection-methods/40-wifi-signals-radar.md) | Router radio waves reflect off people and objects. Analyzing the reflections detects motion, counts occupants, and in lab work tracks breathing. | home, through-wall, no-camera |
| 41 | PHYSICAL | WiFi Location Databases | [tracking/41-wifi-location-databases.md](tracking-data-collection-methods/41-wifi-location-databases.md) | Google and Apple hold large maps of router MAC addresses to coordinates, so a phone can fix its position by scanning nearby networks. | location, mapping, passive |
| 42 | PHYSICAL | Bluetooth Scanning | [tracking/42-bluetooth-scanning.md](tracking-data-collection-methods/42-bluetooth-scanning.md) | Phones, earbuds, and watches advertise BLE packets continuously. Venues run scanners to measure presence and movement. | physical, retail, broadcast |
| 43 | CROWD-SOURCED | Bluetooth Item Trackers | [tracking/43-bluetooth-item-trackers.md](tracking-data-collection-methods/43-bluetooth-item-trackers.md) | The tag has no GPS. Passing phones hear its broadcast and supply their own location, so coverage depends on how many people are nearby. | airtag, bluetooth, missing-not-at-random, threshold |
| 44 | PHYSICAL | QR Code Tracking | [tracking/44-qr-code-tracking.md](tracking-data-collection-methods/44-qr-code-tracking.md) | QR menus, payments, and check-ins each encode a unique target URL, so a scan links a device to a specific place and time. | physical, identity, location |
| 45 | LOW-VISIBILITY | Ultrasonic Beacons | [tracking/45-ultrasonic-beacons.md](tracking-data-collection-methods/45-ultrasonic-beacons.md) | Tones above the audible range embedded in ads. A phone microphone that hears one links the TV playing it to that phone. | audio, low-visibility, cross-device |
| 46 | HIGH-RESOLUTION | Connected Cars | [tracking/46-connected-cars.md](tracking-data-collection-methods/46-connected-cars.md) | Automakers record speed, braking, and routes. GM's sale of driving data to insurance-scoring brokers was documented in 2024. | vehicle, location, insurance |
| 47 | LOCATION | GPS Navigation Devices | [tracking/47-gps-navigation-devices.md](tracking-data-collection-methods/47-gps-navigation-devices.md) | A dashboard sat-nav computes its own position from satellite broadcasts, which are receive-only. Nothing leaves the car until the live-traffic connection uploads position and speed as probe data. | vehicle, location, probe-data |
| 48 | MOBILE | Rideshare & Delivery Apps | [tracking/48-rideshare-delivery-apps.md](tracking-data-collection-methods/48-rideshare-delivery-apps.md) | Routes, frequent destinations, timing, and spend. Background location permission extends collection beyond active trips. | mobile, location, routine |
| 49 | HIGH-RESOLUTION | In-Cabin Driver Monitoring | [tracking/49-in-cabin-driver-monitoring.md](tracking-data-collection-methods/49-in-cabin-driver-monitoring.md) | An infrared camera watches the driver's face whenever the car is on, turning gaze and eyelid movement into drowsiness and distraction states. It keeps the judgment, not the footage. | vehicle, attention, threshold |

## Section: Home, Retail and Commerce

**Blurb:** Home devices, loyalty and payment records, and platform event streams.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 50 | HIGH-RESOLUTION | Smart TV Content Recognition | [tracking/50-smart-tv-content-recognition.md](tracking-data-collection-methods/50-smart-tv-content-recognition.md) | TVs sample on-screen audio or video, match it against a reference database to identify the title, and report it to the manufacturer. | home, always-on, ACR |
| 51 | HIGH-RESOLUTION | Voice Assistant Wake Words | [tracking/51-voice-assistant-wake-words.md](tracking-data-collection-methods/51-voice-assistant-wake-words.md) | Devices buffer audio locally while waiting for a wake word. False triggers upload short clips of unintended speech to the vendor. | home, always-on, audio |
| 52 | IDENTIFICATION | Home Security Cameras | [tracking/52-home-security-cameras.md](tracking-data-collection-methods/52-home-security-cameras.md) | Doorbell cameras record the public sidewalk in front of a home. Footage is stored by Amazon or Google and can be requested by police. | home, video, neighborhood |
| 53 | HIGH-RESOLUTION | Smart Home Mapping | [tracking/53-smart-home-mapping.md](tracking-data-collection-methods/53-smart-home-mapping.md) | Robot vacuums build a floor plan to navigate. Thermostats infer occupancy. Door sensors log entry and exit times. | home, layout, presence |
| 54 | RETAIL | Loyalty Cards & Store Apps | [tracking/54-loyalty-cards-store-apps.md](tracking-data-collection-methods/54-loyalty-cards-store-apps.md) | Loyalty programs trade a discount for an identifier that ties every basket line back to one household. | retail, purchases, identity |
| 55 | FINANCIAL | Payment & Transaction Tracking | [tracking/55-payment-transaction-tracking.md](tracking-data-collection-methods/55-payment-transaction-tracking.md) | Every card swipe, wallet tap, and transfer is recorded for settlement. Networks also license aggregated spending panels. | financial, purchases, settlement |
| 56 | IDENTIFICATION | Store Facial Recognition | [tracking/56-store-facial-recognition.md](tracking-data-collection-methods/56-store-facial-recognition.md) | Cameras matching shoppers against a retailer watchlist. Matching requires processing every face in view, not only flagged ones. | retail, face, identity |
| 57 | COMMERCE | Two-Sided Marketplace Events | [tracking/57-two-sided-marketplace-events.md](tracking-data-collection-methods/57-two-sided-marketplace-events.md) | Search, view, and purchase events on a marketplace. The seller identity in each event is what separates a marketplace schema from a plain retail one. | commerce, payload, marketplace |
| 58 | COMMERCE | Visual Search Commerce Events | [tracking/58-visual-search-commerce-events.md](tracking-data-collection-methods/58-visual-search-commerce-events.md) | The tracked unit is an image, not a product listing, and a save is deferred intent. Conversion may land weeks later, off-platform. | commerce, payload, deferred-intent |
| 59 | COMMERCE | Shopify Merchant Platform | [tracking/59-shopify-merchant-platform.md](tracking-data-collection-methods/59-shopify-merchant-platform.md) | One event stream serves two collectors: the merchant sees its own store, Shopify sees the same event across every store on the platform. | commerce, payload, webhooks |
| 60 | HOME | Connected Appliance Telemetry | [tracking/60-connected-appliance-telemetry.md](tracking-data-collection-methods/60-connected-appliance-telemetry.md) | Appliances report status on a schedule the vendor sets. The reporting interval, not the appliance, decides how much detail the record holds. | home, sampling-interval, public-spec |
| 61 | FINANCIAL | Stock Trading & Order Flow | [tracking/61-stock-trading-order-flow.md](tracking-data-collection-methods/61-stock-trading-order-flow.md) | Retail orders are routed to wholesalers that fill them from inventory. The routing step is what creates the record. | financial, order-routing, execution-quality |

## Section: Infrastructure and Networks

**Blurb:** Records created because the network has to work.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 62 | INFRASTRUCTURE | Internet Provider Logging | [tracking/62-internet-provider-logging.md](tracking-data-collection-methods/62-internet-provider-logging.md) | Your provider resolves and routes your traffic, so it can log which domains you reach and when — independent of cookies or private browsing. | network, domain-level, retention |
| 63 | INFRASTRUCTURE | Network Device Fingerprinting | [tracking/63-network-device-fingerprinting.md](tracking-data-collection-methods/63-network-device-fingerprinting.md) | A device joining a network reveals its vendor, OS, and handshake quirks — used for asset inventory and fraud detection. | security, enterprise, fraud |
| 64 | INFRASTRUCTURE | Carrier Network Records | [tracking/64-carrier-network-records.md](tracking-data-collection-methods/64-carrier-network-records.md) | The call log on the handset is a copy. The network's record was created separately for billing, and the delete button does not reach it. | telecom, deletion, selection-bias |
| 65 | INFRASTRUCTURE | Chat & Messaging Metadata | [tracking/65-chat-messaging-metadata.md](tracking-data-collection-methods/65-chat-messaging-metadata.md) | End-to-end encryption protects content, not routing. Who talks to whom, how often, and in what groups remains visible. | messaging, social-graph, metadata |

## Section: Traffic Monitoring

**Blurb:** Roadside cameras that record passing vehicles as a by-product of managing traffic.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 66 | PUBLIC ROAD | Traffic Cameras | [tracking/66-traffic-cameras.md](tracking-data-collection-methods/66-traffic-cameras.md) | Cameras at intersections record continuous video to manage signal timing. Every vehicle and pedestrian in frame is part of that recording. | public-road, video, retention |
| 67 | PUBLIC ROAD | Red Light & Speed Cameras | [tracking/67-red-light-speed-cameras.md](tracking-data-collection-methods/67-red-light-speed-cameras.md) | An enforcement camera reads every plate that passes, not only the ones it acts on. A non-violation read still produces a timestamped record. | plates, selection-effect, metadata |
| 68 | PUBLIC ROAD | License Plate Readers (ALPR) | [tracking/68-license-plate-readers-alpr.md](tracking-data-collection-methods/68-license-plate-readers-alpr.md) | Pole cameras turn each passing vehicle into a searchable row; many systems keep only the row. Where the photo is dropped, so is the only way to check a misread plate. | plates, ocr-error, residential |

## Section: Workplace Measurement

**Blurb:** Tool logs reused as productivity measures.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 69 | WORKPLACE | Robotics & Warehouse Tracking | [tracking/69-robotics-warehouse-tracking.md](tracking-data-collection-methods/69-robotics-warehouse-tracking.md) | Fulfilment systems log each worker's pick rate, idle gaps, and movement between stations, then feed those metrics into performance management. | workplace, movement, performance |
| 70 | WORKPLACE | Version Control Activity Metrics | [tracking/70-version-control-activity-metrics.md](tracking-data-collection-methods/70-version-control-activity-metrics.md) | Commit history is an audit log of a tool. Author timestamps are client-supplied, and review and design work leaves no diff. | workplace, goodhart, unit-of-analysis |
| 71 | WORKPLACE | Ticket Workflow Metrics | [tracking/71-ticket-workflow-metrics.md](tracking-data-collection-methods/71-ticket-workflow-metrics.md) | Cycle time is the gap between two clicks. Batch updates compress it, so diligent teams record worse numbers than careless ones. | workplace, censoring, ordinal-scale |
| 72 | WORKPLACE | Badge Access Control | [tracking/72-badge-access-control.md](tracking-data-collection-methods/72-badge-access-control.md) | A reader decides one door. Two people on one badge produce one record, so the second is absent from the data while present in the building. | workplace, MNAR, goodhart |
| 73 | WORKPLACE | Workplace Video Monitoring | [tracking/73-workplace-video-monitoring.md](tracking-data-collection-methods/73-workplace-video-monitoring.md) | The camera records pixels. Headcount, dwell and compliance flags are model outputs whose error rate is rarely measured once live. | workplace, model-output, occlusion |
| 74 | WORKPLACE | Parking and Garage Access | [tracking/74-parking-garage-access.md](tracking-data-collection-methods/74-parking-garage-access.md) | A barrier logs what it decided. Occupancy from entries minus exits drifts, because each unpaired event is an offset that never self-corrects. | workplace, record-pairing, drift |
| 75 | WORKPLACE | Document Viewer Analytics | [tracking/75-document-viewer-analytics.md](tracking-data-collection-methods/75-document-viewer-analytics.md) | A wiki page and a shared document report who opened them — to the author. A view event is not a read, and once views are attributed, opening stops being neutral. | workplace, goodhart, MNAR |
| 76 | WORKPLACE | Workplace Input Activity Monitoring | [tracking/76-workplace-input-activity-monitoring.md](tracking-data-collection-methods/76-workplace-input-activity-monitoring.md) | An endpoint agent counts keystrokes and mouse events into an active-minutes score. Typing registers; reading, thinking, and meetings record as idle. | workplace, goodhart, threshold |

## Section: Driver Assistance

**Blurb:** Features that help a human driver with one bounded task — each is roughly one sensor plus one alert or nudge, and the driver stays responsible.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 77 | DRIVER ASSIST | Adaptive Cruise Control | [tracking/77-adaptive-cruise-control.md](tracking-data-collection-methods/77-adaptive-cruise-control.md) | A radar behind the front grille times radio echoes to hold a set gap to the car ahead. It reads closing speed directly from the echo, but stopped objects can be filtered away as roadside clutter. | vehicle, radar, filtered-returns |
| 78 | DRIVER ASSIST | Blind Spot & Cross-Traffic Alert | [tracking/78-blind-spot-cross-traffic-alert.md](tracking-data-collection-methods/78-blind-spot-cross-traffic-alert.md) | Small radars in the rear corners answer one yes/no question — is something in the zone the mirrors miss? The light in the mirror is a verdict, not a picture. | vehicle, radar, threshold |
| 79 | DRIVER ASSIST | Lane Departure & Lane Keeping | [tracking/79-lane-departure-lane-keeping.md](tracking-data-collection-methods/79-lane-departure-lane-keeping.md) | A camera behind the windshield finds the painted lines and nudges the wheel back toward the middle. The lane it steers to is a guess drawn from paint, and faded markings weaken it quietly. | vehicle, camera, model-output |
| 80 | DRIVER ASSIST | Reverse Parking Sensors & Camera | [tracking/80-reverse-parking-sensors-camera.md](tracking-data-collection-methods/80-reverse-parking-sensors-camera.md) | Bumper discs send out sound pulses too high to hear and time the echo; the beeps speed up as the gap shrinks. The beep carries distance only — the camera exists to answer what the object is. | vehicle, ultrasonic, near-field |
| 81 | DRIVER ASSIST | Automatic Emergency Braking | [tracking/81-automatic-emergency-braking.md](tracking-data-collection-methods/81-automatic-emergency-braking.md) | The one assist that acts on its own: if the seconds-to-impact estimate falls below a cutoff and the driver has not reacted, the car brakes. Every setting trades false alarms against missed stops. | vehicle, actuation, error-tradeoff |

## Section: Self-Driving Perception

**Blurb:** The fused sensor stack a car needs to drive itself — and what it records about everyone around it as a by-product.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 82 | SELF-DRIVING | Automotive Lidar | [tracking/82-automotive-lidar.md](tracking-data-collection-methods/82-automotive-lidar.md) | A laser times how long each light pulse takes to bounce back, building a 3-D dot picture of the street. The dots are genuine distance measurements, but they thin out with range and rain scatters them. | vehicle, laser, sparsity |
| 83 | SELF-DRIVING | Surround Cameras & Object Detection | [tracking/83-surround-cameras-object-detection.md](tracking-data-collection-methods/83-surround-cameras-object-detection.md) | Cameras around the body feed a model that draws boxes around cars, people, and signs. Each box is the model's opinion held with a confidence score, and a cutoff decides which opinions become objects. | vehicle, camera, classifier-output |
| 84 | SELF-DRIVING | Sensor Fusion & 3D Scene Reconstruction | [tracking/84-sensor-fusion-3d-scene-reconstruction.md](tracking-data-collection-methods/84-sensor-fusion-3d-scene-reconstruction.md) | The car merges camera, radar, and lidar into one live picture of its surroundings. When the sensors disagree, which one to believe is a built-in policy choice, not a measurement. | vehicle, fusion, estimate |
| 85 | SELF-DRIVING | Fleet Data Capture & Shadow Mode | [tracking/85-fleet-data-capture-shadow-mode.md](tracking-data-collection-methods/85-fleet-data-capture-shadow-mode.md) | Cars upload moments the maker flagged as interesting — recording streets, pedestrians, and other drivers who never opted in. What counts as interesting decides what the dataset sees. | vehicle, bystanders, selection-bias |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`) with sections. Page order: h1, `.subtitle`, `.philosophy` callout, `.toc` contents box, then per section: `.section-title` h2 (with `id` anchor), `.section-blurb` paragraph, `.grid` of `.card` anchors.
- **Layout:** `.grid` is CSS grid, `repeat(3, 1fr)`, 16px gap, margin `14px 0 30px 0`; responsive: 2 columns below 800px, 1 column below 500px.
- **Links:** the tables above link to `.md` versions; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (subfolder `tracking/`).
- **Card structure:** `<a class="card" href="...">` containing `<div class="card-label" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, then `<div class="topics">` holding one `<span class="topic-tag">` per topic listed in the Topics column.
- **Category label colors:** WEB `#2980b9`; HIGH-RESOLUTION, LOW-VISIBILITY and PERMANENT `#e74c3c`; ADVERTISING, AGGREGATION, COMMERCE and HOME `#e67e22`; MOBILE, BODY, CROWD-SOURCED and AI CONTENT `#8e44ad`; BIOMETRIC, PHYSICAL and RETAIL `#27ae60`; LOCATION and WORKPLACE `#16a085`; IDENTIFICATION, FINANCIAL, INFRASTRUCTURE and PUBLIC ROAD `#1a5276`.
- **Card style:** background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 16px; hover: shadow `0 4px 12px rgba(0,0,0,0.1)`, border `#2980b9`. Label 0.72em bold uppercase letter-spacing 0.5px, h3 `#1a5276` 1.0em, description 0.85em `#555`. Topic tags: background `#eef4f8`, border `1px solid #cdd`, radius 4px, padding 2px 6px, 0.7em `#555`, flex-wrapped with 4px gap.
- **Section headers:** `.section-title` 1.15em `#1a5276`, bottom border `2px solid #d6e4ee`, margin `34px 0 4px 0`; `.section-blurb` `#666` 0.88em.
- **TOC style:** `.toc` box background `#f8fafb`, border `1px solid #e0e0e0`, radius 8px, padding 14px 18px; heading "Contents" 0.8em uppercase `#1a5276`; ordered list, links `#2980b9` no underline (underline on hover).
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No canvases on this page; if any are added they use `window.devicePixelRatio` scaling.
