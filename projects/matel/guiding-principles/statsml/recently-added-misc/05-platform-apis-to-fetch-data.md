# Platform APIs to Fetch Data

**Page type:** grid page (12 numbered sections, each with an underlined h2, a section note, and a nav-card grid; two callout boxes up top)
**HTML title tag:** Platform APIs to Fetch Data

**Subtitle:** A landscape view of what data can be pulled programmatically — content, metadata, telemetry, and audit logs — across consumer and enterprise platforms.

## Callout (philosophy box)

**Why this matters:** Knowing *what exists behind an API* shapes what analysis is even possible. Each platform exposes a different slice: some return user-authored artifacts (messages, files, posts), some return only derived aggregates, and some return rich audit trails while withholding the content itself. The access model matters as much as the schema — a documented endpoint gated behind an enterprise tier, a 90-day retention window, or an admin-consent scope is effectively a different dataset than the docs suggest.

## Callout (caveat box, yellow)

**Read this before relying on any entry:** API surfaces change faster than docs. Three failure modes recur — (1) *retention windows* silently truncate history, so a backfill returns less than a live stream would have captured; (2) *tier gating* means an endpoint documented as available requires a plan the org may not hold; (3) *aggregate-only* endpoints cannot be disaggregated after the fact, so a metric not collected at the needed granularity is permanently unavailable at that granularity. Verify against current vendor docs before designing a pipeline. Entries where no public API exists are called out rather than omitted, because that absence is itself a planning constraint.

## Section 1: Team Messaging & Communication

**Note:** Message bodies, threads, reactions, and channel membership. Retention limits and bot-membership scoping are the recurring constraints.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | MESSAGING | Slack | [platform-apis-fetch/01-slack.md](platform-apis-fetch/01-slack.md) | Messages, threads, reactions, files, channel history. Tiered rate limits; free plan truncates history. | messages, events-API, retention-limit |
| 2 | MESSAGING | Microsoft Teams | [platform-apis-fetch/02-microsoft-teams.md](platform-apis-fetch/02-microsoft-teams.md) | Channel and chat messages, meetings, call records via Graph. Protected APIs need justified access. | graph-API, call-records, protected |
| 3 | MESSAGING | Discord | [platform-apis-fetch/03-discord.md](platform-apis-fetch/03-discord.md) | Channel messages, threads, reactions, attachments. Bot tokens; Gateway for real-time events. | messages, bot-token, gateway |
| 4 | MEETINGS | Zoom | [platform-apis-fetch/04-zoom.md](platform-apis-fetch/04-zoom.md) | Meeting metadata, participants, join/leave times, cloud recordings, transcripts, webinar registrants. | recordings, transcripts, participants |
| 5 | MEETINGS | Cisco Webex | [platform-apis-fetch/05-webex.md](platform-apis-fetch/05-webex.md) | Messages, rooms, meetings, recordings, plus admin-scoped meeting quality and device telemetry. | meetings, quality-metrics, devices |
| 6 | MEETINGS | Google Meet | [platform-apis-fetch/06-google-meet.md](platform-apis-fetch/06-google-meet.md) | Conference records, participant sessions, recordings, transcripts. Separate Reports API audit events. | conference-records, transcripts |
| 7 | COMMS | Twilio | [platform-apis-fetch/07-twilio.md](platform-apis-fetch/07-twilio.md) | SMS/voice logs, call duration, delivery receipts, recordings. Message bodies subject to retention config. | SMS, call-logs, delivery |
| 8 | MESSAGING | WhatsApp Business | [platform-apis-fetch/08-whatsapp-business.md](platform-apis-fetch/08-whatsapp-business.md) | Send/receive messages, templates, media, delivery status. Business-initiated needs approved templates. | templates, business, cloud-API |
| 9 | MESSAGING | Telegram | [platform-apis-fetch/09-telegram.md](platform-apis-fetch/09-telegram.md) | Bot API for messages in bot-visible chats; client API broader. No access to arbitrary user history. | bot-API, channels |

## Section 2: Email & Calendar

**Note:** Message bodies, headers, threading, and scheduling. The richest behavioral signal here is often metadata, not content.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 10 | EMAIL | Gmail | [platform-apis-fetch/10-gmail.md](platform-apis-fetch/10-gmail.md) | Messages, threads, labels, attachments, history deltas. Restricted scopes need security assessment. | threads, labels, restricted-scope |
| 11 | EMAIL | Outlook / Exchange | [platform-apis-fetch/11-outlook-mail.md](platform-apis-fetch/11-outlook-mail.md) | Mail, folders, rules, categories via Graph. Delta queries for sync; app-only tenant-wide access. | graph-API, delta-sync, app-only |
| 12 | CALENDAR | Google Calendar | [platform-apis-fetch/12-google-calendar.md](platform-apis-fetch/12-google-calendar.md) | Events, attendees, RSVP status, recurrence, free/busy, working location. Push notifications supported. | events, attendees, free-busy |
| 13 | CALENDAR | Outlook Calendar | [platform-apis-fetch/13-outlook-calendar.md](platform-apis-fetch/13-outlook-calendar.md) | Events, rooms, attendee responses, findMeetingTimes. Room and equipment resource booking data. | events, rooms, scheduling |

## Section 3: Storage, Files & Documents

**Note:** File content, version history, sharing graphs, and access events. Revision trails often outlive the visible document.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 14 | STORAGE | Dropbox | [platform-apis-fetch/14-dropbox.md](platform-apis-fetch/14-dropbox.md) | Files, revisions, sharing links, team events log, file requests. Longpoll for change detection. | revisions, team-events, sharing |
| 15 | STORAGE | Google Drive | [platform-apis-fetch/15-google-drive.md](platform-apis-fetch/15-google-drive.md) | Files, folders, comments, revisions, permissions. Export Docs/Sheets; shared drive support. | files, revisions, export |
| 16 | STORAGE | Box | [platform-apis-fetch/16-box.md](platform-apis-fetch/16-box.md) | Files, custom metadata templates, tasks, legal holds, retention policies, enterprise events. | metadata, legal-hold, events |
| 17 | STORAGE | OneDrive / SharePoint | [platform-apis-fetch/17-onedrive-sharepoint.md](platform-apis-fetch/17-onedrive-sharepoint.md) | Drive items, versions, activities, site lists, permissions via Graph. Tenant-load-based throttling. | drive-items, activities, throttling |
| 18 | DOCUMENTS | Google Docs & Sheets | [platform-apis-fetch/18-google-docs-sheets.md](platform-apis-fetch/18-google-docs-sheets.md) | Structural document content, named ranges, formulas, cell formatting, batch updates. | structured-doc, formulas |
| 19 | DOCUMENTS | Microsoft Office / Excel | [platform-apis-fetch/19-microsoft-office.md](platform-apis-fetch/19-microsoft-office.md) | Workbook ranges, tables, charts, worksheet functions via Graph. OOXML parsing for offline files. | workbooks, ranges, OOXML |
| 20 | FORMS | Google Forms | [platform-apis-fetch/20-google-forms.md](platform-apis-fetch/20-google-forms.md) | Form structure, questions, and individual responses with timestamps. Watches for new submissions. | responses, survey |

## Section 4: Knowledge Bases & Wikis

**Note:** Page trees, block-level content, and edit history. Useful for org knowledge decay and authorship analysis.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 21 | WIKI | Confluence | [platform-apis-fetch/21-confluence.md](platform-apis-fetch/21-confluence.md) | Pages, spaces, versions, comments, labels, attachments. CQL search; page history diffs. | pages, versions, CQL |
| 22 | WIKI | Notion | [platform-apis-fetch/22-notion.md](platform-apis-fetch/22-notion.md) | Pages, databases, blocks, comments. Content as block trees; integration-scoped access. | blocks, databases |
| 23 | WIKI | SharePoint Lists | [platform-apis-fetch/23-sharepoint-lists.md](platform-apis-fetch/23-sharepoint-lists.md) | List items, columns, content types, version history. Effectively a queryable org database. | list-items, versions |

## Section 5: Work Tracking & Developer Platforms

**Note:** Tickets, code, reviews, and pipeline runs — plus the state-transition trails that reveal how work actually flowed.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 24 | TICKETS | Jira | [platform-apis-fetch/24-jira.md](platform-apis-fetch/24-jira.md) | Issues, changelogs, sprints, worklogs, custom fields. JQL search; transition history per issue. | issues, changelog, JQL |
| 25 | TICKETS | Asana & Monday | [platform-apis-fetch/25-asana-monday.md](platform-apis-fetch/25-asana-monday.md) | Tasks, projects, custom fields, dependencies, stories. Asana REST; Monday GraphQL with complexity budget. | tasks, GraphQL |
| 26 | ITSM | ServiceNow | [platform-apis-fetch/26-servicenow.md](platform-apis-fetch/26-servicenow.md) | Incidents, changes, CMDB records, SLA timers, audit history. Table API exposes most schemas. | incidents, CMDB, SLA |
| 27 | CODE | GitHub | [platform-apis-fetch/27-github.md](platform-apis-fetch/27-github.md) | Repos, commits, issues, PRs, reviews, Actions runs. REST + GraphQL; secondary abuse limits bite. | commits, PRs, GraphQL |
| 28 | CODE | GitLab & Bitbucket | [platform-apis-fetch/28-gitlab-bitbucket.md](platform-apis-fetch/28-gitlab-bitbucket.md) | Repos, MRs/PRs, pipelines, job logs, deployments, code review approvals. | merge-requests, pipelines |
| 29 | TELEMETRY | Datadog & Observability | [platform-apis-fetch/29-datadog-observability.md](platform-apis-fetch/29-datadog-observability.md) | Metrics timeseries, logs, traces, monitors, incidents, RUM sessions. Query-cost and retention tiers. | metrics, traces, RUM |

## Section 6: Identity, Access & Audit Logs

**Note:** No user content at all — but the highest-fidelity record of who did what, when, and from where.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 30 | IDENTITY | Okta | [platform-apis-fetch/30-okta.md](platform-apis-fetch/30-okta.md) | System log events, users, groups, app assignments, factors, sessions. Log retention is bounded. | system-log, SSO, MFA |
| 31 | IDENTITY | Microsoft Entra ID | [platform-apis-fetch/31-entra-id.md](platform-apis-fetch/31-entra-id.md) | Sign-in logs, audit logs, risk detections, conditional access, directory objects. Tier-gated retention. | sign-ins, risk, directory |
| 32 | IDENTITY | Google Workspace Admin | [platform-apis-fetch/32-google-workspace-admin.md](platform-apis-fetch/32-google-workspace-admin.md) | Reports API activity per app, usage reports, users, org units, devices, alert center events. | reports-API, usage, devices |
| 33 | AUDIT | Cloud Audit Logs | [platform-apis-fetch/33-cloud-audit-logs.md](platform-apis-fetch/33-cloud-audit-logs.md) | AWS CloudTrail, GCP Cloud Audit, Azure Activity. Control-plane and data-plane API call records. | cloudtrail, control-plane |

## Section 7: Finance, Expense & Commerce

**Note:** Transactions, line items, and receipts. Categorization is usually vendor-derived and should be treated as a model output, not ground truth.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 34 | EXPENSE | Expensify | [platform-apis-fetch/34-expensify.md](platform-apis-fetch/34-expensify.md) | Expense reports, individual expenses, receipt images, categories, policy violations, approval chains. | receipts, approvals, violations |
| 35 | PAYMENTS | Stripe | [platform-apis-fetch/35-stripe.md](platform-apis-fetch/35-stripe.md) | Charges, subscriptions, invoices, disputes, payouts, Radar risk scores. Full event log via webhooks. | charges, subscriptions, churn |
| 36 | BANKING | Plaid / Open Banking | [platform-apis-fetch/36-plaid-banking.md](platform-apis-fetch/36-plaid-banking.md) | Account balances, transactions, merchant enrichment, income, liabilities. Consent-scoped and revocable. | transactions, enrichment, consent |
| 37 | ACCOUNTING | QuickBooks & NetSuite | [platform-apis-fetch/37-quickbooks-netsuite.md](platform-apis-fetch/37-quickbooks-netsuite.md) | Invoices, bills, journal entries, chart of accounts, vendors, purchase orders. | invoices, general-ledger |
| 38 | COMMERCE | Shopify | [platform-apis-fetch/38-shopify.md](platform-apis-fetch/38-shopify.md) | Orders, line items, customers, carts, fulfillment, refunds, product catalog. GraphQL cost-based limits. | orders, carts, fulfillment |
| 39 | COMMERCE | Amazon Selling & Ads | [platform-apis-fetch/39-amazon-selling-ads.md](platform-apis-fetch/39-amazon-selling-ads.md) | SP-API orders, inventory, settlements, fees, plus Ads API campaign and search-term reports. | SP-API, search-terms, fees |
| 40 | CRM | Salesforce & HubSpot | [platform-apis-fetch/40-salesforce-hubspot.md](platform-apis-fetch/40-salesforce-hubspot.md) | Accounts, contacts, opportunities, stage history, activity logs, email engagement, field audit trail. | pipeline, stage-history, SOQL |

## Section 8: Health, Fitness & Medical Devices

**Note:** The most granular personal data available. Note that on-device stores (HealthKit, Health Connect) have no cloud API — data leaves only through an installed app the user authorizes.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 41 | HEALTH | Apple HealthKit | [platform-apis-fetch/41-apple-healthkit.md](platform-apis-fetch/41-apple-healthkit.md) | Heart rate, HRV, steps, sleep stages, ECG, VO2max, blood oxygen, workouts. On-device only, no server API. | on-device, sleep-stages, ECG |
| 42 | HEALTH | Android Health Connect | [platform-apis-fetch/42-health-connect.md](platform-apis-fetch/42-health-connect.md) | Unified on-device store for steps, sleep, heart rate, nutrition. Replaced the deprecated Google Fit APIs. | on-device, fit-successor |
| 43 | WEARABLE | Fitbit | [platform-apis-fetch/43-fitbit.md](platform-apis-fetch/43-fitbit.md) | Intraday heart rate, sleep stages, steps, SpO2, activity. Intraday access needs special approval. | intraday, sleep, gated |
| 44 | WEARABLE | Oura & Whoop | [platform-apis-fetch/44-oura-whoop.md](platform-apis-fetch/44-oura-whoop.md) | Readiness/recovery scores, sleep architecture, HRV, temperature deviation, strain. Vendor-computed indices. | recovery, HRV, derived-score |
| 45 | WEARABLE | Garmin & Polar | [platform-apis-fetch/45-garmin-polar.md](platform-apis-fetch/45-garmin-polar.md) | Activity files with GPS tracks, per-second HR, power, cadence, training load. Partner-program gated. | FIT-files, GPS, partner-only |
| 46 | MEDICAL | CGM / Glucose Monitors | [platform-apis-fetch/46-cgm-glucose.md](platform-apis-fetch/46-cgm-glucose.md) | Dexcom and Libre interstitial glucose at ~5-min intervals, trends, calibration events. Delayed feeds. | glucose, 5-min-interval, regulated |
| 47 | DEVICE | Smart Scales & BP Cuffs | [platform-apis-fetch/47-smart-scales-bp.md](platform-apis-fetch/47-smart-scales-bp.md) | Withings and similar — weight, body composition estimates, blood pressure, multi-user disambiguation. | weight, bio-impedance, multi-user |
| 48 | DEVICE | Bluetooth Fitness Machines | [platform-apis-fetch/48-bluetooth-fitness-machines.md](platform-apis-fetch/48-bluetooth-fitness-machines.md) | FTMS, Cycling Power, and HR GATT profiles — live power, cadence, resistance, speed from treadmills and bikes. | FTMS, GATT, open-standard |
| 49 | FITNESS | Strava | [platform-apis-fetch/49-strava.md](platform-apis-fetch/49-strava.md) | Activities, GPS streams, segment efforts, kudos, social graph. Strict terms against bulk analysis. | streams, segments, TOS-limits |
| 50 | MEDICAL | EHR / FHIR | [platform-apis-fetch/50-ehr-fhir.md](platform-apis-fetch/50-ehr-fhir.md) | Epic, Cerner, and SMART-on-FHIR — labs, meds, conditions, encounters. Standardized resource model. | FHIR, labs, HIPAA |

## Section 9: Mobile OS, App Stores & Device Telemetry

**Note:** What the phone itself knows. Screen Time and Digital Wellbeing are deliberately locked down — aggregate developer reporting is available, per-user usage export generally is not.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 51 | OS | iOS Screen Time | [platform-apis-fetch/51-ios-screentime.md](platform-apis-fetch/51-ios-screentime.md) | Screen Time API gives on-device app usage to an authorized extension — opaque tokens, no data export. | DeviceActivity, opaque-tokens, no-export |
| 52 | OS | Android Usage Stats | [platform-apis-fetch/52-android-usage-stats.md](platform-apis-fetch/52-android-usage-stats.md) | UsageStatsManager foreground time and launch counts per package. Requires special user-granted access. | usage-stats, foreground-time |
| 53 | STORE | App Store Connect | [platform-apis-fetch/53-app-store-connect.md](platform-apis-fetch/53-app-store-connect.md) | Installs, sessions, crashes, retention cohorts, sales, subscription events, App Store impressions. | retention, subscriptions, aggregate |
| 54 | STORE | Google Play Console | [platform-apis-fetch/54-google-play-console.md](platform-apis-fetch/54-google-play-console.md) | Installs, uninstalls, ANRs, crashes, vitals, reviews, purchase and subscription state. | vitals, reviews, purchases |
| 55 | DEVICE | MDM / Device Management | [platform-apis-fetch/55-mdm-device-management.md](platform-apis-fetch/55-mdm-device-management.md) | Managed-device inventory, installed app lists, OS versions, compliance state, location for supervised devices. | inventory, compliance, supervised |
| 56 | ANALYTICS | Mobile Analytics SDKs | [platform-apis-fetch/56-mobile-analytics-sdks.md](platform-apis-fetch/56-mobile-analytics-sdks.md) | Firebase, Amplitude, Mixpanel — custom event streams, funnels, cohorts. You define the schema, so you own the gaps. | event-stream, funnels, BigQuery-export |

## Section 10: Maps, Location & Mobility

**Note:** Place data and routing are readily available; individual location history is largely not, and has been moving on-device.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 57 | MAPS | Google Maps Platform | [platform-apis-fetch/57-google-maps-platform.md](platform-apis-fetch/57-google-maps-platform.md) | Places, geocoding, routes, distance matrix, roads, aggregate Popular Times. Per-call pricing. | places, routing, priced |
| 58 | LOCATION | Personal Location History | [platform-apis-fetch/58-location-history.md](platform-apis-fetch/58-location-history.md) | Google Timeline moved on-device; export via Takeout. Apple exposes no location-history API at all. | takeout, on-device-shift |
| 59 | MAPS | OSM, Mapbox & HERE | [platform-apis-fetch/59-osm-mapbox-here.md](platform-apis-fetch/59-osm-mapbox-here.md) | Full OSM planet extracts and edit history, Mapbox routing/tiles, HERE traffic flow and incidents. | bulk-extract, edit-history, traffic |
| 60 | TRANSIT | Transit GTFS | [platform-apis-fetch/60-transit-gtfs.md](platform-apis-fetch/60-transit-gtfs.md) | Static schedules plus GTFS-Realtime vehicle positions, trip updates, service alerts. Open by convention. | GTFS-RT, vehicle-positions |

## Section 11: AR, VR & Spatial Computing

**Note:** The newest and most restricted category. Sensor-level access — eye tracking, room geometry, hand pose — is deliberately mediated by the OS for privacy reasons.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 61 | VR | Meta Quest | [platform-apis-fetch/61-meta-quest.md](platform-apis-fetch/61-meta-quest.md) | Platform SDK for entitlements, achievements, sessions; on-device hand/eye/scene data stays local to the app. | scene-mesh, hand-tracking, local-only |
| 62 | XR | Apple Vision Pro | [platform-apis-fetch/62-apple-vision-pro.md](platform-apis-fetch/62-apple-vision-pro.md) | ARKit scene reconstruction, hand anchors, plane detection. Raw gaze is withheld — only resolved input is exposed. | ARKit, no-raw-gaze, visionOS |
| 63 | WEARABLE | Smart Glasses | [platform-apis-fetch/63-smart-glasses.md](platform-apis-fetch/63-smart-glasses.md) | Ray-Ban Meta exposes no third-party data API; capture syncs to a companion app. Device access is narrow. | no-public-API, companion-app |
| 64 | AR | ARCore & ARKit Mobile | [platform-apis-fetch/64-arcore-arkit-mobile.md](platform-apis-fetch/64-arcore-arkit-mobile.md) | Depth maps, plane anchors, light estimation, face mesh, body pose, geospatial anchors. On-device frames. | depth, face-mesh, geospatial |

## Section 12: Social, Media & Content Platforms

**Note:** Once the most open category, now the most restricted. Several research-grade endpoints here were removed or repriced, which breaks longitudinal work built on them.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 65 | SOCIAL | Twitter / X | [platform-apis-fetch/65-twitter-x.md](platform-apis-fetch/65-twitter-x.md) | Tweets, threads, entity annotations, media keys. Free research access largely eliminated; tiered pricing. | tweets, v2-API, repriced |
| 66 | SOCIAL | Reddit | [platform-apis-fetch/66-reddit.md](platform-apis-fetch/66-reddit.md) | Submissions, comment trees, subreddit metadata. Pushshift gone — historical bulk access effectively closed. | comment-trees, pushshift-gone |
| 67 | VIDEO | YouTube | [platform-apis-fetch/67-youtube.md](platform-apis-fetch/67-youtube.md) | Video metadata, captions, playlists, comments, plus channel-owner analytics. Quota-unit accounting. | captions, quota-units, analytics |
| 68 | SOCIAL | Meta Graph & Instagram | [platform-apis-fetch/68-meta-graph.md](platform-apis-fetch/68-meta-graph.md) | Page and IG business insights, media, comments, reach, follower demographics. App Review required. | insights, app-review, business-only |
| 69 | VIDEO | TikTok | [platform-apis-fetch/69-tiktok.md](platform-apis-fetch/69-tiktok.md) | Display API for own content; Research API for vetted academics; Business API for ad and creative metrics. | research-API, vetted-access |
| 70 | SOCIAL | LinkedIn | [platform-apis-fetch/70-linkedin.md](platform-apis-fetch/70-linkedin.md) | Member profile under narrow scopes, org page shares and follower stats, ad reporting. No people search. | org-shares, partner-gated |
| 71 | MEDIA | Spotify | [platform-apis-fetch/71-spotify.md](platform-apis-fetch/71-spotify.md) | Playlists, saved tracks, recently played, top items. Algorithmic mixes invisible to the API. | playlists, recently-played |
| 72 | VIDEO | Twitch | [platform-apis-fetch/72-twitch.md](platform-apis-fetch/72-twitch.md) | Streams, clips, chat via IRC/EventSub, channel points, subscriber and viewership stats. | chat, EventSub, clips |
| 73 | WRITING | Medium & Substack | [platform-apis-fetch/73-medium-substack.md](platform-apis-fetch/73-medium-substack.md) | Medium is publish-only with no read API; Substack has no official API. RSS is the practical surface. | publish-only, RSS-fallback |
| 74 | SEARCH | Web Search APIs | [platform-apis-fetch/74-web-search-apis.md](platform-apis-fetch/74-web-search-apis.md) | Programmable Search, Bing (retired into Azure), Brave. Search Console gives own-site queries and impressions. | search-console, impressions |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`) with numbered sections. Page order: h1, `.subtitle`, `.philosophy` callout, `.caveat` callout, then per section: `<h2>N. Section Title</h2>`, `.sec-note` paragraph, `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(auto-fill, minmax(300px, 1fr))`, 14px gap.
- **Links:** the tables above link to `.md` versions; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead (subfolder `platform-apis-fetch/`).
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:BRAND_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index, numbered continuously 1-74 across sections), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` per topic.
- **Category label colors (per-card brand colors):** 1 `#4a154b`, 2 `#6264a7`, 3 `#5865f2`, 4 `#2d8cff`, 5 `#00bceb`, 6 `#00832d`, 7 `#f22f46`, 8 `#25d366`, 9 `#229ed9`, 10 `#ea4335`, 11 `#0078d4`, 12 `#4285f4`, 13 `#0078d4`, 14 `#0061ff`, 15 `#4285f4`, 16 `#0061d5`, 17 `#0078d4`, 18 `#0f9d58`, 19 `#d83b01`, 20 `#7248b9`, 21 `#172b4d`, 22 `#000`, 23 `#038387`, 24 `#0052cc`, 25 `#f06a6a`, 26 `#62d84e`, 27 `#333`, 28 `#fc6d26`, 29 `#632ca6`, 30 `#007dc1`, 31 `#0078d4`, 32 `#4285f4`, 33 `#ff9900`, 34 `#0b1b34`, 35 `#635bff`, 36 `#000`, 37 `#2ca01c`, 38 `#96bf48`, 39 `#ff9900`, 40 `#00a1e0`, 41 `#ff2d55`, 42 `#3ddc84`, 43 `#00b0b9`, 44 `#8e44ad`, 45 `#007cc3`, 46 `#e74c3c`, 47 `#16a085`, 48 `#2c3e50`, 49 `#fc4c02`, 50 `#c0392b`, 51 `#555`, 52 `#3ddc84`, 53 `#0d84ff`, 54 `#01875f`, 55 `#7f8c8d`, 56 `#f39c12`, 57 `#34a853`, 58 `#e67e22`, 59 `#7ebc6f`, 60 `#2980b9`, 61 `#1877f2`, 62 `#333`, 63 `#000`, 64 `#4285f4`, 65 `#1da1f2`, 66 `#ff4500`, 67 `#ff0000`, 68 `#1877f2`, 69 `#000`, 70 `#0a66c2`, 71 `#1db954`, 72 `#9146ff`, 73 `#000`, 74 `#4285f4`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 16px, shadow `0 2px 4px rgba(0,0,0,0.05)`, display block; hover: border `#2980b9`, `translateY(-2px)`. Card-num 0.72em bold, letter-spacing 0.03em; h3 `#1a3a4a` 0.98em; description `#555` 0.83em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.69em `#666`.
- **Section headers:** h2 `#1a5276` 1.15em, bottom border `2px solid #2980b9`, padding-bottom 6px, margin `34px 0 6px 0`; `.sec-note` `#666` 0.86em.
- **Callout styles:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em, text `#222`. `.caveat` — background `#fef9e7`, left border `4px solid #f39c12`, padding 12px 16px, 0.88em, text `#222`. Italic emphasis (`<em>`) on "what exists behind an API", "retention windows", "tier gating", "aggregate-only".
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 30px 40px, line-height 1.6; h1 1.8em `#2980b9`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (card labels use brand-specific colors listed above). No canvases on this page; any added canvases use `window.devicePixelRatio` scaling.
