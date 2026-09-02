# Platform Privacy Policies — Collect, Use, Keep, Return

**Page type:** grid page (nav-card grid, 3 columns, cards with colored category label, numbered title, description, and topic tag pills)
**HTML title tag:** Platform Privacy Policies — Collect, Use, Keep, Return

**Subtitle:** What each platform archetype actually collects, how it uses it, how long it keeps it, and what an export gives back. Everyone knows "some data is collected" — almost nobody knows the extent. Each doc answers the same four questions and ends with the assumed-vs-actual gap.

## Callout (disclaimer box)

**Disclaimer:** Generalized, illustrative synthesis of common practices per platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## Cards

Each card links to a detail page under `platform-privacy-policies/`. The card shows a colored uppercase category label, a numbered title, a one-sentence description, and a row of topic tag pills.

| # | Category | Title | Link | Description | Topics |
|---|----------|-------|------|-------------|--------|
| 1 | CONSUMER | Social Network / Feed | [platform-privacy-policies/01-social-network.md](platform-privacy-policies/01-social-network.md) | The social graph, dwell time per post, drafts you never published — and interests inferred from all three. | social graph, dwell time, inferred interests |
| 2 | CONSUMER | Search Engine | [platform-privacy-policies/02-search-engine.md](platform-privacy-policies/02-search-engine.md) | Every query is a confession log — health worries, finances, intentions — tied to location and click trails. | query history, click trails, intent |
| 3 | CONSUMER | Messaging App | [platform-privacy-policies/03-messaging-app.md](platform-privacy-policies/03-messaging-app.md) | Content may be end-to-end encrypted, but metadata is not — who, when, how often, from where. | metadata, contact graph, encryption limits |
| 4 | CONSUMER | Maps / Ride-Hailing | [platform-privacy-policies/04-maps-ride-hailing.md](platform-privacy-policies/04-maps-ride-hailing.md) | Continuous location history reveals home, work, routines — and every deviation from them. | location trails, routines, place inference |
| 5 | CONSUMER | E-Commerce / Payments | [platform-privacy-policies/05-ecommerce-payments.md](platform-privacy-policies/05-ecommerce-payments.md) | Purchase history as a personality profile — life events, income band, price sensitivity. | purchase history, price sensitivity, life events |
| 6 | CONSUMER | Video Streaming | [platform-privacy-policies/06-video-streaming.md](platform-privacy-policies/06-video-streaming.md) | Watch time, pause points, abandonment — a taste and attention profile built from what you almost finished. | watch behavior, attention, taste profile |
| 7 | CONSUMER | Voice Assistant / Smart Speaker | [platform-privacy-policies/07-voice-assistant.md](platform-privacy-policies/07-voice-assistant.md) | Wake-word buffers, voice prints, and the ambient context of a home — sometimes reviewed by humans. | voice recordings, wake word, human review |
| 8 | CONSUMER | Wearable / Fitness | [platform-privacy-policies/08-wearable-fitness.md](platform-privacy-policies/08-wearable-fitness.md) | Heart rate, sleep, cycles — health data collected outside medical privacy law's protection. | health signals, sensors, regulatory gap |
| 9 | CONSUMER | Email / Productivity Suite | [platform-privacy-policies/09-email-productivity.md](platform-privacy-policies/09-email-productivity.md) | The archive of your life — attachment scanning, calendar patterns, and who you correspond with. | content scanning, calendar, correspondence graph |
| 10 | CONSUMER | Mobile OS / App Store | [platform-privacy-policies/10-mobile-os-app-store.md](platform-privacy-policies/10-mobile-os-app-store.md) | The collector under all other collectors — app usage, sensors, identifiers shared across every app. | device identifiers, app usage, sensors |
| 11 | CONSUMER | Browser | [platform-privacy-policies/11-browser.md](platform-privacy-policies/11-browser.md) | The observation point for everything not in an app — history, autofill, sync, and fingerprintable surface. | history, sync, fingerprinting |
| 12 | CONSUMER | Photo Storage | [platform-privacy-policies/12-photo-storage.md](platform-privacy-policies/12-photo-storage.md) | Face clustering, object detection, and location extracted from EXIF — indexes built from your camera roll. | face clustering, EXIF, scene detection |
| 13 | SECURITY | Endpoint Security | [platform-privacy-policies/13-endpoint-security.md](platform-privacy-policies/13-endpoint-security.md) | Process lists, files, browsing, USB activity — everything on the machine, personal use included, for an employee who never read the policy. | endpoint agent, workplace, personal use |
| 14 | SECURITY | Network Monitoring | [platform-privacy-policies/14-network-monitoring.md](platform-privacy-policies/14-network-monitoring.md) | Who talks to whom, when, how much — full traffic metadata for everyone on the network. | traffic metadata, flow logs, workplace |
| 15 | SECURITY | Firewall / Gateway | [platform-privacy-policies/15-firewall-gateway.md](platform-privacy-policies/15-firewall-gateway.md) | Every site visited — often with TLS inspection reading inside sessions users believe are encrypted. | web logs, TLS inspection, workplace |
| 16 | SECURITY | Identity Provider | [platform-privacy-policies/16-identity-provider.md](platform-privacy-policies/16-identity-provider.md) | Every login, location, device — a complete access history per person, kept for audit. | login history, device tracking, audit retention |
| 17 | SECURITY | Cloud Security | [platform-privacy-policies/17-cloud-security.md](platform-privacy-policies/17-cloud-security.md) | File contents, sharing patterns, admin actions — scanned across an organization's entire cloud footprint. | content scanning, sharing patterns, cloud footprint |
| 18 | CONSUMER | Always-On Voice Interfaces | [platform-privacy-policies/18-always-on-voice-interfaces.md](platform-privacy-policies/18-always-on-voice-interfaces.md) | Assistants enabled by default inside phones, earbuds, cars, and TVs — an always-processing mic attached to advanced AI, in places nobody chose to put one. | wake word, bystanders, OS-level AI |
| 19 | CONSUMER | Ad Tracking, Cookies & Third-Party Cookies | [platform-privacy-policies/19-ad-tracking-cookies.md](platform-privacy-policies/19-ad-tracking-cookies.md) | The tracking layer under the web itself — no signup, no product, no export. Third-party cookies, cookie syncing, fingerprinting, and identity graphs held by companies you've never heard of. | 3rd-party cookies, fingerprinting, identity graphs |
| 20 | CONSUMER | Desktop Operating System | [platform-privacy-policies/20-desktop-operating-system.md](platform-privacy-policies/20-desktop-operating-system.md) | Usage telemetry under every app — app launches and durations, shell search queries sent to the cloud as you type, crash dumps carrying document fragments, and an advertising ID on the desktop. | telemetry, crash dumps, shell search |
| 21 | CONSUMER | Smart TV | [platform-privacy-policies/21-smart-tv.md](platform-privacy-policies/21-smart-tv.md) | The screen watches back — automatic content recognition fingerprints whatever is displayed, including HDMI inputs, and viewing data became the business model of thin-margin hardware. | ACR, viewing data, cross-device ads |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** nav-grid style (see `docs/statsml/ui-templates/02-nav-grid`). Single page: h1, `.subtitle` paragraph, one `.disclaimer` callout, then one `.nav-grid` of `.nav-card` anchors.
- **Layout:** `.nav-grid` is CSS grid, `repeat(3, 1fr)`, 14px gap.
- **Links:** the table above links to the `.md` versions for navigation in markdown; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num" style="color:CATEGORY_COLOR">CATEGORY</div>`, `<h3>N. Title</h3>` (unpadded index number matching the file index), `<p>description</p>`, then `<div class="topics">` with one `<span class="topic-tag">` per topic.
- **Category label colors:** CONSUMER `#1a5276`; SECURITY `#e74c3c`.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 18px, shadow `0 2px 4px rgba(0,0,0,0.05)`; hover: border `#8e44ad`, `translateY(-2px)`. `.card-num` 0.72em bold; h3 `#1a3a4a` 1em; description `#555` 0.84em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, flex-wrap row with 4px gap.
- **Callout style:** `.disclaimer` — background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`.
- **Page style:** body -apple-system/Segoe UI/Roboto sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; h1 1.8em `#8e44ad`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple accent `#8e44ad`. This page has no canvases; site canvases in general use `window.devicePixelRatio` scaling.
