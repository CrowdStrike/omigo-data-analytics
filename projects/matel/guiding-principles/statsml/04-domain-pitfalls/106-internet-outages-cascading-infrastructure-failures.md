# Internet Outages / Cascading Infrastructure Failures

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one h2 + one-row table per pitfall)
**HTML title tag:** 106. Internet Outages / Cascading Infrastructure Failures

**Subtitle:** Shared infrastructure makes failures correlated, fast, and self-hiding — the monitoring dies with the system, and the mitigations (retries, updates, caches) amplify the damage.

## One Cloud Region Takes Down Half the Internet

**Shared Dependency = Correlated Failure**

- **The blast radius:** One region failure takes thousands of companies' services offline simultaneously.
- **What goes dark:** Streaming, chat apps, smart doorbells, and robot vacuums all stop at the same moment.
- **Broken assumption:** Models treating failures between services as "independent" are wrong.
- **Shared substrate:** Every one of those services sits on the same underlying infrastructure.
- **The lesson:** Correlated failure from a shared dependency, not many independent events.

### Visualization (canvas `canvas1`, 720×200)

Hub-and-spoke dependency diagram.

- **Title (17px `#1a5276` at 10,20):** "Shared Dependency = Correlated Failure".
- **Hub:** filled red `#e74c3c` circle radius 35 at (360,110) with white 12px label "us-east-1".
- **Spokes:** 8 dependent services placed on an ellipse (radius 90 horizontal, 75 vertical) starting at top: "streaming service", "chat app", "streaming", "smart home", "robot vacuum", "payment processor", "PaaS provider", "hosting platform". Each is a light-red `#f5b7b1` filled circle radius 18 with red `#e74c3c` 1px outline and 10px `#333` label, connected to the hub by a red width-2 line.
- **Annotation (17px red at 550,190):** "ALL fail simultaneously".

## EDR Vendor BSOD (July 2024) — 8.5M Machines Simultaneously

**One Bad Push = 8.5M Bricks at Once**

- **The event:** A faulty update was pushed to ALL Windows endpoints at once, not to a staged subset.
- **The scale:** That single update put 8.5M machines into a boot loop at essentially the same moment.
- **No remote fix:** A machine that can't boot can't receive the fix, so recovery is manual per machine.
- **The impact:** Airlines grounded, hospitals dark, banks offline — the failure crossed every sector.
- **The irony:** The "reliability" of automated updates is a single point of failure at global scale.

### Visualization (canvas `canvas2`, 720×200)

Timeline contrasting instant failure with slow manual recovery.

- **Title (17px `#1a5276` at 10,20):** "Simultaneous Global Brick: 8.5M Machines".
- **Timeline:** blue `#2980b9` width-2 horizontal line at y=100 from x=50 to 680.
- **Event marker:** red `#e74c3c` filled circle radius 10 at (100,100), labeled 13px `#333` "Update pushed" at (70,130).
- **Failure spike:** translucent red band `rgba(231,76,60,0.3)` rect (120,40) 30×120; red width-3 line rising from (100,80) through machine counts 0 → 1M → 4M → 7M → 8.5M (x steps of 40, y = 80 − (count/8.5M)*50).
- **Recovery line:** green `#27ae60` width-2 nearly-flat line from (260,30) drifting slowly down-right over ~440px, labeled in green 12px "Manual recovery (days/weeks)" at (350,55).
- **Bottom annotation (14px red at 200,180):** "Failure: instant | Recovery: manual per machine".

## BGP Misconfiguration Propagates Globally in Seconds

**One Wrong Route Advertisement Black-Holes Worldwide Traffic**

- **The weakness:** BGP (the routing protocol) has no authentication on the routes it accepts.
- **Black hole:** One ISP advertising a wrong route pulls the internet's traffic into a dead end.
- **Real case:** Pakistan trying to block a video platform (2008) blocked it globally for 2 hours.
- **The speed:** A misconfiguration in one country reaches worldwide impact in under 60 seconds.
- **Too fast for humans:** No operator can detect, diagnose, and withdraw a route in that window.

### Visualization (canvas `canvas3`, 720×200)

Network graph with expanding propagation rings.

- **Title (17px `#1a5276` at 10,20):** "BGP Misroute Propagation Speed".
- **Nodes:** 30 pseudo-randomly placed 5px dots (deterministic formula: x = 50 + (i*21 + (i%7)*13) mod 620, y = 50 + (i*17 + (i%5)*11) mod 120); node 0 pinned at (450,90) as the source, labeled 11px `#333` "Source" at (460,80). Node pairs closer than 120px connected by faint `#ddd` 0.5px lines.
- **Propagation rings:** 4 concentric circles around (450,90) at radii 60/120/180/240, stroked `rgba(231,76,60, 0.6 − r*0.12)` width 2.
- **Node coloring:** nodes within distance 240 of the source filled red `#e74c3c` (affected); farther nodes green `#27ae60`.
- **Annotation (14px red at 220,190):** "< 60 seconds to global propagation".

## DNS Failures Are Invisible — Everything Just "Doesn't Work"

**One Root Cause, Fifty Different Symptoms**

- **No error message:** DNS down means the browser only says "cannot resolve" — no cause is named.
- **Misplaced blame:** Users blame their WiFi, the website, or their own computer instead of DNS.
- **Symptom scatter:** EVERY service fails simultaneously, but each one fails with DIFFERENT symptoms.
- **Debugging blocked:** That scatter makes the shared cause invisible to anyone who isn't an expert.
- **The data trap:** User reports describe 50 different problems; the root cause is ONE DNS failure.

### Visualization (canvas `canvas4`, 720×200)

Fan diagram: one root cause node feeding many symptom boxes.

- **Title (17px `#1a5276` at 10,20):** "One Root Cause, Many Symptoms".
- **Root node:** red `#e74c3c` filled circle radius 25 at (360,170) with white 11px label "DNS".
- **Symptom boxes:** 7 light-red `#f5b7b1` rects (80×25) along y=40, x = 80 + i*90, each connected to the root by a faint `#bbb` 1px line. Labels (10px `#333`): "WiFi broken", "Site down", "App crashed", "Can't login", "Timeout", "502 error", "No internet" (each in quotes).
- **Middle annotation (14px `#333` at 120,120):** "Users report 50 different problems. Actual problem: 1 DNS failure."

## Cascading Retry Storms Amplify the Problem

**The Retry Designed for Resilience CAUSES the Failure**

- **The setup:** Service A calls a slow Service B and retries 3x, which looks harmless in isolation.
- **The multiplier:** 1000 instances of A retry at once, so B gets 3000x normal load and crashes.
- **The feedback loop:** B's errors make A retry MORE aggressively, feeding the failure it reacts to.
- **The growth curve:** Load climbs 1x → 3x → 9x → 27x within seconds of the first slow response.
- **The lesson:** Exponential amplification from a mechanism built for reliability.

### Visualization (canvas `canvas5`, 720×200)

Exponential load-growth line chart.

- **Title (17px `#1a5276` at 10,20):** "Retry Storm: Exponential Load Amplification".
- **Series:** red `#e74c3c` width-3 line through points at x = 80/180/280/380/480 with load values 1x, 3x, 9x, 27x, 81x (y = 160 − min(load*1.5, 130)); 5px red dots at each point with 13px `#333` value labels ("1x" … "81x") above.
- **Time labels (12px `#333`, y=180):** t=0, t=1s, t=2s, t=3s, t=4s under the points.
- **Capacity line:** dashed green `#27ae60` (dash 4/4, width 2) horizontal at y=145 from x=60 to 600, labeled in green 12px "Service B capacity" at (520,140).
- **Annotation (14px red at 480,80):** "Each retry multiplies load 3x".

## Observability Dies WITH the System It Monitors

**"No Alerts" Might Mean "Alerting Is Dead"**

- **The paradox:** The monitoring service runs on the same infrastructure that it is monitoring.
- **Co-death:** Cloud provider goes down, CloudWatch goes down, and you can't see that the cloud is down.
- **The trap:** "No alerts" does not mean "no problem" — the alerting system IS the broken thing.
- **Silence reads as health:** A dashboard with nothing red looks identical to one that stopped receiving data.
- **The regress:** You need monitoring of your monitoring — but where does that run?

### Visualization (canvas `canvas6`, 720×200)

Diagram of dead monitoring inside dead infrastructure vs a green dashboard.

- **Title (17px `#1a5276` at 10,20):** "Monitoring Paradox".
- **Left box:** blue `#2980b9` 2px stroked rect (100,50) 250×120 labeled 14px `#333` "cloud provider Infrastructure" (140,75). Inside: light-red `#f5b7b1` rect (130,90) 190×50 with red 13px labels "CloudWatch (monitoring)" (140,115) and "ALSO DOWN" (185,132). A big red X (two 4px `#e74c3c` diagonal lines) crosses the whole left box.
- **Right box:** green `#27ae60` 2px stroked rect (450,60) 200×80 with green 14px label "Dashboard: "All Green"" (465,85) and 12px `#333` lines "(no alerts = alerting is dead)" (465,110), "Not: everything is healthy" (465,128).
- **Bottom annotation (17px red at 320,190):** "No alerts ≠ No problems".

## Partial Failure Is Worse Than Complete Failure

**"99% Success Rate — Healthy!" While Some Users Are 100% Broken**

- **The contrast:** Complete failure is obvious and gets an immediate coordinated response.
- **The messier case:** In partial failure some requests succeed, some fail, and some are merely slow.
- **Shifting target:** 10% of users are affected — but a different 10% of them each minute.
- **Hidden in aggregates:** Monitoring reports a "99% success rate" and the page is marked healthy.
- **What the 1% is:** Those users are COMPLETELY unable to use the service, not slightly degraded.

### Visualization (canvas `canvas7`, 720×200)

User grid plus contrasting aggregate/affected banners.

- **Title (17px `#1a5276` at 10,20):** "Partial Failure Hides in Aggregates".
- **Grid:** 10×10 grid of 16px cells (2px gaps) starting at (50,45); cells at fixed indices 3, 12, 27, 34, 48, 56, 61, 73, 85, 94 filled red `#e74c3c` (affected), all others green `#27ae60`. Caption 12px `#333` at (50,195): "100 users (red = affected)".
- **Banners:** green `#27ae60` rect (350,50) 300×40 with white 16px text "Aggregate: 99% Success Rate"; red `#e74c3c` rect (350,110) 300×40 with white text "Affected Users: 100% Failure".
- **Bottom annotation (14px `#333` at 300,180):** "Monitoring says "healthy" while 10% are completely broken".

## DNS Poisoning / Cache Corruption

**Your Server Sees Zero Traffic — Because It All Went Elsewhere**

- **The mechanism:** An attacker or a bug corrupts a DNS cache, so users receive the wrong IP address.
- **How long it lasts:** Traffic goes to the wrong server for the whole TTL duration, minutes to hours.
- **Invisible internally:** From your server's perspective nobody is connecting, so nothing looks wrong.
- **Wrong conclusion:** A quiet traffic graph reads as low demand, not as traffic that went elsewhere.
- **Detection:** Requires EXTERNAL probing of your own domain, not internal monitoring.

### Visualization (canvas `canvas8`, 720×200)

Traffic-flow diagram showing misdirected requests.

- **Title (17px `#1a5276` at 10,20):** "DNS Cache Poisoning: Traffic Misdirection".
- **Boxes:** green `#27ae60` rect (500,40) 120×40 white label "Real Server"; red `#e74c3c` rect (500,120) 120×40 white label "Attacker Server"; orange `#f39c12` rect (250,80) 120×40 white label "DNS Cache" with red 10px "POISONED" beneath the label; blue `#2980b9` 13px text "Users" at (80,100).
- **Flows:** intended path Users→(dashed gray `#ccc` 1px from DNS cache to Real Server); actual path in red width 3: Users (120,100)→DNS Cache (250,100), then DNS Cache (370,105)→Attacker Server (500,140).
- **Bottom annotations (14px):** `#333` "Your server sees: zero traffic (looks fine internally)" at (100,185); red "Detection requires EXTERNAL probing" at (400,185).

## Malware Propagation Outruns Human Response

**By the Time You Detect It, the Network Is Already Compromised**

- **Ransomware worm (2017):** Spread laterally across networks by way of an SMB exploit.
- **How it moved:** An infected machine scans the local network and infects ALL vulnerable machines in seconds.
- **Destructive malware:** Arrived through an accounting software update, not through any network perimeter.
- **Maersk case:** That update destroyed 10,000+ machines at Maersk within a 10-minute window.
- **The asymmetry:** Propagation outruns human response, so detection arrives after compromise is total.

### Visualization (canvas `canvas9`, 720×200)

Machine grid with an infection wavefront.

- **Title (17px `#1a5276` at 10,20):** "Lateral Propagation Speed (ransomware worm/destructive malware)".
- **Grid:** 20 columns × 6 rows of 30×22 cells (2px gaps) starting at (40,45). Infection center at column 2, row 3; Manhattan distance ≤ 8 = infected. Cells at distance 6-8 (the wavefront) orange `#f39c12` ("spreading now"), distance ≤ 5 red `#e74c3c` (infected), beyond 8 green `#27ae60` (not yet reached).
- **Legend (11px `#333`, y≈190, 12px swatches):** red "Infected"; orange "Spreading now"; green "Not yet reached".
- **Annotation (13px red at 400,192):** "Maersk: 10,000+ machines in 10 minutes".

## Post-Mortem Data Reconstruction Is Forensic Archaeology

**The Most Critical 5 Minutes of Data = The 5 Minutes Where Collection Failed**

- **Missing logs:** After a major outage the logs sit ON the very machines that crashed.
- **No fallback copy:** The metrics service was down too, so nothing was shipped off those machines.
- **Unreliable time:** Timestamps can't be trusted because NTP failed alongside everything else.
- **Skew across services:** Clock skew makes events from different services impossible to correlate.
- **The result:** Reconstructing the incident timeline is forensic archaeology, not data analysis.

### Visualization (canvas `canvas10`, 720×200)

Timeline with a data-collection gap during the critical window.

- **Title (17px `#1a5276` at 10,20):** "Data Gaps During Critical Period".
- **Timeline:** blue `#2980b9` width-2 horizontal line at y=100 from x=50 to 680.
- **Bands (height 30, y=60, white 12px inset labels):** green `#27ae60` (50,60) 200 wide "Logs/Metrics available"; red `#e74c3c` (250,60) 180 wide "DATA COLLECTION FAILED"; green (430,60) 230 wide "Logs/Metrics available".
- **Critical window:** dashed red 2px rect (270,45) 140×55 (dash 4/4) labeled in red 13px "CRITICAL 5 MIN" above at (295,42).
- **Notes (13px `#333`, left column):** "Logs: on crashed machines" (50,135); "Timestamps: NTP failed (clock skew)" (50,155); "Correlation: impossible across services" (50,175).
- **Takeaway (14px red at 400,155):** "Forensic archaeology, not data analysis".

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table` layout — one `h2` per pitfall followed by a single-row table: left `<td>` (40%) with `.obj-title` + bullet list, right `<td>` (60%, centered) with one `<canvas width="720" height="300">` (the setup script draws at 720×200 logical size and fixes CSS size to 720×200). Even rows background `#fafcfe`; cell borders `1px solid #e0e0e0`, padding 20px 24px. Ten sections total.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px `#2980b9` bottom border; subtitle `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. `.philosophy` callout style defined but unused. No nav bar, no back/home links.
- **Canvas:** shared `setupCanvas(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), sets CSS size 720×200, `ctx.scale` back to logical coordinates, default font 17px system sans.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c` (light red `#f5b7b1` for affected boxes), orange `#f39c12`, gray `#333`/`#666`/`#bbb`/`#ddd`.
- Note: in regenerated HTML, any card/page links use `.html` extensions (this page has none).
