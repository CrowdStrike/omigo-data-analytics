# Cybersecurity Data Collection — By Domain

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas + sample-payload code block right 55%, one `.lang-section` per topic; h1 carries a BACKLOG status pill)
**HTML title tag:** Cybersecurity Data Collection — By Domain

**Subtitle:** Explicit, purpose-built data collection across security product categories — what they ingest, how they capture it, and what telemetry feeds detection and response.

**Intro callout:** Unlike behavioral tracking that piggybacks on user activity, security telemetry is collected deliberately — it is the product. Each product category has its own capture point and event shape, and detections usually require joining several of them.

## 1. The Security Telemetry Stack

Security products collect data deliberately and continuously. Unlike behavioral tracking that piggybacks on user activity, security telemetry is the product's primary function.

- **Always-on** — collection runs whether or not the user is active; threats don't wait for sessions
- **High volume, low signal** — billions of events per day, single-digit alerts worth acting on
- **Correlation across domains** — a detection often requires joining endpoint + network + identity logs

**Key point:** The statistical challenge: baselines must be built from noisy, non-stationary data — and anomaly thresholds must adapt per-entity (user, device, subnet) rather than globally.

### Visualization (canvas `c1`, 720×340)

Convergence diagram: five telemetry source boxes on the left feeding a central SIEM/XDR hub, output arrow to alerts.

- **Title (bold 16px, `#1a5276`, top center):** "Telemetry Sources → Correlation".
- **Source boxes (120×34 at x=40, fill = color + `22` alpha, 2px stroke, 13px centered label in color):** "Endpoint" `#2980b9` y=60; "Network" `#27ae60` y=114; "Identity" `#8e44ad` y=168; "Cloud" `#e67e22` y=222; "Firewall" `#e74c3c` y=276.
- **Hub:** circle r=50 at (w/2+80, h/2), fill `rgba(26,82,118,0.12)`, stroke `#1a5276` 2.5px; bold 13px label "SIEM / XDR" with 11px `#5a6875` sub-label "correlate & detect".
- **Arrows:** from each source box to the hub edge in the source color at `88` alpha (1.5px, filled triangular heads); output arrow from hub to the right in `#e74c3c` 2px labeled "alerts" (12px red, left-aligned).

## 2. Deep Dive: Endpoint Sensor

An endpoint sensor is not a simple log forwarder — it is a sophisticated application running at the kernel level that observes *everything* happening on the device in real time.

- **Process lifecycle** — every process spawn, parent-child relationship, command-line arguments, environment variables. Builds full execution trees.
- **File system activity** — file create/modify/delete/rename, file hash on write, access to sensitive paths (credential stores, SSH keys, system config).
- **Memory operations** — code injection into other processes, reflective DLL loading, shellcode execution, memory-mapped file abuse.
- **Network connections** — every outbound/inbound socket from every process, with PID attribution. DNS resolution mapped to the requesting binary.
- **Registry / config changes** — persistence mechanisms (startup keys, scheduled tasks, launchd plists), security policy changes.
- **User behavior** — logon type (interactive, remote, service), privilege escalation (sudo, UAC), credential access patterns.
- **Hardware events** — USB device insertion, Bluetooth pairing, peripheral enumeration.

**Key point:** The sensor sees the machine the way the OS kernel sees it — not what the user intended, but what actually executed. This is what makes fileless malware, living-off-the-land techniques, and in-memory attacks visible.

**Architecture:** Kernel hooks (ETW + minifilters on Windows, kprobes/eBPF on Linux, kext/EndpointSecurity on macOS) feed a user-space agent that buffers, deduplicates, and streams events to cloud. Local ML models make sub-second kill decisions; heavier models run server-side for correlation.

### Visualization (canvas `c3`, 720×420)

Concentric-rings diagram of observation depth plus a sensor-agent box observing all layers.

- **Title (bold 16px, `#1a5276`, top center):** "Endpoint Sensor — Depth of Observation".
- **Rings (centered at w/2−40, h/2+20; fill = color + `12` alpha, 2px stroke, bold 11px label in color):** "Kernel" r=46 `#e74c3c` (label at center); "Process / Memory" r=92 `#8e44ad`; "File System" r=138 `#27ae60`; "Network Sockets" r=178 `#e67e22` (outer-ring labels near each ring's top).
- **Sensor agent:** 90×60 box at right (x = w−130), fill `rgba(26,82,118,0.08)`, stroke `#1a5276` 2.5px, bold 12px two-line label "Sensor" / "Agent"; 12px `#5a6875` annotation above it: "sees every layer" / "simultaneously".
- **Connections:** dashed (4/3) 1.2px line from each ring's right edge to the agent, in ring color at `66` alpha.
- **Cloud arrow:** blue `#2980b9` 2px downward arrow below the agent with 11px label "→ cloud".

### Sample payload (`.sample-payload` code block): "Sample: Process Creation Event"

```json
{
  "event_type": "process_create",
  "timestamp": "2024-03-15T09:23:41.003Z",
  "device_id": "WKS-042",
  "process": {
    "name": "powershell.exe",
    "pid": 8412,
    "cmdline": "powershell -enc SQBFAFgA...",
    "hash_sha256": "a3f2b8c...",
    "parent": {
      "name": "excel.exe",
      "pid": 3201
    }
  },
  "user": "CORP\\jsmith",
  "detection": "encoded_powershell_from_office"
}
```

## 3. Deep Dive: Network Security as Man-in-the-Middle

Network security products position themselves as an **authorized man-in-the-middle** — all traffic passes through them, and they terminate, inspect, and re-establish connections on both sides.

- **TLS interception (SSL decrypt)** — device trusts a corporate CA. The proxy terminates the TLS session from the client, inspects cleartext, then opens a new TLS session to the destination. Full content visibility.
- **Inline proxy / Secure Web Gateway** — HTTP(S) requests routed through proxy (PAC file, WPAD, or forced via DNS/routing). Proxy sees full URL, headers, request/response body, and can block or modify in flight.
- **DNS sinkholing** — controlled DNS resolver returns bogus IPs for known-bad domains, or logs every resolution for later forensic query.
- **ZTNA / SASE tunnel** — all device traffic (not just browser) forced through a cloud tunnel. The provider sees every connection, applies policy per-app, per-user, per-posture.
- **Network TAP / span port (passive)** — copy of traffic without interception. Sees metadata and unencrypted payloads; blind to encrypted content unless combined with TLS decrypt.

**Key point:** With TLS decrypt deployed, the network sees the same cleartext the browser does — URLs, POST bodies, API payloads, file uploads. Without it, visibility drops to metadata: SNI, JA3/JA4 fingerprint, certificate fields, packet sizes, timing.

**What gets logged:**

- Full URL path and query parameters (with decrypt)
- File hashes of downloads and uploads
- Data loss prevention (DLP) matches — credit card patterns, PII regex, document classifiers
- Application identification — classify traffic as Slack, Dropbox, ChatGPT, etc. regardless of port
- User identity — tied via auth token, certificate, or agent enrollment
- Threat feed matches — known C2 domains, malware download URLs, phishing pages

### Visualization (canvas `c4`, 720×420)

Client–proxy–server MITM diagram with two TLS sessions and a "Proxy sees" callout.

- **Title (bold 16px, `#1a5276`, top center):** "Network Security — Authorized MITM".
- **Boxes on a horizontal midline (100×50, fill = color + `22` alpha, 2px stroke):** "Client" (sub "trusts corp CA") `#2980b9` at x=60; "Proxy" (subs "terminates TLS" / "inspects cleartext", box 20px taller, 2.5px stroke) `#e74c3c` at center; "Destination" `#27ae60` at right.
- **Arrows:** Client → Proxy in `#2980b9` labeled "TLS session 1" (11px, above); Proxy → Destination in `#27ae60` labeled "TLS session 2".
- **Callout box (below proxy, `#f8f9fa` fill, `#e74c3c` 1.5px border, connected by a dashed red line):** bold 11px red header "Proxy sees:" and 11px `#4a5866` bullet list: "Full URLs & query params", "POST bodies & uploads", "File downloads (hashed)", "DLP pattern matches".
- **Product names (11px `#5a6875`, right-aligned top-right):** "Zscaler, Netskope, Palo Alto Prisma" / "Squid, BlueCoat, Forcepoint".

### Sample payload (`.sample-payload` code block): "Sample: Proxy Connection Log"

```json
{
  "timestamp": "2024-03-15T14:05:22Z",
  "user": "jsmith@corp.com",
  "action": "allowed",
  "method": "POST",
  "url": "https://paste.ee/api/v1/submit",
  "category": "paste_site",
  "bytes_out": 248000,
  "tls_decrypted": true,
  "dlp_match": "credit_card_pattern",
  "ja4_fingerprint": "t13d1715h2...",
  "risk_score": 87
}
```

## 4. Deep Dive: Email Security

Email security products sit on the delivery path — either as a **gateway** (MX record points to them first) or as an **API integration** (post-delivery scan via Graph API / journaling).

- **Pre-delivery inspection** — sender reputation scoring (IP, domain age, volume patterns), SPF/DKIM/DMARC validation, header anomaly detection (forged reply-to, lookalike domains).
- **Content analysis** — attachment sandboxing (detonate in VM, observe behavior), macro detection, password-protected archive handling, embedded URL extraction and real-time crawl.
- **Link rewriting** — every URL replaced with a redirect through the vendor. Captures who clicked, when, from what device — even days after delivery.
- **BEC / impersonation detection** — tone analysis, display-name spoofing, VIP protection lists, unusual request patterns (wire transfer, credential sharing).
- **Post-delivery clawback** — retroactive removal of messages when a URL or hash is later classified malicious.

**Key point:** The gateway sees every inbound and outbound email in full — headers, body, attachments, recipient list. API-mode sees the same but after delivery, adding a race window.

**What gets logged:** sender/receiver, subject, attachment hashes, URL list, verdict (clean/spam/phish/malware), sandbox report, click events on rewritten links, user-reported phish submissions.

### Visualization (canvas `c5`, 720×380)

Pipeline diagram: six inspection stages with arrows, drop-off labels, and a post-delivery loop.

- **Title (bold 16px, `#1a5276`, top center):** "Email Security — Inspection Pipeline".
- **Stages (90×56 boxes, 20px gaps, centered row; fill = color + `20` alpha, 2px stroke, 11px two-line centered labels; each successive box slightly taller-tapered by 3px per index):** "Sender reputation" `#e67e22`; "SPF/DKIM /DMARC" `#2980b9`; "Content scan" `#8e44ad`; "Sandbox detonate" `#e74c3c`; "URL rewrite" `#27ae60`; "Deliver" `#1a5276`. Gray `#95a5a6` arrows between stages.
- **Drop-offs (11px `#e74c3c`, below stages):** "↓ blocked" under stage 1; "↓ quarantined" under stage 4.
- **Post-delivery:** dashed (4/3) green `#27ae60` 1.5px path from the "Deliver" stage down and left, with 11px green caption centered: "post-delivery: link click tracking, clawback".

### Sample payload (`.sample-payload` code block): "Sample: Email Verdict Event"

```json
{
  "message_id": "<abc123@attacker.com>",
  "timestamp": "2024-03-15T08:12:03Z",
  "from": "ceo-urgent@corp-secure.xyz",
  "to": "finance@corp.com",
  "subject": "Wire Transfer Needed ASAP",
  "verdict": "phish",
  "signals": {
    "spf": "fail",
    "dkim": "none",
    "domain_age_days": 3,
    "display_name_spoof": true,
    "url_count": 1,
    "attachment": "invoice.pdf.exe"
  },
  "action": "quarantined"
}
```

## 5. Deep Dive: Identity Threat Detection

Identity security monitors the **authentication and authorization plane** — who is proving they are who, what access they're requesting, and whether the pattern is consistent with prior behavior.

- **Authentication events** — every login attempt across all IdPs (Entra ID, Okta, Ping, on-prem AD). Success, failure, MFA method used, MFA bypass, token refresh.
- **Impossible travel** — login from New York at 10:00, then from Singapore at 10:15. Geolocation + timestamp analysis per user.
- **Privilege escalation** — role assignment changes, group membership modifications, service account creation, conditional access policy changes.
- **Token abuse** — OAuth token replay, refresh token theft, consent grant to malicious apps, token lifetime anomalies.
- **Lateral movement via credentials** — pass-the-hash, Kerberoasting, golden ticket detection via DC sensor (timestamp/encryption anomalies in TGT requests).
- **Service account monitoring** — interactive logon from a service account, access from unexpected hosts, credential rotation failures.

**Key point:** Identity is the new perimeter. A compromised credential looks legitimate from the network's perspective — only behavioral baselines on auth patterns reveal the anomaly.

**Data sources:** IdP audit logs, domain controller event logs (4624/4625/4768/4769), SAML/OIDC assertions, directory change feeds, conditional access evaluation logs.

### Visualization (canvas `c6`, 720×380)

Timeline scatter of auth events, normal vs anomalous, with an impossible-travel bracket.

- **Title (bold 16px, `#1a5276`, top center):** "Identity — Auth Event Signals".
- **Axis:** horizontal timeline at the plot bottom (plot x=60, y=60, width = canvas−120, height = canvas−130), `#95a5a6` 1.5px, labeled "Time →" (12px `#5a6875`).
- **Events (dots with dashed drop-lines to axis; normal = green `#27ae60` r=6, alert = red `#e74c3c` r=8; fill = color + `44` alpha, 2px stroke; 10px two-line labels above each dot):**
  - t=0.05, y=0.3: "Login / NY" (normal)
  - t=0.15, y=0.4: "MFA / pass" (normal)
  - t=0.30, y=0.25: "Token / refresh" (normal)
  - t=0.42, y=0.7: "Login / Singapore" (alert)
  - t=0.55, y=0.85: "Priv / escalation" (alert)
  - t=0.68, y=0.75: "Service acct / created" (alert)
  - t=0.82, y=0.9: "Bulk / download" (alert)
- **Impossible-travel bracket:** orange `#e67e22` 2px horizontal line below the axis from t=0.05 to t=0.42, captioned "impossible travel detected" (11px orange, centered).
- **Legend (top right):** green dot "normal", red dot "anomalous" (11px).

### Sample payload (`.sample-payload` code block): "Sample: Impossible Travel Alert"

```json
{
  "alert_type": "impossible_travel",
  "timestamp": "2024-03-15T10:15:00Z",
  "user": "jsmith@corp.com",
  "event_1": {
    "time": "10:00:00Z",
    "location": "New York, US",
    "ip": "72.14.201.33",
    "mfa": "push_approved"
  },
  "event_2": {
    "time": "10:15:00Z",
    "location": "Singapore",
    "ip": "103.56.44.12",
    "mfa": "token_replay"
  },
  "distance_km": 15300,
  "time_gap_min": 15,
  "severity": "critical"
}
```

## 6. Deep Dive: Cloud Security (CSPM / CWPP)

Cloud security products collect data at two layers: the **control plane** (API calls, config changes) and the **data plane** (workload runtime behavior).

- **API audit logs** — every API call to the cloud provider recorded (AWS CloudTrail, Azure Activity Log, GCP Audit Logs). Who did what, to which resource, from where, and whether it succeeded.
- **Configuration snapshots** — periodic scan of all resource configs (S3 bucket policies, security group rules, IAM policies, encryption settings). Compared against compliance baselines (CIS, SOC2, PCI).
- **Network flow logs** — VPC flow logs capture source/dest IP, port, bytes, action (accept/reject) for every connection between cloud resources.
- **Container runtime** — agent in each node observes process execution, file access, network calls inside containers. Detects drift from golden image.
- **Serverless invocations** — function execution logs, cold start vs warm, input payloads (if configured), IAM role assumed, downstream API calls made.
- **Storage access patterns** — who accessed which objects, bulk download detection, access from unusual IPs, public exposure scanning.

**Key point:** In cloud, the attack surface is the configuration. A single overly-permissive IAM policy or public S3 bucket is a finding — no runtime exploit needed.

**Collection method:** agentless (API-based snapshot + log pull) for control plane; agent-based (eBPF, sidecar) for workload runtime. Most products combine both.

### Visualization (canvas `c7`, 720×380)

Two stacked panels: control plane (top, blue) and data plane (bottom, red), split by a dashed boundary.

- **Title (bold 16px, `#1a5276`, top center):** "Cloud Security — Two Collection Planes".
- **Control plane panel:** rectangle fill `rgba(41,128,185,0.08)`, stroke `#2980b9` 2px; bold 13px blue header "CONTROL PLANE (agentless — API logs & config)"; 12px `#4a5866` two-column bullets: "CloudTrail / Activity Log", "IAM policy changes", "Config snapshots", "Storage ACLs".
- **Data plane panel:** rectangle fill `rgba(231,76,60,0.08)`, stroke `#e74c3c` 2px; bold 13px red header "DATA PLANE (agent-based — runtime behavior)"; bullets: "Container process exec", "Network connections", "File system drift", "Syscall traces".
- **Divider:** dashed (6/4) gray `#95a5a6` 1.5px horizontal line between panels, right-aligned 11px gray label "cloud provider boundary".

### Sample payload (`.sample-payload` code block): "Sample: CloudTrail API Call"

```json
{
  "eventTime": "2024-03-15T03:42:18Z",
  "eventSource": "s3.amazonaws.com",
  "eventName": "PutBucketPolicy",
  "userIdentity": {
    "type": "IAMUser",
    "arn": "arn:aws:iam::1234:user/dev-intern"
  },
  "sourceIPAddress": "198.51.100.7",
  "requestParameters": {
    "bucket": "prod-customer-data",
    "policy": "{\"Effect\":\"Allow\",\"Principal\":\"*\"}"
  },
  "finding": "public_access_granted",
  "compliance": ["CIS-3.8", "SOC2-CC6.1"]
}
```

## 7. Deep Dive: Firewall / Next-Gen Firewall

A firewall sits inline on the network boundary and makes an **allow/deny decision on every connection**. A next-gen firewall (NGFW) adds application-layer intelligence on top of traditional port/protocol rules.

- **Connection log** — every session: source IP, dest IP, source port, dest port, protocol, bytes sent/received, duration, action (allow/deny/drop/reset).
- **Application identification** — deep packet inspection classifies traffic by application (Zoom, Slack, BitTorrent, SSH tunnel) regardless of port used.
- **URL filtering** — full URL logged for HTTP; SNI + category for HTTPS. Categorized against threat intel and policy (gambling, malware, social media).
- **Threat prevention** — IPS signatures matched against traffic (exploit attempts, vulnerability probes). Spyware/C2 callback detection via DNS and connection patterns.
- **User-ID mapping** — ties connections to Active Directory users (via agent on DC, or GlobalProtect client). Logs become "user X accessed Y" not just "IP accessed Y."
- **SSL decrypt log** — which sessions were decrypted, which bypassed (pinned certs, health/finance categories), decrypt failures.
- **NAT translation table** — maps internal private IPs to external IPs. Critical for forensic attribution after the fact.

**Key point:** The firewall generates the single richest source of network metadata in most organizations — every allowed and denied connection, attributed to a user and an application, at line rate.

**Log volume:** A mid-size org (5,000 users) generates 1-5 billion firewall log entries per day. This is typically the largest single log source feeding the SIEM.

### Visualization (canvas `c8`, 720×380)

Horizontal funnel of processing stages, each a bar whose width is the fraction of connections surviving.

- **Title (bold 16px, `#1a5276`, top center):** "Firewall — Decision per Connection".
- **Lanes (bars start at x=160, max width = canvas−180, height 50, 14px gaps; fill = color + `30` alpha, 2px stroke; label right-aligned left of bar in color 12px; bold 12px percentage at the bar's right end):**
  - "Connection arrives" 100% `#2980b9`
  - "App identified" 95% `#8e44ad`
  - "Policy evaluated" 90% `#e67e22`
  - "Threat scanned (IPS)" 85% `#e74c3c`
  - "Allowed through" 62% `#27ae60`
- **Connectors:** faint gray `#95a5a6` 1px vertical lines between lanes.
- **Callout (11px `#e74c3c`, centered below the lanes):** "denied / dropped → logged with reason".
- **Caption (11px `#5a6875`, bottom center):** "every connection logged: src, dst, app, user, action, bytes".

### Sample payload (`.sample-payload` code block): "Sample: Firewall Session Log"

```json
{
  "timestamp": "2024-03-15T11:33:07Z",
  "rule": "deny-outbound-c2",
  "action": "deny",
  "src_ip": "10.1.5.42",
  "dst_ip": "185.220.101.6",
  "dst_port": 443,
  "app": "cobalt-strike",
  "user": "CORP\\jsmith",
  "bytes_sent": 1240,
  "bytes_recv": 0,
  "threat": {
    "name": "CobaltStrike.Gen.C2",
    "severity": "critical",
    "direction": "outbound"
  },
  "nat_src": "203.0.113.5:49201"
}
```

## Regeneration instructions

- **Layout:** backlog detail page. `h1` (2rem `#1a5276`, bottom border `2px solid #2980b9`) with inline `.status` pill "BACKLOG" (background `#fef9e7`, border `1px solid #f39c12`, text `#b7950b`, 4px radius, 0.8rem); `.subtitle` (`#666`, 0.95rem); `.intro` callout (background `#f0f4f8`, left border `3px solid #2980b9`, 8px 12px padding, 0.9rem). One `.lang-section` per numbered h2 (1.3rem `#1a5276`, bottom border `2px solid #2980b9`); inside each, `table.layout` with `td.text-col` 45% and `td.viz-col` 55%, both `vertical-align: top`, 12px padding. Sections 2–7 place the canvas plus a `.sample-payload` block in the viz column. No index number in the h1.
- **Text blocks:** lead `<p>`, `<ul>` bullets (0.92rem) with `<strong>` lead-ins, `.key-point` callout (background `#f8f9fa`, left border `3px solid #e74c3c`, 0.9rem), and a trailing 0.9em paragraph with a bold lead-in (Architecture / What gets logged / Data sources / Collection method / Log volume).
- **Sample payload style:** `.sample-payload` — background `#f8f9fa`, border `1px solid #dce1e6`, 6px radius, 12px 14px padding, monospace ('SF Mono', 'Fira Code', Consolas) 0.78em, `white-space: pre`, inline-block; `.payload-title` bold `#1a5276`; syntax spans: `.key` `#1a5276`, `.str` `#27ae60`, `.num` `#e74c3c`, `.comment` italic `#7f8c8d`. JSON keys colored blue, string values green, numbers/booleans red.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. Canvases `width: 100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; secondary `#2980b9` blue and `#8e44ad` purple; gray labels `#5a6875`/`#4a5866`, axes `#95a5a6`.
- **Canvas:** intrinsic sizes 720×340 (`c1`), 720×420 (`c3`, `c4`), 720×380 (`c5`–`c8`); canvas ids skip `c2`. Scale backing store by `window.devicePixelRatio`, `ctx.scale` back to logical coordinates via a shared `setupCanvas(id)` helper.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
