# app-ads.txt Scraper — Detailed Design

> Detailed version. The concise one is in [README.md](../README.md).
> All figures come from live tests and verified sources (August 2026).

## 1. Overall Architecture

```
┌─────────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Discovery Service  │     │  Orchestrator    │     │  Fetcher Workers    │
│  (BullMQ queues)    │────►│  (BullMQ + Redis)│────►│  (stateless, HPA)   │
│  AppStore + Play    │     │  rate_limit +    │     │  residential pool + │
│  batchexecute/HTML  │     │  IP budget ceil  │     │  batchexecute/HTML  │
└─────────────────────┘     └──────────────────┘     └─────────────────────┘
         │                            │                            │
         ▼                            ▼                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  Postgres: apps, sites, domains, snapshots, events                      │
│  S3/object storage: raw app-ads.txt (content-addressed by SHA-256)      │
└─────────────────────────────────────────────────────────────────────────┘
```

**Stack:** Node.js + Express (stateless API), Postgres (data), Redis (BullMQ + rate limiting), bull-board for queue inspection, Prometheus + Grafana for metrics.

**Two data phases — separate cycles:**

- **Weekly** — marketplace discovery (App Store lookup API / Play scraper) → developer_name, app_title, website URL, status (ACTIVE/REMOVED/OWNER_CHANGED)
- **Daily** — fetch app-ads.txt from publisher sites → content_hash, status (OK/NOT_FOUND/UNREACHABLE/BLOCKED)

## 2. Database Schema

### 2.1 apps

```sql
CREATE TABLE apps (
  id             BIGSERIAL PRIMARY KEY,
  bundle_id      TEXT NOT NULL,
  store          TEXT NOT NULL CHECK (store IN ('APPSTORE','GOOGLE_PLAY')),
  status         TEXT NOT NULL DEFAULT 'ACTIVE'
                 CHECK (status IN ('ACTIVE','REMOVED','OWNER_CHANGED')),
  developer_name TEXT,
  app_title      TEXT,
  site_id        BIGINT REFERENCES sites(id),
  next_check_at  TIMESTAMPTZ,
  created_at     TIMESTAMPTZ DEFAULT now(),
  updated_at     TIMESTAMPTZ DEFAULT now(),
  UNIQUE (bundle_id, store)
);

CREATE INDEX ON apps (next_check_at) WHERE status = 'ACTIVE';
```

Weekly scheduling logic:

```sql
-- Atomic batch claim (no races between replicas)
UPDATE apps SET next_check_at = now() + interval '15 min'
WHERE id IN (
  SELECT id FROM apps
  WHERE next_check_at <= now() AND status = 'ACTIVE'
  ORDER BY next_check_at LIMIT $batch
  FOR UPDATE SKIP LOCKED
)
RETURNING *;
-- 15 min = claim timeout: if a worker dies after claiming,
-- the row returns to work on its own
```

**Schedule drift:** after processing — `next_check_at = now() + interval '7 days'`, not `COALESCE(next_check_at, ...)` (COALESCE would return the stale date and dump the whole backlog into the queue at once).

### 2.2 sites

```sql
CREATE TABLE sites (
  id             BIGSERIAL PRIMARY KEY,
  subdomain      TEXT NOT NULL,   -- '@' if root
  domain_id      BIGINT NOT NULL REFERENCES domains(id),
  fail_count     INT DEFAULT 0,
  last_failed_at TIMESTAMPTZ,
  next_check_at  TIMESTAMPTZ,
  last_ok_at     TIMESTAMPTZ,
  UNIQUE (subdomain, domain_id)
);

CREATE INDEX ON sites (next_check_at) WHERE fail_count <= 5;
```

Daily cycle: success → fail_count=0, next_check_at = now() + 1d (with jitter); failure → fail_count+=1, last_failed_at=now(), next attempt with exponential backoff.

**Signal upward:** UNREACHABLE/DNS failures on a site pull `domains.next_probe_at` forward to now() — the probe job runs earlier than every site of the domain burns through its own backoff.

### 2.3 domains

The domain level is not just for normalization — it is also the **Health Check Center**: if an entire domain is down, knocking on /app-ads.txt of each of its sites every day is pointless. Probe the domain, not the sites.

```sql
CREATE TABLE domains (
  id              BIGSERIAL PRIMARY KEY,
  name            TEXT NOT NULL UNIQUE,
  status          TEXT NOT NULL DEFAULT 'OK'
                  CHECK (status IN ('OK','DOWN','PARKED')),
  fail_count      INT DEFAULT 0,
  last_checked_at TIMESTAMPTZ,
  next_probe_at   TIMESTAMPTZ DEFAULT now(),
  site_count      INT DEFAULT 0        -- denormalized, for the queue
);
CREATE INDEX ON domains (next_probe_at) WHERE status <> 'OK';
```

**Domain probe policy** (a separate lightweight job, not the daily fetch):

1. Probe = cheap two-step check:
   - DNS resolve (if NXDOMAIN → fail immediately, don't spend an HTTP request)
   - HEAD `https://<domain>/` with a 10s timeout. Any HTTP response (even 404/403) = domain is ALIVE — the file may be gone, but the host answers
2. **DOWN**: 3 consecutive failed probes (DNS failure / timeout / connection refused). After that, PAUSE daily fetch for all sites of the domain: next_check_at = now() + 3d
3. While DOWN: probe the domain once every 3 days with exponential backoff up to 14 days. Sites of the domain are NOT fetched and don't spam events — one status at the domain level instead of thousands of UNREACHABLE snapshots
4. Recovery: 2 consecutive successful probes → status OK, sites of the domain immediately get their daily next_check_at back. Two successes in a row — protection against flapping (a domain that blinked for an hour)
5. **PARKED**: DOWN persists 30+ days OR DNS is consistently NXDOMAIN. Sites of the domain get next_check_at = NULL (removed from the schedule). Returning from PARKED — manual only, or via weekly discovery (if the app reappears with a new URL)
6. Boundary rule: 404/403 on app-ads.txt with a live host is NOT_FOUND at the site level — the domain does NOT go down (the file was removed, the host works)

**Volume impact:** empirically ~10–20% of domains in a catalog are dead or parked — at 30M apps that's millions of wasted daily requests, which the domain level cuts down to a single probe every 3–14 days.

**FUTURE-PROOFING NOTE:** splitting domain_id/subdomain is groundwork for growth, not MVP-mandatory. Today deduplication is by (subdomain, domain) — the full host. Later, the domain level allows aggregating metrics across the whole publisher (how many apps point to it, overall domain health) rather than per-subdomain — and adding other files with no schema change:

- /ads.txt (web version, same model)
- /app-ads.txt/app-ads.txt (legacy variant per IAB 1.1)
- sellers.json (IAB) from the same domain

So a separate domains table is deliberate normalization, not over-engineering: one publisher domain change updates ALL of its sites at once (fix at the domains level → all sites pick it up automatically via FOREIGN KEY).

### 2.4 snapshots

```sql
CREATE TABLE snapshots (
  id           BIGSERIAL PRIMARY KEY,
  site_id      BIGINT NOT NULL REFERENCES sites(id),
  content_hash TEXT NOT NULL,      -- SHA-256 of the normalized text
  text         TEXT,
  status       TEXT NOT NULL
               CHECK (status IN ('OK','NOT_FOUND','UNREACHABLE','BLOCKED')),
  updated_at   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON snapshots (site_id, updated_at DESC);
```

A new row is written ONLY if the SHA-256 of the normalized text changed — otherwise the table grows by millions of duplicates every day.

Statuses are split semantically because they require different strategies:

| Status | Meaning | Strategy |
|---|---|---|
| OK | 200 + valid body | persist to snapshots |
| NOT_FOUND | 404, domain alive, no file | no aggressive retries |
| UNREACHABLE | timeout / DNS / 5xx | retry + exponential backoff |
| BLOCKED | CAPTCHA interstitial | circuit breaker signal |

### 2.5 events

```sql
CREATE TABLE events (
  id         BIGSERIAL PRIMARY KEY,
  bundle_id  TEXT,
  site_id    BIGINT,
  event_type TEXT NOT NULL
             CHECK (event_type IN
               ('OWNER_CHANGED','DOMAIN_CHANGED','APP_REMOVED','APPADS_CHANGED')),
  old_value  TEXT,
  new_value  TEXT,
  at         TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX ON events (bundle_id, at DESC);
```

Change history without bloating snapshots — for auditing and verification.

## 3. Discovery: weekly

### 3.1 App Store

```
GET https://itunes.apple.com/lookup?bundleId=com.example.app
```

Official API, ~20 req/min per source. In the response: sellerUrl (publisher site), sellerName (developer), trackName (title), resultCount (0 = app removed).

For volumes beyond the limit — Enterprise Partner Feed (EPF), separate application.

### 3.2 Google Play

```
GET https://play.google.com/store/apps/details?id={bundle_id}
```

Fetch + cheerio, anchor — `a[aria-label^="Website "]` → href. The site is already in the raw HTML (SSR), no Puppeteer needed.

Careful with CSS classes (e.g. "Si6A0c RrSxVb") — they are generated and change between builds. Anchor on aria-label, not classes.

After extraction — compare against previous state:

- developer_name changed → OWNER_CHANGED in events + out-of-band app-ads.txt fetch
- 404 → REMOVED in apps + APP_REMOVED in events
- URL changed → DOMAIN_CHANGED in events + upsert into sites/domains

### 3.3 Play breakdown: request formats (verified)

| Mode | Wire size | When to use |
|---|---|---|
| Top charts (batchexecute) | ~150 B/app (batches of 660, ~100KB response) | full catalog |
| App details (full HTML) | ~250–350KB/app (gzip; raw HTML ~1.2MB) | priority tail only |
| App details (batchexecute) | unstable (tests: 400/empty bodies, exact args schema unknown) | not used: rpcid and args schema are brittle — reverse-engineering per every UI change |

Top charts via batchexecute (`play.google.com/_/PlayStoreUi/data/batchexecute`) — verified, works. Server-side pagination limit is 660 (source: baltpeter's write-up).

App details — fetch the full page + parse inline JSON: the `AF_initDataCallback 'ds:5'` block contains ALL required fields (developer, website, id, email). Verified on the live Roblox page: ds:5 = ~20KB out of 1.2MB of the full page (1.6%), but ~300KB over the wire with gzip. Field paths are the same as in the google-play-scraper library (MAPPINGS):

```
developer:        ds:5 [1,2,68,0]
developerId:      ds:5 [1,2,68,1,4,2]
developerWebsite: ds:5 [1,2,69,0,5,2]
developerEmail:   ds:5 [1,2,69,1,0]
```

## 4. Fetching app-ads.txt: daily

0. **Domain gate**: the claim query joins sites with domains and returns ONLY sites whose `domain.status = 'OK'`. No requests to /app-ads.txt for DOWN/PARKED domains — saving both us and the publisher:

```sql
SELECT s.* FROM sites s
JOIN domains d ON d.id = s.domain_id
WHERE s.next_check_at <= now()
  AND d.status = 'OK'
ORDER BY s.next_check_at
LIMIT $batch
FOR UPDATE OF s SKIP LOCKED;
```

A site under a DOWN domain won't appear in the result set anyway: its next_check_at was already pushed back by the probe policy (see 2.3).

1. Fetch by the FULL host reconstructed from subdomain + domain: `https://www.example.com/app-ads.txt` and `https://example.com/app-ads.txt` may differ.
2. Conditional requests: ETag / If-Modified-Since → 304 = "no change", don't download the body. **Verified live:** Roblox returned 200 even for an If-Modified-Since with a future date — conditional requests are unreliable as the sole change signal, content_hash stays primary.
3. Change detection by content_hash (SHA-256 of normalized text), not by dates/headers.
4. Jitter: when a site is created, its daily next_check_at is initialized to a random hour of day — otherwise all domains of a big publisher fire in one salvo every day.
5. Per-domain rate limit — token bucket in Redis: 1 req/sec/domain, so we don't DDoS large publishers.

## 5. Throughput (verified, not invented)

### 5.1 App Store

Officially ~20 req/min per source (Apple Performance Partners docs). Not enough for the full catalog — the official path for larger volume: Enterprise Partner Feed (EPF), separate application.

### 5.2 Google Play

No official limits (it's not an API); empirically ~500–1000 req/IP/day, then 503+CAPTCHA and an IP ban for ~an hour.

Scale-out path: horizontally scale workers. Important to pin down: the per-IP limit and the aggregate request volume toward Google do NOT depend on the number of replicas — the scaling ceiling is the number of IPs in the proxy pool:

```
max_replicas = IP_budget × per_IP_rate / concurrency_per_replica
```

Workers give throughput, IP budget gives coverage — TWO SEPARATE LEVERS. Consequence of our own empirical numbers: 30M/week ÷ ~750 req/IP/day ≈ ~6000 fresh IPs/day — the real price of full HTML-mode coverage in-house.

Circuit breaker in the spirit of sites (fail_count): under mass blocking we slow down, we don't try to punch through the protection. No detection-evasion infrastructure — that's a separate business risk, not an architectural decision.

## 6. Financial Model (verified August 2026)

### 6.1 Market prices

| Provider | Residential price |
|---|---|
| Evomi | from $0.49/GB |
| DataImpulse | $1/GB PAYG (traffic never expires, $5 min) |
| IPRoyal | $1.00–1.75/GB |
| Decodo (ex-Smartproxy) | $2.20–3.75/GB |
| Bright Data | $2.50–4/GB ($4 PAYG) |
| Oxylabs | ~$4–6/GB |

Median at ~1 TB/mo volume — ~$3.40/GB, at 10 TB/mo — ~$2.10/GB.

### 6.2 Cost scenarios

**Scenario A: 30M catalog/week via top charts (batchexecute)**

```
4.3M/day ÷ 660 = 6400 requests/day × 100KB
≈ 0.64 GB/day ≈ 19 GB/mo
at $1/GB → ~$19/mo — DO IT OURSELVES, cheap
```

**Scenario B: 5M priority apps via full HTML (weekly)**

```
5M × ~300KB ≈ 1.5 TB/week ≈ 6.5 TB/mo
at $1/GB → ~$6.5k/mo
(not $540 as previously estimated for batchexecute — details
via batchexecute are unstable, HTML is ~12x heavier)
at $3.4/GB (median) → ~$22k/mo
```

**Scenario B-alt: in-house app-details scraper (HTML+gzip)**

Justified ONLY for fresh new apps not yet in the 42matters feed (hundreds of thousands, not millions):

```
100k × 300KB ≈ 30 GB/week ≈ ~$130/mo — acceptable
as a supplement to the feed
```

**Scenario C: full 30M catalog via full HTML (not doing)**

```
30M × 300KB ≈ 9 TB/week ≈ 39 TB/mo
at $1/GB → ~$39k/mo — categorically not an option
```

Conclusion: per-app detail data is expensive — exactly the volume where a licensed feed wins.

### 6.3 Comparison with a licensed feed

42matters (verified August 2026):

- File Dumps from €998/mo
- API plans €79–€999/mo
- Post-Similarweb pricing is not public — per-client quota by endpoints/volume/platforms

**Switching rule** (our own, from the doc): if in-house infrastructure ≈ €998+ → buy the feed.

Applying the rule to the new numbers:

- Catalog (top charts, ~$19/mo) → in-house scraper
- Details for 5M priority apps (~$6.5k/mo) → 42matters feed (€998+)
  - IN-HOUSE HTML MODE IS ~6.5x MORE EXPENSIVE THAN THE FEED
  - → the "infrastructure ≈ subscription → buy" rule TRIGGERS

Technical detail (verified live): all needed fields live in AF_initDataCallback ds:5 = 19.9KB = 1.6% of the page, but Play won't serve that block separately via batchexecute without reverse-engineering a private RPC protocol — so we fetch full HTML (gzip ~300KB over the wire) and parse using paths from google-play-scraper (see 3.3).

**Final configuration:**

- scraper-play: top-charts batchexecute — full catalog for ~$19/mo
- licensed-feed (42matters): detailed data on specific apps
- Source Adapter: both behind one interface, switched by config (already built into the architecture)

Dashboard metric: cost per 1000 apps — catalog ~$0.00015/1000 (top charts), details ~$0.30/1000 (HTML) — compare against a provider's price for the same volume.

## 7. Risks and Limitations

| Risk | Explanation | Mitigation |
|---|---|---|
| Google ToS prohibits automated scraping | legal review mandatory before production launch | without legal sign-off we run only the App Store branch |
| Changes to batchexecute format / ds:5 structure (AF_initDataCallback) in HTML | undocumented, changes without notice; field paths are brittle (google-play-scraper MAPPINGS) | Source Adapter: scraper-play and licensed-feed behind one interface, config switch; ds:5 structure monitoring + alerts |
| CAPTCHA detection | TLS/JA3 fingerprint, not just IP | under mass blocking — circuit breaker, not "punching through" |
| IP sales policy | providers sell GB, not IPs | budget driven by traffic, not IP count; formula (5.2) pinned in config |
| Recrawl scenario | refreshes not weekly but per-app | arithmetic of B scales linearly — multiply by frequency |

## 8. Implementation Stages

1. **MVP (1–2 weeks):** App Store lookup → fetch app-ads.txt → Postgres, one worker. Goal — metrics for assumptions: % of bundle_ids with sellerUrl, % of live domains, average file size.
2. **Play scraper (2–3 weeks):** HTML fetch + ds:5 parsing, top charts via batchexecute, residential pool, BullMQ queues, horizontal workers, database backfill.
3. **Schedules (1–2 weeks):** daily/weekly with jitter, conditional requests, claim logic, DLQ, history + events.
4. **Read API + dashboards (1 week):** /appads/:bundle_id, /status/:bundle_id; metrics: % successful fetches, status distribution, data age, cost per 1000 apps (the key metric for the scraper-vs-feed decision).

## 9. Open Questions

1. What share of the catalog is actually valuable for verification? (determines the size of Scenario B)
2. Is full change history a product requirement, or is current state + a "changed" flag enough?
3. Legal review of Play scraping — by which stage must it be complete? (recommend before Stage 2)

## Sources (verified August 2026)

- **batchexecute:** top charts work (baltpeter's write-up, 660 limit); details via batchexecute — tests yield 400/empty bodies, exact args schema unknown — not used
- **App details:** AF_initDataCallback 'ds:5' in the page HTML, field paths — MAPPINGS from github.com/facundoolano/google-play-scraper (lib/app.js); measured on the live Roblox page: ds:5 ≈ 20KB of 1.2MB, ~300KB over the wire with gzip
- **Proxy prices:** DataImpulse $1/GB PAYG, Evomi $0.49/GB — pricing pages + aggregators, August 2026
- **42matters:** File Dumps from €998, API plans — Swiss Martech + 42matters docs
- **Empirical Play limits** (500–1000 req/IP/day) — our own observations, verified with curl
