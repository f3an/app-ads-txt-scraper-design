# app-ads-txt-scraper-design

Test assignment: design for a scraper service collecting app-ads.txt at 30M+ app scale — weekly discovery, daily fetch, domain health checks, Postgres + BullMQ, scraper vs licensed-feed cost trade-offs.

## TL;DR

- **Weekly** — marketplace discovery (App Store lookup API / Google Play scraper) finds the publisher's website for each app
- **Daily** — fetch `https://[domain]/app-ads.txt`, detect changes by SHA-256 of the normalized body (not by ETag/headers — verified unreliable on live sites)
- **Domain-level health checks** — dead domains are probed once every 3–14 days instead of knocking on every site daily (~10–20% of domains in a real catalog are dead or parked)
- **Cost-driven sourcing** — full catalog scraped in-house (~$19/mo via batchexecute), per-app details bought from a licensed feed (in-house HTML would be ~6.5x more expensive)

## Documents

- 📄 **Full design** — [docs/detailed-design.md](docs/detailed-design.md): architecture, SQL schema (5 tables), discovery/fetch pipelines, throughput math, financial model, risks, implementation stages

## Stack

Node.js + Express · Postgres · Redis (BullMQ) · residential proxy pool
