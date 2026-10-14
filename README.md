# Ticket Heat — Phoenix traffic tickets, crashes & DUIs

Live: **https://tickets.traqqit.com** · part of [traqqit](https://traqqit.com)

A dark MapLibre map of **where and when Phoenix writes traffic tickets** — and where
**crashes, hit-and-runs, drunk-driver and street-racing calls** cluster. Pick a category,
slide through the hours / day-of-week, and read the **worst blocks** off a live leaderboard
(click a row or a numbered pin to fly there). Built entirely on public records.

> **Honesty:** *enforcement & incident history, not a prediction.* Two sources, two meanings:
> "Tickets" are police-issued **citations**; "Crashes / DUIs / etc." are police **Calls for
> Service** (dispatched/self-initiated calls, not convictions). Locations are hundred-block /
> intersection, geocoded → block-level approximate. Phoenix publishes **no** charge-level
> violation detail (only civil vs criminal), which is why the granular categories come from
> the calls dataset.

## Data (both City of Phoenix Open Data, 2018–2025)
- **Citations** — [Traffic Citations](https://www.phoenixopendata.com/dataset/citations)
  (`7725bbf3…`), ~355k rows. Only civil vs criminal; no charge detail, no fine, no lat/long.
- **Incidents** — [Calls for Service](https://www.phoenixopendata.com/dataset/calls-for-service)
  (per-year resources). We pull only the traffic call types and bucket them into
  **crash / hit & run / dui / hazard / racing**. Has `HUNDREDBLOCKADDR` + timestamp.

Categories (record `ct`): `civil, criminal, crash{N,I,F}, hitrun{N,I,F}, dui, hazard, racing`
— crashes/hit&runs split by severity (No-injury / Injury / Fatal); the front-end sums them into
Crashes / Injury / Fatal lenses.

## Pipeline (`pipeline.py`, stdlib only — no pandas/pip)
    py pipeline.py
1. **pull** → `citations_raw.csv` (CKAN datastore) + `calls_raw.csv` (datastore SQL, filtered
   to traffic call types, paginated — the SQL endpoint caps at 32k rows/query).
2. **geocode** → `geocode_cache.csv` (US Census batch geocoder, free/no-key; `XX`→`50`,
   intersections kept as `A & B`, 1k-row batches). Both datasets share the cache.
3. **build** → `data.json` (~445 m grid × day-of-week × hour × category, columnar arrays;
   per-category all-time "worst block" leaderboards; **per-ZIP per-capita rollup**). Client-side.

**ZIP rollup:** point-in-polygon (stdlib ray-casting) maps each grid cell into Phoenix ZIP
polygons, then incidents are divided by population for a danger-per-resident rate. Sources are
keyless (the **Census API now requires a key**): boundaries = OpenDataDE AZ GeoJSON, population =
OpenDataSoft `georef-...-zc-point` (Arizona, usps_city=Phoenix). Cached to `zips_geo.json`.

Caches make reruns cheap; only new blocks get geocoded. **Gotcha:** Calls-for-Service
timestamps come in **two** formats across years (ISO `2018-01-03T23:41:37` early, then
`1/1/2024  12:21:31AM`) — `parse_cfs_time` handles both (missing one silently drops ~46%).

## Front-end (`index.html`, one file, no build)
MapLibre GL + CARTO **raster** dark tiles (keyless; the vector style hangs headless). Heatmap
layer + HTML-marker ranked pins. Category selector, **year filter (all / 2018–2025)**, day/hour
filters, all-day toggle, click-a-block popups, leaderboard panel (re-ranks by category & year).
`data.json` records carry a year index and are sorted cid-major so the columnar arrays gzip
hard — ~8.5 MB raw but **~0.9 MB gzipped** via Caddy.

Also: **address/intersection search** (top bar), **"near me"** (geolocation → nearest-block
stats), and **shareable deep-links** — the URL hash carries category/year/day/hour + map
position (`#v=crash&y=2024&d=5&h=2&z=12&at=lng,lat`), restored on load; the ↗ button copies it.

### `/geocode` proxy (search)
Census sends no CORS header, so the browser can't call it directly. The tickets Caddy block
proxies `GET /geocode?address=…` → Census `onelineaddress` (same-origin, no key), so searched
streets/intersections land exactly on the data (both geocoded by Census). See `travelmap/Caddyfile`.

## Deploy (shared Hostinger VPS, same pattern as habooby / traqqit-home)
Static `caddy:2` file server `tickets` (`-v /root/phx-tickets:/usr/share/caddy:ro`,
`travelmap_default`). Route `tickets.traqqit.com → tickets:80` in `travelmap/Caddyfile`.
To update: `scp index.html data.json root@85.31.233.230:/root/phx-tickets/` (file server reads
live, no reload). DNS `tickets` A → `85.31.233.230`.

`shoot.mjs` = dependency-free CDP screenshotter (Node global WebSocket) — headless MapLibre
needs `--enable-unsafe-swiftshader` and a real clock (virtual-time races the tile paint).
