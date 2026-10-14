"""Phoenix traffic pipeline: pull -> geocode -> aggregate -> static data.json.

Two public datasets, both from City of Phoenix Open Data, both located to the
hundred-block / intersection (no lat/long), geocoded with the free US Census batch geocoder:

  A. Traffic CITATIONS (police-issued) — civil + criminal moving violations.
     resource 7725bbf3...  ~355k rows, 2018-2025.  (the only "violation type" the city
     publishes is civil vs criminal — there is no charge-level detail anywhere public.)
  B. Calls for Service INCIDENTS — the granular traffic categories the citations lack:
     crashes, hit & run, drunk drivers, traffic hazards, speeding/racing. Block-level +
     timestamp. NOTE: these are dispatched/​self-initiated police CALLS, not citations.

Output: one data.json, unified by category, grid-binned (~445 m) x day-of-week x hour, plus
per-category all-time "worst block" leaderboards. All map filtering is client-side.

ponytail: stdlib only (urllib/csv/json). pandas isn't on py3.14; dict loops are plenty.
Run: `py pipeline.py`. citations_raw.csv / calls_raw.csv / geocode_cache.csv are caches.
"""
import csv, io, json, os, re, sys, time, urllib.request, urllib.parse, uuid, datetime
from collections import defaultdict

DS = "https://www.phoenixopendata.com/api/3/action/datastore_search"
SQL = "https://www.phoenixopendata.com/api/3/action/datastore_search_sql"
CENSUS = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0 Safari/537.36"

CIT_RES = "7725bbf3-7829-4f57-8cc2-0faac51b90de"
CIT_FIELDS = ["TICK_DATE", "DAY_OF_WEEK", "HOUR_OF_DAY", "HUNDREDBLOCKADDR",
              "CIVIL_TRAFFIC_VIOLATIONS", "CRIMINAL_TRAFFIC_VIOLATIONS"]
CFS_RES = {2018:"30a4d911-41a3-4d4a-a817-ade6f12b8131", 2019:"7edc831c-9167-41a9-9b7b-61a167cb9739",
           2020:"3c0ae3ec-456f-45f4-801d-b8d6699ba32e", 2021:"33094536-e3b8-4634-a4a6-4e0daca4aa91",
           2022:"27963751-fe11-49ab-a0a8-627cd9338e17", 2023:"cb578ca1-e7b2-4952-a441-a0eb7cded1a2",
           2024:"e8417a83-fcad-4cad-803c-423de3ad2d92", 2025:"00775c9c-026d-41e5-a4f4-057ad60bea90"}
# CFS final-call-type buckets. Matched on a TRIMMED/UPPERCASED value via substrings, because
# the 2025 resource pads values with trailing spaces and adds FATALITY/SUPPLEMENT variants the
# 2018-24 files lack — an exact IN() match silently dropped all of 2025.
TRAFFIC_LIKE = ["ACCIDENT", "HIT & RUN", "DRUNK DRIVER", "TRAFFIC HAZARD", "SPEEDING", "RACING"]
def cat_of(t):
    t = (t or "").strip().upper()
    if not t or "SUPPLEMENT" in t: return None        # SUPPLEMENT = follow-up on an already-logged call
    hr = "HIT & RUN" in t
    if "ACCIDENT" in t or hr:                          # crashes, split by severity (note: "NO INJUR" before "INJUR")
        sev = "F" if "FATAL" in t else ("N" if "NO INJUR" in t else ("I" if "INJUR" in t else "N"))
        return ("hitrun" if hr else "crash") + sev     # crashN/crashI/crashF, hitrunN/hitrunI/hitrunF
    if "DRUNK DRIVER" in t: return "dui"
    if "TRAFFIC HAZARD" in t: return "hazard"
    if "SPEEDING" in t or "RACING" in t: return "racing"
    return None

# unified category order (record `ct` index). 0/1 = citations, 2-6 = incidents.
CATS = ["civil", "criminal", "crashN", "crashI", "crashF",
        "hitrunN", "hitrunI", "hitrunF", "dui", "hazard", "racing"]
CIDX = {c: i for i, c in enumerate(CATS)}

RAW, CALLS, CACHE, OUT = "citations_raw.csv", "calls_raw.csv", "geocode_cache.csv", "data.json"
GRID = 0.004
LON_GRID = GRID / 0.835
BOX_S, BOX_N, BOX_W, BOX_E = 33.10, 34.00, -112.90, -111.60
YEAR0, NYEARS = 2018, 8                      # records carry a year index 0..7 (2018..2025)

def get(url, tries=5):
    for a in range(tries):                          # the portal resets connections mid-pull; retry
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=180))
        except Exception as e:
            if a == tries - 1: raise
            print(f"  get retry {a}: {e}", file=sys.stderr); time.sleep(3 * (a + 1))

# ----------------------------------------------------------------- pull citations
def pull_citations():
    if os.path.exists(RAW):
        print(f"[pull] {RAW} exists, skipping"); return
    def fetch(off, lim):
        q = urllib.parse.urlencode({"resource_id": CIT_RES, "limit": lim, "offset": off,
                                    "fields": ",".join(CIT_FIELDS)})
        return get(f"{DS}?{q}")["result"]
    total = fetch(0, 1)["total"]; print(f"[pull citations] {total} rows")
    with open(RAW, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CIT_FIELDS); w.writeheader()
        off = 0
        while off < total:
            recs = fetch(off, 32000)["records"]
            if not recs: break
            for r in recs: w.writerow({k: r.get(k, "") for k in CIT_FIELDS})
            off += len(recs); print(f"  {off}/{total}", file=sys.stderr)

# ----------------------------------------------------------------- pull CFS incidents
def pull_calls():
    if os.path.exists(CALLS):
        print(f"[pull] {CALLS} exists, skipping"); return
    where = " OR ".join(f"\"FINAL_CALL_TYPE\" LIKE '%{p}%'" for p in TRAFFIC_LIKE)
    with open(CALLS, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["block", "received", "type"])
        grand = 0
        for yr, rid in CFS_RES.items():
            off = 0
            while True:
                q = (f'SELECT "HUNDREDBLOCKADDR","CALL_RECEIVED","FINAL_CALL_TYPE" FROM "{rid}" '
                     f'WHERE {where} ORDER BY "_id" LIMIT 30000 OFFSET {off}')
                recs = get(f"{SQL}?sql=" + urllib.parse.quote(q))["result"]["records"]
                if not recs: break
                for r in recs:
                    w.writerow([r["HUNDREDBLOCKADDR"], r["CALL_RECEIVED"], r["FINAL_CALL_TYPE"]])
                off += len(recs); grand += len(recs)
                print(f"  CFS {yr}: {off}", file=sys.stderr)
                if len(recs) < 30000: break
        print(f"[pull calls] {grand} traffic incident rows")

# ----------------------------------------------------------------- geocode (both sets)
def normalize(block):
    b = (block or "").strip().upper()
    if b in ("", "NA"):
        return None
    return f"{b.replace('XX', '50')}, PHOENIX, AZ"

def census_batch(items):
    buf = io.StringIO(); cw = csv.writer(buf)
    for i, q in items: cw.writerow([i, q, "PHOENIX", "AZ", ""])
    payload = buf.getvalue().encode(); bnd = "----b" + uuid.uuid4().hex
    def part(name, fn, content, ct):
        h = f'--{bnd}\r\nContent-Disposition: form-data; name="{name}"'
        h += (f'; filename="{fn}"\r\nContent-Type: {ct}\r\n\r\n' if fn else "\r\n\r\n")
        return h.encode() + content + b"\r\n"
    body = part("addressFile", "a.csv", payload, "text/csv") + part("benchmark", None, b"Public_AR_Current", "")
    body += f"--{bnd}--\r\n".encode()
    req = urllib.request.Request(CENSUS, data=body, headers={
        "Content-Type": f"multipart/form-data; boundary={bnd}", "User-Agent": UA})
    out = urllib.request.urlopen(req, timeout=600).read().decode()
    res = {}
    for row in csv.reader(io.StringIO(out)):
        if len(row) >= 6 and row[2] == "Match" and row[5]:
            lon, lat = row[5].split(","); res[row[0]] = (round(float(lon), 5), round(float(lat), 5))
    return res

def geocode():
    cache = {}
    if os.path.exists(CACHE):
        with open(CACHE, encoding="utf-8") as f:
            for r in csv.DictReader(f): cache[r["block"]] = (r["lon"], r["lat"])
    need = {}
    def consider(b):
        b = (b or "").strip()
        if b and b not in cache and b not in need:
            q = normalize(b)
            if q: need[b] = q
    with open(RAW, encoding="utf-8") as f:
        for r in csv.DictReader(f): consider(r["HUNDREDBLOCKADDR"])
    if os.path.exists(CALLS):
        with open(CALLS, encoding="utf-8") as f:
            for r in csv.DictReader(f): consider(r["block"])
    print(f"[geocode] cached={len(cache)} to-do={len(need)}")
    if not need: return cache
    blocks = list(need); new = open(CACHE, "a", newline="", encoding="utf-8"); nw = csv.writer(new)
    if os.path.getsize(CACHE) == 0: nw.writerow(["block", "lon", "lat"])
    CH = 1000
    for s in range(0, len(blocks), CH):
        chunk = blocks[s:s + CH]; items = [(str(i), need[chunk[i]]) for i in range(len(chunk))]
        for attempt in range(3):
            try: res = census_batch(items); break
            except Exception as e: print(f"  batch {s} retry {attempt}: {e}", file=sys.stderr); time.sleep(5)
        else: print(f"  batch {s} FAILED", file=sys.stderr); continue
        hit = 0
        for i in range(len(chunk)):
            ll = res.get(str(i))
            if ll: cache[chunk[i]] = ll; nw.writerow([chunk[i], ll[0], ll[1]]); hit += 1
            else: cache[chunk[i]] = ("", ""); nw.writerow([chunk[i], "", ""])
        new.flush(); print(f"  geocoded {s+len(chunk)}/{len(blocks)} (+{hit})", file=sys.stderr)
    new.close(); return cache

# ----------------------------------------------------------------- ZIP rollup
# Boundaries: OpenDataDE AZ ZIP GeoJSON. Population + city + centroid: OpenDataSoft
# (Census-derived). Both keyless (the Census API itself now requires a key). Cached to
# zips_geo.json so reruns don't re-fetch. We keep only usps_city='Phoenix' AZ ZIPs so the
# per-capita comparison is among ZIPs actually covered by Phoenix PD.
ZIP_GEO_URL = "https://raw.githubusercontent.com/OpenDataDE/State-zip-code-GeoJSON/master/az_arizona_zip_codes_geo.min.json"
ODS = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-zc-point/records"
ZCACHE, AZGEO = "zips_geo.json", "az_zips.json"

def load_zips():
    if os.path.exists(ZCACHE):
        return json.load(open(ZCACHE, encoding="utf-8"))
    pops, off = {}, 0
    while True:
        q = urllib.parse.urlencode({"where": "ste_name='Arizona'", "limit": 100, "offset": off,
                                    "select": "zip_code,usps_city,population,geo_point_2d"})
        res = get(f"{ODS}?{q}").get("results", [])
        for r in res:
            if r.get("usps_city") == "Phoenix" and r.get("population"):
                g = r.get("geo_point_2d") or {}
                pops[r["zip_code"]] = {"pop": int(r["population"]),
                                       "c": [round(g.get("lon", 0), 5), round(g.get("lat", 0), 5)]}
        off += len(res)
        if len(res) < 100: break
    if not os.path.exists(AZGEO):
        urllib.request.urlretrieve(ZIP_GEO_URL, AZGEO)
    az = json.load(open(AZGEO, encoding="utf-8"))
    zips = []
    for f in az["features"]:
        z = f["properties"].get("ZCTA5CE10")
        if z not in pops: continue
        geom = f["geometry"]; xs, ys = [], []
        def walk(c):
            if isinstance(c[0], (int, float)): xs.append(c[0]); ys.append(c[1])
            else:
                for cc in c: walk(cc)
        walk(geom["coordinates"])
        zips.append({"z": z, **pops[z], "bbox": [min(xs), min(ys), max(xs), max(ys)],
                     "geom": geom["coordinates"], "gt": geom["type"]})
    json.dump(zips, open(ZCACHE, "w"), separators=(",", ":"))
    print(f"[zips] {len(zips)} Phoenix ZIPs, pop {sum(z['pop'] for z in zips):,}")
    return zips

def _pip_ring(x, y, ring):
    inside = False; n = len(ring); j = n - 1
    for i in range(n):
        xi, yi = ring[i]; xj, yj = ring[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / ((yj - yi) or 1e-12) + xi):
            inside = not inside
        j = i
    return inside

def _pip(x, y, coords, gt):
    for poly in ([coords] if gt == "Polygon" else coords):
        if _pip_ring(x, y, poly[0]) and not any(_pip_ring(x, y, h) for h in poly[1:]):
            return True
    return False

def zip_of(lon, lat, zips):
    for z in zips:
        b = z["bbox"]
        if b[0] <= lon <= b[2] and b[1] <= lat <= b[3] and _pip(lon, lat, z["geom"], z["gt"]):
            return z["z"]
    return None

# ----------------------------------------------------------------- build
def parse_cfs_time(s):
    """CFS uses two formats across years -> (dow Sun=0..Sat=6, hour 0-23) or None:
       ISO '2018-01-03T23:41:37' (earlier years) and '1/1/2024  12:21:31AM' (later)."""
    s = (s or "").strip()
    m = re.match(r"(\d{4})-(\d{1,2})-(\d{1,2})T(\d{1,2}):", s)        # ISO, 24-hour
    if m:
        yr, mo, da, hh = int(m[1]), int(m[2]), int(m[3]), int(m[4])
    else:
        m = re.match(r"(\d+)/(\d+)/(\d+)\s+(\d+):\d+:\d+\s*([AP]M)", s, re.I)  # M/D/Y 12-hour
        if not m: return None
        mo, da, yr, hh = int(m[1]), int(m[2]), int(m[3]), int(m[4]) % 12
        if m[5].upper() == "PM": hh += 12
    try: wd = datetime.date(yr, mo, da).weekday()                    # Mon=0..Sun=6
    except ValueError: return None
    return (wd + 1) % 7, hh, yr                                      # -> Sun=0..Sat=6, hour, year

def cell_of(lon, lat, cells, cellpt):
    gx, gy = round(lon / LON_GRID), round(lat / GRID)
    key = (gx, gy); cid = cells.get(key)
    if cid is None:
        cid = len(cellpt); cells[key] = cid
        cellpt.append([round(gx * LON_GRID, 5), round(gy * GRID, 5)])
    return cid

def titlecase(s):
    s = re.sub(r"\s+", " ", (s or "").strip())       # 2025 data pads with double spaces
    return re.sub(r"\b([a-z])", lambda m: m.group(1).upper(), s.lower())

def build(cache):
    cells, cellpt = {}, []
    agg = defaultdict(int)                      # (cid,dow,hour,cat) -> count
    cell_block = defaultdict(lambda: defaultdict(int))   # cid -> block -> total (for labels)
    block_geo = {}                              # block -> [lng,lat]
    block_cat = defaultdict(lambda: defaultdict(int))    # block -> cat -> count (leaderboards)
    hour_hist = [0] * 24; dow_hist = [0] * 7
    n_cit = n_inc = skipped = 0
    dmin, dmax = "9999", "0000"

    def place(block, dow, hour, cat, yr, cnt):
        nonlocal skipped
        yi = yr - YEAR0
        if not (0 <= yi < NYEARS): skipped += 1; return False
        ll = cache.get(block)
        if not ll or ll[0] == "": skipped += 1; return False
        lon, lat = float(ll[0]), float(ll[1])
        if not (BOX_S <= lat <= BOX_N and BOX_W <= lon <= BOX_E): skipped += 1; return False
        cid = cell_of(lon, lat, cells, cellpt)
        agg[(cid, dow, hour, CIDX[cat], yi)] += cnt
        cell_block[cid][block] += cnt
        block_geo[block] = [round(lon, 5), round(lat, 5)]
        block_cat[block][(cat, yi)] += cnt
        hour_hist[hour] += cnt; dow_hist[dow] += cnt
        return True

    # --- citations ---
    with open(RAW, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            b = (r["HUNDREDBLOCKADDR"] or "").strip()
            try:
                dow = int(str(r["DAY_OF_WEEK"]).split("-")[0]) - 1; hour = int(r["HOUR_OF_DAY"])
            except (ValueError, IndexError): skipped += 1; continue
            if not (0 <= dow <= 6 and 0 <= hour <= 23): skipped += 1; continue
            cv = int(float(r["CIVIL_TRAFFIC_VIOLATIONS"] or 0))
            cr = int(float(r["CRIMINAL_TRAFFIC_VIOLATIONS"] or 0))
            yr = int(((r["TICK_DATE"] or "0")[:4]) or 0)
            ok = False
            if cv: ok |= place(b, dow, hour, "civil", yr, cv)
            if cr: ok |= place(b, dow, hour, "criminal", yr, cr)
            if ok: n_cit += cv + cr
            d = (r["TICK_DATE"] or "")[:10]
            if d: dmin, dmax = min(dmin, d), max(dmax, d)

    # --- incidents (calls for service) ---
    if os.path.exists(CALLS):
        with open(CALLS, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                t = parse_cfs_time(r["received"])
                if not t: skipped += 1; continue
                dow, hour, yr = t
                cat = cat_of(r["type"])
                if not cat: skipped += 1; continue
                if place((r["block"] or "").strip(), dow, hour, cat, yr, 1): n_inc += 1

    # flatten records, sorted by key (cid-major) so the columnar arrays gzip well
    ci, dw, hr, ct, yrr, nn = [], [], [], [], [], []
    for (cid, dow, hour, cat, yi), cnt in sorted(agg.items()):
        ci.append(cid); dw.append(dow); hr.append(hour); ct.append(cat); yrr.append(yi); nn.append(cnt)

    # cell labels (dominant block in each cell)
    lab = []
    for cid in range(len(cellpt)):
        bl = cell_block.get(cid)
        lab.append(titlecase(max(bl.items(), key=lambda kv: kv[1])[0]) if bl else "")

    # per-view, per-year leaderboards (worst blocks). lb[view]["all"|"2018"...] -> [40 blocks].
    CR = {"crashN", "crashI", "crashF"}; HR = {"hitrunN", "hitrunI", "hitrunF"}
    VIEWS = {"allcit": {"civil", "criminal"}, "civil": {"civil"}, "criminal": {"criminal"},
             "crash": CR, "hitrun": HR, "injury": {"crashI", "hitrunI"}, "fatal": {"crashF", "hitrunF"},
             "dui": {"dui"}, "hazard": {"hazard"}, "racing": {"racing"},
             "allinc": CR | HR | {"dui", "hazard", "racing"}}
    YEARS = list(range(YEAR0, YEAR0 + NYEARS))
    lb = {}
    for view, cats in VIEWS.items():
        per = {}
        for key in ["all"] + [str(y) for y in YEARS]:
            yi = None if key == "all" else int(key) - YEAR0
            scored = []
            for b, cc in block_cat.items():
                if b not in block_geo: continue
                tot = sum(v for (cat, y), v in cc.items() if cat in cats and (yi is None or y == yi))
                if tot: scored.append((tot, b))
            scored.sort(reverse=True)
            per[key] = [{"a": titlecase(b), "g": block_geo[b], "n": tot} for tot, b in scored[:40]]
        lb[view] = per

    # ---- ZIP per-capita rollup (point-in-polygon each cell -> Phoenix ZIP) ----
    zips = load_zips()
    cell_zip = [zip_of(lon, lat, zips) for (lon, lat) in cellpt]
    zc = defaultdict(lambda: defaultdict(int))
    for cid, cat, cnt in zip(ci, ct, nn):
        z = cell_zip[cid]
        if z is not None: zc[z][CATS[cat]] += cnt
    INC = ["crashN", "crashI", "crashF", "hitrunN", "hitrunI", "hitrunF", "dui", "hazard", "racing"]
    CIT = ["civil", "criminal"]
    zout = []
    for zz in zips:
        cc = zc.get(zz["z"], {})
        inc = {k: cc.get(k, 0) for k in INC}; cit = {k: cc.get(k, 0) for k in CIT}
        zout.append({"z": zz["z"], "pop": zz["pop"], "c": zz["c"], "inc": inc, "cit": cit,
                     "incN": sum(inc.values()), "citN": sum(cit.values())})
    print(f"[zips] cells matched to a Phoenix ZIP: {sum(1 for z in cell_zip if z)}/{len(cellpt)}")

    data = {
        "meta": {
            "cit_source": "City of Phoenix Open Data — Traffic Citations (police-issued)",
            "inc_source": "City of Phoenix Open Data — Police Calls for Service",
            "cit_url": "https://www.phoenixopendata.com/dataset/citations",
            "inc_url": "https://www.phoenixopendata.com/dataset/calls-for-service",
            "date_min": dmin, "date_max": dmax, "years": YEARS,
            "citations": n_cit, "incidents": n_inc, "skipped_no_geo": skipped,
            "grid_m": round(GRID * 111000), "hour_hist": hour_hist, "dow_hist": dow_hist,
        },
        "cats": CATS,
        "lng": [p[0] for p in cellpt], "lat": [p[1] for p in cellpt], "lab": lab,
        "ci": ci, "dw": dw, "hr": hr, "ct": ct, "yr": yrr, "n": nn,
        "lb": lb, "zips": zout,
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"[build] cells={len(cellpt)} records={len(ci)} citations={n_cit} incidents={n_inc} "
          f"skipped={skipped}")
    print(f"[build] wrote {OUT} ({os.path.getsize(OUT)/1e6:.2f} MB raw)")

if __name__ == "__main__":
    pull_citations()
    pull_calls()
    cache = geocode()
    build(cache)
