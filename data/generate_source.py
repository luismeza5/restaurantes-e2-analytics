"""
generate_source.py — synthetic OLTP source for restaurantes-e2-analytics.

Produces data with the SAME shape Proyecto 1 persists (users, categories,
products, orders, order_items, reservations), so the analytics pipeline can run
standalone — no need to spin up Proyecto 1 first. Patterns are deliberately
non-uniform so the three Spark analyses actually surface signal:

  * monthly growth   -> order volume ramps month over month
  * peak hours       -> lunch (12-13h) and dinner (19-21h) spikes, weekend uplift
  * geography        -> customers spread across Costa Rica's GAM with lat/long
  * cancellations    -> ~10% cancelled, ~6% no-show reservations

Output: CSV files under data/source/ (read by spark/jobs/extract.py).

Usage:
    python data/generate_source.py --orders 50000 --seed 42 --out data/source
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import random
from datetime import datetime, timedelta

try:
    from faker import Faker
except ImportError:  # pragma: no cover
    raise SystemExit("pip install faker  (see data/README.md)")

# --- Costa Rica Greater Metropolitan Area: zones with a centroid -------------
# (province, canton, district, zone, lat, lng)
GAM = [
    ("San Jose",  "San Jose",   "Carmen",        "Centro",     9.9333, -84.0833),
    ("San Jose",  "San Jose",   "Pavas",         "Oeste",      9.9550, -84.1300),
    ("San Jose",  "Escazu",     "San Rafael",    "Oeste",      9.9189, -84.1419),
    ("San Jose",  "Santa Ana",  "Pozos",         "Oeste",      9.9326, -84.1830),
    ("San Jose",  "Curridabat", "Granadilla",    "Este",       9.9128, -84.0299),
    ("San Jose",  "Desamparados","Desamparados", "Sur",        9.8990, -84.0660),
    ("Cartago",   "Cartago",    "Oriental",      "Cartago",    9.8644, -83.9194),
    ("Cartago",   "La Union",   "Tres Rios",     "Este",       9.9060, -83.9840),
    ("Heredia",   "Heredia",    "Heredia",       "Norte",      9.9981, -84.1167),
    ("Heredia",   "San Pablo",  "San Pablo",     "Norte",      9.9990, -84.0890),
    ("Alajuela",  "Alajuela",   "Alajuela",      "Aeropuerto", 10.0162, -84.2117),
    ("Alajuela",  "Atenas",     "Atenas",        "Oeste",      9.9790, -84.3790),
]

CATALOGUE = {
    "Bebidas":   [("Cafe chorreado", 1200), ("Agua dulce", 900), ("Fresco de cas", 1100),
                  ("Cerveza nacional", 1800), ("Refresco natural", 1300)],
    "Casados":   [("Casado con pollo", 3800), ("Casado con pescado", 4500),
                  ("Casado con bistec", 4200), ("Casado vegetariano", 3400)],
    "Gallos":    [("Gallo de carne", 1500), ("Gallo de chicharron", 1700),
                  ("Gallo de queso", 1300)],
    "Sopas":     [("Olla de carne", 3900), ("Sopa de mariscos", 5200), ("Sopa negra", 2600)],
    "Postres":   [("Tres leches", 2200), ("Arroz con leche", 1600), ("Flan de coco", 1900)],
    "Desayunos": [("Gallo pinto", 2400), ("Huevos rancheros", 2800), ("Tortilla aliñada", 2100)],
}

STATUSES = [  # (status, weight, is_completed, is_cancelled)
    ("delivered", 70, True,  False),
    ("completed", 12, True,  False),
    ("preparing",  5, False, False),
    ("pending",    3, False, False),
    ("cancelled", 10, False, True),
]


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def pick_status(rng):
    r = rng.uniform(0, sum(w for _, w, _, _ in STATUSES))
    acc = 0
    for status, w, comp, canc in STATUSES:
        acc += w
        if r <= acc:
            return status, comp, canc
    return STATUSES[0][0], True, False


def pick_hour(rng, is_weekend):
    """Bimodal lunch/dinner distribution; weekends shift a bit later."""
    bucket = rng.random()
    if bucket < 0.40:                       # lunch
        h = int(rng.normalvariate(12.5, 1.0))
    elif bucket < 0.80:                     # dinner
        h = int(rng.normalvariate(19.5 + (0.7 if is_weekend else 0), 1.2))
    else:                                   # off-peak
        h = int(rng.uniform(7, 22))
    return max(7, min(22, h))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=50000)
    ap.add_argument("--users", type=int, default=2500)
    ap.add_argument("--reservations", type=int, default=8000)
    ap.add_argument("--months", type=int, default=14, help="history depth in months")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/source")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    fake = Faker("es_CO")  # closest Latin-American Spanish locale bundled w/ Faker
    Faker.seed(args.seed)
    os.makedirs(args.out, exist_ok=True)

    end = datetime(2025, 6, 1, 23, 59)
    start = end - timedelta(days=30 * args.months)

    # ---- locations (dedup of GAM list) --------------------------------------
    locations = []
    for i, (prov, cant, dist, zone, lat, lng) in enumerate(GAM, start=1):
        locations.append(dict(location_id=f"loc-{i:03d}", province=prov, canton=cant,
                              district=dist, zone=zone, latitude=lat, longitude=lng))
    write_csv(os.path.join(args.out, "locations.csv"), locations)

    # ---- categories & products ---------------------------------------------
    categories, products = [], []
    pid = 0
    for ci, (cat, items) in enumerate(CATALOGUE.items(), start=1):
        categories.append(dict(category_id=f"cat-{ci:02d}", name=cat))
        for name, price in items:
            pid += 1
            products.append(dict(product_id=f"prod-{pid:04d}", name=name,
                                 category=cat, price=price, is_active=True))
    write_csv(os.path.join(args.out, "categories.csv"), categories)
    write_csv(os.path.join(args.out, "products.csv"), products)

    # ---- users (each anchored to a GAM location with jitter) ----------------
    users = []
    for i in range(1, args.users + 1):
        loc = rng.choice(locations)
        # ~35% of users were referred by an earlier user => a referral DAG that
        # Neo4j/GDS can rank for "influential recommenders".
        referred_by = ""
        if i > 1 and rng.random() < 0.35:
            referred_by = f"user-{rng.randint(1, i - 1):05d}"
        users.append(dict(
            user_id=f"user-{i:05d}",
            full_name=fake.name(),
            email=fake.unique.email(),
            signup_date=(start + timedelta(days=rng.randint(0, 30 * args.months))).date().isoformat(),
            location_id=loc["location_id"],
            referred_by=referred_by,
            latitude=round(loc["latitude"] + rng.uniform(-0.02, 0.02), 6),
            longitude=round(loc["longitude"] + rng.uniform(-0.02, 0.02), 6),
        ))
    write_csv(os.path.join(args.out, "users.csv"), users)

    # ---- orders + order_items (with monthly-growth weighting) ---------------
    total_days = (end - start).days
    orders, items = [], []
    item_seq = 0
    for i in range(1, args.orders + 1):
        # bias day selection toward the recent end => monthly growth
        frac = rng.random() ** 0.6          # skew toward 1.0 (recent)
        day_offset = int(frac * total_days)
        d = start + timedelta(days=day_offset)
        is_weekend = d.weekday() >= 5
        hour = pick_hour(rng, is_weekend)
        ts = d.replace(hour=hour, minute=rng.randint(0, 59), second=0)

        user = rng.choice(users)
        loc = next(lc for lc in locations if lc["location_id"] == user["location_id"])
        status, is_comp, is_canc = pick_status(rng)

        n_lines = rng.randint(1, 5)
        gross = 0.0
        order_id = f"ord-{i:06d}"
        line_products = rng.sample(products, k=min(n_lines, len(products)))
        for p in line_products:
            item_seq += 1
            qty = rng.randint(1, 3)
            line_amt = qty * p["price"]
            gross += line_amt
            items.append(dict(
                order_item_id=f"oi-{item_seq:07d}",
                order_id=order_id, product_id=p["product_id"],
                quantity=qty, unit_price=p["price"], line_amount=line_amt,
            ))
        discount = round(gross * (0.10 if rng.random() < 0.18 else 0.0), 2)
        net = round(gross - discount, 2)
        # courier distance: customer jitter vs a fixed kitchen near San Jose centre
        dist_km = round(haversine_km(9.9333, -84.0833, user["latitude"], user["longitude"]), 3)
        orders.append(dict(
            order_id=order_id, user_id=user["user_id"], location_id=loc["location_id"],
            created_at=ts.isoformat(sep=" "), status=status,
            gross_amount=round(gross, 2), discount_amount=discount, net_amount=net,
            item_count=len(line_products), delivery_km=dist_km,
            customer_lat=user["latitude"], customer_lng=user["longitude"],
        ))
    write_csv(os.path.join(args.out, "orders.csv"), orders)
    write_csv(os.path.join(args.out, "order_items.csv"), items)

    # ---- reservations -------------------------------------------------------
    reservations = []
    for i in range(1, args.reservations + 1):
        frac = rng.random() ** 0.6
        d = start + timedelta(days=int(frac * total_days))
        is_weekend = d.weekday() >= 5
        ts = d.replace(hour=pick_hour(rng, is_weekend), minute=rng.choice([0, 30]))
        user = rng.choice(users)
        roll = rng.random()
        no_show = roll < 0.06
        honored = (not no_show) and roll < 0.94
        reservations.append(dict(
            reservation_id=f"res-{i:06d}", user_id=user["user_id"],
            location_id=user["location_id"], reserved_at=ts.isoformat(sep=" "),
            party_size=rng.randint(1, 8),
            status="no_show" if no_show else ("honored" if honored else "cancelled"),
        ))
    write_csv(os.path.join(args.out, "reservations.csv"), reservations)

    print(f"[generate_source] wrote {len(users)} users, {len(products)} products, "
          f"{len(orders)} orders, {len(items)} items, {len(reservations)} reservations "
          f"to {args.out}/")


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
