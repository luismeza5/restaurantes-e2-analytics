"""Delivery route assignment & optimisation.

Takes deliverable orders (with customer geolocation), assigns them to couriers by
GAM zone under a capacity cap, then sequences each courier's stops with a
nearest-neighbour heuristic refined by 2-opt. Distances use the haversine metric;
the same stop sequence can be re-timed with Neo4j ROUTE minutes (see
neo4j/cypher/40_routing.cypher) for an ETA-accurate view.

Standalone: reads the lake (bronze/orders), so it can run on its own.

    python neo4j/routing/nearest_neighbor.py --couriers 4 --capacity 8 --out routes.json
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
from collections import defaultdict

import pandas as pd

LAKE = os.getenv("LAKE_PATH", "/lake/warehouse")
# Central kitchen (San Jose centre) — start/end of every route.
KITCHEN = {"name": "Cocina Central", "lat": 9.9333, "lng": -84.0833}
AVG_SPEED_KMH = float(os.getenv("ROUTING_AVG_SPEED_KMH", "28"))
DELIVERABLE = {"pending", "preparing"}  # orders still awaiting a courier


def haversine_km(a, b):
    r = 6371.0
    p1, p2 = math.radians(a[0]), math.radians(b[0])
    dphi = math.radians(b[0] - a[0])
    dl = math.radians(b[1] - a[1])
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


def route_length(points):
    """Total km for kitchen -> stops in order -> back to kitchen."""
    pts = [(KITCHEN["lat"], KITCHEN["lng"])] + points + [(KITCHEN["lat"], KITCHEN["lng"])]
    return sum(haversine_km(pts[i], pts[i + 1]) for i in range(len(pts) - 1))


def nearest_neighbour(stops):
    """Greedy NN tour starting from the kitchen. stops: list of dicts w/ lat,lng."""
    remaining = stops[:]
    cur = (KITCHEN["lat"], KITCHEN["lng"])
    order = []
    while remaining:
        nxt = min(remaining, key=lambda s: haversine_km(cur, (s["lat"], s["lng"])))
        order.append(nxt)
        cur = (nxt["lat"], nxt["lng"])
        remaining.remove(nxt)
    return order


def two_opt(order):
    """Local-search refinement: reverse segments while it shortens the tour."""
    def coords(seq):
        return [(s["lat"], s["lng"]) for s in seq]

    improved = True
    best = order[:]
    best_len = route_length(coords(best))
    while improved:
        improved = False
        for i in range(len(best) - 1):
            for j in range(i + 1, len(best)):
                cand = best[:i] + best[i:j + 1][::-1] + best[j + 1:]
                cl = route_length(coords(cand))
                if cl + 1e-9 < best_len:
                    best, best_len, improved = cand, cl, True
    return best, best_len


def assign_to_couriers(orders, n_couriers, capacity):
    """Zone-aware, load-balanced assignment.

    For each order we prefer a courier that already serves its zone and is under
    capacity; otherwise the least-loaded courier under capacity; if every courier
    is full (more orders than total capacity) the order spills to the globally
    least-loaded courier so load stays balanced instead of dumping on courier 1.
    """
    couriers = [[] for _ in range(n_couriers)]
    courier_zones = [set() for _ in range(n_couriers)]

    # Process larger zones first so coherent clusters are placed before spillover.
    by_zone = defaultdict(list)
    for o in orders:
        by_zone[o["zone"]].append(o)

    for zone, zone_orders in sorted(by_zone.items(), key=lambda kv: -len(kv[1])):
        for o in zone_orders:
            under = [i for i in range(n_couriers) if len(couriers[i]) < capacity]
            if under:
                same_zone = [i for i in under if zone in courier_zones[i]]
                pool = same_zone if same_zone else under
                pick = min(pool, key=lambda i: len(couriers[i]))
            else:  # everyone full -> keep it balanced
                pick = min(range(n_couriers), key=lambda i: len(couriers[i]))
            couriers[pick].append(o)
            courier_zones[pick].add(zone)
    return couriers


def load_orders():
    files = glob.glob(f"{LAKE}/bronze/orders/*.parquet")
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df = df[df["status"].isin(DELIVERABLE)]
    locs = pd.concat([pd.read_parquet(f)
                      for f in glob.glob(f"{LAKE}/bronze/locations/*.parquet")],
                     ignore_index=True)[["location_id", "zone"]]
    df = df.merge(locs, on="location_id", how="left")
    return [
        dict(order_id=r.order_id, lat=float(r.customer_lat), lng=float(r.customer_lng),
             zone=r.zone, net_amount=float(r.net_amount))
        for r in df.itertuples(index=False)
    ]


def build_routes(n_couriers, capacity):
    orders = load_orders()
    if not orders:
        return {"couriers": [], "note": "no deliverable orders found"}
    assignments = assign_to_couriers(orders, n_couriers, capacity)

    result = {"kitchen": KITCHEN, "avg_speed_kmh": AVG_SPEED_KMH, "couriers": []}
    for idx, stops in enumerate(assignments, start=1):
        if not stops:
            continue
        nn = nearest_neighbour(stops)
        optimised, total_km = two_opt(nn)
        naive_km = route_length([(s["lat"], s["lng"]) for s in nn])
        result["couriers"].append({
            "courier": f"repartidor-{idx}",
            "stops": len(optimised),
            "zones": sorted({s["zone"] for s in optimised}),
            "total_km": round(total_km, 2),
            "est_minutes": round(total_km / AVG_SPEED_KMH * 60, 1),
            "naive_nn_km": round(naive_km, 2),
            "improvement_pct": round(100 * (naive_km - total_km) / naive_km, 1) if naive_km else 0,
            "sequence": [s["order_id"] for s in optimised],
        })
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--couriers", type=int, default=4)
    ap.add_argument("--capacity", type=int, default=10)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    routes = build_routes(args.couriers, args.capacity)
    text = json.dumps(routes, indent=2, ensure_ascii=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"[routing] wrote {args.out}")
    print(text)


if __name__ == "__main__":
    main()
