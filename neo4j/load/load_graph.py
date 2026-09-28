"""Load the property graph into Neo4j from the data lake (bronze zone).

Graph model
-----------
  (:User)-[:LIVES_IN]->(:Location)
  (:User)-[:RECOMMENDED]->(:User)          # referral DAG -> influencer analysis
  (:User)-[:PLACED]->(:Order)
  (:Order)-[:CONTAINS {quantity}]->(:Product)
  (:Location)-[:ROUTE {distance_km, minutes}]->(:Location)   # geonode mesh

The ROUTE mesh turns the GAM locations into geonodes with weighted edges so
Neo4j GDS can compute minimum-cost delivery paths (Proyecto 2 §5/§6).

Reads Parquet with pandas (fine at this volume); batches with UNWIND. For very
large graphs swap in the neo4j-spark connector — the Cypher is identical.

Env: NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, LAKE_PATH.
"""
from __future__ import annotations

import glob
import math
import os

import pandas as pd

from neo4j import GraphDatabase

LAKE = os.getenv("LAKE_PATH", "/lake/warehouse")
URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PWD = os.getenv("NEO4J_PASSWORD", "neo4jpass")
AVG_SPEED_KMH = float(os.getenv("ROUTING_AVG_SPEED_KMH", "28"))  # urban GAM speed


def _read(zone, table):
    files = glob.glob(f"{LAKE}/{zone}/{table}/*.parquet")
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def haversine_km(a, b):
    r = 6371.0
    p1, p2 = math.radians(a[0]), math.radians(b[0])
    dphi = math.radians(b[0] - a[0])
    dl = math.radians(b[1] - a[1])
    h = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


def batched(rows, size=2000):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


CONSTRAINTS = [
    "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
    "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE",
    "CREATE CONSTRAINT order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.order_id IS UNIQUE",
    "CREATE CONSTRAINT location_id IF NOT EXISTS FOR (l:Location) REQUIRE l.location_id IS UNIQUE",
]


def main():
    driver = GraphDatabase.driver(URI, auth=(USER, PWD))
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")  # idempotent full reload
        for c in CONSTRAINTS:
            s.run(c)

        locations = _read("bronze", "locations")
        users = _read("bronze", "users")
        products = _read("bronze", "products")
        orders = _read("bronze", "orders")
        items = _read("bronze", "order_items")

        # Location nodes
        s.run("""UNWIND $rows AS r
                 MERGE (l:Location {location_id:r.location_id})
                 SET l += {province:r.province, canton:r.canton, district:r.district,
                           zone:r.zone, lat:r.latitude, lng:r.longitude}""",
              rows=locations.to_dict("records"))

        # Product nodes
        s.run("""UNWIND $rows AS r
                 MERGE (p:Product {product_id:r.product_id})
                 SET p += {name:r.name, category:r.category, price:toFloat(r.price)}""",
              rows=products.to_dict("records"))

        # User nodes + LIVES_IN + RECOMMENDED
        urecs = users.fillna("").to_dict("records")
        for chunk in batched(urecs):
            s.run("""UNWIND $rows AS r
                     MERGE (u:User {user_id:r.user_id})
                     SET u += {name:r.full_name, zone:'', lat:toFloat(r.latitude),
                               lng:toFloat(r.longitude)}
                     WITH u, r
                     MATCH (l:Location {location_id:r.location_id})
                     MERGE (u)-[:LIVES_IN]->(l)""", rows=chunk)
        ref = [r for r in urecs if r.get("referred_by")]
        for chunk in batched(ref):
            s.run("""UNWIND $rows AS r
                     MATCH (a:User {user_id:r.referred_by})
                     MATCH (b:User {user_id:r.user_id})
                     MERGE (a)-[:RECOMMENDED]->(b)""", rows=chunk)

        # Order nodes + PLACED
        orecs = orders.to_dict("records")
        for chunk in batched(orecs):
            s.run("""UNWIND $rows AS r
                     MERGE (o:Order {order_id:r.order_id})
                     SET o += {created_at:r.created_at, status:r.status,
                               net_amount:toFloat(r.net_amount),
                               delivery_km:toFloat(r.delivery_km)}
                     WITH o, r
                     MATCH (u:User {user_id:r.user_id})
                     MERGE (u)-[:PLACED]->(o)""", rows=chunk)

        # CONTAINS
        irecs = items.to_dict("records")
        for chunk in batched(irecs, 4000):
            s.run("""UNWIND $rows AS r
                     MATCH (o:Order {order_id:r.order_id})
                     MATCH (p:Product {product_id:r.product_id})
                     MERGE (o)-[c:CONTAINS]->(p)
                     SET c.quantity = toInteger(r.quantity)""", rows=chunk)

        # ROUTE mesh between location geonodes (weighted by distance & minutes)
        locs = locations.to_dict("records")
        edges = []
        for i, a in enumerate(locs):
            for b in locs[i + 1:]:
                d = round(haversine_km((a["latitude"], a["longitude"]),
                                       (b["latitude"], b["longitude"])), 3)
                mins = round(d / AVG_SPEED_KMH * 60, 1)
                edges.append(dict(a=a["location_id"], b=b["location_id"],
                                  distance_km=d, minutes=mins))
        s.run("""UNWIND $rows AS r
                 MATCH (a:Location {location_id:r.a})
                 MATCH (b:Location {location_id:r.b})
                 MERGE (a)-[e1:ROUTE]->(b) SET e1.distance_km=r.distance_km, e1.minutes=r.minutes
                 MERGE (b)-[e2:ROUTE]->(a) SET e2.distance_km=r.distance_km, e2.minutes=r.minutes""",
              rows=edges)

        counts = s.run("""MATCH (n) WITH labels(n)[0] AS l, count(*) AS c
                          RETURN l, c ORDER BY l""").data()
    driver.close()
    print("[neo4j] loaded:", {c["l"]: c["c"] for c in counts})


if __name__ == "__main__":
    main()
