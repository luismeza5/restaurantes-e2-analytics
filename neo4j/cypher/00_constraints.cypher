// =============================================================================
// Schema constraints (mirror of neo4j/load/load_graph.py). Run once.
// =============================================================================
CREATE CONSTRAINT user_id     IF NOT EXISTS FOR (u:User)     REQUIRE u.user_id     IS UNIQUE;
CREATE CONSTRAINT product_id  IF NOT EXISTS FOR (p:Product)  REQUIRE p.product_id  IS UNIQUE;
CREATE CONSTRAINT order_id    IF NOT EXISTS FOR (o:Order)    REQUIRE o.order_id     IS UNIQUE;
CREATE CONSTRAINT location_id IF NOT EXISTS FOR (l:Location) REQUIRE l.location_id IS UNIQUE;
