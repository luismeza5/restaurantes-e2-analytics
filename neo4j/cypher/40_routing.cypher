// =============================================================================
// Delivery routing over geonodes (Proyecto 2 §5/§6: "Caminos mínimos entre
// ubicaciones para reparto eficiente")
// =============================================================================
// The :Location nodes form a weighted geonode mesh via :ROUTE {distance_km,
// minutes}. These queries find minimum-cost paths the courier module consumes.

// --- Q1: Single shortest path between two locations (Dijkstra, by minutes) ---
// :params { from:'loc-001', to:'loc-012' }
MATCH (src:Location {location_id:$from}), (dst:Location {location_id:$to})
CALL gds.graph.project.cypher(
  'routes',
  'MATCH (l:Location) RETURN id(l) AS id',
  'MATCH (a:Location)-[r:ROUTE]->(b:Location)
   RETURN id(a) AS source, id(b) AS target, r.minutes AS minutes, r.distance_km AS km'
) YIELD graphName
WITH src, dst
CALL gds.shortestPath.dijkstra.stream('routes', {
  sourceNode: src, targetNode: dst, relationshipWeightProperty: 'minutes'
})
YIELD totalCost, nodeIds, costs
RETURN [n IN nodeIds | gds.util.asNode(n).district] AS route,
       round(totalCost, 1) AS total_minutes;

// --- Q2: All shortest delivery times from the kitchen to every zone ---------
// (single-source Dijkstra) — feeds the "estimated ETA per zone" panel.
MATCH (kitchen:Location {location_id:$kitchen})
CALL gds.allShortestPaths.dijkstra.stream('routes', {
  sourceNode: kitchen, relationshipWeightProperty: 'minutes'
})
YIELD targetNode, totalCost
RETURN gds.util.asNode(targetNode).district AS district,
       gds.util.asNode(targetNode).zone     AS zone,
       round(totalCost, 1) AS eta_minutes
ORDER BY eta_minutes;

// drop projection when finished
CALL gds.graph.drop('routes', false) YIELD graphName;

// --- Q3: Pure-Cypher fallback (no GDS) — shortest weighted path --------------
// Useful for the report when GDS isn't installed; uses the built-in
// variable-length expansion with a total-distance reduction.
MATCH p = (a:Location {location_id:$from})-[:ROUTE*1..4]->(b:Location {location_id:$to})
WITH p, reduce(km = 0.0, r IN relationships(p) | km + r.distance_km) AS total_km
RETURN [n IN nodes(p) | n.district] AS route, round(total_km, 2) AS total_km
ORDER BY total_km ASC
LIMIT 1;
