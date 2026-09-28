// =============================================================================
// Influential users in recommendations (Proyecto 2 §5: "Usuarios que recomiendan
// a otros" + "usuarios influyentes en recomendaciones")
// =============================================================================

// --- Q1: Users who recommend others, ranked by direct referrals -------------
MATCH (u:User)-[:RECOMMENDED]->(referred:User)
WITH u, count(referred) AS direct_referrals
RETURN u.user_id AS user_id, u.name AS name, direct_referrals
ORDER BY direct_referrals DESC
LIMIT 10;

// --- Q2: Reach of a referrer (direct + indirect, the whole referral subtree)-
// Counts every user reachable through a chain of RECOMMENDED edges.
MATCH (u:User)-[:RECOMMENDED*1..]->(d:User)
WITH u, count(DISTINCT d) AS network_size
RETURN u.user_id AS user_id, u.name AS name, network_size
ORDER BY network_size DESC
LIMIT 10;

// --- Q3: GDS PageRank over the referral graph => structural influencers ------
// PageRank rewards being recommended *by influential recommenders*, not just
// raw out-degree. Requires the Graph Data Science plugin (bundled in compose).
CALL gds.graph.project(
  'referrals',
  'User',
  { RECOMMENDED: { orientation: 'NATURAL' } }
) YIELD graphName;

CALL gds.pageRank.stream('referrals')
YIELD nodeId, score
WITH gds.util.asNode(nodeId) AS u, score
RETURN u.user_id AS user_id, u.name AS name, round(score, 4) AS pagerank
ORDER BY pagerank DESC
LIMIT 10;

// tidy up the in-memory projection when done
CALL gds.graph.drop('referrals', false) YIELD graphName;
