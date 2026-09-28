// =============================================================================
// Co-purchase analysis: the 5 products most often bought together
// =============================================================================

// --- Q1: Top 5 product PAIRS bought together --------------------------------
// Two products are "bought together" when the same order contains both.
// p.product_id < q.product_id avoids counting each unordered pair twice.
MATCH (p:Product)<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(q:Product)
WHERE p.product_id < q.product_id
WITH p, q, count(DISTINCT o) AS times_together
RETURN p.name AS product_a, q.name AS product_b, times_together
ORDER BY times_together DESC
LIMIT 5;

// --- Q2: For a given product, its strongest "also bought" companions --------
// Drives a basic recommendation widget ("clientes que pidieron X también pidieron…").
// :param seed => 'Casado con pollo'
MATCH (seed:Product {name:$seed})<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(other:Product)
WHERE other <> seed
WITH other, count(DISTINCT o) AS support
RETURN other.name AS recommended, other.category AS category, support
ORDER BY support DESC
LIMIT 5;

// --- Q3: Category affinity matrix (which categories co-occur in baskets) -----
MATCH (p:Product)<-[:CONTAINS]-(o:Order)-[:CONTAINS]->(q:Product)
WHERE p.category < q.category
WITH p.category AS cat_a, q.category AS cat_b, count(DISTINCT o) AS baskets
RETURN cat_a, cat_b, baskets
ORDER BY baskets DESC
LIMIT 10;
