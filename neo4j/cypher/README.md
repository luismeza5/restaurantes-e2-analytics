# Catálogo Cypher

| Archivo | Propósito |
|---------|-----------|
| `00_constraints.cypher` | restricciones de unicidad (espejo del cargador) |
| `20_copurchase.cypher` | top-5 comprados juntos · "también compraron" · afinidad de categorías |
| `30_influencers.cypher` | recomendadores directos · alcance de red · PageRank (GDS) |
| `40_routing.cypher` | camino mínimo (Dijkstra GDS) · ETA por zona · fallback puro-Cypher |

Las consultas con parámetros usan `$param` (p. ej. `:param seed => 'Casado con pollo'`
en Neo4j Browser, o el mapa `parameters` vía driver). Detalle en
[`../../docs/neo4j.md`](../../docs/neo4j.md).
