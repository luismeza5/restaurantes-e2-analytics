# Analítica de grafos (Neo4j)

## Modelo de grafo

```
(:User)-[:LIVES_IN]->(:Location)
(:User)-[:RECOMMENDED]->(:User)            // DAG de referidos → influencia
(:User)-[:PLACED]->(:Order)
(:Order)-[:CONTAINS {quantity}]->(:Product)
(:Location)-[:ROUTE {distance_km, minutes}]->(:Location)   // malla de geonodos
```

El cargador [`neo4j/load/load_graph.py`](../neo4j/load/load_graph.py) lee el lago
(zona `bronze`), hace recarga total idempotente (`MATCH (n) DETACH DELETE n`),
crea restricciones de unicidad y construye nodos y relaciones por lotes con
`UNWIND`. La malla `:ROUTE` se genera calculando la distancia haversine entre
todos los pares de ubicaciones y un tiempo estimado (`distancia / velocidad`).

## Consultas (en [`neo4j/cypher/`](../neo4j/cypher))

### Co-compra — `20_copurchase.cypher`
- **Top 5 productos comprados juntos**: pares de productos que aparecen en el
  mismo pedido, contando pedidos distintos.
- **"También compraron"**: para un producto semilla, sus acompañantes más
  frecuentes (base de un recomendador).
- **Afinidad de categorías**: qué categorías co-ocurren en las canastas.

### Influencia en recomendaciones — `30_influencers.cypher`
- **Recomendadores directos**: usuarios ordenados por número de referidos.
- **Alcance de la red**: tamaño del subárbol de referidos (`RECOMMENDED*1..`).
- **PageRank (GDS)**: influencia estructural sobre el grafo de referidos —
  premia ser recomendado por recomendadores influyentes, no solo el grado.

### Ruteo — `40_routing.cypher`
- **Camino mínimo** entre dos ubicaciones (Dijkstra de GDS, peso = minutos).
- **ETA por zona**: Dijkstra de fuente única desde la cocina a cada zona.
- **Fallback sin GDS**: expansión de caminos de longitud variable con reducción
  de distancia total, para entornos sin el plugin instalado.

## Graph Data Science

El `docker-compose` levanta Neo4j con el plugin GDS habilitado
(`NEO4J_PLUGINS='["graph-data-science"]'`) y los procedimientos `gds.*` en la
allowlist. Las consultas proyectan grafos en memoria (`gds.graph.project`),
ejecutan el algoritmo y liberan la proyección (`gds.graph.drop`).
