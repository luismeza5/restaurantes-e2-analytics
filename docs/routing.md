# Ruteo de reparto

Objetivo: simular y optimizar rutas de entrega usando la
geolocalización de los clientes, asignando rutas por repartidor.

## Enfoque

[`neo4j/routing/nearest_neighbor.py`](../neo4j/routing/nearest_neighbor.py) es
autónomo (lee `bronze/orders` del lago) y procede en tres pasos:

1. **Selección**: toma los pedidos entregables (estado `pending`/`preparing`) con
   sus coordenadas de cliente y su zona GAM.
2. **Asignación a repartidores**: balanceada y consciente de zona. Cada pedido se
   asigna preferentemente a un repartidor que ya cubre su zona y tiene capacidad;
   si no, al menos cargado bajo capacidad; si todos están llenos (más pedidos que
   capacidad total), al menos cargado globalmente. Resultado: carga pareja en vez
   de saturar al primer repartidor.
3. **Secuenciación**: por cada repartidor, una ruta cocina → paradas → cocina
   construida con **vecino más cercano** (NN) y refinada con **2-opt** (búsqueda
   local que invierte segmentos mientras acorte la ruta).

Las distancias usan la métrica **haversine**. La misma secuencia de paradas puede
re-cronometrarse con los tiempos `:ROUTE` de Neo4j para una vista de ETA precisa
(ver [`docs/neo4j.md`](neo4j.md)).

## Métricas reportadas

Por repartidor: número de paradas, zonas cubiertas, kilómetros totales, minutos
estimados, y **% de mejora del 2-opt sobre NN puro** (típicamente 12–24% en los
datos sintéticos). Salida en JSON:

```json
{
  "courier": "repartidor-1",
  "stops": 43, "zones": ["Centro","Este","Oeste"],
  "total_km": 138.34, "est_minutes": 311.5,
  "naive_nn_km": 177.8, "improvement_pct": 22.2,
  "sequence": ["ord-000333", "ord-000884", ...]
}
```

## Alternativa basada en grafo

Para distancias por red vial real (no euclídeas), el módulo se integra con la
malla `:ROUTE` de Neo4j: `gds.shortestPath.dijkstra` da el costo mínimo en
minutos entre ubicaciones, y la secuenciación NN puede usar esa matriz de costos
en lugar de la distancia haversine.
