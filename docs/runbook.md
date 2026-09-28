# Runbook operativo

## Arranque (Docker)

```bash
cp .env.example .env
make seed                 # genera data/source/*.csv (≈50k pedidos)
make up                   # warehouse + neo4j + airflow + superset
make up-lake              # (opcional) + Hive Metastore
```

Servicios: Airflow `:8080`, Superset `:8088`, Neo4j `:7474`, warehouse `:5432`.

## Ejecutar el pipeline

- **Vía Airflow**: `make etl` (o desde la UI, DAG `restaurant_analytics`).
- **Local sin Docker**: `make etl-local` (Spark `local[*]` → `.lake/`).

Orden de tareas: `extract → build_dimensions → build_facts → analyses →
load_warehouse → {data_quality, reindex_elasticsearch}`; en paralelo
`build_facts → load_neo4j`.

## Cargar grafo y rutas

```bash
make neo4j                          # carga el grafo desde el lago
make routing COURIERS=4 CAPACITY=12 # rutas optimizadas -> routes.json
```

## Dashboards

```bash
make dashboards     # registra la base y los datasets virtuales en Superset
```
Luego construir los 3 dashboards en la UI (ver
[`dashboards/superset/README.md`](../dashboards/superset/README.md)).

## Conmutar el origen a Proyecto 1

Por defecto `SOURCE_KIND=csv`. Para leer el OLTP real de Proyecto 1:

```bash
# en .env
SOURCE_KIND=postgres
SOURCE_JDBC_URL=jdbc:postgresql://<host>:5432/restaurant
SOURCE_USER=...; SOURCE_PASSWORD=...
# o SOURCE_KIND=mongo con SOURCE_MONGO_URI / SOURCE_MONGO_DB
```

Para encadenar la reindexación de ElasticSearch de Proyecto 1, fijar
`PROY1_REINDEX_URL` al endpoint `POST .../search/reindex`.

## Calidad de datos

`make quality` (o la tarea `data_quality` del DAG) verifica: hechos no vacíos,
sin huérfanos de producto, gran total del cubo == suma de hojas, sin importes
netos negativos. Falla el DAG si algo no cuadra.

## Problemas comunes

- **Superset no ve el warehouse**: confirmar que `warehouse-db` está *healthy*
  (`docker compose ps`) y que la URI del datasource apunta a `warehouse-db:5432`.
- **Spark tarda en el primer run**: descarga el driver JDBC de Postgres desde
  Maven; la imagen de Airflow lo pre-cachea en build.
- **GDS no disponible**: usar el fallback puro-Cypher de `40_routing.cypher` o
  levantar Neo4j con el plugin (ya configurado en `docker-compose.yml`).
