# Arquitectura

## Visión general

`restaurantes-e2-analytics` es la plataforma analítica (Proyecto 2) que consume
los datos del backend transaccional `polyglot-restaurant-platform` (Proyecto 1). Sigue una
arquitectura **lakehouse + data warehouse** con tres capas:

1. **Ingesta / lago de datos (medallion):** Spark extrae el origen OLTP y lo
   materializa en Parquet en tres zonas: `bronze` (copia cruda), `silver`
   (dimensiones conformadas y limpias) y `gold` (tablas de hechos y resultados
   de análisis).
2. **Servicio (warehouse):** las dimensiones y hechos `gold` se cargan a un
   **esquema estrella en PostgreSQL** sobre el que se construyen **6 cubos OLAP**
   como vistas materializadas (`ROLLUP`/`CUBE`/`GROUPING SETS`). Es la capa de
   baja latencia que lee Superset.
3. **Analítica de grafos:** el mismo `gold`/`bronze` alimenta **Neo4j** (patrones
   de co-compra, influencia por recomendaciones y malla de geonodos para ruteo).

La orquestación es responsabilidad de un **DAG de Apache Airflow** que ejecuta el
flujo completo y, opcionalmente, dispara la reindexación de ElasticSearch en
Proyecto 1 cuando cambia el catálogo.

## Decisión de diseño: PostgreSQL (servicio) + Hive (lago)

El enunciado pide un almacén "con herramientas open source como Apache Hive".
La solución combina **ambos enfoques** en lugar de elegir uno solo:

- **Hive Metastore + Parquet** registra el lago como tablas externas, dejando los
  datos columnar abiertos a cualquier motor compatible con el metastore
  (Spark SQL, Trino). Esta es la "bodega tipo Hive" del lakehouse.
- **PostgreSQL con vistas materializadas** ofrece OLAP multinivel real
  (`ROLLUP`/`CUBE`) con refresco sub-segundo, índices únicos para
  `REFRESH ... CONCURRENTLY` y conectividad BI sólida. Es la capa de servicio.

Ventajas: confiabilidad para la demostración y los dashboards (Postgres no
depende de un cluster Hadoop), apertura del dato crudo (Hive), y separación
limpia entre cómputo (Spark) y servicio (Postgres). Hive es un **perfil opcional**
de `docker-compose` (`make up-lake`) para no fragilizar el arranque base.

## Flujo de datos

```
OLTP (Proyecto 1 / generador)
   │  extract.py            (SOURCE_KIND = csv | postgres | mongo)
   ▼
bronze/*  (Parquet, copia cruda)
   │  build_dimensions.py
   ▼
silver/dim_*  (dimensiones conformadas, claves sustitutas)
   │  build_facts.py
   ▼
gold/fact_*  +  analyses.py → gold/analysis_*
   │  load_warehouse.py (JDBC)        load_graph.py (bolt)
   ▼                                   ▼
PostgreSQL dw.*  →  refresh_cubes()    Neo4j (User/Order/Product/Location)
   │                                   │
   ▼                                   ▼
Superset (3 dashboards)               Cypher / GDS (co-compra, PageRank, Dijkstra)
```

## Escalabilidad

Los jobs de Spark corren con `spark-submit --master local[*]` dentro del worker
de Airflow, suficiente para el volumen del curso. Para escalar horizontalmente
basta apuntar `SPARK_MASTER` a un cluster standalone/YARN/K8s sin cambiar el
código: la E/S es por ruta de Parquet y la lógica vive en funciones puras.
