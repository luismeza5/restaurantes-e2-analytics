# Guía de instalación y ejecución (desde cero)

Esta guía lleva el proyecto de cero a un sistema funcionando. Hay **dos caminos**:

- **Camino A — Docker (recomendado):** levanta todo el stack (warehouse, Neo4j,
  Airflow, Superset) con un comando. Es el camino para la demostración/entrega.
- **Camino B — Local sin Docker:** corre el pipeline de Spark en tu máquina
  contra un Postgres local. Útil para desarrollo y para entender el flujo.

---

## 0. Requisitos

| Camino | Necesitas |
|--------|-----------|
| A (Docker) | Docker Desktop (o Docker Engine + Compose v2). ~6 GB de RAM libres. |
| B (Local) | Python 3.11, Java 17 (JDK), y opcionalmente PostgreSQL 16. |

Verificar:
```bash
docker --version && docker compose version    # Camino A
python --version && java -version             # Camino B
```

---

## Camino A — Docker (todo el stack)

### A.1 Clonar y configurar
```bash
git clone <URL-de-tu-repo> restaurantes-e2-analytics
cd restaurantes-e2-analytics
cp .env.example .env
```

### A.2 Generar los datos de origen
El repo es autónomo: un generador sintético reproduce la forma de datos de
Proyecto 1, así que no necesitas levantar el backend primero.
```bash
# necesita 'faker' en tu máquina solo para este paso:
pip install faker
python data/generate_source.py --orders 50000 --out data/source
```
> Si no quieres instalar nada local, puedes generarlo dentro del contenedor luego
> de `make up`: `docker compose exec airflow python data/generate_source.py --orders 50000 --out data/source`

### A.3 Levantar el stack
```bash
make up           # build + arranque en segundo plano
# (sin make:  docker compose up -d --build)
```
Servicios y credenciales por defecto:

| Servicio | URL | Usuario / clave |
|----------|-----|------------------|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| Neo4j Browser | http://localhost:7474 | neo4j / neo4jpass |
| Warehouse (Postgres) | localhost:5432 | warehouse / warehouse |

Espera ~1–2 min a que todo quede *healthy* (`docker compose ps`). El esquema del
warehouse y los cubos se crean automáticamente al iniciar `warehouse-db`.

### A.4 Ejecutar el ETL
```bash
make etl          # dispara el DAG 'restaurant_analytics' una vez
```
O desde la UI de Airflow: activa el DAG `restaurant_analytics` y pulsa *Trigger*.
El DAG corre: extract → dimensiones → hechos → análisis → carga warehouse →
(calidad de datos + reindex ES) y en paralelo carga Neo4j.

Verifica que el warehouse se llenó:
```bash
docker compose exec warehouse-db psql -U warehouse -d warehouse -f - < scripts/validate_warehouse.sql
```

### A.5 Cargar el grafo y calcular rutas
```bash
make neo4j                              # carga nodos y relaciones en Neo4j
make routing COURIERS=4 CAPACITY=12     # rutas optimizadas -> routes.json
```
Luego, en Neo4j Browser, ejecuta las consultas de `neo4j/cypher/` (co-compra,
influencers, ruteo). Para las de parámetros usa, por ejemplo:
`:param seed => 'Casado con pollo'` antes de correr la consulta.

### A.6 Dashboards (Superset)
```bash
make dashboards    # registra la conexión y los datasets virtuales
```
Entra a Superset (http://localhost:8088) y construye los 3 dashboards siguiendo
[`dashboards/superset/README.md`](dashboards/superset/README.md):
1. Ingresos por mes y categoría
2. Actividad de clientes por zona
3. Pedidos completados vs cancelados

### A.7 (Opcional) Capa Hive del lakehouse
```bash
make up-lake       # añade el Hive Metastore
# luego registra las tablas externas:
docker compose exec airflow env HIVE_ENABLED=1 \
  spark-submit spark/jobs/register_hive_tables.py
```

### A.8 Apagar
```bash
make down          # detiene contenedores (conserva los datos/volúmenes)
```

---

## Camino B — Local sin Docker

### B.1 Dependencias
```bash
cd restaurantes-e2-analytics
python -m venv .venv && source .venv/bin/activate   # opcional
pip install -r requirements.txt
```

### B.2 Generar datos y correr el pipeline de Spark
```bash
make seed                    # genera data/source (≈50k pedidos)
make etl-local               # Spark local[*] -> lago Parquet en .lake/
```
Esto produce el lago completo: `.lake/bronze`, `.lake/silver`, `.lake/gold`
(dimensiones, hechos y los 3 análisis).

### B.3 (Opcional) Cargar a un Postgres local y refrescar cubos
Con un Postgres 16 corriendo y una base `warehouse`:
```bash
# 1) aplicar el esquema + cubos + índices + tablas de análisis
psql "host=localhost dbname=warehouse user=warehouse" \
  -f warehouse/ddl/00_schema.sql \
  -f warehouse/ddl/10_olap_cubes.sql \
  -f warehouse/ddl/20_indexes.sql \
  -f warehouse/ddl/30_analysis_tables.sql

# 2) cargar el lago al warehouse (sin Spark/JDBC, vía pandas)
WAREHOUSE_TEST_DSN="host=localhost dbname=warehouse user=warehouse password=warehouse" \
LAKE_PATH=.lake python scripts/load_local_warehouse.py
```

### B.4 Rutas y pruebas
```bash
LAKE_PATH=.lake python neo4j/routing/nearest_neighbor.py --couriers 4 --capacity 10
make test          # tests unitarios + integridad
bash scripts/smoke.sh   # prueba end-to-end rápida
```

---

## Conectar el origen real de Proyecto 1 (opcional)

Por defecto `SOURCE_KIND=csv`. Para leer directamente el OLTP de Proyecto 1,
edita `.env`:
```bash
SOURCE_KIND=postgres
SOURCE_JDBC_URL=jdbc:postgresql://<host>:5432/restaurant
SOURCE_USER=...
SOURCE_PASSWORD=...
# o, para MongoDB:
# SOURCE_KIND=mongo
# SOURCE_MONGO_URI=mongodb://<host>:27017
# SOURCE_MONGO_DB=restaurant
```
Para encadenar la reindexación de ElasticSearch de Proyecto 1, define
`PROY1_REINDEX_URL` con el endpoint `POST .../search/reindex`.

---

## Solución de problemas

- **Un contenedor no queda *healthy*:** `docker compose logs <servicio>`.
- **El primer `make etl` tarda:** Spark descarga el driver JDBC de Postgres desde
  Maven la primera vez (la imagen de Airflow intenta pre-cachearlo en el build).
- **Superset no conecta al warehouse:** revisa que la URI apunte a
  `warehouse-db:5432` (dentro de la red de Docker), no a `localhost`.
- **GDS no disponible en Neo4j:** usa el fallback puro-Cypher en
  `neo4j/cypher/40_routing.cypher`.
- **Reset total (borra datos):** `docker compose down -v`.
