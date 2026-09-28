# =============================================================================
# restaurantes-e2-analytics — developer entrypoints
# =============================================================================
COMPOSE ?= docker compose
PY      ?= python

.DEFAULT_GOAL := help
.PHONY: help up up-lake down logs seed warehouse-init etl etl-local \
        analyses neo4j routing dashboards quality test lint clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## ---- stack lifecycle -------------------------------------------------------
up:  ## Start the core stack (warehouse, neo4j, airflow, superset)
	$(COMPOSE) up -d --build
	@echo "Airflow  → http://localhost:8080  (admin/admin)"
	@echo "Superset → http://localhost:8088  (admin/admin)"
	@echo "Neo4j    → http://localhost:7474  (neo4j/neo4jpass)"

up-lake:  ## Start the core stack + Hive metastore (lakehouse SQL)
	$(COMPOSE) --profile lake up -d --build

down:  ## Stop and remove containers (keep volumes)
	$(COMPOSE) down

logs:  ## Tail Airflow logs
	$(COMPOSE) logs -f airflow

## ---- data + ETL ------------------------------------------------------------
seed:  ## Generate the synthetic OLTP source (override: ORDERS=100000)
	$(PY) data/generate_source.py --orders $(or $(ORDERS),50000) --out data/source

warehouse-init:  ## Apply schema + cubes + indexes to the warehouse
	$(COMPOSE) exec -T warehouse-db psql -U warehouse -d warehouse \
	  -f - < warehouse/ddl/00_schema.sql
	$(COMPOSE) exec -T warehouse-db psql -U warehouse -d warehouse \
	  -f - < warehouse/ddl/10_olap_cubes.sql
	$(COMPOSE) exec -T warehouse-db psql -U warehouse -d warehouse \
	  -f - < warehouse/ddl/20_indexes.sql
	$(COMPOSE) exec -T warehouse-db psql -U warehouse -d warehouse \
	  -f - < warehouse/ddl/30_analysis_tables.sql

etl:  ## Trigger the full Airflow DAG once (inside the stack)
	$(COMPOSE) exec airflow airflow dags trigger restaurant_analytics

neo4j:  ## (Re)load the Neo4j graph from the lake
	$(COMPOSE) exec airflow python neo4j/load/load_graph.py

routing:  ## Compute optimised courier routes (override COURIERS/CAPACITY)
	$(COMPOSE) exec airflow python neo4j/routing/nearest_neighbor.py \
	  --couriers $(or $(COURIERS),4) --capacity $(or $(CAPACITY),10) --out routes.json

dashboards:  ## Register Superset DB + virtual datasets
	$(COMPOSE) exec airflow env SUPERSET_URL=http://superset:8088 \
	  $(PY) dashboards/superset/bootstrap_assets.py

quality:  ## Run warehouse data-quality assertions
	$(COMPOSE) exec airflow python -c "import sys; sys.path.insert(0,'/opt/project'); \
	  from airflow.dags.restaurant_analytics_dag import _data_quality; _data_quality()"

## ---- local dev (no Docker; runs Spark in local[*]) -------------------------
etl-local:  ## Run the Spark pipeline locally against data/source (needs Java+pyspark)
	WAREHOUSE_MODE=local LAKE_PATH=.lake SOURCE_KIND=csv SOURCE_PATH=data/source \
	  PYTHONPATH=. $(PY) spark/jobs/extract.py && \
	WAREHOUSE_MODE=local LAKE_PATH=.lake PYTHONPATH=. $(PY) spark/jobs/build_dimensions.py && \
	WAREHOUSE_MODE=local LAKE_PATH=.lake PYTHONPATH=. $(PY) spark/jobs/build_facts.py && \
	WAREHOUSE_MODE=local LAKE_PATH=.lake PYTHONPATH=. $(PY) spark/jobs/analyses.py

## ---- quality gates ---------------------------------------------------------
test:  ## Run unit + integrity tests
	PYTHONPATH=. pytest -q

lint:  ## Static checks
	ruff check . || true

clean:  ## Remove local lake + generated source
	rm -rf .lake data/source routes.json
