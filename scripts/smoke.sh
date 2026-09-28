#!/usr/bin/env bash
# End-to-end local smoke test (no Docker): seed -> Spark pipeline -> assert outputs.
set -euo pipefail
export PYTHONPATH=. WAREHOUSE_MODE=local LAKE_PATH=.lake \
       SOURCE_KIND=csv SOURCE_PATH=data/source SPARK_LOG_LEVEL=ERROR

echo "[smoke] generating source..."
python data/generate_source.py --orders 2000 --users 200 --reservations 400 --out data/source

echo "[smoke] running Spark pipeline..."
python spark/jobs/extract.py
python spark/jobs/build_dimensions.py
python spark/jobs/build_facts.py
python spark/jobs/analyses.py

echo "[smoke] checking gold outputs exist..."
for t in fact_orders fact_order_items analysis_monthly_growth; do
  ls .lake/gold/$t/*.parquet >/dev/null && echo "  ok gold/$t"
done

echo "[smoke] routing..."
python neo4j/routing/nearest_neighbor.py --couriers 3 --capacity 8 >/dev/null && echo "  ok routing"
echo "[smoke] PASSED"
