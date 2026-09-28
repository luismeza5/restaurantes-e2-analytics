# Visualization layer — Apache Superset

Three dashboards are required by the rubric; each is backed by an OLAP cube so
the BI tool only ever reads pre-aggregated, internally-consistent data.

| Dashboard | Source (virtual dataset) | Backing cube |
|-----------|--------------------------|--------------|
| **1. Ingresos por mes y categoría** | `revenue_by_month_category` | `dw.cube_revenue_time_category` |
| **2. Actividad de clientes por zona** | `customer_activity_by_zone` | `dw.cube_customer_activity` |
| **3. Pedidos completados vs cancelados** | `orders_completed_vs_cancelled` | `dw.cube_orders_status` |
| _(bonus)_ Horas pico | `peak_hours_heatmap` | `dw.analysis_peak_hours` |

The SQL behind each dataset lives in [`../sql/`](../sql).

## Option A — automated (recommended)

With the stack up (`make up`), register the database + datasets in one shot:

```bash
make dashboards          # wraps bootstrap_assets.py
# or manually:
SUPERSET_URL=http://localhost:8088 SUPERSET_USER=admin SUPERSET_PASSWORD=admin \
    python dashboards/superset/bootstrap_assets.py
```

Superset is at <http://localhost:8088> (admin/admin by default). The script
creates the **Restaurantes DW** connection and the four virtual datasets.

## Option B — manual

1. **Settings ▸ Database Connections ▸ + Database ▸ PostgreSQL** and paste the
   URI from [`datasources.yaml`](datasources.yaml).
2. **SQL Lab**: paste each file from `../sql/`, run it, then **Save ▸ Save as
   dataset** with the names in the table above.

## Building the dashboards

**Dashboard 1 — Ingresos por mes y categoría**
- Stacked bar — X `month_label`, metric `SUM(revenue)`, breakdown `category`.
- Line — X `month_label`, metric `SUM(revenue)`, series `category`.
- Big Number — `SUM(revenue)`.

**Dashboard 2 — Actividad de clientes por zona**
- Bar — X `zone`, metric `SUM(active_customers)`.
- Heatmap — X `month_label`, Y `zone`, metric `SUM(orders)`.
- Table — `zone`, `SUM(orders)`, `AVG(orders_per_customer)`, `AVG(revenue_per_customer)`.

**Dashboard 3 — Pedidos completados vs cancelados**
- Grouped bar — X `month_label`, metrics `SUM(completed)` and `SUM(cancelled)`.
- Line — X `month_label`, metric `AVG(cancel_rate_pct)`.
- Big Number — overall cancel rate (filter `grp_flag = 3` for the grand-total row).

## Deliverable export

Once built, export each dashboard from **Dashboards ▸ … ▸ Export** and commit the
resulting `.zip` under `assets/` so the dashboards are reproducible by the grader.
Place screenshots there too (referenced from the technical PDF).
