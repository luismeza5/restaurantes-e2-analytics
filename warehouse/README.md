# Warehouse (esquema estrella + cubos OLAP)

DDL idempotente aplicado en orden alfabético (también por el `initdb` de
`warehouse-db` en Docker):

| Archivo | Contenido |
|---------|-----------|
| `ddl/00_schema.sql` | dimensiones conformadas + 3 tablas de hechos |
| `ddl/10_olap_cubes.sql` | 6 cubos (vistas materializadas) + `dw.refresh_cubes()` |
| `ddl/20_indexes.sql` | índices de claves foráneas para star-joins |
| `ddl/30_analysis_tables.sql` | tablas destino de los análisis de Spark |

Ver el detalle del modelo en [`../docs/data-model.md`](../docs/data-model.md) y de
los cubos en [`../docs/olap-cubes.md`](../docs/olap-cubes.md).

Aplicar manualmente:
```bash
make warehouse-init
# o: psql -f ddl/00_schema.sql -f ddl/10_olap_cubes.sql -f ddl/20_indexes.sql -f ddl/30_analysis_tables.sql
```
