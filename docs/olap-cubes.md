# Capa OLAP — cubos

Seis cubos implementados como **vistas materializadas** sobre el esquema estrella,
usando operadores OLAP genuinos de SQL (`ROLLUP`, `CUBE`, `GROUPING SETS`). Cada
cubo incluye una columna `grp_flag = GROUPING(...)` que permite a los dashboards
distinguir filas hoja de subtotales y del gran total.

| Cubo | Operador | Ejes (dimensiones) | Cobertura del rubro |
|------|----------|--------------------|---------------------|
| `cube_revenue_time_category` | `GROUPING SETS` | año › trimestre › mes × categoría | tiempo + tipo de producto |
| `cube_sales_by_location` | `ROLLUP` | provincia › cantón › distrito (+ zona) | ubicación |
| `cube_product_frequency` | `GROUPING SETS` | categoría × producto × mes | tipo de producto + frecuencia de uso |
| `cube_orders_status` | `CUBE` | mes × zona | completados vs cancelados |
| `cube_peak_hours` | `GROUPING SETS` | día de semana × hora | tiempo (horas pico) |
| `cube_customer_activity` | `GROUPING SETS` | zona × provincia × mes | ubicación + frecuencia |

## Interpretación de `grp_flag`

`GROUPING(a, b, …)` produce un bitmask: el bit es 1 cuando esa columna está
agregada (NULL en la fila). Ejemplos:

- `cube_revenue_time_category` con 4 columnas agrupadas → `grp_flag = 0` son
  hojas (año+trimestre+mes+categoría); `grp_flag = 15` es el **gran total**.
- `cube_orders_status` con 2 columnas (`CUBE`) → `grp_flag = 0` hoja (mes×zona),
  `1` subtotal por mes, `2` subtotal por zona, `3` gran total.

Los datasets de Superset filtran `grp_flag = 0` para graficar al grano más fino y
leen las filas de subtotal/gran total para los "big numbers".

## Refresco

`dw.refresh_cubes()` refresca los seis cubos. Usa `REFRESH MATERIALIZED VIEW
CONCURRENTLY` (habilitado por los índices únicos de cada cubo) y, en el primer
build cuando aún no hay datos materializados, cae a un refresco no concurrente
mediante un manejador de excepción. El DAG lo invoca tras cada carga.

## Validación de consistencia

Una propiedad invariante: **la suma de las hojas debe igualar el gran total**.
Se verifica en `tests/test_warehouse_integrity.py` y en la tarea `data_quality`
del DAG (`|gran_total − Σ hojas| < 0.01`).
