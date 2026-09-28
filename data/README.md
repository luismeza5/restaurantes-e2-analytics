# Datos de origen (generador sintético)

`generate_source.py` produce datos con la **misma forma** que persiste Proyecto 1
(`users`, `categories`, `products`, `orders`, `order_items`, `reservations`) como
CSV en `data/source/`, para que el pipeline corra sin levantar el backend.

```bash
python data/generate_source.py --orders 50000 --users 2500 \
       --reservations 8000 --months 14 --seed 42 --out data/source
```

Patrones intencionales para que los análisis tengan señal:

- **Crecimiento mensual**: el volumen de pedidos sube mes a mes.
- **Horas pico**: distribución bimodal almuerzo (~12–13h) / cena (~19–21h), con
  recargo de fin de semana.
- **Geografía**: clientes distribuidos en 12 zonas del GAM con lat/long.
- **Cancelaciones**: ~10% de pedidos cancelados; ~6% de reservas no-show.
- **Referidos**: ~35% de usuarios referidos por uno anterior (DAG de referidos).

Requiere `faker`. La salida es determinista por `--seed`.
