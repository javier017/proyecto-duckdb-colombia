# Pipeline de ventas con DuckDB

Pipeline de datos en Python que ingiere un CSV, lo organiza en capas (raw, limpia, mart),
valida la calidad y exporta un mart en Parquet. Todo el procesamiento usa **DuckDB** en local,
sin servidor de base de datos.

## Arquitectura

```
CSV (local o URL)
   │  ingest.py       → raw/fecha_ingesta=YYYY-MM-DD/ventas.parquet   (texto, sin transformar)
   ▼
   │  transform.py    → tabla ventas_limpias   (tipos, filtros, deduplicación)
   │                  → tabla mart_ventas_diarias (agregado por día y categoría)
   ▼
   │  quality.py      → validaciones (vacío, duplicados, llaves nulas, totales)
   ▼
   mart/ventas_diarias.parquet                                       (salida)
```

## Decisiones de diseño

- **Raw en texto:** la capa raw no decide tipos. Así un dato malo no se pierde y se puede auditar.
- **Limpieza con `TRY_CAST`:** un valor inválido queda nulo en vez de romper la ejecución, y luego se filtra.
- **Deduplicación con `QUALIFY`:** si una venta llega en varias ingestas, se conserva la más reciente.
- **Validaciones que fallan el proceso:** el pipeline retorna código distinto de cero si algo no cuadra.
- **Pruebas en memoria:** los tests no tocan disco ni requieren infraestructura.

## Ejecución

```bash
python -m venv .venv
source .venv/bin/activate        # En Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Con los datos de muestra incluidos
python -m pipeline.run --fuente data/muestra/ventas_muestra.csv

# Con una fuente remota (reemplaza la URL por la del dataset que elijas)
python -m pipeline.run --fuente https://example.com/ventas.csv
```

Salidas en `data/`:
- `raw/` → Parquet por fecha de ingesta
- `analitica.duckdb` → base con las tablas limpia y mart
- `mart/ventas_diarias.parquet` → resultado final

## Pruebas

```bash
pytest -q
```

## Consultar el resultado

```python
import duckdb

duckdb.sql("""
    SELECT categoria, SUM(total_monto) AS total
    FROM read_parquet('data/mart/ventas_diarias.parquet')
    GROUP BY categoria
    ORDER BY total DESC
""").show()
```

## Próximos pasos

- Cambiar la fuente por un dataset abierto de Colombia (datos.gov.co) y ajustar `ventas_limpias` a sus columnas.
- Programar la ejecución diaria con cron o un orquestador como Airflow.
- Migrar las transformaciones SQL a dbt con `dbt-duckdb`.
