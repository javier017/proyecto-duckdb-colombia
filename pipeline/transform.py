"""Capas LIMPIA y MART construidas con SQL dentro de DuckDB."""

from pathlib import Path

import duckdb

SQL_LIMPIA = """
CREATE OR REPLACE TABLE ventas_limpias AS
WITH base AS (
    SELECT
        TRY_CAST(id_venta AS BIGINT)          AS id_venta,
        TRY_CAST(fecha AS DATE)               AS fecha,
        TRIM(LOWER(categoria))                AS categoria,
        TRY_CAST(monto AS DECIMAL(12, 2))     AS monto,
        COALESCE(TRY_CAST(cantidad AS INTEGER), 1) AS cantidad
    FROM read_parquet(?, union_by_name = true)
)
SELECT *
FROM base
WHERE id_venta IS NOT NULL
  AND fecha IS NOT NULL
  AND monto IS NOT NULL
  AND monto >= 0
-- Si la misma venta llega en varias ingestas, nos quedamos con la más reciente
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_venta ORDER BY fecha DESC) = 1
"""

SQL_MART = """
CREATE OR REPLACE TABLE mart_ventas_diarias AS
SELECT
    fecha,
    categoria,
    COUNT(*)        AS num_ventas,
    SUM(monto)      AS total_monto,
    SUM(cantidad)   AS unidades
FROM ventas_limpias
GROUP BY fecha, categoria
ORDER BY fecha, categoria
"""


def construir_capas(con: duckdb.DuckDBPyConnection, raw_glob: str) -> None:
    """Crea las tablas ventas_limpias y mart_ventas_diarias en la conexión dada."""
    con.execute(SQL_LIMPIA, [raw_glob])
    con.execute(SQL_MART)


def exportar_mart(con: duckdb.DuckDBPyConnection, destino: Path) -> Path:
    """Escribe el mart en Parquet para consumo por BI o notebooks."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    con.execute("COPY mart_ventas_diarias TO ? (FORMAT parquet)", [str(destino)])
    return destino
