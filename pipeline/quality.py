"""Validaciones de calidad sobre las capas construidas."""

import duckdb


def validar(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Devuelve la lista de fallos encontrados; vacía significa que todo pasó."""
    fallos: list[str] = []

    filas_limpias = con.execute("SELECT COUNT(*) FROM ventas_limpias").fetchone()[0]
    if filas_limpias == 0:
        fallos.append("La capa limpia quedó vacía.")

    duplicados = con.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT id_venta) FROM ventas_limpias"
    ).fetchone()[0]
    if duplicados:
        fallos.append(f"Hay {duplicados} id_venta duplicados en la capa limpia.")

    nulos_mart = con.execute(
        "SELECT COUNT(*) FROM mart_ventas_diarias WHERE fecha IS NULL OR categoria IS NULL"
    ).fetchone()[0]
    if nulos_mart:
        fallos.append(f"El mart tiene {nulos_mart} filas con llaves nulas.")

    total_limpio = con.execute("SELECT COALESCE(SUM(monto), 0) FROM ventas_limpias").fetchone()[0]
    total_mart = con.execute("SELECT COALESCE(SUM(total_monto), 0) FROM mart_ventas_diarias").fetchone()[0]
    if total_limpio != total_mart:
        fallos.append(f"Los totales no cuadran: limpia={total_limpio}, mart={total_mart}.")

    return fallos
