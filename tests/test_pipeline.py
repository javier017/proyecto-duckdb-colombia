from datetime import date
from pathlib import Path

import duckdb
import pytest

from pipeline.ingest import ingestar_raw
from pipeline.quality import validar
from pipeline.transform import construir_capas

CSV_PRUEBA = """id_venta,fecha,categoria,monto,cantidad
1,2026-11-01,Ropa,100000,2
2,2026-11-01,Ropa,50000,1
2,2026-11-02,Ropa,50000,1
3,fecha_mala,Alimentos,20000,1
4,2026-11-03,Alimentos,-5,1
5,2026-11-03,  Alimentos ,30000,
"""


@pytest.fixture
def pipeline_prueba(tmp_path: Path):
    origen = tmp_path / "ventas.csv"
    origen.write_text(CSV_PRUEBA, encoding="utf-8")
    raw = tmp_path / "raw"
    ingestar_raw(origen, raw, fecha_ingesta=date(2026, 11, 26))

    con = duckdb.connect()  # en memoria: las pruebas no tocan disco
    construir_capas(con, f"{raw.as_posix()}/*/ventas.parquet")
    yield con
    con.close()


def test_raw_conserva_texto(tmp_path: Path):
    origen = tmp_path / "ventas.csv"
    origen.write_text(CSV_PRUEBA, encoding="utf-8")
    salida = ingestar_raw(origen, tmp_path / "raw", fecha_ingesta=date(2026, 11, 26))

    tipos = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{salida.as_posix()}')").fetchall()
    # fecha_ingesta es la columna de partición (hive) que DuckDB infiere; las demás deben ser texto
    columnas_origen = [fila for fila in tipos if fila[0] != "fecha_ingesta"]
    assert columnas_origen and all(fila[1] == "VARCHAR" for fila in columnas_origen)


def test_limpia_descarta_invalidos_y_deduplica(pipeline_prueba):
    ids = sorted(r[0] for r in pipeline_prueba.execute("SELECT id_venta FROM ventas_limpias").fetchall())
    # 1 válido, 2 deduplicado, 3 fecha mala, 4 monto negativo, 5 válido
    assert ids == [1, 2, 5]


def test_limpia_normaliza_categoria_y_cantidad_por_defecto(pipeline_prueba):
    fila = pipeline_prueba.execute(
        "SELECT categoria, cantidad FROM ventas_limpias WHERE id_venta = 5"
    ).fetchone()
    assert fila == ("alimentos", 1)


def test_mart_agrega_por_dia_y_categoria(pipeline_prueba):
    total = pipeline_prueba.execute(
        "SELECT total_monto FROM mart_ventas_diarias WHERE fecha = DATE '2026-11-03'"
    ).fetchone()[0]
    assert float(total) == 30000.0


def test_validacion_pasa_con_datos_correctos(pipeline_prueba):
    assert validar(pipeline_prueba) == []
