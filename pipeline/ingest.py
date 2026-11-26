"""Capa RAW: descarga el CSV fuente y lo guarda como Parquet sin transformar.

Todas las columnas se leen como texto (all_varchar) para que la capa raw
conserve los datos tal como llegaron; los tipos se asignan en la capa limpia.
"""

from datetime import date
from pathlib import Path

import duckdb
import requests


def descargar_csv(url: str, destino: Path, timeout: int = 60) -> Path:
    """Descarga un CSV desde una URL y lo guarda en 'destino'."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    respuesta = requests.get(url, timeout=timeout)
    respuesta.raise_for_status()
    destino.write_bytes(respuesta.content)
    return destino


def ingestar_raw(origen_csv: Path, raw_dir: Path, fecha_ingesta: date | None = None) -> Path:
    """Convierte el CSV a Parquet dentro de raw/fecha_ingesta=YYYY-MM-DD/."""
    fecha_ingesta = fecha_ingesta or date.today()
    salida = raw_dir / f"fecha_ingesta={fecha_ingesta.isoformat()}" / "ventas.parquet"
    salida.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    try:
        con.execute(
            "COPY (SELECT * FROM read_csv(?, header = true, all_varchar = true)) "
            "TO ? (FORMAT parquet)",
            [str(origen_csv), str(salida)],
        )
    finally:
        con.close()
    return salida
