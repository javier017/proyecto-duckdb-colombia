"""Punto de entrada: ejecuta el pipeline completo raw -> limpia -> mart.

Ejemplos:
    python -m pipeline.run --fuente data/muestra/ventas_muestra.csv
    python -m pipeline.run --fuente https://ejemplo.gov.co/ventas.csv
"""

import argparse
import sys
from pathlib import Path

import duckdb

from pipeline.ingest import descargar_csv, ingestar_raw
from pipeline.quality import validar
from pipeline.transform import construir_capas, exportar_mart


def ejecutar(fuente: str, data_dir: Path) -> int:
    raw_dir = data_dir / "raw"
    mart_dir = data_dir / "mart"
    db_path = data_dir / "analitica.duckdb"

    if fuente.startswith(("http://", "https://")):
        csv_local = descargar_csv(fuente, data_dir / "fuente" / "ventas.csv")
    else:
        csv_local = Path(fuente)

    print(f"[1/4] Ingesta raw desde {csv_local}")
    ingestar_raw(csv_local, raw_dir)

    print("[2/4] Construyendo capas limpia y mart")
    con = duckdb.connect(str(db_path))
    try:
        construir_capas(con, f"{raw_dir.as_posix()}/*/ventas.parquet")

        print("[3/4] Validando calidad")
        fallos = validar(con)
        if fallos:
            for fallo in fallos:
                print(f"  FALLO: {fallo}")
            return 1

        print("[4/4] Exportando mart a Parquet")
        salida = exportar_mart(con, mart_dir / "ventas_diarias.parquet")
        print(f"Listo: {salida}")
    finally:
        con.close()
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline de ventas con DuckDB")
    parser.add_argument("--fuente", required=True, help="Ruta local o URL del CSV")
    parser.add_argument("--data-dir", default="data", type=Path, help="Carpeta de salida")
    args = parser.parse_args()
    sys.exit(ejecutar(args.fuente, args.data_dir))


if __name__ == "__main__":
    main()
