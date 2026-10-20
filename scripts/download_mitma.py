"""Descarga reproducible de los datos de MITMA (requisito de la inscripción).

- Descarga los ficheros diarios .csv.gz de la semana definida en config/pipeline.env
  (FECHA_INICIO..FECHA_FIN) en data/downloads/ y los DESCOMPRIME en data/raw/viajes/.
  Motivo: gzip no es divisible -> Spark leería cada fichero con UNA sola task.
- Descarga la tabla de zonificación (nombres de municipios) en data/raw/municipios/.
- Idempotente: lo que ya existe no se vuelve a descargar.
Solo usa la librería estándar de Python. Se ejecuta en el contenedor jupyter:
    docker exec jupyter python /home/jovyan/work/scripts/download_mitma.py
"""
import datetime as dt
import gzip
import shutil
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from common import load_config  # noqa: E402


def fetch(url, dest):
    tmp = dest.with_name(dest.name + ".part")          # si se corta, no deja un fichero a medias
    print(f"  descargando {url}")
    with urllib.request.urlopen(url) as r, open(tmp, "wb") as f:
        shutil.copyfileobj(r, f, length=1 << 20)
    tmp.rename(dest)


def main():
    cfg = load_config()
    downloads = ROOT / cfg["LOCAL_DOWNLOADS"]
    viajes = ROOT / cfg["LOCAL_VIAJES"]
    municipios = ROOT / cfg["LOCAL_MUNICIPIOS"]
    for d in (downloads, viajes, municipios):
        d.mkdir(parents=True, exist_ok=True)

    day = dt.date.fromisoformat(cfg["FECHA_INICIO"])
    end = dt.date.fromisoformat(cfg["FECHA_FIN"])
    while day <= end:
        ymd, ym = day.strftime("%Y%m%d"), day.strftime("%Y-%m")
        csv_path = viajes / f"{ymd}_Viajes_municipios.csv"
        gz_path = downloads / f"{ymd}_Viajes_municipios.csv.gz"
        if csv_path.exists() and csv_path.stat().st_size > 0:
            print(f"  ya existe {csv_path.name}")
        else:
            if not gz_path.exists():
                fetch(cfg["URL_VIAJES"].format(ym=ym, ymd=ymd), gz_path)
            print(f"  descomprimiendo {gz_path.name}")
            with gzip.open(gz_path, "rb") as src, open(csv_path, "wb") as dst:
                shutil.copyfileobj(src, dst, length=1 << 20)
        day += dt.timedelta(days=1)

    mun_path = municipios / "nombres_municipios.csv"
    if mun_path.exists():
        print(f"  ya existe {mun_path.name}")
    else:
        fetch(cfg["URL_MUNICIPIOS"], mun_path)
    print("Descarga completa.")


if __name__ == "__main__":
    main()
