"""Entrega 2 (Databricks) — Selección de un subconjunto de FILAS (< 2 GB) de los CSV originales de MITMA.

CRITERIO
--------
Se conservan TODAS las columnas y el formato original (las filas se copian tal cual, byte a byte).
Solo se eligen filas, con este criterio:

  1. Los 7 DÍAS completos (26-10 a 01-11-2022). P1 compara laborable / fin de semana / festivo
     y P3 necesita la semana entera (media semanal + día anterior): quitar días rompería las preguntas.
  2. Un conjunto S de PROVINCIAS, elegidas por volumen de viajes (las de mayor movilidad primero),
     hasta llenar el límite de tamaño. Se guarda toda fila cuyo ORIGEN o DESTINO esté en S.

Por qué "origen O destino":
  - P1 (viajes por provincia de ORIGEN): se guardan todas las filas que salen de S
    -> las cifras de P1 para las provincias de S son EXACTAS.
  - P2 (top destinos por día) y P3 (desviación de cada municipio de DESTINO): se guardan todas
    las filas que llegan a S -> los viajes recibidos por los municipios de S son EXACTOS.
    Los destinos líderes de España (Madrid, Barcelona, València, Sevilla) están en las provincias
    con más viajes, así que el ranking de P2 sale igual que en local.
  - No se usa un muestreo aleatorio: con él TODAS las cifras serían aproximadas.

Funciona en dos pasadas, solo con la librería estándar de Python (no hace falta pandas):
  pasada 1: mide cuántos bytes y viajes hay por par (provincia origen, provincia destino);
            con eso elige S sin pasarse del límite.
  pasada 2: copia las filas seleccionadas a data/seleccion/viajes/ (mismos nombres de fichero).

USO (desde la carpeta del repo):
    python scripts/seleccion_datos.py
    python scripts/seleccion_datos.py --limite-gb 1.9
    python scripts/seleccion_datos.py --provincias 28 08 46 41      # elegir las provincias a mano
Lee   data/raw/viajes/*_Viajes_municipios.csv, data/raw/municipios/ y data_aux/
Salida data/seleccion/  (7 CSV + aux/ + resumen_seleccion.json)
"""
import argparse
import csv
import json
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

SEP = b"|"
EXTRANJERO = "EX"                     # FRxxx, PTxxx, externo... (fuera de España)


def provincia(codigo: bytes) -> str:
    """Código INE de municipio (5 dígitos) -> provincia (2 primeros dígitos)."""
    return codigo[:2].decode() if len(codigo) >= 5 and codigo[:5].isdigit() else EXTRANJERO


def pasada_1(ficheros):
    """bytes y viajes por par (prov_origen, prov_destino), y viajes por provincia de origen."""
    bytes_par = defaultdict(int)
    viajes_par = defaultdict(float)
    filas_total, bytes_total = 0, 0
    for f in ficheros:
        t = time.time()
        with open(f, "rb") as fh:
            cabecera = fh.readline()
            bytes_total += len(cabecera)
            for line in fh:
                c = line.split(SEP)
                par = (provincia(c[2]), provincia(c[3]))
                bytes_par[par] += len(line)
                try:
                    viajes_par[par] += float(c[13])
                except ValueError:          # viajes vacío o mal formado: no suma (la ETL lo mandará a cuarentena)
                    pass
                filas_total += 1
                bytes_total += len(line)
        print(f"  pasada 1 · {f.name}: {time.time() - t:.0f} s", flush=True)
    return bytes_par, viajes_par, filas_total, bytes_total


def tam_seleccion(S, bytes_par):
    return sum(b for (o, d), b in bytes_par.items() if o in S or d in S)


def elegir_provincias(bytes_par, viajes_par, limite):
    """Provincias ordenadas por viajes con origen en ellas; se añaden mientras quepan."""
    viajes_prov = defaultdict(float)
    for (o, _), v in viajes_par.items():
        if o != EXTRANJERO:
            viajes_prov[o] += v
    orden = sorted(viajes_prov, key=viajes_prov.get, reverse=True)
    S = []
    for p in orden:
        if tam_seleccion(set(S + [p]), bytes_par) > limite:
            break                      # paramos en la primera que no cabe: S = las N mayores
        S.append(p)
    return S, viajes_prov


def pasada_2(ficheros, S, destino_dir):
    S = set(S)
    por_dia = {}
    for f in ficheros:
        t = time.time()
        filas_in = filas_out = 0
        out = destino_dir / f.name
        with open(f, "rb") as fh, open(out, "wb") as fo:
            fo.write(fh.readline())                       # cabecera original
            for line in fh:
                filas_in += 1
                c = line.split(SEP, 4)
                if provincia(c[2]) in S or provincia(c[3]) in S:
                    fo.write(line)                        # la fila tal cual, todas las columnas
                    filas_out += 1
        por_dia[f.name[:8]] = {"filas_original": filas_in, "filas_seleccion": filas_out,
                               "bytes_original": f.stat().st_size, "bytes_seleccion": out.stat().st_size}
        print(f"  pasada 2 · {f.name}: {filas_in:>11,} -> {filas_out:>11,} filas | "
              f"{f.stat().st_size / 2**20:>5.0f} MB -> {out.stat().st_size / 2**20:>5.0f} MB | "
              f"{time.time() - t:.0f} s", flush=True)
    return por_dia


def nombres_provincias(root):
    path = root / "data_aux" / "provincias.csv"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return {r["cod_provincia"]: r["provincia"] for r in csv.DictReader(f)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]),
                    help="carpeta raíz del repo (por defecto, la carpeta padre de scripts/)")
    ap.add_argument("--limite-gb", type=float, default=1.9, help="tamaño máximo de la selección (GB)")
    ap.add_argument("--provincias", nargs="+", help="códigos INE de provincia a mano (p. ej. 28 08 46)")
    ap.add_argument("--salida", default="data/seleccion")
    args = ap.parse_args()

    root = Path(args.repo)
    origen = root / "data" / "raw" / "viajes"
    salida = root / args.salida
    (salida / "viajes").mkdir(parents=True, exist_ok=True)
    ficheros = sorted(origen.glob("*_Viajes_municipios.csv"))
    if len(ficheros) != 7:
        sys.exit(f"Se esperaban 7 CSV diarios en {origen} y hay {len(ficheros)}. "
                 f"Ejecuta antes: python scripts/download_mitma.py")
    limite = int(args.limite_gb * 1024**3)
    nombres = nombres_provincias(root)

    print("Pasada 1: midiendo bytes y viajes por par de provincias...", flush=True)
    bytes_par, viajes_par, filas_total, bytes_total = pasada_1(ficheros)

    S, viajes_prov = elegir_provincias(bytes_par, viajes_par, limite)
    if args.provincias:
        S = [p.zfill(2) for p in args.provincias]
    viajes_total = sum(viajes_par.values())
    print(f"\nProvincias seleccionadas ({len(S)}):")
    acumulado = set()
    for p in S:
        acumulado.add(p)
        print(f"  {p} {nombres.get(p, '?'):28s} {100 * viajes_prov.get(p, 0) / viajes_total:5.1f} % de los viajes"
              f" | selección acumulada {tam_seleccion(acumulado, bytes_par) / 2**30:.2f} GB")
    est = tam_seleccion(set(S), bytes_par)
    if est > limite:
        sys.exit(f"Con esas provincias saldrían {est / 2**30:.2f} GB (> {args.limite_gb} GB). Elige menos.")

    print("\nPasada 2: copiando las filas seleccionadas...", flush=True)
    por_dia = pasada_2(ficheros, S, salida / "viajes")

    (salida / "aux").mkdir(exist_ok=True)
    shutil.copy(root / "data" / "raw" / "municipios" / "nombres_municipios.csv", salida / "aux")
    for name in ("provincias.csv", "calendario.csv"):
        shutil.copy(root / "data_aux" / name, salida / "aux")

    filas_sel = sum(d["filas_seleccion"] for d in por_dia.values())
    bytes_sel = sum(d["bytes_seleccion"] for d in por_dia.values())
    viajes_sel = sum(v for (o, d), v in viajes_par.items() if o in S or d in S)
    resumen = {
        "criterio": "7 días completos; filas con origen o destino en las provincias seleccionadas; "
                    "todas las columnas sin modificar",
        "provincias": {p: nombres.get(p, "?") for p in S},
        "filas_original": filas_total, "filas_seleccion": filas_sel,
        "pct_filas": round(100 * filas_sel / filas_total, 1),
        "bytes_original": bytes_total, "bytes_seleccion": bytes_sel,
        "gb_original": round(bytes_total / 2**30, 2), "gb_seleccion": round(bytes_sel / 2**30, 2),
        "pct_viajes_conservados": round(100 * viajes_sel / viajes_total, 1),
        "cumple_limite_2GB": bytes_sel <= 2 * 1024**3,
        "dias": por_dia,
    }
    (salida / "resumen_seleccion.json").write_text(json.dumps(resumen, indent=2, ensure_ascii=False),
                                                   encoding="utf-8")
    print(f"\nTOTAL: {filas_total:,} -> {filas_sel:,} filas ({resumen['pct_filas']} %) | "
          f"{resumen['gb_original']} GB -> {resumen['gb_seleccion']} GB | "
          f"{resumen['pct_viajes_conservados']} % de los viajes")
    print(f"Para el notebook:  PROVINCIAS_SELECCION = {S}")
    print(f"Resumen en {salida / 'resumen_seleccion.json'}")


if __name__ == "__main__":
    main()
