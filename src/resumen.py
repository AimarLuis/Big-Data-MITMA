"""Métricas del pipeline en formato legible, por consola.

Se usa de dos formas:
- Durante la ejecución: Evidence.record() (common.py) llama a lineas(clave, valor) y cada
  métrica se imprime en cuanto se calcula, debajo de la etapa que la produce.
- Al final: el punto de entrada ejecuta `python src/resumen.py`, que lee evidencias/metrics.json
  y muestra las cifras clave de cada apartado (A a F) en un único bloque.

Solo usa la biblioteca estándar, así que no necesita Spark para ejecutarse.
"""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVID = ROOT / "evidencias"


# ----------------------------------------------------------------------------- formato de números
def num(x, dec=0):
    """78340906 -> '78.340.906';  6.93 (dec=2) -> '6,93'  (formato español)."""
    if x is None:
        return "-"
    s = f"{x:,.{dec}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")


def gb(b):
    return num(b / 1e9, 2) + " GB"


def seg(t):
    return num(t, 2 if t < 10 else 1) + " s"


def veces(lento, rapido):
    return "x" + num(lento / rapido, 1) if rapido else "-"


def _celda(x, entero=False):
    """'20426994.0' -> '20.426.994';  '-22.3' -> '-22,3';  un texto se deja igual."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return str(x), False
    return num(v, 0 if entero or abs(v) >= 1000 or "." not in str(x) else 1), True


def tabla(filas, columnas, max_filas=5, sangria="      "):
    """Tabla de texto con las primeras filas de un CSV de resultados.
    columnas = [(columna_del_csv, cabecera), ...]; los números van alineados a la derecha."""
    filas = filas[:max_filas]
    celdas = [[_celda(f.get(c, ""), entero=c.startswith(("viajes", "media_"))) for c, _ in columnas]
              for f in filas]
    anchos = [max([len(h)] + [len(fila[i][0]) for fila in celdas]) for i, (_, h) in enumerate(columnas)]
    out = [sangria + "  ".join(h.ljust(a) for (_, h), a in zip(columnas, anchos))]
    for fila in celdas:
        out.append(sangria + "  ".join(t.rjust(a) if es_num else t.ljust(a)
                                       for (t, es_num), a in zip(fila, anchos)))
    return out


def leer_csv(nombre):
    path = EVID / "resultados" / f"{nombre}.csv"
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ----------------------------------------------------------------------------- una función por métrica
def _smoke(v):
    return [f"Spark {v['spark_version']} (Python {v['python']}) en {v['master']}, {v['executors']} executor(s)",
            f"HDFS: escribe y vuelve a leer {v['filas_leidas']} filas -> OK"]


REGLAS = [("regla_fecha", "fecha válida y dentro de la semana"),
          ("regla_periodo", "periodo (hora) entre 0 y 23"),
          ("regla_origen_destino", "origen y destino no nulos"),
          ("regla_viajes", "viajes no nulos y > 0"),
          ("regla_viajes_km", "viajes_km >= 0 (o nulo)")]


def _etl_entrada(v):
    out = [f"raw: {num(v['filas_raw'])} filas, {num(v['viajes_raw'] / 1e6, 1)} M viajes",
           f"nulos en fecha / periodo / viajes: {v['nulos_fecha']} / {v['nulos_periodo']} / {v['nulos_viajes']}",
           f"edad NA: {num(v['edad_NA'])}  |  sexo NA: {num(v['sexo_NA'])}  |  "
           f"filas con zona extranjera: {num(v['filas_con_zona_extranjera'])} (se conservan y se marcan)"]
    if all(k in v for k, _ in REGLAS):
        out.append(f"{'control de calidad':<38}{'filas que fallan':>17}   acción")
        for k, texto in REGLAS:
            out.append(f"{texto:<38}{num(v[k]):>17}   -> cuarentena")
    return out


def _etl_cuarentena(v):
    accion = "DETENER" if v["proporcion"] > v["umbral_detener"] else "continuar"
    return [f"cuarentena: {num(v['filas'])} filas ({num(100 * v['proporcion'], 3)} %) -> {accion} "
            f"(se detiene por encima del {num(100 * v['umbral_detener'], 0)} %)"]


def _etl_curated(v):
    return [f"curated: {num(v['filas_curated'])} filas en {v['fechas']} particiones fecha=..., "
            f"{num(v['viajes_curated'] / 1e6, 1)} M viajes",
            f"duplicados (clave natural de 13 columnas): {num(v['duplicados_eliminados'])} -> eliminar y registrar"]


def _etl_aux(v):
    return [f"tabla de municipios: {num(v['municipios_en_tabla'])} códigos ({v['claves_repetidas']} repetidos); "
            f"orígenes sin nombre: {v['origenes_sin_nombre']} de {num(v['origenes_espanoles_distintos'])} -> {v['accion']} (avisa por encima del 1 %)"]


def _formatos(v):
    a, b = v["A_todas_las_filas_2_columnas"], v["B_un_solo_dia"]
    return [f"tamaño: CSV {gb(v['tamano_csv_bytes'])} vs Parquet {gb(v['tamano_parquet_bytes'])} "
            f"(Parquet ocupa {num(v['tamano_csv_bytes'] / v['tamano_parquet_bytes'], 1)} veces menos)",
            f"consulta A (todas las filas, 2 columnas): CSV {seg(a['csv']['mediana_s'])} vs Parquet "
            f"{seg(a['parquet']['mediana_s'])} -> {veces(a['csv']['mediana_s'], a['parquet']['mediana_s'])}",
            f"consulta B (un solo día):                 CSV {seg(b['csv']['mediana_s'])} vs Parquet "
            f"{seg(b['parquet']['mediana_s'])} -> {veces(b['csv']['mediana_s'], b['parquet']['mediana_s'])}",
            f"particiones al leer los 7 días: CSV {v['particiones_lectura']['csv_7_dias']}, "
            f"Parquet {v['particiones_lectura']['parquet_7_dias']}  (medianas de {len(a['csv']['tiempos_s'])} ejecuciones)"]


def _gzip(v):
    return [f"un día en .gz: {v['particiones_gz']} partición ({seg(v['conteo_gz']['mediana_s'])}) vs CSV: "
            f"{v['particiones_csv']} particiones ({seg(v['conteo_csv']['mediana_s'])}) -> gzip no es divisible"]


def _p1(n):
    filas = leer_csv("d2_p1_provincia_tipo_dia")
    out = [f"P1 · viajes medios diarios por provincia y tipo de día: {n} provincias. Las 3 con más viajes:"]
    if filas:
        out += tabla(filas, [("provincia", "provincia"), ("media_laborable", "laborable"),
                             ("media_fin_de_semana", "fin de semana"), ("festivo_1_nov", "festivo 1-nov"),
                             ("finde_vs_laborable_pct", "finde vs lab. %"),
                             ("festivo_vs_laborable_pct", "festivo vs lab. %")], 3)
    return out


def _p2(n):
    filas = leer_csv("d2_p2_top_destinos_dia")
    out = [f"P2 · municipios que más viajes reciben cada día: {n} filas (7 días x top N). Primer día:"]
    if filas:
        dia = filas[0]["fecha"]
        out += tabla([f for f in filas if f["fecha"] == dia], [("puesto", "puesto"), ("nombre_municipio", "municipio"), ("viajes_recibidos", "viajes recibidos")])
    return out


def _p3(n):
    out = [f"P3 · desviación de cada municipio grande respecto a su media semanal: {num(n)} filas"]
    resumen = leer_csv("d2_p3_resumen_por_dia")
    if resumen:
        out.append("   mediana de la desviación por día:")
        out += tabla(resumen, [("fecha", "fecha"), ("municipios_grandes", "municipios"),
                               ("mediana_desviacion_pct", "mediana desv. %"),
                               ("municipios_mas_de_20pct_por_debajo", "con más de -20 %")], 7)
    caidas = leer_csv("d2_p3_mayores_caidas_festivo")
    if caidas:
        out.append("   mayores caídas el festivo:")
        out += tabla(caidas, [("nombre_municipio", "municipio"), ("viajes_dia", "viajes 1-nov"),
                               ("media_semana", "media semana"), ("desviacion_pct", "desviación %")], 3)
    return out


def _d3(v):
    a, d = v["antes_sort_merge"], v["despues_broadcast"]
    return [f"D3 · P1 con sort-merge join: {seg(a['mediana_s'])}, shuffle write "
            f"{num(a['stages_spark_ui']['shuffle_write_total_MB'], 0)} MB, {a['stages_spark_ui']['stages_ejecutados']} stages",
            f"     P1 con broadcast join:  {seg(d['mediana_s'])}, shuffle write "
            f"{num(d['stages_spark_ui']['shuffle_write_total_MB'], 0)} MB, {d['stages_spark_ui']['stages_ejecutados']} stages",
            f"     mejora: -{num(v['mejora_pct'], 1)} % de tiempo"]


def _mr_rdd(v):
    out = [f"RDD (reduceByKey): {seg(v['rdd_tiempo']['mediana_s'])}, {v['rdd_claves']} claves provincia|fecha"]
    if "streaming_tiempo_ms" in v:
        t_mr = v["streaming_tiempo_ms"] / 1000
        out.append(f"Hadoop Streaming: {seg(t_mr)} -> el RDD es {veces(t_mr, v['rdd_tiempo']['mediana_s'])} más rápido")
    c = v.get("comparacion_con_streaming")
    if isinstance(c, str):
        out.append(f"comparación con Streaming: {c}")
    elif c:
        out.append(f"comparación: {c['claves']} claves, {c['claves_solo_en_una_version']} solo en una versión, "
                   f"diferencia máxima {num(c['max_diferencia_abs'], 4)} viajes")
    loc = v["lineas_de_codigo"]
    out.append(f"líneas de código: Streaming (mapper + reducer) {loc['streaming_mapper_mas_reducer']} vs RDD {loc['rdd']}")
    return out


def _ml(v):
    lr, g, m = v["regresion_lineal"], v["baseline_media_global"], v["baseline_media_municipio"]
    f = v["filas"]
    return [f"dataset: {num(f['total'])} filas (municipio x edad) -> train {num(f['train'])} / test {num(f['test'])}; "
            f"{num(v['n_features'])} variables tras one-hot",
            f"objetivo: {v['objetivo']}",
            f"{'modelo':<28}{'RMSE':>8}{'MAE':>8}{'R2':>8}",
            f"{'regresión lineal':<28}{num(lr['rmse'], 3):>8}{num(lr['mae'], 3):>8}{num(lr['r2'], 3):>8}",
            f"{'base: media global':<28}{num(g['rmse'], 3):>8}{num(g['mae'], 3):>8}{num(abs(g['r2']), 3):>8}",
            f"{'base: media del municipio':<28}{num(m['rmse'], 3):>8}{num(m['mae'], 3):>8}{num(m['r2'], 3):>8}"]


def _f3(v):
    out = [f"{'variante':<28}{'ficheros':>9}{'MB/fich.':>10}{'escritura':>11}{'lectura':>9}{'tasks':>7}"]
    for nombre, r in v.items():
        out.append(f"{nombre:<28}{num(r['ficheros']):>9}{num(r['tamano_medio_MB'], 1):>10}"
                   f"{seg(r['escritura']['mediana_s']):>11}{seg(r['lectura_suma_por_dia']['mediana_s']):>9}"
                   f"{r['particiones_al_leer']:>7}")
    return out


FORMATOS = {
    "a3_smoke": _smoke,
    "etl_1_entrada": _etl_entrada,
    "etl_2_cuarentena": _etl_cuarentena,
    "etl_3_curated": _etl_curated,
    "etl_4_tabla_auxiliar": _etl_aux,
    "b2_formatos": _formatos,
    "b2_gzip_vs_csv_un_dia": _gzip,
    "d2_p1_filas": _p1,
    "d2_p2_filas": _p2,
    "d2_p3_filas": _p3,
    "d3_optimizacion_broadcast": _d3,
    "c_mapreduce_vs_rdd": _mr_rdd,
    "e3_ml": _ml,
    "f3_archivos_pequenos": _f3,
}


def lineas(clave, valor):
    """Líneas legibles para una métrica. Si algo falla al formatear, se muestra el valor tal cual:
    imprimir nunca debe romper el pipeline."""
    if clave.startswith("tiempo_etapa_"):
        return [f"tiempo de la etapa: {seg(valor)}"]
    try:
        return FORMATOS[clave](valor)
    except Exception:
        return [f"{clave} = {json.dumps(valor, ensure_ascii=False, default=str)[:300]}"]


# ----------------------------------------------------------------------------- resumen final
SECCIONES = [
    ("A.3 · Entorno", ["a3_smoke"]),
    ("D.1 · ETL y calidad (raw -> curated)", ["etl_1_entrada", "etl_2_cuarentena", "etl_3_curated", "etl_4_tabla_auxiliar"]),
    ("B.2 · Formatos (CSV frente a Parquet)", ["b2_formatos", "b2_gzip_vs_csv_un_dia"]),
    ("D.2 · Preguntas analíticas", ["d2_p1_filas", "d2_p2_filas", "d2_p3_filas"]),
    ("D.3 · Optimización", ["d3_optimizacion_broadcast"]),
    ("C · MapReduce (Hadoop Streaming) frente a RDD", ["c_mapreduce_vs_rdd"]),
    ("E.3 · Machine Learning (MLlib)", ["e3_ml"]),
    ("F.3 · Desafío: archivos pequeños", ["f3_archivos_pequenos"]),
]
ETAPAS = ["smoke", "etl", "formats", "analytics", "mr_rdd", "ml", "challenge"]


def main():
    path = EVID / "metrics.json"
    if not path.exists():
        print("No hay evidencias/metrics.json todavía: ejecuta antes el pipeline.")
        return
    m = json.loads(path.read_text(encoding="utf-8"))
    print("=" * 78)
    print(" RESUMEN DE MÉTRICAS (evidencias/metrics.json)")
    print("=" * 78)
    for titulo, claves in SECCIONES:
        presentes = [k for k in claves if k in m]
        if not presentes:
            continue
        print(f"\n-- {titulo}")
        for k in presentes:
            for linea in lineas(k, m[k]):
                print("   " + linea)
    tiempos = [(e, m[f"tiempo_etapa_{e}_s"]) for e in ETAPAS if f"tiempo_etapa_{e}_s" in m]
    if tiempos:
        print("\n-- Tiempo de cada etapa de Spark")
        for e, t in tiempos:
            print(f"   {e:<10}{seg(t):>10}")
    print("\nDetalle: evidencias/metrics.json · resultados: evidencias/resultados/ · planes: evidencias/plan_*.txt")


if __name__ == "__main__":
    main()
