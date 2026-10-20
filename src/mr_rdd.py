"""C.1 + C.2 — La MISMA agregación que el job de Hadoop Streaming, con la API de RDDs.

Agregación: viajes totales por provincia de origen y fecha (clave "PP|AAAAMMDD").
Estilo de la clase 4 ("RDD version: read the CSV as plain text lines"):
    lines = sc.textFile(...); header = lines.first()
    lines.filter(lambda l: l != header).map(split).map(-> (clave, valor)).reduceByKey(+)
Después se compara con la salida de MapReduce (si ya existe en HDFS): mismas claves y
mismos totales.
"""
import inspect
import json

from common import ROOT, hdfs, log, measure


def provincia_fecha_rdd(sc, path, sep):
    lines = sc.textFile(path)
    header = lines.first()                         # los 7 ficheros tienen la misma cabecera
    return (lines.filter(lambda l: l != header)                       # fuera las cabeceras
                 .map(lambda l: l.split(sep))                         # línea -> lista de campos
                 .filter(lambda f: f[2][:2].isdigit())                # fuera zonas extranjeras
                 .map(lambda f: (f[2][:2] + "|" + f[0], float(f[13])))  # (provincia|fecha, viajes)
                 .reduceByKey(lambda a, b: a + b))                    # combina en el map y luego shuffle


def lines_of_code(text):
    return sum(1 for l in text.splitlines()
               if l.strip() and not l.strip().startswith(("#", '"""')) and '"""' not in l)


def run(spark, cfg, ev):
    sc = spark.sparkContext
    # Cada repetición construye un RDD NUEVO. Si se repitiera count() sobre el mismo RDD, Spark
    # reutilizaría los ficheros de shuffle de la ejecución anterior (el stage de map aparece como
    # "skipped" en la Spark UI) y solo se mediría el reduce.
    log(f"   textFile -> map -> filter -> reduceByKey sobre raw, {cfg['BENCH_REPEATS']} repeticiones...")
    tiempo = measure(lambda: provincia_fecha_rdd(sc, hdfs(cfg, "RAW_VIAJES"), cfg["CSV_SEP"]).count(),
                     cfg["BENCH_REPEATS"], "RDD reduceByKey")
    rdd = provincia_fecha_rdd(sc, hdfs(cfg, "RAW_VIAJES"), cfg["CSV_SEP"])
    resultado = rdd.mapValues(lambda v: round(v, 3)).collect()           # 52 provincias x 7 días: pequeño
    filas = sorted(f"{k}\t{v}" for k, v in resultado)
    (ROOT / cfg["EVIDENCIAS"] / "resultados" / "c_rdd_provincia_fecha.tsv").write_text("\n".join(filas) + "\n")

    loc_streaming = sum(lines_of_code((ROOT / f).read_text()) for f in ("mapreduce/mapper.py", "mapreduce/reducer.py"))
    loc_rdd = lines_of_code(inspect.getsource(provincia_fecha_rdd))
    res = {"rdd_tiempo": tiempo, "rdd_claves": len(resultado),
           "lineas_de_codigo": {"streaming_mapper_mas_reducer": loc_streaming, "rdd": loc_rdd}}

    # ---- Comparación con la salida de Hadoop Streaming ------------------------------------
    mr_time = ROOT / cfg["EVIDENCIAS"] / "mr_streaming_time.json"
    try:
        mr = (sc.textFile(hdfs(cfg, "MR_OUTPUT"))
                .map(lambda l: l.split("\t"))
                .map(lambda kv: (kv[0], float(kv[1]))))
        log("   comparando con la salida de Hadoop Streaming (fullOuterJoin por clave)...")
        joined = rdd.fullOuterJoin(mr).collect()
        diffs = [abs((a or 0) - (b or 0)) for _, (a, b) in joined]
        res["comparacion_con_streaming"] = {
            "claves": len(joined),
            "claves_solo_en_una_version": sum(1 for _, (a, b) in joined if a is None or b is None),
            "max_diferencia_abs": round(max(diffs), 6) if diffs else None,
        }
        if mr_time.exists():
            res["streaming_tiempo_ms"] = json.loads(mr_time.read_text())["mr_streaming_ms"]
    except Exception as e:
        res["comparacion_con_streaming"] = f"sin salida de MapReduce todavía ({type(e).__name__})"
    ev.record("c_mapreduce_vs_rdd", res)
