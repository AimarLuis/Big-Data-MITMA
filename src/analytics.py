"""D.2 + D.3 — Tres preguntas analíticas de dificultad creciente y una optimización medida.

P1 (agrupación, DataFrame API): viajes medios DIARIOS por provincia de origen según el tipo
    de día (laborable / fin de semana / festivo). Sustituye a la pregunta inscrita "por día
    de la semana": con una sola semana, cada día de la semana aparece UNA vez (n = 1).
P2 (ventana de ranking, Spark SQL): los 3 municipios que más viajes reciben cada día.
P3 (ventana de comparación + lag): para cada municipio grande y día, desviación respecto a su
    media de la semana y variación respecto al día anterior.
D.3: P1 hace joins con dos tablas pequeñas (calendario y provincias). Se mide el sort-merge
    join (los 70 M de filas se mueven por la red) frente al broadcast join (clase 6).
"""
import json
import time
import urllib.request

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import hdfs, log, measure, run_noop
from etl import read_aux, read_municipios


# ----------------------------------------------------------------------------- P1
def p1_provincia_tipo_dia(cur, calendario, provincias):
    """Dos niveles de agregación:
    1) total diario por provincia de origen: SUM(viajes) por (provincia, fecha)
    2) media de esos totales diarios según el tipo de día
    (AVG(viajes) directamente sobre las filas sería la media por FILA, no por día)."""
    diario = (cur.filter(~F.col("origen_extranjero"))
                 .join(calendario, "fecha")                                          # join 1: tipo de día
                 .join(provincias, cur.cod_prov_origen == provincias.cod_provincia)  # join 2: nombre de provincia
                 .groupBy("provincia", "ccaa", "fecha", "tipo_dia")
                 .agg(F.sum("viajes").alias("viajes_dia")))
    return (diario.groupBy("provincia", "ccaa")
                  .agg(F.round(F.avg(F.when(F.col("tipo_dia") == "laborable", F.col("viajes_dia")))).alias("media_laborable"),
                       F.round(F.avg(F.when(F.col("tipo_dia") == "fin_de_semana", F.col("viajes_dia")))).alias("media_fin_de_semana"),
                       F.round(F.avg(F.when(F.col("tipo_dia") == "festivo", F.col("viajes_dia")))).alias("festivo_1_nov"))
                  .withColumn("finde_vs_laborable_pct",
                              F.round(100 * (F.col("media_fin_de_semana") / F.col("media_laborable") - 1), 1))
                  .withColumn("festivo_vs_laborable_pct",
                              F.round(100 * (F.col("festivo_1_nov") / F.col("media_laborable") - 1), 1))
                  .orderBy(F.desc("media_laborable")))


# ----------------------------------------------------------------------------- P2
def p2_top_destinos_dia(spark, top_n):
    """Spark SQL sobre vistas temporales (clase 4: createOrReplaceTempView + spark.sql).
    ROW_NUMBER: siempre 1, 2, 3 (con RANK, un empate podría dar más de 3 filas por día)."""
    return spark.sql(f"""
        WITH diario AS (
            SELECT fecha, destino, SUM(viajes) AS viajes_recibidos
            FROM viajes
            WHERE NOT destino_extranjero
            GROUP BY fecha, destino
        ),
        ranking AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY fecha ORDER BY viajes_recibidos DESC) AS puesto
            FROM diario
        )
        SELECT r.fecha, r.puesto, r.destino, m.nombre_municipio,
               ROUND(r.viajes_recibidos) AS viajes_recibidos
        FROM ranking r
        LEFT JOIN municipios m ON r.destino = m.id_municipio
        WHERE r.puesto <= {int(top_n)}
        ORDER BY r.fecha, r.puesto
    """)


# ----------------------------------------------------------------------------- P3
def p3_desviacion_municipio(cur, municipios, umbral):
    """Funciones de ventana que NO colapsan filas (clase 6):
    - avg over partitionBy(destino) sin orderBy -> media de TODA la semana del municipio
    - lag over partitionBy(destino).orderBy(fecha) -> valor del día anterior (null el primer día)
    Solo municipios 'grandes' (media >= umbral de viajes/día): en los pequeños un % es ruido."""
    diario = (cur.filter(~F.col("destino_extranjero"))
                 .groupBy("destino", "fecha")
                 .agg(F.sum("viajes").alias("viajes_dia")))
    w_semana = Window.partitionBy("destino")
    w_orden = Window.partitionBy("destino").orderBy("fecha")
    return (diario
            .withColumn("media_semana", F.avg("viajes_dia").over(w_semana))
            .withColumn("desviacion_pct", F.round(100 * (F.col("viajes_dia") / F.col("media_semana") - 1), 1))
            .withColumn("dia_anterior", F.lag("viajes_dia").over(w_orden))
            .withColumn("variacion_dia_anterior_pct",
                        F.round(100 * (F.col("viajes_dia") / F.col("dia_anterior") - 1), 1))
            .filter(F.col("media_semana") >= float(umbral))
            .join(municipios, F.col("destino") == F.col("id_municipio"), "left")
            .select("fecha", "destino", "nombre_municipio",
                    F.round("viajes_dia").alias("viajes_dia"), F.round("media_semana").alias("media_semana"),
                    "desviacion_pct", "variacion_dia_anterior_pct"))


def p3_resumen_por_dia(p3):
    """Lectura de P3: para cada día, cómo se desvían los municipios grandes de su media."""
    return (p3.groupBy("fecha")
              .agg(F.count("*").alias("municipios_grandes"),
                   F.percentile_approx("desviacion_pct", 0.5).alias("mediana_desviacion_pct"),
                   F.min("desviacion_pct").alias("min_desviacion_pct"),
                   F.max("desviacion_pct").alias("max_desviacion_pct"),
                   F.sum((F.col("desviacion_pct") < -20).cast("int")).alias("municipios_mas_de_20pct_por_debajo"))
              .orderBy("fecha"))


# ----------------------------------------------------------------------------- D.3
def stages_of_group(spark, group):
    """Resumen de los stages de un grupo de jobs, leído de la API REST de la Spark UI
    (la misma información que la pestaña Stages: tareas y bytes de shuffle)."""
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    try:
        jobs = json.load(urllib.request.urlopen(f"{base}/jobs"))
        stage_ids = sorted({s for j in jobs if j.get("jobGroup") == group for s in j["stageIds"]})
        out = []
        for sid in stage_ids:
            st = json.load(urllib.request.urlopen(f"{base}/stages/{sid}"))[0]
            if st["status"] != "COMPLETE":
                continue                                  # stages saltados (skipped) no cuentan
            out.append({"stage": sid, "tareas": st["numTasks"],
                        "lectura_entrada_MB": round(st["inputBytes"] / 2**20, 1),
                        "shuffle_write_MB": round(st["shuffleWriteBytes"] / 2**20, 1),
                        "shuffle_read_MB": round(st["shuffleReadBytes"] / 2**20, 1)})
        return {"stages_ejecutados": len(out),
                "shuffle_write_total_MB": round(sum(s["shuffle_write_MB"] for s in out), 1),
                "detalle": out}
    except Exception as e:                                # la UI puede estar desactivada
        return {"error": f"{type(e).__name__}: {e}"}


def run(spark, cfg, ev):
    cur = spark.read.parquet(hdfs(cfg, "CURATED"))
    calendario = read_aux(spark, cfg, "calendario").withColumn("fecha", F.to_date("fecha"))
    provincias = read_aux(spark, cfg, "provincias")
    municipios = read_municipios(spark, cfg)

    # ---- P1 ----------------------------------------------------------------------------
    log("   P1: agrupación por provincia y tipo de día...")
    p1 = p1_provincia_tipo_dia(cur, calendario, provincias)
    ev.save_plan(p1, "d2_p1")
    ev.save_result(p1, "d2_p1_provincia_tipo_dia")
    ev.record("d2_p1_filas", p1.count())

    # ---- P2 (SQL) ------------------------------------------------------------------------
    log("   P2: ranking de destinos por día (Spark SQL + ROW_NUMBER)...")
    cur.createOrReplaceTempView("viajes")
    municipios.createOrReplaceTempView("municipios")
    p2 = p2_top_destinos_dia(spark, cfg["TOP_N"])
    ev.save_plan(p2, "d2_p2")
    ev.save_result(p2, "d2_p2_top_destinos_dia")
    ev.record("d2_p2_filas", p2.count())

    # ---- P3 ----------------------------------------------------------------------------
    log("   P3: desviación respecto a la media semanal (ventanas avg + lag)...")
    p3 = p3_desviacion_municipio(cur, municipios, cfg["UMBRAL_MUNICIPIO_GRANDE"]).cache()
    ev.save_plan(p3, "d2_p3")
    ev.save_result(p3.orderBy("destino", "fecha"), "d2_p3_desviacion_municipio_dia")
    ev.save_result(p3_resumen_por_dia(p3), "d2_p3_resumen_por_dia")
    ev.save_result(p3.filter(F.col("fecha") == cfg["FECHA_FIN"]).orderBy("desviacion_pct").limit(10),
                   "d2_p3_mayores_caidas_festivo")
    ev.record("d2_p3_filas", p3.count())
    p3.unpersist()

    # ---- D.3: sort-merge join frente a broadcast join en P1 -------------------------------
    reps = cfg["BENCH_REPEATS"]
    sc = spark.sparkContext

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)          # -1 = nunca broadcast
    antes = p1_provincia_tipo_dia(cur, calendario, provincias)
    ev.save_plan(antes, "d3_antes_sort_merge_join")
    log(f"   D3: P1 con sort-merge join (broadcast desactivado), {reps} repeticiones...")
    t_antes = measure(lambda: run_noop(antes), reps, "P1 con sort-merge join")
    sc.setJobGroup("d3_antes", "P1 con sort-merge join")        # una ejecución más, etiquetada,
    run_noop(antes)                                             # para leer sus stages en la UI
    stages_antes = stages_of_group(spark, "d3_antes")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)   # por defecto: 10 MB
    despues = p1_provincia_tipo_dia(cur, calendario, provincias)
    ev.save_plan(despues, "d3_despues_broadcast_join")
    log(f"   D3: P1 con broadcast join (umbral 10 MB), {reps} repeticiones...")
    t_despues = measure(lambda: run_noop(despues), reps, "P1 con broadcast join")
    sc.setJobGroup("d3_despues", "P1 con broadcast join")
    run_noop(despues)
    stages_despues = stages_of_group(spark, "d3_despues")
    sc.setJobGroup("pipeline", "resto del pipeline")

    ev.record("d3_optimizacion_broadcast", {
        "antes_sort_merge": {**t_antes, "stages_spark_ui": stages_antes},
        "despues_broadcast": {**t_despues, "stages_spark_ui": stages_despues},
        "mejora_pct": round(100 * (1 - t_despues["mediana_s"] / t_antes["mediana_s"]), 1),
    })
    time.sleep(1)
