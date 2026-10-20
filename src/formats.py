"""B.2 — Comparación de formatos: tamaño en disco y tiempo de lectura.

Método (el mismo de la Activity 2.3 de la clase 4-5, pero repetido y con mediana):
  - Misma consulta sobre el CSV (raw) y sobre el Parquet (curated), BENCH_REPEATS veces.
  - Sin cache(). Se mide con write.format("noop"): ejecuta la consulta entera sin traer
    datos al driver (así no medimos el collect).
  - Dos consultas, porque miden efectos distintos:
      A) suma de viajes por día: lee TODAS las filas pero solo 2 columnas -> efecto columnar
      B) lo mismo filtrando un único día -> efecto partition pruning (Parquet salta carpetas)
Además: el problema de gzip (no divisible) medido con getNumPartitions().
"""
from pyspark.sql import functions as F

from common import hdfs, measure, run_noop
from etl import VIAJES_SCHEMA, read_raw


def run(spark, cfg, ev):
    reps = cfg["BENCH_REPEATS"]
    csv = read_raw(spark, cfg).withColumn("fecha", F.to_date("fecha", cfg["DATE_FORMAT"]))
    pq = spark.read.parquet(hdfs(cfg, "CURATED"))
    un_dia = cfg["FECHA_INICIO"]

    def q_a(df):                      # todas las filas, 2 columnas
        return df.groupBy("fecha").agg(F.sum("viajes").alias("viajes"))

    def q_b(df):                      # un solo día
        return df.filter(F.col("fecha") == un_dia).groupBy("origen").agg(F.sum("viajes").alias("viajes"))

    # Tamaños con la API de Hadoop que ya trae Spark (lo mismo que `hdfs dfs -du -s`)
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(cfg["HDFS_URI"]),
                                                 spark._jsc.hadoopConfiguration())
    size = lambda key: fs.getContentSummary(jvm.org.apache.hadoop.fs.Path(hdfs(cfg, key))).getLength()

    res = {
        "tamano_csv_bytes": size("RAW_VIAJES"),
        "tamano_parquet_bytes": size("CURATED"),
        "A_todas_las_filas_2_columnas": {"csv": measure(lambda: run_noop(q_a(csv)), reps),
                                         "parquet": measure(lambda: run_noop(q_a(pq)), reps)},
        "B_un_solo_dia": {"csv": measure(lambda: run_noop(q_b(csv)), reps),
                          "parquet": measure(lambda: run_noop(q_b(pq)), reps)},
        "particiones_lectura": {"csv_7_dias": csv.rdd.getNumPartitions(),
                                "parquet_7_dias": pq.rdd.getNumPartitions()},
    }
    res["ratio_tamano"] = round(res["tamano_csv_bytes"] / res["tamano_parquet_bytes"], 2)
    ev.record("b2_formatos", res)

    # Planes: en el FileScan de Parquet aparecen ReadSchema (solo 2 columnas) y PartitionFilters
    ev.save_plan(q_a(pq), "b2_parquet_todas_las_filas")
    ev.save_plan(q_b(pq), "b2_parquet_un_dia")
    ev.save_plan(q_b(csv), "b2_csv_un_dia")

    # ---- gzip no es divisible: 1 fichero .gz = 1 partición = 1 task ----------------------
    gz = spark.read.schema(VIAJES_SCHEMA).option("header", True).option("sep", cfg["CSV_SEP"]) \
              .csv(hdfs(cfg, "TMP_GZ"))
    one_csv = spark.read.schema(VIAJES_SCHEMA).option("header", True).option("sep", cfg["CSV_SEP"]) \
                   .csv(hdfs(cfg, "RAW_VIAJES") + "/" + cfg["FECHA_INICIO"].replace("-", "") + "_Viajes_municipios.csv")
    try:
        ev.record("b2_gzip_vs_csv_un_dia", {
            "particiones_gz": gz.rdd.getNumPartitions(),
            "particiones_csv": one_csv.rdd.getNumPartitions(),
            "conteo_gz": measure(lambda: gz.count(), 1),
            "conteo_csv": measure(lambda: one_csv.count(), 1),
        })
    except Exception as e:                      # si no se subió el .gz (no estaba en data/downloads)
        print(f"   (sin .gz en {cfg['TMP_GZ']}: se omite la comparación gzip) {type(e).__name__}")
