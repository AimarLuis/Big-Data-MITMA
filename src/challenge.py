"""F.3 — Desafío de tuning: el problema de los ARCHIVOS PEQUEÑOS (Tema 5, "Small files").

Al escribir con partitionBy("fecha"), CADA task escribe un fichero en CADA fecha que contiene.
Tras una transformación ancha (dropDuplicates, join...), cada partición contiene filas de los
7 días, así que salen  nº_particiones x 7  ficheros.

Se escribe la misma tabla curada de tres formas y se mide:
  1. 200 particiones (spark.sql.shuffle.partitions por defecto de Spark) -> hasta 1.400 ficheros
  2. SPARK_SHUFFLE_PARTITIONS del proyecto                                 -> hasta 7 x N ficheros
  3. repartition("fecha") (lo que hace etl.py)                             -> 7 ficheros
Para cada una: nº de ficheros, tamaño medio, tiempo de escritura y tiempo de lectura.
Cada fichero es un objeto en la memoria del NameNode y una task más que planificar al leer.
"""
from pyspark.sql import functions as F

from common import hdfs, measure, run_noop


def run(spark, cfg, ev):
    cur = spark.read.parquet(hdfs(cfg, "CURATED"))
    col = cfg["PARTITION_COLUMN"]
    base = hdfs(cfg, "SMALLFILES_PATH")
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(cfg["HDFS_URI"]), spark._jsc.hadoopConfiguration())

    variantes = {
        "1_shuffle_200_por_defecto": cur.repartition(200),
        f"2_shuffle_{cfg['SPARK_SHUFFLE_PARTITIONS']}_proyecto": cur.repartition(int(cfg["SPARK_SHUFFLE_PARTITIONS"])),
        "3_repartition_por_fecha": cur.repartition(col),
    }
    res = {}
    for name, df in variantes.items():
        path = f"{base}/{name}"
        t = measure(lambda: df.write.mode("overwrite").partitionBy(col).parquet(path), 1)
        summary = fs.getContentSummary(jvm.org.apache.hadoop.fs.Path(path))
        files = summary.getFileCount() - 1                     # -1: el fichero _SUCCESS
        lectura = spark.read.parquet(path)
        res[name] = {
            "ficheros": files,
            "tamano_medio_MB": round(summary.getLength() / max(files, 1) / 2**20, 2),
            "escritura": t,
            "lectura_suma_por_dia": measure(lambda: run_noop(lectura.groupBy(col).agg(F.sum("viajes"))),
                                            cfg["BENCH_REPEATS"]),
            "particiones_al_leer": lectura.rdd.getNumPartitions(),
        }
    ev.record("f3_archivos_pequenos", res)
