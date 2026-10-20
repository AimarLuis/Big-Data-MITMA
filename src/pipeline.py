"""Etapas Spark del pipeline, en una sola SparkSession.

Lo lanza el punto de entrada (run_pipeline.ps1 / run_pipeline.sh) dentro del contenedor jupyter:
    bash scripts/spark_python.sh src/pipeline.py --stages etl formats analytics

Etapas: smoke | etl | formats | analytics | mr_rdd | ml | challenge
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import analytics, challenge, etl, formats, ml, mr_rdd       # noqa: E402
from common import Evidence, get_spark, hdfs, load_config, log  # noqa: E402


def smoke(spark, cfg, ev):
    """Paso 5 del T5 (smoke test): Spark llega al NameNode Y a los DataNodes, y hay executors."""
    path = hdfs(cfg, "RESULTS") + "/_smoke"
    spark.range(5).write.mode("overwrite").parquet(path)
    n = spark.read.parquet(path).count()
    executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1   # -1 = el driver
    ev.record("a3_smoke", {"filas_leidas": n, "executors": executors, "master": spark.sparkContext.master,
                           "spark_version": spark.version, "python": sys.version.split()[0]})
    assert n == 5, "HDFS no devuelve lo que se ha escrito"


TITULOS = {"smoke": "A.3 · smoke test: Spark <-> HDFS",
           "etl": "D.1 · ETL raw -> curated con controles de calidad",
           "formats": "B.2 · CSV frente a Parquet y el problema de gzip",
           "analytics": "D.2 · preguntas P1-P3 y D.3 · optimización del join",
           "mr_rdd": "C · la agregación de MapReduce con RDDs",
           "ml": "E.3 · regresión lineal con MLlib",
           "challenge": "F.3 · desafío: archivos pequeños"}

STAGES = {"smoke": smoke, "etl": etl.run, "formats": formats.run, "analytics": analytics.run,
          "mr_rdd": mr_rdd.run, "ml": ml.run, "challenge": challenge.run}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stages", nargs="+", default=list(STAGES), choices=list(STAGES))
    args = parser.parse_args()

    cfg = load_config()
    ev = Evidence(cfg)
    spark = get_spark(cfg)
    log(f"SparkSession creada (master {spark.sparkContext.master}). Spark UI: http://localhost:4040")
    try:
        for name in args.stages:
            print(flush=True)
            log(f"=== {name.upper()} · {TITULOS[name]} ===")
            t = time.time()
            STAGES[name](spark, cfg, ev)
            ev.record(f"tiempo_etapa_{name}_s", round(time.time() - t, 1))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
