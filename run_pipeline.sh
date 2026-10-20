#!/usr/bin/env bash
# =============================================================================
# PUNTO DE ENTRADA ÚNICO (Linux / macOS):   ./run_pipeline.sh
# Equivalente a run_pipeline.ps1 (Windows). Cada paso se ejecuta DENTRO de su contenedor;
# este script solo los encadena y se detiene en el primer error.
#   ingesta -> HDFS (raw) -> Spark (curated, formatos, preguntas) -> MapReduce -> Spark
#   (RDD, ML, desafío) -> evidencias de HDFS
# Rutas y parámetros: config/pipeline.env
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

paso() { echo; echo "############ $1"; }

paso "0/7 Levantar el entorno (no hace nada si ya está levantado)"
docker compose up -d --build

paso "1/7 Descarga reproducible (solo lo que falte)"
docker exec jupyter python /home/jovyan/work/scripts/download_mitma.py

paso "2/7 Ingesta en la zona raw de HDFS"
docker exec namenode bash /opt/project/scripts/hdfs_ingest.sh

paso "3/7 Spark: smoke test, ETL (curated), formatos y preguntas analíticas"
docker exec jupyter bash /home/jovyan/work/scripts/spark_python.sh src/pipeline.py --stages smoke etl formats analytics

paso "4/7 MapReduce con Hadoop Streaming (YARN)"
docker exec namenode bash /opt/project/scripts/mr_streaming.sh

paso "5/7 Spark: misma agregación con RDDs, ML y desafío F"
docker exec jupyter bash /home/jovyan/work/scripts/spark_python.sh src/pipeline.py --stages mr_rdd ml challenge

paso "6/7 Evidencias de HDFS (ls -R, du, count, fsck)"
docker exec namenode bash /opt/project/scripts/hdfs_evidence.sh

paso "7/7 Fin. Métricas en evidencias/metrics.json y resultados en evidencias/resultados/"
