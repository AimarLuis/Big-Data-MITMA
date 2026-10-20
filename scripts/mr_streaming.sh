#!/bin/bash
# =============================================================================
# C.1 - Job de MapReduce con Hadoop Streaming (mapper/reducer en Python) sobre YARN.
# Mismo mecanismo que el WordCount del Tema 3 (hadoop jar ... en el namenode), pero con el
# jar de streaming, que permite escribir map y reduce en cualquier lenguaje (stdin -> stdout).
# Se ejecuta DENTRO del namenode: docker exec namenode bash /opt/project/scripts/mr_streaming.sh
# =============================================================================
set -euo pipefail
cd /opt/project
set -a; source config/pipeline.env; set +a

JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)
hdfs dfs -rm -r -f -skipTrash "$MR_OUTPUT" > /dev/null    # idempotente: la salida no puede existir

echo ">> hadoop jar $(basename "$JAR")  input=$RAW_VIAJES  output=$MR_OUTPUT"
start=$(date +%s%N)
hadoop jar "$JAR" \
  -D mapreduce.job.name="mitma-viajes-provincia-fecha" \
  -D mapreduce.job.reduces="$MR_REDUCERS" \
  -files mapreduce/mapper.py,mapreduce/reducer.py \
  -cmdenv MR_SEP="$CSV_SEP" \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py" \
  -input "$RAW_VIAJES" \
  -output "$MR_OUTPUT" 2>&1 | tee "$EVIDENCIAS/mr_streaming.log"
end=$(date +%s%N)

ms=$(( (end - start) / 1000000 ))
echo "{\"mr_streaming_ms\": $ms}" > "$EVIDENCIAS/mr_streaming_time.json"
hdfs dfs -cat "$MR_OUTPUT"/part-* | sort > "$EVIDENCIAS/resultados/c_mr_streaming_provincia_fecha.tsv"
echo ">> Job terminado en $ms ms. $(wc -l < "$EVIDENCIAS/resultados/c_mr_streaming_provincia_fecha.tsv") claves (provincia|fecha)"
