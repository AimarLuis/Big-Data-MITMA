#!/bin/bash
# =============================================================================
# EVIDENCIAS DE HDFS (B.1 y B.2): estructura de zonas, tamaños, nº de ficheros y bloques.
# Se ejecuta DENTRO del namenode: docker exec namenode bash /opt/project/scripts/hdfs_evidence.sh
# =============================================================================
set -uo pipefail
cd /opt/project
set -a; source config/pipeline.env; set +a
E="$EVIDENCIAS"

echo ">> hdfs dfs -ls -R (estructura de zonas raw / curated / quarantine / results)"
hdfs dfs -ls -R /raw /curated /quarantine /results > "$E/hdfs_ls_R.txt" 2>&1
echo ">> hdfs dfs -du -s -h (1ª columna: datos; 2ª: espacio con réplicas)"
hdfs dfs -du -s -h "$RAW_VIAJES" "$CURATED" "$QUARANTINE" > "$E/hdfs_du.txt" 2>&1
echo ">> hdfs dfs -count (carpetas, ficheros, bytes)"
hdfs dfs -count "$RAW_VIAJES" "$CURATED" "$SMALLFILES_PATH" > "$E/hdfs_count.txt" 2>&1

FICHERO=$(hdfs dfs -ls "$RAW_VIAJES" | awk '/\.csv$/{print $NF}' | tail -1)
echo ">> hdfs fsck $FICHERO -files -blocks -locations"
hdfs fsck "$FICHERO" -files -blocks -locations > "$E/fsck_raw_un_dia.txt" 2>&1
hdfs fsck "$CURATED" -files -blocks -locations > "$E/fsck_curated.txt" 2>&1
hdfs dfsadmin -report > "$E/hdfs_dfsadmin_report.txt" 2>&1
grep -E "Total size|Total blocks|Average block replication|Under-replicated|Status" "$E/fsck_raw_un_dia.txt"
echo ">> Evidencias de HDFS guardadas en $E/"
