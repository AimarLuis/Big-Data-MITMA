#!/bin/bash
# =============================================================================
# EVIDENCIAS DE HDFS (B.1 y B.2): estructura de zonas, tamaños, nº de ficheros y bloques.
# Se ejecuta DENTRO del namenode: docker exec namenode bash /opt/project/scripts/hdfs_evidence.sh
# =============================================================================
set -uo pipefail
cd /opt/project
set -a; source config/pipeline.env; set +a
E="$EVIDENCIAS"
log() { echo "[$(date +%H:%M:%S)] $*"; }

log ">> hdfs dfs -ls -R (estructura de zonas raw / curated / quarantine / results)"
hdfs dfs -ls -R /raw /curated /quarantine /results > "$E/hdfs_ls_R.txt" 2>&1
log ">> hdfs dfs -du -s -h (1ª columna: datos; 2ª: espacio con réplicas)"
hdfs dfs -du -s -h "$RAW_VIAJES" "$CURATED" "$QUARANTINE" > "$E/hdfs_du.txt" 2>&1
sed 's/^/   /' "$E/hdfs_du.txt"
log ">> hdfs dfs -count (carpetas, ficheros, bytes)"
hdfs dfs -count "$RAW_VIAJES" "$CURATED" "$SMALLFILES_PATH" > "$E/hdfs_count.txt" 2>&1
sed 's/^/   /' "$E/hdfs_count.txt"

FICHERO=$(hdfs dfs -ls "$RAW_VIAJES" | awk '/\.csv$/{print $NF}' | tail -1)
log ">> hdfs fsck -files -blocks -locations (un CSV de raw y toda la zona curated)"
hdfs fsck "$FICHERO" -files -blocks -locations > "$E/fsck_raw_un_dia.txt" 2>&1
hdfs fsck "$CURATED" -files -blocks -locations > "$E/fsck_curated.txt" 2>&1
hdfs dfsadmin -report > "$E/hdfs_dfsadmin_report.txt" 2>&1
FILTRO="Total size|Total files|Total blocks|Average block replication|Under-replicated blocks|Missing blocks|Status"
echo "   raw: $FICHERO"
sed '/Erasure Coded/q' "$E/fsck_raw_un_dia.txt" | grep -E "$FILTRO" | sed 's/^[[:space:]]*/      /'
echo "   curated: $CURATED"
sed '/Erasure Coded/q' "$E/fsck_curated.txt" | grep -E "$FILTRO" | sed 's/^[[:space:]]*/      /'
log ">> DataNodes: $(grep -m1 'Live datanodes' "$E/hdfs_dfsadmin_report.txt" | tr -d ':')"
log ">> Evidencias de HDFS guardadas en $E/"
