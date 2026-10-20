#!/bin/bash
# =============================================================================
# INGESTA: datos locales -> zona RAW de HDFS (mismos comandos que el Lab Step 1 del Tema 5,
# pero sin "docker cp": la carpeta del repo está montada en el namenode como /opt/project).
# Se ejecuta DENTRO del namenode:  docker exec namenode bash /opt/project/scripts/hdfs_ingest.sh
# Idempotente: un fichero que ya está en HDFS con el mismo tamaño no se vuelve a subir
# (raw es inmutable: nunca se modifica ni se sobrescribe un dato correcto).
# =============================================================================
set -euo pipefail
cd /opt/project
set -a; source config/pipeline.env; set +a

echo ">> Esperando a que el NameNode salga del modo seguro"
hdfs dfsadmin -safemode wait

hdfs dfs -mkdir -p "$RAW_VIAJES" "$RAW_MUNICIPIOS" "$RAW_AUX" "$RESULTS" "$TMP_GZ" "$(dirname "$CURATED")"

put_if_new() {          # put_if_new <fichero local> <carpeta HDFS>
  local src="$1" dst="$2/$(basename "$1")"
  if hdfs dfs -test -e "$dst"; then
    if [ "$(hdfs dfs -stat %b "$dst")" = "$(stat -c %s "$src")" ]; then
      echo "   = $(basename "$src") (ya está en HDFS)"; return 0
    fi
  fi
  echo "   + $(basename "$src") -> $2"
  hdfs dfs -put -f "$src" "$2/"
}

echo ">> Viajes (7 CSV diarios)"
for f in "$LOCAL_VIAJES"/*.csv; do put_if_new "$f" "$RAW_VIAJES"; done
echo ">> Tabla auxiliar de municipios (zonificación MITMA)"
for f in "$LOCAL_MUNICIPIOS"/*.csv; do put_if_new "$f" "$RAW_MUNICIPIOS"; done
echo ">> Tablas auxiliares construidas por el equipo (provincias, calendario)"
for f in "$LOCAL_AUX"/*.csv; do put_if_new "$f" "$RAW_AUX"; done
echo ">> Un .csv.gz original, solo para medir el problema de gzip (B.2) -> $TMP_GZ"
GZ=$(ls "$LOCAL_DOWNLOADS"/*.csv.gz 2>/dev/null | head -1 || true)
if [ -n "$GZ" ]; then put_if_new "$GZ" "$TMP_GZ"; else echo "   (no hay .gz en $LOCAL_DOWNLOADS: se omite)"; fi

hdfs dfs -ls -R /raw > "$EVIDENCIAS/hdfs_raw_ls.txt"
echo ">> Ingesta terminada. Listado en $EVIDENCIAS/hdfs_raw_ls.txt"
