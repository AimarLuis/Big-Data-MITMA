#!/usr/bin/env bash
# =============================================================================
# PUNTO DE ENTRADA ÚNICO (Linux / macOS):   ./run_pipeline.sh
# Equivalente a run_pipeline.ps1 (Windows). Cada paso se ejecuta DENTRO de su contenedor;
# este script solo los encadena, muestra por consola las métricas de cada paso a medida que
# se calculan y se detiene en el primer error.
#   ingesta -> HDFS (raw) -> Spark (curated, formatos, preguntas) -> MapReduce -> Spark
#   (RDD, ML, desafío) -> evidencias de HDFS -> resumen de métricas
#
#   ./run_pipeline.sh          # todos los pasos (0 a 6) y el resumen final
#   ./run_pipeline.sh 3 5      # solo esos pasos (con el entorno ya levantado)
#
# Rutas y parámetros: config/pipeline.env
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

PASOS=" ${*:-0 1 2 3 4 5 6} "
COMPLETO=0; [ $# -eq 0 ] && COMPLETO=1

# Copia de toda la salida en evidencias/pipeline_run.log (se reinicia en cada ejecución completa)
mkdir -p evidencias
LOG=evidencias/pipeline_run.log
[ "$COMPLETO" = 1 ] && : > "$LOG"
exec > >(tee -a "$LOG") 2>&1

hora() { date +%H:%M:%S; }
linea() { printf '%*s\n' 78 '' | tr ' ' '='; }
INICIO=$SECONDS
TIEMPOS=""

paso() {        # paso <n> <título> <comando...>
  local n="$1" titulo="$2"; shift 2
  case "$PASOS" in
    *" $n "*) ;;
    *) echo; echo "[$(hora)] ---- $n/7 $titulo (se salta)"; return 0;;
  esac
  echo; linea; echo "[$(hora)] PASO $n/7 · $titulo"; linea
  local t0=$SECONDS
  "$@" || { local rc=$?; echo "[$(hora)] ERROR: falló el paso $n/7 ($titulo), código $rc"; exit "$rc"; }
  local s=$((SECONDS - t0))
  TIEMPOS="${TIEMPOS}${n}/7|${titulo}|${s}"$'\n'
  echo "[$(hora)] Paso $n/7 terminado en $s s"
}

SPARK=/home/jovyan/work/scripts/spark_python.sh
paso 0 "Levantar el entorno (no hace nada si ya está levantado)" docker compose up -d --build
paso 1 "Descarga reproducible (solo lo que falte)" docker exec jupyter python /home/jovyan/work/scripts/download_mitma.py
paso 2 "Ingesta en la zona raw de HDFS" docker exec namenode bash /opt/project/scripts/hdfs_ingest.sh
paso 3 "Spark: smoke test, ETL (curated), formatos y preguntas analíticas" docker exec jupyter bash "$SPARK" src/pipeline.py --stages smoke etl formats analytics
paso 4 "MapReduce con Hadoop Streaming (YARN)" docker exec namenode bash /opt/project/scripts/mr_streaming.sh
paso 5 "Spark: misma agregación con RDDs, ML y desafío F" docker exec jupyter bash "$SPARK" src/pipeline.py --stages mr_rdd ml challenge
paso 6 "Evidencias de HDFS (ls -R, du, count, fsck)" docker exec namenode bash /opt/project/scripts/hdfs_evidence.sh

# ---- 7/7 Resumen: tiempo de cada paso y métricas clave de todos los apartados
echo; linea; echo "[$(hora)] 7/7 Fin del pipeline en $(( (SECONDS - INICIO) / 60 )) min"; linea
echo "Tiempo de cada paso:"
printf '%s' "$TIEMPOS" | while IFS='|' read -r n t s; do printf '   %-5s %6s s   %s\n' "$n" "$s" "$t"; done
echo
if [ "$COMPLETO" = 1 ]; then
  { echo "paso,descripcion,segundos"
    printf '%s' "$TIEMPOS" | while IFS='|' read -r n t s; do echo "\"$n\",\"$t\",\"$s\""; done
  } > evidencias/tiempos_pasos.csv
fi
docker exec jupyter python /home/jovyan/work/src/resumen.py || echo "(no se pudo mostrar el resumen de métricas)"
