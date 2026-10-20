# =============================================================================
# PUNTO DE ENTRADA ÚNICO (Windows / PowerShell):   .\run_pipeline.ps1
# Equivalente a run_pipeline.sh (Linux/macOS). Cada paso se ejecuta DENTRO de su contenedor;
# este script solo los encadena y se detiene en el primer error.
#   ingesta -> HDFS (raw) -> Spark (curated, formatos, preguntas) -> MapReduce -> Spark
#   (RDD, ML, desafío) -> evidencias de HDFS
# Rutas y parámetros: config/pipeline.env
# Si PowerShell no deja ejecutar scripts:  powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1
# =============================================================================
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
# Copia de toda la salida de esta ejecución en evidencias\pipeline_run.log
Start-Transcript -Path "$PSScriptRoot\evidencias\pipeline_run.log" -Append | Out-Null

function Paso($titulo, [scriptblock]$comando) {
    Write-Host "`n############ $titulo" -ForegroundColor Cyan
    & $comando
    if ($LASTEXITCODE -ne 0) { throw "Falló el paso: $titulo (código $LASTEXITCODE)" }
}

Paso "0/7 Levantar el entorno (no hace nada si ya está levantado)" { docker compose up -d --build }
Paso "1/7 Descarga reproducible (solo lo que falte)" { docker exec jupyter python /home/jovyan/work/scripts/download_mitma.py }
Paso "2/7 Ingesta en la zona raw de HDFS" { docker exec namenode bash /opt/project/scripts/hdfs_ingest.sh }
Paso "3/7 Spark: smoke test, ETL (curated), formatos y preguntas analíticas" { docker exec jupyter bash /home/jovyan/work/scripts/spark_python.sh src/pipeline.py --stages smoke etl formats analytics }
Paso "4/7 MapReduce con Hadoop Streaming (YARN)" { docker exec namenode bash /opt/project/scripts/mr_streaming.sh }
Paso "5/7 Spark: misma agregación con RDDs, ML y desafío F" { docker exec jupyter bash /home/jovyan/work/scripts/spark_python.sh src/pipeline.py --stages mr_rdd ml challenge }
Paso "6/7 Evidencias de HDFS (ls -R, du, count, fsck)" { docker exec namenode bash /opt/project/scripts/hdfs_evidence.sh }
Write-Host "`n############ 7/7 Fin. Métricas en evidencias\metrics.json y resultados en evidencias\resultados\" -ForegroundColor Green
Stop-Transcript | Out-Null
