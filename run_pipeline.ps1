# =============================================================================
# PUNTO DE ENTRADA ÚNICO (Windows / PowerShell):   .\run_pipeline.ps1
# Equivalente a run_pipeline.sh (Linux/macOS). Cada paso se ejecuta DENTRO de su contenedor;
# este script solo los encadena, muestra por consola las métricas de cada paso a medida que
# se calculan y se detiene en el primer error.
#   ingesta -> HDFS (raw) -> Spark (curated, formatos, preguntas) -> MapReduce -> Spark
#   (RDD, ML, desafío) -> evidencias de HDFS -> resumen de métricas
#
#   .\run_pipeline.ps1               # todos los pasos (0 a 6) y el resumen final
#   .\run_pipeline.ps1 -Pasos 3,5    # solo esos pasos (con el entorno ya levantado)
#
# Rutas y parámetros: config/pipeline.env
# Si PowerShell no deja ejecutar scripts:  powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1
# =============================================================================
param([string]$Pasos = "0,1,2,3,4,5,6")

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$lista = @($Pasos -split '[,\s]+' | Where-Object { $_ } | ForEach-Object { [int]$_ })
$completo = (@(0..6 | Where-Object { $lista -notcontains $_ }).Count -eq 0)

# Los contenedores escriben en UTF-8: sin esto, las tildes salen mal en la consola de Windows
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$OutputEncoding = [System.Text.UTF8Encoding]::new()

# Copia de toda la salida en evidencias\pipeline_run.log (se reinicia en cada ejecución completa)
$log = "$PSScriptRoot\evidencias\pipeline_run.log"
if ($completo) { Start-Transcript -Path $log | Out-Null } else { Start-Transcript -Path $log -Append | Out-Null }

$tiempos = New-Object System.Collections.Generic.List[object]
$inicio = Get-Date
function Hora { Get-Date -Format "HH:mm:ss" }

function Paso([int]$n, [string]$titulo, [scriptblock]$comando) {
    if ($lista -notcontains $n) { Write-Host "`n[$(Hora)] ---- $n/7 $titulo (se salta)" -ForegroundColor DarkGray; return }
    Write-Host ""
    Write-Host ("=" * 78) -ForegroundColor Cyan
    Write-Host "[$(Hora)] PASO $n/7 · $titulo" -ForegroundColor Cyan
    Write-Host ("=" * 78) -ForegroundColor Cyan
    $t = Get-Date
    # La salida de docker pasa por Out-Host: se ve en directo y queda en el transcript.
    # 2>&1 incluye stderr (Spark y Hadoop escriben ahí sus avisos). Con "Continue", una línea en
    # stderr no se trata como error: el fallo de verdad lo indica el código de salida.
    $ErrorActionPreference = "Continue"
    & $comando 2>&1 | ForEach-Object { "$_" } | Out-Host
    $codigo = $LASTEXITCODE
    $ErrorActionPreference = "Stop"
    $seg = [math]::Round(((Get-Date) - $t).TotalSeconds)
    $tiempos.Add([pscustomobject]@{ paso = "$n/7"; descripcion = $titulo; segundos = $seg })
    if ($codigo -ne 0) { Stop-Transcript | Out-Null; throw "Falló el paso $n/7: $titulo (código $codigo)" }
    Write-Host "[$(Hora)] Paso $n/7 terminado en $seg s" -ForegroundColor DarkCyan
}

$spark = "/home/jovyan/work/scripts/spark_python.sh"
Paso 0 "Levantar el entorno (no hace nada si ya está levantado)" { docker compose up -d --build }
Paso 1 "Descarga reproducible (solo lo que falte)" { docker exec jupyter python /home/jovyan/work/scripts/download_mitma.py }
Paso 2 "Ingesta en la zona raw de HDFS" { docker exec namenode bash /opt/project/scripts/hdfs_ingest.sh }
Paso 3 "Spark: smoke test, ETL (curated), formatos y preguntas analíticas" { docker exec jupyter bash $spark src/pipeline.py --stages smoke etl formats analytics }
Paso 4 "MapReduce con Hadoop Streaming (YARN)" { docker exec namenode bash /opt/project/scripts/mr_streaming.sh }
Paso 5 "Spark: misma agregación con RDDs, ML y desafío F" { docker exec jupyter bash $spark src/pipeline.py --stages mr_rdd ml challenge }
Paso 6 "Evidencias de HDFS (ls -R, du, count, fsck)" { docker exec namenode bash /opt/project/scripts/hdfs_evidence.sh }

# ---- 7/7 Resumen: tiempo de cada paso y métricas clave de todos los apartados
Write-Host ""
Write-Host ("=" * 78) -ForegroundColor Green
Write-Host "[$(Hora)] 7/7 Fin del pipeline en $([math]::Round(((Get-Date) - $inicio).TotalMinutes, 1)) min" -ForegroundColor Green
Write-Host ("=" * 78) -ForegroundColor Green
Write-Host "Tiempo de cada paso:"
foreach ($t in $tiempos) { Write-Host ("   {0,-5} {1,6} s   {2}" -f $t.paso, $t.segundos, $t.descripcion) }
Write-Host ""
if ($completo) { $tiempos | Export-Csv -Path "$PSScriptRoot\evidencias\tiempos_pasos.csv" -NoTypeInformation -Encoding UTF8 }

$ErrorActionPreference = "Continue"
docker exec jupyter python /home/jovyan/work/src/resumen.py 2>&1 | ForEach-Object { "$_" } | Out-Host
if ($LASTEXITCODE -ne 0) { Write-Host "(no se pudo mostrar el resumen de métricas)" -ForegroundColor Yellow }
$ErrorActionPreference = "Stop"
Stop-Transcript | Out-Null
