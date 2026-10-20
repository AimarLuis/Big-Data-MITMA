"""Piezas comunes del pipeline: configuración, SparkSession, evidencias y medición de tiempos.

Equivale a las celdas de "Setup" de los notebooks de clase (Tema 5):
    os.environ["HADOOP_USER_NAME"] = "root"
    spark = SparkSession.builder.appName(...).getOrCreate()
    HDFS = "hdfs://namenode:9000"
    CONFIG = {...}
pero con los valores leídos de config/pipeline.env en lugar de escritos en el código.
"""
import csv
import datetime as dt
import io
import json
import os
import time
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]      # raíz del repo (/home/jovyan/work en el contenedor)


def log(msg):
    """Línea de log con la hora: [19:42:07] msg. flush=True para que salga al momento por la
    consola del punto de entrada (docker exec no espera a que termine el proceso)."""
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


# ----------------------------------------------------------------------------- configuración
def load_config(path=None):
    """Lee config/pipeline.env (formato CLAVE="valor") y devuelve un diccionario CONFIG.

    Se puede apuntar a otro fichero con la variable de entorno PIPELINE_CONFIG
    (por ejemplo, una config de pruebas con master local[*])."""
    path = Path(path or os.environ.get("PIPELINE_CONFIG") or ROOT / "config" / "pipeline.env")
    config = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        config[key.strip()] = value.strip().strip('"').strip("'")
    return config


def hdfs(cfg, key):
    """Ruta completa en HDFS: hdfs(cfg, "CURATED") -> hdfs://namenode:9000/curated/mitma/viajes"""
    return cfg["HDFS_URI"] + cfg[key]


# ----------------------------------------------------------------------------- Spark
def get_spark(cfg, app_name="mitma-pipeline"):
    # Igual que en clase: actuamos como root en HDFS. Tiene que ir ANTES de crear la sesión.
    os.environ["HADOOP_USER_NAME"] = "root"
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder
             .appName(app_name)
             .master(cfg["SPARK_MASTER"])                                 # cluster standalone
             .config("spark.driver.host", cfg["SPARK_DRIVER_HOST"])       # nombre del contenedor driver
             .config("spark.driver.bindAddress", "0.0.0.0")
             .config("spark.driver.memory", cfg["SPARK_DRIVER_MEMORY"])
             .config("spark.executor.memory", cfg["SPARK_EXECUTOR_MEMORY"])
             .config("spark.executor.cores", cfg["SPARK_EXECUTOR_CORES"])
             .config("spark.sql.shuffle.partitions", cfg["SPARK_SHUFFLE_PARTITIONS"])
             # El cliente HDFS de Spark no conoce el dfs.replication=2 del cluster y escribiría con 3
             # réplicas (bloques "under-replicated" con 2 DataNodes). spark.hadoop.* pasa la propiedad
             # a la configuración de Hadoop que usa Spark.
             .config("spark.hadoop.dfs.replication", cfg["HDFS_REPLICATION"])
             # Spark 4 activa el modo ANSI por defecto: un valor mal formado (to_date de un texto
             # que no es fecha, división por 0) lanza una excepción. Lo desactivamos para tener el
             # comportamiento de Spark 3 visto en clase: el valor queda a null y lo detectan las
             # reglas de calidad, que lo mandan a cuarentena en lugar de parar todo el job.
             .config("spark.sql.ansi.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ----------------------------------------------------------------------------- evidencias
class Evidence:
    """Todo lo que el documento cita queda en evidencias/, para que cada cifra se pueda reproducir.

    - metrics.json: un diccionario de métricas. Cada ejecución sobrescribe las mismas claves.
    - runs.csv: una línea por ejecución de la ETL (historial para demostrar la idempotencia).
    - resultados/*.csv: resultados pequeños de las consultas.
    - plan_*.txt: planes de ejecución (explain).
    """

    def __init__(self, cfg):
        self.cfg = cfg
        self.dir = ROOT / cfg["EVIDENCIAS"]
        (self.dir / "resultados").mkdir(parents=True, exist_ok=True)
        self.metrics_path = self.dir / "metrics.json"
        self.metrics = json.loads(self.metrics_path.read_text()) if self.metrics_path.exists() else {}

    def record(self, key, value):
        self.metrics[key] = value
        self.metrics_path.write_text(json.dumps(self.metrics, indent=2, ensure_ascii=False, default=str),
                                     encoding="utf-8")
        # Cada métrica se muestra por consola en cuanto se calcula, en formato legible (resumen.py)
        from resumen import lineas
        for linea in lineas(key, value):
            log("   " + linea)

    def append_run(self, row):
        path = self.dir / "runs.csv"
        new = not path.exists()
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["ejecucion", *row.keys()])
            if new:
                w.writeheader()
            w.writerow({"ejecucion": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), **row})

    def save_result(self, df, name):
        """Resultado pequeño -> zona results de HDFS (CSV) + copia en evidencias/resultados/ (va al repo)."""
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(hdfs(self.cfg, "RESULTS") + "/" + name)
        df.toPandas().to_csv(self.dir / "resultados" / f"{name}.csv", index=False)

    def save_plan(self, df, name, mode="formatted"):
        """Guarda la salida de df.explain() (Tema 4: leer los planes de abajo arriba)."""
        buf = io.StringIO()
        with redirect_stdout(buf):
            df.explain(mode=mode)
        (self.dir / f"plan_{name}.txt").write_text(buf.getvalue(), encoding="utf-8")


# ----------------------------------------------------------------------------- tiempos
def run_noop(df):
    """Ejecuta la consulta entera sin traer nada al driver ni escribir datos (clase 6: format("noop"))."""
    df.write.format("noop").mode("overwrite").save()


def measure(fn, repeats):
    """Ejecuta fn() `repeats` veces y devuelve los tiempos (s) y la mediana.
    La 1.ª ejecución suele ser más lenta (arranque, caché del sistema de ficheros): por eso la mediana."""
    times = []
    for _ in range(int(repeats)):
        t = time.time()
        fn()
        times.append(round(time.time() - t, 2))
    return {"tiempos_s": times, "mediana_s": sorted(times)[len(times) // 2]}
