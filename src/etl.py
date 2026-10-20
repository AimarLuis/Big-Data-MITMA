"""B.1 + D.1 + E.2 — RAW (CSV) -> CURATED (Parquet particionado por fecha).

Recorrido, igual que el Lab de integración del Tema 5:
  1. Leer con ESQUEMA EXPLÍCITO (StructType, nada de inferSchema).
  2. Tipar (fecha texto -> DateType) y marcar zonas extranjeras.
  3. Reglas de calidad -> filas válidas / CUARENTENA (no se borran, se guardan).
  4. Duplicados.
  5. Columnas derivadas para el análisis (provincia de origen y destino).
  6. Escritura IDEMPOTENTE: overwrite dinámico + repartition(fecha) (1 fichero por día).
Cada paso deja su nº de filas en evidencias/metrics.json (D.1: antes / después).
"""
from pyspark.sql import functions as F
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

from common import hdfs

# Esquema de los ficheros diarios de MITMA, en el ORDEN de la cabecera:
# fecha|periodo|origen|destino|distancia|actividad_origen|actividad_destino|
# estudio_origen_posible|estudio_destino_posible|residencia|renta|edad|sexo|viajes|viajes_km
VIAJES_SCHEMA = StructType([
    StructField("fecha", StringType(), True),                  # 20221026 -> se convierte a fecha
    StructField("periodo", IntegerType(), True),                # hora de inicio: "00".."23" -> 0..23
    StructField("origen", StringType(), True),                  # código INE de municipio (o FR..., PT..., externo)
    StructField("destino", StringType(), True),
    StructField("distancia", StringType(), True),               # banda: 0.5-2, 2-10, 10-50, >50 km
    StructField("actividad_origen", StringType(), True),        # casa, trabajo_estudio, frecuente, no_frecuente
    StructField("actividad_destino", StringType(), True),
    StructField("estudio_origen_posible", StringType(), True),  # si/no
    StructField("estudio_destino_posible", StringType(), True),
    StructField("residencia", StringType(), True),              # provincia de residencia (2 dígitos)
    StructField("renta", StringType(), True),                   # <10, 10-15, >15 (miles de euros)
    StructField("edad", StringType(), True),                    # 0-25, 25-45, 45-65, 65-100, NA
    StructField("sexo", StringType(), True),                    # hombre, mujer, NA
    StructField("viajes", DoubleType(), True),                  # estimación de MITMA (decimal)
    StructField("viajes_km", DoubleType(), True),               # viajes x km: NUNCA como predictor (fuga)
])

# La tabla de zonificación viene exportada con el índice de pandas como primera columna sin nombre:
# |ID|name  ->  0|01001|Alegría-Dulantzi
# Los nombres del esquema coinciden con la cabecera (así Spark no avisa) y se renombran al leer.
MUNICIPIOS_SCHEMA = StructType([
    StructField("_c0", IntegerType(), True),
    StructField("ID", StringType(), True),
    StructField("name", StringType(), True),
])

# Todas las columnas excepto las medidas: identifican un flujo (clave natural de una fila)
CLAVE_NATURAL = ["fecha", "periodo", "origen", "destino", "distancia", "actividad_origen",
                 "actividad_destino", "estudio_origen_posible", "estudio_destino_posible",
                 "residencia", "renta", "edad", "sexo"]


def read_raw(spark, cfg):
    return (spark.read.schema(VIAJES_SCHEMA)
            .option("header", True)
            .option("sep", cfg["CSV_SEP"])
            .csv(hdfs(cfg, "RAW_VIAJES")))            # lee la carpeta entera: los 7 días


def read_municipios(spark, cfg):
    return (spark.read.schema(MUNICIPIOS_SCHEMA)
            .option("header", True)
            .option("sep", cfg["CSV_SEP"])
            .csv(hdfs(cfg, "RAW_MUNICIPIOS"))
            .select(F.col("ID").alias("id_municipio"), F.col("name").alias("nombre_municipio")))


def read_aux(spark, cfg, name):
    """Tablas auxiliares construidas por el equipo (data_aux/): provincias.csv, calendario.csv."""
    return spark.read.option("header", True).csv(hdfs(cfg, "RAW_AUX") + f"/{name}.csv")


def es_extranjero(col):
    """MITMA codifica las zonas fuera de España como FRxxx (Francia), PTxxx (Portugal) o 'externo'."""
    return ~F.col(col).rlike("^[0-9]{5}")


def run(spark, cfg, ev):
    raw = read_raw(spark, cfg)

    # ---- 1-2. Tipado ---------------------------------------------------------------
    typed = (raw
             .withColumn("fecha", F.to_date("fecha", cfg["DATE_FORMAT"]))   # texto -> fecha (null si no encaja)
             .withColumn("origen_extranjero", es_extranjero("origen"))
             .withColumn("destino_extranjero", es_extranjero("destino")))

    # ---- 3. Reglas de calidad -> válidas / cuarentena (Tema 5: "Quarantine, don't delete")
    rule = (F.col("fecha").between(F.lit(cfg["FECHA_INICIO"]), F.lit(cfg["FECHA_FIN"]))   # fecha válida y de la semana
            & F.col("periodo").between(0, 23)                                            # hora del día
            & F.col("origen").isNotNull() & F.col("destino").isNotNull()                 # claves del flujo
            & F.col("viajes").isNotNull() & (F.col("viajes") > 0)                        # un flujo sin viajes no es un flujo
            & (F.col("viajes_km").isNull() | (F.col("viajes_km") >= 0)))                 # km negativos = imposible
    ok = F.coalesce(rule, F.lit(False))            # True AND null = null -> el null cuenta como malo

    flagged = typed.withColumn("valida", ok)

    # Todos los conteos de la entrada en UNA sola pasada (cada count() relee los 7 GB de CSV)
    stats = flagged.agg(
        F.count(F.lit(1)).alias("filas_raw"),
        F.sum(F.col("valida").cast("int")).alias("filas_validas"),
        F.sum(F.col("fecha").isNull().cast("int")).alias("nulos_fecha"),
        F.sum(F.col("periodo").isNull().cast("int")).alias("nulos_periodo"),
        F.sum(F.col("viajes").isNull().cast("int")).alias("nulos_viajes"),
        F.sum((F.col("edad") == "NA").cast("int")).alias("edad_NA"),
        F.sum((F.col("sexo") == "NA").cast("int")).alias("sexo_NA"),
        F.sum((F.col("origen_extranjero") | F.col("destino_extranjero")).cast("int")).alias("filas_con_zona_extranjera"),
        F.sum("viajes").alias("viajes_raw"),
    ).first().asDict()
    filas_raw, filas_validas = stats["filas_raw"], stats["filas_validas"]
    filas_malas = filas_raw - filas_validas
    ev.record("etl_1_entrada", stats)

    bad = flagged.filter(~F.col("valida")).drop("valida")
    bad.write.mode("overwrite").parquet(hdfs(cfg, "QUARANTINE"))
    bad_ratio = filas_malas / filas_raw if filas_raw else 1.0
    ev.record("etl_2_cuarentena", {"filas": filas_malas, "proporcion": round(bad_ratio, 6),
                                   "umbral_detener": float(cfg["MAX_BAD_RATIO"])})
    # Tema 5: "Fail loudly". Acción = DETENER si la fuente cambia de golpe
    assert bad_ratio <= float(cfg["MAX_BAD_RATIO"]), \
        f"Demasiadas filas inválidas: {bad_ratio:.2%}. Revisa la zona de cuarentena."

    good = flagged.filter(F.col("valida")).drop("valida")

    # ---- 4. Duplicados (acción = REGISTRAR y eliminar) -----------------------------------
    deduped = good.dropDuplicates(CLAVE_NATURAL)

    # ---- 5. Columnas derivadas ------------------------------------------------------------
    # La provincia son los 2 primeros dígitos del código INE. Para zonas extranjeras -> null.
    curated = (deduped
               .withColumn("cod_prov_origen",
                           F.when(~F.col("origen_extranjero"), F.substring("origen", 1, 2)))
               .withColumn("cod_prov_destino",
                           F.when(~F.col("destino_extranjero"), F.substring("destino", 1, 2))))

    # ---- 6. Escritura idempotente (Tema 5, Demo 2) ----------------------------------------
    # dynamic: solo se reemplazan las particiones (fechas) presentes en el DataFrame
    # repartition(fecha): todas las filas de un día van a la misma task -> 1 fichero por día
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    col = cfg["PARTITION_COLUMN"]
    (curated.repartition(col)
            .write.mode("overwrite")
            .partitionBy(col)
            .parquet(hdfs(cfg, "CURATED")))

    # ---- Conteos de después (sobre el Parquet: ya es rápido) -------------------------------
    out = spark.read.parquet(hdfs(cfg, "CURATED"))
    after = out.agg(F.count(F.lit(1)).alias("filas_curated"),
                    F.countDistinct("fecha").alias("fechas"),
                    F.sum("viajes").alias("viajes_curated")).first().asDict()
    after["duplicados_eliminados"] = filas_validas - after["filas_curated"]
    ev.record("etl_3_curated", after)

    # ---- Control de la tabla auxiliar (acción = AVISAR) -------------------------------------
    # Antes de un join: la clave tiene que ser única (clase 6, "TRAP: duplicate keys")
    mun = read_municipios(spark, cfg)
    claves_repetidas = mun.groupBy("id_municipio").count().filter("count > 1").count()
    sin_nombre = (out.filter(~F.col("origen_extranjero"))
                     .select("origen").distinct()
                     .join(mun, out.origen == mun.id_municipio, "left_anti").count())
    total_origenes = out.filter(~F.col("origen_extranjero")).select("origen").distinct().count()
    ratio = sin_nombre / total_origenes if total_origenes else 0
    ev.record("etl_4_tabla_auxiliar", {"municipios_en_tabla": mun.count(),
                                       "claves_repetidas": claves_repetidas,
                                       "origenes_espanoles_distintos": total_origenes,
                                       "origenes_sin_nombre": sin_nombre,
                                       "accion": "AVISO" if ratio > float(cfg["MAX_UNMATCHED_RATIO"]) else "OK"})
    if ratio > float(cfg["MAX_UNMATCHED_RATIO"]):
        print(f"   AVISO: {ratio:.2%} de municipios de origen no están en la tabla auxiliar")

    # Historial de ejecuciones: con 2 ejecuciones seguidas, las cifras deben ser idénticas (E.2)
    ev.append_run({"filas_raw": filas_raw, "filas_cuarentena": filas_malas,
                   "filas_curated": after["filas_curated"],
                   "viajes_curated": round(after["viajes_curated"], 1)})
