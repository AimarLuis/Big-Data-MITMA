"""E.3 — Línea base de Machine Learning con MLlib (clase 7).

Problema: regresión lineal multivariable.
  Objetivo (label): viajes medios diarios que salen de un municipio para un rango de edad.
  Entradas: municipio de origen y rango de edad. Las dos son CATEGÓRICAS
            -> StringIndexer + OneHotEncoder (clase 7: "Encoding a text column").
  Sin variables de tiempo (decisión del equipo: no se plantea como serie temporal).
Grano: una fila por (municipio, edad) con la media de viajes/día de la semana. Con la fila
  cruda (origen x destino x hora x segmento) casi toda la variación vendría de lo que no está
  en el modelo (destino, hora, distancia) y el R2 sería ~0.
Objetivo en escala log: log1p(viajes/día). Hay municipios de 300 habitantes y Madrid: en
  escala lineal el error lo dominarían las grandes ciudades.
Evaluación: RMSE, MAE y R2 en test, comparados con dos líneas base triviales.
"""
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions as F

from common import hdfs


def build_dataset(cur, municipio_col):
    n_dias = cur.select("fecha").distinct().count()
    return (cur.filter(~F.col(f"{municipio_col}_extranjero"))
               .groupBy(municipio_col, "edad")
               .agg((F.sum("viajes") / n_dias).alias("viajes_dia"))
               .withColumn("label", F.log1p("viajes_dia")))


def evaluate(pred):
    return {m: round(RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName=m).evaluate(pred), 4)
            for m in ("rmse", "mae", "r2")}


def run(spark, cfg, ev):
    mun = cfg["ML_MUNICIPIO_COL"]
    cur = spark.read.parquet(hdfs(cfg, "CURATED"))
    data = build_dataset(cur, mun).cache()        # MLlib recorre los datos varias veces: cache (clase 6)

    frac = float(cfg["ML_TRAIN_FRACTION"])
    train, test = data.randomSplit([frac, 1 - frac], seed=int(cfg["ML_SEED"]))   # fit solo verá train

    # handleInvalid="keep": un municipio que solo aparece en test no rompe el modelo
    indexer_mun = StringIndexer(inputCol=mun, outputCol="mun_idx", handleInvalid="keep")
    indexer_edad = StringIndexer(inputCol="edad", outputCol="edad_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["mun_idx", "edad_idx"], outputCols=["mun_vec", "edad_vec"],
                            handleInvalid="keep")
    assembler = VectorAssembler(inputCols=["mun_vec", "edad_vec"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="label", regParam=0.01)

    pipeline = Pipeline(stages=[indexer_mun, indexer_edad, encoder, assembler, lr])
    model = pipeline.fit(train)                    # los codificadores también se ajustan SOLO con train
    pred = model.transform(test)

    # Líneas base triviales: (1) la media global de train; (2) la media del municipio en train
    media_global = train.agg(F.avg("label")).first()[0]
    base_global = test.withColumn("prediction", F.lit(media_global))
    media_mun = train.groupBy(mun).agg(F.avg("label").alias("prediction"))
    base_mun = (test.join(media_mun, mun, "left")
                    .withColumn("prediction", F.coalesce("prediction", F.lit(media_global))))

    # Coeficientes de la edad: diferencias relativas entre rangos (escala log1p).
    # El vector de features es [municipio one-hot | edad one-hot]. El ancho de cada bloque lo da
    # el OneHotEncoder ajustado: categorySizes = nº de etiquetas + 1 (la categoría "desconocida"
    # que añade handleInvalid="keep"). La última posición (desconocida) es la que dropLast elimina.
    edad_labels = model.stages[1].labels
    n_mun = len(model.stages[0].labels)
    ancho_mun = model.stages[2].categorySizes[0]
    coefs = model.stages[-1].coefficients.toArray()
    coef_edad = {lab: round(float(c), 4) for lab, c in zip(edad_labels, coefs[ancho_mun:ancho_mun + len(edad_labels)])}

    ev.record("e3_ml", {
        "filas": {"total": data.count(), "train": train.count(), "test": test.count()},
        "municipios_distintos": n_mun, "rangos_edad": edad_labels,
        "n_features": model.stages[-1].numFeatures,
        "objetivo": "log1p(viajes medios diarios por municipio de origen y edad)",
        "regresion_lineal": evaluate(pred),
        "baseline_media_global": evaluate(base_global),
        "baseline_media_municipio": evaluate(base_mun),
        "coeficientes_edad": coef_edad,
    })
    ev.save_result(pred.select(mun, "edad", F.round("viajes_dia", 1).alias("viajes_dia"),
                               F.round("label", 4).alias("label"),
                               F.round("prediction", 4).alias("prediccion")),
                   "e3_predicciones_test")
    data.unpersist()
