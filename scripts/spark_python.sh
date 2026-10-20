#!/bin/bash
# Ejecuta un script PySpark dentro del contenedor jupyter.
# En la imagen jupyter/pyspark-notebook, PySpark se añade al PYTHONPATH al arrancar Jupyter,
# pero no en un "docker exec". Aquí se añade a mano (librería de Spark + py4j) y se lanza
# python, que crea la SparkSession (y con ella el driver) según config/pipeline.env.
# PYTHONUNBUFFERED=1: cada print sale al momento por la consola del punto de entrada
# (sin él, Python acumula la salida en un búfer cuando no escribe en una terminal).
set -euo pipefail
cd /home/jovyan/work
export PYTHONPATH="$SPARK_HOME/python:$(ls "$SPARK_HOME"/python/lib/py4j-*-src.zip | head -1)"
export PYTHONUNBUFFERED=1
exec python "$@"
