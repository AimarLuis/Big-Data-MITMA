#!/usr/bin/env python3
"""MAPPER de Hadoop Streaming (Tema 2: map_step).

Entrada (stdin): líneas del CSV de MITMA, tal cual están en HDFS. Cada mapper recibe un
  trozo (split) de un fichero: normalmente un bloque de 128 MB.
Salida (stdout): un par clave<TAB>valor por línea.
  clave = provincia de origen | fecha   (p. ej. "20|20221026" = Gipuzkoa, 26 de octubre)
  valor = viajes de esa fila
La provincia son los 2 primeros dígitos del código INE del municipio, así que no hace
falta ningún join. Las zonas extranjeras (FR..., PT..., "externo") se descartan.

Se ejecuta en el NodeManager con Python 3.5 (Debian 9): por eso no hay f-strings.
"""
import os
import sys

SEP = os.environ.get("MR_SEP", "|")      # llega desde config/pipeline.env con -cmdenv
I_FECHA, I_ORIGEN, I_VIAJES = 0, 2, 13   # posiciones en la cabecera de MITMA

for line in sys.stdin:
    campos = line.rstrip("\n").split(SEP)
    if len(campos) <= I_VIAJES or campos[I_FECHA] == "fecha":   # cabecera o línea rota
        continue
    origen = campos[I_ORIGEN]
    if not origen[:2].isdigit():                                 # zona extranjera
        continue
    try:
        viajes = float(campos[I_VIAJES])
    except ValueError:
        continue
    print(origen[:2] + "|" + campos[I_FECHA] + "\t" + repr(viajes))
