#!/usr/bin/env python3
"""REDUCER de Hadoop Streaming (Tema 2: reduce_step).

Entrada (stdin): pares clave<TAB>valor YA ORDENADOS por clave. Esa ordenación (y que todas
  las líneas de una clave lleguen al mismo reducer) es el shuffle + sort de Hadoop.
OJO: Streaming no entrega grupos (clave, [valores]) como en la clase de Python del Tema 2,
  sino líneas sueltas ordenadas. El reducer detecta dónde termina cada clave.
Salida: clave<TAB>suma de viajes.
"""
import sys

clave_actual = None
total = 0.0

for line in sys.stdin:
    clave, valor = line.rstrip("\n").split("\t", 1)
    valor = float(valor)
    if clave == clave_actual:
        total += valor                                   # misma clave: acumular
    else:
        if clave_actual is not None:                     # cambio de clave: emitir la anterior
            print(clave_actual + "\t" + str(round(total, 3)))
        clave_actual, total = clave, valor

if clave_actual is not None:                             # la última clave no tiene "siguiente"
    print(clave_actual + "\t" + str(round(total, 3)))
