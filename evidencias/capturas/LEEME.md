# Capturas de las interfaces web (20-10-2026)

| Fichero | Qué muestra | Cuándo |
|---|---|---|
| `a3_namenode_datanodes.jpg` | NameNode (:9870): 2 DataNodes en servicio | Tras la 2.ª ejecución |
| `a3_spark_master.jpg` | Spark master (:8080): 1 worker de 4 cores y 3 GiB; aplicaciones `mitma-pipeline` | Durante el paso 5/7 de la 2.ª ejecución |
| `c_yarn_jobs_mapreduce.jpg` | YARN (:8088): los dos jobs `mitma-viajes-provincia-fecha` (1.ª y 2.ª ejecución), SUCCEEDED | Tras el paso 4/7 |
| `c_sparkui_stages_rdd_reduceByKey.jpg` | Spark UI (:4040), Stages de `mr_rdd`: cada repetición lee 6,4 GiB con 54 tasks y escribe solo 175,6 KiB de shuffle (combinación en el map) | Durante el paso 5/7 |
| `d3_sparkui_sql_lista.jpg`, `d3_sparkui_dag_antes_sortmerge.jpg`, `d3_sparkui_plan_antes_sortmerge.jpg`, `d3_sparkui_dag_despues_broadcast.jpg`, `d3_sparkui_plan_despues_broadcast.jpg` | Spark UI, pestaña SQL: P1 con sort-merge join (2 × `SortMergeJoin`, `ShuffleQueryStage` de 2,9 GiB y 4,6 GiB, 78 M filas) frente a broadcast join (2 × `BroadcastHashJoin`, `BroadcastQueryStage` de 7 y 52 filas) | Ejecución aparte de P1 lanzada solo para estas capturas, con el mismo código (`analytics.p1_provincia_tipo_dia`) sobre la misma tabla curada: la Spark UI de una aplicación desaparece al terminar el pipeline |
