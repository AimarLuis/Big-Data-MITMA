# NodeManager del curso + python3.
# En Hadoop Streaming, YARN arranca mapper.py y reducer.py como procesos DENTRO del
# NodeManager: sin intérprete de Python allí, cada task falla con "python3: not found".
# La imagen base es Debian 9 (stretch), cuyos repositorios ya solo están en archive.debian.org.
# OJO: stretch instala Python 3.5 -> mapper.py y reducer.py no usan f-strings.
FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8

RUN echo "deb [trusted=yes] http://archive.debian.org/debian stretch main" > /etc/apt/sources.list \
 && echo "deb [trusted=yes] http://archive.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list \
 && rm -f /etc/apt/sources.list.d/* \
 && apt-get -o Acquire::Check-Valid-Until=false update \
 && apt-get install -y --no-install-recommends python3 \
 && rm -rf /var/lib/apt/lists/* \
 && python3 --version
