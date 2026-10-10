# Punto 3: procesamiento con Spark, Kafka y Structured Streaming

## 1. Procesamiento batch

Requisitos: Apache Spark 3.5.3 con soporte para Scala 2.12, Kafka 3.9.2 y Python 3. Descargue el recurso CSV **Histórico Siniestros Bogotá D.C.** desde [Datos Abiertos Bogotá](https://datosabiertos.bogota.gov.co/dataset/historico-siniestros-bogota-d-c) y guárdelo en `data/siniestros_bogota.csv`.

Ejecute desde el entorno donde Spark esté configurado:

```bash
spark-submit batch_siniestros.py data/siniestros_bogota.csv
```

El programa normaliza encabezados, identifica columnas disponibles, convierte la fecha, elimina duplicados si encuentra un identificador y descarta filas sin fecha válida. Imprime resúmenes por año, gravedad, clase y localidad y guarda Parquet limpio y resúmenes CSV en `data/resultados_siniestros/`.

Para crear gráficos estáticos de los resúmenes, instale pandas y matplotlib (`pip install -r punto3_spark/requirements.txt`) y ejecute:

```bash
python3 visualizar_resultados.py data/resultados_siniestros
```

Se guardan gráficos PNG por año, gravedad, clase y localidad en `punto3_spark/data/resultados_siniestros/graficos/`.

## 2. Kafka y Spark Structured Streaming

Inicie ZooKeeper y Kafka según el instructivo del curso, cree el topic y prepare la librería de Python:

```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic siniestros_bogota
pip install -r requirements.txt
```

Abra dos terminales adicionales conectadas a la máquina virtual. En una, inicie el productor:

```bash
python3 kafka_traffic_producer.py
```

En otra, inicie Structured Streaming con el conector compatible con el entorno del anexo (Spark 3.5.3 / Scala 2.12):

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

El productor emite eventos **sintéticos** cada dos segundos. El consumidor calcula el número de eventos por ventana de un minuto, localidad y gravedad. Las ventanas se publican después de un retraso de hasta diez segundos por la marca de agua; deje correr la demostración al menos un minuto para observar resultados. Detenga ambos programas con `Ctrl+C`.

## Nota sobre la fuente y los resultados

La fuente histórica es georreferenciada y contiene variables de fecha, clase, gravedad, localidad y coordenadas. El productor Kafka no transmite datos oficiales actuales: simula eventos con campos comparables para demostrar el flujo en tiempo real. Antes de analizar, compruebe el rango de fechas, el tamaño y la calidad del CSV descargado.
