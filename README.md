# Tarea 3 - Procesamiento de Datos con Apache Spark

Proyecto desarrollado para la Tarea 3 del curso Big Data de la Universidad Nacional Abierta y a Distancia (UNAD).

## Descripción

El proyecto analiza patrones de consumo eléctrico residencial utilizando Apache Spark. Se trabaja con el conjunto de datos **Individual Household Electric Power Consumption**, publicado en el UCI Machine Learning Repository.

El análisis incluye dos componentes:

- Procesamiento batch con PySpark sobre un conjunto de datos de más de dos millones de registros.
- Procesamiento de datos en tiempo real utilizando Apache Kafka y Spark Structured Streaming.

## Problema seleccionado

El objetivo es analizar el comportamiento del consumo eléctrico de un hogar, identificar patrones de consumo por hora y por año, y detectar posibles consumos anómalos a partir de valores significativamente superiores al comportamiento promedio.

## Conjunto de datos

Dataset: **Individual Household Electric Power Consumption**

Fuente: UCI Machine Learning Repository.

Archivo utilizado:

```text
household_power_consumption.txt
```

El archivo contiene mediciones de consumo eléctrico tomadas cada minuto entre diciembre de 2006 y noviembre de 2010.

Principales variables utilizadas:

- `Global_active_power`
- `Global_reactive_power`
- `Voltage`
- `Global_intensity`
- `Sub_metering_1`
- `Sub_metering_2`
- `Sub_metering_3`

El dataset no se incluye en este repositorio debido a su tamaño.

## Estructura

```text
.
├── tarea3_batch.py
├── kafka_producer.py
├── spark_streaming_consumer.py
├── requirements.txt
├── .gitignore
└── README.md
```

## Procesamiento batch

El archivo `tarea3_batch.py` realiza:

1. Carga del dataset.
2. Conversión de tipos.
3. Identificación de valores faltantes.
4. Limpieza de registros incompletos.
5. Creación de variables de fecha y hora.
6. Cálculo de energía consumida en kWh.
7. Estadísticas descriptivas.
8. Análisis del consumo por hora.
9. Análisis del consumo por año.
10. Detección de posibles consumos anómalos.
11. Exportación de resultados en CSV.
12. Generación de gráficas.

### Ejecución

Ubicar el dataset en el mismo directorio del script:

```bash
python3 tarea3_batch.py
```

Los resultados se generan en:

```text
tarea3_resultados/
```

y las gráficas en:

```text
tarea3_resultados/graficas/
```

## Procesamiento en tiempo real

Para el procesamiento en tiempo real se utiliza un topic de Kafka llamado:

```text
sensor_data
```

El productor genera cada segundo datos simulados de sensores con:

- identificador del sensor;
- temperatura;
- humedad;
- timestamp.

### Ejecutar productor Kafka

```bash
python3 kafka_producer.py
```

## Spark Structured Streaming

El archivo `spark_streaming_consumer.py` consume los datos del topic `sensor_data` y calcula promedios de temperatura y humedad por sensor en ventanas de un minuto.

La ejecución utilizada en el entorno de práctica fue:

```bash
spark-submit \
  --master local[2] \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=4 \
  --jars /home/vboxuser/spark-sql-kafka-0-10_2.12-3.5.8.jar,/home/vboxuser/spark-token-provider-kafka-0-10_2.12-3.5.8.jar,/home/vboxuser/kafka-clients-3.4.1.jar,/home/vboxuser/commons-pool2-2.11.1.jar \
  /home/vboxuser/spark_streaming_consumer.py
```

La reducción de las particiones de shuffle a cuatro permite ejecutar los micro-batches de forma adecuada en una máquina virtual con recursos limitados.

## Tecnologías utilizadas

- Python 3.10
- Apache Spark 3.5.8
- PySpark
- Apache Kafka 3.9.2
- Java 11
- Matplotlib
- Pandas

## Resultados principales

El procesamiento batch analizó 2.075.259 registros originales y, después de eliminar registros con valores faltantes, procesó 2.049.280 mediciones válidas.

Entre los resultados obtenidos se identificó:

- consumo activo promedio aproximado de 1,0916 kW;
- mayor consumo promedio durante las horas de la noche;
- valor máximo de potencia activa de 11,122 kW;
- 36.160 registros considerados como posibles consumos anómalos usando un umbral de media más tres desviaciones estándar.

## Autor

Sebastián Alberto Naranjo Quiceno  
Ingeniería de Sistemas - UNAD
