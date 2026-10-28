import os

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


DATASET_PATH = "household_power_consumption.txt"
OUTPUT_DIR = "tarea3_resultados"
GRAPH_DIR = os.path.join(OUTPUT_DIR, "graficas")


spark = (
    SparkSession.builder
    .appName("Tarea3ConsumoElectrico")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print()
print("====================================================")
print(" TAREA 3 - ANALISIS BATCH DE CONSUMO ELECTRICO")
print("====================================================")
print()

print("[1/10] Cargando dataset...")
print()

df = (
    spark.read
    .option("header", True)
    .option("sep", ";")
    .option("inferSchema", False)
    .csv(DATASET_PATH)
)

total_original = df.count()
print(f"Registros originales: {total_original}")
print()
print("Primeros registros del dataset:")
df.show(10, truncate=False)

print()
print("[2/10] Transformando tipos de datos...")
print()

numeric_columns = [
    "Global_active_power",
    "Global_reactive_power",
    "Voltage",
    "Global_intensity",
    "Sub_metering_1",
    "Sub_metering_2",
    "Sub_metering_3",
]

timestamp_expression = F.to_timestamp(
    F.concat_ws(" ", F.col("Date"), F.col("Time")),
    "d/M/yyyy HH:mm:ss",
)

transformed_df = df.withColumn("timestamp", timestamp_expression)

for column in numeric_columns:
    transformed_df = transformed_df.withColumn(
        column,
        F.when(F.col(column) == "?", None)
        .otherwise(F.col(column))
        .cast(DoubleType()),
    )

transformed_df = transformed_df.select(
    "timestamp",
    *numeric_columns,
)

print("Esquema después de transformar los tipos:")
transformed_df.printSchema()

print()
print("[3/10] Analizando valores faltantes...")
print()

missing_values = transformed_df.select(
    [
        F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
        for column in transformed_df.columns
    ]
)

print("Valores faltantes por columna:")
missing_values.show(truncate=False)

print()
print("[4/10] Limpiando y transformando dataset...")
print()

clean_df = transformed_df.dropna()

clean_df = (
    clean_df
    .withColumn("date", F.to_date("timestamp"))
    .withColumn("year", F.year("timestamp"))
    .withColumn("month", F.month("timestamp"))
    .withColumn("day", F.dayofmonth("timestamp"))
    .withColumn("hour", F.hour("timestamp"))
    .withColumn("energy_kwh", F.col("Global_active_power") / 60.0)
)

total_clean = clean_df.count()

print(f"Registros después de la limpieza: {total_clean}")
print(f"Registros eliminados por valores faltantes: {total_original - total_clean}")
print()
print("Dataset transformado:")
clean_df.show(10, truncate=False)

print()
print("[5/10] Calculando estadísticas descriptivas...")
print()
print("====================================================")
print(" ESTADISTICAS DESCRIPTIVAS")
print("====================================================")

clean_df.select(*numeric_columns).describe().show(truncate=False)

print()
print("[6/10] Analizando consumo por hora...")
print()
print("====================================================")
print(" CONSUMO ELECTRICO POR HORA DEL DIA")
print("====================================================")

hourly_df = (
    clean_df
    .groupBy("hour")
    .agg(
        F.avg("Global_active_power").alias("average_kw"),
        F.max("Global_active_power").alias("max_kw"),
        F.count("*").alias("measurements"),
    )
    .orderBy("hour")
)

hourly_rows = hourly_df.collect()

print(f"{'Hora':<8}{'Promedio kW':<18}{'Máximo kW':<18}{'Mediciones'}")
for row in hourly_rows:
    print(
        f"{row['hour']:<8}"
        f"{round(row['average_kw'], 4):<18}"
        f"{round(row['max_kw'], 3):<18}"
        f"{row['measurements']}"
    )

print()
print("[7/10] Analizando consumo por año...")
print()
print("====================================================")
print(" CONSUMO ELECTRICO POR AÑO")
print("====================================================")

yearly_df = (
    clean_df
    .groupBy("year")
    .agg(
        F.avg("Global_active_power").alias("average_kw"),
        F.max("Global_active_power").alias("max_kw"),
        F.count("*").alias("measurements"),
    )
    .orderBy("year")
)

yearly_rows = yearly_df.collect()

print(f"{'Año':<8}{'Promedio kW':<18}{'Máximo kW':<18}{'Mediciones'}")
for row in yearly_rows:
    print(
        f"{row['year']:<8}"
        f"{round(row['average_kw'], 4):<18}"
        f"{round(row['max_kw'], 3):<18}"
        f"{row['measurements']}"
    )

print()
print("[8/10] Detectando posibles consumos anómalos...")
print()
print("====================================================")
print(" DETECCION DE POSIBLES CONSUMOS ANOMALOS")
print("====================================================")

stats = clean_df.select(
    F.avg("Global_active_power").alias("mean"),
    F.stddev("Global_active_power").alias("stddev"),
).first()

mean_power = stats["mean"]
stddev_power = stats["stddev"]
threshold = mean_power + 3 * stddev_power

anomalies_df = clean_df.filter(
    F.col("Global_active_power") > F.lit(threshold)
)

anomaly_count = anomalies_df.count()

print(f"Promedio de consumo activo: {mean_power:.4f} kW")
print(f"Desviación estándar: {stddev_power:.4f}")
print(f"Umbral utilizado (media + 3 desviaciones): {threshold:.4f} kW")
print(f"Cantidad de posibles anomalías detectadas: {anomaly_count}")
print()
print("20 consumos más altos detectados:")
print(
    f"{'Fecha y hora':<21}"
    f"{'Potencia kW':<15}"
    f"{'Voltaje':<12}"
    f"{'Intensidad'}"
)

top_anomalies = (
    anomalies_df
    .select(
        "timestamp",
        "Global_active_power",
        "Voltage",
        "Global_intensity",
    )
    .orderBy(F.desc("Global_active_power"))
    .limit(20)
    .collect()
)

for row in top_anomalies:
    print(
        f"{str(row['timestamp']):<21}"
        f"{row['Global_active_power']:<15}"
        f"{row['Voltage']:<12}"
        f"{row['Global_intensity']}"
    )

print()
print("[9/10] Guardando resultados CSV...")
print()

os.makedirs(OUTPUT_DIR, exist_ok=True)

hourly_df.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(os.path.join(OUTPUT_DIR, "consumo_por_hora"))

yearly_df.coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(os.path.join(OUTPUT_DIR, "consumo_por_anio"))

anomalies_df.select(
    "timestamp",
    "Global_active_power",
    "Voltage",
    "Global_intensity",
).coalesce(1).write.mode("overwrite").option(
    "header", True
).csv(os.path.join(OUTPUT_DIR, "consumos_anomalos"))

print("[10/10] Generando visualizaciones...")
print()

os.makedirs(GRAPH_DIR, exist_ok=True)

hourly_pd = pd.DataFrame(
    [
        {
            "hour": row["hour"],
            "average_kw": row["average_kw"],
        }
        for row in hourly_rows
    ]
)

plt.figure(figsize=(10, 5))
plt.plot(hourly_pd["hour"], hourly_pd["average_kw"], marker="o")
plt.xlabel("Hora del día")
plt.ylabel("Consumo promedio (kW)")
plt.title("Consumo eléctrico promedio por hora")
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(GRAPH_DIR, "consumo_por_hora.png"))
plt.close()

yearly_pd = pd.DataFrame(
    [
        {
            "year": row["year"],
            "average_kw": row["average_kw"],
        }
        for row in yearly_rows
    ]
)

plt.figure(figsize=(8, 5))
plt.bar(yearly_pd["year"].astype(str), yearly_pd["average_kw"])
plt.xlabel("Año")
plt.ylabel("Consumo promedio (kW)")
plt.title("Consumo eléctrico promedio por año")
plt.tight_layout()
plt.savefig(os.path.join(GRAPH_DIR, "consumo_por_anio.png"))
plt.close()

print(f"Resultados CSV guardados en:")
print(os.path.abspath(OUTPUT_DIR))
print()
print("Gráficas generadas en:")
print(os.path.abspath(GRAPH_DIR))
print()
print("====================================================")
print(" ANALISIS BATCH FINALIZADO CORRECTAMENTE")
print("====================================================")

spark.stop()
