"""Limpieza y análisis batch de siniestros viales de Bogotá con PySpark.

Entrada esperada: CSV exportado desde el conjunto oficial, con encabezados.
Uso: spark-submit punto3_spark/batch_siniestros.py ruta/al/archivo.csv
"""

import re
import sys
import unicodedata
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def normalize_name(name: str) -> str:
    """Normaliza encabezados de ArcGIS/Socrata para poder tratarlos uniformemente."""
    text = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def find_column(columns, *candidates):
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def parse_date(column):
    """Admite fechas ISO comunes y fechas ArcGIS exportadas como epoch en ms."""
    value = F.col(column).cast("string")
    epoch = value.cast("double")
    return F.coalesce(
        F.to_timestamp(value, "yyyy/MM/dd HH:mm:ssX"),
        F.to_timestamp(value, "yyyy/MM/dd HH:mm:ssXXX"),
        F.to_timestamp(value, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(value),
        F.to_timestamp(F.from_unixtime(epoch / F.lit(1000.0))),
        F.to_timestamp(F.from_unixtime(epoch)),
    )


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Uso: spark-submit batch_siniestros.py <archivo.csv>")

    input_path = Path(sys.argv[1]).expanduser()
    if not input_path.exists():
        raise SystemExit(f"No se encontró el CSV: {input_path}")

    spark = SparkSession.builder.appName("AnalisisBatchSiniestrosBogota").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.read.option("header", True).option("inferSchema", False).csv(str(input_path))
    data = raw.toDF(*[normalize_name(c) for c in raw.columns])
    cols = set(data.columns)

    date_col = find_column(cols, "fecha_hora_acc", "fecha_hora", "fecha_ocurrencia_acc", "fecha_ocurrencia", "fecha")
    if not date_col:
        raise SystemExit(f"No se identificó la fecha. Encabezados disponibles: {sorted(cols)}")

    locality_col = find_column(cols, "localidad", "localidad_accidente")
    class_col = find_column(cols, "clase_acc", "clase_accidente", "clase")
    severity_col = find_column(cols, "gravedad", "gravedad_accidente")
    lat_col = find_column(cols, "latitud", "latitude")
    lon_col = find_column(cols, "longitud", "longitude", "longuitud")
    id_col = find_column(cols, "codigo_accidente", "formulario", "objectid")

    cleaned = data.withColumn("event_time", parse_date(date_col))
    if locality_col:
        cleaned = cleaned.withColumn("locality", F.trim(F.col(locality_col).cast("string")))
    else:
        cleaned = cleaned.withColumn("locality", F.lit("SIN_DATO"))
    if class_col:
        cleaned = cleaned.withColumn("accident_class", F.trim(F.col(class_col).cast("string")))
    else:
        cleaned = cleaned.withColumn("accident_class", F.lit("SIN_DATO"))
    if severity_col:
        cleaned = cleaned.withColumn("severity", F.trim(F.col(severity_col).cast("string")))
    else:
        cleaned = cleaned.withColumn("severity", F.lit("SIN_DATO"))
    if lat_col:
        cleaned = cleaned.withColumn("latitude", F.regexp_replace(F.col(lat_col).cast("string"), ",", ".").cast("double"))
    else:
        cleaned = cleaned.withColumn("latitude", F.lit(None).cast("double"))
    if lon_col:
        cleaned = cleaned.withColumn("longitude", F.regexp_replace(F.col(lon_col).cast("string"), ",", ".").cast("double"))
    else:
        cleaned = cleaned.withColumn("longitude", F.lit(None).cast("double"))
    if id_col:
        cleaned = cleaned.dropDuplicates([id_col])

    cleaned = cleaned.filter(F.col("event_time").isNotNull())
    cleaned = cleaned.withColumn("year", F.year("event_time")).withColumn("month", F.month("event_time"))

    output = input_path.parent / "resultados_siniestros"
    cleaned.select("event_time", "year", "month", "locality", "accident_class", "severity", "latitude", "longitude").write.mode("overwrite").parquet(str(output / "limpios"))

    print("=== Resumen general ===")
    print(f"Registros válidos: {cleaned.count()}")
    print("=== Siniestros por año ===")
    by_year = cleaned.groupBy("year").count().orderBy("year")
    by_year.show(100, truncate=False)
    by_year.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output / "por_anio"))

    for label, column in (("por_gravedad", "severity"), ("por_clase", "accident_class"), ("por_localidad", "locality")):
        summary = cleaned.groupBy(column).count().orderBy(F.desc("count"))
        print(f"=== {label} ===")
        summary.show(30, truncate=False)
        summary.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output / label))

    print(f"Resultados guardados en: {output}")
    spark.stop()


if __name__ == "__main__":
    main()
