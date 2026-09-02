"""
Análisis genérico de datasets tabulares con PySpark sobre HDFS.
Dataset actual: Tarifas de Peajes ANI (datos.gov.co)
Columnas: idpeaje, peaje, ultimofechacambiopeaje, idcategoriatarifa, valor
"""
import argparse

from pyspark.sql import SparkSession, functions as F


def build_spark_session(app_name: str = "AnalyzeDataset") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .getOrCreate()
    )


def load_dataset(spark: SparkSession, hdfs_path: str):
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(hdfs_path)
    )


def run_analysis(df):
    print("=== Schema ===")
    df.printSchema()

    print("=== Muestra de datos ===")
    df.show(10, truncate=False)

    print("=== Estadísticas descriptivas ===")
    df.summary().show()

    print("=== Peajes con tarifa superior a 10000 COP ===")
    high_value = df.filter(F.col("valor") > 10000).select(
        "peaje", "idcategoriatarifa", "valor", "ultimofechacambiopeaje"
    )
    high_value.show(20, truncate=False)

    print("=== Top 10 tarifas más altas ===")
    df.sort(F.col("valor").desc()).show(10, truncate=False)

    print("=== Tarifa promedio y máxima por categoría vehicular ===")
    by_category = (
        df.groupBy("idcategoriatarifa")
        .agg(
            F.count("*").alias("cantidad_peajes"),
            F.round(F.avg("valor"), 2).alias("tarifa_promedio"),
            F.max("valor").alias("tarifa_maxima"),
            F.min("valor").alias("tarifa_minima"),
        )
        .orderBy(F.col("idcategoriatarifa"))
    )
    by_category.show(truncate=False)

    return high_value, by_category


def save_results(df, output_path: str):
    (
        df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"Resultados guardados en: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Análisis genérico con PySpark sobre HDFS")
    parser.add_argument(
        "--input",
        default="hdfs://namenode:8020/data/peajes_ani.csv",
        help="Ruta HDFS del CSV de entrada",
    )
    parser.add_argument(
        "--output",
        default="hdfs://namenode:8020/data/output/high_value",
        help="Ruta HDFS donde guardar el resultado filtrado",
    )
    parser.add_argument(
        "--output-agg",
        default="hdfs://namenode:8020/data/output/by_category",
        help="Ruta HDFS donde guardar la agregación por categoría",
    )
    args = parser.parse_args()

    spark = build_spark_session()
    df = load_dataset(spark, args.input)
    high_value, by_category = run_analysis(df)
    save_results(high_value, args.output)
    save_results(by_category, args.output_agg)

    spark.stop()


if __name__ == "__main__":
    main()