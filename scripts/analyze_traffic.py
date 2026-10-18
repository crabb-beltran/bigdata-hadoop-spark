"""
Batch analysis of ANI toll traffic with PySpark.

Dataset: "Tráfico Vehicular ANI" (datos.gov.co, id 8yi9-t44c)
Grain:   one row per toll station x tariff category x month.

Pipeline:
  1. Load from HDFS with an explicit schema.
  2. Profile data quality (nulls, duplicates, invalid values).
  3. Clean and derive columns (period, year, theoretical revenue).
  4. Exploratory analysis with DataFrames, one RDD operation and one Spark SQL query.
  5. Persist aggregates to HDFS (CSV) and charts to /reports/figures (PNG).
"""
import argparse
import os
from operator import add

os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402
from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402

# Numeric columns are read as double on purpose: if a value comes with
# decimals or text, it shows up as null in the quality profile instead of
# being silently truncated.
SCHEMA = T.StructType([
    T.StructField("idpeaje", T.IntegerType()),
    T.StructField("peaje", T.StringType()),
    T.StructField("categoriatarifa", T.StringType()),
    T.StructField("desde", T.StringType()),
    T.StructField("hasta", T.StringType()),
    T.StructField("valortarifa", T.DoubleType()),
    T.StructField("cantidadtrafico", T.DoubleType()),
    T.StructField("cantidadevasores", T.DoubleType()),
    T.StructField("cantidadexentos787", T.DoubleType()),
])

MESES = ["ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic"]
BASE_CATEGORIES = ["I", "II", "III", "IV", "V", "VI", "VII"]
MIN_TRAFFIC_FOR_EVASION_RANKING = 1_000_000  # avoids small-denominator noise

# Chart styling (reference data-viz palette, light mode).
SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
SERIES_1 = "#2a78d6"
HIGHLIGHT = "#eb6834"


# --------------------------------------------------------------------------- #
# Spark
# --------------------------------------------------------------------------- #
def build_spark_session():
    spark = (
        SparkSession.builder
        .appName("AnalyzeTrafficANI")
        .master("spark://spark-master:7077")
        # ~150k rows: the default of 200 shuffle partitions only adds task overhead.
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_dataset(spark, path):
    return (
        spark.read
        .option("header", "true")
        .schema(SCHEMA)
        .csv(path)
    )


# --------------------------------------------------------------------------- #
# Data quality
# --------------------------------------------------------------------------- #
def profile(df):
    total = df.count()
    distinct = df.distinct().count()
    nulls = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ])
    negatives = df.select([
        F.sum((F.col(c) < 0).cast("int")).alias(c)
        for c in ["valortarifa", "cantidadtrafico", "cantidadevasores", "cantidadexentos787"]
    ])

    print("\n=== Perfil de calidad de datos ===")
    print(f"Registros totales:   {total:,}")
    print(f"Registros duplicados: {total - distinct:,}")
    print("Nulos por columna:")
    nulls.show(truncate=False)
    print("Valores negativos por columna numérica:")
    negatives.show(truncate=False)
    return total


def clean(df):
    cleaned = (
        df.dropDuplicates()
        .withColumn("peaje", F.upper(F.trim("peaje")))
        .withColumn("categoriatarifa", F.upper(F.trim("categoriatarifa")))
        .withColumn("periodo", F.to_date(F.substring("desde", 1, 10), "yyyy-MM-dd"))
        .filter(
            F.col("idpeaje").isNotNull()
            & F.col("periodo").isNotNull()
            & F.col("cantidadtrafico").isNotNull()
            & (F.col("cantidadtrafico") >= 0)
            & (F.col("valortarifa") >= 0)
        )
        .fillna(0, subset=["cantidadevasores", "cantidadexentos787"])
        .withColumn("periodo", F.trunc("periodo", "month"))
        .withColumn("anio", F.year("periodo"))
        .withColumn("mes", F.month("periodo"))
        .withColumn("cantidadtrafico", F.col("cantidadtrafico").cast("long"))
        .withColumn("cantidadevasores", F.col("cantidadevasores").cast("long"))
        .withColumn("cantidadexentos787", F.col("cantidadexentos787").cast("long"))
        # Theoretical revenue: upper bound, assumes every registered vehicle
        # paid the full tariff (exempt vehicles are not discounted).
        .withColumn(
            "recaudo_teorico",
            (F.col("valortarifa") * F.col("cantidadtrafico")).cast("long"),
        )
        .withColumn(
            "grupo_categoria",
            F.when(F.col("categoriatarifa").isin(BASE_CATEGORIES), F.col("categoriatarifa"))
            .otherwise(F.lit("ESPECIALES")),
        )
    )
    return cleaned.cache()


# --------------------------------------------------------------------------- #
# Analysis
# --------------------------------------------------------------------------- #
def monthly_traffic(df):
    return (
        df.groupBy("periodo")
        .agg(
            F.sum("cantidadtrafico").alias("trafico"),
            F.sum("recaudo_teorico").alias("recaudo_teorico"),
            F.sum("cantidadevasores").alias("evasores"),
        )
        .orderBy("periodo")
    )


def yearly_summary(df):
    return (
        df.groupBy("anio")
        .agg(
            F.sum("cantidadtrafico").alias("trafico"),
            F.sum("recaudo_teorico").alias("recaudo_teorico"),
            F.countDistinct("mes").alias("meses_reportados"),
            F.countDistinct("idpeaje").alias("peajes_activos"),
        )
        .orderBy("anio")
    )


def top_tolls_by_traffic(df, n=10):
    return (
        df.groupBy("idpeaje", "peaje")
        .agg(F.sum("cantidadtrafico").alias("trafico"))
        .orderBy(F.col("trafico").desc())
        .limit(n)
    )


def evasion_ranking_sql(spark, df, n=10):
    df.createOrReplaceTempView("trafico")
    return spark.sql(f"""
        SELECT peaje,
               SUM(cantidadtrafico)  AS trafico,
               SUM(cantidadevasores) AS evasores,
               ROUND(SUM(cantidadevasores) * 1000.0 / SUM(cantidadtrafico), 2)
                   AS evasores_por_mil
        FROM trafico
        GROUP BY peaje
        HAVING SUM(cantidadtrafico) >= {MIN_TRAFFIC_FOR_EVASION_RANKING}
        ORDER BY evasores_por_mil DESC
        LIMIT {n}
    """)


def traffic_by_category(df):
    total = df.agg(F.sum("cantidadtrafico")).first()[0]
    return (
        df.groupBy("grupo_categoria")
        .agg(F.sum("cantidadtrafico").alias("trafico"))
        .withColumn("participacion_pct", F.round(F.col("trafico") * 100.0 / total, 2))
        .orderBy(F.col("trafico").desc())
    )


def traffic_by_category_rdd(df, n=5):
    """Same aggregation as the DataFrame version, with the RDD API
    (map -> reduceByKey -> takeOrdered), to contrast both abstractions."""
    return (
        df.select("categoriatarifa", "cantidadtrafico").rdd
        .map(lambda row: (row.categoriatarifa, row.cantidadtrafico))
        .reduceByKey(add)
        .takeOrdered(n, key=lambda kv: -kv[1])
    )


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def save_csv(df, path):
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"  -> {path}")


def _style(ax, title, xlabel=None, ylabel=None):
    ax.set_facecolor(SURFACE)
    ax.figure.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", fontsize=13, fontweight="bold", color=TEXT_PRIMARY, pad=12)
    if xlabel:
        ax.set_xlabel(xlabel, color=TEXT_SECONDARY)
    if ylabel:
        ax.set_ylabel(ylabel, color=TEXT_SECONDARY)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=9)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _save(fig, out_dir, name):
    path = os.path.join(out_dir, name)
    fig.tight_layout()
    fig.savefig(path, dpi=150, facecolor=SURFACE)
    plt.close(fig)
    print(f"  -> {path}")


def plot_monthly(pdf, out_dir):
    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.plot(pdf["periodo"], pdf["trafico"] / 1e6, color=SERIES_1, linewidth=2)
    low = pdf.loc[pdf["trafico"].idxmin()]
    ax.scatter([low["periodo"]], [low["trafico"] / 1e6], color=HIGHLIGHT, s=50, zorder=3)
    ax.annotate(
        f"Mínimo: {MESES[low['periodo'].month - 1]} {low['periodo'].year}\n"
        f"{low['trafico'] / 1e6:.1f} M vehículos",
        xy=(low["periodo"], low["trafico"] / 1e6),
        xytext=(25, -10), textcoords="offset points", fontsize=9, color=TEXT_PRIMARY,
    )
    _style(ax, "Tráfico mensual en peajes ANI", ylabel="Millones de vehículos")
    _save(fig, out_dir, "01_trafico_mensual.png")


def plot_yearly_revenue(pdf, out_dir):
    fig, ax = plt.subplots(figsize=(11, 4.5))
    partial = pdf["meses_reportados"] < 12
    colors = [GRID if p else SERIES_1 for p in partial]
    ax.bar(pdf["anio"].astype(str), pdf["recaudo_teorico"] / 1e12, color=colors, width=0.7)
    for x, (val, p) in enumerate(zip(pdf["recaudo_teorico"] / 1e12, partial)):
        if p:
            ax.text(x, val, "parcial", ha="center", va="bottom", fontsize=8, color=TEXT_SECONDARY)
    _style(ax, "Recaudo teórico anual (tráfico × tarifa)", ylabel="Billones de COP")
    _save(fig, out_dir, "02_recaudo_teorico_anual.png")


def plot_ranking(pdf, label_col, value_col, title, xlabel, out_dir, name, fmt, axis_fmt=None):
    pdf = pdf.sort_values(value_col)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.barh(pdf[label_col], pdf[value_col], color=SERIES_1, height=0.65)
    for y, val in enumerate(pdf[value_col]):
        ax.text(val, y, f" {fmt(val)}", va="center", fontsize=8, color=TEXT_SECONDARY)
    _style(ax, title, xlabel=xlabel)
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.grid(axis="y", visible=False)
    if axis_fmt:
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: axis_fmt(v)))
    _save(fig, out_dir, name)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    parser = argparse.ArgumentParser(description="Batch analysis of ANI toll traffic")
    parser.add_argument("--input", default="hdfs://namenode:8020/data/trafico_ani.csv")
    parser.add_argument("--output", default="hdfs://namenode:8020/data/output/trafico")
    parser.add_argument("--figures", default="/reports/figures")
    args = parser.parse_args()

    spark = build_spark_session()

    raw = load_dataset(spark, args.input)
    total_raw = profile(raw)

    df = clean(raw)
    total_clean = df.count()
    print(f"Registros después de limpieza: {total_clean:,} "
          f"(descartados: {total_raw - total_clean:,})")
    df.printSchema()

    monthly = monthly_traffic(df)
    yearly = yearly_summary(df)
    top_tolls = top_tolls_by_traffic(df)
    evasion = evasion_ranking_sql(spark, df)
    categories = traffic_by_category(df)

    print("\n=== Resumen anual ===")
    yearly.show(30, truncate=False)
    print("=== Top 10 peajes por tráfico ===")
    top_tolls.show(truncate=False)
    print(f"=== Top 10 peajes por evasión (tráfico >= {MIN_TRAFFIC_FOR_EVASION_RANKING:,}) ===")
    evasion.show(truncate=False)
    print("=== Participación por grupo de categoría ===")
    categories.show(truncate=False)

    print("=== Top 5 categorías por tráfico (API RDD: map + reduceByKey) ===")
    for category, traffic in traffic_by_category_rdd(df):
        print(f"  {category:<10} {traffic:>15,}")

    print("\nGuardando agregados en HDFS:")
    save_csv(monthly, f"{args.output}/mensual")
    save_csv(yearly, f"{args.output}/anual")
    save_csv(top_tolls, f"{args.output}/top_peajes_trafico")
    save_csv(evasion, f"{args.output}/top_peajes_evasion")
    save_csv(categories, f"{args.output}/participacion_categoria")

    # Aggregates are small (<200 rows): safe to bring to the driver for charts.
    os.makedirs(args.figures, exist_ok=True)
    print("\nGenerando gráficas:")
    plot_monthly(monthly.toPandas(), args.figures)
    plot_yearly_revenue(yearly.toPandas(), args.figures)
    plot_ranking(
        top_tolls.toPandas(), "peaje", "trafico",
        "Top 10 peajes por tráfico acumulado", "Millones de vehículos",
        args.figures, "03_top_peajes_trafico.png", lambda v: f"{v / 1e6:.1f} M",
        axis_fmt=lambda v: f"{v / 1e6:.0f}",
    )
    plot_ranking(
        evasion.toPandas(), "peaje", "evasores_por_mil",
        "Top 10 peajes por evasión", "Evasores por cada 1.000 vehículos",
        args.figures, "04_top_peajes_evasion.png", lambda v: f"{v:.2f}",
    )
    plot_ranking(
        categories.toPandas(), "grupo_categoria", "participacion_pct",
        "Participación del tráfico por categoría", "% del tráfico total",
        args.figures, "05_participacion_categoria.png", lambda v: f"{v:.1f} %",
    )

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()