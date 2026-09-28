"""Job 4/5 — ANALYSES: the three required Spark analyses.

Demonstrates BOTH the DataFrame API and SparkSQL (rubric: "transformaciones con
Spark DataFrames y SparkSQL"). Each analysis is written to the gold zone and
later loaded to the warehouse so Superset can chart it directly.

  1. consumption_trends  — units & revenue per category per month + 3-month
                           moving average of revenue (window functions).
  2. peak_hours          — order density by day-of-week x hour (SparkSQL).
  3. monthly_growth      — month-over-month revenue & order growth % (lag window).
"""
from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark.lib.io import read_lake, write_lake
from spark.lib.spark_session import get_spark


def consumption_trends(spark):
    """DataFrame API: monthly revenue/units per category + moving average."""
    fi = read_lake(spark, "gold", "fact_order_items")
    d = read_lake(spark, "silver", "dim_date").select("date_key", "month_label", "year", "month")
    p = read_lake(spark, "silver", "dim_product").select("product_key", "category")

    monthly = (
        fi.join(d, "date_key").join(p, "product_key")
        .groupBy("category", "month_label", "year", "month")
        .agg(F.sum("line_amount").alias("revenue"),
             F.sum("quantity").alias("units"),
             F.countDistinct("order_id").alias("orders"))
    )
    w = (Window.partitionBy("category").orderBy("year", "month")
         .rowsBetween(-2, 0))
    return (
        monthly
        .withColumn("revenue_ma3", F.round(F.avg("revenue").over(w), 2))
        .orderBy("category", "year", "month")
    )


def peak_hours(spark):
    """SparkSQL: order volume by day-of-week and hour-of-day."""
    read_lake(spark, "gold", "fact_orders").createOrReplaceTempView("fo")
    read_lake(spark, "silver", "dim_date").createOrReplaceTempView("dd")
    read_lake(spark, "silver", "dim_time").createOrReplaceTempView("dt")
    return spark.sql("""
        SELECT dd.day_of_week,
               dd.day_name,
               dt.hour,
               dt.daypart,
               COUNT(*)                       AS orders,
               ROUND(SUM(fo.net_amount), 2)   AS revenue,
               ROUND(AVG(fo.item_count), 2)   AS avg_items
        FROM fo
        JOIN dd ON dd.date_key = fo.date_key
        JOIN dt ON dt.time_key = fo.time_key
        WHERE fo.is_cancelled = false
        GROUP BY dd.day_of_week, dd.day_name, dt.hour, dt.daypart
        ORDER BY dd.day_of_week, dt.hour
    """)


def monthly_growth(spark):
    """DataFrame API + window lag: month-over-month revenue & order growth."""
    fo = read_lake(spark, "gold", "fact_orders")
    d = read_lake(spark, "silver", "dim_date").select("date_key", "month_label", "year", "month")
    monthly = (
        fo.join(d, "date_key").where(~F.col("is_cancelled"))
        .groupBy("month_label", "year", "month")
        .agg(F.round(F.sum("net_amount"), 2).alias("revenue"),
             F.count("*").alias("orders"))
    )
    w = Window.orderBy("year", "month")
    return (
        monthly
        .withColumn("prev_revenue", F.lag("revenue").over(w))
        .withColumn("prev_orders", F.lag("orders").over(w))
        .withColumn("revenue_growth_pct",
                    F.round(100 * (F.col("revenue") - F.col("prev_revenue"))
                            / F.col("prev_revenue"), 2))
        .withColumn("orders_growth_pct",
                    F.round(100.0 * (F.col("orders") - F.col("prev_orders"))
                            / F.col("prev_orders"), 2))
        .orderBy("year", "month")
    )


def main():
    spark = get_spark("analyses")

    trends = consumption_trends(spark)
    peaks = peak_hours(spark)
    growth = monthly_growth(spark)

    write_lake(trends, "gold", "analysis_consumption_trends")
    write_lake(peaks, "gold", "analysis_peak_hours")
    write_lake(growth, "gold", "analysis_monthly_growth")

    print("[analyses] consumption_trends / peak_hours / monthly_growth written")
    print("  --- sample monthly growth ---")
    growth.show(6, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
