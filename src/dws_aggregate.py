"""
DWS 层 - 汇总数据层（轻度聚合宽表）
基于 DWD 明细数据，构建三个维度汇总宽表：
  1. dws_user    用户级宽表（RFM 基础数据）
  2. dws_product 商品级宽表（商品销售汇总）
  3. dws_daily   日级销售汇总（时间趋势分析基础）
  4. dws_country 国家级汇总（地域分析基础）
输出：Parquet 到 output/dws/
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
import os


def build_dws_user(spark, dwd_path):
    """
    用户级汇总宽表 - 后续 RFM 分析的数据基础
    字段：customer_id, first_order_date, last_order_date, order_days,
          total_orders, total_quantity, total_amount, avg_order_amount,
          product_count, country
    """
    print("\n--- DWS 用户级宽表 ---")
    dwd = spark.read.parquet(dwd_path)

    # 按用户聚合
    user_df = (
        dwd.filter(F.col("CustomerID").isNotNull())
        .groupBy("CustomerID", "country")
        .agg(
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_date").alias("order_days"),
            F.countDistinct("InvoiceNo").alias("total_orders"),
            F.sum("Quantity").alias("total_quantity"),
            F.sum("amount").cast(DecimalType(14, 2)).alias("total_amount"),
            F.countDistinct("StockCode").alias("product_count"),
        )
        .withColumn(
            "avg_order_amount",
            (F.col("total_amount") / F.col("total_orders")).cast(DecimalType(12, 2))
        )
        .withColumnRenamed("CustomerID", "customer_id")
    )

    # RFM 基础字段（R=最近购买距今天数，以数据集最大日期为基准）
    max_date = dwd.agg(F.max("order_date")).collect()[0][0]
    user_df = user_df.withColumn(
        "recency_days",
        F.datediff(F.lit(max_date), F.col("last_order_date"))
    )

    user_df.createOrReplaceTempView("dws_user")

    count = user_df.count()
    print(f"用户数: {count:,}")
    print(f"人均消费: {user_df.agg(F.avg('total_amount')).collect()[0][0]}")
    print(f"人均订单数: {user_df.agg(F.avg('total_orders')).collect()[0][0]}")

    return user_df


def build_dws_product(spark, dwd_path):
    """
    商品级汇总宽表
    字段：stock_code, description, total_quantity, total_amount,
          order_count, customer_count, avg_unit_price
    """
    print("\n--- DWS 商品级宽表 ---")
    dwd = spark.read.parquet(dwd_path)

    product_df = (
        dwd.groupBy("StockCode")
        .agg(
            F.first("description").alias("description"),
            F.sum("Quantity").alias("total_quantity"),
            F.sum("amount").cast(DecimalType(14, 2)).alias("total_amount"),
            F.countDistinct("InvoiceNo").alias("order_count"),
            F.countDistinct("CustomerID").alias("customer_count"),
            F.avg("UnitPrice").alias("avg_unit_price"),
        )
        .withColumnRenamed("StockCode", "stock_code")
        .orderBy(F.desc("total_amount"))
    )

    product_df.createOrReplaceTempView("dws_product")

    count = product_df.count()
    print(f"商品数: {count:,}")
    print("\nTOP 10 销售额商品:")
    product_df.show(10, truncate=False)

    return product_df


def build_dws_daily(spark, dwd_path):
    """
    日级销售汇总
    字段：order_date, year, month, quarter, dayofweek,
          order_count, total_amount, total_quantity, product_count, customer_count
    """
    print("\n--- DWS 日级销售汇总 ---")
    dwd = spark.read.parquet(dwd_path)

    daily_df = (
        dwd.groupBy("order_date", "year", "month", "quarter", "dayofweek")
        .agg(
            F.countDistinct("InvoiceNo").alias("order_count"),
            F.sum("amount").cast(DecimalType(14, 2)).alias("total_amount"),
            F.sum("Quantity").alias("total_quantity"),
            F.countDistinct("StockCode").alias("product_count"),
            F.countDistinct("CustomerID").alias("customer_count"),
        )
        .orderBy("order_date")
    )

    daily_df.createOrReplaceTempView("dws_daily")

    count = daily_df.count()
    print(f"交易日数: {count}")
    print(f"日均销售额: {daily_df.agg(F.avg('total_amount')).collect()[0][0]}")

    return daily_df


def build_dws_country(spark, dwd_path):
    """国家级销售汇总"""
    print("\n--- DWS 国家级汇总 ---")
    dwd = spark.read.parquet(dwd_path)

    country_df = (
        dwd.groupBy("country")
        .agg(
            F.countDistinct("InvoiceNo").alias("order_count"),
            F.sum("amount").cast(DecimalType(14, 2)).alias("total_amount"),
            F.countDistinct("CustomerID").alias("customer_count"),
        )
        .orderBy(F.desc("total_amount"))
    )

    country_df.createOrReplaceTempView("dws_country")

    print(f"国家数: {country_df.count()}")
    country_df.show(10, truncate=False)

    return country_df


def save_dws(user_df, product_df, daily_df, country_df, output_dir):
    """保存 DWS 层各宽表"""
    dws_dir = os.path.join(output_dir, "dws")

    for name, df in [
        ("dws_user", user_df),
        ("dws_product", product_df),
        ("dws_daily", daily_df),
        ("dws_country", country_df),
    ]:
        path = os.path.join(dws_dir, f"{name}.parquet")
        df.write.mode("overwrite").option("compression", "snappy").parquet(path)
        print(f"已保存: {path}")


if __name__ == "__main__":
    from ods_load import create_spark_session

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dwd_path = os.path.join(project_dir, "output", "dwd", "dwd_retail_detail.parquet")
    output_dir = os.path.join(project_dir, "output")

    user_df = build_dws_user(spark, dwd_path)
    product_df = build_dws_product(spark, dwd_path)
    daily_df = build_dws_daily(spark, dwd_path)
    country_df = build_dws_country(spark, dwd_path)

    save_dws(user_df, product_df, daily_df, country_df, output_dir)

    spark.stop()
    print("\nDWS 层处理完成。")
