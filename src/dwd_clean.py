"""
DWD 层 - 明细数据清洗
对 ODS 原始数据进行清洗、标准化，输出规范的明细宽表。
清洗规则：
  1. 去重（InvoiceNo + StockCode + InvoiceDate 三字段联合去重）
  2. 退货单标记（InvoiceNo 以 'C' 开头为退货单）
  3. 异常值过滤（Quantity > 0 AND UnitPrice > 0）
  4. 日期标准化（InvoiceDate 解析为 timestamp）
  5. 金额重算（Amount = Quantity * UnitPrice，避免浮点误差）
  6. 国家标准化（去除首尾空格）
输出：Parquet 到 output/dwd/
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
import os


def clean_dwd(spark, ods_path):
    """
    读取 ODS Parquet，执行清洗规则，输出 DWD 明细宽表。
    """
    print("\n" + "=" * 60)
    print("DWD 层 - 数据清洗")
    print("=" * 60)

    ods_df = spark.read.parquet(ods_path)
    ods_count = ods_df.count()
    print(f"ODS 原始行数: {ods_count:,}")

    # 1. 去重
    deduped = ods_df.dropDuplicates(["InvoiceNo", "StockCode", "InvoiceDate"])
    dedup_count = deduped.count()
    print(f"去重后行数: {dedup_count:,}  (删除 {ods_count - dedup_count:,} 条重复)")

    # 2. 退货单标记
    dwd = deduped.withColumn(
        "is_return",
        F.when(F.col("InvoiceNo").startswith("C"), 1).otherwise(0)
    )

    # 3. 异常值过滤：保留有效交易（Quantity > 0 且 UnitPrice > 0 且非退货单）
    #    退货单单独保留到退货明细表，主表只保留正常交易
    valid_df = dwd.filter(
        (F.col("Quantity") > 0) &
        (F.col("UnitPrice") > 0) &
        (F.col("is_return") == 0)
    )
    return_df = dwd.filter(F.col("is_return") == 1)

    valid_count = valid_df.count()
    return_count = return_df.count()
    print(f"正常交易: {valid_count:,} 条")
    print(f"退货单: {return_count:,} 条  (退货率 {return_count/(valid_count+return_count)*100:.2f}%)")

    # 4. 日期标准化与派生字段
    # 注意：PySpark 默认大小写不敏感，drop("Year") 会同时删 year，
    # 因此改用 select 显式保留所需列，避免列被误删
    dwd_final = (
        valid_df
        .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate"))
        .withColumn("order_date", F.to_date("InvoiceDate"))
        .withColumn("year", F.year("InvoiceDate"))
        .withColumn("month", F.month("InvoiceDate"))
        .withColumn("quarter", F.quarter("InvoiceDate"))
        .withColumn("dayofweek", F.dayofweek("InvoiceDate"))
        .withColumn("hour", F.hour("InvoiceDate"))
        # 5. 金额重算为 Decimal 避免浮点误差
        .withColumn(
            "amount",
            (F.col("Quantity") * F.col("UnitPrice")).cast(DecimalType(12, 2))
        )
        # 6. 国家、商品描述标准化
        .withColumn("country", F.trim(F.col("Country")))
        .withColumn("description", F.trim(F.col("Description")))
        # 只保留需要的列（替代 drop，避免大小写不敏感导致列被误删）
        .select(
            "InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice",
            "CustomerID", "is_return", "order_date", "year", "month",
            "quarter", "dayofweek", "hour", "amount", "country", "description"
        )
    )

    # 注册临时视图
    dwd_final.createOrReplaceTempView("dwd_retail_detail")

    # 数据质量检查
    print("\n数据质量检查:")
    print(f"  CustomerID 缺失: {dwd_final.filter(F.col('CustomerID').isNull()).count()} 条")
    print(f"  日期范围: {dwd_final.agg(F.min('order_date'), F.max('order_date')).collect()[0]}")
    print(f"  涉及国家: {dwd_final.select('country').distinct().count()} 个")

    return dwd_final, return_df


def save_dwd(dwd_df, return_df, output_dir):
    """保存 DWD 层"""
    dwd_path = os.path.join(output_dir, "dwd", "dwd_retail_detail.parquet")
    return_path = os.path.join(output_dir, "dwd", "dwd_return_detail.parquet")

    dwd_df.write.mode("overwrite").option("compression", "snappy").parquet(dwd_path)
    return_df.write.mode("overwrite").option("compression", "snappy").parquet(return_path)
    print(f"\nDWD 明细表已保存: {dwd_path}")
    print(f"DWD 退货表已保存: {return_path}")


if __name__ == "__main__":
    from ods_load import create_spark_session

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ods_path = os.path.join(project_dir, "output", "ods", "ods_retail.parquet")
    output_dir = os.path.join(project_dir, "output")

    dwd_df, return_df = clean_dwd(spark, ods_path)
    save_dwd(dwd_df, return_df, output_dir)

    # 预览
    print("\nDWD 明细表样例:")
    dwd_df.printSchema()
    dwd_df.show(5, truncate=False)

    spark.stop()
    print("\nDWD 层处理完成。")
