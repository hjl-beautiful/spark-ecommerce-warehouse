"""
ODS 层 - 原始数据加载
将 UCI 电商 CSV 数据加载到 Spark，保持原始结构，注册为临时视图 ods_retail。
输出：Parquet 列式存储到 output/ods/
"""
import os
import sys

# === Windows 本地运行环境变量（必须在 import pyspark 之前设置） ===
os.environ["JAVA_HOME"] = r"C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
os.environ["HADOOP_HOME"] = r"C:\Users\何金玲\hadoop_winutils"
# 让 Spark worker 用当前 venv 的 python（避免用错 Python 版本导致 worker 崩溃）
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PATH"] = (
    os.path.join(os.environ["JAVA_HOME"], "bin")
    + os.pathsep
    + os.path.join(os.environ["HADOOP_HOME"], "bin")
    + os.pathsep
    + os.environ["PATH"]
)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session(app_name="ecommerce_warehouse"):
    """创建本地模式 SparkSession（worker 数限制为 1，避免 Windows 多 worker 崩溃）"""
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def load_ods(spark, csv_path):
    """
    读取原始 CSV，保持原始字段，不做任何清洗。
    UCI Online Retail 数据集字段：
    - InvoiceNo    订单号
    - StockCode    商品编码
    - Description  商品描述
    - Quantity     数量
    - InvoiceDate  订单日期
    - UnitPrice    单价
    - CustomerID   客户ID
    - Country      国家
    - Amount       金额(Quantity*UnitPrice)
    - Year/Month/Day/Hour/DayOfWeek  时间维度字段
    """
    print("\n" + "=" * 60)
    print("ODS 层 - 加载原始数据")
    print("=" * 60)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
    )

    # 注册临时视图
    df.createOrReplaceTempView("ods_retail")

    # 基本统计
    total_rows = df.count()
    print(f"原始数据行数: {total_rows:,}")
    print(f"字段数: {len(df.columns)}")
    print(f"字段列表: {df.columns}")

    # 按年月分布
    print("\n各月数据量:")
    df.groupBy("Year", "Month").count().orderBy("Year", "Month").show()

    return df


def save_ods(df, output_dir):
    """保存 ODS 层为 Parquet 列式存储"""
    ods_path = os.path.join(output_dir, "ods", "ods_retail.parquet")
    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(ods_path)
    )
    print(f"ODS 层已保存: {ods_path}")


if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 数据路径（默认从项目 data 目录读取）
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    csv_path = os.path.join(project_dir, "data", "OnlineRetail_cleaned.csv")
    output_dir = os.path.join(project_dir, "output")

    if not os.path.exists(csv_path):
        print(f"错误: 数据文件不存在: {csv_path}")
        print("请将 UCI 电商数据 CSV 放到 data/ 目录下")
        spark.stop()
        exit(1)

    df = load_ods(spark, csv_path)
    save_ods(df, output_dir)

    spark.stop()
    print("\nODS 层处理完成。")
