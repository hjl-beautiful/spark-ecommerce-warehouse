"""
ADS 层 - 应用数据层（业务指标计算）
基于 DWS 宽表计算面向业务的应用指标：
  1. ads_rfm             RFM 用户分群（Spark MLlib KMeans）
  2. ads_monthly_trend    月度销售趋势
  3. ads_top_products     TOP20 热销商品
  4. ads_repurchase_rate  复购率分析
  5. ads_cohort_summary   同期群汇总
输出：CSV 到 output/ads/（便于业务方直接查看）
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.types import DecimalType
import os


def calc_rfm(spark, dws_user_path):
    """
    RFM 用户分群分析
    R = Recency  最近购买距今天数（越小越好）
    F = Frequency 购买频次（订单数，越大越好）
    M = Monetary  消费总额（越大越好）
    使用 Spark MLlib KMeans 分为 4 群：高价值 / 潜力 / 一般 / 流失
    """
    print("\n" + "=" * 60)
    print("ADS 层 - RFM 用户分群")
    print("=" * 60)

    user_df = spark.read.parquet(dws_user_path)

    # 构造 RFM 特征
    rfm = user_df.select(
        "customer_id",
        "recency_days",
        "total_orders",
        "total_amount",
    ).withColumnRenamed("recency_days", "R") \
     .withColumnRenamed("total_orders", "F") \
     .withColumnRenamed("total_amount", "M")

    # 特征向量化 + 标准化
    assembler = VectorAssembler(
        inputCols=["R", "F", "M"],
        outputCol="features_raw"
    )
    rfm_vec = assembler.transform(rfm)

    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    scaler_model = scaler.fit(rfm_vec)
    rfm_scaled = scaler_model.transform(rfm_vec)

    # KMeans 聚类 K=4
    kmeans = KMeans(k=4, seed=42, maxIter=100, featuresCol="features")
    model = kmeans.fit(rfm_scaled)
    rfm_result = model.transform(rfm_scaled).withColumnRenamed("prediction", "cluster")

    # 根据聚类中心定义用户分群标签
    centers = model.clusterCenters()
    print("\n聚类中心（标准化后 R/F/M 均值）:")
    for i, c in enumerate(centers):
        print(f"  Cluster {i}: R={c[0]:.2f}, F={c[1]:.2f}, M={c[2]:.2f}")

    # 计算每个簇的原始 RFM 均值，用于分群命名
    cluster_stats = rfm_result.groupBy("cluster").agg(
        F.avg("R").alias("avg_R"),
        F.avg("F").alias("avg_F"),
        F.avg("M").alias("avg_M"),
        F.count("customer_id").alias("user_count"),
    ).orderBy(F.desc("avg_M"))

    print("\n各分群 RFM 均值:")
    # 用 toPandas 替代 show
    try:
        cs_pdf = cluster_stats.toPandas()
        print(cs_pdf.to_string(index=False))
        clusters_ordered = cs_pdf.to_dict("records")
    except Exception as e:
        print(f"toPandas 失败: {e}")
        clusters_ordered = cluster_stats.collect()

    # 自动命名：按 M 值降序分配 高价值/潜力/一般/流失
    labels = ["高价值用户", "潜力用户", "一般用户", "流失用户"]
    label_map = {row["cluster"]: labels[idx] for idx, row in enumerate(clusters_ordered)}

    from pyspark.sql.functions import udf, broadcast
    from pyspark.sql.types import StringType

    # 用 join 替代 udf——udf 会触发 Python worker，本地模式下不稳定
    label_rows = [(int(c), label) for c, label in label_map.items()]
    label_df = spark.createDataFrame(label_rows, ["cluster", "user_segment"])
    rfm_final = (
        rfm_result.join(label_df, on="cluster", how="left")
        .select("customer_id", "R", "F", "M", "cluster", "user_segment")
    )

    rfm_final.createOrReplaceTempView("ads_rfm")

    # 用 toPandas 替代 groupBy+count+collect（避开 shuffle worker 崩溃）
    try:
        pdf = rfm_final.select("user_segment").toPandas()
        from collections import Counter
        cnt = Counter(pdf["user_segment"].tolist())
        print("用户分群结果:")
        for k, v in sorted(cnt.items(), key=lambda x: -x[1]):
            print(f"  {k}: {v} 人")
    except Exception as e:
        print(f"toPandas 失败，跳过分群统计: {e}")

    return rfm_final


def calc_monthly_trend(spark, dws_daily_path):
    """月度销售趋势"""
    print("\n--- ADS 月度销售趋势 ---")
    daily = spark.read.parquet(dws_daily_path)

    monthly = (
        daily.groupBy("year", "month")
        .agg(
            F.sum("order_count").alias("total_orders"),
            F.sum("total_amount").cast(DecimalType(14, 2)).alias("total_amount"),
            F.sum("total_quantity").alias("total_quantity"),
            F.sum("customer_count").alias("total_customers"),
            F.count("order_date").alias("active_days"),
        )
        .withColumn(
            "avg_daily_amount",
            (F.col("total_amount") / F.col("active_days")).cast(DecimalType(12, 2))
        )
        .orderBy("year", "month")
    )

    monthly.createOrReplaceTempView("ads_monthly_trend")
    # 用 toPandas 替代 show
    try:
        m_pdf = monthly.toPandas()
        print("月度销售趋势:")
        for _, row in m_pdf.iterrows():
            print(f"  {row['year']}-{int(row['month']):02d}: 订单 {row['total_orders']} 笔, "
                  f"GMV {row['total_amount']}, 客户数 {row['total_customers']}, "
                  f"日均 {row['avg_daily_amount']}")
    except Exception as e:
        print(f"toPandas 失败: {e}")
    return monthly


def calc_top_products(spark, dws_product_path, top_n=20):
    """TOP N 热销商品"""
    print(f"\n--- ADS TOP {top_n} 热销商品 ---")
    product = spark.read.parquet(dws_product_path)

    from pyspark.sql.window import Window
    top = (
        product
        .orderBy(F.desc("total_amount"))
        .limit(top_n)
        .withColumn("rank", F.row_number().over(
            Window.orderBy(F.desc("total_amount"))
        ))
    )

    top.createOrReplaceTempView("ads_top_products")
    # 用 toPandas 替代 collect
    try:
        t_pdf = top.toPandas()
        print(f"TOP {top_n} 热销商品:")
        for _, row in t_pdf.iterrows():
            desc = str(row['description'])[:30] if row['description'] else ""
            print(f"  #{row['rank']} {row['stock_code']} {desc:30} "
                  f"GMV={row['total_amount']} 订单数={row['order_count']} 客户数={row['customer_count']}")
    except Exception as e:
        print(f"toPandas 失败: {e}")
    return top


def calc_repurchase_rate(spark, dws_user_path):
    """复购率分析（购买2次以上为复购用户）"""
    print("\n--- ADS 复购率分析 ---")
    user = spark.read.parquet(dws_user_path)

    total_users = user.count()
    repurchase_users = user.filter(F.col("total_orders") >= 2).count()
    rate = repurchase_users / total_users * 100 if total_users > 0 else 0

    print(f"  总用户数: {total_users}")
    print(f"  复购用户数: {repurchase_users}")
    print(f"  复购率: {rate:.2f}%")

    # 用 pandas DataFrame 替代 spark.createDataFrame（避开 worker collect）
    import pandas as pd
    result_pdf = pd.DataFrame(
        [{"total_users": total_users, "repurchase_users": repurchase_users,
          "repurchase_rate_pct": float(f"{rate:.2f}")}]
    )
    # 同时构造一个等价的 Spark DataFrame 用于注册视图（不触发 collect）
    result = spark.createDataFrame(
        [(total_users, repurchase_users, float(f"{rate:.2f}"))],
        ["total_users", "repurchase_users", "repurchase_rate_pct"]
    )
    result.createOrReplaceTempView("ads_repurchase_rate")
    return result


def save_ads(rfm_df, monthly_df, top_df, repurchase_df, output_dir, spark=None):
    """保存 ADS 层为 CSV（用 toPandas 写出，避开 Spark worker 写文件崩溃）"""
    ads_dir = os.path.join(output_dir, "ads")
    os.makedirs(ads_dir, exist_ok=True)
    import pandas as pd

    # 月度趋势、TOP 商品：DF 较小，直接 toPandas
    for name, df in [
        ("ads_monthly_trend", monthly_df),
        ("ads_top_products", top_df),
    ]:
        path = os.path.join(ads_dir, f"{name}.csv")
        try:
            pdf = df.toPandas()
            pdf.to_csv(path, index=False, encoding="utf-8-sig")
            print(f"已保存: {path}  ({len(pdf)} 行)")
        except Exception as e:
            print(f"保存 {name} 失败: {e}")

    # 复购率：用 pandas 写本地（直接重新读 parquet 聚合）
    try:
        rep_path = os.path.join(ads_dir, "ads_repurchase_rate.csv")
        # 直接从 dws_user parquet 重新算
        dws_user_path = os.path.join(output_dir, "dws", "dws_user.parquet")
        user_for_rep = spark.read.parquet(dws_user_path)
        total_u = user_for_rep.count()
        rep_u = user_for_rep.filter(F.col("total_orders") >= 2).count()
        rep_rate = rep_u / total_u * 100 if total_u > 0 else 0
        rep_pdf = pd.DataFrame([{
            "total_users": total_u,
            "repurchase_users": rep_u,
            "repurchase_rate_pct": float(f"{rep_rate:.2f}")
        }])
        rep_pdf.to_csv(rep_path, index=False, encoding="utf-8-sig")
        print(f"已保存: {rep_path}  ({len(rep_pdf)} 行)")
    except Exception as e:
        print(f"保存 ads_repurchase_rate 失败: {e}")

    # RFM：用 spark.sql + pandas 读取，失败时用 parquet 写出
    rfm_path = os.path.join(ads_dir, "ads_rfm.csv")
    rfm_parquet = os.path.join(ads_dir, "ads_rfm.parquet")
    rfm_saved = False
    # 方案A：用 spark.sql 写 parquet（不需要 Python worker）
    try:
        rfm_df.write.mode("overwrite").parquet(rfm_parquet)
        print(f"已保存(Parquet): {rfm_parquet}")
        rfm_saved = True
    except Exception as e:
        print(f"ads_rfm Parquet 写出失败: {e}")
    # 方案B：尝试 CSV（toPandas 可能崩溃，崩溃也不影响主流程）
    try:
        rfm_pdf = spark.sql("SELECT customer_id, R, F, M, cluster, user_segment FROM ads_rfm").toPandas()
        rfm_pdf.to_csv(rfm_path, index=False, encoding="utf-8-sig")
        print(f"已保存: {rfm_path}  ({len(rfm_pdf)} 行)")
    except Exception as e:
        print(f"ads_rfm CSV 跳过（toPandas 不稳定）: {e}")


if __name__ == "__main__":
    from ods_load import create_spark_session

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    dws_dir = os.path.join(project_dir, "output", "dws")
    output_dir = os.path.join(project_dir, "output")

    rfm_df = calc_rfm(spark, os.path.join(dws_dir, "dws_user.parquet"))
    monthly_df = calc_monthly_trend(spark, os.path.join(dws_dir, "dws_daily.parquet"))
    top_df = calc_top_products(spark, os.path.join(dws_dir, "dws_product.parquet"))
    repurchase_df = calc_repurchase_rate(spark, os.path.join(dws_dir, "dws_user.parquet"))

    save_ads(rfm_df, monthly_df, top_df, repurchase_df, output_dir, spark=spark)

    spark.stop()
    print("\nADS 层处理完成。")
