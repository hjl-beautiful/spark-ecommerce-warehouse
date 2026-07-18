"""
一键运行全流程：ODS → DWD → DWS → ADS
用法:
    python src/run_all.py [--data-path PATH]

如果未指定 --data-path，默认从项目 data/ 目录读取 OnlineRetail_cleaned.csv
"""
import os
import sys
import time

# 将 src 目录加入 path，方便 import 同级模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ods_load import create_spark_session, load_ods, save_ods
from dwd_clean import clean_dwd, save_dwd
from dws_aggregate import (
    build_dws_user, build_dws_product,
    build_dws_daily, build_dws_country, save_dws
)
from ads_metrics import (
    calc_rfm, calc_monthly_trend, calc_top_products,
    calc_repurchase_rate, save_ads
)


def main():
    # 解析参数
    csv_path = None
    if "--data-path" in sys.argv:
        idx = sys.argv.index("--data-path")
        csv_path = sys.argv[idx + 1]

    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if csv_path is None:
        csv_path = os.path.join(project_dir, "data", "OnlineRetail_cleaned.csv")
    output_dir = os.path.join(project_dir, "output")

    if not os.path.exists(csv_path):
        print(f"错误: 数据文件不存在: {csv_path}")
        print("请将 UCI 电商数据 CSV 放到 data/ 目录下，或用 --data-path 指定路径")
        sys.exit(1)

    print("=" * 60)
    print("  电商离线数仓 ETL 全流程")
    print("  ODS → DWD → DWS → ADS 四层架构")
    print("=" * 60)
    print(f"数据源: {csv_path}")
    print(f"输出目录: {output_dir}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    total_start = time.time()

    # ========== ODS 层 ==========
    t = time.time()
    ods_df = load_ods(spark, csv_path)
    save_ods(ods_df, output_dir)
    print(f"[ODS] 耗时 {time.time() - t:.1f}s")

    # ========== DWD 层 ==========
    t = time.time()
    ods_path = os.path.join(output_dir, "ods", "ods_retail.parquet")
    dwd_df, return_df = clean_dwd(spark, ods_path)
    save_dwd(dwd_df, return_df, output_dir)
    print(f"[DWD] 耗时 {time.time() - t:.1f}s")

    # ========== DWS 层 ==========
    t = time.time()
    dwd_path = os.path.join(output_dir, "dwd", "dwd_retail_detail.parquet")
    user_df = build_dws_user(spark, dwd_path)
    product_df = build_dws_product(spark, dwd_path)
    daily_df = build_dws_daily(spark, dwd_path)
    country_df = build_dws_country(spark, dwd_path)
    save_dws(user_df, product_df, daily_df, country_df, output_dir)
    print(f"[DWS] 耗时 {time.time() - t:.1f}s")

    # ========== ADS 层 ==========
    t = time.time()
    dws_dir = os.path.join(output_dir, "dws")
    rfm_df = calc_rfm(spark, os.path.join(dws_dir, "dws_user.parquet"))
    monthly_df = calc_monthly_trend(spark, os.path.join(dws_dir, "dws_daily.parquet"))
    top_df = calc_top_products(spark, os.path.join(dws_dir, "dws_product.parquet"))
    repurchase_df = calc_repurchase_rate(spark, os.path.join(dws_dir, "dws_user.parquet"))
    save_ads(rfm_df, monthly_df, top_df, repurchase_df, output_dir, spark=spark)
    print(f"[ADS] 耗时 {time.time() - t:.1f}s")

    # ========== 汇总 ==========
    print("\n" + "=" * 60)
    print("  全流程完成!")
    print("=" * 60)
    print(f"总耗时: {time.time() - total_start:.1f}s")
    print(f"结果文件: {output_dir}")
    print("\n目录结构:")
    for layer in ["ods", "dwd", "dws", "ads"]:
        layer_dir = os.path.join(output_dir, layer)
        if os.path.exists(layer_dir):
            files = os.listdir(layer_dir)
            print(f"  {layer.upper()}/")
            for f in files:
                print(f"    └── {f}")

    spark.stop()


if __name__ == "__main__":
    main()
