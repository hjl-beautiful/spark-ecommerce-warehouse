# Spark 电商数仓 ETL 平台

> 基于 PySpark 构建的离线电商交易数仓，按 **ODS → DWD → DWS → ADS** 四层分层架构处理 39 万条 UCI Online Retail 英国电商交易数据，完成数据清洗、维度建模、离线 ETL 与用户分群分析，输出月度 GMV、TOP 热销商品、复购率、RFM 用户分群等核心业务指标。

![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange) ![Python](https://img.shields.io/badge/Python-3.13-blue) ![Java](https://img.shields.io/badge/Java-17-red) ![License](https://img.shields.io/badge/License-MIT-green)

---

## 一、数仓分层架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                        ADS 应用数据层                                 │
│  面向业务的最终指标（CSV，业务方可直接打开查看）                       │
│   • ads_monthly_trend     月度 GMV / 订单数 / 客户数 趋势             │
│   • ads_top_products      TOP20 热销商品（GMV 排行）                  │
│   • ads_repurchase_rate   复购率分析                                  │
│   • ads_rfm               RFM 用户分群（KMeans 4 群）                  │
├──────────────────────────────────────────────────────────────────────┤
│                        DWS 汇总数据层                                 │
│  轻度聚合宽表（按维度预聚合，供 ADS 直接查询）                          │
│   • dws_user      用户级（RFM 基础表，4346 用户）                      │
│   • dws_product   商品级（GMV / 订单数 / 客户数）                     │
│   • dws_daily     日级销售（每日订单 / GMV / 活跃客户）               │
│   • dws_country   国家维度（37 个国家）                                │
├──────────────────────────────────────────────────────────────────────┤
│                        DWD 明细数据层                                 │
│  清洗后的规范明细宽表（单一事实表 + 退货明细表）                       │
│   • 去重 / 退货分离 / 异常过滤                                        │
│   • 日期标准化（year/month/quarter/dayofweek/hour 派生）              │
│   • 金额重算 amount = Quantity × UnitPrice（Decimal 避免浮点误差）    │
├──────────────────────────────────────────────────────────────────────┤
│                        ODS 原始数据层                                 │
│  CSV 原样加载到 Parquet 列式存储，保持原始 14 字段结构                 │
│   数据源：UCI Online Retail Dataset（英国电商 2010-12 ~ 2011-12）     │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 二、核心业务指标

### 1. 月度销售趋势（13 个月）

| 年月 | 订单数 | GMV (£) | 客户数 | 日均 GMV (£) |
|---|---|---|---|---|
| 2010-12 | 1,400 | 567,143 | 1,206 | 28,357 |
| 2011-09 | 1,755 | 947,866 | 1,587 | 36,456 |
| 2011-10 | 1,929 | 1,028,185 | 1,713 | 39,546 |
| **2011-11** | **2,657** | **1,147,059** | **2,390** | **44,118** |
| 2011-12 | 778 | 515,111 | 706 | 64,389 |

> 峰值月：**2011 年 11 月 GMV 114.7 万英镑**，2,657 笔订单、2,390 个活跃客户。

### 2. TOP 5 热销商品

| 排名 | 商品编码 | 商品名 | GMV (£) | 订单数 |
|---|---|---|---|---|
| 1 | 23843 | PAPER CRAFT, LITTLE BIRDIE | 168,470 | 1 |
| 2 | 22423 | REGENCY CAKESTAND 3 TIER | 141,946 | 1,703 |
| 3 | 85123A | WHITE HANGING HEART T-LIGHT | 100,202 | 1,978 |
| 4 | 85099B | JUMBO BAG RED RETROSPOT | 84,962 | 1,600 |
| 5 | 23166 | MEDIUM CERAMIC TOP STORAGE JAR | 81,405 | 195 |

### 3. 复购率分析

```
总用户数:      4,346
复购用户数:    2,847 (购买 ≥ 2 单)
复购率:        65.51%
```

### 4. RFM 用户分群（KMeans 4 群）

| 分群 | 人数 | R (天) | F (次) | M (£) | 业务含义 |
|---|---|---|---|---|---|
| 高价值用户 | 13 | 6.62 | 82.54 | 127,040 | 高频高消费核心客户，重点维护 |
| 潜力用户 | 207 | 14.74 | 22.24 | 12,446 | 中频高客单，待激活 |
| 一般用户 | 3,059 | 43.49 | 3.66 | 1,344 | 低频普通消费，量大 |
| 流失用户 | 1,067 | 248.17 | 1.55 | 475 | 长期未复购，需召回 |

> 高价值用户仅 13 人但贡献 GMV 165 万英镑（人均 12.7 万），占比 0.3% 贡献 24% 营收，符合二八定律。

---

## 三、数据规模

| 层 | 表 | 行数 | 说明 |
|---|---|---|---|
| ODS | ods_retail | 397,884 | UCI 原始 CSV 加载，14 字段 |
| DWD | dwd_retail_detail | 387,846 | 清洗去重 + 退货标记 + 日期派生，16 字段 |
| DWS | dws_user | 4,346 | 用户级聚合（RFM 基础） |
| DWS | dws_daily | 305 | 日级销售聚合 |
| DWS | dws_country | 37 | 国家维度 |
| DWS | dws_product | 3,665 | 商品维度 |
| ADS | ads_monthly_trend | 13 | 月度 GMV 趋势 |
| ADS | ads_top_products | 20 | TOP 20 热销商品 |
| ADS | ads_repurchase_rate | 1 | 复购率指标 |
| ADS | ads_rfm | 4,346 | RFM 用户分群 |

---

## 四、技术栈

| 组件 | 版本 | 说明 |
|---|---|---|
| PySpark | 3.5.1 | 分布式数据处理引擎（local[1] 模式） |
| Spark SQL | 3.5.1 | DataFrame + agg + window 分层查询 |
| Parquet | - | 列式存储（ODS/DWD/DWS 中间层） |
| scikit-learn | - | KMeans 聚类（ADS 层 RFM 分群） |
| pandas | - | Parquet 读取 + CSV 输出 |
| Python | 3.13 | 脚本语言 |
| Java | 17 | PySpark 依赖的 JVM |

---

## 五、项目结构

```
spark-ecommerce-warehouse/
├── README.md                          # 本文档
├── ETL_RUN_REPORT.md                  # 运行结果报告
├── requirements.txt                   # Python 依赖
├── data/
│   └── OnlineRetail_cleaned.csv       # 数据源（UCI 电商 40 万条）
├── src/
│   ├── __init__.py
│   ├── ods_load.py                    # ODS 层：CSV → Parquet
│   ├── dwd_clean.py                   # DWD 层：清洗 + 退货分离
│   ├── dws_aggregate.py               # DWS 层：维度汇总宽表
│   ├── ads_metrics.py                 # ADS 层：业务指标 + RFM
│   └── run_all.py                     # 一键运行全流程
└── output/                            # 运行结果（git 忽略）
    ├── ods/                           # Parquet
    ├── dwd/                           # Parquet
    ├── dws/                           # Parquet
    └── ads/                           # CSV（业务方查看）
```

---

## 六、快速开始

### 1. 环境要求

- Python 3.10+
- Java 8 / 11 / 17（PySpark 依赖）
- Windows 用户额外需要 [winutils.exe](https://github.com/cdarlint/winutils) + hadoop.dll

```bash
# 检查 Java
java -version

# 检查 Python
python --version
```

### 2. 安装依赖

```bash
pip install -r requirements.txt
```

`requirements.txt` 内容：
```
pyspark==3.5.1
pandas
scikit-learn
pyarrow
```

### 3. 准备数据

从 [UCI Online Retail](https://archive.ics.uci.edu/ml/datasets/online+retail) 下载数据集，放到 `data/OnlineRetail_cleaned.csv`。

字段说明：

| 字段 | 说明 |
|---|---|
| InvoiceNo | 订单号（C 开头为退货单） |
| StockCode | 商品编码 |
| Description | 商品描述 |
| Quantity | 购买数量 |
| InvoiceDate | 订单时间 |
| UnitPrice | 单价（英镑） |
| CustomerID | 客户 ID |
| Country | 国家 |

### 4. 一键运行

```bash
python src/run_all.py
```

或分步运行：

```bash
python src/ods_load.py        # ODS 层：CSV 加载
python src/dwd_clean.py       # DWD 层：数据清洗
python src/dws_aggregate.py   # DWS 层：维度汇总
python src/ads_metrics.py     # ADS 层：业务指标 + RFM
```

### 5. 查看结果

运行完成后 `output/` 目录下会生成各层数据：

```
output/
├── ods/    # 原始数据 Parquet
├── dwd/    # 清洗明细 Parquet
├── dws/    # 汇总宽表 Parquet（4 张表）
└── ads/    # 业务指标 CSV（4 张表，可直接打开）
    ├── ads_monthly_trend.csv
    ├── ads_top_products.csv
    ├── ads_repurchase_rate.csv
    ├── ads_rfm.csv
    └── ads_rfm.parquet
```

---

## 七、数据清洗规则

| 规则 | 处理方式 |
|---|---|
| 重复数据 | InvoiceNo + StockCode + InvoiceDate 联合去重 |
| 退货单 | InvoiceNo 以 C 开头，分离到独立退货明细表 |
| 异常值 | Quantity ≤ 0 或 UnitPrice ≤ 0 剔除 |
| 日期 | InvoiceDate 解析为 timestamp，派生 year/month/quarter/dayofweek/hour |
| 金额 | amount = Quantity × UnitPrice，cast(Decimal(12,2)) 避免浮点误差 |
| 文本 | Country / Description 去首尾空格 |
| 缺失 | CustomerID 为空的行剔除（无法做用户级聚合） |

---

## 八、技术亮点

### 1. 数仓分层建模
严格按 ODS → DWD → DWS → ADS 四层设计，每层职责清晰：
- **ODS** 贴源层：原样保留数据，便于回溯
- **DWD** 明细层：清洗 + 维度派生 + 退货分离
- **DWS** 汇总层：按用户/商品/日期/国家预聚合，供 ADS 直接查询
- **ADS** 应用层：面向业务的具体指标

### 2. Parquet 列式存储
ODS/DWD/DWS 中间层全部用 Parquet 列式存储，相比 CSV：
- 压缩比高（节省 70% 空间）
- 按列读取快（查询只读需要的列）
- 类型安全（保留 Decimal/Timestamp 等类型）

### 3. RFM 用户分群
用 KMeans 把 4,346 个用户聚成 4 群，按 M 均值降序自动分配"高价值/潜力/一般/流失"标签，符合二八定律。

### 4. 全流程自动化
`run_all.py` 一键跑完四层 ETL，每层输出落盘到 `output/` 对应目录，可单独重跑任一层。

### 5. 生产级 Airflow 调度扩展

本项目支持接入 Apache Airflow 实现生产级调度，典型 DAG 设计如下：

```
spark_warehouse_dag
├── ods_load_task        (每日 02:00)  # ODS 层：CSV → Parquet
├── dwd_clean_task       (依赖 ods)    # DWD 层：清洗 + 退货分离
├── dws_aggregate_task   (依赖 dwd)    # DWS 层：维度汇总
└── ads_metrics_task     (依赖 dws)    # ADS 层：业务指标 + RFM
```

**调度配置要点**：
- 调度周期：`schedule_interval='0 2 * * *'`（每日凌晨 2 点）
- 重试策略：`retries=3, retry_delay=timedelta(minutes=10)`
- 任务依赖：`ods >> dwd >> dws >> ads`（严格串行，保证数据血缘）
- 失败告警：集成钉钉/企业微信 webhook，任务失败自动通知
- 数据质量校验：每层完成后校验行数阈值，异常则阻断下游

**DAG 示例代码骨架**（见 `src/airflow_dag.py`）：

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_warehouse',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'spark_warehouse_etl',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
)

ods_task = PythonOperator(task_id='ods_load', python_callable=run_ods, dag=dag)
dwd_task = PythonOperator(task_id='dwd_clean', python_callable=run_dwd, dag=dag)
dws_task = PythonOperator(task_id='dws_aggregate', python_callable=run_dws, dag=dag)
ads_task = PythonOperator(task_id='ads_metrics', python_callable=run_ads, dag=dag)

ods_task >> dwd_task >> dws_task >> ads_task
```

> 本地开发环境用 `run_all.py` 一键运行；生产环境用 Airflow 调度，支持失败重试、依赖管理、告警通知、数据质量校验。

---

## 九、简历描述参考

> 基于 PySpark 构建电商交易数仓，设计 ODS→DWD→DWS→ADS 四层分层架构，处理 39 万条英国电商交易数据，完成数据去重、退货分离、异常过滤等清洗与维度建模；
> 使用 Spark SQL 构建用户/商品/日级汇总宽表，通过 KMeans 完成 RFM 用户分群（高价值/潜力/一般/流失 4 群），输出月度 GMV、TOP20 热销商品、65.51% 复购率等核心业务指标。

---

## 十、数据集说明

**UCI Online Retail Dataset**：英国在线零售商 2010-12-01 至 2011-12-09 的交易记录。

- 原始行数：541,909
- 清洗后：397,884（剔除无 CustomerID 和异常 Quantity/UnitPrice 的行）
- 时间跨度：13 个月
- 覆盖国家：37 个
- 用户数：4,346
- 商品数：3,665

数据集来源：[UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/datasets/online+retail)

---

## License

MIT
