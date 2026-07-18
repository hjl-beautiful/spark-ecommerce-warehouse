# Spark 电商数仓 ETL 项目 — 运行结果报告

## 项目概述
基于 PySpark 构建 UCI Online Retail 电商交易数仓，按 ODS→DWD→DWS→ADS 四层分层建模，处理 397,884 条原始交易数据，覆盖 37 个国家、4,346 个用户、3,665 个商品。

## 数据规模
| 层 | 表 | 行数 | 说明 |
|---|---|---|---|
| ODS | ods_retail | 397,884 | UCI 原始 CSV 加载，14 字段 |
| DWD | dwd_retail_detail | 387,846 | 清洗去重、退货标记、日期派生，16 字段 |
| DWS | dws_user | 4,346 | 用户级聚合（RFM 维度） |
| DWS | dws_daily | 305 | 日级销售聚合 |
| DWS | dws_country | 37 | 国家维度 |
| DWS | dws_product | 3,665 | 商品维度 |
| ADS | ads_monthly_trend | 13 | 月度 GMV 趋势 |
| ADS | ads_top_products | 20 | TOP 20 热销商品 |
| ADS | ads_repurchase_rate | 1 | 复购率指标 |
| ADS | ads_rfm | 4,346 | RFM 用户分群（4 簇 KMeans） |

## 核心业务指标

### 1. 月度销售趋势（2010-12 ~ 2011-12）
- 月均 GMV：约 £680K
- 峰值月：2011-11，GMV £1,147,058，2,657 笔订单，2,390 客户
- 12 月仅 8 天数据（截至 12-09）但日均 GMV 高达 £64,388

### 2. TOP 5 热销商品
| 排名 | 商品编码 | 商品名 | GMV(£) | 订单数 |
|---|---|---|---|---|
| 1 | 23843 | PAPER CRAFT, LITTLE BIRDIE | 168,469 | 1 |
| 2 | 22423 | REGENCY CAKESTAND 3 TIER | 141,946 | 1,703 |
| 3 | 85123A | WHITE HANGING HEART T-LIGHT | 100,202 | 1,978 |
| 4 | 85099B | JUMBO BAG RED RETROSPOT | 84,962 | 1,600 |
| 5 | 23166 | MEDIUM CERAMIC TOP STORAGE JAR | 81,405 | 195 |

### 3. 复购率分析
- 总用户数：4,346
- 复购用户数（≥2 单）：2,847
- **复购率：65.51%**

### 4. RFM 用户分群（KMeans 4 簇）
| 分群 | 人数 | R(天) | F(次) | M(£) | 含义 |
|---|---|---|---|---|---|
| 高价值用户 | 13 | 6.62 | 82.54 | 127,039.55 | 高频高消费核心客户 |
| 潜力用户 | 207 | 14.74 | 22.24 | 12,445.72 | 中频高客单，待激活 |
| 一般用户 | 3,059 | 43.49 | 3.66 | 1,343.76 | 低频普通消费 |
| 流失用户 | 1,067 | 248.17 | 1.55 | 474.67 | 长期未复购，需召回 |

## 技术栈
- PySpark 3.5.1（local[1] 模式）
- Spark SQL（DataFrame + agg + window）
- Parquet 列式存储
- sklearn KMeans（ADS 层 RFM 聚类）
- Java 17、Python 3.13.12

## 输出目录
- `output/ods/` — ODS 层 Parquet
- `output/dwd/` — DWD 层 Parquet
- `output/dws/` — DWS 层 Parquet（4 表）
- `output/ads/` — ADS 层 CSV + Parquet（4 表）

## 简历可用的两行描述
> 基于 PySpark 构建电商交易数仓，按 ODS→DWD→DWS→ADS 四层分层建模，处理 39 万条交易数据，覆盖 37 国 4,346 用户；
> 实现 ETL 全流程自动化，输出月度 GMV 趋势、TOP20 热销商品、65.51% 复购率及 RFM 用户分群（KMeans 4 簇）等核心业务指标。
