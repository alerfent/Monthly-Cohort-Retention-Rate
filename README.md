# Monthly Cohort Retention Rate

透過 Cohort Analysis（世代分析）方法，追蹤零售品牌顧客在首次購買後的留存與回購行為，找出留存率隨時間變化的模式，並建立一條自動化的資料工程 Pipeline 來完成整個分析流程。

**[查看互動式留存率熱力圖（Tableau Public）](https://public.tableau.com/views/cohort_hot/1?:language=zh-TW&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)**

---

## 背景與動機

我曾在餐飲零售業擔任店長與值班經理超過 6 年，第一線觀察到：每週總有固定的客人回來消費，點的品項也往往和上一次相似；相對地，也有不少客人只出現過一次就沒再回來。憑經驗判斷，熟客的回購頻率明顯高於一次性顧客，這也讓我體會到「熟客經營」對品牌長期營運的重要性——但當時始終只能憑觀察與直覺判斷，沒有系統化的方法去追蹤、驗證這個現象，也無法回答更進一步的問題：不同時期加入的客群，留存表現是否有差異？

這個專案希望補上當時缺少的那塊：透過建立完整的資料工程流程（Data Pipeline），將原始交易資料轉換為可驗證的顧客留存分析結果，用系統化的方法回答一個我過去只能憑經驗回答的問題。

## 分析問題與假設

**核心問題：** 新客在首購後的留存/回購率，如何隨著時間變化？不同時期加入的顧客世代，留存表現是否有差異？

**假設：** 越晚加入的顧客世代，留存率會低於越早加入的世代，可能反映品牌顧客黏著度隨時間下降。

**驗證結果：** 實際資料顯示的趨勢較為複雜，並非嚴格單調遞減——大致上有隨時間走低的傾向，但部分較晚期的世代仍出現局部回升（例如 2009-12 世代在 period 11 留存率達 49.53%，明顯高於其他世代同期表現）。這代表除了「加入時間」之外，可能還有其他因素（如季節性、行銷活動）影響留存表現，值得後續進一步探究。

## 資料來源

- 資料集：[Online Retail II](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)（UCI / Kaggle 公開資料集）
- 資料性質：英國一家線上禮品零售商的真實交易紀錄
- 原始資料筆數：1,067,371 筆
- 時間範圍：2009-12-01 ～ 2011-12-09（約 2 年）
- 主要欄位：

  | 欄位名稱 | 說明 |
  |---|---|
  | Invoice | 發票編號（同一張發票可能包含多筆商品） |
  | StockCode | 商品編號 |
  | Description | 商品名稱 |
  | Quantity | 購買數量 |
  | InvoiceDate | 交易日期時間 |
  | Price | 單價 |
  | Customer ID | 顧客編號（本次分析的核心欄位） |
  | Country | 顧客所在國家/地區 |

### 資料品質檢查與處理原則

1. **缺少 Customer ID**：約 22.77%（243,007 筆）的交易缺少 Customer ID。由於 cohort 分析需依賴顧客編號追蹤同一顧客的回購行為，這部分資料予以排除。
2. **退貨紀錄**：Invoice 編號以 `C` 開頭的交易代表退貨/取消交易，非真實購買行為，予以排除，避免扭曲留存率計算。
3. **非商品交易項目**：資料中混雜了人工帳務調整（Manual）、運費（Carriage）、折扣調整（Discount）、銀行手續費（Bank Charges）、網購郵資（Dotcom Postage）、系統測試資料（TEST001/TEST002）等非真實商品交易，經逐一查驗 Description 內容後排除（同時確認如 PADS、SP1002 等代碼雖格式特殊但為真實商品，予以保留）。
4. **Quantity 為負值**：約 2.1%（22,950 筆）判斷為退貨紀錄，與上述 Invoice 開頭 `C` 的排除邏輯重疊處理。

**清洗後有效分析資料：**
- 排除無 Customer ID 後：824,364 筆
- 再排除退貨與非商品項目後：810,635 筆（此為 cohort 分析實際使用的資料範圍）
- 不重複顧客數：5,942 位

## 技術架構

```
Online Retail II (CSV)
        ↓
   Python 清洗（排除無 Customer ID 交易）
        ↓
   MySQL（transactions 表）
        ↓
   SQL Views（退貨/非商品項目排除 → cohort_month 計算 → 留存率彙總）
        ↓
   Tableau（留存率熱力圖）
```

整條流程透過 **Apache Airflow**（Docker 部署）自動化串接：

| Task | 說明 |
|---|---|
| `clean_data` | 讀取原始資料，排除無 Customer ID 的交易 |
| `load_to_mysql` | 將清洗後資料寫入 MySQL，透過 Airflow Connections 管理資料庫帳密（不寫死在程式碼中） |
| `run_cohort_analysis` | 執行 SQL，建立資料清洗、cohort 月份計算、留存率彙總等 View |

- **資料處理與清洗**：Python（Pandas）、SQL
- **排程與自動化**：Apache Airflow（TaskFlow API），Docker 容器化部署
- **資料庫**：MySQL（本機開發環境）
- **視覺化**：Tableau / Tableau Public
- **版本控制**：Git / GitHub

## 分析結果

留存率呈現典型的「斷崖式下滑後趨於穩定」模式：首購當月為 100%，第二個月普遍降至 15-35%，之後大致維持在 10-20% 之間波動，沒有持續探底歸零，顯示撐過第一個月的顧客，後續留存相對穩定。

完整互動式熱力圖（可依世代、期數篩選檢視）：
**[查看 Tableau Public 熱力圖](https://public.tableau.com/views/cohort_hot/1?:language=zh-TW&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)**

## 如何執行本專案

### 前置需求
- Python 3.x（含 pandas）
- MySQL（本機安裝）
- Docker Desktop
- Tableau Desktop 或 Tableau Public Desktop（用於檢視/重新製作視覺化）

### 執行步驟

1. **資料清洗**
   ```bash
   python 01_data_cleaning.py
   ```
   讀取原始 CSV，排除無 Customer ID 的交易，輸出 `cleaned_online_retail_ii.csv`。

2. **建立資料庫與 SQL 分析邏輯**
   ```bash
   mysql -u your_user -p < 02_cohort_analysis.sql
   ```
   建立資料表結構，並依序建立資料清洗、cohort 計算、留存率彙總等 View。

3. **啟動 Airflow 自動化流程**
   ```bash
   docker compose up airflow-init
   docker compose up -d
   ```
   啟動後於瀏覽器開啟 `http://localhost:8080`，在 Admin → Connections 設定資料庫連線（Connection ID: `mysql_cohort`），即可觸發 `cohort_retention_pipeline` DAG，自動執行清洗、載入、分析三個步驟。

4. **視覺化**
   將 `cohort_retention_summary` 資料匯出或連接至 Tableau，依 `cohort_month`（Rows）、`period_number`（Columns）、`retention_rate`（Color）繪製熱力圖。

## 學到的事 / 未來可延伸方向

- **資料品質檢查不能只看表面**：一開始只發現 Invoice 開頭 `C` 的退貨紀錄，後續逐步排查才發現運費、銀行手續費、測試資料等多種非交易性質的雜訊，這個過程讓我理解資料清洗需要反覆查驗，不能只套用單一規則。
- **目前架構的限制**：MySQL 目前依賴宿主機環境（未容器化），Airflow 容器透過 `host.docker.internal` 連接本機資料庫，這代表目前的環境還不是完全可攜——換一台電腦執行，仍需另外安裝與設定 MySQL。未來可將資料庫也一併容器化（納入同一個 `docker-compose.yaml`），並將資料檔案改為程式自動下載，以提升整體環境的可重現性。
- **可延伸方向**：目前排程為手動觸發（`schedule=None`），未來可設定為定期自動執行；也可以加入資料品質的自動化測試（如 dbt tests），或嘗試以 Kafka 模擬即時資料擷取，與現有的批次處理架構做對比。