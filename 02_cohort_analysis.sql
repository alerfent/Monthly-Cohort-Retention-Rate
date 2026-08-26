-- ============================================================
-- 02_cohort_analysis.sql
--
-- 專案：Monthly Cohort Retention Rate
-- 用途：建立資料表結構、清洗非交易性資料、計算月度顧客留存率
-- 資料庫：MySQL（本機開發環境）
-- 前置步驟：需先執行 01_data_cleaning.py，產出 cleaned_online_retail_ii.csv
-- ============================================================


-- ------------------------------------------------------------
-- STEP 0：建立專案專用資料庫
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS cohort_retention;
USE cohort_retention;


-- ------------------------------------------------------------
-- STEP 1：建立資料表結構
-- 欄位型別對應清洗後 CSV 的欄位；
-- Price 使用 DECIMAL 而非 FLOAT，避免金額欄位的浮點數誤差
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS transactions (
    Invoice VARCHAR(20),
    StockCode VARCHAR(20),
    Description VARCHAR(255),
    Quantity INT,
    InvoiceDate DATETIME,
    Price DECIMAL(10, 2),
    customer_id INT,
    Country VARCHAR(100)
);


-- ------------------------------------------------------------
-- STEP 2：匯入清洗後資料（範本，路徑需依實際環境調整）
--
-- 注意事項：
-- 1. 需先在 MySQL 連線設定中開啟 OPT_LOCAL_INFILE=1，
--    並執行 SET GLOBAL local_infile = 1;，否則會遇到 Error 2068
-- 2. Price 欄位透過 @price 暫存變數處理，避免千分位逗號造成解析錯誤
-- ------------------------------------------------------------
-- LOAD DATA LOCAL INFILE '你的檔案完整路徑/cleaned_online_retail_ii.csv'
-- INTO TABLE transactions
-- FIELDS TERMINATED BY ','
-- ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 ROWS
-- (Invoice, StockCode, Description, Quantity, InvoiceDate, @price, customer_id, Country)
-- SET Price = REPLACE(@price, ',', '');


-- ------------------------------------------------------------
-- STEP 3：排除非真實交易資料，建立乾淨版資料視圖
--
-- 排除項目：
-- 1. Invoice 開頭為 'C' 的紀錄 → 代表退貨/取消交易，非真實購買行為
-- 2. 非商品性質的 StockCode：
--    - M            人工調整項目 (Manual)
--    - C2           運費 (Carriage)
--    - D            折扣調整項 (Discount)
--    - ADJUST/ADJUST2  人工帳務調整
--    - BANK CHARGES 銀行手續費
--    - DOT          網購郵資 (Dotcom Postage)
--    - CRUK         Cancer Research UK 相關項目
--    - TEST001/TEST002  系統測試假資料
--
-- 注意：PADS、SP1002 等代碼雖然格式不像標準商品編號，
-- 但經查詢 Description 確認為真實商品，故不排除
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW clean_transactions AS
SELECT *
FROM transactions
WHERE Invoice NOT LIKE 'C%'
  AND StockCode NOT IN (
      'M', 'C2', 'D', 'ADJUST', 'ADJUST2',
      'BANK CHARGES', 'DOT', 'CRUK', 'TEST001', 'TEST002'
  );


-- ------------------------------------------------------------
-- STEP 4：計算每位顧客的首購月份（cohort_month）
-- 使用 DATE_FORMAT 將日期統一到「該月第一天」，
-- 只需月份粒度，不需精確到日
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW customer_cohort AS
SELECT
    customer_id,
    MIN(DATE_FORMAT(InvoiceDate, '%Y-%m-01')) AS cohort_month
FROM clean_transactions
GROUP BY customer_id;


-- ------------------------------------------------------------
-- STEP 5：計算每個世代（cohort）的首購總人數，作為留存率分母
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW cohort_size AS
SELECT
    cohort_month,
    COUNT(DISTINCT customer_id) AS total_customers
FROM customer_cohort
GROUP BY cohort_month;


-- ------------------------------------------------------------
-- STEP 6：計算留存率主表
--
-- 邏輯說明：
-- 1. 內層子查詢：依 cohort_month + period_number 分組，
--    計算每個世代在首購後第 N 個月，有多少「不重複」顧客回購
--    （PERIOD_DIFF 需要 YYYYMM 格式，故用 DATE_FORMAT 轉換）
-- 2. 外層 JOIN：將上述活躍人數，與該世代的首購總人數（cohort_size）對應，
--    計算留存率 = 活躍人數 / 首購總人數 × 100
--
-- 輸出格式為「長格式（long format）」，可直接作為 Tableau 熱力圖的資料來源：
-- cohort_month 當 Rows，period_number 當 Columns，retention_rate 當 Color
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW cohort_retention_summary AS
SELECT
    r.cohort_month,
    r.period_number,
    r.active_customers,
    s.total_customers,
    ROUND(r.active_customers / s.total_customers * 100, 2) AS retention_rate
FROM (
    SELECT
        c.cohort_month,
        PERIOD_DIFF(
            DATE_FORMAT(c1.InvoiceDate, '%Y%m'),
            DATE_FORMAT(c.cohort_month, '%Y%m')
        ) AS period_number,
        COUNT(DISTINCT c1.customer_id) AS active_customers
    FROM customer_cohort c
    JOIN clean_transactions c1 ON c.customer_id = c1.customer_id
    GROUP BY c.cohort_month, period_number
) r
JOIN cohort_size s ON r.cohort_month = s.cohort_month
ORDER BY r.cohort_month, r.period_number;


-- ------------------------------------------------------------
-- 驗證查詢（執行以下語句確認結果正確）
-- ------------------------------------------------------------
-- SELECT COUNT(*) FROM clean_transactions;                -- 預期：810,635
-- SELECT COUNT(DISTINCT customer_id) FROM clean_transactions;
-- SELECT * FROM cohort_retention_summary LIMIT 30;