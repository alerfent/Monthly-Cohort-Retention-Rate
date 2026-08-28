"""
01_data_cleaning.py

專案：Monthly Cohort Retention Rate
用途：讀取 Online Retail II 原始資料，進行初步資料品質檢查，
      並排除無 Customer ID 的交易紀錄（cohort 分析必須依賴顧客編號追蹤回購行為）。

輸入：原始 CSV 檔案（Online Retail II）
輸出：清洗後的 DataFrame（df_clean），可另存為新的 CSV 供後續步驟使用
"""

import pandas as pd

# ---------- 1. 讀取原始資料 ----------
RAW_DATA_PATH = "online_retail_II.csv" 

df = pd.read_csv(RAW_DATA_PATH, encoding="ISO-8859-1") 
#因為讀取的檔案是英歐國家,透過ISO-8859-1可以將所有 0 至 255 的位元值都映射到某個字元上,能確保 Pandas 順利將資料讀入，不會因位元解析失敗而崩潰。

print("=" * 50)
print("【原始資料檢查】")
print("=" * 50)
print("資料筆數與欄位數：", df.shape)
print("時間範圍：", df["InvoiceDate"].min(), "~", df["InvoiceDate"].max())
print("\n各欄位缺值數量：")
print(df.isnull().sum())

negative_qty_count = df[df["Quantity"] < 0].shape[0]
missing_customer_id_count = df["Customer ID"].isnull().sum()

print("\nQuantity 為負值的筆數（推測為退貨）：", negative_qty_count)
print("缺少 Customer ID 的筆數：", missing_customer_id_count)
print(
    "缺少 Customer ID 的比例：",
    round(missing_customer_id_count / df.shape[0] * 100, 2),
    "%",
)

# ---------- 2. 排除無 Customer ID 的交易 ----------
# 理由：cohort 分析需要追蹤同一位顧客的行為，沒有 Customer ID 的交易
#      無法判斷是否為同一顧客，因此排除，不納入本次分析範圍。
df_clean = df[df["Customer ID"].notnull()].copy()

print("\n" + "=" * 50)
print("【排除無 Customer ID 交易後】")
print("=" * 50)
print("排除前總筆數：", df.shape[0])
print("排除後剩餘筆數：", df_clean.shape[0])
print(
    "排除比例：",
    round((df.shape[0] - df_clean.shape[0]) / df.shape[0] * 100, 2),
    "%",
)
print("剩餘不重複顧客數：", df_clean["Customer ID"].nunique())
print(
    "清洗後時間範圍：",
    df_clean["InvoiceDate"].min(),
    "~",
    df_clean["InvoiceDate"].max(),
)

# ---------- 3. 輸出清洗後資料（供後續步驟使用） ----------
# 注意：輸出的 CSV 檔案不建議 commit 進 Git（已在 .gitignore 中排除 *.csv）
OUTPUT_PATH = "cleaned_online_retail_ii.csv"
df_clean.to_csv(OUTPUT_PATH, index=False)
print(f"\n清洗後資料已輸出至：{OUTPUT_PATH}")