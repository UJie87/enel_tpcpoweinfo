import duckdb, pandas as pd
from pathlib import Path
import streamlit as st

SRC = Path(r"C:\Users\14507402\Desktop\TPC_powerinfo\clean.csv")
DST = Path(r"C:\Users\14507402\Desktop\tpc_app\data\clean.parquet")
DST.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(SRC, low_memory=False)

num_float_cols = ["capacity", "used", "percent"]
for c in num_float_cols:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")

# unit_id 若為整數但有缺值，轉成可空整數 Int64
if "unit_id" in df.columns:
    df["unit_id"] = pd.to_numeric(df["unit_id"], errors="coerce").astype("Int64")

# 本來就是文字/分類的欄位轉 string（避免再混型）
text_cols = ["type", "name", "gov", "status", "note", "noteId", "key", "mappingName"]
for c in text_cols:
    if c in df.columns:
        df[c] = df[c].astype("string")

# 寫 Parquet
df.to_parquet(DST, index=False)

print("success!", DST.resolve())
