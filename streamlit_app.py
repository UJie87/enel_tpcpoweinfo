import io
from datetime import date, time, datetime
from pathlib import Path
import pyarrow
import pandas as pd
import streamlit as st
import requests

st.set_page_config(page_title = "TPC power info", layout= "wide")

st.title("TPC power information: EnelX internal use")
st.write("test")


# ===path setting ===
PARQUET_PATH = Path("data/clean.parquet")
assert PARQUET_PATH.exists(), f"can't find the file: {PARQUET_PATH.resolve()}"

# === 載入資料（快取） ===
@st.cache_data(show_spinner=True)
def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    # 標準化欄位：確保有 time/type/name/capacity/used
    # time 轉成 datetime（若已是 datetime64 則不會重複轉）
    if not pd.api.types.is_datetime64_any_dtype(df.get("time")):
        df["time"] = pd.to_datetime(df["time"], errors="coerce", infer_datetime_format=True)
    df = df.dropna(subset=["time"]).copy()

    # 常見欄位型別統一
    if "capacity" in df.columns:
        df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
    if "used" in df.columns:
        df["used"] = pd.to_numeric(df["used"], errors="coerce")

    # 派生欄位方便篩選
    df["date"] = df["time"].dt.date
    df["tod"] = df["time"].dt.time  # time-of-day
    return df

df = load_data(PARQUET_PATH)

# === 側邊欄：篩選條件 ===
with st.sidebar:
    st.header("篩選條件")

    # 日期範圍
    min_d, max_d = df["date"].min(), df["date"].max()
    d1, d2 = st.date_input(
        "日期範圍",
        value=(min_d, max_d),
        min_value=min_d,
        max_value=max_d,
        format="YYYY-MM-DD",
    )
    if isinstance(d1, date) and isinstance(d2, date) and d1 > d2:
        st.error("開始日期不可大於結束日期")
        st.stop()

    # 時間區間（同日內），預設整天
    c1, c2 = st.columns(2)
    with c1:
        t_start = st.time_input("開始時間", time(0, 0))
    with c2:
        t_end = st.time_input("結束時間", time(23, 59))
    if t_start > t_end:
        st.error("開始時間不可大於結束時間")
        st.stop()

    # 技術別
    all_types = sorted([x for x in df["type"].dropna().unique()])
    picked_types = st.multiselect("技術別（type）", all_types, default=all_types[:1] if all_types else [])
    if not picked_types:
        st.warning("請至少選擇一個技術別")
        st.stop()

    # 當只選一個技術別時，提供案場選擇
    picked_names = []
    if len(picked_types) == 1:
        sub = df[df["type"] == picked_types[0]]
        all_names = sorted([x for x in sub["name"].dropna().unique()])
        picked_names = st.multiselect("案場（name，可選）", all_names, default=[])

    # 下載格式
    dl_fmt = st.radio("下載格式", ["CSV", "Parquet"], horizontal=True)

# === 依篩選條件過濾資料 ===
mask_date = (df["date"] >= d1) & (df["date"] <= d2)
mask_time = (df["tod"] >= t_start) & (df["tod"] <= t_end)
mask_type = df["type"].isin(picked_types)

mask = mask_date & mask_time & mask_type
df_filt = df.loc[mask].copy()

# 若只選 1 個 type 且指定了案場：只篩選那些案場
if len(picked_types) == 1 and picked_names:
    df_filt = df_filt[df_filt["name"].isin(picked_names)]

# === 聚合規則 ===
# 「技術別需要不同案場相加」：以時間為 Key，把 capacity/used 相加
agg_cols = ["capacity", "used"]
df_agg = (
    df_filt.groupby("time", as_index=False)[agg_cols]
    .sum(min_count=1)  # 若全是 NaN，結果保持 NaN
    .sort_values("time")
)

# === 視覺化 ===
st.title("TPC 發電資訊視覺化")

# 篩選摘要
meta = []
meta.append(f"日期：{d1} → {d2}")
meta.append(f"時間：{t_start.strftime('%H:%M')} → {t_end.strftime('%H:%M')}")
meta.append(f"type：{', '.join(picked_types)}")
if len(picked_types) == 1 and picked_names:
    meta.append(f"name：{', '.join(picked_names)}")
st.caption("；".join(meta))

# 折線圖（兩條線）
if df_agg.empty:
    st.warning("目前篩選沒有資料。請調整條件。")
else:
    show_cols = ["capacity", "used"]
    chart_df = df_agg.set_index("time")[show_cols]
    st.line_chart(chart_df, height=360)
    st.dataframe(df_filt.sort_values("time"), use_container_width=True, height=300)

# === 下載區 ===
st.subheader("下載資料")

colA, colB = st.columns(2)

# 下載：篩選後原始資料
with colA:
    if dl_fmt == "CSV":
        buf = io.StringIO()
        df_filt.to_csv(buf, index=False)
        st.download_button(
            "下載：篩選後原始資料（CSV）",
            data=buf.getvalue().encode("utf-8"),
            file_name="filtered_raw.csv",
            mime="text/csv",
        )
    else:
        buf = io.BytesIO()
        df_filt.to_parquet(buf, index=False)
        st.download_button(
            "下載：篩選後原始資料（Parquet）",
            data=buf.getvalue(),
            file_name="filtered_raw.parquet",
            mime="application/octet-stream",
        )

# 下載：依時間聚合資料（capacity/used 相加）
with colB:
    if dl_fmt == "CSV":
        buf2 = io.StringIO()
        df_agg.to_csv(buf2, index=False)
        st.download_button(
            "下載：依時間聚合資料（CSV）",
            data=buf2.getvalue().encode("utf-8"),
            file_name="aggregated_by_time.csv",
            mime="text/csv",
        )
    else:
        buf2 = io.BytesIO()
        df_agg.to_parquet(buf2, index=False)
        st.download_button(
            "下載：依時間聚合資料（Parquet）",
            data=buf2.getvalue(),
            file_name="aggregated_by_time.parquet",
            mime="application/octet-stream",
        )

