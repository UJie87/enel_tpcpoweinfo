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



