# Streamlit Amazon Fee Analyzer (SKU Summary + Pricing Suggestions)
# ---------------------------------------------------------------
# How to deploy on Streamlit Cloud / GitHub
# 1) Create a repo with two files:
#    - app.py  (this file)
#    - requirements.txt  with:  streamlit\npandas\nopenpyxl
# 2) Push to GitHub, then "New app" on streamlit.io and point to app.py.
#
# Local run:
#    pip install -r requirements.txt
#    streamlit run app.py
#
# What it does
# • Upload your Amazon Date Range/Settlement CSV (or XLSX)
# • Auto-detect key columns (case-insensitive)
# • Choose pricing logic: include tax or not, fee thresholds, target fee ratio
# • Outputs per-SKU summary with: Avg Price, Avg Fees (FBA+Commission+Other), Fee %, Raise-Price flag, Suggested Price
# • Download clean Excel with Summary + (optional) Detail sheet.

import io
import math
from typing import Dict, List, Optional

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Amazon Fee Analyzer — SKU Pricing", layout="wide")

st.title("📊 Amazon Fee Analyzer — SKU Pricing (SKU 汇总 + 建议售价)")
st.caption("Upload Amazon Date Range/Settlement report (CSV/XLSX). We'll compute per‑SKU fee ratios and pricing suggestions.")

# --------------------------
# Helpers
# --------------------------

def _lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

# Common alias map for columns found in Amazon settlement/date-range reports
COL_ALIASES: Dict[str, List[str]] = {
    "date": ["date/time", "date", "posted date", "posteddate", "transaction posted date"],
    "order_id": ["order id", "amazon order id", "amazonorderid"],
    "sku": ["sku", "merchant_sku", "seller-sku", "seller sku", "sku number"],
    # revenue
    "principal": ["product sales", "principal", "item-price", "item price"],
    "tax": ["product sales tax", "tax", "item-tax", "item tax"],
    # fees
    "selling_fees": ["selling fees", "commission", "referral fee", "selling fee"],
    "fba_fees": [
        "fba fees",
        "fbaperunitfulfillmentfee",
        "fulfillment fee",
        "fulfilment fee",
        "fulfillment-fee",
    ],
    "other_txn_fees": ["other transaction fees", "shipping chargeback", "shippingchargeback"],
    "other": ["other"],
    "qty": ["quantity", "qty"],
    "type": ["type"],
    "marketplace": ["marketplace"]
}


def pick_col(df: pd.DataFrame, keys: List[str]) -> Optional[str]:
    cols = set(df.columns)
    for k in keys:
        if k in cols:
            return k
    return None


def auto_map_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    dfl = _lower_cols(df)
    mapping: Dict[str, Optional[str]] = {}
    for std_name, aliases in COL_ALIASES.items():
        mapping[std_name] = pick_col(dfl, [a.lower() for a in aliases])
    return mapping


def coerce_number(s):
    """Convert Amazon money fields to float. Handles parentheses negatives, commas, blanks."""
    if isinstance(s, (int, float)):
        return float(s)
    if s is None:
        return 0.0
    t = str(s).strip()
    if t == "" or t.lower() in {"nan", "none"}:
        return 0.0
    # Remove currency symbols and commas
    t = t.replace(",", "")
    t = t.replace("£", "")
    # Parentheses negative
    neg = False
    if t.startswith("(") and t.endswith(")"):
        neg = True
        t = t[1:-1]
    try:
        v = float(t)
        return -v if neg else v
    except Exception:
        return 0.0


def build_summary(
    df_raw: pd.DataFrame,
    include_tax: bool = True,
    raise_fee_abs_threshold: float = 5.0,
    raise_fee_ratio_threshold: float = 0.50,
    target_fee_ratio: float = 0.40,
    include_detail_sheet: bool = False,
) -> pd.DataFrame:
    df = _lower_cols(df_raw)
    cols = auto_map_columns(df)

    required = ["sku", "principal", "selling_fees", "fba_fees"]
    missing = [r for r in required if not cols.get(r)]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found columns: {list(df.columns)[:20]} ...")

    sku_col = cols["sku"]
    principal_col = cols["principal"]
    tax_col = cols.get("tax")
    selling_col = cols["selling_fees"]
    fba_col = cols["fba_fees"]
    other_txn_col = cols.get("other_txn_fees")
    other_col = cols.get("other")
    qty_col = cols.get("qty")

    # Prepare numeric fields
    for c in [principal_col, tax_col, selling_col, fba_col, other_txn_col, other_col]:
        if c and c in df.columns:
            df[c] = df[c].map(coerce_number)

    df["price_incl_tax"] = df[principal_col] + (df[tax_col] if include_tax and tax_col else 0.0)
    df["fees_total"] = df[selling_col].fillna(0) + df[fba_col].fillna(0)
    if other_txn_col:
        df["fees_total"] += df[other_txn_col].fillna(0)
    if other_col:
        df["fees_total"] += df[other_col].fillna(0)

    if qty_col and qty_col in df.columns:
        df[qty_col] = df[qty_col].apply(lambda x: coerce_number(x) if pd.notna(x) else 1)
    else:
        df[qty_col or "quantity"] = 1
        qty_col = qty_col or "quantity"

    # Only keep order-like rows with a valid price (exclude adjustments without price)
    work = df.loc[df["price_incl_tax"].notna()]

    grp = work.groupby(sku_col, dropna=False)
    out = grp.agg(
        orders=(sku_col, "count"),
        units=(qty_col, "sum"),
        avg_price=("price_incl_tax", "mean"),
        med_price=("price_incl_tax", "median"),
        avg_fees=("fees_total", "mean"),
        med_fees=("fees_total", "median"),
        price_p10=("price_incl_tax", lambda s: s.quantile(0.10)),
        price_p90=("price_incl_tax", lambda s: s.quantile(0.90)),
    ).reset_index().rename(columns={sku_col: "SKU"})

    out["fee_ratio"] = out["avg_fees"] / out["avg_price"].replace({0: math.nan})

    # Raise-price decision and suggested price to hit target ratio
    out["raise_price"] = (
        (out["avg_fees"] > raise_fee_abs_threshold) & (out["fee_ratio"] > raise_fee_ratio_threshold)
    )
    out["suggested_price"] = out.apply(
        lambda r: (r["avg_fees"] / target_fee_ratio) if (r["avg_fees"] > 0 and not math.isclose(target_fee_ratio, 0.0)) else math.nan,
        axis=1,
    )

    # Human-friendly columns
    out = out[[
        "SKU", "orders", "units", "avg_price", "avg_fees", "fee_ratio", "price_p10", "price_p90", "raise_price", "suggested_price", "med_price", "med_fees"
    ]]

    out = out.sort_values(["raise_price", "fee_ratio", "avg_fees"], ascending=[False, False, False]).reset_index(drop=True)

    # Formatting for display only (we'll output raw numbers to Excel for accuracy)
    return out


# --------------------------
# Sidebar controls
# --------------------------
with st.sidebar:
    st.header("⚙️ Settings")
    include_tax = st.toggle("Use tax-inclusive price (含税价)", value=True, help="If off, price = Principal only.")
    fee_abs = st.number_input("Raise-Price absolute fee threshold (GBP)", value=5.0, min_value=0.0, step=0.5)
    fee_ratio = st.slider("Raise-Price ratio threshold", min_value=0.0, max_value=1.0, value=0.50, step=0.05)
    target_ratio = st.slider("Target fee ratio for suggested price", min_value=0.10, max_value=0.90, value=0.40, step=0.05, help="We suggest a price so Fees/Price ≤ this target.")
    include_detail = st.checkbox("Include raw detail sheet in Excel", value=False)

uploaded = st.file_uploader("Upload Amazon report CSV / XLSX", type=["csv", "xlsx"])  

if not uploaded:
    st.info("👆 Upload your report to start. Example: Date Range report with columns like 'SKU', 'Product Sales', 'Product Sales Tax', 'Selling fees', 'FBA fees', 'Other transaction fees', 'Other'.")
    st.stop()

# Read file
try:
    if uploaded.name.lower().endswith(".csv"):
        df_raw = pd.read_csv(uploaded)
    else:
        df_raw = pd.read_excel(uploaded)
except Exception as e:
    st.error(f"Failed to read file: {e}")
    st.stop()

st.success(f"Loaded file: {uploaded.name} — rows: {len(df_raw):,}")

# Build summary
try:
    summary = build_summary(
        df_raw,
        include_tax=include_tax,
        raise_fee_abs_threshold=fee_abs,
        raise_fee_ratio_threshold=fee_ratio,
        target_fee_ratio=target_ratio,
        include_detail_sheet=include_detail,
    )
except Exception as e:
    st.exception(e)
    st.stop()

# Display summary
st.subheader("🔎 SKU Summary (点击列头可排序)")
fmt = summary.copy()
for c in ["avg_price", "avg_fees", "price_p10", "price_p90", "med_price", "med_fees", "suggested_price"]:
    if c in fmt.columns:
        fmt[c] = fmt[c].map(lambda x: "" if pd.isna(x) else f"£{x:,.2f}")
if "fee_ratio" in fmt.columns:
    fmt["fee_ratio"] = fmt["fee_ratio"].map(lambda x: "" if pd.isna(x) else f"{x*100:.1f}%")
fmt["raise_price"] = fmt["raise_price"].map(lambda b: "✅ 建议涨价" if b else "👍 可维持")

st.dataframe(fmt, use_container_width=True, hide_index=True)

# Top offenders
with st.expander("🔥 高费用占比 SKU（建议优先处理）"):
    hot = summary[summary["raise_price"]].copy()
    if hot.empty:
        st.write("No SKU hit the raise-price thresholds.")
    else:
        hfmt = hot.copy()
        for c in ["avg_price", "avg_fees", "suggested_price"]:
            if c in hfmt.columns:
                hfmt[c] = hfmt[c].map(lambda x: f"£{x:,.2f}")
        hfmt["fee_ratio"] = hfmt["fee_ratio"].map(lambda x: f"{x*100:.1f}%")
        st.dataframe(hfmt, use_container_width=True, hide_index=True)

# Build Excel download
st.subheader("📥 Download Excel")
try:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary.to_excel(writer, index=False, sheet_name="SKU_Summary")
        # Auto widths
        ws = writer.book["SKU_Summary"]
        for col in ws.columns:
            max_len = 8
            col_letter = col[0].column_letter
            for cell in col:
                try:
                    val = str(cell.value)
                except Exception:
                    val = ""
                max_len = max(max_len, len(val))
            ws.column_dimensions[col_letter].width = min(max_len + 2, 60)
        if include_detail:
            raw = _lower_cols(df_raw)
            raw.to_excel(writer, index=False, sheet_name="Detail")
    st.download_button(
        label="⬇️ Download Amazon_Fee_Analysis_2025Q4.xlsx",
        data=output.getvalue(),
        file_name="Amazon_Fee_Analysis_2025Q4.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
except Exception as e:
    st.error(f"Excel export failed: {e}")

st.caption("Tips: For low-priced items (≤£15), fixed FBA fees dominate. Consider price bump, multipacks, or FBM to reduce fee ratio.")
