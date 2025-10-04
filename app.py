# Streamlit Amazon Fee Analyzer (SKU Summary + Pricing + Cost Config)
# ---------------------------------------------------------------
# Robust CSV/Excel loader with delimiter detection and header auto-locate.
#
# How to deploy on Streamlit Cloud / GitHub
# 1) Create a repo with two files:
#    - app.py  (this file)
#    - requirements.txt  with:  streamlit
#pandas
#openpyxl
# 2) Push to GitHub, then "New app" on streamlit.io and point to app.py.
#
# Local run:
#    pip install -r requirements.txt
#    streamlit run app.py
#
# What it does
# • Upload your Amazon Date Range/Settlement CSV (or XLSX)
# • Auto-detect key columns (case-insensitive)
# • Robustly parses messy CSV (commas/semicolons/tabs), skips preface rows, handles UTF‑8/Windows‑1252
# • Optional: Upload a Cost Config (CSV/XLSX) per SKU (unit_cost, inbound, packaging, extra, vat_rate)
# • Choose pricing logic: include tax or not, fee thresholds, target fee ratio, target margin
# • Outputs per‑SKU summary with: Avg Price (incl/ex VAT), Avg Fees (FBA+Commission+Other), Fee %, Raise‑Price flag,
#   Suggested Price for fee ratio target, Gross Profit & Margin using your costs, Suggested Price for target margin.

import io
import math
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Amazon Fee + Profit Analyzer", layout="wide")

st.title("📊 Amazon Fee & Profit Analyzer — SKU 定价与利润")
st.caption("Upload Amazon Date Range/Settlement report (CSV/XLSX) + optional Cost Config to get fee ratios, margins, and price suggestions.")

# --------------------------
# Helpers
# --------------------------

def _lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

# Common alias map for columns found in Amazon reports
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
    "marketplace": ["marketplace"],
    "asin": ["asin", "asin/ isbn", "asin/isbn", "asin (child)", "asin (parent)"]
}

# Aliases for Cost Config file
COST_ALIASES: Dict[str, List[str]] = {
    "sku": ["sku", "seller-sku", "merchant_sku"],
    "unit_cost": ["unit_cost", "cogs", "cost", "unit cost"],
    "inbound": ["inbound", "inbound_per_unit", "inbound cost", "inbound_peru", "freight", "shipping"],
    "packaging": ["packaging", "packaging_per_unit", "pack", "pack cost"],
    "extra": ["extra", "extra_per_unit", "overhead", "other_cost"],
    "vat_rate": ["vat_rate", "vat", "vat %", "vat percent", "vatpercentage"],
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


def auto_map_cost_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    dfl = _lower_cols(df)
    mapping: Dict[str, Optional[str]] = {}
    for std_name, aliases in COST_ALIASES.items():
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
    t = t.replace(",", "").replace("£", "")
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

# --------------------------
# Robust file readers
# --------------------------

def _detect_header_row_and_sep(text: str) -> Tuple[int, str]:
    """Find the line index of the header row and a reasonable delimiter.
    Looks for a row containing at least 'order' and 'sku' tokens.
    """
    lines = text.splitlines()
    # Common separators to try
    seps = [",", ";", "	", "|"]
    target_tokens = ["order", "sku"]
    best = (0, ",")
    for i in range(min(50, len(lines))):
        raw = lines[i].lower()
        for sep in seps:
            cells = [c.strip() for c in raw.split(sep)]
            if sum(any(tok in c for c in cells) for tok in target_tokens) >= 2:
                return i, sep
    # Fallback: try to sniff by max columns
    max_cols, best_sep, best_i = 0, ",", 0
    for i in range(min(50, len(lines))):
        for sep in seps:
            cols = len(lines[i].split(sep))
            if cols > max_cols:
                max_cols, best_sep, best_i = cols, sep, i
    return best_i, best_sep


def read_amazon_report(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    data = uploaded_file.read()
    # Try Excel first
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(data))
    # CSV/text branch: try encodings and delimiters
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            text = data.decode(enc, errors="replace")
            hdr_idx, sep = _detect_header_row_and_sep(text)
            return pd.read_csv(io.StringIO(text), skiprows=hdr_idx, sep=sep, engine="python")
        except Exception:
            continue
    # Last resort
    return pd.read_csv(io.BytesIO(data), sep=None, engine="python", on_bad_lines="skip")


def read_cost_file(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(uploaded_file)
    # CSV with common encodings
    uploaded_file.seek(0)
    data = uploaded_file.read()
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return pd.read_csv(io.BytesIO(data), engine="python")
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(data), sep=None, engine="python", on_bad_lines="skip")

# --------------------------
# Core builder
# --------------------------

def build_summary(
    df_raw: pd.DataFrame,
    include_tax: bool = True,
    raise_fee_abs_threshold: float = 5.0,
    raise_fee_ratio_threshold: float = 0.50,
    target_fee_ratio: float = 0.40,
    target_margin: float = 0.30,
    cost_df: Optional[pd.DataFrame] = None,
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
    asin_col = cols.get("asin")

    # Prepare numeric fields
    for c in [principal_col, tax_col, selling_col, fba_col, other_txn_col, other_col]:
        if c and c in df.columns:
            df[c] = df[c].map(coerce_number)

    df["price_incl_tax"] = df[principal_col] + (df[tax_col] if include_tax and tax_col else 0.0)
    df["price_ex_vat"] = df[principal_col]

    # Split fee components
    df["commission_fee"] = df[selling_col].fillna(0)
    df["fba_fee"] = df[fba_col].fillna(0)
    df["other_fees"] = 0.0
    if other_txn_col:
        df["other_fees"] += df[other_txn_col].fillna(0)
    if other_col:
        df["other_fees"] += df[other_col].fillna(0)

    df["fees_total"] = df["commission_fee"] + df["fba_fee"] + df["other_fees"]

    if qty_col and qty_col in df.columns:
        df[qty_col] = df[qty_col].apply(lambda x: coerce_number(x) if pd.notna(x) else 1)
    else:
        df[qty_col or "quantity"] = 1
        qty_col = qty_col or "quantity"

    work = df.loc[df["price_incl_tax"].notna()]

    group_cols = [sku_col] + ([asin_col] if asin_col else [])
    grp = work.groupby(group_cols, dropna=False)
    out = grp.agg(
        orders=(sku_col, "count"),
        units=(qty_col, "sum"),
        avg_price_incl=("price_incl_tax", "mean"),
        avg_price_ex=("price_ex_vat", "mean"),
        avg_commission=("commission_fee", "mean"),
        avg_fba=("fba_fee", "mean"),
        avg_other=("other_fees", "mean"),
    ) .reset_index() .rename(columns={**({sku_col: "SKU"}), **({asin_col: "ASIN"} if asin_col else {})})

    out["avg_fees"] = out["avg_commission"] + out["avg_fba"] + out["avg_other"]
    base_price = out["avg_price_incl"] if include_tax else out["avg_price_ex"]
    out["fee_ratio"] = out["avg_fees"] / base_price.replace({0: math.nan})

    # Raise-price flag (fee absolute + ratio)
    out["raise_price"] = (out["avg_fees"] > raise_fee_abs_threshold) & (out["fee_ratio"] > raise_fee_ratio_threshold)

    # Suggested price for fee-ratio target (simple): assume fees fixed at current avg_fees
    out["suggest_price_fee_target_incl"] = out.apply(
        lambda r: (r["avg_fees"] / target_fee_ratio) if include_tax else math.nan,
        axis=1,
    )
    out["suggest_price_fee_target_ex"] = out.apply(
        lambda r: (r["avg_fees"] / target_fee_ratio) if not include_tax else math.nan,
        axis=1,
    )

    # --- Merge Costs for Profit/Margin ---
    # Pre-create optional columns so downstream selection doesn't fail even without a cost file
    out["unit_cost_total"] = math.nan
    out["gross_profit_ex"] = math.nan
    out["margin_ex"] = math.nan
    out["suggest_price_margin_ex"] = math.nan
    out["suggest_price_margin_incl"] = math.nan
    # Also create commission_rate / fixed_fees_ex defaults
    out["commission_rate"] = math.nan
    # Default fixed fees (ex‑VAT proxy) as FBA + Other; will be recomputed if cost file present
    out["fixed_fees_ex"] = out["avg_fba"] + out["avg_other"]

    if cost_df is not None and not cost_df.empty:
        cdf = _lower_cols(cost_df)
        cmap = auto_map_cost_columns(cdf)
        c_sku = cmap.get("sku")
        if not c_sku:
            raise ValueError("Cost file missing SKU column. Expected one of: sku, seller-sku, merchant_sku")
        # Coerce numbers
        for key in ["unit_cost", "inbound", "packaging", "extra", "vat_rate"]:
            col = cmap.get(key)
            if col and col in cdf.columns:
                cdf[col] = cdf[col].map(coerce_number)
        # Build cost totals
        unit_cost = cdf[cmap.get("unit_cost")] if cmap.get("unit_cost") in cdf.columns else 0.0
        inbound = cdf[cmap.get("inbound")] if cmap.get("inbound") in cdf.columns else 0.0
        packaging = cdf[cmap.get("packaging")] if cmap.get("packaging") in cdf.columns else 0.0
        extra = cdf[cmap.get("extra")] if cmap.get("extra") in cdf.columns else 0.0
        cdf["unit_cost_total"] = unit_cost.fillna(0) + inbound.fillna(0) + packaging.fillna(0) + extra.fillna(0)
        # Normalize VAT rate to fraction (e.g., 20 -> 0.20)
        if cmap.get("vat_rate") and cmap.get("vat_rate") in cdf.columns:
            cdf["vat_rate_norm"] = cdf[cmap["vat_rate"]].apply(lambda v: (v/100.0) if v and abs(v) > 1 else (v if pd.notna(v) else math.nan))
        else:
            cdf["vat_rate_norm"] = math.nan

        cdf_keep = cdf[[c_sku, "unit_cost_total", "vat_rate_norm"]].drop_duplicates()
        merged = out.merge(cdf_keep, left_on="SKU", right_on=c_sku, how="left")
        out = merged.drop(columns=[c_sku])

        # Revenue ex VAT for profit calc — prefer avg_price_ex from report; otherwise back out using vat_rate
        rev_ex = out["avg_price_ex"].copy()
        need_backout = rev_ex.isna() | (rev_ex == 0)
        if need_backout.any():
            out.loc[need_backout & out["vat_rate_norm"].notna(), "avg_price_ex"] = (
                out.loc[need_backout & out["vat_rate_norm"].notna(), "avg_price_incl"]
                / (1 + out.loc[need_backout & out["vat_rate_norm"].notna(), "vat_rate_norm"])
            )
        rev_ex = out["avg_price_ex"]

        # Effective commission rate estimated from ex‑VAT price
        with pd.option_context('mode.use_inf_as_na', True):
            out["commission_rate"] = out["avg_commission"] / rev_ex.replace({0: math.nan})
            out["commission_rate"] = out["commission_rate"].clip(lower=0, upper=0.20)  # cap 0-20% as sanity
        out["fixed_fees_ex"] = out["avg_fba"] + out["avg_other"]

        # Profit & margin (ex VAT)
        out["unit_cost_total"] = out["unit_cost_total"].astype(float)
        out["gross_profit_ex"] = rev_ex - (out["unit_cost_total"] + out["fixed_fees_ex"] + out["commission_rate"] * rev_ex)
        out["margin_ex"] = out["gross_profit_ex"] / rev_ex.replace({0: math.nan})

        # Suggested price to hit target margin (ex VAT)
        denom = 1 - out["commission_rate"] - target_margin
        out.loc[(denom > 0) & (out["unit_cost_total"].notna()), "suggest_price_margin_ex"] = (
            (out["unit_cost_total"] + out["fixed_fees_ex"]) / denom
        )
        # Convert suggested ex‑VAT to inc VAT if vat_rate known
        out.loc[out["suggest_price_margin_ex"].notna() & out["vat_rate_norm"].notna(), "suggest_price_margin_incl"] = (
            out["suggest_price_margin_ex"] * (1 + out["vat_rate_norm"])
        )

    nice = out.copy()
    # Build columns dynamically to include ASIN when available
    cols_order = ["SKU"]
    if "ASIN" in nice.columns:
        cols_order.append("ASIN")
    cols_order += [
        "orders", "units",
        "avg_price_incl", "avg_price_ex",
        "avg_commission", "avg_fba", "avg_other", "avg_fees", "fee_ratio", "raise_price",
        "suggest_price_fee_target_incl", "suggest_price_fee_target_ex",
        "unit_cost_total", "commission_rate", "fixed_fees_ex", "gross_profit_ex", "margin_ex",
        "suggest_price_margin_ex", "suggest_price_margin_incl"
    ]
    # Keep only those that exist (safety)
    cols_order = [c for c in cols_order if c in nice.columns]
    nice = nice[cols_order]

    nice = nice.sort_values(["raise_price", "margin_ex", "fee_ratio", "avg_fees"], ascending=[False, True, False, False]).reset_index(drop=True)
    return nice

# --------------------------
# Sidebar controls
# --------------------------
with st.sidebar:
    st.header("⚙️ Settings")
    include_tax = st.toggle("Use tax-inclusive price for fee % (含税)", value=True, help="Only affects fee ratio display; profit uses ex‑VAT.")
    fee_abs = st.number_input("Raise-Price absolute fee threshold (GBP)", value=5.0, min_value=0.0, step=0.5)
    fee_ratio = st.slider("Raise-Price ratio threshold", min_value=0.0, max_value=1.0, value=0.50, step=0.05)
    target_ratio = st.slider("Target fee ratio for suggested price", min_value=0.10, max_value=0.90, value=0.40, step=0.05, help="We suggest a price so Fees/Price ≤ this target.")
    target_margin = st.slider("Target gross margin (ex‑VAT)", min_value=0.10, max_value=0.80, value=0.30, step=0.05)
    include_detail = st.checkbox("Include raw Detail sheet in Excel", value=False)

uploaded = st.file_uploader("Upload Amazon report CSV / XLSX", type=["csv", "xlsx"])  
cost_file = st.file_uploader("(Optional) Upload Cost Config per SKU (CSV/XLSX) — columns: sku, unit_cost, inbound, packaging, extra, vat_rate", type=["csv", "xlsx"], key="cost")

if not uploaded:
    st.info("👆 Upload your Amazon report to start. Example columns: 'SKU', 'Product Sales', 'Product Sales Tax', 'Selling fees', 'FBA fees', 'Other transaction fees', 'Other'. If your CSV has a preface, we'll auto-skip it.")
    st.stop()

# Read files (robust)
try:
    df_raw = read_amazon_report(uploaded)
except Exception as e:
    st.error(f"Failed to read Amazon report: {e}")
    st.stop()

cost_df = None
if cost_file is not None:
    try:
        cost_df = read_cost_file(cost_file)
        st.success(f"Loaded cost file: {cost_file.name} — rows: {len(cost_df):,}")
    except Exception as e:
        st.warning(f"Cost file read failed (ignored): {e}")

st.success(f"Loaded Amazon file: {uploaded.name} — rows: {len(df_raw):,}")

# Build summary
try:
    summary = build_summary(
        df_raw,
        include_tax=include_tax,
        raise_fee_abs_threshold=fee_abs,
        raise_fee_ratio_threshold=fee_ratio,
        target_fee_ratio=target_ratio,
        target_margin=target_margin,
        cost_df=cost_df,
    )
except Exception as e:
    st.exception(e)
    st.stop()

# Display summary
st.subheader("🔎 SKU Summary (点击列头可排序)")
fmt = summary.copy()
for c in ["avg_price_incl", "avg_price_ex", "avg_commission", "avg_fba", "avg_other", "avg_fees", "fixed_fees_ex", "gross_profit_ex", "suggest_price_fee_target_incl", "suggest_price_fee_target_ex", "suggest_price_margin_ex", "suggest_price_margin_incl", "unit_cost_total"]:
    if c in fmt.columns:
        fmt[c] = fmt[c].map(lambda x: "" if pd.isna(x) else f"£{x:,.2f}")
if "fee_ratio" in fmt.columns:
    fmt["fee_ratio"] = fmt["fee_ratio"].map(lambda x: "" if pd.isna(x) else f"{x*100:.1f}%")
if "margin_ex" in fmt.columns:
    fmt["margin_ex"] = fmt["margin_ex"].map(lambda x: "" if pd.isna(x) else f"{x*100:.1f}%")
if "commission_rate" in fmt.columns:
    fmt["commission_rate"] = fmt["commission_rate"].map(lambda x: "" if pd.isna(x) else f"{x*100:.1f}%")
fmt["raise_price"] = fmt["raise_price"].map(lambda b: "✅ 建议涨价" if b else "👍 可维持")

st.dataframe(fmt, use_container_width=True, hide_index=True)

# Top offenders
with st.expander("🔥 高费用占比或低毛利 SKU（建议优先处理）"):
    hot = summary[(summary["raise_price"]) | (summary["margin_ex"].notna() & (summary["margin_ex"] < target_margin))].copy()
    if hot.empty:
        st.write("No SKU hit the thresholds.")
    else:
        hfmt = hot.copy()
        for c in ["avg_price_incl", "avg_fees", "unit_cost_total", "gross_profit_ex", "suggest_price_margin_incl", "suggest_price_margin_ex"]:
            if c in hfmt.columns:
                hfmt[c] = hfmt[c].map(lambda x: f"£{x:,.2f}" if pd.notna(x) else "")
        for c in ["fee_ratio", "margin_ex"]:
            if c in hfmt.columns:
                hfmt[c] = hfmt[c].map(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "")
        st.dataframe(hfmt, use_container_width=True, hide_index=True)

# Build Excel download
st.subheader("📥 Download Excel")
try:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary.to_excel(writer, index=False, sheet_name="SKU_Summary")
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
        if cost_df is not None:
            _lower_cols(cost_df).to_excel(writer, index=False, sheet_name="Cost_Config")
        # Optionally include raw Detail (disabled by default to keep size small)
        # raw = _lower_cols(df_raw)
        # raw.to_excel(writer, index=False, sheet_name="Detail")
    st.download_button(
        label="⬇️ Download Amazon_Fee_Analysis_2025Q4.xlsx",
        data=output.getvalue(),
        file_name="Amazon_Fee_Analysis_2025Q4.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
except Exception as e:
    st.error(f"Excel export failed: {e}")

st.caption("Tips: CSV 带有说明前言或用分号/制表符分隔也没关系，应用会自动定位表头与分隔符。利润以不含税口径计算；费率显示可切换含/不含税。")
