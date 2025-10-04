# Streamlit Amazon Fee Analyzer（中文表头版：SKU 汇总 + ASIN + 成本/毛利 + 建议售价）
# ---------------------------------------------------------------------------
# 部署说明（GitHub + Streamlit Cloud）
# 1) 仓库内放两个文件：
#    - app.py  （本文件）
#    - requirements.txt  内容：
#        streamlit
#        pandas
#        openpyxl
# 2) 在 streamlit.io 选择该仓库并指定 app.py 部署。
# 
# 本应用支持：
# • 上传 Amazon Date Range/Settlement 报表（CSV/XLSX），自动识别表头、分隔符、编码，跳过前言行；
# • 可选上传 成本配置表（支持 sku、unit_cost、inbound、packaging、extra、vat_rate、可选 asin）；
# • 可选上传 目录/Listing 映射表（sku, asin），当交易报表没有 ASIN 时补齐；
# • 侧边栏设置：是否用含税价显示费率、涨价阈值、目标费用占比、目标毛利率、是否中文表头；
# • 输出：每 SKU（或 SKU+ASIN）平均售价、费用结构、费用占比、是否建议涨价、建议售价（达成目标费用占比）、
#         毛利/毛利率（不含税口径）及为达成目标毛利率的建议售价（不含税/含税）。

import io
import math
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Amazon 费用与利润分析", layout="wide")

st.title("📊 Amazon 费用与利润分析 — SKU/ASIN 定价建议（中文表头版）")
st.caption("上传 Amazon 报表（CSV/XLSX）+ 可选成本/目录表，自动计算费用占比、毛利与建议售价。")

# ==========================
# 工具方法
# ==========================

def _lower_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df

# 报表列名别名映射
COL_ALIASES: Dict[str, List[str]] = {
    "date": ["date/time", "date", "posted date", "posteddate", "transaction posted date"],
    "order_id": ["order id", "amazon order id", "amazonorderid"],
    "sku": ["sku", "merchant_sku", "seller-sku", "seller sku", "seller sku id", "sku number"],
    # 收入
    "principal": ["product sales", "principal", "item-price", "item price"],
    "tax": ["product sales tax", "tax", "item-tax", "item tax"],
    # 费用
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
    "asin": ["asin", "asin/isbn", "asin / isbn", "asin (child)", "asin (parent)"]
}

# 成本表别名
COST_ALIASES: Dict[str, List[str]] = {
    "sku": ["sku", "seller-sku", "merchant_sku"],
    "unit_cost": ["unit_cost", "cogs", "cost", "unit cost"],
    "inbound": ["inbound", "inbound_per_unit", "inbound cost", "inbound_peru", "freight", "shipping"],
    "packaging": ["packaging", "packaging_per_unit", "pack", "pack cost"],
    "extra": ["extra", "extra_per_unit", "overhead", "other_cost"],
    "vat_rate": ["vat_rate", "vat", "vat %", "vat percent", "vatpercentage"],
    "asin": ["asin", "asin/isbn", "asin (child)", "asin (parent)"]
}

# 目录表（sku, asin）别名
CATALOG_ALIASES: Dict[str, List[str]] = {
    "sku": ["sku", "seller-sku", "merchant_sku"],
    "asin": ["asin", "asin/isbn", "asin (child)", "asin (parent)"]
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


def auto_map_catalog_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    dfl = _lower_cols(df)
    mapping: Dict[str, Optional[str]] = {}
    for std_name, aliases in CATALOG_ALIASES.items():
        mapping[std_name] = pick_col(dfl, [a.lower() for a in aliases])
    return mapping


def coerce_number(s):
    """金额字符串转浮点，支持( )负号、货币符号、千分位。"""
    if isinstance(s, (int, float)):
        return float(s)
    if s is None:
        return 0.0
    t = str(s).strip()
    if t == "" or t.lower() in {"nan", "none"}:
        return 0.0
    t = t.replace(",", "").replace("£", "").replace("¥", "").replace("$", "")
    neg = False
    if t.startswith("(") and t.endswith(")"):
        neg = True
        t = t[1:-1]
    try:
        v = float(t)
        return -v if neg else v
    except Exception:
        return 0.0

# ==========================
# 文件读取（鲁棒）
# ==========================

def _detect_header_row_and_sep(text: str) -> Tuple[int, str]:
    """自动定位数据表头行与分隔符（逗号/分号/Tab/竖线）。"""
    lines = text.splitlines()
    seps = [",", ";", "	", "|"]
    target_tokens = ["order", "sku"]
    for i in range(min(50, len(lines))):
        raw = lines[i].lower()
        for sep in seps:
            cells = [c.strip() for c in raw.split(sep)]
            if sum(any(tok in c for c in cells) for tok in target_tokens) >= 2:
                return i, sep
    # 兜底：按列数最多的行猜测
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
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(io.BytesIO(data))
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            text = data.decode(enc, errors="replace")
            hdr_idx, sep = _detect_header_row_and_sep(text)
            return pd.read_csv(io.StringIO(text), skiprows=hdr_idx, sep=sep, engine="python")
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(data), sep=None, engine="python", on_bad_lines="skip")


def read_any_csv_like(uploaded_file) -> pd.DataFrame:
    name = uploaded_file.name.lower()
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(uploaded_file)
    uploaded_file.seek(0)
    data = uploaded_file.read()
    for enc in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            text = data.decode(enc, errors="replace")
            return pd.read_csv(io.StringIO(text), engine="python")
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(data), sep=None, engine="python", on_bad_lines="skip")

# ==========================
# 核心汇总
# ==========================

def build_summary(
    df_raw: pd.DataFrame,
    include_tax: bool = True,
    raise_fee_abs_threshold: float = 5.0,
    raise_fee_ratio_threshold: float = 0.50,
    target_fee_ratio: float = 0.40,
    target_margin: float = 0.30,
    cost_df: Optional[pd.DataFrame] = None,
    catalog_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    df = _lower_cols(df_raw)
    cols = auto_map_columns(df)

    required = ["sku", "principal", "selling_fees", "fba_fees"]
    missing = [r for r in required if not cols.get(r)]
    if missing:
        raise ValueError(f"缺少关键列: {missing}. 现有列举例: {list(df.columns)[:20]} ...")

    sku_col = cols["sku"]
    principal_col = cols["principal"]
    tax_col = cols.get("tax")
    selling_col = cols["selling_fees"]
    fba_col = cols["fba_fees"]
    other_txn_col = cols.get("other_txn_fees")
    other_col = cols.get("other")
    qty_col = cols.get("qty")
    asin_col = cols.get("asin")

    # 数值化
    for c in [principal_col, tax_col, selling_col, fba_col, other_txn_col, other_col]:
        if c and c in df.columns:
            df[c] = df[c].map(coerce_number)

    df["price_incl_tax"] = df[principal_col] + (df[tax_col] if include_tax and tax_col else 0.0)
    df["price_ex_vat"] = df[principal_col]

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
    out = (
        grp.agg(
            orders=(sku_col, "count"),
            units=(qty_col, "sum"),
            avg_price_incl=("price_incl_tax", "mean"),
            avg_price_ex=("price_ex_vat", "mean"),
            avg_commission=("commission_fee", "mean"),
            avg_fba=("fba_fee", "mean"),
            avg_other=("other_fees", "mean"),
        )
        .reset_index()
        .rename(columns={**({sku_col: "SKU"}), **({asin_col: "ASIN"} if asin_col else {})})
    )

    out["avg_fees"] = out["avg_commission"] + out["avg_fba"] + out["avg_other"]
    base_price = out["avg_price_incl"] if include_tax else out["avg_price_ex"]
    out["fee_ratio"] = out["avg_fees"] / base_price.replace({0: math.nan})

    out["raise_price"] = (out["avg_fees"] > raise_fee_abs_threshold) & (out["fee_ratio"] > raise_fee_ratio_threshold)

    out["suggest_price_fee_target_incl"] = out.apply(
        lambda r: (r["avg_fees"] / target_fee_ratio) if include_tax else math.nan, axis=1
    )
    out["suggest_price_fee_target_ex"] = out.apply(
        lambda r: (r["avg_fees"] / target_fee_ratio) if not include_tax else math.nan, axis=1
    )

    # 预创建利润相关列（即使无成本表也不报错）
    out["unit_cost_total"], out["gross_profit_ex"], out["margin_ex"] = math.nan, math.nan, math.nan
    out["suggest_price_margin_ex"], out["suggest_price_margin_incl"] = math.nan, math.nan
    out["commission_rate"] = math.nan
    out["fixed_fees_ex"] = out["avg_fba"] + out["avg_other"]

    # ==== 从成本表/目录表补齐 ASIN（当报表无 ASIN） ====
    def _pick(df, names):
        cols = [c for c in df.columns if c.lower().strip() in [n.lower() for n in names]]
        return cols[0] if cols else None

    if ("ASIN" not in out.columns) and (cost_df is not None) and (not cost_df.empty):
        c = _lower_cols(cost_df)
        sku_c = _pick(c, COST_ALIASES["sku"])
        asin_c = _pick(c, COST_ALIASES["asin"]) if "asin" in COST_ALIASES else None
        if sku_c and asin_c and asin_c in c.columns:
            out = (
                out.merge(c[[sku_c, asin_c]].drop_duplicates(), left_on="SKU", right_on=sku_c, how="left")
                   .rename(columns={asin_c: "ASIN"})
                   .drop(columns=[sku_c])
            )

    if ("ASIN" not in out.columns) and (catalog_df is not None) and (not catalog_df.empty):
        t = _lower_cols(catalog_df)
        sku_t = _pick(t, CATALOG_ALIASES["sku"])
        asin_t = _pick(t, CATALOG_ALIASES["asin"])
        if sku_t and asin_t:
            out = (
                out.merge(t[[sku_t, asin_t]].drop_duplicates(), left_on="SKU", right_on=sku_t, how="left")
                   .rename(columns={asin_t: "ASIN"})
                   .drop(columns=[sku_t])
            )

    # ==== 利润（需成本表） ====
    if cost_df is not None and not cost_df.empty:
        cdf = _lower_cols(cost_df)
        cmap = auto_map_cost_columns(cdf)
        c_sku = cmap.get("sku")
        if not c_sku:
            raise ValueError("成本表缺少 SKU 列（支持 sku/seller-sku/merchant_sku）")
        for key in ["unit_cost", "inbound", "packaging", "extra", "vat_rate"]:
            col = cmap.get(key)
            if col and col in cdf.columns:
                cdf[col] = cdf[col].map(coerce_number)
        unit_cost = cdf[cmap.get("unit_cost")] if cmap.get("unit_cost") in cdf.columns else 0.0
        inbound = cdf[cmap.get("inbound")] if cmap.get("inbound") in cdf.columns else 0.0
        packaging = cdf[cmap.get("packaging")] if cmap.get("packaging") in cdf.columns else 0.0
        extra = cdf[cmap.get("extra")] if cmap.get("extra") in cdf.columns else 0.0
        cdf["unit_cost_total"] = unit_cost.fillna(0) + inbound.fillna(0) + packaging.fillna(0) + extra.fillna(0)
        if cmap.get("vat_rate") and cmap.get("vat_rate") in cdf.columns:
            cdf["vat_rate_norm"] = cdf[cmap["vat_rate"]].apply(lambda v: (v/100.0) if v and abs(v) > 1 else (v if pd.notna(v) else math.nan))
        else:
            cdf["vat_rate_norm"] = math.nan

        cdf_keep = cdf[[c_sku, "unit_cost_total", "vat_rate_norm"]].drop_duplicates()
        out = out.merge(cdf_keep, left_on="SKU", right_on=c_sku, how="left").drop(columns=[c_sku])

        rev_ex = out["avg_price_ex"].copy()
        need_backout = rev_ex.isna() | (rev_ex == 0)
        if need_backout.any():
            out.loc[need_backout & out["vat_rate_norm"].notna(), "avg_price_ex"] = (
                out.loc[need_backout & out["vat_rate_norm"].notna(), "avg_price_incl"] / (1 + out.loc[need_backout & out["vat_rate_norm"].notna(), "vat_rate_norm"])
            )
        rev_ex = out["avg_price_ex"]

        with pd.option_context('mode.use_inf_as_na', True):
            out["commission_rate"] = out["avg_commission"] / rev_ex.replace({0: math.nan})
            out["commission_rate"] = out["commission_rate"].clip(lower=0, upper=0.20)
        out["fixed_fees_ex"] = out["avg_fba"] + out["avg_other"]

        out["unit_cost_total"] = out["unit_cost_total"].astype(float)
        out["gross_profit_ex"] = rev_ex - (out["unit_cost_total"] + out["fixed_fees_ex"] + out["commission_rate"] * rev_ex)
        out["margin_ex"] = out["gross_profit_ex"] / rev_ex.replace({0: math.nan})

        denom = 1 - out["commission_rate"] - target_margin
        out.loc[(denom > 0) & (out["unit_cost_total"].notna()), "suggest_price_margin_ex"] = (
            (out["unit_cost_total"] + out["fixed_fees_ex"]) / denom
        )
        out.loc[out["suggest_price_margin_ex"].notna() & out["vat_rate_norm"].notna(), "suggest_price_margin_incl"] = (
            out["suggest_price_margin_ex"] * (1 + out["vat_rate_norm"])
        )

    # ==== 输出列顺序（含 ASIN） ====
    nice = out.copy()
    cols_order = ["SKU"]
    if "ASIN" in nice.columns:
        cols_order.append("ASIN")
    cols_order += [
        "orders", "units",
        "avg_price_incl", "avg_price_ex",
        "avg_commission", "avg_fba", "avg_other", "avg_fees", "fee_ratio", "raise_price",
        "suggest_price_fee_target_incl", "suggest_price_fee_target_ex",
        "unit_cost_total", "commission_rate", "fixed_fees_ex", "gross_profit_ex", "margin_ex",
        "suggest_price_margin_ex", "suggest_price_margin_incl",
    ]
    cols_order = [c for c in cols_order if c in nice.columns]
    nice = nice[cols_order].sort_values(["raise_price", "margin_ex", "fee_ratio", "avg_fees"], ascending=[False, True, False, False]).reset_index(drop=True)
    return nice

# ==========================
# 侧边栏 & 上传
# ==========================
with st.sidebar:
    st.header("⚙️ 设置")
    include_tax = st.toggle("费用占比按含税价计算", value=True, help="只影响费用%展示；利润计算使用不含税口径。")
    fee_abs = st.number_input("建议涨价：绝对费用阈值(£)", value=5.0, min_value=0.0, step=0.5)
    fee_ratio = st.slider("建议涨价：费用占比阈值", min_value=0.0, max_value=1.0, value=0.50, step=0.05)
    target_ratio = st.slider("建议售价的目标费用占比", min_value=0.10, max_value=0.90, value=0.40, step=0.05)
    target_margin = st.slider("目标毛利率(不含税)", min_value=0.10, max_value=0.80, value=0.30, step=0.05)
    use_cn_headers = st.toggle("页面/导出使用中文表头", value=True)
    include_detail = st.checkbox("Excel 另存原始Sheet(体积较大)", value=False)

uploaded = st.file_uploader("上传 Amazon 报表 CSV / XLSX", type=["csv", "xlsx"])
cost_file = st.file_uploader("(可选) 成本配置表：sku, unit_cost, inbound, packaging, extra, vat_rate, (可选 asin)", type=["csv", "xlsx"], key="cost")
catalog_file = st.file_uploader("(可选) 目录/Listing 映射：sku, asin", type=["csv", "xlsx"], key="catalog")

if not uploaded:
    st.info("👆 先上传交易报表。若CSV带前言说明或用分号/Tab分隔，系统会自动识别。")
    st.stop()

# 读取文件
try:
    df_raw = read_amazon_report(uploaded)
except Exception as e:
    st.error(f"交易报表读取失败: {e}")
    st.stop()

cost_df = None
if cost_file is not None:
    try:
        cost_df = read_any_csv_like(cost_file)
        st.success(f"已载入成本表：{cost_file.name} — {len(cost_df):,} 行")
    except Exception as e:
        st.warning(f"成本表读取失败（忽略）：{e}")

catalog_df = None
if catalog_file is not None:
    try:
        catalog_df = read_any_csv_like(catalog_file)
        st.success(f"已载入目录/Listing：{catalog_file.name} — {len(catalog_df):,} 行")
    except Exception as e:
        st.warning(f"目录表读取失败（忽略）：{e}")

st.success(f"已载入交易报表：{uploaded.name} — {len(df_raw):,} 行, {len(df_raw.columns)} 列")

# 汇总
try:
    summary = build_summary(
        df_raw,
        include_tax=include_tax,
        raise_fee_abs_threshold=fee_abs,
        raise_fee_ratio_threshold=fee_ratio,
        target_fee_ratio=target_ratio,
        target_margin=target_margin,
        cost_df=cost_df,
        catalog_df=catalog_df,
    )
except Exception as e:
    st.exception(e)
    st.stop()

# ==========================
# 展示（中文表头）
# ==========================
CN_MAP = {
    "SKU": "SKU",
    "ASIN": "ASIN",
    "orders": "订单数",
    "units": "销量（件）",
    "avg_price_incl": "平均含税售价",
    "avg_price_ex": "平均不含税售价",
    "avg_commission": "平均佣金",
    "avg_fba": "平均FBA费用",
    "avg_other": "其他费用",
    "avg_fees": "总费用",
    "fee_ratio": "费用占比",
    "raise_price": "是否建议涨价",
    "suggest_price_fee_target_incl": "目标费用占比建议售价（含税）",
    "suggest_price_fee_target_ex": "目标费用占比建议售价（不含税）",
    "unit_cost_total": "单件成本",
    "commission_rate": "佣金率",
    "fixed_fees_ex": "固定费用（不含税）",
    "gross_profit_ex": "毛利（不含税）",
    "margin_ex": "毛利率（不含税）",
    "suggest_price_margin_ex": "达标毛利建议售价（不含税）",
    "suggest_price_margin_incl": "达标毛利建议售价（含税）",
}

st.subheader("🔎 SKU/ASIN 汇总（点击列头可排序）")
fmt = summary.copy()
for c in [
    "avg_price_incl", "avg_price_ex", "avg_commission", "avg_fba", "avg_other", "avg_fees",
    "fixed_fees_ex", "gross_profit_ex", "suggest_price_fee_target_incl", "suggest_price_fee_target_ex",
    "suggest_price_margin_ex", "suggest_price_margin_incl", "unit_cost_total",
]:
    if c in fmt.columns:
        fmt[c] = fmt[c].map(lambda x: "" if pd.isna(x) else f"£{x:,.2f}")
if "fee_ratio" in fmt.columns:
    fmt["fee_ratio"] = fmt["fee_ratio"].map(lambda x: "" if pd.isna(x) else f"{x*100:.1f}%")
if "margin_ex" in fmt.columns:
    fmt["margin_ex"] = fmt["margin_ex"].map(lambda x: "" if pd.isna(x) else f"{x*100:.1f}%")
if "commission_rate" in fmt.columns:
    fmt["commission_rate"] = fmt["commission_rate"].map(lambda x: "" if pd.isna(x) else f"{x*100:.1f}%")
fmt["raise_price"] = fmt["raise_price"].map(lambda b: "✅ 建议涨价" if b else "👍 可维持")

if use_cn_headers:
    fmt = fmt.rename(columns={k: v for k, v in CN_MAP.items() if k in fmt.columns})

st.dataframe(fmt, use_container_width=True, hide_index=True)

# 高优先级
with st.expander("🔥 高费用占比或低毛利（建议优先处理）"):
    hot = summary[(summary.get("raise_price", False)) | (summary.get("margin_ex").notna() & (summary.get("margin_ex") < target_margin))].copy()
    if hot.empty:
        st.write("暂无命中阈值的SKU/ASIN。")
    else:
        hfmt = hot.copy()
        for c in ["avg_price_incl", "avg_fees", "unit_cost_total", "gross_profit_ex", "suggest_price_margin_incl", "suggest_price_margin_ex"]:
            if c in hfmt.columns:
                hfmt[c] = hfmt[c].map(lambda x: f"£{x:,.2f}" if pd.notna(x) else "")
        for c in ["fee_ratio", "margin_ex"]:
            if c in hfmt.columns:
                hfmt[c] = hfmt[c].map(lambda x: f"{x*100:.1f}%" if pd.notna(x) else "")
        if use_cn_headers:
            hfmt = hfmt.rename(columns={k: v for k, v in CN_MAP.items() if k in hfmt.columns})
        st.dataframe(hfmt, use_container_width=True, hide_index=True)

# ==========================
# 导出 Excel（中文表头）
# ==========================
st.subheader("📥 导出 Excel")
try:
    export_df = summary.copy()
    if use_cn_headers:
        export_df = export_df.rename(columns={k: v for k, v in CN_MAP.items() if k in export_df.columns})

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="SKU_汇总")
        ws = writer.book["SKU_汇总"]
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
            _lower_cols(df_raw).to_excel(writer, index=False, sheet_name="Detail")
        if cost_df is not None:
            _lower_cols(cost_df).to_excel(writer, index=False, sheet_name="Cost_Config")
        if catalog_df is not None:
            _lower_cols(catalog_df).to_excel(writer, index=False, sheet_name="Catalog_Mapping")

    st.download_button(
        label="⬇️ 下载：Amazon_Fee_Analysis_2025Q4.xlsx",
        data=output.getvalue(),
        file_name="Amazon_Fee_Analysis_2025Q4.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
except Exception as e:
    st.error(f"导出失败: {e}")

st.caption("提示：若交易报表无 ASIN，可上传 成本表（含 asin 列）或 目录表（sku, asin）补齐。利润以不含税口径计算；费用占比可切换含/不含税显示。")
