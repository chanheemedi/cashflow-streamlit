import streamlit as st
import pandas as pd
import numpy as np
import re
import hashlib
from io import BytesIO
from datetime import datetime, timedelta, date
import calendar
import math
import openpyxl
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

@st.cache_resource
def gs_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)

def ws(name: str):
    sh = gs_client().open_by_key(st.secrets["app"]["sheet_id"])
    return sh.worksheet(name)

def read_df(sheet_name: str) -> pd.DataFrame:
    w = ws(sheet_name)
    values = w.get_all_values()
    if not values:
        return pd.DataFrame()
    header = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=header)

def overwrite_df(sheet_name: str, df: pd.DataFrame):
    w = ws(sheet_name)
    w.clear()
    if df is None or df.empty:
        return
    w.update([df.columns.tolist()] + df.astype(str).fillna("").values.tolist())
# =========================================================
# Config
# =========================================================
EXCLUDED_ACCOUNT_NAMES = {"신한_에셀", "하나_꾸러기건식"}  # 병원 범위에서 제외
INTERNAL_WINDOW = timedelta(hours=2)

# 메디칼론 이벤트 키워드(기본값) - 앱에서 수정 가능
DEFAULT_DRAWDOWN_KEYWORDS = ["메디컬네트워크론"]
PRINCIPAL_KEYWORDS = ["메디칼론원금"]
INTEREST_KEYWORDS = ["메디칼론이자"]

DOW_KO = ["월", "화", "수", "목", "금", "토", "일"]

st.set_page_config(page_title="현금흐름 MVP", layout="wide")

# =========================================================
# Display helpers
# =========================================================
def fmt_int(x):
    if x is None:
        return ""
    try:
        if isinstance(x, (np.integer, int)):
            return f"{int(x):,}"
        if isinstance(x, (np.floating, float)):
            if math.isnan(x):
                return ""
            return f"{int(round(x, 0)):,}"
        if isinstance(x, str) and x.strip() == "":
            return ""
        return f"{int(float(x)):,}"
    except Exception:
        return str(x)

def parse_korean_dt_str(s):
    if pd.isna(s) or s is None:
        return pd.NaT
    ss = str(s).replace("오전", "AM").replace("오후", "PM")
    ss = re.sub(r"\s+", " ", ss).strip()
    return pd.to_datetime(ss, format="%Y/%m/%d %p %I:%M:%S", errors="coerce")

def parse_korean_ampm_series(s: pd.Series) -> pd.Series:
    def _one(x):
        if pd.isna(x):
            return pd.NaT
        x = str(x).replace("오전", "AM").replace("오후", "PM")
        x = re.sub(r"\s+", " ", x).strip()
        return pd.to_datetime(x, format="%Y/%m/%d %p %I:%M:%S", errors="coerce")
    return s.apply(_one)

def month_range(year: int, month: int):
    first = date(year, month, 1)
    last = date(year, month, calendar.monthrange(year, month)[1])
    return first, last

def make_tx_id(posted_at: pd.Timestamp, account_name: str, direction: str, amount: int, counterparty: str | None) -> str:
    base = f"{posted_at.isoformat()}|{account_name}|{direction}|{amount}|{counterparty or ''}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

# =========================================================
# Detect file types
# =========================================================
@st.cache_data(show_spinner=False)
def get_sheetnames(file_bytes: bytes) -> list[str]:
    wb = openpyxl.load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    return wb.sheetnames

def detect_template_workbook(file_bytes: bytes) -> bool:
    s = set(get_sheetnames(file_bytes))
    return ("이카운트 DB" in s) and ("월수입지출" in s)

def detect_balance_package(file_bytes: bytes) -> bool:
    s = set(get_sheetnames(file_bytes))
    return ("계좌별 잔액" in s) and ("메디칼론한도여유액" in s) and ("rawdata" in s)

# =========================================================
# Parsers
# =========================================================
@st.cache_data(show_spinner=False)
def parse_template_cashflow(file_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(file_bytes), sheet_name="이카운트 DB", engine="openpyxl", header=1)

    need = ["입/출금일자", "입/출", "변환금액", "계좌명", "입금처(출금처)", "원화잔액", "거래처코드"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"[템플릿] 컬럼 누락: {c} / 현재컬럼: {list(df.columns)}")

    df["posted_at"] = pd.to_datetime(df["입/출금일자"], errors="coerce")
    df = df[df["posted_at"].notna()].copy()

    df["amount"] = pd.to_numeric(df["변환금액"], errors="coerce")
    df = df[df["amount"].notna()].copy()
    df["amount"] = df["amount"].astype(int)

    df["direction"] = np.where(df["입/출"].astype(str).str.startswith("출금"), "OUT", "IN")

    df["account_name"] = df["계좌명"].astype(str)
    df["counterparty"] = df["입금처(출금처)"].astype(str)

    df["is_excluded_account"] = df["account_name"].isin(EXCLUDED_ACCOUNT_NAMES)
    df["balance"] = pd.to_numeric(df["원화잔액"], errors="coerce")
    df["biz_date"] = df["posted_at"].dt.date

    # 메디칼론 이벤트
    df["is_principal"] = (df["counterparty"].isin(PRINCIPAL_KEYWORDS)) & (df["direction"] == "OUT")
    df["is_interest"] = (df["counterparty"].isin(INTEREST_KEYWORDS)) & (df["direction"] == "OUT")
    df["is_drawdown"] = False  # 템플릿에는 보통 별도 표기가 없을 수 있어 false로 둠

    # subtype: 템플릿은 "입금(청구)" 같은 구분이 있을 수 있으니 최대한 반영
    df["subtype"] = np.where(df["입/출"].astype(str).str.contains("청구"), "CLAIM", "NORMAL")

    # 템플릿 "자금이동" 자동 표시
    df["is_internal_auto"] = df["거래처코드"].astype(str).eq("자금이동")

    df["tx_id"] = df.apply(
        lambda r: make_tx_id(r["posted_at"], r["account_name"], r["direction"], int(r["amount"]), r.get("counterparty")),
        axis=1
    )
    df["source"] = "TEMPLATE"

    return df[[
        "tx_id","posted_at","biz_date","account_name","direction","subtype","amount",
        "counterparty","balance","is_excluded_account","is_internal_auto",
        "is_principal","is_interest","is_drawdown","source"
    ]].copy()

@st.cache_data(show_spinner=False)
def read_ecount_export(file_bytes: bytes) -> pd.DataFrame:
    raw = pd.read_excel(BytesIO(file_bytes), engine="openpyxl", header=None).dropna(how="all")
    header_row_candidates = raw.index[
        raw.iloc[:, 0].astype(str).str.contains("입/출금일자", na=False)
    ]
    if len(header_row_candidates) == 0:
        raise ValueError(f"[ECOUNT] 헤더 행을 찾지 못했습니다. 첫 열 샘플: {raw.iloc[:5,0].tolist()}")
    header_row = int(header_row_candidates[0])

    df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl", header=header_row)
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    df = df.dropna(axis=1, how="all").dropna(how="all")

    need = ["입/출금일자","구분","계좌명","금액"]
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"[ECOUNT] 필수 컬럼 누락: {missing} / 현재컬럼: {list(df.columns)}")

    if df["입/출금일자"].dtype == object:
        dt_series = parse_korean_ampm_series(df["입/출금일자"])
    else:
        dt_series = pd.to_datetime(df["입/출금일자"], errors="coerce")

    df["posted_at"] = dt_series
    df = df[df["posted_at"].notna()].copy()

    df["direction"] = df["구분"].astype(str).map({"입금":"IN","출금":"OUT"})
    df = df[df["direction"].isin(["IN","OUT"])].copy()

    df["amount"] = pd.to_numeric(df["금액"], errors="coerce").fillna(0).astype(int)
    df = df[df["amount"] > 0].copy()

    df["account_name"] = df["계좌명"].astype(str)
    df["counterparty"] = df["입금처(출금처)"].astype(str) if "입금처(출금처)" in df.columns else ""
    df["balance"] = pd.to_numeric(df["원화잔액"], errors="coerce") if "원화잔액" in df.columns else np.nan

    df["is_excluded_account"] = df["account_name"].isin(EXCLUDED_ACCOUNT_NAMES)

    # 메디칼론 이벤트(기본)
    df["is_principal"] = (df["counterparty"].isin(PRINCIPAL_KEYWORDS)) & (df["direction"] == "OUT")
    df["is_interest"] = (df["counterparty"].isin(INTEREST_KEYWORDS)) & (df["direction"] == "OUT")
    df["is_drawdown"] = False  # drawdown은 balance_package에서 키워드로 잡는 방식이 더 안정적

    # 청구성 입금 간이 분류(채권 키워드)
    df["subtype"] = np.where(
        (df["direction"] == "IN") & (df["counterparty"].astype(str).str.contains("채권", na=False)),
        "CLAIM",
        "NORMAL"
    )

    df["is_internal_auto"] = False
    df["biz_date"] = df["posted_at"].dt.date

    df["tx_id"] = df.apply(
        lambda r: make_tx_id(r["posted_at"], r["account_name"], r["direction"], int(r["amount"]), r.get("counterparty")),
        axis=1
    )
    df["source"] = "ECOUNT_EXPORT"

    return df[[
        "tx_id","posted_at","biz_date","account_name","direction","subtype","amount",
        "counterparty","balance","is_excluded_account","is_internal_auto",
        "is_principal","is_interest","is_drawdown","source"
    ]].copy()

@st.cache_data(show_spinner=False)
def parse_balance_package(file_bytes: bytes) -> tuple[pd.DataFrame, dict]:
    # ---- rawdata sheet
    raw = pd.read_excel(BytesIO(file_bytes), sheet_name="rawdata", engine="openpyxl", header=None).dropna(how="all")
    header_row_candidates = raw.index[
        raw.iloc[:, 0].astype(str).str.contains("입/출금일자", na=False)
    ]
    if len(header_row_candidates) == 0:
        raise ValueError("[balance_package] rawdata 헤더를 찾지 못했습니다.")
    h = int(header_row_candidates[0])
    cols = raw.iloc[h].tolist()
    df = raw.iloc[h+1:].copy()
    df.columns = cols
    df = df.dropna(how="all")

    df["posted_at"] = parse_korean_ampm_series(df["입/출금일자"])
    df = df[df["posted_at"].notna()].copy()

    df["direction"] = df["구분"].astype(str).map({"입금":"IN","출금":"OUT"})
    df = df[df["direction"].isin(["IN","OUT"])].copy()

    df["amount"] = pd.to_numeric(df["금액"], errors="coerce").fillna(0).astype(int)
    df = df[df["amount"] > 0].copy()

    df["account_name"] = df["계좌명"].astype(str)
    df["counterparty"] = df["입금처(출금처)"].astype(str)
    df["balance"] = pd.to_numeric(df["원화잔액"], errors="coerce")

    df["is_excluded_account"] = df["account_name"].isin(EXCLUDED_ACCOUNT_NAMES)

    # 청구성 입금(채권)
    df["subtype"] = np.where(
        (df["direction"] == "IN") & (df["counterparty"].astype(str).str.contains("채권", na=False)),
        "CLAIM",
        "NORMAL"
    )

    # 메디칼론 이벤트(원금/이자)
    df["is_principal"] = (df["counterparty"].isin(PRINCIPAL_KEYWORDS)) & (df["direction"] == "OUT")
    df["is_interest"] = (df["counterparty"].isin(INTEREST_KEYWORDS)) & (df["direction"] == "OUT")
    df["is_drawdown"] = False  # drawdown은 키워드 입력으로 후처리

    df["is_internal_auto"] = False
    df["biz_date"] = df["posted_at"].dt.date

    df["tx_id"] = df.apply(
        lambda r: make_tx_id(r["posted_at"], r["account_name"], r["direction"], int(r["amount"]), r.get("counterparty")),
        axis=1
    )
    df["source"] = "BALANCE_PACKAGE"

    # ---- account snapshot
    acc = pd.read_excel(BytesIO(file_bytes), sheet_name="계좌별 잔액", engine="openpyxl")
    acc["snap_time"] = acc["최종성공일시"].apply(parse_korean_dt_str)
    cash_time = acc["snap_time"].max()

    acc["통장잔액_num"] = pd.to_numeric(acc["통장잔액"], errors="coerce")
    bank_bal = dict(zip(acc["계좌명"].astype(str), acc["통장잔액_num"]))

    # ---- loan snapshot
    loan = pd.read_excel(BytesIO(file_bytes), sheet_name="메디칼론한도여유액", engine="openpyxl")
    loan_time = pd.to_datetime(loan.loc[0, "조회시점"])
    loan_avail = int(pd.to_numeric(loan.loc[0, "한도여유액"], errors="coerce"))

    anchor = {
        "cash_time": cash_time,
        "bank_balances": bank_bal,
        "loan_time": loan_time,
        "loan_avail": loan_avail,
    }
    return df[[
        "tx_id","posted_at","biz_date","account_name","direction","subtype","amount",
        "counterparty","balance","is_excluded_account","is_internal_auto",
        "is_principal","is_interest","is_drawdown","source"
    ]].copy(), anchor

def parse_any_excel(file_bytes: bytes) -> tuple[pd.DataFrame, dict | None]:
    if detect_balance_package(file_bytes):
        df, anchor = parse_balance_package(file_bytes)
        return df, anchor
    if detect_template_workbook(file_bytes):
        return parse_template_cashflow(file_bytes), None
    return read_ecount_export(file_bytes), None

# =========================================================
# Internal transfer matching (optional, for readability)
# =========================================================
def build_internal_candidates(tx: pd.DataFrame) -> pd.DataFrame:
    base = tx[
        (~tx["is_excluded_account"]) &
        (~tx["is_principal"]) &
        (~tx["is_interest"])
    ].copy()

    outs = base[base["direction"] == "OUT"].sort_values("posted_at")
    ins  = base[base["direction"] == "IN"].sort_values("posted_at")

    ins_by_amount = {}
    for _, r in ins.iterrows():
        ins_by_amount.setdefault(int(r["amount"]), []).append(r)

    used_in = set()
    rows = []
    for _, o in outs.iterrows():
        pool = ins_by_amount.get(int(o["amount"]), [])
        best = None
        best_diff = None
        for i in pool:
            if i["tx_id"] in used_in:
                continue
            if i["account_name"] == o["account_name"]:
                continue
            diff = abs(i["posted_at"] - o["posted_at"])
            if diff <= INTERNAL_WINDOW:
                if best is None or diff < best_diff:
                    best, best_diff = i, diff
        if best is not None:
            used_in.add(best["tx_id"])
            rows.append({
                "out_tx_id": o["tx_id"],
                "in_tx_id": best["tx_id"],
                "amount": int(o["amount"]),
                "time_diff_seconds": int(best_diff.total_seconds()),
                "out_time": o["posted_at"],
                "out_account": o["account_name"],
                "in_time": best["posted_at"],
                "in_account": best["account_name"],
                "out_counterparty": o.get("counterparty",""),
                "in_counterparty": best.get("counterparty",""),
            })
    return pd.DataFrame(rows)

# =========================================================
# Anchor & balance math
# =========================================================
def balance_at_time_for_account(g: pd.DataFrame, t: pd.Timestamp):
    g = g[g["posted_at"] <= t].sort_values("posted_at")
    cur = None
    for _, r in g.iterrows():
        b = r["balance"]
        amt = int(r["amount"])
        if pd.notna(b):
            cur = float(b)
        else:
            if cur is not None:
                if r["direction"] == "IN":
                    cur += amt
                else:
                    cur -= amt
    return cur

def compute_anchor_cash_from_bank_and_tx(tx: pd.DataFrame, anchor: dict) -> tuple[float, list[str]]:
    """
    bank snapshot에서 병원범위 계좌 합산.
    통장잔액이 비어있는 계좌는 rawdata(거래내역)의 잔액으로 보정.
    그래도 없으면 0 처리 + missing 목록 리턴.
    """
    cash_time = anchor["cash_time"]
    bank_bal = anchor["bank_balances"]

    hospital_accounts = [a for a in bank_bal.keys() if a and (a not in EXCLUDED_ACCOUNT_NAMES)]
    missing = []
    total = 0.0

    for acct in hospital_accounts:
        v = bank_bal.get(acct)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            # tx 기반 보정
            g = tx[tx["account_name"] == acct]
            inferred = balance_at_time_for_account(g, cash_time) if len(g) else None
            if inferred is None:
                missing.append(acct)
                inferred = 0.0
            total += float(inferred)
        else:
            total += float(v)

    return total, missing

def cash_balance_from_anchor(tx: pd.DataFrame, anchor_time: pd.Timestamp, anchor_cash: float, target_time: pd.Timestamp,
                            confirmed_pairs: set[tuple[str,str]]) -> float:
    confirmed_out = {p[0] for p in confirmed_pairs}
    confirmed_in = {p[1] for p in confirmed_pairs}
    excluded_ids = confirmed_out.union(confirmed_in)

    # 병원 범위: excluded 계좌 제외
    base = tx[~tx["is_excluded_account"]].copy()

    # 내부이동(확정) 제외(가독성 목적이지만 net에는 영향 거의 없음)
    base = base[~base["tx_id"].isin(excluded_ids)]

    mask = (base["posted_at"] > target_time) & (base["posted_at"] <= anchor_time)
    seg = base.loc[mask, ["direction","amount"]].copy()
    seg["cash_delta"] = np.where(seg["direction"] == "IN", seg["amount"], -seg["amount"])
    net = float(seg["cash_delta"].sum())
    return float(anchor_cash) - net

def loan_avail_from_anchor(tx: pd.DataFrame, anchor_time: pd.Timestamp, anchor_loan_avail: float, target_time: pd.Timestamp,
                           drawdown_keywords: list[str]) -> float:
    """
    loan_delta:
      +원금상환(메디칼론원금 출금)
      -대출사용(키워드 포함 입금)
    """
    base = tx[~tx["is_excluded_account"]].copy()
    mask = (base["posted_at"] > target_time) & (base["posted_at"] <= anchor_time)
    seg = base.loc[mask, ["direction","amount","counterparty","is_principal"]].copy()

    # principal
    seg["loan_delta"] = 0
    seg.loc[seg["is_principal"] == True, "loan_delta"] = seg.loc[seg["is_principal"] == True, "amount"]

    # drawdown
    if drawdown_keywords:
        pat = "|".join([re.escape(k) for k in drawdown_keywords if k.strip()])
        if pat:
            is_dd = (seg["direction"] == "IN") & (seg["counterparty"].astype(str).str.contains(pat, na=False))
            # 원금/이자 키워드가 들어가면 drawdown에서 제외
            is_dd = is_dd & (~seg["counterparty"].astype(str).str.contains("원금|이자", na=False))
            seg.loc[is_dd, "loan_delta"] = -seg.loc[is_dd, "amount"]

    net = float(seg["loan_delta"].sum())
    return float(anchor_loan_avail) - net

def daily_aggregations(tx: pd.DataFrame, confirmed_pairs: set[tuple[str,str]], drawdown_keywords: list[str]) -> pd.DataFrame:
    """
    일자별:
      - 실제입금(청구/일반)
      - 실제출금(운영성)
      - 원금상환(메디칼론원금)
      - 대출사용(키워드 입금)
      - 대출여유액변동(원금상환 - 대출사용)
      - 현금순변동(입금-출금(운영+원금+이자))
    """
    confirmed_out = {p[0] for p in confirmed_pairs}
    confirmed_in = {p[1] for p in confirmed_pairs}
    excluded_ids = confirmed_out.union(confirmed_in)

    base = tx[~tx["is_excluded_account"]].copy()
    base = base[~base["tx_id"].isin(excluded_ids)].copy()

    # drawdown flag(후처리)
    dd = np.zeros(len(base), dtype=bool)
    if drawdown_keywords:
        pat = "|".join([re.escape(k) for k in drawdown_keywords if k.strip()])
        if pat:
            dd = (base["direction"] == "IN") & (base["counterparty"].astype(str).str.contains(pat, na=False))
            dd = dd & (~base["counterparty"].astype(str).str.contains("원금|이자", na=False))
    base["is_drawdown_calc"] = dd

    base["in_claim"] = np.where((base["direction"]=="IN") & (base["subtype"]=="CLAIM"), base["amount"], 0)
    base["in_normal"] = np.where((base["direction"]=="IN") & (base["subtype"]!="CLAIM"), base["amount"], 0)

    base["out_oper"] = np.where((base["direction"]=="OUT") & (~base["is_principal"]) & (~base["is_interest"]), base["amount"], 0)
    base["out_principal"] = np.where(base["is_principal"], base["amount"], 0)
    base["out_interest"] = np.where(base["is_interest"], base["amount"], 0)

    base["drawdown_in"] = np.where(base["is_drawdown_calc"], base["amount"], 0)

    # loan delta
    base["loan_delta"] = 0
    base.loc[base["is_principal"], "loan_delta"] = base.loc[base["is_principal"], "amount"]
    base.loc[base["is_drawdown_calc"], "loan_delta"] = -base.loc[base["is_drawdown_calc"], "amount"]

    g = base.groupby("biz_date").agg(
        actual_in_claim=("in_claim","sum"),
        actual_in=("in_normal","sum"),
        actual_out_oper=("out_oper","sum"),
        principal=("out_principal","sum"),
        interest=("out_interest","sum"),
        drawdown=("drawdown_in","sum"),
        loan_delta=("loan_delta","sum"),
    ).reset_index()

    g["total_in"] = g["actual_in_claim"] + g["actual_in"]
    g["total_out_cash"] = g["actual_out_oper"] + g["principal"] + g["interest"]
    g["cash_net"] = g["total_in"] - g["total_out_cash"]
    return g

def build_month_table(tx: pd.DataFrame, plan_df: pd.DataFrame, year: int, month: int,
                      anchor_info: dict | None,
                      confirmed_pairs: set[tuple[str,str]],
                      drawdown_keywords: list[str]) -> pd.DataFrame:
    first, last = month_range(year, month)
    days = pd.date_range(first, last, freq="D")

    # 예정
    if plan_df is None or len(plan_df) == 0:
        plan_map = {}
    else:
        p = plan_df.copy()
        p["biz_date"] = pd.to_datetime(p["date"], errors="coerce").dt.date
        p = p[p["biz_date"].notna()].copy()
        p["amount"] = pd.to_numeric(p["amount"], errors="coerce").fillna(0).astype(int)
        p["direction"] = p["direction"].astype(str).str.upper().replace({"입금":"IN","출금":"OUT"})
        p["plan_in"] = np.where(p["direction"]=="IN", p["amount"], 0)
        p["plan_out"] = np.where(p["direction"]=="OUT", p["amount"], 0)
        plan_map = p.groupby("biz_date").agg(plan_in=("plan_in","sum"), plan_out=("plan_out","sum")).to_dict("index")

    # 실제 집계
    daily = daily_aggregations(tx, confirmed_pairs, drawdown_keywords)
    daily_map = daily.set_index("biz_date").to_dict("index")

    rows = []

    # 앵커 기반 시작값(현금/메디칼론)
    if anchor_info is not None:
        anchor_time = anchor_info["anchor_time"]
        anchor_cash = anchor_info["anchor_cash"]
        anchor_loan = anchor_info["anchor_loan_avail"]

        # 월 시작(해당월 1일 00:00)
        month_start_ts = pd.Timestamp(datetime.combine(first, datetime.min.time()))
        cash_start = cash_balance_from_anchor(tx, anchor_time, anchor_cash, month_start_ts, confirmed_pairs)
        loan_start = loan_avail_from_anchor(tx, anchor_time, anchor_loan, month_start_ts, drawdown_keywords)
    else:
        cash_start = 0.0
        loan_start = 0.0

    cash = cash_start
    loan_avail = loan_start

    for d in days:
        biz = d.date()
        a = daily_map.get(biz, {
            "actual_in_claim":0,"actual_in":0,"actual_out_oper":0,"principal":0,"interest":0,
            "drawdown":0,"loan_delta":0,"total_in":0,"total_out_cash":0,"cash_net":0
        })
        p = plan_map.get(biz, {"plan_in":0,"plan_out":0})

        cash_start_day = cash
        loan_start_day = loan_avail
        total_start = cash_start_day + loan_start_day

        # cash end = cash + 실제 cash_net + 예정(in-out)
        cash_change = float(a["cash_net"]) + float(p["plan_in"]) - float(p["plan_out"])
        cash_end = cash_start_day + cash_change

        # loan end = loan + loan_delta(원금 - 대출사용)
        loan_end = loan_start_day + float(a["loan_delta"])

        total_end = cash_end + loan_end

        rows.append({
            "date": biz,
            "dow": DOW_KO[d.weekday()],
            "cash_start": int(round(cash_start_day, 0)),
            "loan_avail_start": int(round(loan_start_day, 0)),
            "total_start": int(round(total_start, 0)),

            "actual_in_claim": int(a["actual_in_claim"]),
            "actual_in": int(a["actual_in"]),
            "actual_out_oper": int(a["actual_out_oper"]),
            "principal": int(a["principal"]),
            "interest": int(a["interest"]),
            "drawdown": int(a["drawdown"]),
            "plan_in": int(p["plan_in"]),
            "plan_out": int(p["plan_out"]),

            "cash_change": int(round(cash_change, 0)),
            "loan_change": int(a["loan_delta"]),
            "total_change": int(round((cash_change + float(a["loan_delta"])), 0)),

            "cash_end": int(round(cash_end, 0)),
            "loan_avail_end": int(round(loan_end, 0)),
            "total_end": int(round(total_end, 0)),
        })

        cash = cash_end
        loan_avail = loan_end

    return pd.DataFrame(rows)

# =========================================================
# UI
# =========================================================
st.title("현금흐름 MVP (업로드 → 내부이동 검수 → 월수입지출/가용금액)")

st.sidebar.header("데이터 업로드")
files = st.sidebar.file_uploader(
    "엑셀 업로드 (여러개 가능) - 잔액정렬data.xlsx도 가능",
    type=["xlsx"],
    accept_multiple_files=True
)

if not files:
    st.info("좌측에서 엑셀 파일을 업로드하세요.")
    st.stop()

# drawdown keyword UI
st.sidebar.subheader("메디칼론 대출사용(인출) 키워드")
dd_kw_text = st.sidebar.text_input(
    "입금처(출금처)에 포함된 키워드(콤마로 구분)",
    value=",".join(DEFAULT_DRAWDOWN_KEYWORDS)
)
drawdown_keywords = [x.strip() for x in dd_kw_text.split(",") if x.strip()]

all_tx = []
anchors = []
parse_errors = []

for f in files:
    b = f.getvalue()
    try:
        tx_one, anchor = parse_any_excel(b)
        tx_one["file_name"] = f.name
        all_tx.append(tx_one)
        if anchor is not None:
            anchors.append(anchor)
    except Exception as e:
        parse_errors.append((f.name, str(e)))

if parse_errors:
    st.error("일부 파일 파싱 실패")
    for fn, msg in parse_errors:
        st.write(f"- {fn}: {msg}")
    st.stop()

tx = pd.concat(all_tx, ignore_index=True).drop_duplicates(subset=["tx_id"])
tx = tx.sort_values("posted_at")

min_dt = tx["posted_at"].min()
max_dt = tx["posted_at"].max()

if "confirmed_pairs" not in st.session_state:
    st.session_state.confirmed_pairs = set()

# ---- anchor resolution
anchor_info = None
if anchors:
    # 가장 최신 loan_time 기준
    anchors_sorted = sorted(anchors, key=lambda a: a["loan_time"])
    a = anchors_sorted[-1]

    bank_cash_total, missing_accounts = compute_anchor_cash_from_bank_and_tx(tx, a)

    # cash_time과 loan_time 차이가 있으면 cash 총합을 loan_time으로 정렬(총현금은 net flow로만 변하므로 단순 조정 가능)
    cash_time = a["cash_time"]
    loan_time = a["loan_time"]
    # 병원 범위 tx만
    base = tx[~tx["is_excluded_account"]].copy()
    seg = base[(base["posted_at"] > cash_time) & (base["posted_at"] <= loan_time)].copy()
    seg["cash_delta"] = np.where(seg["direction"]=="IN", seg["amount"], -seg["amount"])
    cash_adjust = float(seg["cash_delta"].sum())
    anchor_cash_at_loan = float(bank_cash_total + cash_adjust)

    anchor_info = {
        "anchor_time": loan_time,
        "anchor_cash": anchor_cash_at_loan,
        "anchor_loan_avail": float(a["loan_avail"]),
        "missing_accounts": missing_accounts,
        "cash_time": cash_time,
        "loan_time": loan_time,
    }

# ---- top metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("거래기간", f"{min_dt} ~ {max_dt}")
c2.metric("전체 거래(중복제거)", f"{len(tx):,}")
c3.metric("제외계좌 건수", f"{int(tx['is_excluded_account'].sum()):,}")
c4.metric("확정 내부이동(세션)", f"{len(st.session_state.confirmed_pairs):,}쌍")

page = st.sidebar.radio("페이지", ["월수입지출(핵심)", "내부이동 검수", "일자별 요약", "원장(거래 목록)"])

# =========================================================
# Page: Internal transfer review
# =========================================================
if page == "내부이동 검수":
    st.header("내부이동 후보 검수")
    cand = build_internal_candidates(tx)

    if cand.empty:
        st.info("내부이동 후보가 없습니다.")
    else:
        st.write(f"후보 {len(cand):,}쌍 (체크하면 내부이동으로 확정 → 집계에서 제외)")

        for _, r in cand.sort_values("time_diff_seconds").iterrows():
            key = (r["out_tx_id"], r["in_tx_id"])
            default = key in st.session_state.confirmed_pairs

            left, mid, right = st.columns([7,7,2])
            with left:
                st.markdown(f"**OUT** {r['out_time']} / {r['out_account']} / {fmt_int(r['amount'])}")
                if r.get("out_counterparty"):
                    st.caption(f"{r['out_counterparty']}")
            with mid:
                st.markdown(f"**IN**  {r['in_time']} / {r['in_account']} / {fmt_int(r['amount'])}")
                if r.get("in_counterparty"):
                    st.caption(f"{r['in_counterparty']}")
                st.caption(f"time diff: {int(r['time_diff_seconds'])}s")
            with right:
                checked = st.checkbox("확정", value=default, key=f"it_{r['out_tx_id']}_{r['in_tx_id']}")
                if checked:
                    st.session_state.confirmed_pairs.add(key)
                else:
                    st.session_state.confirmed_pairs.discard(key)

# =========================================================
# Page: Daily summary
# =========================================================
elif page == "일자별 요약":
    st.header("일자별 요약")
    daily = daily_aggregations(tx, st.session_state.confirmed_pairs, drawdown_keywords).sort_values("biz_date")

    if daily.empty:
        st.info("데이터가 없습니다.")
    else:
        disp = daily.copy()
        disp["날짜"] = disp["biz_date"].astype(str)
        for col in ["actual_in_claim","actual_in","actual_out_oper","principal","interest","drawdown","loan_delta","cash_net"]:
            disp[col] = disp[col].map(fmt_int)
        st.dataframe(
            disp[["날짜","actual_in_claim","actual_in","actual_out_oper","principal","interest","drawdown","loan_delta","cash_net"]],
            use_container_width=True,
            hide_index=True
        )

# =========================================================
# Page: Monthly cashflow core
# =========================================================
elif page == "월수입지출(핵심)":
    st.header("월수입지출 (현금 + 메디칼론 + Total 가용금액)")

    # anchor box
    if anchor_info is not None:
        cash = anchor_info["anchor_cash"]
        loan_av = anchor_info["anchor_loan_avail"]
        total = cash + loan_av

        b1, b2, b3, b4 = st.columns(4)
        b1.metric("앵커 시점", str(anchor_info["anchor_time"]))
        b2.metric("현금(병원 범위)", fmt_int(cash))
        b3.metric("메디칼론 여유액", fmt_int(loan_av))
        b4.metric("Total 가용금액", fmt_int(total))

        if anchor_info["missing_accounts"]:
            st.warning(f"계좌별 잔액에서 공란(자동 0 처리)된 계좌: {', '.join(anchor_info['missing_accounts'])}")
    else:
        st.info("잔액정렬data.xlsx(계좌별 잔액 + 메디칼론한도여유액 + rawdata)가 업로드되면 앵커 기반 정합이 자동으로 활성화됩니다.")

    # month selector
    months = sorted({(d.year, d.month) for d in pd.to_datetime(tx["biz_date"]).dt.to_pydatetime()})
    month_labels = [f"{y}-{m:02d}" for y,m in months]
    sel = st.selectbox("월 선택", month_labels, index=len(month_labels)-1 if month_labels else 0)
    year, month = map(int, sel.split("-"))

    if "plan_df" not in st.session_state:
        st.session_state.plan_df = pd.DataFrame(columns=["date","direction","amount","label"])

    st.subheader("예정 입력 (베타: 세션 저장 / CSV 다운로드)")
    st.session_state.plan_df = st.data_editor(
        st.session_state.plan_df,
        use_container_width=True,
        num_rows="dynamic",
        column_config={
            "date": st.column_config.DateColumn("날짜"),
            "direction": st.column_config.SelectboxColumn("구분", options=["IN","OUT","입금","출금"]),
            "amount": st.column_config.NumberColumn("금액"),
            "label": st.column_config.TextColumn("항목/메모"),
        }
    )

    table = build_month_table(
        tx=tx,
        plan_df=st.session_state.plan_df,
        year=year,
        month=month,
        anchor_info=anchor_info,
        confirmed_pairs=st.session_state.confirmed_pairs,
        drawdown_keywords=drawdown_keywords
    )

    st.subheader("일자별 롤링표 (현금/메디칼론/Total)")
    disp = table.copy()
    disp["날짜"] = disp["date"].astype(str)
    disp["요일"] = disp["dow"]

    # 숫자 포맷(문자열로)
    num_cols = [
        "cash_start","loan_avail_start","total_start",
        "actual_in_claim","actual_in","actual_out_oper","principal","interest","drawdown","plan_in","plan_out",
        "cash_change","loan_change","total_change",
        "cash_end","loan_avail_end","total_end"
    ]
    for c in num_cols:
        disp[c] = disp[c].map(fmt_int)

    show_cols = [
        "날짜","요일",
        "cash_start","loan_avail_start","total_start",
        "actual_in_claim","actual_in","actual_out_oper","principal","interest","drawdown",
        "plan_in","plan_out",
        "cash_change","loan_change","total_change",
        "cash_end","loan_avail_end","total_end"
    ]
    st.dataframe(disp[show_cols], use_container_width=True, hide_index=True)

    st.download_button(
        "월수입지출 CSV 다운로드",
        data=table.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"monthly_available_{year}-{month:02d}.csv",
        mime="text/csv"
    )

    st.subheader("일자 상세(거래 + 메디칼론 이벤트 마킹)")
    first, _ = month_range(year, month)
    sel_date = st.date_input("조회할 날짜", value=first)

    confirmed_out = {p[0] for p in st.session_state.confirmed_pairs}
    confirmed_in = {p[1] for p in st.session_state.confirmed_pairs}
    excluded_ids = confirmed_out.union(confirmed_in)

    day_tx = tx[(tx["biz_date"] == sel_date) & (~tx["is_excluded_account"]) & (~tx["tx_id"].isin(excluded_ids))].copy()
    if day_tx.empty:
        st.info("해당 날짜 거래가 없습니다.")
    else:
        # drawdown 마킹
        if drawdown_keywords:
            pat = "|".join([re.escape(k) for k in drawdown_keywords if k.strip()])
            is_dd = (day_tx["direction"]=="IN") & (day_tx["counterparty"].astype(str).str.contains(pat, na=False))
            is_dd = is_dd & (~day_tx["counterparty"].astype(str).str.contains("원금|이자", na=False))
            day_tx["is_drawdown_calc"] = is_dd
        else:
            day_tx["is_drawdown_calc"] = False

        def loan_tag(r):
            if r["is_principal"]:
                return "원금상환(+여유액)"
            if r["is_drawdown_calc"]:
                return "대출사용(-여유액)"
            if r["is_interest"]:
                return "이자(현금↓)"
            return ""

        day_tx["loan_event"] = day_tx.apply(loan_tag, axis=1)

        disp2 = day_tx.sort_values("posted_at")[["posted_at","account_name","direction","subtype","amount","counterparty","loan_event","source","file_name"]].copy()
        disp2["시간"] = disp2["posted_at"].astype(str)
        disp2["금액"] = disp2["amount"].map(fmt_int)

        st.dataframe(
            disp2[["시간","account_name","direction","subtype","금액","counterparty","loan_event","source","file_name"]],
            use_container_width=True,
            hide_index=True
        )

# =========================================================
# Page: Ledger
# =========================================================
else:
    st.header("원장(거래 목록)")
    q = st.text_input("검색(계좌/상대/파일명)", value="")
    df = tx.copy()

    if q.strip():
        mask = (
            df["account_name"].astype(str).str.contains(q, na=False) |
            df["counterparty"].astype(str).str.contains(q, na=False) |
            df["file_name"].astype(str).str.contains(q, na=False)
        )
        df = df[mask]

    df = df.sort_values("posted_at", ascending=False).head(800)

    # drawdown tag
    if drawdown_keywords:
        pat = "|".join([re.escape(k) for k in drawdown_keywords if k.strip()])
        is_dd = (df["direction"]=="IN") & (df["counterparty"].astype(str).str.contains(pat, na=False))
        is_dd = is_dd & (~df["counterparty"].astype(str).str.contains("원금|이자", na=False))
        df["is_drawdown_calc"] = is_dd
    else:
        df["is_drawdown_calc"] = False

    def loan_tag(r):
        if r["is_principal"]:
            return "원금상환"
        if r["is_drawdown_calc"]:
            return "대출사용"
        if r["is_interest"]:
            return "이자"
        return ""

    df["loan_event"] = df.apply(loan_tag, axis=1)

    disp = df[["posted_at","biz_date","account_name","direction","subtype","amount","counterparty","balance","loan_event","source","file_name"]].copy()
    disp["일시"] = disp["posted_at"].astype(str)
    disp["일자"] = disp["biz_date"].astype(str)
    disp["금액"] = disp["amount"].map(fmt_int)
    disp["잔액"] = disp["balance"].map(fmt_int)

    st.dataframe(
        disp[["일시","일자","account_name","direction","subtype","금액","counterparty","잔액","loan_event","source","file_name"]],
        use_container_width=True,
        hide_index=True
    )
