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

# =========================================================
# Config
# =========================================================
EXCLUDED_ACCOUNT_NAMES = {"신한_에셀", "하나_꾸러기건식"}  # 병원 집계에서 제외
INTERNAL_WINDOW = timedelta(hours=2)
PRINCIPAL_KEYWORDS = {"메디칼론원금"}  # 원금상환(지출 아님) 제외

DOW_KO = ["월", "화", "수", "목", "금", "토", "일"]

st.set_page_config(page_title="현금흐름 MVP", layout="wide")

# =========================================================
# Display helpers (Streamlit format 에러 회피용)
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

def fmt_dt(x):
    if pd.isna(x) or x is None:
        return ""
    if isinstance(x, (datetime, pd.Timestamp)):
        return str(x)
    return str(x)

def month_range(year: int, month: int):
    first = date(year, month, 1)
    last = date(year, month, calendar.monthrange(year, month)[1])
    return first, last

# =========================================================
# Utils
# =========================================================
def safe_to_datetime_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def parse_korean_ampm_series(s: pd.Series) -> pd.Series:
    """
    '2026/01/02 오전  5:45:01' -> Timestamp
    """
    def _one(x):
        if pd.isna(x):
            return pd.NaT
        x = str(x)
        x = x.replace("오전", "AM").replace("오후", "PM")
        x = re.sub(r"\s+", " ", x).strip()
        return pd.to_datetime(x, format="%Y/%m/%d %p %I:%M:%S", errors="coerce")
    return s.apply(_one)

def make_tx_id(posted_at: pd.Timestamp, account_name: str, direction: str, amount: int, counterparty: str | None) -> str:
    base = f"{posted_at.isoformat()}|{account_name}|{direction}|{amount}|{counterparty or ''}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

@st.cache_data(show_spinner=False)
def detect_template_workbook(file_bytes: bytes) -> bool:
    wb = openpyxl.load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    return "이카운트 DB" in wb.sheetnames and "월수입지출" in wb.sheetnames

@st.cache_data(show_spinner=False)
def parse_template_available_map(file_bytes: bytes) -> dict:
    """
    템플릿 '월수입지출' 시트에서 날짜별 '가용가능금액'(I열=9)을 추출.
    - A열 날짜가 비어있는 행은 직전 날짜의 연속 라인으로 간주(엑셀 구조 그대로)
    - 날짜별 마지막 가용가능금액을 저장
    """
    wb = openpyxl.load_workbook(BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb["월수입지출"]

    date_col = 1  # A
    avail_col = 9 # I (가용가능금액)
    current_date = None
    last_by_date = {}

    for r in range(1, ws.max_row + 1):
        d = ws.cell(r, date_col).value

        if isinstance(d, datetime):
            current_date = d.date()
        elif isinstance(d, date):
            current_date = d

        v = ws.cell(r, avail_col).value
        if current_date and isinstance(v, (int, float)) and not (isinstance(v, float) and math.isnan(v)):
            last_by_date[current_date] = int(v)

    return last_by_date

# =========================================================
# Parsers
# =========================================================
@st.cache_data(show_spinner=False)
def parse_template_cashflow(file_bytes: bytes) -> pd.DataFrame:
    """
    '히즈메디 현금흐름- 원본본.xlsx' 템플릿 파일 파싱
    - sheet: '이카운트 DB'
    - 사용 컬럼: 입/출금일자, 입/출, 변환금액, 계좌명, 입금처(출금처), 원화잔액, 거래처코드
    """
    df = pd.read_excel(BytesIO(file_bytes), sheet_name="이카운트 DB", engine="openpyxl", header=1)

    need = ["입/출금일자", "입/출", "변환금액", "계좌명", "입금처(출금처)", "원화잔액", "거래처코드"]
    for c in need:
        if c not in df.columns:
            raise ValueError(f"[템플릿] 컬럼 누락: {c} / 현재컬럼: {list(df.columns)}")

    df["posted_at"] = safe_to_datetime_series(df["입/출금일자"])
    df = df[df["posted_at"].notna()].copy()

    # 템플릿의 '변환금액'은 이미 내부이동/원금상환 제외 로직이 들어간 값으로 보임(엑셀 베이스라인)
    df["amount"] = pd.to_numeric(df["변환금액"], errors="coerce")
    df = df[df["amount"].notna()].copy()
    df["amount"] = df["amount"].astype(int)

    df["subtype"] = np.where(df["입/출"].astype(str).str.contains("청구"), "CLAIM", "NORMAL")
    df["direction"] = np.where(df["입/출"].astype(str).str.startswith("출금"), "OUT", "IN")

    df["account_name"] = df["계좌명"].astype(str)
    df["counterparty"] = df["입금처(출금처)"].astype(str)

    df["is_excluded_account"] = df["account_name"].isin(EXCLUDED_ACCOUNT_NAMES)
    df["is_principal"] = df["counterparty"].isin(PRINCIPAL_KEYWORDS)
    df["is_internal_auto"] = df["거래처코드"].astype(str).eq("자금이동")

    df["balance"] = pd.to_numeric(df["원화잔액"], errors="coerce")
    df["biz_date"] = df["posted_at"].dt.date

    df["tx_id"] = df.apply(
        lambda r: make_tx_id(r["posted_at"], r["account_name"], r["direction"], int(r["amount"]), r.get("counterparty")),
        axis=1
    )
    df["source"] = "TEMPLATE"

    return df[[
        "tx_id","posted_at","biz_date","account_name","direction","subtype","amount",
        "counterparty","balance","is_excluded_account","is_principal","is_internal_auto","source"
    ]].copy()

@st.cache_data(show_spinner=False)
def read_ecount_export(file_bytes: bytes) -> pd.DataFrame:
    """
    ECOUNT 계좌연동 raw export (EBS003M...) 파싱
    - 헤더행 자동 탐지(첫 컬럼에 '입/출금일자' 있는 행)
    - 한글 오전/오후 시간 파싱
    """
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
    counterparty_col = "입금처(출금처)" if "입금처(출금처)" in df.columns else None
    df["counterparty"] = df[counterparty_col].astype(str) if counterparty_col else ""

    balance_col = "원화잔액" if "원화잔액" in df.columns else None
    df["balance"] = pd.to_numeric(df[balance_col], errors="coerce") if balance_col else np.nan

    df["is_excluded_account"] = df["account_name"].isin(EXCLUDED_ACCOUNT_NAMES)
    df["is_principal"] = df["counterparty"].isin(PRINCIPAL_KEYWORDS)
    df["is_internal_auto"] = False
    df["subtype"] = "NORMAL"

    df["biz_date"] = df["posted_at"].dt.date

    df["tx_id"] = df.apply(
        lambda r: make_tx_id(r["posted_at"], r["account_name"], r["direction"], int(r["amount"]), r.get("counterparty")),
        axis=1
    )
    df["source"] = "ECOUNT_EXPORT"

    return df[[
        "tx_id","posted_at","biz_date","account_name","direction","subtype","amount",
        "counterparty","balance","is_excluded_account","is_principal","is_internal_auto","source"
    ]].copy()

def parse_any_excel(file_bytes: bytes) -> pd.DataFrame:
    if detect_template_workbook(file_bytes):
        return parse_template_cashflow(file_bytes)
    return read_ecount_export(file_bytes)

# =========================================================
# Internal transfer candidate matching
# =========================================================
def build_internal_candidates(tx: pd.DataFrame) -> pd.DataFrame:
    base = tx[
        (~tx["is_excluded_account"]) &
        (~tx["is_principal"])
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
# Balance computation (fallback)
# =========================================================
def compute_total_balance_at(tx: pd.DataFrame, at_dt: datetime) -> float | None:
    base = tx[(~tx["is_excluded_account"])].copy()
    if base["balance"].notna().sum() == 0:
        return None

    total = 0.0
    for acct, g in base.groupby("account_name"):
        g = g.sort_values("posted_at")
        before = g[g["posted_at"] <= at_dt]
        if len(before) > 0 and pd.notna(before.iloc[-1]["balance"]):
            total += float(before.iloc[-1]["balance"])
            continue

        first = g.iloc[0]
        if pd.notna(first["balance"]):
            if first["direction"] == "IN":
                pre = float(first["balance"]) - float(first["amount"])
            else:
                pre = float(first["balance"]) + float(first["amount"])
            total += pre
        else:
            total += 0.0

    return total

# =========================================================
# Aggregations
# =========================================================
def daily_actuals(tx: pd.DataFrame, confirmed_pairs: set[tuple[str,str]]) -> pd.DataFrame:
    confirmed_out = {p[0] for p in confirmed_pairs}
    confirmed_in = {p[1] for p in confirmed_pairs}
    excluded_ids = confirmed_out.union(confirmed_in)

    base = tx[
        (~tx["is_excluded_account"]) &
        (~tx["is_principal"]) &
        (~tx["is_internal_auto"]) &
        (~tx["tx_id"].isin(excluded_ids))
    ].copy()

    base["inflow_claim"] = np.where((base["direction"]=="IN") & (base["subtype"]=="CLAIM"), base["amount"], 0)
    base["inflow_normal"] = np.where((base["direction"]=="IN") & (base["subtype"]!="CLAIM"), base["amount"], 0)
    base["outflow"] = np.where((base["direction"]=="OUT"), base["amount"], 0)

    g = base.groupby("biz_date").agg(
        inflow_claim=("inflow_claim","sum"),
        inflow=("inflow_normal","sum"),
        outflow=("outflow","sum"),
    ).reset_index()
    g["total_inflow"] = g["inflow_claim"] + g["inflow"]
    g["net"] = g["total_inflow"] - g["outflow"]
    return g

def plan_daily(plan_df: pd.DataFrame) -> pd.DataFrame:
    if plan_df is None or len(plan_df)==0:
        return pd.DataFrame(columns=["biz_date","plan_in","plan_out"])
    p = plan_df.copy()
    p["biz_date"] = pd.to_datetime(p["date"], errors="coerce").dt.date
    p = p[p["biz_date"].notna()].copy()
    p["amount"] = pd.to_numeric(p["amount"], errors="coerce").fillna(0).astype(int)
    p["direction"] = p["direction"].astype(str).str.upper().replace({"입금":"IN","출금":"OUT"})
    p["plan_in"] = np.where(p["direction"]=="IN", p["amount"], 0)
    p["plan_out"] = np.where(p["direction"]=="OUT", p["amount"], 0)
    g = p.groupby("biz_date").agg(plan_in=("plan_in","sum"), plan_out=("plan_out","sum")).reset_index()
    return g

def build_month_table(tx: pd.DataFrame, plan_df: pd.DataFrame, year: int, month: int, start_balance: float, confirmed_pairs: set[tuple[str,str]]) -> pd.DataFrame:
    first, last = month_range(year, month)
    days = pd.date_range(first, last, freq="D")

    act = daily_actuals(tx, confirmed_pairs)
    act_map = act.set_index("biz_date").to_dict("index")

    plan = plan_daily(plan_df)
    plan_map = plan.set_index("biz_date").to_dict("index")

    rows = []
    bal = float(start_balance)

    for d in days:
        biz = d.date()
        a = act_map.get(biz, {"inflow_claim":0,"inflow":0,"outflow":0,"total_inflow":0,"net":0})
        p = plan_map.get(biz, {"plan_in":0,"plan_out":0})

        start = bal
        net = float(a["total_inflow"]) - float(a["outflow"]) + float(p["plan_in"]) - float(p["plan_out"])
        end = start + net

        rows.append({
            "date": biz,
            "dow": DOW_KO[d.weekday()],
            "start_balance": int(round(start, 0)),
            "actual_in_claim": int(a["inflow_claim"]),
            "actual_in": int(a["inflow"]),
            "actual_out": int(a["outflow"]),
            "plan_in": int(p["plan_in"]),
            "plan_out": int(p["plan_out"]),
            "net_change": int(round(net, 0)),
            "end_balance": int(round(end, 0)),
        })
        bal = end

    return pd.DataFrame(rows)

# =========================================================
# UI
# =========================================================
st.title("현금흐름 MVP (업로드 → 내부이동 검수 → 월수입지출)")

st.sidebar.header("데이터 업로드")
files = st.sidebar.file_uploader(
    "엑셀 업로드 (현금흐름 원본 or ECOUNT export) / 여러개 가능",
    type=["xlsx"],
    accept_multiple_files=True
)

if not files:
    st.info("좌측에서 엑셀 파일을 업로드하세요. (현금흐름 원본 엑셀 또는 ECOUNT export)")
    st.stop()

all_tx = []
parse_errors = []
available_maps = []  # 템플릿 월수입지출(가용가능금액) 맵들

for f in files:
    b = f.getvalue()
    try:
        if detect_template_workbook(b):
            available_maps.append(parse_template_available_map(b))

        tx_one = parse_any_excel(b)
        tx_one["file_name"] = f.name
        all_tx.append(tx_one)
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

c1, c2, c3, c4 = st.columns(4)
c1.metric("거래기간", f"{min_dt.date()} ~ {max_dt.date()}")
c2.metric("전체 거래(중복제거)", f"{len(tx):,}")
c3.metric("제외계좌 건수", f"{int(tx['is_excluded_account'].sum()):,}")
c4.metric("원금상환(제외) 건수", f"{int(tx['is_principal'].sum()):,}")

page = st.sidebar.radio("페이지", ["월수입지출(핵심)", "내부이동 검수", "일자별 요약(피벗 대체)", "원장(거래 목록)"])

# =========================================================
# Page: Internal transfer review
# =========================================================
if page == "내부이동 검수":
    st.header("내부이동 후보 검수")

    cand = build_internal_candidates(tx)
    auto_cnt = int((~tx["is_excluded_account"] & tx["is_internal_auto"]).sum())
    st.caption(f"자동 제외(템플릿의 '자금이동' 표시) 거래: {auto_cnt:,}건")

    if cand.empty:
        st.info("내부이동 후보가 없습니다.")
    else:
        st.write(f"후보 {len(cand):,}쌍 (체크하면 내부이동으로 확정 → 집계/월수입지출에서 제외)")

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

        conf = cand[cand.apply(lambda x: (x["out_tx_id"], x["in_tx_id"]) in st.session_state.confirmed_pairs, axis=1)].copy()
        st.download_button(
            "확정 내부이동 CSV 다운로드",
            data=conf.to_csv(index=False).encode("utf-8-sig"),
            file_name="internal_transfers_confirmed.csv",
            mime="text/csv"
        )

# =========================================================
# Page: Daily summary (pivot replacement)
# =========================================================
elif page == "일자별 요약(피벗 대체)":
    st.header("일자별 요약 (피벗 대체)")

    months = sorted({(d.year, d.month) for d in pd.to_datetime(tx["biz_date"]).dt.to_pydatetime()})
    month_labels = [f"{y}-{m:02d}" for y,m in months]
    sel = st.selectbox("월 선택", month_labels, index=len(month_labels)-1)
    y, m = map(int, sel.split("-"))
    first, last = month_range(y, m)

    act = daily_actuals(tx, st.session_state.confirmed_pairs)
    act = act[(act["biz_date"] >= first) & (act["biz_date"] <= last)].copy()

    if act.empty:
        st.info("해당 월 데이터가 없습니다.")
    else:
        show_claim = st.toggle("심사청구(입금(청구)) 분리해서 보기", value=True)
        act = act.sort_values("biz_date")

        disp = act.copy()
        disp["날짜"] = disp["biz_date"].astype(str)

        if show_claim:
            disp["입금(청구)"] = disp["inflow_claim"].map(fmt_int)
            disp["입금"] = disp["inflow"].map(fmt_int)
            disp["출금"] = disp["outflow"].map(fmt_int)
            disp["총입금"] = disp["total_inflow"].map(fmt_int)
            disp["순증"] = disp["net"].map(fmt_int)
            out = disp[["날짜","입금(청구)","입금","출금","총입금","순증"]]
        else:
            disp["총입금"] = disp["total_inflow"].map(fmt_int)
            disp["출금"] = disp["outflow"].map(fmt_int)
            disp["순증"] = disp["net"].map(fmt_int)
            out = disp[["날짜","총입금","출금","순증"]]

        st.dataframe(out, use_container_width=True, hide_index=True)

# =========================================================
# Page: Monthly cashflow (core)
# =========================================================
elif page == "월수입지출(핵심)":
    st.header("월수입지출 (예정 + 실제 + 잔액 롤링)")

    months = sorted({(d.year, d.month) for d in pd.to_datetime(tx["biz_date"]).dt.to_pydatetime()})
    month_labels = [f"{y}-{m:02d}" for y,m in months]
    default_idx = len(month_labels)-1 if month_labels else 0
    sel = st.selectbox("월 선택", month_labels, index=default_idx)
    year, month = map(int, sel.split("-"))
    first, last = month_range(year, month)

    # ---- 시작잔고 자동값 후보 1: 템플릿 월수입지출(I열 가용가능금액 기반)
    template_auto = None
    template_source = None
    if available_maps:
        # 여러 템플릿이 올라올 수 있으니 "가장 정보가 많은 맵" 우선 사용
        best_map = max(available_maps, key=lambda m: len(m))
        prev = first - timedelta(days=1)
        if prev in best_map:
            template_auto = best_map[prev]
            template_source = f"템플릿 월수입지출(전일 {prev})"
        elif first in best_map:
            template_auto = best_map[first]
            template_source = f"템플릿 월수입지출(당일 {first})"

    # ---- 시작잔고 자동값 후보 2: 원장 balance 컬럼 기반 추정(템플릿 없을 때 fallback)
    prev_day_end = datetime.combine(first - timedelta(days=1), datetime.max.time())
    balance_auto = compute_total_balance_at(tx, prev_day_end)

    st.caption("시작잔고는 템플릿 파일이 있으면 '월수입지출' 가용가능금액을 우선 사용합니다. 없으면 원장 잔액 합계를 추정하거나 수동 입력합니다.")

    left, right = st.columns([3,2])
    with left:
        if template_auto is not None:
            st.success(f"시작잔고 자동값: {fmt_int(template_auto)}  ({template_source})")
            start_balance = st.number_input("시작잔고(수정 가능)", value=int(template_auto), step=1_000_000, format="%d")
        elif balance_auto is not None:
            st.info(f"시작잔고 추정값: {fmt_int(balance_auto)}  (원장 잔액 합계 추정)")
            start_balance = st.number_input("시작잔고(수정 가능)", value=int(balance_auto), step=1_000_000, format="%d")
        else:
            start_balance = st.number_input("시작잔고(수동 입력)", value=0, step=1_000_000, format="%d")

    with right:
        danger = st.number_input("경고 기준 잔액(이하)", value=0, step=10_000_000, format="%d")

    if "plan_df" not in st.session_state:
        st.session_state.plan_df = pd.DataFrame(columns=["date","direction","amount","label"])

    st.subheader("예정 입력 (베타: 세션 저장 / CSV로 보관)")
    colA, colB = st.columns([2,3])

    with colA:
        plan_upload = st.file_uploader("예정표 CSV 업로드(선택)", type=["csv"], key="plan_csv")
        if plan_upload is not None:
            try:
                p = pd.read_csv(plan_upload)
                rename_map = {
                    "날짜":"date","일자":"date",
                    "구분":"direction","입출":"direction",
                    "금액":"amount",
                    "항목":"label","내용":"label","메모":"label"
                }
                p = p.rename(columns={k:v for k,v in rename_map.items() if k in p.columns})
                need = {"date","direction","amount"}
                if not need.issubset(set(p.columns)):
                    st.error(f"CSV 컬럼 필요: {need} / 현재: {list(p.columns)}")
                else:
                    if "label" not in p.columns:
                        p["label"] = ""
                    st.session_state.plan_df = p[["date","direction","amount","label"]].copy()
                    st.success("예정표를 불러왔습니다.")
            except Exception as e:
                st.error(f"CSV 읽기 실패: {e}")

        st.download_button(
            "예정표 CSV 다운로드",
            data=st.session_state.plan_df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"plan_{year}-{month:02d}.csv",
            mime="text/csv"
        )

    with colB:
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

    month_table = build_month_table(
        tx=tx,
        plan_df=st.session_state.plan_df,
        year=year,
        month=month,
        start_balance=float(start_balance),
        confirmed_pairs=st.session_state.confirmed_pairs
    )

    min_row = month_table.loc[month_table["end_balance"].idxmin()]
    end_row = month_table.iloc[-1]

    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("시작잔고", fmt_int(start_balance))
    s2.metric("월말 예상잔고", fmt_int(end_row["end_balance"]))
    s3.metric("최저 잔고", fmt_int(min_row["end_balance"]))
    s4.metric("최저 잔고 날짜", f"{min_row['date']} ({min_row['dow']})")
    s5.metric("확정 내부이동(세션)", f"{len(st.session_state.confirmed_pairs):,}쌍")

    st.subheader("일자별 잔액 롤링표 (숫자 중심)")
    disp = month_table.copy()
    disp["날짜"] = disp["date"].astype(str)
    disp["요일"] = disp["dow"].astype(str)
    disp["시작잔고"] = disp["start_balance"].map(fmt_int)
    disp["실제입금(청구)"] = disp["actual_in_claim"].map(fmt_int)
    disp["실제입금"] = disp["actual_in"].map(fmt_int)
    disp["실제출금"] = disp["actual_out"].map(fmt_int)
    disp["예정입금"] = disp["plan_in"].map(fmt_int)
    disp["예정출금"] = disp["plan_out"].map(fmt_int)
    disp["순변동"] = disp["net_change"].map(fmt_int)
    disp["종료잔고"] = disp["end_balance"].map(fmt_int)
    disp["⚠️경고"] = month_table["end_balance"].astype(int) <= int(danger)

    out_cols = ["날짜","요일","시작잔고","실제입금(청구)","실제입금","실제출금","예정입금","예정출금","순변동","종료잔고","⚠️경고"]
    st.dataframe(disp[out_cols], use_container_width=True, hide_index=True)

    st.download_button(
        "월수입지출(롤링표) CSV 다운로드",
        data=month_table.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"monthly_cashflow_{year}-{month:02d}.csv",
        mime="text/csv"
    )

    st.subheader("일자 상세(실제 거래 확인)")
    sel_date = st.date_input("조회할 날짜", value=first)

    confirmed_out = {p[0] for p in st.session_state.confirmed_pairs}
    confirmed_in = {p[1] for p in st.session_state.confirmed_pairs}
    excluded_ids = confirmed_out.union(confirmed_in)

    day_tx = tx[tx["biz_date"] == sel_date].copy()
    if day_tx.empty:
        st.info("해당 날짜 거래가 없습니다.")
    else:
        day_tx["include_in_calc"] = (
            (~day_tx["is_excluded_account"]) &
            (~day_tx["is_principal"]) &
            (~day_tx["is_internal_auto"]) &
            (~day_tx["tx_id"].isin(excluded_ids))
        )
        day_tx = day_tx.sort_values("posted_at")

        disp2 = day_tx[["posted_at","account_name","direction","subtype","amount","counterparty","include_in_calc","source","file_name"]].copy()
        disp2["시간"] = disp2["posted_at"].map(fmt_dt)
        disp2["금액"] = disp2["amount"].map(fmt_int)

        st.dataframe(
            disp2[["시간","account_name","direction","subtype","금액","counterparty","include_in_calc","source","file_name"]],
            use_container_width=True,
            hide_index=True
        )

# =========================================================
# Page: Ledger
# =========================================================
else:
    st.header("원장(거래 목록)")
    st.caption("금액 불일치가 있을 때, 특정 날짜/계좌/상대를 검색해서 원인을 찾는 용도입니다.")

    q = st.text_input("검색(계좌/상대/파일명)", value="")
    df = tx.copy()

    if q.strip():
        mask = (
            df["account_name"].astype(str).str.contains(q, na=False) |
            df["counterparty"].astype(str).str.contains(q, na=False) |
            df["file_name"].astype(str).str.contains(q, na=False)
        )
        df = df[mask]

    df = df.sort_values("posted_at", ascending=False).head(500)

    disp = df[["posted_at","biz_date","account_name","direction","subtype","amount","counterparty","balance","source","file_name","is_internal_auto","is_principal","is_excluded_account"]].copy()
    disp["일시"] = disp["posted_at"].map(fmt_dt)
    disp["일자"] = disp["biz_date"].astype(str)
    disp["금액"] = disp["amount"].map(fmt_int)
    disp["잔액"] = disp["balance"].map(fmt_int)

    st.dataframe(
        disp[["일시","일자","account_name","direction","subtype","금액","counterparty","잔액","source","file_name","is_internal_auto","is_principal","is_excluded_account"]],
        use_container_width=True,
        hide_index=True
    )
