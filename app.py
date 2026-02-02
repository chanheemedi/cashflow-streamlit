import streamlit as st
import pandas as pd
import re
from datetime import timedelta

st.set_page_config(page_title="현금흐름 MVP", layout="wide")

# -----------------------
# 0) 고정 설정
# -----------------------
EXCLUDED_ACCOUNT_NAMES = {"신한_에셀", "하나_꾸러기건식"}  # 병원 집계에서 제외
WINDOW = timedelta(hours=2)  # 내부이동 매칭 시간창

# 네 ECOUNT 파일 컬럼 (확정)
COL_DT = "입/출금일자"
COL_DIR = "구분"          # 입금/출금
COL_ACCT = "계좌명"
COL_AMT = "금액"
COL_BAL = "원화잔액"
COL_CP = "입금처(출금처)"  # 상대

st.title("현금흐름 MVP (ECOUNT 업로드 → 내부이동 검수 → 피벗)")

st.info(
    "⚠️ Streamlit Community Cloud는 로컬 파일(SQLite 등) 저장을 영구 보장하지 않습니다.\n"
    "이 MVP는 업로드 파일 기반으로 계산하고, 내부이동 확정은 세션 동안 유지됩니다.\n"
    "필요하면 내부이동 확정 목록을 다운로드해 보관하세요."
)

def read_ecount_excel(uploaded_file) -> pd.DataFrame:
    """
    1행: '회사명 : ...' 같은 제목줄
    2행: 실제 헤더
    → 헤더 행 자동 탐지 후 DataFrame 반환
    """
    raw = pd.read_excel(uploaded_file, engine="openpyxl", header=None).dropna(how="all")
    # 첫 컬럼에서 '입/출금일자'를 찾는 행을 헤더로 사용
    header_row_candidates = raw.index[
        raw.iloc[:, 0].astype(str).str.contains("입/출금일자", na=False)
    ]
    if len(header_row_candidates) == 0:
        raise ValueError(f"헤더 행을 찾지 못했습니다. 첫 열 샘플: {raw.iloc[:5,0].tolist()}")

    header_row = int(header_row_candidates[0])  # 0-based
    df = pd.read_excel(uploaded_file, engine="openpyxl", header=header_row)

    # Unnamed 컬럼 제거 + 전부 비어있는 컬럼 제거
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed")]
    df = df.dropna(axis=1, how="all")
    df = df.dropna(how="all")
    return df

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
        # 예: 2026/01/02 AM 5:45:01
        return pd.to_datetime(x, format="%Y/%m/%d %p %I:%M:%S", errors="coerce")
    return s.apply(_one)

# -----------------------
# 1) 업로드
# -----------------------
uploaded = st.file_uploader("ECOUNT DB 엑셀(.xlsx) 업로드", type=["xlsx"])
if uploaded is None:
    st.stop()

try:
    df = read_ecount_excel(uploaded)
except Exception as e:
    st.error(f"엑셀 읽기/헤더 탐지 실패: {e}")
    st.stop()

# 필수 컬럼 체크
need = [COL_DT, COL_DIR, COL_ACCT, COL_AMT]
missing = [c for c in need if c not in df.columns]
if missing:
    st.error(f"필수 컬럼 누락: {missing}\n현재 컬럼: {list(df.columns)}")
    st.stop()

# 타입 정리
df[COL_DT] = parse_korean_ampm_series(df[COL_DT])
df = df[df[COL_DT].notna()].copy()

df[COL_AMT] = pd.to_numeric(df[COL_AMT], errors="coerce").fillna(0).astype(int)
df[COL_BAL] = pd.to_numeric(df[COL_BAL], errors="coerce")

df["account_name"] = df[COL_ACCT].astype(str)
df["is_hospital"] = ~df["account_name"].isin(EXCLUDED_ACCOUNT_NAMES)
df_h = df[df["is_hospital"]].copy()

# direction 만들기
df_h["direction"] = df_h[COL_DIR].astype(str).map({"입금": "IN", "출금": "OUT"})
df_h = df_h[df_h["direction"].isin(["IN", "OUT"])].copy()
df_h["amount"] = df_h[COL_AMT].astype(int)

st.subheader("업로드 요약")
c1, c2, c3 = st.columns(3)
c1.metric("전체 거래 수", f"{len(df):,}")
c2.metric("병원 범위 거래 수", f"{len(df_h):,}")
c3.metric("제외(에셀/건기식) 거래 수", f"{(len(df)-len(df_h)):,}")

# -----------------------
# 2) 내부이동 후보 생성 + 검수
# -----------------------
if "confirmed" not in st.session_state:
    st.session_state.confirmed = set()

outs = df_h[df_h["direction"] == "OUT"].sort_values(COL_DT).reset_index()
ins = df_h[df_h["direction"] == "IN"].sort_values(COL_DT).reset_index()

ins_by_amount = {}
for _, r in ins.iterrows():
    ins_by_amount.setdefault(int(r["amount"]), []).append(r)

used_in = set()
candidates = []
for _, o in outs.iterrows():
    pool = ins_by_amount.get(int(o["amount"]), [])
    best = None
    best_diff = None
    for i in pool:
        if int(i["index"]) in used_in:
            continue
        if str(i["account_name"]) == str(o["account_name"]):
            continue
        diff = abs(i[COL_DT] - o[COL_DT])
        if diff <= WINDOW:
            if best is None or diff < best_diff:
                best, best_diff = i, diff
    if best is not None:
        used_in.add(int(best["index"]))
        candidates.append((int(o["index"]), int(best["index"]), int(o["amount"]), int(best_diff.total_seconds())))

st.subheader("내부이동 후보 검수 (체크하면 내부이동 확정)")
st.caption("룰: 동일금액 + 반대방향 + 서로 다른 우리 계좌 + 2시간 이내 + 1:1(가장 가까운 거래)")

cand_rows = []
for out_idx, in_idx, amt, diff_sec in candidates:
    out_row = df_h.loc[out_idx]
    in_row = df_h.loc[in_idx]
    key = (out_idx, in_idx)
    default = key in st.session_state.confirmed

    left, mid, right = st.columns([7, 7, 2])
    with left:
        st.markdown(f"**OUT** {out_row[COL_DT]} / {out_row['account_name']} / {amt:,} / {out_row.get(COL_CP,'')}")
    with mid:
        st.markdown(f"**IN**  {in_row[COL_DT]} / {in_row['account_name']} / {amt:,} / {in_row.get(COL_CP,'')}")
        st.caption(f"time diff: {diff_sec}s")
    with right:
        checked = st.checkbox("확정", value=default, key=f"ck_{out_idx}_{in_idx}")
        if checked:
            st.session_state.confirmed.add(key)
        else:
            st.session_state.confirmed.discard(key)

    cand_rows.append({
        "out_idx": out_idx, "in_idx": in_idx, "amount": amt, "time_diff_seconds": diff_sec,
        "out_time": str(out_row[COL_DT]), "out_account": out_row["account_name"],
        "in_time": str(in_row[COL_DT]), "in_account": in_row["account_name"],
        "confirmed": key in st.session_state.confirmed
    })

cand_df = pd.DataFrame(cand_rows)

st.download_button(
    "내부이동 확정 목록 CSV 다운로드",
    data=cand_df[cand_df["confirmed"]].to_csv(index=False).encode("utf-8-sig"),
    file_name="internal_transfers_confirmed.csv",
    mime="text/csv"
)

# -----------------------
# 3) 피벗(일자별 입출금) - 내부이동 확정 제외
# -----------------------
confirmed_out = {k[0] for k in st.session_state.confirmed}
confirmed_in = {k[1] for k in st.session_state.confirmed}
exclude_idx = confirmed_out.union(confirmed_in)

df_f = df_h.drop(index=list(exclude_idx), errors="ignore").copy()
df_f["biz_date"] = df_f[COL_DT].dt.date

daily = df_f.groupby("biz_date").apply(
    lambda g: pd.Series({
        "inflow": int(g.loc[g["direction"] == "IN", "amount"].sum()),
        "outflow": int(g.loc[g["direction"] == "OUT", "amount"].sum()),
    })
).reset_index()

daily["net"] = daily["inflow"] - daily["outflow"]

st.subheader("현금현황(피벗 대체) - 병원 범위 / 내부이동 확정 제외")
st.dataframe(daily, use_container_width=True)
st.line_chart(daily.set_index("biz_date")[["inflow", "outflow", "net"]])
