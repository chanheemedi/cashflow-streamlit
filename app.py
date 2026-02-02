import streamlit as st
import pandas as pd
from datetime import timedelta

st.set_page_config(page_title="현금흐름 MVP", layout="wide")

# -----------------------
# 0) 고정 설정
# -----------------------
EXCLUDED_ACCOUNT_NAMES = {"신한_에셀", "하나_꾸러기건식"}  # 병원 집계에서 제외
WINDOW = timedelta(hours=2)  # 내부이동 매칭 시간창

# ECOUNT 엑셀에서 기대하는 컬럼명(필요하면 여기만 바꾸면 됨)
COL_DT = "거래일시"
COL_ACCT = "계좌명"
COL_IN = "입금액"
COL_OUT = "출금액"

st.title("현금흐름 MVP (ECOUNT 업로드 → 내부이동 검수 → 피벗)")

st.info(
    "⚠️ Streamlit Community Cloud는 로컬 파일 저장(예: SQLite)을 영구 보장하지 않습니다.\n"
    "그래서 이 MVP는 **업로드 파일 기반으로 계산**하고, 내부이동 확정은 **세션 동안 유지**합니다.\n"
    "필요하면 '내부이동 확정 목록'을 다운로드해두세요."
)

# -----------------------
# 1) 업로드
# -----------------------
uploaded = st.file_uploader("ECOUNT DB 엑셀(.xlsx) 업로드", type=["xlsx"])

if uploaded is None:
    st.stop()

try:
    df = pd.read_excel(uploaded, engine="openpyxl").dropna(how="all")
except Exception as e:
    st.error(f"엑셀 읽기 실패: {e}")
    st.stop()

missing = [c for c in [COL_DT, COL_ACCT, COL_IN, COL_OUT] if c not in df.columns]
if missing:
    st.error(f"엑셀 컬럼이 예상과 다릅니다. 누락: {missing}\n"
             f"현재 컬럼: {list(df.columns)}\n\n"
             f"→ app.py 상단의 COL_* 값을 너 파일 컬럼명으로 맞춰주세요.")
    st.stop()

# 거래일시 파싱
df[COL_DT] = pd.to_datetime(df[COL_DT], errors="coerce")
df = df[df[COL_DT].notna()].copy()

# 입금/출금 방향 + 금액 결정
df[COL_IN] = pd.to_numeric(df[COL_IN], errors="coerce").fillna(0)
df[COL_OUT] = pd.to_numeric(df[COL_OUT], errors="coerce").fillna(0)

def pick_dir_amt(r):
    if r[COL_IN] > 0 and r[COL_OUT] == 0:
        return "IN", int(r[COL_IN])
    if r[COL_OUT] > 0 and r[COL_IN] == 0:
        return "OUT", int(r[COL_OUT])
    return None, None

tmp = df.apply(pick_dir_amt, axis=1, result_type="expand")
df["direction"] = tmp[0]
df["amount"] = tmp[1]
df = df[df["direction"].notna()].copy()

# 병원 범위 필터(계좌명 기준)
df["account_name"] = df[COL_ACCT].astype(str)
df["is_hospital"] = ~df["account_name"].isin(EXCLUDED_ACCOUNT_NAMES)
df_h = df[df["is_hospital"]].copy()

st.subheader("업로드 요약")
c1, c2, c3 = st.columns(3)
c1.metric("전체 거래 수", f"{len(df):,}")
c2.metric("병원 범위 거래 수", f"{len(df_h):,}")
c3.metric("제외(에셀/건기식) 거래 수", f"{(len(df)-len(df_h)):,}")

# -----------------------
# 2) 내부이동 후보 생성
# -----------------------
# 세션 상태: confirmed_pairs 저장 (key = (out_idx, in_idx))
if "confirmed" not in st.session_state:
    st.session_state.confirmed = set()

# 후보 생성: 동일금액, 반대방향, 다른 계좌, 시간차 <= WINDOW, 1:1 (가장 가까운 것)
outs = df_h[df_h["direction"] == "OUT"].sort_values(COL_DT).reset_index()
ins = df_h[df_h["direction"] == "IN"].sort_values(COL_DT).reset_index()

ins_by_amount = {}
for _, r in ins.iterrows():
    ins_by_amount.setdefault(r["amount"], []).append(r)

used_in = set()
candidates = []
for _, o in outs.iterrows():
    pool = ins_by_amount.get(o["amount"], [])
    best = None
    best_diff = None
    for i in pool:
        if i["index"] in used_in:
            continue
        if i["account_name"] == o["account_name"]:
            continue
        diff = abs(i[COL_DT] - o[COL_DT])
        if diff <= WINDOW:
            if best is None or diff < best_diff:
                best, best_diff = i, diff
    if best is not None:
        used_in.add(best["index"])
        candidates.append((int(o["index"]), int(best["index"]), int(o["amount"]), int(best_diff.total_seconds())))

st.subheader("내부이동 후보 검수 (체크하면 내부이동으로 확정)")
st.caption("룰: 동일금액 + 반대방향 + 서로 다른 우리 계좌 + 2시간 이내 + 1:1(가장 가까운 거래)")

# 후보 테이블 표시 + 체크박스로 확정
cand_rows = []
for out_idx, in_idx, amt, diff_sec in candidates:
    out_row = df_h.loc[out_idx]
    in_row = df_h.loc[in_idx]
    key = (out_idx, in_idx)
    default = key in st.session_state.confirmed

    left, mid, right = st.columns([7, 7, 2])
    with left:
        st.markdown(f"**OUT** {out_row[COL_DT]} / {out_row['account_name']} / {amt:,}")
    with mid:
        st.markdown(f"**IN**  {in_row[COL_DT]} / {in_row['account_name']} / {amt:,}")
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

# 확정 목록 다운로드
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
