import pyodbc
import pandas as pd
from datetime import datetime

# ======= CẤU HÌNH =======
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=.;"
    "DATABASE=dataQLCL;"
    "UID=sa;"
    "PWD=123;"
    "TrustServerCertificate=Yes;"
)

# BẮT BUỘC: Id của T_THUTHAP (DataInput)
THUTHAP_ID = "00055595-7e34-4b39-a7fc-7556ddee5702"

# TUỲ CHỌN: danh sách T_DKCHUANCT (StandardDetail). Để [] để xuất tất cả StandardDetail của THUTHAP_ID
DKCHUANCT_IDS = [
    "026b081c-5038-4492-9a0c-c6833b7cab91",
    "033ff1ce-4b08-4fa2-9744-fdc4418ebd32",
    # thêm id khác nếu muốn lọc, hoặc để [] để lấy tất cả liên quan
]

# Thư mục/tiền tố file xuất
TS = datetime.now().strftime("%Y%m%d_%H%M%S")
PREFIX = f"export_{THUTHAP_ID[:8]}_{TS}"

# ======= HÀM TRỢ GIÚP =======
def _placeholders(n: int) -> str:
    return ",".join("?" for _ in range(n))

def _ensure_list(xs):
    return [x for x in (xs or []) if x]

# ======= CHẠY =======
with pyodbc.connect(CONN_STR) as conn:
    # 1) Lấy DataInput (T_THUTHAP) theo ID
    sql_di = """
        SELECT *
        FROM dbo.T_THUTHAP
        WHERE ID = ?
    """
    df_di = pd.read_sql(sql_di, conn, params=[THUTHAP_ID])
    file_di = f"{PREFIX}_DataInput.xlsx"
    df_di.to_excel(file_di, index=False, engine="openpyxl")
    print(f"✅ DataInput: {len(df_di)} dòng -> {file_di}")

    # 2) Xác định danh sách StandardDetail cần lấy
    dk_list = _ensure_list(DKCHUANCT_IDS)
    if not dk_list:
        # Nếu không chỉ định, tự suy ra từ T_THUTHAPCT (tất cả standard liên quan tới THUTHAP_ID)
        sql_distinct_std = """
            SELECT DISTINCT T_DKCHUANCT
            FROM dbo.T_THUTHAPCT
            WHERE T_THUTHAP = ?
        """
        df_dist = pd.read_sql(sql_distinct_std, conn, params=[THUTHAP_ID])
        dk_list = df_dist["T_DKCHUANCT"].dropna().astype(str).tolist()
        print(f"ℹ️  Suy ra {len(dk_list)} StandardDetail từ T_THUTHAPCT.")

    # 3) Lấy StandardDetail (T_DKCHUANCT) theo danh sách
    df_std = pd.DataFrame()
    if dk_list:
        sql_std = f"""
            SELECT *
            FROM dbo.T_DKCHUANCT
            WHERE ID IN ({_placeholders(len(dk_list))})
        """
        df_std = pd.read_sql(sql_std, conn, params=dk_list)
    file_std = f"{PREFIX}_StandardDetail.xlsx"
    df_std.to_excel(file_std, index=False, engine="openpyxl")
    print(f"✅ StandardDetail: {len(df_std)} dòng -> {file_std}")

    # 4) Lấy DataInputDetail (T_THUTHAPCT) theo THUTHAP_ID (+ IN danh sách std nếu có)
    params = [THUTHAP_ID]
    sql_detail = """
        SELECT ID, T_THUTHAP, T_DKCHUANCT, VALUE, ORVALUE, NG, ToolID, PERSON, TYPEOK
        FROM dbo.T_THUTHAPCT
        WHERE T_THUTHAP = ?
    """
    if dk_list:
        sql_detail += f" AND T_DKCHUANCT IN ({_placeholders(len(dk_list))})"
        params += dk_list

    df_detail_raw = pd.read_sql(sql_detail, conn, params=params)

    # 4.1. Đổi tên cột khớp DataInputDetailModel (nếu bạn import vào Mongo theo model mới)
    rename_map = {
        "ID": "Id",
        "T_THUTHAP": "DataInputId",
        "T_DKCHUANCT": "StandardDetailId",
        "VALUE": "Value",
        "ORVALUE": "OrValue",
        "ToolID": "ToolId",
        "PERSON": "Person",
        "TYPEOK": "TypeOk",
        "NG": "NG",
    }
    df_detail = df_detail_raw.rename(columns=rename_map)

    # (tuỳ chọn) sort theo ID để giữ thứ tự thời gian tương đối
    if "Id" in df_detail.columns:
        df_detail = df_detail.sort_values("Id")

    file_detail = f"{PREFIX}_DataInputDetail.xlsx"
    df_detail.to_excel(file_detail, index=False, engine="openpyxl")
    print(f"✅ DataInputDetail: {len(df_detail)} dòng -> {file_detail}")

print("🎉 Done.")
