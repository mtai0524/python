import pyodbc
import pandas as pd

# Kết nối đến SQL Server
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=.;"
    "DATABASE=dataQLCL;"
    "UID=sa;"
    "PWD=123;"
    "TrustServerCertificate=Yes;"
)

try:
    conn = pyodbc.connect(conn_str)

    # --- Export dữ liệu từ T_DKCHUAN ---
    query_dkchuan = "SELECT * FROM T_DKCHUAN"
    df_dkchuan = pd.read_sql(query_dkchuan, conn)
    count_dkchuan = len(df_dkchuan)
    file_dkchuan = f"T_DKCHUAN_export_{count_dkchuan}.xlsx"
    df_dkchuan.to_excel(file_dkchuan, index=False, engine='openpyxl')
    print(f"✅ Đã xuất {count_dkchuan} dòng từ T_DKCHUAN ra file: {file_dkchuan}")

    # --- Export dữ liệu từ T_THUTHAP ---
    query_thuthap = "SELECT TOP 500 * FROM T_THUTHAP"
    df_thuthap = pd.read_sql(query_thuthap, conn)
    count_thuthap = len(df_thuthap)
    file_thuthap = f"T_THUTHAP_export_{count_thuthap}.xlsx"
    df_thuthap.to_excel(file_thuthap, index=False, engine='openpyxl')
    print(f"✅ Đã xuất {count_thuthap} dòng từ T_THUTHAP ra file: {file_thuthap}")

    # --- Export dữ liệu từ T_DKCHUANCT ---
    query_dkchuanct = "SELECT TOP 2000 * FROM T_DKCHUANCT"
    df_dkchuanct = pd.read_sql(query_dkchuanct, conn)
    count_dkchuanct = len(df_dkchuanct)
    file_dkchuanct = f"T_DKCHUANCT_export_{count_dkchuanct}.xlsx"
    df_dkchuanct.to_excel(file_dkchuanct, index=False, engine='openpyxl')
    print(f"✅ Đã xuất {count_dkchuanct} dòng từ T_DKCHUANCT ra file: {file_dkchuanct}")

except Exception as e:
    print("❌ Lỗi khi xử lý:", e)

finally:
    if 'conn' in locals() and conn:
        conn.close()
