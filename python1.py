import pyodbc
import pandas as pd

conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=.;"
    "DATABASE=dataQLCL;"
    "UID=sa;"
    "PWD=123;"
    "TrustServerCertificate=Yes;"
)

try:
    # Tạo kết nối
    conn = pyodbc.connect(conn_str)

    # Truy vấn dữ liệu dùng pandas
    query = "SELECT TOP 500 * FROM T_DKCHUAN"
    df = pd.read_sql(query, conn)

    # Xuất ra file Excel
    output_file = "T_DKCHUAN_export.xlsx"
    df.to_excel(output_file, index=False, engine='openpyxl')

    print(f"✅ Đã xuất dữ liệu ra file: {output_file}")

except Exception as e:
    print("❌ Lỗi khi xử lý:", e)

finally:
    conn.close()
