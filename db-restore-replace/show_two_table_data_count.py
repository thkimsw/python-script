import pandas as pd
import psycopg2
from datetime import datetime

# CSV 파일 읽기 및 결과 파일 설정
csv_file = "C:/문서/UDS/table_list.csv"  # CSV 파일 경로
result_file = "C:/문서/UDS/compare_result.csv"  # 결과 저장할 CSV 파일 경로
df = pd.read_csv(csv_file)

# Target DB 접속 정보(dds 원격)
target_host = '210.123.245.141'
target_dbname = 'dds_db'
target_user = 'dds_u'
target_password = 'dds123!@#'


# #Target DB 접속 정보(로컬)
# target_host = 'localhost'
# target_dbname = 'gis_db'
# target_user = 'postgres'
# target_password = 'mysecretpassword'

# Target DB 연결
target_conn = psycopg2.connect(host=target_host, dbname=target_dbname, user=target_user, password=target_password)
target_cur = target_conn.cursor()

# 결과를 저장할 리스트
results = []

# 테이블의 레코드 수를 비교하는 함수
def compare_table_counts(schema_name, table_name):
    try:
        # 원본 테이블의 레코드 수 조회 (Target DB)
        target_cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
        original_count = target_cur.fetchone()[0]

        # 백업 테이블의 레코드 수 조회 (Target DB)
        backup_table = f"{table_name}_back0115"
        target_cur.execute(f"SELECT COUNT(*) FROM {schema_name}.{backup_table}")
        backup_count = target_cur.fetchone()[0]

        # 결과 출력 및 저장
        print(f"\n{schema_name}.{table_name}:")
        print(f"Original table count: {original_count:,}")
        print(f"Backup table count: {backup_count:,}")
        print("-" * 50)

        # 결과를 리스트에 추가 (테이블 이름과 레코드 수)
        results.append({
            'schema_name': schema_name,
            'table_name': table_name,
            'original_count': original_count,
            'backup_count': backup_count,
            'check_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    except Exception as e:
        print(f"Error comparing {schema_name}.{table_name}: {e}")
        results.append({
            'schema_name': schema_name,
            'table_name': table_name,
            'original_count': 'ERROR',
            'backup_count': 'ERROR',
            'check_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'error_message': str(e)
        })
        target_conn.rollback()

# CSV 파일에서 스키마와 테이블 이름 읽기
for index, row in df.iterrows():
    schema_name = row.iloc[0]  # 첫 번째 열: 스키마 이름
    table_name = row.iloc[1]   # 두 번째 열: 테이블 이름
    compare_table_counts(schema_name, table_name)

# 결과를 DataFrame으로 변환하고 CSV 파일로 저장
results_df = pd.DataFrame(results)
results_df.to_csv(result_file, index=False, encoding='utf-8-sig')

# 연결 종료
target_cur.close()
target_conn.close()

print(f"\nCount comparison completed. Results saved to {result_file}")


