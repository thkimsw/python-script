import pandas as pd
import psycopg2

# CSV 파일 읽기
csv_file = "path/table_list.csv"  # CSV 파일 경로
df = pd.read_csv(csv_file)

# PostgreSQL 연결 정보
host = 'localhost'
dbname = 'database'
user = 'user'
password = 'password'

# PostgreSQL 연결
conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
cur = conn.cursor()

# 테이블 이름 변경
successful_tables = 0  # 성공적으로 이름이 변경된 테이블 수
failed_tables = []  # 실패한 테이블 목록

for index, row in df.iterrows():
    schema_name = row.iloc[0]  # 첫 번째 열: 스키마 이름
    table_name = row.iloc[1]   # 두 번째 열: 테이블 이름
    # 새로운 테이블 이름 생성
    new_table_name = f"{table_name}_back0115"
    
    try:
        # 트랜잭션을 시작합니다.
        conn.autocommit = False  # 자동 커밋을 비활성화하여 명시적인 트랜잭션을 사용할 수 있게 합니다.
        
        # 테이블 이름 변경 쿼리
        query = f"""
        ALTER TABLE {schema_name}.{table_name} RENAME TO {new_table_name};
        """
        cur.execute(query)
        conn.commit()  # 트랜잭션 커밋
        successful_tables += 1  # 성공적인 테이블 카운트 증가
        print(f"Table {schema_name}.{table_name} renamed to {new_table_name}.")
        
    except Exception as e:
        # 오류 발생 시 롤백하고, 실패한 테이블 목록에 추가
        conn.rollback()  # 오류가 발생하면 롤백하여 트랜잭션을 되돌립니다.
        failed_tables.append(f"{schema_name}.{table_name}")  # 실패한 테이블 추가
        print(f"Error renaming {schema_name}.{table_name}: {e}")
    
    finally:
        # 각 테이블 작업 후 트랜잭션을 다시 활성화하여 계속 진행할 수 있도록 합니다.
        conn.autocommit = True  # 자동 커밋을 다시 활성화

# 결과 출력
print(f"\nTotal successful table renames: {successful_tables}")
print(f"Failed tables: {', '.join(failed_tables) if failed_tables else 'None'}")

# 연결 종료
cur.close()
conn.close()
