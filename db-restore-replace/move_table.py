import pandas as pd
import psycopg2

# CSV 파일 읽기
csv_file = "C:/문서/UDS/table_list.csv"  # CSV 파일 경로
df = pd.read_csv(csv_file)

# Source DB 접속 정보
source_host = 'localhost'
source_dbname = 'data_base'
source_user = 'postgres'
source_password = 'mysecretpassword'

# Target DB 접속 정보
target_host = 'localhost'
target_dbname = 'database'
target_user = 'user'
target_password = 'password'

# Source DB 연결
source_conn = psycopg2.connect(host=source_host, dbname=source_dbname, user=source_user, password=source_password)
source_cur = source_conn.cursor()

# Target DB 연결
target_conn = psycopg2.connect(host=target_host, dbname=target_dbname, user=target_user, password=target_password)
target_cur = target_conn.cursor()

# 테이블을 복사하는 함수
def copy_table(schema_name, table_name):
    try:
        print(f"\nStarting to copy {schema_name}.{table_name}")
        
        # Source DB에서 테이블 구조 조회 시 geometry 타입의 SRID도 함께 조회
        source_cur.execute(f"""
            SELECT 
                c.column_name,
                CASE 
                    WHEN c.udt_name = 'geometry' THEN 
                        CONCAT('geometry(', g.type, ', ', g.srid, ')')
                    WHEN c.udt_name = 'geography' THEN 
                        CONCAT('geography(', g.type, ', ', g.srid, ')')
                    ELSE c.data_type
                END as data_type,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT 
                    f_table_schema,
                    f_table_name,
                    f_geometry_column,
                    type,
                    srid
                FROM geometry_columns
            ) g ON 
                c.table_schema = g.f_table_schema AND
                c.table_name = g.f_table_name AND
                c.column_name = g.f_geometry_column
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position;
        """, (schema_name, table_name))
        
        columns_info = source_cur.fetchall()
        print(f"Found {len(columns_info)} columns in source table")

        # Target DB에 스키마 생성
        target_cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        
        # 테이블 생성 쿼리 구성
        create_table_query = f"CREATE TABLE {schema_name}.{table_name} ("
        columns = []
        for col_name, data_type, char_max_length, num_precision, num_scale in columns_info:
            col_def = f"{col_name} {data_type}"
            if data_type == 'character varying' and char_max_length:
                col_def += f"({char_max_length})"
            elif data_type == 'numeric' and num_precision:
                if num_scale:
                    col_def += f"({num_precision},{num_scale})"
                else:
                    col_def += f"({num_precision})"
            columns.append(col_def)
        
        create_table_query += ", ".join(columns) + ");"
        print(f"Creating table with query: {create_table_query}")
        
        # 기존 테이블이 있으면 삭제
        target_cur.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;")
        target_cur.execute(create_table_query)

        # Source DB에서 데이터 조회
        source_cur.execute(f"SELECT * FROM {schema_name}.{table_name};")
        rows = source_cur.fetchall()
        print(f"Found {len(rows)} rows in source table")
        
        if rows:
            # 데이터 삽입
            columns = [col[0] for col in columns_info]
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))
            
            insert_query = f"INSERT INTO {schema_name}.{table_name} ({columns_str}) VALUES ({placeholders})"
            print(f"Inserting data with query template: {insert_query}")
            
            # 배치 처리로 변경 (한 번에 1000행씩)
            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                target_cur.executemany(insert_query, batch)
                target_conn.commit()  # 각 배치마다 커밋
                print(f"Inserted batch {i//batch_size + 1} ({len(batch)} rows)")

    except Exception as e:
        print(f"Detailed error copying {schema_name}.{table_name}:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        import traceback
        print("Traceback:")
        print(traceback.format_exc())
        target_conn.rollback()
        source_conn.rollback()
        raise  # 에러를 다시 발생시켜서 전체 프로세스 중단

# CSV 파일에서 스키마와 테이블 이름 읽기
for index, row in df.iterrows():
    schema_name = row.iloc[0]  # 첫 번째 열: 스키마 이름
    table_name = row.iloc[1]   # 두 번째 열: 테이블 이름
    print(schema_name, table_name)
    # 테이블 복사
    copy_table(schema_name, table_name)

# 변경 사항 커밋
target_conn.commit()

# 연결 종료
source_cur.close()
source_conn.close()
target_cur.close()
target_conn.close()

print("Data copy process completed.")
