import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
import os
# PostgreSQL 연결 설정

DB_SETTINGS = {
    'dbname': 'database',
    'user': 'user',
    'password': 'password',
    'host': 'localhost',  # 예: 'localhost'
    'port': '5432'   # 기본적으로 5432
}

# 로그 파일 설정
current_date = datetime.now().strftime('%Y%m%d')
txt_log_file_name = f"sequence_update_{current_date}.txt"
error_log_file_name = f"sequence_update_error_{current_date}.txt"

# 스키마 배열
SCHEMAS = [
   "schema"
]

# 시퀀스 및 테이블 정보 쿼리 템플릿
SEQUENCE_QUERY_TEMPLATE = """
SELECT 
    n.nspname AS schema_name,
    t.relname AS table_name,
    a.attname AS column_name,
    s.relname AS sequence_name
FROM 
    pg_class t
JOIN 
    pg_namespace n ON t.relnamespace = n.oid
JOIN 
    pg_attribute a ON t.oid = a.attrelid
JOIN 
    pg_attrdef d ON a.attnum = d.adnum AND t.oid = d.adrelid
JOIN 
    pg_class s ON pg_get_expr(d.adbin, d.adrelid) LIKE ('nextval(%' || s.relname || '%)')
WHERE 
    n.nspname = '{schema_name}'  -- 특정 스키마 이름
    AND t.relkind = 'r'  -- 'r' indicates regular table
    AND s.relkind = 'S'  -- 'S' indicates sequence
ORDER BY 
    t.relname, a.attname;
"""

# DO 블록 템플릿
DO_BLOCK_TEMPLATE = """
DO $$
DECLARE
    v_table_name CONSTANT TEXT := '{table_name}';
    v_schema_name CONSTANT TEXT := '{schema_name}';
    pk_column TEXT;
    seq_name TEXT;
    next_index_value BIGINT;
    current_sequence_value BIGINT;
BEGIN
    -- 기본 키 컬럼 이름 조회
    SELECT kcu.column_name
    INTO pk_column
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    WHERE tc.table_name = v_table_name
      AND tc.table_schema = v_schema_name
      AND tc.constraint_type = 'PRIMARY KEY';

    -- 시퀀스 이름 조회
    SELECT pg_get_serial_sequence(format('%I.%I', v_schema_name, v_table_name), pk_column)
    INTO seq_name;

    -- 현재 시퀀스 값 조회
    EXECUTE format('SELECT last_value FROM %I.%I', v_schema_name, seq_name)
    INTO current_sequence_value;

    -- PK 컬럼의 최대값 + 1 계산
    EXECUTE format('SELECT MAX(%I) + 1 FROM %I.%I', pk_column, v_schema_name, v_table_name)
    INTO next_index_value;

    -- 시퀀스 값 업데이트
    EXECUTE format('SELECT setval(%L, %s, true)', format('%I.%I', v_schema_name, seq_name), next_index_value);

    -- 결과 출력
    RAISE NOTICE 'Table: %, PK Column: %, Sequence: %, Before: %, After: %',
                 v_table_name, pk_column, seq_name, current_sequence_value, next_index_value;
END $$;
"""

def execute_do_blocks():
    try:
        # 로그 파일 열기
        with open(txt_log_file_name, 'w', encoding='utf-8') as log_file, open(error_log_file_name, 'w', encoding='utf-8') as error_log_file:
            # 데이터베이스 연결
            with psycopg2.connect(**DB_SETTINGS) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    for schema_name in SCHEMAS:
                        try:
                            # 스키마별 시퀀스 정보 쿼리 생성 및 실행
                            sequence_query = SEQUENCE_QUERY_TEMPLATE.format(schema_name=schema_name)
                            cur.execute(sequence_query)
                            results = cur.fetchall()

                            for row in results:
                                table_name = row['table_name']
                                sequence_name = row['sequence_name']
                                column_name = row['column_name']

                                try:
                                    # DO 블록 실행 전에 시퀀스 값 확인
                                    cur.execute(f'SELECT last_value FROM "{schema_name}"."{sequence_name}"')
                                    current_sequence_value = cur.fetchone()['last_value']

                                    cur.execute(f'SELECT MAX("{column_name}") + 1 AS next_value FROM "{schema_name}"."{table_name}"')
                                    next_index_value = cur.fetchone()['next_value']

                                    # 시퀀스 값이 None이 아닌 경우에만 업데이트
                                    if next_index_value is not None:
                                        cur.execute(f"SELECT setval('\"{schema_name}\".\"{sequence_name}\"', {next_index_value}, true)")

                                        log_message = (
                                            f"Schema: {schema_name}, Table: {table_name}, Sequence: {sequence_name}\n"
                                            f"Before: {current_sequence_value}, After: {next_index_value}\n"
                                        )
                                        log_file.write(log_message + "\n")  # 파일에 기록
                                        print(log_message)  # 콘솔에도 출력
                                    else:
                                        error_message = (
                                            f"Schema: {schema_name}, Table: {table_name}, Sequence: {sequence_name}\n"
                                            f"Error: next_index_value is None, skipping setval.\n"
                                        )
                                        error_log_file.write(error_message + "\n")
                                        print(error_message)  # 콘솔에도 출력

                                except Exception as e:
                                    error_message = (
                                        f"Error in Schema: {schema_name}, Table: {table_name}, Sequence: {sequence_name}\n"
                                        f"Error: {e}\n"
                                    )
                                    error_log_file.write(error_message + "\n")
                                    print(error_message)  # 콘솔에도 출력

                        except Exception as e:
                            error_message = f"Error processing schema: {schema_name}\nError: {e}\n"
                            error_log_file.write(error_message + "\n")
                            print(error_message)  # 콘솔에도 출력

    except psycopg2.Error as e:
        error_message = f"Error connecting to the database: {e}"
        with open(error_log_file_name, 'a', encoding='utf-8') as error_log_file:
            error_log_file.write(error_message + "\n")
        print(error_message)  # 콘솔에도 출력

if __name__ == "__main__":
    execute_do_blocks()