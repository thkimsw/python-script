#docker run --name postgis_container -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_DB=gis_db -p 5432:5432 -d postgis/postgis
#docker exec -it postgis_container psql -U postgres -d gis_db
#CREATE EXTENSION postgis;
#CREATE EXTENSION postgis_topology;
#CREATE EXTENSION postgis_raster;

import subprocess
import os
import time
import psycopg2
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple
import threading

# 설정
dump_file = "path/example.dump"
table_list_file = "path/table_list.csv"
host = "localhost"
database_name = "database"
username = "user"
password = "password"
pg_restore_path = "path/pg_restore.exe"

# 환경 변수 설정
os.environ["PGPASSWORD"] = password

# 스레드 안전한 출력을 위한 락
print_lock = threading.Lock()

def safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

# 스키마 존재 여부 확인 및 생성을 위한 함수
def drop_existing_tables_and_schemas(schemas: set):
    try:
        conn = psycopg2.connect(
            dbname=database_name,
            user=username,
            password=password,
            host=host
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            for schema in schemas:
                # Drop all tables in schema
                cursor.execute(f"""
                    DO $$ 
                    BEGIN
                        EXECUTE (
                            SELECT string_agg('DROP TABLE IF EXISTS ' || quote_ident(schemaname) || '.' || quote_ident(tablename) || ' CASCADE', ';')
                            FROM pg_tables
                            WHERE schemaname = '{schema}'
                        );
                    END $$;
                """)
                # Drop schema
                cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
                safe_print(f"스키마 {schema}와 관련 테이블들이 제거되었습니다.")
        conn.close()
    except Exception as e:
        safe_print(f"스키마와 테이블 제거 중 오류 발생: {e}")

def ensure_schemas_exist(schemas: set):
    try:
        # 먼저 기존 스키마와 테이블 제거
        drop_existing_tables_and_schemas(schemas)
        
        conn = psycopg2.connect(
            dbname=database_name,
            user=username,
            password=password,
            host=host
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            # public 스키마 먼저 생성
            cursor.execute("CREATE SCHEMA IF NOT EXISTS public;")
            
            for schema in schemas:
                # 스키마 이름을 따옴표로 감싸서 생성
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
                safe_print(f"스키마 {schema}가 새로 생성되었습니다.")
                
                # 스키마 권한 부여
                cursor.execute(f'GRANT ALL ON SCHEMA "{schema}" TO postgres;')
                cursor.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO postgres;')
        conn.close()
    except Exception as e:
        safe_print(f"스키마 생성 중 오류 발생: {e}")
        raise  # 에러를 상위로 전파하여 프로그램 중단

def load_table_lists(file_path: str) -> List[Tuple[str, str]]:
    with open(file_path, mode='r', encoding='utf-8') as f:
        reader = csv.reader(f)
        return [(row[0], row[1]) for row in reader]

def restore_table_structure(schema: str, table: str) -> bool:
    command = [
        pg_restore_path,
        "--host", host,
        "--username", username,
        "--dbname", database_name,
        "--no-owner",
        "--schema", schema,
        "--table", table,
        "--schema-only",
        "--verbose",
        dump_file
    ]
    try:
        result = subprocess.run(
            command, 
            check=True, 
            text=True, 
            capture_output=True,
            encoding='utf-8'
        )
        safe_print(f"{schema}.{table} 테이블 구조 복원 완료")
        return True
    except subprocess.CalledProcessError as e:
        safe_print(f"{schema}.{table} 테이블 구조 복원 실패: {e.stderr}")
        return False
    except Exception as e:
        safe_print(f"{schema}.{table} 예기치 못한 오류: {e}")
        return False

def restore_table_data(schema: str, table: str) -> bool:
    command = [
        pg_restore_path,
        "--host", host,
        "--username", username,
        "--dbname", database_name,
        "--no-owner",
        "--schema", schema,
        "--table", table,
        "--data-only",
        "--verbose",
        dump_file
    ]
    try:
        result = subprocess.run(
            command, 
            check=True, 
            text=True, 
            capture_output=True,
            encoding='utf-8'
        )
        safe_print(f"{schema}.{table} 데이터 복원 완료")
        return True
    except subprocess.CalledProcessError as e:
        safe_print(f"{schema}.{table} 데이터 복원 실패: {e.stderr}")
        return False
    except Exception as e:
        safe_print(f"{schema}.{table} 예기치 못한 오류: {e}")
        return False

def check_table_exists(schema: str, table: str) -> bool:
    try:
        conn = psycopg2.connect(
            dbname=database_name,
            user=username,
            password=password,
            host=host
        )
        with conn.cursor() as cursor:
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = '{schema}' 
                    AND table_name = '{table}'
                );
            """)
            exists = cursor.fetchone()[0]
        conn.close()
        return exists
    except Exception as e:
        safe_print(f"테이블 존재 여부 확인 중 오류 발생: {e}")
        return False

def parallel_restore(tables: List[Tuple[str, str]], max_workers: int = 4):
    # 1. 먼저 모든 고유한 스키마를 확인
    unique_schemas = {schema for schema, _ in tables}
    try:
        ensure_schemas_exist(unique_schemas)
        time.sleep(1)  # 스키마 생성 후 잠시 대기
    except Exception as e:
        safe_print(f"스키마 생성 실패로 프로그램을 종료합니다: {e}")
        return
    
    # 2. 테이블별로 처리
    structure_results = []
    for schema, table in tables:
        table_exists = check_table_exists(schema, table)
        if table_exists:
            safe_print(f"{schema}.{table} 테이블이 이미 존재합니다. 데이터만 복원합니다.")
            structure_results.append((schema, table, True))
        else:
            safe_print(f"{schema}.{table} 테이블 구조를 복원합니다.")
            success = restore_table_structure(schema, table)
            structure_results.append((schema, table, success))
    
    # 3. 성공한 테이블만 데이터 복원
    successful_tables = [(schema, table) for schema, table, success in structure_results if success]
    safe_print(f"\n=== 데이터 병렬 복원 시작 ({len(successful_tables)} 테이블) ===")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {
            executor.submit(restore_table_data, schema, table): (schema, table)
            for schema, table in successful_tables
        }
        for future in as_completed(future_to_table):
            schema, table = future_to_table[future]
            try:
                success = future.result()
                if success:
                    safe_print(f"{schema}.{table} 데이터 복원 완료")
                else:
                    safe_print(f"{schema}.{table} 데이터 복원 실패")
            except Exception as e:
                safe_print(f"{schema}.{table} 처리 중 오류 발생: {e}")

def ensure_postgis_extension():
    try:
        conn = psycopg2.connect(
            dbname=database_name,
            user=username,
            password=password,
            host=host
        )
        conn.autocommit = True
        with conn.cursor() as cursor:
            # Create extension in public schema first
            cursor.execute("CREATE SCHEMA IF NOT EXISTS public;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;")
            # Set search path to include public schema
            cursor.execute("SET search_path TO public, pg_catalog;")
            safe_print("PostGIS 확장이 활성화되었습니다.")
        conn.close()
    except Exception as e:
        safe_print(f"PostGIS 활성화 중 오류 발생: {e}")
        
def main():
    start_time = time.time()
    # Ensure PostGIS is set up before any restore operations
    ensure_postgis_extension()
    
    # Set up connection and search path for all operations
    conn = psycopg2.connect(
        dbname=database_name,
        user=username,
        password=password,
        host=host
    )
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO public, pg_catalog;")
    conn.close()
    
    tables_to_restore = load_table_lists(table_list_file)
    total_tables = len(tables_to_restore)
    
    safe_print(f"총 {total_tables}개의 테이블 복원을 시작합니다...")
    parallel_restore(tables_to_restore)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    safe_print(f"\n복원 작업 완료! 소요 시간: {elapsed_time:.2f}초")

if __name__ == "__main__":
    main()