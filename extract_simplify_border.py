import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import Polygon, MultiPolygon
import asyncio
import os

source_path = "F:/border/source"
result_path = "F:/border/result"
output_sql = f"{result_path}/border_full.sql"

codes = ['11', '26', '27', '28', '29', '30', '31', '36', '41', '42', '43', '44', '45', '46', '47', '48', '50']

input_srid = 5186   # 현재 shapefile 좌표계
output_srid = 4326  # PostGIS 좌표계

# ---------------------------
# 🟣 구멍(holes) 제거 함수
# ---------------------------
def remove_holes(geom):
    if geom.geom_type == 'Polygon':
        return Polygon(geom.exterior)  # 내부 구멍 제거
    elif geom.geom_type == 'MultiPolygon':
        return MultiPolygon([Polygon(p.exterior) for p in geom.geoms])  # 각각의 폴리곤에 적용
    else:
        return geom  # 폴리곤이 아니면 그대로 반환

# ---------------------------
# 🟣 메인 함수 (SQL + SHP 생성)
# ---------------------------
async def generate_full_sql_and_shp(f):
    # 테이블 생성 쿼리
    f.write(f"DROP TABLE IF EXISTS border_table;\n\n")
    f.write(f"CREATE TABLE border_table (\n")
    f.write(f"    code VARCHAR(10) PRIMARY KEY,\n")
    f.write(f"    geom geometry(MultiPolygon, {output_srid})\n")
    f.write(");\n\n")

    for code in codes:
        input_shp = f"{source_path}/{code}/{code}.shp"
        gdf = gpd.read_file(input_shp)
        print(f"▶ {code} shp 읽기 완료")

        # 좌표계 변환
        gdf = gdf.to_crs(epsg=output_srid)

        # union + hole 제거만
        merged_geom = unary_union(gdf.geometry)
        no_hole_geom = remove_holes(merged_geom)

        # shp로 저장
        merged_gdf = gpd.GeoDataFrame(geometry=[no_hole_geom], crs=output_srid)
        output_shp = f"{result_path}/{code}.shp"
        merged_gdf.to_file(output_shp)
        print(f"✅ {code}.shp 저장 완료")

        # INSERT 쿼리 작성
        wkt = no_hole_geom.wkt
        insert_sql = f"INSERT INTO border_table (code, geom) VALUES ('{code}', ST_GeomFromText('{wkt}', {output_srid}));\n"
        f.write(insert_sql)
        print(f"✅ {code} INSERT 쿼리 생성 완료")

    # 공간 인덱스
    f.write(f"\nCREATE INDEX border_table_geom_idx ON border_table USING GIST (geom);\n")
    print(f"✅ 공간 인덱스 생성 쿼리 추가 완료")

# ---------------------------
# 🟣 진짜 메인
# ---------------------------
async def main():
    os.makedirs(result_path, exist_ok=True)   # result 폴더 없으면 생성
    with open(output_sql, "w", encoding="utf-8") as f:
        await generate_full_sql_and_shp(f)

    print(f"\n💾 SQL: {output_sql} 생성 완료")
    print(f"💾 SHP: {result_path} 폴더에 합쳐진 shapefile 저장 완료")

if __name__ == "__main__":
    asyncio.run(main())
