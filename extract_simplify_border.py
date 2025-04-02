import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import Polygon, MultiPolygon
import asyncio
import sys
import os

source_path = "F:/border/source"
result_path = "F:/border/result"

codes = ['11', '26', '27', '28', '29', '30', '31', '36', '41', '42', '43', '44', '45', '46', '47', '48', '50']

simplify_tolerance = 50  # 적절히 조절

# 🟣 구멍(holes) 제거 함수
def remove_holes(geom):
    if geom.geom_type == 'Polygon':
        return Polygon(geom.exterior)  # 내부 구멍 제거
    elif geom.geom_type == 'MultiPolygon':
        return MultiPolygon([Polygon(p.exterior) for p in geom.geoms])  # 각각의 폴리곤에 적용
    else:
        return geom  # 폴리곤이 아니면 그대로 반환

async def extract_border(input_shp, output_shp, code):
    gdf = gpd.read_file(input_shp)
    merged_geom = unary_union(gdf.geometry)

    # 🟣 내부 구멍 제거
    no_hole_geom = remove_holes(merged_geom)

    # 🟣 외곽선 단순화
    simplified_geom = no_hole_geom.simplify(tolerance=simplify_tolerance)

    merged_gdf = gpd.GeoDataFrame(geometry=[simplified_geom], crs=gdf.crs)

    # 좌표 출력
    print(f"=== {code} 좌표 목록 (구멍 제거 + 단순화 적용) ===")
    if simplified_geom.geom_type == 'Polygon':
        for coord in list(simplified_geom.exterior.coords):
            print(coord)
    elif simplified_geom.geom_type == 'MultiPolygon':
        for poly in simplified_geom.geoms:
            for coord in list(poly.exterior.coords):
                print(coord)
    else:
        print(f"{code} is not a Polygon or MultiPolygon")

    # 저장
    merged_gdf.to_file(output_shp)

async def main():
    for code in codes:
        input_shp = f"{source_path}/{code}/{code}.shp"
        output_shp = f"{result_path}/{code}.shp"
        await extract_border(input_shp, output_shp, code)

if __name__ == "__main__":
    asyncio.run(main())
