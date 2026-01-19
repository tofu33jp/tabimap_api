import os
import geopandas as gpd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from shapely.geometry import box
import ujson # 高速なJSON処理のため

app = FastAPI()

# 全てのドメインからのアクセスを許可（CORS設定）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# データの読み込み
# points.geojson は main.py と同じフォルダに置いてください
print("Loading data...")
gdf = gpd.read_parquet("points.parquet")
spatial_index = gdf.sindex
lines_gdf = gpd.read_parquet("lines.parquet")
lines_spatial_index = lines_gdf.sindex

@app.get("/points")
def get_points(west: float, south: float, east: float, north: float):
    bbox = box(west, south, east, north)
    possible_matches_index = list(spatial_index.intersection(bbox.bounds))
    subset = gdf.iloc[possible_matches_index]
    precise_matches = subset[subset.intersects(bbox)]
    
    # ujsonを使って高速に変換
    return ujson.loads(precise_matches.to_json())

@app.get("/lines")
def get_lines(west: float, south: float, east: float, north: float):
    bbox = box(west, south, east, north)
    possible_matches_index = list(lines_spatial_index.intersection(bbox.bounds))
    subset = lines_gdf.iloc[possible_matches_index]
    precise_matches = subset[subset.intersects(bbox)]
    
    # GeoJSON形式で返却
    return ujson.loads(precise_matches.to_json())

if __name__ == "__main__":
    # Renderのポート指定に対応
    port = int(os.environ.get("PORT", 8000))
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)
