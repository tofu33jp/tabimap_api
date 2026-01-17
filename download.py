import geopandas as gpd
import pandas as pd
import numpy as np
import zipfile, tempfile, requests, io, os, pathlib
from tqdm import tqdm

# def fetch(url):
#     r = requests.get(url)
#     r.raise_for_status()
#     return r.content

# def unzip(zipdata):
#     with tempfile.TemporaryDirectory() as tmpdir:
#         with zipfile.ZipFile(io.BytesIO(zipdata)) as z:
#             filename = [name for name in z.namelist() if name.lower().endswith(".shp")]
#             filepath = pathlib.Path(tmpdir) / pathlib.Path(filename[0])
#             filedir = pathlib.Path(tmpdir) / pathlib.Path(filename[0]).stem
#             filedir.mkdir(exist_ok=True)
#             with open(filepath, "wb") as f:
#                 f.write(z.read(filename[0]))

    # with zipfile.ZipFile(io.BytesIO(zipdata)) as z:
    #     filename = [name for name in z.namelist() if name.lower().endswith(".shp")]
    #     # print(filename)
    #     with z.open(filename[0]) as f:
    #         return gpd.read_file(f)

def download_bus():
    # バス停
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P11-2022.html
    gdf = []
    for i in tqdm(range(1,48)):
        url = f"https://nlftp.mlit.go.jp/ksj/gml/data/P11/P11-22/P11-22_{i:02}_SHP.zip"
        r = requests.get(url)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            # shapeデータは.shpファイルなど複数に分かれており、同じフォルダにないと読み込めない
            with tempfile.TemporaryDirectory() as tmpdir:
                z.extractall(tmpdir)
                filepath = pathlib.Path(tmpdir) / pathlib.Path(f"P11-22_{i:02}_SHP/P11-22_{i:02}.shp")
                # filepath = os.path.join(tmpdir, f"P11-22_{i:02}_SHP/P11-22_{i:02}.shp")
                gdf.append(gpd.read_file(filepath))
        # if i>3: break
    gdf = pd.concat(gdf)
    # print(gdf.head())
    # print(gdf.shape)
    gdf = gdf[["P11_001","P11_002","P11_003_01","geometry"]]
    gdf = gdf.rename(columns={"P11_001": "name"})
    gdf["description"] = gdf["P11_002"] + " " + gdf["P11_003_01"]
    gdf = gdf.groupby("geometry", as_index=False).agg({
                                                    "name": "first",
                                                    "description": lambda s: "<br>".join(s)
                                                })
    gdf["type"] = "BUS"
    print(gdf.head())
    print(gdf.shape)
    return gdf

def download_railway():
    # 鉄道
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N02-2024.html
    url = "https://nlftp.mlit.go.jp/ksj/gml/data/N02/N02-24/N02-24_GML.zip"
    r = requests.get(url)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # shapeデータは.shpファイルなど複数に分かれており、同じフォルダにないと読み込めない
        with tempfile.TemporaryDirectory() as tmpdir:
            z.extractall(tmpdir)
            # print(tmpdir)
            # items = os.listdir(tmpdir)
            # print(items)
            filepath = pathlib.Path(tmpdir) / pathlib.Path("Shift-JIS/N02-24_Station.shp")
            # filepath = os.path.join(tmpdir, "N02-24_GML/UTF-8/N02-24_Station.shp")
            gdf = gpd.read_file(filepath)

    # print(gdf.head())
    # print(gdf.shape)
    gdf = gdf.rename(columns={"N02_005": "name", "N02_005c": "id", "N02_005g": "ref_id"})
    # print(len(gdf["ref_id"].drop_duplicates()))
    gdf["type"] = np.where(gdf["N02_002"].isin(["1","2"]), "RAILWAY(JR)", "RAILWAY(OTHER)")
    gdf["description"] = gdf["N02_004"] + " " + gdf["N02_003"]
    gdf["geometry"] = gdf["geometry"].centroid
    # gdf["geometry"].interpolate(gdf["geometry"].length / 2)
    gdf_ref = gdf.query("id==ref_id")[["ref_id","geometry"]].copy()
    gdf = pd.merge(gdf[["ref_id","name","type","description"]], gdf_ref, on="ref_id", how="left")
    gdf = gdf.groupby("ref_id").agg({
                                  "name": "first",
                                  "type": "min",  # JR優先
                                  "description": lambda s: "<br>".join(s),
                                  "geometry": "first"
                                }).reset_index(drop=True)
    print(gdf.head())
    print(gdf.shape)
    return gdf


gdf = []
gdf.append(download_bus())
gdf.append(download_railway())
gdf = pd.concat(gdf)
gdf = gpd.GeoDataFrame(gdf)
gdf.to_file("points.geojson", driver="GeoJSON")
