import geopandas as gpd
import pandas as pd
import numpy as np
import zipfile, tempfile, requests, io, os, pathlib, time
from tqdm import tqdm
from shapely.geometry import LineString, Point

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
    gdf_bus = []
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
                gdf_bus.append(gpd.read_file(filepath))
        # if i>3: break
        time.sleep(1)
    gdf_bus = pd.concat(gdf_bus)
    # print(gdf_bus.head())
    # print(gdf_bus.shape)
    gdf_bus = gdf_bus.rename(columns={"P11_001": "name"})
    gdf_bus["type"] = "BUS"
    gdf_bus["description"] = gdf_bus["P11_002"] + " " + gdf_bus["P11_003_01"]
    gdf_bus = gdf_bus[["name","type","description","geometry"]]

    # 高速バス
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P36-2023.html
    gdf_exbus = []
    for i in tqdm(range(1,48)):
        url = f"https://nlftp.mlit.go.jp/ksj/gml/data/P36/P36-23/P36-23_{i:02}_SHP.zip"
        r = requests.get(url)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            # shapeデータは.shpファイルなど複数に分かれており、同じフォルダにないと読み込めない
            with tempfile.TemporaryDirectory() as tmpdir:
                z.extractall(tmpdir)
                # items = os.listdir(tmpdir)
                # print(items)
                filepath = pathlib.Path(tmpdir) / pathlib.Path(f"P36-23_{i:02}_SHP/P36-23_{i:02}.shp")
                # filepath = os.path.join(tmpdir, f"P11-22_{i:02}_SHP/P11-22_{i:02}.shp")
                gdf_exbus.append(gpd.read_file(filepath))
        # if i>3: break
        time.sleep(1)
    gdf_exbus = pd.concat(gdf_exbus)
    # print(gdf_exbus.head())
    # print(gdf_exbus.shape)
    gdf_exbus = gdf_exbus.rename(columns={"P36_001": "name"})
    gdf_exbus["type"] = "EXBUS"
    gdf_exbus["description"] = gdf_exbus["P36_002"] + " " + gdf_exbus["P36_003_01"]
    gdf_exbus = gdf_exbus[["name","type","description","geometry"]]

    gdf = pd.concat([gdf_exbus, gdf_bus])
    gdf = gdf.groupby("geometry", as_index=False).agg({
                                                    "name": "first",
                                                    "type": "max",  # 高速バス優先
                                                    "description": lambda s: "<br>".join(s)
                                                })
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

def download_airport():
    # 空港
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-C28-2021.html
    url = "https://nlftp.mlit.go.jp/ksj/gml/data/C28/C28-21/C28-21_GML.zip"
    r = requests.get(url)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # shapeデータは.shpファイルなど複数に分かれており、同じフォルダにないと読み込めない
        with tempfile.TemporaryDirectory() as tmpdir:
            z.extractall(tmpdir)
            # print(tmpdir)
            items = os.listdir(tmpdir)
            print(items)
            filepath = pathlib.Path(tmpdir) / pathlib.Path("Shift-JIS/C28-21_Airport.shp")
            # filepath = os.path.join(tmpdir, "N02-24_GML/UTF-8/N02-24_Station.shp")
            gdf = gpd.read_file(filepath)

    print(gdf.head())
    print(gdf.shape)
    gdf = gdf.rename(columns={"C28_005": "name"})
    gdf = gdf[["name","geometry"]]
    gdf["description"] = ""
    gdf["type"] = "PORT"
    gdf["geometry"] = gdf["geometry"].centroid
    gdf = gdf.groupby("name").first()
    gdf = gdf.set_crs("EPSG:4612").to_crs(6668)
    print(gdf.head())
    print(gdf.shape)
    return gdf

def download_ferryport():
    # 旅客港
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N09.html
    url = "https://nlftp.mlit.go.jp/ksj/gml/data/N09/N09-12/N09-12_GML.zip"
    r = requests.get(url)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        # shapeデータは.shpファイルなど複数に分かれており、同じフォルダにないと読み込めない
        with tempfile.TemporaryDirectory() as tmpdir:
            z.extractall(tmpdir)
            # print(tmpdir)
            # items = os.listdir(tmpdir)
            # print(items)
            filepath = pathlib.Path(tmpdir) / pathlib.Path("N09-12_l.shp")
            gdf = gpd.read_file(filepath)
            # gdf = gdf.set_crs("EPSG:4612").to_crs(6668)

    # print(gdf.head())
    # print(gdf.columns)
    # print(gdf.shape)
    gdf = gdf.rename(columns={"N09_013": "name1", "N09_016": "name2"})
    gdf["description"] = gdf["N09_009"] + " " + gdf["N09_006"]
    gdf = gdf.assign(
        point1=gdf["geometry"].apply(lambda line: Point(line.coords[0])),
        point2=gdf["geometry"].apply(lambda line: Point(line.coords[1])),
    )
    gdf = gdf[["name1","name2","description","point1","point2"]]
    gdf1 = gdf[["name1","description","point1"]].copy()
    gdf2 = gdf[["name2","description","point2"]].copy()
    gdf1 = gdf1.rename(columns={"name1": "name", "point1": "geometry"})
    gdf2 = gdf2.rename(columns={"name2": "name", "point2": "geometry"})
    gdf = pd.concat([gdf1, gdf2])
    gdf = gdf.groupby("geometry").agg({
                                  "name": "first",
                                  "description": lambda s: "<br>".join(s),
                                  "geometry": "first"
                                }).reset_index(drop=True)
    gdf = gdf.groupby("name").agg({
                                  "description": lambda s: "<br>".join(s),
                                  "geometry": "first"
                                }).reset_index()
    gdf["type"] = "PORT"
    gdf = gpd.GeoDataFrame(gdf, crs="EPSG:4612").to_crs(6668)
    print(gdf.head())
    print(gdf.shape)
    return gdf

# def download_sightseeing():
#     # 観光資源
#     # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P12-2014.html
#     gdf = []
#     for i in tqdm(range(1,48)):
#         url = f"https://nlftp.mlit.go.jp/ksj/gml/data/P12/P12-14/P12-14_{i:02}_GML.zip"
#         r = requests.get(url)
#         r.raise_for_status()
#         with zipfile.ZipFile(io.BytesIO(r.content)) as z:
#             # shapeデータは.shpファイルなど複数に分かれており、同じフォルダにないと読み込めない
#             with tempfile.TemporaryDirectory() as tmpdir:
#                 z.extractall(tmpdir)
#                 # items = os.listdir(tmpdir)
#                 # print(items)
#                 filepath = pathlib.Path(tmpdir) / pathlib.Path(f"P12a-14_{i:02}.shp")
#                 gdf.append(gpd.read_file(filepath, encoding="cp932"))
#         if i>3: break
#         time.sleep(1)
#     gdf = pd.concat(gdf)
#     gdf = gdf.to_crs(6668)
#     print(gdf.shape)
#     return gdf


gdf = []
gdf.append(download_bus())
gdf.append(download_railway())
# gdf.append(download_sightseeing())
gdf.append(download_airport())
gdf.append(download_ferryport())
gdf = pd.concat(gdf)
gdf = gpd.GeoDataFrame(gdf)
gdf = gdf.to_crs(6668)
gdf.to_file("points.geojson", driver="GeoJSON")
