import geopandas as gpd
import pandas as pd
import numpy as np
import zipfile, tempfile, requests, io, os, pathlib, time, warnings
from tqdm import tqdm
from shapely.geometry import LineString, Point

def fetch(url):
    r = requests.get(url)
    r.raise_for_status()
    return r.content

def unzip(zipdata, target_file):
    with zipfile.ZipFile(io.BytesIO(zipdata)) as z:
        # shapeデータは.shpファイルなど複数に分かれており、同じフォルダにないと読み込めない
        with tempfile.TemporaryDirectory() as tmpdir:
            z.extractall(tmpdir)
            filepath = pathlib.Path(tmpdir) / pathlib.Path(target_file)
            with warnings.catch_warnings():
                # 旅客港データで大量に出る
                warnings.filterwarnings(
                    "ignore",
                    message=r".*parsed incompletely to integer.*",
                    category=RuntimeWarning,
                )
                return gpd.read_file(filepath, encoding="cp932")

def download_bus():
    # バス停
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P11-2022.html
    gdf_bus = []
    for i in tqdm(range(1,48)):
        url = f"https://nlftp.mlit.go.jp/ksj/gml/data/P11/P11-22/P11-22_{i:02}_SHP.zip"
        target_shape = f"P11-22_{i:02}_SHP/P11-22_{i:02}.shp"
        gdf_bus.append(unzip(fetch(url), target_shape))
        # if i > 0: break
        time.sleep(1)

    gdf_bus = pd.concat(gdf_bus)
    gdf_bus = gdf_bus.rename(columns={"P11_001": "name"})
    gdf_bus["type"] = "BUS"
    gdf_bus["description"] = gdf_bus["P11_002"] + " " + gdf_bus["P11_003_01"]
    gdf_bus = gdf_bus[["name","type","description","geometry"]]

    # 高速バス
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P36-2023.html
    gdf_exbus = []
    for i in tqdm(range(1,48)):
        url = f"https://nlftp.mlit.go.jp/ksj/gml/data/P36/P36-23/P36-23_{i:02}_SHP.zip"
        target_shape = f"P36-23_{i:02}_SHP/P36-23_{i:02}.shp"
        gdf_exbus.append(unzip(fetch(url), target_shape))
        # if i > 0: break
        time.sleep(1)

    gdf_exbus = pd.concat(gdf_exbus)
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
    gdf = gpd.GeoDataFrame(gdf, crs="EPSG:6668")
    gdf.to_file("bus.geojson", driver="GeoJSON")
    return gdf

def download_busline():
    # バスライン
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N07-2022.html
    url = "https://nlftp.mlit.go.jp/ksj/gml/data/N07/N07-22/N07-22_SHP.zip"
    target_shape = "N07-22_SHP/N07-22.shp"
    gdf = unzip(fetch(url), target_shape)

    gdf = gdf["geometry"]
    gdf.to_file("busline.geojson", driver="GeoJSON")
    return gdf

def download_railway():
    # 鉄道
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N02-2024.html
    url = "https://nlftp.mlit.go.jp/ksj/gml/data/N02/N02-24/N02-24_GML.zip"
    target_shape = "Shift-JIS/N02-24_Station.shp"
    gdf = unzip(fetch(url), target_shape)

    gdf = gdf.rename(columns={"N02_005": "name", "N02_005c": "id", "N02_005g": "ref_id"})
    gdf["type"] = np.where(gdf["N02_002"].isin(["1","2"]), "RAILWAY(JR)", "RAILWAY(OTHER)")
    gdf["description"] = gdf["N02_004"] + " " + gdf["N02_003"]
    gdf["geometry"] = gdf["geometry"].centroid
    gdf_ref = gdf.query("id==ref_id")[["ref_id","geometry"]].copy()
    gdf = pd.merge(gdf[["ref_id","name","type","description"]], gdf_ref, on="ref_id", how="left")
    gdf = gdf.groupby("ref_id").agg({
                                  "name": "first",
                                  "type": "min",  # JR優先
                                  "description": lambda s: "<br>".join(s),
                                  "geometry": "first"
                                }).reset_index(drop=True)
    gdf = gpd.GeoDataFrame(gdf, crs="EPSG:6668")
    gdf.to_file("railway.geojson", driver="GeoJSON")
    return gdf

def download_airport():
    # 空港
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-C28-2021.html
    url = "https://nlftp.mlit.go.jp/ksj/gml/data/C28/C28-21/C28-21_GML.zip"
    target_shape = "Shift-JIS/C28-21_Airport.shp"
    gdf = unzip(fetch(url), target_shape)

    gdf = gdf.rename(columns={"C28_005": "name"})
    gdf = gdf[["name","geometry"]]
    gdf["description"] = "-"
    gdf["type"] = "PORT"
    gdf["geometry"] = gdf["geometry"].centroid
    gdf = gdf.groupby("name", as_index=False).first()
    gdf = gdf.set_crs("EPSG:4612").to_crs(6668)
    gdf.to_file("airport.geojson", driver="GeoJSON")
    return gdf

def download_ferryport():
    # 旅客港
    # SEE: https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-N09.html
    url = "https://nlftp.mlit.go.jp/ksj/gml/data/N09/N09-12/N09-12_GML.zip"
    target_shape = "N09-12_l.shp"
    gdf = unzip(fetch(url), target_shape)
    gdf["N09_008"] = gdf["N09_008"].astype(float)

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
    gdf = gdf.groupby("geometry", as_index=False).agg({
                                                  "name": "first",
                                                  "description": set
                                                })
    gdf["description"] = gdf["description"].apply(lambda s: "<br>".join(s))
    gdf = gdf.groupby("name", as_index=False).agg({
                                              "description": lambda s: "<br>".join(s),
                                              "geometry": "first"  # TMP
                                            })
    gdf["type"] = "PORT"
    gdf = gpd.GeoDataFrame(gdf, crs="EPSG:4612").to_crs(6668)
    gdf.to_file("ferryport.geojson", driver="GeoJSON")
    return gdf

gdf = []
gdf.append(download_bus())
gdf.append(download_railway())
gdf.append(download_airport())
gdf.append(download_ferryport())
gdf = pd.concat(gdf)
gdf = gpd.GeoDataFrame(gdf).to_crs(6668)
gdf.to_file("points.geojson", driver="GeoJSON")

gdf = download_busline()
gdf.to_file("lines.geojson", driver="GeoJSON")
