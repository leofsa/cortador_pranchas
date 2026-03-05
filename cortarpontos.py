import os
import math
import zipfile
import geopandas as gpd
import pandas as pd
import numpy as np
import simplekml

from shapely.geometry import MultiPoint
from shapely.ops import unary_union, voronoi_diagram
from pyproj import CRS


# ---------------------------------------------------
# helpers
# ---------------------------------------------------

def _is_geom(g):
    return g is not None and not g.is_empty


def _safe_unary_union(geoms):
    geoms = [g for g in geoms if _is_geom(g)]
    if not geoms:
        return None
    return unary_union(geoms).buffer(0)


# ---------------------------------------------------
# Morton spatial sort (igual desktop)
# ---------------------------------------------------

def _part1by1(n):
    n = (n | (n << 8)) & 0x00FF00FF
    n = (n | (n << 4)) & 0x0F0F0F0F
    n = (n | (n << 2)) & 0x33333333
    n = (n | (n << 1)) & 0x55555555
    return n


def morton_code(x, y):
    return _part1by1(x) | (_part1by1(y) << 1)


def spatial_sort(gdf):

    g = gdf.copy()

    xs = g.geometry.x
    ys = g.geometry.y

    xmin, xmax = xs.min(), xs.max()
    ymin, ymax = ys.min(), ys.max()

    dx = xmax - xmin if xmax != xmin else 1
    dy = ymax - ymin if ymax != ymin else 1

    x_norm = ((xs - xmin) / dx * 65535).astype(int)
    y_norm = ((ys - ymin) / dy * 65535).astype(int)

    g["__morton"] = morton_code(x_norm, y_norm)

    g = g.sort_values("__morton").drop(columns="__morton")

    return g.reset_index(drop=True)


# ---------------------------------------------------
# carregar municípios
# ---------------------------------------------------

def load_municipios():

    shp = "data/municipios.shp"

    mun = gpd.read_file(shp)

    if mun.crs is None:
        mun = mun.set_crs(4674)

    return mun


# ---------------------------------------------------
# guess UTM
# ---------------------------------------------------

def guess_utm(gdf):

    g = gdf.to_crs(4326)

    centroid = g.unary_union.centroid

    lon = centroid.x
    lat = centroid.y

    zone = int((lon + 180) // 6) + 1

    epsg = 32700 + zone if lat < 0 else 32600 + zone

    return CRS.from_epsg(epsg)


# ---------------------------------------------------
# grupos fixos
# ---------------------------------------------------

def assign_groups(gdf, cap):

    g = gdf.copy()

    g["__gid"] = np.arange(len(g)) // cap

    return g


# ---------------------------------------------------
# criar partições
# ---------------------------------------------------

def build_cells(points, boundary):

    geoms = [g for g in points.geometry if _is_geom(g)]

    if len(geoms) < 2:
        raise ValueError("Poucos pontos para gerar Voronoi")

    mp = MultiPoint(geoms)

    vd = voronoi_diagram(mp, envelope=boundary)

    cells = []

    for c in vd.geoms:

        c = c.intersection(boundary).buffer(0)

        if _is_geom(c):
            cells.append(c)

    return cells


# ---------------------------------------------------
# export excel
# ---------------------------------------------------

def export_excel(points, out):

    with pd.ExcelWriter(out, engine="openpyxl") as writer:

        n = points["__gid"].max() + 1

        for gid in range(n):

            g = points[points["__gid"] == gid]

            df = g.drop(columns="geometry")

            df["lon"] = g.geometry.x
            df["lat"] = g.geometry.y

            df.to_excel(writer, sheet_name=f"Prancha {gid+1}", index=False)


# ---------------------------------------------------
# export kmz
# ---------------------------------------------------

def export_kmz(points, cell, municipio, path):

    kml = simplekml.Kml()

    fol = kml.newfolder()

    for _, r in points.iterrows():

        if not _is_geom(r.geometry):
            continue

        p = fol.newpoint()

        p.coords = [(r.geometry.x, r.geometry.y)]

    if cell and _is_geom(cell):

        pol = fol.newpolygon()

        pol.outerboundaryis = list(cell.exterior.coords)

    if _is_geom(municipio):

        pol = fol.newpolygon()

        pol.outerboundaryis = list(municipio.exterior.coords)

    kml.savekmz(path)


# ---------------------------------------------------
# pipeline principal
# ---------------------------------------------------

def processar(shp_path, uf, municipio, cap, out_dir):

    mun = load_municipios()

    gdf = gpd.read_file(shp_path)

    if len(gdf) == 0:
        raise ValueError("Shapefile vazio")

    gdf = gdf[gdf.geometry.notnull()].copy()

    mun_geom = mun[
        (mun["SIGLA_UF"] == uf) &
        (mun["NM_MUN"] == municipio)
    ].geometry.iloc[0]

    # recorte mais seguro
    gdf = gdf[
        gdf.geometry.within(mun_geom) |
        gdf.geometry.touches(mun_geom)
    ]

    if len(gdf) == 0:
        raise ValueError("Nenhum ponto dentro do município")

    utm = guess_utm(gdf)

    gdf = gdf.to_crs(utm)

    mun_geom = gpd.GeoSeries(
        [mun_geom],
        crs=mun.crs
    ).to_crs(utm).iloc[0].buffer(0)

    # ordenar espacialmente
    gdf = spatial_sort(gdf)

    # criar grupos
    gdf = assign_groups(gdf, cap)

    # gerar voronoi
    cells = build_cells(gdf, mun_geom)

    gdf_wgs = gdf.to_crs(4326)

    os.makedirs(out_dir, exist_ok=True)

    excel = os.path.join(out_dir, "pranchas.xlsx")

    export_excel(gdf_wgs, excel)

    kmz_dir = os.path.join(out_dir, "kmz")

    os.makedirs(kmz_dir, exist_ok=True)

    n = gdf["__gid"].max() + 1

    for gid in range(n):

        pts = gdf_wgs[gdf_wgs["__gid"] == gid]

        cell = cells[gid] if gid < len(cells) else None

        kmz = os.path.join(kmz_dir, f"prancha_{gid+1}.kmz")

        export_kmz(pts, cell, mun_geom, kmz)

    zip_path = os.path.join(out_dir, "resultado.zip")

    with zipfile.ZipFile(zip_path, "w") as z:

        z.write(excel, "pranchas.xlsx")

        for f in os.listdir(kmz_dir):
            z.write(os.path.join(kmz_dir, f), f)

    return zip_path
