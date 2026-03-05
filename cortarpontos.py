import os
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
# Morton spatial sort
# ---------------------------------------------------

def _part1by1(n):
    n = int(n) & 0xFFFFFFFF
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

    dx = (xmax - xmin) if xmax != xmin else 1.0
    dy = (ymax - ymin) if ymax != ymin else 1.0

    x_norm = ((xs - xmin) / dx * 65535).astype(int)
    y_norm = ((ys - ymin) / dy * 65535).astype(int)

    g["__morton"] = [
        morton_code(int(x), int(y)) for x, y in zip(x_norm, y_norm)
    ]

    g = g.sort_values("__morton").drop(columns="__morton")

    return g.reset_index(drop=True)


# ---------------------------------------------------
# carregar municípios
# ---------------------------------------------------

def load_municipios():

    path = "data/Municipios.geojson"

    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    mun = gpd.read_file(path)

    mun.columns = [c.upper() for c in mun.columns]

    if "GEOMETRY" in mun.columns:
        mun = mun.set_geometry("GEOMETRY")

    if mun.crs is None:
        mun = mun.set_crs(4674)

    return mun


# ---------------------------------------------------
# detectar colunas
# ---------------------------------------------------

def detect_columns(mun):

    uf_col = None
    mun_col = None

    for c in mun.columns:

        if "UF" in c:
            uf_col = c

        if "MUN" in c or "NOME" in c:
            mun_col = c

    if uf_col is None or mun_col is None:
        raise ValueError("Não foi possível detectar colunas de UF ou Município")

    return uf_col, mun_col


# ---------------------------------------------------
# guess utm
# ---------------------------------------------------

def guess_utm(gdf):

    g = gdf.to_crs(4326)

    centroid = g.geometry.unary_union.centroid

    lon = centroid.x
    lat = centroid.y

    zone = int((lon + 180) // 6) + 1

    epsg = 32700 + zone if lat < 0 else 32600 + zone

    return CRS.from_epsg(epsg)


# ---------------------------------------------------
# grupos
# ---------------------------------------------------

def assign_groups(gdf, cap):

    g = gdf.copy()

    g["__gid"] = np.arange(len(g)) // cap

    return g


# ---------------------------------------------------
# voronoi
# ---------------------------------------------------

def build_cells(points, boundary):

    mp = MultiPoint(list(points.geometry))

    vd = voronoi_diagram(mp, envelope=boundary)

    cells = []

    for c in vd.geoms:

        c = c.intersection(boundary).buffer(0)

        if _is_geom(c):
            cells.append(c)

    return cells


# ---------------------------------------------------
# dissolve
# ---------------------------------------------------

def dissolve_por_grupo(points, cells):

    cells_gdf = gpd.GeoDataFrame(
        {"cell_id": range(len(cells))},
        geometry=cells,
        crs=points.crs
    )

    join = gpd.sjoin(
        points[["__gid", "geometry"]],
        cells_gdf,
        how="left",
        predicate="within"
    )

    grupos = {}

    for _, r in join.iterrows():

        gid = int(r["__gid"])
        cid = int(r["cell_id"])

        geom = cells_gdf.loc[cid, "geometry"]

        grupos.setdefault(gid, []).append(geom)

    n = points["__gid"].max() + 1

    dissolvidos = []

    for gid in range(n):

        uu = _safe_unary_union(grupos.get(gid, []))

        dissolvidos.append(uu)

    return dissolvidos


# ---------------------------------------------------
# excel
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
# kmz
# ---------------------------------------------------

def export_kmz(points, cell, municipio, path):

    kml = simplekml.Kml()

    fol = kml.newfolder()

    for _, r in points.iterrows():

        p = fol.newpoint()
        p.coords = [(r.geometry.x, r.geometry.y)]

    if cell and _is_geom(cell):

        pol = fol.newpolygon()
        pol.outerboundaryis = list(cell.exterior.coords)

    pol = fol.newpolygon()
    pol.outerboundaryis = list(municipio.exterior.coords)

    kml.savekmz(path)


# ---------------------------------------------------
# pipeline
# ---------------------------------------------------

def processar(shp_path, uf, municipio, cap, out_dir):

    mun = load_municipios()

    uf_col, mun_col = detect_columns(mun)

    gdf = gpd.read_file(shp_path)

    gdf = gdf[gdf.geometry.notnull()].copy()

    mun_geom = mun[
        (mun[uf_col] == uf) &
        (mun[mun_col].str.upper() == municipio.upper())
    ].geometry.iloc[0]

    gdf = gdf[
        gdf.geometry.within(mun_geom) |
        gdf.geometry.touches(mun_geom)
    ]

    if len(gdf) == 0:
        raise ValueError("Nenhum ponto dentro do município")

    utm = guess_utm(gdf)

    gdf = gdf.to_crs(utm)

    mun_geom = gpd.GeoSeries([mun_geom], crs=mun.crs).to_crs(utm).iloc[0]

    gdf = spatial_sort(gdf)

    gdf = assign_groups(gdf, cap)

    cells = build_cells(gdf, mun_geom)

    grupos = dissolve_por_grupo(gdf, cells)

    gdf_wgs = gdf.to_crs(4326)

    os.makedirs(out_dir, exist_ok=True)

    excel = os.path.join(out_dir, "pranchas.xlsx")

    export_excel(gdf_wgs, excel)

    kmz_dir = os.path.join(out_dir, "kmz")

    os.makedirs(kmz_dir, exist_ok=True)

    n = gdf["__gid"].max() + 1

    for gid in range(n):

        pts = gdf_wgs[gdf_wgs["__gid"] == gid]

        cell = grupos[gid]

        kmz = os.path.join(kmz_dir, f"prancha_{gid+1}.kmz")

        export_kmz(pts, cell, mun_geom, kmz)

    zip_path = os.path.join(out_dir, "resultado.zip")

    with zipfile.ZipFile(zip_path, "w") as z:

        z.write(excel, "pranchas.xlsx")

        for f in os.listdir(kmz_dir):
            z.write(os.path.join(kmz_dir, f), f)

    return zip_path
