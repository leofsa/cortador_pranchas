import zipfile
import os
import math
import numpy as np
import geopandas as gpd
import pandas as pd
import simplekml

from shapely.geometry import MultiPoint
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union, voronoi_diagram


# ============================================================
# GEOMETRIA SEGURA
# ============================================================

def _is_geom(g):
    return isinstance(g, BaseGeometry) and not g.is_empty


def _safe_unary_union(iterable):
    geoms = [g for g in iterable if _is_geom(g)]
    if not geoms:
        return None
    return unary_union(geoms).buffer(0)


# ============================================================
# EXTRAIR ZIP
# ============================================================

def extrair_zip(zip_path, pasta):

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(pasta)

    for f in os.listdir(pasta):
        if f.lower().endswith(".shp"):
            return os.path.join(pasta, f)

    raise Exception("Shapefile não encontrado no ZIP")


# ============================================================
# MORTON ORDER (igual desktop)
# ============================================================

def ordenar_pontos(gdf):

    xs = gdf.geometry.x.values
    ys = gdf.geometry.y.values

    xmin, xmax = xs.min(), xs.max()
    ymin, ymax = ys.min(), ys.max()

    dx = xmax - xmin if xmax != xmin else 1
    dy = ymax - ymin if ymax != ymin else 1

    xnorm = (xs - xmin) / dx
    ynorm = (ys - ymin) / dy

    morton = (xnorm * 65535).astype(int) << 16 | (ynorm * 65535).astype(int)

    gdf["__morton"] = morton

    return gdf.sort_values("__morton").drop(columns=["__morton"]).reset_index(drop=True)


# ============================================================
# AGRUPAMENTO FIXO
# ============================================================

def agrupar_pranchas(gdf, cap):

    gdf["__gid"] = np.arange(len(gdf)) // cap

    return gdf


# ============================================================
# NORMALIZAR PARTIÇÃO
# ============================================================

def normalize_partition(polys, boundary):

    cleaned = []
    union_prev = None

    for p in polys:

        if not _is_geom(p):
            cleaned.append(None)
            continue

        p2 = p.intersection(boundary).buffer(0)

        if union_prev is not None and _is_geom(union_prev):
            p2 = p2.difference(union_prev).buffer(0)

        cleaned.append(p2)

        union_prev = p2 if union_prev is None else unary_union([union_prev, p2])

    return cleaned


# ============================================================
# SUAVIZAR
# ============================================================

def smooth_polygons(polys, boundary, smooth):

    if smooth <= 0:
        return polys

    sm = []

    for p in polys:

        if not _is_geom(p):
            sm.append(None)
            continue

        p2 = p.buffer(smooth).buffer(-smooth)

        p2 = p2.intersection(boundary).buffer(0)

        sm.append(p2)

    return normalize_partition(sm, boundary)


# ============================================================
# GERAR VORONOI
# ============================================================

def gerar_voronoi(points, limite):

    mp = MultiPoint(list(points.geometry))

    vd = voronoi_diagram(mp, envelope=limite)

    cells = []

    for c in vd.geoms:

        cc = c.intersection(limite).buffer(0)

        if _is_geom(cc):
            cells.append(cc)

    return cells


# ============================================================
# DISSOLVER
# ============================================================

def dissolver_por_grupo(points, cells):

    gid_to_cells = {}

    for _, r in points.iterrows():

        p = r.geometry
        gid = int(r["__gid"])

        for c in cells:

            if c.covers(p):
                gid_to_cells.setdefault(gid, []).append(c)
                break

    n_groups = int(points["__gid"].max()) + 1

    dissolved = []

    for gid in range(n_groups):

        poly = _safe_unary_union(gid_to_cells.get(gid, []))

        dissolved.append(poly)

    return dissolved


# ============================================================
# KMZ
# ============================================================

def gerar_kmz(poligono, pontos, caminho):

    kml = simplekml.Kml()

    def draw(geom):

        if not _is_geom(geom):
            return

        if geom.geom_type == "Polygon":

            coords = [(x, y) for x, y in geom.exterior.coords]

            pol = kml.newpolygon()
            pol.outerboundaryis = coords

        elif geom.geom_type == "MultiPolygon":

            for part in geom.geoms:

                coords = [(x, y) for x, y in part.exterior.coords]

                pol = kml.newpolygon()
                pol.outerboundaryis = coords

    draw(poligono)

    for p in pontos.geometry:

        pt = kml.newpoint()

        pt.coords = [(p.x, p.y)]

    kml.savekmz(caminho)


# ============================================================
# PROCESSAMENTO PRINCIPAL
# ============================================================

def processar_zip_shapefile(zip_path, params, workdir, mun_geom=None, mun_crs=None):

    pasta = workdir / "shape"
    pasta.mkdir(exist_ok=True)

    shp = extrair_zip(zip_path, pasta)

    gdf = gpd.read_file(shp)

    gdf = gdf[gdf.geometry.type == "Point"]

    if len(gdf) == 0:
        raise Exception("Shapefile não possui pontos.")

    # recorte municipal
    if mun_geom is not None:

        mun = gpd.GeoSeries([mun_geom], crs=mun_crs).to_crs(gdf.crs)[0]

        gdf = gdf[gdf.geometry.within(mun)]

    if len(gdf) == 0:
        raise Exception("Nenhum ponto dentro do município.")

    # ordenar
    gdf = ordenar_pontos(gdf)

    # agrupar
    cap = params["cap"]

    gdf = agrupar_pranchas(gdf, cap)

    limite = unary_union(gdf.geometry).convex_hull

    # voronoi
    cells = gerar_voronoi(gdf, limite)

    # dissolve
    grupos = dissolver_por_grupo(gdf, cells)

    # normalizar
    grupos = normalize_partition(grupos, limite)

    # suavizar
    grupos = smooth_polygons(grupos, limite, params.get("smooth_m", 50))

    # saída
    saida = workdir / "resultado"
    saida.mkdir(exist_ok=True)

    for i, pol in enumerate(grupos):

        pts = gdf[gdf["__gid"] == i]

        kmz = saida / f"prancha_{i+1}.kmz"

        gerar_kmz(pol, pts, kmz)

    zip_saida = workdir / "resultado.zip"

    with zipfile.ZipFile(zip_saida, "w") as z:

        for f in os.listdir(saida):

            z.write(saida / f, f)

    return zip_saida
