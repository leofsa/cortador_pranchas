import zipfile
import os
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import MultiPoint
from shapely.ops import voronoi_diagram, unary_union
from pathlib import Path
import simplekml


# ============================================================
# EXTRAIR ZIP
# ============================================================

def extrair_zip(zip_path, pasta):

    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(pasta)

    for f in os.listdir(pasta):
        if f.lower().endswith(".shp"):
            return os.path.join(pasta, f)

    raise Exception("Shapefile não encontrado dentro do ZIP")


# ============================================================
# ORDENAR PONTOS ESPACIALMENTE
# ============================================================

def ordenar_pontos(gdf):

    xs = gdf.geometry.x.values
    ys = gdf.geometry.y.values

    xmin, xmax = xs.min(), xs.max()
    ymin, ymax = ys.min(), ys.max()

    xnorm = (xs - xmin) / (xmax - xmin)
    ynorm = (ys - ymin) / (ymax - ymin)

    morton = (xnorm * 65535).astype(int) << 16 | (ynorm * 65535).astype(int)

    gdf["morton"] = morton

    return gdf.sort_values("morton").drop(columns=["morton"]).reset_index(drop=True)


# ============================================================
# AGRUPAR POR PRANCHA
# ============================================================

def agrupar_pranchas(gdf, cap):

    gdf["gid"] = np.arange(len(gdf)) // cap

    return gdf


# ============================================================
# GERAR POLÍGONOS VORONOI
# ============================================================

def gerar_voronoi(gdf, limite):

    mp = MultiPoint(list(gdf.geometry))

    vor = voronoi_diagram(mp, envelope=limite)

    cells = []

    for c in vor.geoms:
        cells.append(c.intersection(limite))

    return cells


# ============================================================
# DISSOLVER POR GRUPO
# ============================================================

def dissolver_por_grupo(gdf, cells):

    gdf["cell"] = cells[:len(gdf)]

    resultado = []

    for gid in sorted(gdf["gid"].unique()):

        subset = gdf[gdf["gid"] == gid]

        union = unary_union(subset["cell"])

        resultado.append(union)

    return resultado


# ============================================================
# GERAR KMZ
# ============================================================

def gerar_kmz(poligono, pontos, caminho):

    kml = simplekml.Kml()

    pol = kml.newpolygon()

    coords = [(x, y) for x, y in poligono.exterior.coords]

    pol.outerboundaryis = coords

    for p in pontos.geometry:

        pt = kml.newpoint()

        pt.coords = [(p.x, p.y)]

    kml.savekmz(caminho)


# ============================================================
# PROCESSAMENTO PRINCIPAL
# ============================================================

def processar_zip_shapefile(zip_path, params, workdir, mun_geom=None, mun_crs=None):

    pasta = workdir / "shape"

    pasta.mkdir()

    shp = extrair_zip(zip_path, pasta)

    gdf = gpd.read_file(shp)

    if mun_geom is not None:

        mun = gpd.GeoSeries([mun_geom], crs=mun_crs).to_crs(gdf.crs)[0]

        gdf = gdf[gdf.geometry.within(mun)]

    gdf = ordenar_pontos(gdf)

    gdf = agrupar_pranchas(gdf, params["cap"])

    limite = unary_union(gdf.geometry).convex_hull

    cells = gerar_voronoi(gdf, limite)

    grupos = dissolver_por_grupo(gdf, cells)

    saida = workdir / "resultado"

    saida.mkdir()

    for i, pol in enumerate(grupos):

        pts = gdf[gdf["gid"] == i]

        kmz = saida / f"prancha_{i+1}.kmz"

        gerar_kmz(pol, pts, kmz)

    zip_saida = workdir / "resultado.zip"

    with zipfile.ZipFile(zip_saida, "w") as z:

        for f in os.listdir(saida):

            z.write(saida / f, f)

    return zip_saida
