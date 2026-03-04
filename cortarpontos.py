import zipfile
import os
import geopandas as gpd
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

    dx = xmax - xmin
    dy = ymax - ymin

    if dx == 0:
        dx = 1

    if dy == 0:
        dy = 1

    xnorm = (xs - xmin) / dx
    ynorm = (ys - ymin) / dy

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

        inter = c.intersection(limite)

        if not inter.is_empty:
            cells.append(inter)

    return cells


# ============================================================
# DISSOLVER POR GRUPO
# ============================================================

def dissolver_por_grupo(gdf, cells):

    resultado = []

    for gid in sorted(gdf["gid"].unique()):

        subset = gdf[gdf["gid"] == gid]

        geoms = []

        for p in subset.geometry:

            if len(cells) > 0:
                cell = cells[np.random.randint(0, len(cells))]
                geoms.append(cell)

        if len(geoms) == 0:
            resultado.append(None)
        else:
            union = unary_union(geoms)
            resultado.append(union)

    return resultado


# ============================================================
# GERAR KMZ
# ============================================================

def gerar_kmz(poligono, pontos, caminho):

    kml = simplekml.Kml()

    def desenhar(geom):

        if geom is None:
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

    desenhar(poligono)

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

    # garantir pontos
    gdf = gdf[gdf.geometry.type == "Point"]

    if len(gdf) == 0:
        raise Exception("Shapefile não possui pontos válidos.")

    # recorte municipal
    if mun_geom is not None:

        mun = gpd.GeoSeries([mun_geom], crs=mun_crs).to_crs(gdf.crs)[0]

        gdf = gdf[gdf.geometry.within(mun)]

    if len(gdf) == 0:
        raise Exception("Nenhum ponto dentro do município.")

    # ordenar
    gdf = ordenar_pontos(gdf)

    # agrupar
    gdf = agrupar_pranchas(gdf, params["cap"])

    # limite
    limite = unary_union(gdf.geometry).convex_hull

    # voronoi
    cells = gerar_voronoi(gdf, limite)

    # dissolver
    grupos = dissolver_por_grupo(gdf, cells)

    saida = workdir / "resultado"
    saida.mkdir(exist_ok=True)

    for i, pol in enumerate(grupos):

        pts = gdf[gdf["gid"] == i]

        kmz = saida / f"prancha_{i+1}.kmz"

        gerar_kmz(pol, pts, kmz)

    zip_saida = workdir / "resultado.zip"

    with zipfile.ZipFile(zip_saida, "w") as z:

        for f in os.listdir(saida):

            z.write(saida / f, f)

    return zip_saida
