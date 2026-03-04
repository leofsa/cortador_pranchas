import zipfile
import os
import numpy as np
import geopandas as gpd
from shapely.geometry import MultiPoint
from shapely.ops import voronoi_diagram, unary_union
from shapely.geometry.base import BaseGeometry
import simplekml


# ============================================================
# GEOMETRIA SEGURA
# ============================================================

def _is_geom(g):
    return isinstance(g, BaseGeometry) and not g.is_empty


def _safe_unary_union(geoms):
    g = [x for x in geoms if _is_geom(x)]
    if not g:
        return None
    return unary_union(g).buffer(0)


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
# MORTON ORDER
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
# AGRUPAMENTO
# ============================================================

def agrupar_pranchas(gdf, cap):

    gdf["__gid"] = np.arange(len(gdf)) // cap

    return gdf


# ============================================================
# GERAR PARTIÇÃO VORONOI
# ============================================================

def gerar_poligonos(points, limite):

    mp = MultiPoint(list(points.geometry))

    vd = voronoi_diagram(mp, envelope=limite)

    cells = []

    for c in vd.geoms:

        p = c.intersection(limite)

        if _is_geom(p):
            cells.append(p)

    return cells


# ============================================================
# DISSOLVER POR GRUPO
# ============================================================

def dissolver_por_grupo(points, cells):

    resultado = []

    for gid in sorted(points["__gid"].unique()):

        subset = points[points["__gid"] == gid]

        polys = []

        for p in subset.geometry:

            for c in cells:

                if c.covers(p):
                    polys.append(c)
                    break

        poly = _safe_unary_union(polys)

        resultado.append(poly)

    return resultado


# ============================================================
# KMZ
# ============================================================

def gerar_kmz(poligono, pontos, caminho):

    kml = simplekml.Kml()

    def desenhar(geom):

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

    gdf = gdf[gdf.geometry.type == "Point"]

    if len(gdf) == 0:
        raise Exception("Shapefile não possui pontos.")

    # recorte municipal
    if mun_geom is not None:

        mun = gpd.GeoSeries([mun_geom], crs=mun_crs).to_crs(gdf.crs)[0]

        gdf = gdf[gdf.geometry.within(mun)]

    if len(gdf) == 0:
        raise Exception("Nenhum ponto dentro do município.")

    # ordenar espacialmente
    gdf = ordenar_pontos(gdf)

    # agrupar
    cap = params["cap"]
    gdf = agrupar_pranchas(gdf, cap)

    limite = unary_union(gdf.geometry).convex_hull

    # gerar células
    cells = gerar_poligonos(gdf, limite)

    # dissolver
    grupos = dissolver_por_grupo(gdf, cells)

    saida = workdir / "resultado"
    saida.mkdir(exist_ok=True)

    for i, pol in enumerate(grupos):

        pts = gdf[gdf["__gid"] == i]

        kmz = saida / f"prancha_{i+1}.kmz"

        gerar_kmz(pol, pts, kmz)

    # zip final
    zip_saida = workdir / "resultado.zip"

    with zipfile.ZipFile(zip_saida, "w") as z:

        for f in os.listdir(saida):

            z.write(saida / f, f)

    return zip_saida
