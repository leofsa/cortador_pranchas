import os
import zipfile
import numpy as np
import pandas as pd
import geopandas as gpd
import simplekml
import unicodedata

from shapely.geometry import MultiPoint
from shapely.ops import unary_union, voronoi_diagram
from shapely.validation import make_valid
from pyproj import CRS

# ---------------------------------------------------
# Helpers de Normalização e Geometria
# ---------------------------------------------------

def _is_geom(g):
    return g is not None and not g.is_empty

def _limpar_geometria(g):
    """Corrige problemas comuns de topologia em Shapefiles."""
    if not _is_geom(g):
        return None
    if not g.is_valid:
        g = make_valid(g)
    return g.buffer(0)

def _safe_unary_union(geoms):
    geoms = [_limpar_geometria(g) for g in geoms if _is_geom(g)]
    if not geoms:
        return None
    return unary_union(geoms).buffer(0)

def remover_acentos(txt):
    """Normaliza strings para comparação (remove acentos e espaços extras)."""
    if not isinstance(txt, str):
        return str(txt).upper().strip()
    return "".join(c for c in unicodedata.normalize('NFD', txt)
                   if unicodedata.category(c) != 'Mn').upper().strip()

# ---------------------------------------------------
# Morton Spatial Sort (Z-Order Curve)
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
    if len(gdf) == 0:
        return gdf
    g = gdf.copy()
    xs = g.geometry.centroid.x
    ys = g.geometry.centroid.y

    xmin, xmax = xs.min(), xs.max()
    ymin, ymax = ys.min(), ys.max()

    dx = (xmax - xmin) if xmax != xmin else 1.0
    dy = (ymax - ymin) if ymax != ymin else 1.0

    x_norm = ((xs - xmin) / dx * 65535).fillna(0).astype(int)
    y_norm = ((ys - ymin) / dy * 65535).fillna(0).astype(int)

    g["__morton"] = [morton_code(x, y) for x, y in zip(x_norm, y_norm)]
    g = g.sort_values("__morton").drop(columns="__morton")
    return g.reset_index(drop=True)

# ---------------------------------------------------
# Funções de Dados e UTM
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

def detect_columns(mun):
    cols = list(mun.columns)
    uf_col = "SIGLA_UF" if "SIGLA_UF" in cols else next((c for c in cols if "UF" in c), None)
    mun_col = "NM_MUN" if "NM_MUN" in cols else next((c for c in cols if "MUN" in c or "NOME" in c), None)
    
    if not uf_col or not mun_col:
        raise ValueError("Não foi possível detectar colunas de UF ou Município.")
    return uf_col, mun_col

def guess_utm(gdf):
    g = gdf.to_crs(4326)
    centroid = g.geometry.unary_union.centroid
    lon, lat = centroid.x, centroid.y
    zone = int((lon + 180) // 6) + 1
    epsg = 32700 + zone if lat < 0 else 32600 + zone
    return CRS.from_epsg(epsg)

# ---------------------------------------------------
# Processamento de Grupos e Voronoi
# ---------------------------------------------------

def build_cells(points, boundary):
    boundary = _limpar_geometria(boundary)
    mp = MultiPoint(list(points.geometry))
    vd = voronoi_diagram(mp, envelope=boundary)
    cells = []
    for c in vd.geoms:
        intersected = c.intersection(boundary)
        if _is_geom(intersected):
            cells.append(_limpar_geometria(intersected))
    return cells

def dissolve_por_grupo(points, cells):
    cells_gdf = gpd.GeoDataFrame(
        {"cell_id": range(len(cells))},
        geometry=cells,
        crs=points.crs
    )

    # Limpeza preventiva de geometrias nulas nos pontos
    points = points[points.geometry.notnull()].copy()

    # Join INNER garante que células sem pontos ou pontos fora de células sejam ignorados
    join = gpd.sjoin(points[["__gid", "geometry"]], cells_gdf, how="inner", predicate="within")

    geom_map = cells_gdf['geometry'].to_dict()
    grupos_geoms = join.groupby("__gid")["cell_id"].unique().to_dict()

    max_gid = int(points["__gid"].max())
    dissolvidos = []

    for gid in range(max_gid + 1):
        cids = grupos_geoms.get(gid, [])
        geoms_to_union = [geom_map[cid] for cid in cids if cid in geom_map]
        dissolvidos.append(_safe_unary_union(geoms_to_union))

    return dissolvidos

# ---------------------------------------------------
# Exportação
# ---------------------------------------------------

def export_excel(points_wgs, out_path):
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for gid in sorted(points_wgs["__gid"].unique()):
            g = points_wgs[points_wgs["__gid"] == gid].copy()
            df = pd.DataFrame(g.drop(columns="geometry"))
            df["LON"] = g.geometry.x
            df["LAT"] = g.geometry.y
            df.to_excel(writer, sheet_name=f"Prancha {int(gid)+1}", index=False)

def export_kmz(points_wgs, cell_wgs, path):
    kml = simplekml.Kml()
    fol = kml.newfolder(name="Dados")

    for idx, r in points_wgs.iterrows():
        p = fol.newpoint(name=f"Ponto {idx}")
        p.coords = [(r.geometry.x, r.geometry.y)]

    if cell_wgs and _is_geom(cell_wgs):
        if cell_wgs.geom_type == 'Polygon':
            pol = fol.newpolygon(name="Limite da Prancha")
            pol.outerboundaryis = list(cell_wgs.exterior.coords)
        elif cell_wgs.geom_type == 'MultiPolygon':
            for i, part in enumerate(cell_wgs.geoms):
                pol = fol.newpolygon(name=f"Parte {i}")
                pol.outerboundaryis = list(part.exterior.coords)

    kml.savekmz(path)

# ---------------------------------------------------
# Pipeline Principal
# ---------------------------------------------------

def processar(shp_path, uf, municipio, cap, out_dir):
    mun = load_municipios()
    uf_col, mun_col = detect_columns(mun)
    
    # Busca Robusta: normaliza entrada e base
    uf_alvo = uf.strip().upper()
    mun_alvo = remover_acentos(municipio)
    mun["__NORM"] = mun[mun_col].apply(remover_acentos)
    
    selecao = mun[(mun[uf_col].str.strip().upper() == uf_alvo) & (mun["__NORM"] == mun_alvo)]
    
    if selecao.empty:
        opcoes = list(mun[mun[uf_col] == uf_alvo][mun_col].unique()[:5])
        raise ValueError(f"Município '{municipio}' não encontrado em {uf}. Sugestões: {opcoes}")
    
    mun_geom_orig = _limpar_geometria(selecao.geometry.iloc[0])

    gdf = gpd.read_file(shp_path)
    gdf = gdf[gdf.geometry.notnull()].copy()
    if gdf.crs != mun.crs:
        gdf = gdf.to_crs(mun.crs)
        
    gdf = gdf[gdf.intersects(mun_geom_orig)].copy()
    if len(gdf) == 0:
        raise ValueError("Nenhum ponto encontrado dentro dos limites do município.")

    utm_crs = guess_utm(gdf)
    gdf_utm = gdf.to_crs(utm_crs)
    mun_geom_utm = gpd.GeoSeries([mun_geom_orig], crs=mun.crs).to_crs(utm_crs).iloc[0]

    gdf_utm = spatial_sort(gdf_utm)
    gdf_utm["__gid"] = np.arange(len(gdf_utm)) // cap

    cells_utm = build_cells(gdf_utm, mun_geom_utm)
    grupos_utm = dissolve_por_grupo(gdf_utm, cells_utm)

    gdf_wgs = gdf_utm.to_crs(4326)
    os.makedirs(out_dir, exist_ok=True)
    
    excel_path = os.path.join(out_dir, "pranchas.xlsx")
    export_excel(gdf_wgs, excel_path)

    kmz_dir = os.path.join(out_dir, "kmz")
    os.makedirs(kmz_dir, exist_ok=True)
    
    max_gid = int(gdf_utm["__gid"].max())
    for gid in range(max_gid + 1):
        pts_prancha = gdf_wgs[gdf_wgs["__gid"] == gid]
        cell_utm = grupos_utm[gid]
        cell_wgs = None
        if cell_utm:
            cell_wgs = gpd.GeoSeries([cell_utm], crs=utm_crs).to_crs(4326).iloc[0]
        
        kmz_path = os.path.join(kmz_dir, f"prancha_{gid+1}.kmz")
        export_kmz(pts_prancha, cell_wgs, kmz_path)

    zip_path = os.path.join(out_dir, "resultado.zip")
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(excel_path, arcname="pranchas.xlsx")
        for f in os.listdir(kmz_dir):
            z.write(os.path.join(kmz_dir, f), arcname=f"kmz/{f}")

    return zip_path
