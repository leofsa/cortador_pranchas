import os
import zipfile
import math
import numpy as np
import pandas as pd
import geopandas as gpd
import simplekml
from shapely.geometry import MultiPoint
from shapely.ops import unary_union, voronoi_diagram
from pyproj import CRS

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def _is_geom(g):
    return g is not None and not g.is_empty

def _safe_unary_union(geoms):
    geoms = [g for g in geoms if _is_geom(g)]
    if not geoms:
        return None
    # buffer(0) resolve problemas comuns de topologia inválida
    return unary_union(geoms).buffer(0)

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
    # Usa coordenadas do centroide caso não seja apenas pontos
    xs = g.geometry.centroid.x
    ys = g.geometry.centroid.y

    xmin, xmax = xs.min(), xs.max()
    ymin, ymax = ys.min(), ys.max()

    dx = (xmax - xmin) if xmax != xmin else 1.0
    dy = (ymax - ymin) if ymax != ymin else 1.0

    x_norm = ((xs - xmin) / dx * 65535).astype(int)
    y_norm = ((ys - ymin) / dy * 65535).astype(int)

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
        mun = mun.set_crs(4674) # SIRGAS 2000
    return mun

def detect_columns(mun):
    uf_col = next((c for c in mun.columns if "UF" in c), None)
    mun_col = next((c for c in mun.columns if "MUN" in c or "NOME" in c), None)
    if not uf_col or not mun_col:
        raise ValueError("Não foi possível detectar colunas de UF ou Município no GeoJSON")
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
    mp = MultiPoint(list(points.geometry))
    # Cria o diagrama e intersecta com o limite do município
    vd = voronoi_diagram(mp, envelope=boundary)
    cells = []
    for c in vd.geoms:
        intersected = c.intersection(boundary)
        if _is_geom(intersected):
            cells.append(intersected)
    return cells

def dissolve_por_grupo(points, cells):
    """
    Aqui corrigimos o erro de NaN usando 'inner' join e 
    evitando iteração manual lenta (iterrows).
    """
    cells_gdf = gpd.GeoDataFrame(
        {"cell_id": range(len(cells))},
        geometry=cells,
        crs=points.crs
    )

    # O 'inner' garante que apenas pontos que caíram em células sejam processados
    # evitando o erro de "float NaN to integer"
    join = gpd.sjoin(points[["__gid", "geometry"]], cells_gdf, how="inner", predicate="within")

    # Agrupa as células por grupo (gid)
    # Mapeamos os IDs das células para suas geometrias
    geom_map = cells_gdf['geometry'].to_dict()
    
    # Criamos um dicionário de {gid: [lista_de_geometrias]}
    grupos_geoms = join.groupby("__gid")["cell_id"].unique().to_dict()

    max_gid = int(points["__gid"].max())
    dissolvidos = []

    for gid in range(max_gid + 1):
        cids = grupos_geoms.get(gid, [])
        geoms_to_union = [geom_map[cid] for cid in cids]
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
            df["lon"] = g.geometry.x
            df["lat"] = g.geometry.y
            df.to_excel(writer, sheet_name=f"Prancha {int(gid)+1}", index=False)

def export_kmz(points_wgs, cell_wgs, mun_wgs, path):
    kml = simplekml.Kml()
    fol = kml.newfolder(name="Dados")

    # Pontos
    for idx, r in points_wgs.iterrows():
        p = fol.newpoint(name=f"Ponto {idx}")
        p.coords = [(r.geometry.x, r.geometry.y)]

    # Célula (Prancha)
    if cell_wgs and _is_geom(cell_wgs):
        if cell_wgs.geom_type == 'Polygon':
            pol = fol.newpolygon(name="Limite da Prancha")
            pol.outerboundaryis = list(cell_wgs.exterior.coords)
        elif cell_wgs.geom_type == 'MultiPolygon':
            for part in cell_wgs.geoms:
                pol = fol.newpolygon()
                pol.outerboundaryis = list(part.exterior.coords)

    # Limite Município (Opcional, pode ficar pesado se o mun for gigante)
    if mun_wgs and _is_geom(mun_wgs):
        if mun_wgs.geom_type == 'Polygon':
            m_pol = fol.newpolygon(name="Limite Municipal")
            m_pol.outerboundaryis = list(mun_wgs.exterior.coords)
            m_pol.style.linestyle.color = simplekml.Color.red
            m_pol.style.polystyle.fill = 0 

    kml.savekmz(path)

# ---------------------------------------------------
# Pipeline Principal
# ---------------------------------------------------

def processar(shp_path, uf, municipio, cap, out_dir):
    # 1. Carrega municípios e filtra o escolhido
    mun = load_municipios()
    uf_col, mun_col = detect_columns(mun)
    
    selecao = mun[(mun[uf_col] == uf) & (mun[mun_col].str.upper() == municipio.upper())]
    if selecao.empty:
        raise ValueError(f"Município {municipio}-{uf} não encontrado na base.")
    
    mun_geom_orig = selecao.geometry.iloc[0]

    # 2. Carrega pontos e filtra espacialmente
    gdf = gpd.read_file(shp_path)
    gdf = gdf[gdf.geometry.notnull()].copy()
    
    # Garante mesmo CRS para o filtro inicial
    if gdf.crs != mun.crs:
        gdf = gdf.to_crs(mun.crs)
        
    gdf = gdf[gdf.intersects(mun_geom_orig)].copy()
    if len(gdf) == 0:
        raise ValueError("Nenhum ponto encontrado dentro dos limites do município.")

    # 3. Projeção UTM para cálculos de área/distância
    utm_crs = guess_utm(gdf)
    gdf_utm = gdf.to_crs(utm_crs)
    mun_geom_utm = gpd.GeoSeries([mun_geom_orig], crs=mun.crs).to_crs(utm_crs).iloc[0]

    # 4. Ordenação e Agrupamento
    gdf_utm = spatial_sort(gdf_utm)
    gdf_utm["__gid"] = np.arange(len(gdf_utm)) // cap

    # 5. Voronoi e Dissolve
    cells_utm = build_cells(gdf_utm, mun_geom_utm)
    grupos_utm = dissolve_por_grupo(gdf_utm, cells_utm)

    # 6. Preparação para Exportação (Tudo em WGS84)
    gdf_wgs = gdf_utm.to_crs(4326)
    mun_geom_wgs = gpd.GeoSeries([mun_geom_utm], crs=utm_crs).to_crs(4326).iloc[0]
    
    os.makedirs(out_dir, exist_ok=True)
    
    # Excel
    excel_path = os.path.join(out_dir, "pranchas.xlsx")
    export_excel(gdf_wgs, excel_path)

    # KMZs individuais
    kmz_dir = os.path.join(out_dir, "kmz")
    os.makedirs(kmz_dir, exist_ok=True)
    
    max_gid = int(gdf_utm["__gid"].max())
    for gid in range(max_gid + 1):
        pts_prancha = gdf_wgs[gdf_wgs["__gid"] == gid]
        
        # Converte a célula do grupo para WGS84
        cell_utm = grupos_utm[gid]
        cell_wgs = None
        if cell_utm:
            cell_wgs = gpd.GeoSeries([cell_utm], crs=utm_crs).to_crs(4326).iloc[0]
        
        kmz_path = os.path.join(kmz_dir, f"prancha_{gid+1}.kmz")
        export_kmz(pts_prancha, cell_wgs, None, kmz_path) # Mun_wgs omitido para KMZ leve

    # 7. Zip Final
    zip_path = os.path.join(out_dir, "resultado.zip")
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(excel_path, arcname="pranchas.xlsx")
        for f in os.listdir(kmz_dir):
            z.write(os.path.join(kmz_dir, f), arcname=f"kmz/{f}")

    return zip_path
