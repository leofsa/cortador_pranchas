import os
import re
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

# ===================================================
# Helpers de Normalização e Geometria
# ===================================================

def _is_geom(g) -> bool:
    return g is not None and (not getattr(g, "is_empty", True))

def _limpar_geometria(g):
    """Corrige problemas comuns de topologia."""
    if not _is_geom(g):
        return None
    try:
        if hasattr(g, "is_valid") and (not g.is_valid):
            g = make_valid(g)
        # buffer(0) resolve muitos self-intersections
        g = g.buffer(0)
    except Exception:
        # se der ruim, tenta retornar como está
        pass
    return g

def _safe_unary_union(geoms):
    geoms = [_limpar_geometria(g) for g in geoms if _is_geom(g)]
    if not geoms:
        return None
    try:
        u = unary_union(geoms)
        return _limpar_geometria(u)
    except Exception:
        return None

def remover_acentos(txt) -> str:
    """Remove acentos + normaliza para comparação."""
    if txt is None:
        return ""
    if not isinstance(txt, str):
        txt = str(txt)
    txt = txt.replace("\u00A0", " ")  # NBSP
    txt = re.sub(r"\s+", " ", txt).strip()
    return "".join(
        c for c in unicodedata.normalize("NFD", txt)
        if unicodedata.category(c) != "Mn"
    ).upper().strip()

def sanitizar_municipio(municipio: str, uf: str) -> str:
    """
    Remove UF embutida no nome (ex: 'Maurilândia-GO', 'Maurilândia / GO', 'Maurilândia (GO)').
    Mantém só o nome do município.
    """
    s = "" if municipio is None else str(municipio).strip()
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s).strip()

    uf2 = (uf or "").strip().upper()
    if not uf2:
        return s

    # remove sufixos comuns no final
    s = re.sub(rf"(\s*[-/]\s*{re.escape(uf2)})\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(rf"\(\s*{re.escape(uf2)}\s*\)\s*$", "", s, flags=re.IGNORECASE).strip()

    # remove UF solta no final (ex: "MAURILANDIA GO")
    if s.upper().endswith(" " + uf2):
        s = s[: -(len(uf2) + 1)].strip()

    return s


# ===================================================
# Morton Spatial Sort (Z-Order Curve)
# ===================================================

def _part1by1(n: int) -> int:
    n = int(n) & 0xFFFFFFFF
    n = (n | (n << 8)) & 0x00FF00FF
    n = (n | (n << 4)) & 0x0F0F0F0F
    n = (n | (n << 2)) & 0x33333333
    n = (n | (n << 1)) & 0x55555555
    return n

def morton_code(x: int, y: int) -> int:
    return _part1by1(x) | (_part1by1(y) << 1)

def spatial_sort(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if len(gdf) == 0:
        return gdf
    g = gdf.copy()
    cent = g.geometry.centroid
    xs = cent.x
    ys = cent.y

    xmin, xmax = xs.min(), xs.max()
    ymin, ymax = ys.min(), ys.max()

    dx = (xmax - xmin) if xmax != xmin else 1.0
    dy = (ymax - ymin) if ymax != ymin else 1.0

    x_norm = ((xs - xmin) / dx * 65535).fillna(0).astype(int)
    y_norm = ((ys - ymin) / dy * 65535).fillna(0).astype(int)

    g["__morton"] = [morton_code(x, y) for x, y in zip(x_norm, y_norm)]
    g = g.sort_values("__morton").drop(columns="__morton")
    return g.reset_index(drop=True)


# ===================================================
# Dados de Municípios e UTM
# ===================================================

def load_municipios() -> gpd.GeoDataFrame:
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

def detect_columns(mun: gpd.GeoDataFrame):
    cols = list(mun.columns)
    uf_col = "SIGLA_UF" if "SIGLA_UF" in cols else next((c for c in cols if "UF" in c), None)
    mun_col = "NM_MUN" if "NM_MUN" in cols else next((c for c in cols if "MUN" in c or "NOME" in c), None)

    if not uf_col or not mun_col:
        raise ValueError(f"Não foi possível detectar colunas UF/Município. Colunas: {cols}")
    return uf_col, mun_col

def guess_utm(gdf: gpd.GeoDataFrame) -> CRS:
    g = gdf.to_crs(4326)
    centroid = g.geometry.unary_union.centroid
    lon, lat = centroid.x, centroid.y
    zone = int((lon + 180) // 6) + 1
    epsg = 32700 + zone if lat < 0 else 32600 + zone
    return CRS.from_epsg(epsg)


# ===================================================
# Voronoi / Dissolve por grupo
# ===================================================

def build_cells(points: gpd.GeoDataFrame, boundary):
    boundary = _limpar_geometria(boundary)
    if boundary is None:
        return []

    mp = MultiPoint(list(points.geometry))
    vd = voronoi_diagram(mp, envelope=boundary)

    cells = []
    for c in vd.geoms:
        inter = c.intersection(boundary)
        if _is_geom(inter):
            cells.append(_limpar_geometria(inter))
    return [c for c in cells if _is_geom(c)]

def dissolve_por_grupo(points: gpd.GeoDataFrame, cells):
    if not cells:
        max_gid = int(points["__gid"].max()) if len(points) else -1
        return [None] * (max_gid + 1)

    cells_gdf = gpd.GeoDataFrame({"cell_id": range(len(cells))}, geometry=cells, crs=points.crs)

    points = points[points.geometry.notnull()].copy()

    # sjoin requer rtree/pygeos dependendo do ambiente; mas no seu requirements tem pyogrio,
    # então geralmente OK. Se falhar, o erro vai ser claro.
    join = gpd.sjoin(points[["__gid", "geometry"]], cells_gdf, how="inner", predicate="within")

    geom_map = cells_gdf["geometry"].to_dict()
    grupos_geoms = join.groupby("__gid")["cell_id"].unique().to_dict()

    max_gid = int(points["__gid"].max())
    dissolvidos = []

    for gid in range(max_gid + 1):
        cids = grupos_geoms.get(gid, [])
        geoms_to_union = [geom_map[cid] for cid in cids if cid in geom_map]
        dissolvidos.append(_safe_unary_union(geoms_to_union))

    return dissolvidos


# ===================================================
# Exportação
# ===================================================

def export_excel(points_wgs: gpd.GeoDataFrame, out_path: str):
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for gid in sorted(points_wgs["__gid"].unique()):
            g = points_wgs[points_wgs["__gid"] == gid].copy()
            df = pd.DataFrame(g.drop(columns="geometry"))
            df["LON"] = g.geometry.x
            df["LAT"] = g.geometry.y
            df.to_excel(writer, sheet_name=f"Prancha {int(gid) + 1}", index=False)

def export_kmz(points_wgs: gpd.GeoDataFrame, cell_wgs, path: str):
    kml = simplekml.Kml()
    fol = kml.newfolder(name="Dados")

    for idx, r in points_wgs.iterrows():
        p = fol.newpoint(name=f"Ponto {idx}")
        p.coords = [(r.geometry.x, r.geometry.y)]

    if cell_wgs and _is_geom(cell_wgs):
        if cell_wgs.geom_type == "Polygon":
            pol = fol.newpolygon(name="Limite da Prancha")
            pol.outerboundaryis = list(cell_wgs.exterior.coords)
        elif cell_wgs.geom_type == "MultiPolygon":
            for i, part in enumerate(cell_wgs.geoms):
                pol = fol.newpolygon(name=f"Parte {i}")
                pol.outerboundaryis = list(part.exterior.coords)

    kml.savekmz(path)


# ===================================================
# Pipeline Principal
# ===================================================

def processar(shp_path: str, uf: str, municipio: str, cap: int, out_dir: str) -> str:
    # --- Normaliza entrada ---
    uf_alvo = (uf or "").strip().upper()
    municipio_limpo = sanitizar_municipio(municipio, uf_alvo)

    mun = load_municipios()
    uf_col, mun_col = detect_columns(mun)

    # --- Normaliza base (AQUI está o ajuste do erro do .upper em Series) ---
    mun[uf_col] = mun[uf_col].astype(str).str.strip().str.upper()
    mun["__NORM"] = mun[mun_col].astype(str).apply(remover_acentos)

    mun_alvo = remover_acentos(municipio_limpo)

    selecao = mun[(mun[uf_col] == uf_alvo) & (mun["__NORM"] == mun_alvo)]

    if selecao.empty:
        sub = mun[mun[uf_col] == uf_alvo].copy()
        opcoes = list(sub[mun_col].dropna().unique()[:10])
        raise ValueError(
            f"Município '{municipio}' (normalizado para '{municipio_limpo}') "
            f"não encontrado em {uf_alvo}. Sugestões: {opcoes}"
        )

    mun_geom_orig = _limpar_geometria(selecao.geometry.iloc[0])
    if mun_geom_orig is None:
        raise ValueError("Geometria do município inválida/nula na base.")

    # --- Ler pontos ---
    gdf = gpd.read_file(shp_path)
    gdf = gdf[gdf.geometry.notnull()].copy()

    # CRS: tenta harmonizar
    if gdf.crs is None:
        gdf = gdf.set_crs(mun.crs)
    elif gdf.crs != mun.crs:
        gdf = gdf.to_crs(mun.crs)

    # filtra pontos no município
    gdf = gdf[gdf.intersects(mun_geom_orig)].copy()
    if len(gdf) == 0:
        raise ValueError("Nenhum ponto encontrado dentro dos limites do município.")

    # --- Processamento ---
    utm_crs = guess_utm(gdf)
    gdf_utm = gdf.to_crs(utm_crs)
    mun_geom_utm = gpd.GeoSeries([mun_geom_orig], crs=mun.crs).to_crs(utm_crs).iloc[0]

    gdf_utm = spatial_sort(gdf_utm)

    cap = int(cap) if int(cap) > 0 else 200
    gdf_utm["__gid"] = np.arange(len(gdf_utm)) // cap

    cells_utm = build_cells(gdf_utm, mun_geom_utm)
    grupos_utm = dissolve_por_grupo(gdf_utm, cells_utm)

    # --- Exportações ---
    gdf_wgs = gdf_utm.to_crs(4326)
    os.makedirs(out_dir, exist_ok=True)

    excel_path = os.path.join(out_dir, "pranchas.xlsx")
    export_excel(gdf_wgs, excel_path)

    kmz_dir = os.path.join(out_dir, "kmz")
    os.makedirs(kmz_dir, exist_ok=True)

    max_gid = int(gdf_utm["__gid"].max())
    for gid in range(max_gid + 1):
        pts_prancha = gdf_wgs[gdf_wgs["__gid"] == gid]
        cell_utm = grupos_utm[gid] if gid < len(grupos_utm) else None
        cell_wgs = None
        if cell_utm:
            cell_wgs = gpd.GeoSeries([cell_utm], crs=utm_crs).to_crs(4326).iloc[0]

        kmz_path = os.path.join(kmz_dir, f"prancha_{gid + 1}.kmz")
        export_kmz(pts_prancha, cell_wgs, kmz_path)

    zip_path = os.path.join(out_dir, "resultado.zip")
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(excel_path, arcname="pranchas.xlsx")
        for f in os.listdir(kmz_dir):
            z.write(os.path.join(kmz_dir, f), arcname=f"kmz/{f}")

    return zip_path
