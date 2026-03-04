import os
import sys
import math
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import CRS

import simplekml
from shapely.geometry import MultiPoint
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

import traceback

# Shapely 2.x
try:
    from shapely.ops import voronoi_diagram
except Exception as ex:
    raise ImportError(
        "Este script precisa do Shapely 2.x para Voronoi.\n"
        "Atualize com: pip install -U shapely"
    ) from ex


# ============================================================
# PyInstaller: localizar arquivos embutidos (data/...)
# ============================================================
def resource_path(relative_path: str) -> str:
    base = getattr(sys, "_MEIPASS", os.path.abspath("."))
    return os.path.join(base, relative_path)


# ============================================================
# Helpers robustos p/ geometrias (evita erro “incorrect type”)
# ============================================================
def _is_geom(g) -> bool:
    return isinstance(g, BaseGeometry) and (not g.is_empty)

def _safe_geoms(iterable):
    out = []
    for g in iterable:
        if _is_geom(g):
            out.append(g)
    return out

def _safe_unary_union(iterable):
    geoms = _safe_geoms(iterable)
    if not geoms:
        return None
    return unary_union(geoms).buffer(0)


# ============================================================
# Utilitários
# ============================================================
def _safe_int(v: str):
    try:
        return int(v)
    except Exception:
        return None

def _safe_float(v: str):
    try:
        return float(v)
    except Exception:
        return None

def _format_prancha(i: int) -> str:
    return f"Prancha {i:02d}"

def _guess_utm_crs(gdf: gpd.GeoDataFrame) -> CRS:
    if gdf.crs is None:
        raise ValueError("O shapefile de pontos está sem CRS (.prj ausente). Defina CRS no QGIS e salve novamente.")

    crs = CRS.from_user_input(gdf.crs)
    if crs.is_projected:
        return crs

    gdf2 = gdf[gdf.geometry.notnull()].copy()
    if len(gdf2) == 0:
        raise ValueError("Todas as geometrias do shape de pontos estão nulas/vazias.")

    union = gdf2.to_crs(4326).geometry.unary_union
    centroid = union.centroid
    lon, lat = float(centroid.x), float(centroid.y)

    zone = int((lon + 180) // 6) + 1
    south = lat < 0
    epsg = 32700 + zone if south else 32600 + zone
    return CRS.from_epsg(epsg)


# ============================================================
# Ordenação espacial (Morton/Z-order): grupos próximos + cap fixo
# ============================================================
def _part1by1(n: np.ndarray) -> np.ndarray:
    n = n.astype(np.uint32)
    n = (n | (n << 8)) & np.uint32(0x00FF00FF)
    n = (n | (n << 4)) & np.uint32(0x0F0F0F0F)
    n = (n | (n << 2)) & np.uint32(0x33333333)
    n = (n | (n << 1)) & np.uint32(0x55555555)
    return n

def _morton_code_xy(x: np.ndarray, y: np.ndarray) -> np.ndarray:
    xi = x.astype(np.uint32)
    yi = y.astype(np.uint32)
    return _part1by1(xi) | (_part1by1(yi) << np.uint32(1))

def _sort_by_spatial_morton(gdf_proj: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    xs = gdf_proj.geometry.x.to_numpy()
    ys = gdf_proj.geometry.y.to_numpy()

    xmin, xmax = float(xs.min()), float(xs.max())
    ymin, ymax = float(ys.min()), float(ys.max())

    dx = (xmax - xmin) if (xmax - xmin) != 0 else 1.0
    dy = (ymax - ymin) if (ymax - ymin) != 0 else 1.0

    x_norm = (xs - xmin) / dx
    y_norm = (ys - ymin) / dy

    x16 = np.clip(np.round(x_norm * 65535), 0, 65535).astype(np.uint32)
    y16 = np.clip(np.round(y_norm * 65535), 0, 65535).astype(np.uint32)

    m = _morton_code_xy(x16, y16)

    g = gdf_proj.copy()
    g["__morton"] = m
    g = g.sort_values("__morton").drop(columns=["__morton"]).reset_index(drop=True)
    return g

def _assign_group_ids_fixed_capacity(gdf_proj_sorted: gpd.GeoDataFrame, cap: int) -> gpd.GeoDataFrame:
    """
    Cria coluna __gid (0..n_groups-1) com cap fixo.
    """
    g = gdf_proj_sorted.copy().reset_index(drop=True)
    g["__gid"] = (np.arange(len(g)) // cap).astype(int)
    return g


# ============================================================
# Base municipal (auto-detecta .shp dentro de data/)
# ============================================================
def _load_municipios_auto() -> tuple[gpd.GeoDataFrame, str]:
    data_dir = resource_path("data")
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"Pasta data não encontrada: {data_dir}")

    shp_files = [f for f in os.listdir(data_dir) if f.lower().endswith(".shp")]
    if not shp_files:
        raise FileNotFoundError(f"Não encontrei nenhum .shp dentro de: {data_dir}")

    shp_path = os.path.join(data_dir, shp_files[0])
    mun = gpd.read_file(shp_path)

    need = {"NM_MUN", "SIGLA_UF"}
    missing = need - set(mun.columns)
    if missing:
        raise ValueError(f"A base municipal precisa ter colunas {need}. Faltando: {missing}")

    mun = mun[mun.geometry.notnull()].copy()
    if mun.crs is None:
        mun = mun.set_crs(4674, allow_override=True)

    return mun, shp_path

def _get_municipio_geom(mun_gdf: gpd.GeoDataFrame, uf: str, nm: str):
    sel = mun_gdf[(mun_gdf["SIGLA_UF"] == uf) & (mun_gdf["NM_MUN"] == nm)]
    if len(sel) == 0:
        raise ValueError("Município não encontrado na base (UF/NM_MUN).")
    return sel.iloc[0].geometry, mun_gdf.crs

def _clip_points_to_municipio(points: gpd.GeoDataFrame, mun_geom, mun_crs) -> gpd.GeoDataFrame:
    if points.crs is None:
        raise ValueError("Shapefile de pontos está sem CRS (.prj ausente).")

    mun_geom_in_points = gpd.GeoSeries([mun_geom], crs=mun_crs).to_crs(points.crs).iloc[0].buffer(0)
    mask = points.geometry.within(mun_geom_in_points) | points.geometry.touches(mun_geom_in_points)
    return points.loc[mask].copy()


# ============================================================
# Partição do município por Voronoi DOS PONTOS + dissolve por grupo
# ============================================================
def _normalize_partition_to_boundary(polys, boundary, seeds=None):
    """
    Garante:
    - sem sobreposição (difference sequencial)
    - união final == boundary (preenche 'gaps' residuais)
    """
    cleaned = []
    union_prev = None

    for p in polys:
        if not _is_geom(p):
            cleaned.append(None)
            continue
        p2 = p.intersection(boundary).buffer(0)
        if union_prev is not None and _is_geom(union_prev):
            p2 = p2.difference(union_prev).buffer(0)
        cleaned.append(p2 if _is_geom(p2) else None)
        if _is_geom(p2):
            union_prev = p2 if union_prev is None else _safe_unary_union([union_prev, p2])

    union_all = _safe_unary_union([p for p in cleaned if _is_geom(p)])
    if union_all is None:
        union_all = boundary.buffer(0)

    residual = boundary.difference(union_all).buffer(0)
    if residual.is_empty:
        return cleaned

    pieces = list(residual.geoms) if residual.geom_type == "MultiPolygon" else [residual]
    for piece in pieces:
        if not _is_geom(piece):
            continue

        if seeds is not None and len(seeds) == len(cleaned):
            rp = piece.representative_point()
            dists = [rp.distance(s) for s in seeds]
            idx = int(np.argmin(dists))
        else:
            areas = [(i, cleaned[i].area) for i in range(len(cleaned)) if _is_geom(cleaned[i])]
            idx = max(areas, key=lambda x: x[1])[0] if areas else 0

        if cleaned[idx] is None:
            cleaned[idx] = piece
        else:
            cleaned[idx] = cleaned[idx].union(piece).buffer(0)

    return cleaned

def _smooth_polygons(polys, boundary, smooth_m, seeds=None):
    if smooth_m <= 0:
        return polys
    sm = []
    for p in polys:
        if not _is_geom(p):
            sm.append(None)
            continue
        p2 = p.buffer(smooth_m, join_style=1).buffer(-smooth_m, join_style=1)
        p2 = p2.intersection(boundary).buffer(0)
        sm.append(p2 if _is_geom(p2) else None)
    sm = _normalize_partition_to_boundary(sm, boundary, seeds=seeds)
    return sm


def _build_partition_polygons_from_points(points_with_gid_proj: gpd.GeoDataFrame,
                                         boundary_proj,
                                         smooth_m: float):
    """
    1) Voronoi para TODOS os pontos (recortado no município)
    2) Atribui cada ponto a uma célula (within / intersects / nearest)
    3) Dissolve células por __gid
    4) Normaliza para união == município (sem sobreposição)
    5) Suaviza (curvas)
    """
    pts = points_with_gid_proj.copy()
    pts = pts[pts.geometry.notnull()].copy()

    # NUNCA use buffer(0) em Point (pode virar empty em alguns casos / arredondamentos)
    def _fix_geom(g):
        if not _is_geom(g):
            return None
        if g.geom_type in ("Polygon", "MultiPolygon", "LineString", "MultiLineString"):
            gg = g.buffer(0)
            return gg if _is_geom(gg) else None
        return g  # Point / MultiPoint

    pts["geometry"] = pts.geometry.apply(_fix_geom)
    pts = pts[pts.geometry.notnull()].copy()

    # remove pontos duplicados exatos (evita Voronoi degenerado)
    pts = pts.drop_duplicates(subset=["geometry"]).copy()

    geoms = _safe_geoms(list(pts.geometry))
    if len(geoms) < 2:
        raise ValueError("Poucos pontos únicos válidos para gerar Voronoi (precisa de >= 2).")

    mp = MultiPoint(geoms)
    vd = voronoi_diagram(mp, envelope=boundary_proj, edges=False)

    raw_cells = list(vd.geoms) if hasattr(vd, "geoms") else [vd]
    cells = []
    for c in raw_cells:
        if not _is_geom(c):
            continue
        cc = c.intersection(boundary_proj).buffer(0)
        if _is_geom(cc):
            cells.append(cc)

    if len(cells) == 0:
        raise ValueError("Voronoi gerou 0 células válidas dentro do município.")

    cells_gdf = gpd.GeoDataFrame({"cell_id": list(range(len(cells)))}, geometry=cells, crs=pts.crs)

    # 1) tenta within (ponto estritamente dentro)
    join = gpd.sjoin(
        pts[["__gid", "geometry"]],
        cells_gdf,
        how="left",
        predicate="within"
    )

    # 2) borda: tenta intersects (ponto na linha)
    miss = join["cell_id"].isna()
    if miss.any():
        join2 = gpd.sjoin(
            pts.loc[miss, ["__gid", "geometry"]],
            cells_gdf,
            how="left",
            predicate="intersects"
        )
        join.loc[miss, "cell_id"] = join2["cell_id"].values

    # 3) fallback final: célula mais próxima
    if join["cell_id"].isna().any():
        miss_idx = join[join["cell_id"].isna()].index
        for i in miss_idx:
            p = pts.loc[i, "geometry"]
            dists = cells_gdf.distance(p)
            join.loc[i, "cell_id"] = int(dists.idxmin())

    # dissolve por __gid
    gid_to_cells = {}
    for _, r in join.iterrows():
        gid = int(r["__gid"])
        cid = int(r["cell_id"])
        geom = cells_gdf.loc[cid, "geometry"]
        if _is_geom(geom):
            gid_to_cells.setdefault(gid, []).append(geom)

    n_groups = int(points_with_gid_proj["__gid"].max()) + 1
    dissolved = [None] * n_groups
    for gid in range(n_groups):
        uu = _safe_unary_union(gid_to_cells.get(gid, []))
        dissolved[gid] = uu if _is_geom(uu) else None

    # seeds = centroide dos pontos do grupo (para normalização)
    seeds = []
    for gid in range(n_groups):
        grp_pts = pts.loc[pts["__gid"] == gid, "geometry"]
        grp_pts = [g for g in grp_pts if _is_geom(g)]
        uu = _safe_unary_union(grp_pts)
        seeds.append(uu.centroid if uu is not None else boundary_proj.centroid)

    dissolved = _normalize_partition_to_boundary(dissolved, boundary_proj, seeds=seeds)
    dissolved = _smooth_polygons(dissolved, boundary_proj, smooth_m=smooth_m, seeds=seeds)
    dissolved = _normalize_partition_to_boundary(dissolved, boundary_proj, seeds=seeds)
    return dissolved


def _ensure_group_points_covered(points_with_gid_proj: gpd.GeoDataFrame,
                                group_polys_proj,
                                boundary_proj,
                                fix_buffer_m: float):
    """
    Garante final:
    - se algum ponto do grupo não estiver coberto pelo polígono do grupo,
      expande o polígono com um buffer desses pontos e normaliza de novo.
    """
    pts = points_with_gid_proj
    n_groups = len(group_polys_proj)

    seeds = []
    for gid in range(n_groups):
        grp_pts = pts.loc[pts["__gid"] == gid, "geometry"]
        grp_pts = [g for g in grp_pts if _is_geom(g)]
        uu = _safe_unary_union(grp_pts)
        seeds.append(uu.centroid if uu is not None else boundary_proj.centroid)

    fixed = []
    for gid in range(n_groups):
        poly = group_polys_proj[gid]
        if not _is_geom(poly):
            fixed.append(None)
            continue

        grp_pts = pts.loc[pts["__gid"] == gid, "geometry"]
        grp_pts = [g for g in grp_pts if _is_geom(g)]
        if len(grp_pts) == 0:
            fixed.append(poly)
            continue

        outside = [p for p in grp_pts if not poly.covers(p)]
        if outside:
            uu = _safe_unary_union(outside)
            if uu is None:
                fixed.append(poly)
                continue
            bump = uu.buffer(max(fix_buffer_m, 0.5), join_style=1)
            poly2 = poly.union(bump).intersection(boundary_proj).buffer(0)
            fixed.append(poly2 if _is_geom(poly2) else poly)
        else:
            fixed.append(poly)

    fixed = _normalize_partition_to_boundary(fixed, boundary_proj, seeds=seeds)
    return fixed


# ============================================================
# Excel
# ============================================================
def _write_excel(points_wgs84_with_gid: gpd.GeoDataFrame, out_xlsx: str):
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        n_groups = int(points_wgs84_with_gid["__gid"].max()) + 1
        for gid in range(n_groups):
            sheet = _format_prancha(gid + 1)
            g = points_wgs84_with_gid[points_wgs84_with_gid["__gid"] == gid].copy()
            df = g.drop(columns=["geometry"])
            df["lon"] = g.geometry.x
            df["lat"] = g.geometry.y
            df.to_excel(writer, sheet_name=sheet[:31], index=False)


# ============================================================
# KMZ estilizado
# ============================================================
def _kml_color(a, r, g, b) -> str:
    # KML: AABBGGRR
    return f"{a:02x}{b:02x}{g:02x}{r:02x}"

def _close_ring(coords):
    if coords and coords[0] != coords[-1]:
        coords.append(coords[0])
    return coords

def _write_kmz_for_group(
    group_points_wgs84: gpd.GeoDataFrame,
    cell_wgs84,
    municipio_wgs84,
    out_kmz: str,
    icon_href: str,
    icon_scale: float,
    line_width_cells: float,
    line_width_mun: float,
):
    kml = simplekml.Kml()
    fol = kml.newfolder(name="")

    # ---- estilo ícone ----
    icon_style = simplekml.Style()
    icon_style.iconstyle.icon.href = icon_href
    icon_style.iconstyle.scale = float(icon_scale)
    icon_style.iconstyle.color = _kml_color(0xFF, 0x00, 0x00, 0xFF)  # azul
    icon_style.labelstyle.scale = 0.0
    icon_style.labelstyle.color = _kml_color(0x00, 0, 0, 0)

    icon_style.iconstyle.hotspot = simplekml.HotSpot(
        x=0.5, y=0.0,
        xunits=simplekml.Units.fraction,
        yunits=simplekml.Units.fraction
    )

    # ---- estilo linhas das células (amarelo) ----
    cell_style = simplekml.Style()
    cell_style.linestyle.width = float(line_width_cells)
    cell_style.linestyle.color = _kml_color(0xFF, 0xFF, 0xFF, 0x00)
    cell_style.polystyle.fill = 0

    # ---- estilo contorno do município (amarelo por cima) ----
    mun_style = simplekml.Style()
    mun_style.linestyle.width = float(line_width_mun)
    mun_style.linestyle.color = _kml_color(0xFF, 0xFF, 0xFF, 0x00)
    mun_style.polystyle.fill = 0

    # ---- desenha célula da prancha ----
    cell_check = None
    if _is_geom(cell_wgs84):
        cell_check = cell_wgs84.buffer(0)
        if cell_check.geom_type == "Polygon":
            coords = [(float(x), float(y)) for x, y in list(cell_check.exterior.coords)]
            coords = _close_ring(coords)
            pol = fol.newpolygon(name="")
            pol.outerboundaryis = coords
            pol.style = cell_style
        elif cell_check.geom_type == "MultiPolygon":
            for piece in cell_check.geoms:
                coords = [(float(x), float(y)) for x, y in list(piece.exterior.coords)]
                coords = _close_ring(coords)
                pol = fol.newpolygon(name="")
                pol.outerboundaryis = coords
                pol.style = cell_style

    # ---- pontos ----
    for _, row in group_points_wgs84.iterrows():
        geom = row.geometry
        if not _is_geom(geom):
            continue
        if cell_check is not None and (not cell_check.covers(geom)):
            continue
        p = fol.newpoint(name="")
        p.coords = [(float(geom.x), float(geom.y))]
        p.style = icon_style

    # ---- contorno do município POR CIMA ----
    mun_poly = municipio_wgs84.buffer(0)
    if mun_poly.geom_type == "Polygon":
        coords = [(float(x), float(y)) for x, y in list(mun_poly.exterior.coords)]
        coords = _close_ring(coords)
        pol = fol.newpolygon(name="")
        pol.outerboundaryis = coords
        pol.style = mun_style
    elif mun_poly.geom_type == "MultiPolygon":
        for piece in mun_poly.geoms:
            coords = [(float(x), float(y)) for x, y in list(piece.exterior.coords)]
            coords = _close_ring(coords)
            pol = fol.newpolygon(name="")
            pol.outerboundaryis = coords
            pol.style = mun_style

    kml.savekmz(out_kmz)


# ============================================================
# GUI
# ============================================================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Corte por Município + pranchas (cap fixo) + Partição (por pontos, com curvas)")
        self.geometry("1040x620")
        self.minsize(1040, 620)

        self.shape_path = tk.StringVar(value="")
        self.out_dir = tk.StringVar(value="")
        self.points_per_prancha = tk.StringVar(value="200")

        self.uf = tk.StringVar(value="")
        self.municipio = tk.StringVar(value="")

        self.icon_href = tk.StringVar(value="http://maps.google.com/mapfiles/kml/paddle/wht-blank.png")
        self.icon_scale = tk.StringVar(value="0.7")
        self.line_width_cells = tk.StringVar(value="3.5")
        self.line_width_mun = tk.StringVar(value="5.0")

        self.smooth_m = tk.StringVar(value="50")
        self.fix_point_buffer_m = tk.StringVar(value="2")

        self.status = tk.StringVar(value="Carregando base de municípios...")
        self.mun_gdf = None
        self.mun_source = None

        self._build_ui()
        self._load_municipios_async()

    def _build_ui(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        main = ttk.Frame(self, padding=16)
        main.pack(fill="both", expand=True)

        ttk.Label(
            main,
            text="Corte por Município + pranchas (cap fixo) + polígonos por PONTOS (JUNTOS = limite municipal)",
            font=("Segoe UI", 13, "bold")
        ).pack(anchor="w", pady=(0, 12))

        box = ttk.LabelFrame(main, text="Entradas", padding=12)
        box.pack(fill="x")

        r1 = ttk.Frame(box); r1.pack(fill="x", pady=6)
        ttk.Label(r1, text="Shapefile (pontos):", width=18).pack(side="left")
        ttk.Entry(r1, textvariable=self.shape_path).pack(side="left", fill="x", expand=True, padx=(0, 8))
        ttk.Button(r1, text="Selecionar...", command=self.pick_shape).pack(side="left")

        r2 = ttk.Frame(box); r2.pack(fill="x", pady=6)
        ttk.Label(r2, text="Pasta de saída:", width=18).pack(side="left")
        ttk.Entry(r2, textvariable=self.out_dir).pack(side="left", fill="x", expand=True, padx=(0, 8))
        ttk.Button(r2, text="Selecionar...", command=self.pick_out_dir).pack(side="left")

        r3 = ttk.Frame(box); r3.pack(fill="x", pady=6)
        ttk.Label(r3, text="UF:", width=18).pack(side="left")
        self.uf_cb = ttk.Combobox(r3, textvariable=self.uf, width=8, state="readonly")
        self.uf_cb.pack(side="left", padx=(0, 10))
        self.uf_cb.bind("<<ComboboxSelected>>", lambda e: self._refresh_municipios())

        ttk.Label(r3, text="Município:").pack(side="left")
        self.mun_cb = ttk.Combobox(r3, textvariable=self.municipio, width=45, state="readonly")
        self.mun_cb.pack(side="left", padx=(8, 0))

        r4 = ttk.Frame(box); r4.pack(fill="x", pady=6)
        ttk.Label(r4, text="Postes por prancha:", width=18).pack(side="left")
        ttk.Entry(r4, textvariable=self.points_per_prancha, width=10).pack(side="left")

        ttk.Label(r4, text="   Ícone escala:").pack(side="left", padx=(12, 4))
        ttk.Entry(r4, textvariable=self.icon_scale, width=8).pack(side="left")

        ttk.Label(r4, text="   Linha pranchas:").pack(side="left", padx=(12, 4))
        ttk.Entry(r4, textvariable=self.line_width_cells, width=8).pack(side="left")

        ttk.Label(r4, text="   Linha município:").pack(side="left", padx=(12, 4))
        ttk.Entry(r4, textvariable=self.line_width_mun, width=8).pack(side="left")

        r5 = ttk.Frame(box); r5.pack(fill="x", pady=6)
        ttk.Label(r5, text="Ícone (URL):", width=18).pack(side="left")
        ttk.Entry(r5, textvariable=self.icon_href).pack(side="left", fill="x", expand=True)

        r6 = ttk.Frame(box); r6.pack(fill="x", pady=6)
        ttk.Label(r6, text="Curvas (m):", width=18).pack(side="left")
        ttk.Entry(r6, textvariable=self.smooth_m, width=10).pack(side="left")
        ttk.Label(r6, text="(0 = retas; 30~120 deixa bem curvo)").pack(side="left", padx=12)

        ttk.Label(r6, text="Fix pontos (m):").pack(side="left", padx=(12, 4))
        ttk.Entry(r6, textvariable=self.fix_point_buffer_m, width=10).pack(side="left")

        r7 = ttk.Frame(main); r7.pack(fill="x", pady=14)
        self.run_btn = ttk.Button(r7, text="Gerar Excel + KMZ", command=self.run, state="disabled")
        self.run_btn.pack(side="left")
        ttk.Button(r7, text="Sair", command=self.destroy).pack(side="right")

        status_box = ttk.LabelFrame(main, text="Status", padding=12)
        status_box.pack(fill="both", expand=True)
        ttk.Label(status_box, textvariable=self.status, wraplength=1000, justify="left").pack(anchor="w")

    def _load_municipios_async(self):
        def worker():
            try:
                mun, src = _load_municipios_auto()
                self.mun_gdf = mun
                self.mun_source = src
                ufs = sorted(mun["SIGLA_UF"].dropna().unique().tolist())
                self.after(0, lambda: self.uf_cb.configure(values=ufs))
                self.after(0, lambda: self.status.set(f"Base municipal carregada: {src}"))
                self.after(0, lambda: self.run_btn.configure(state="normal"))
            except Exception as ex:
                err = str(ex)
                self.after(0, lambda: self.status.set(f"Erro ao carregar municípios: {err}"))
                self.after(0, lambda: messagebox.showerror("Erro", err))

        threading.Thread(target=worker, daemon=True).start()

    def _refresh_municipios(self):
        if self.mun_gdf is None:
            return
        uf = self.uf.get().strip()
        if not uf:
            return
        sub = self.mun_gdf[self.mun_gdf["SIGLA_UF"] == uf]
        muns = sorted(sub["NM_MUN"].dropna().unique().tolist())
        self.mun_cb.configure(values=muns)
        if muns:
            self.municipio.set(muns[0])

    def pick_shape(self):
        path = filedialog.askopenfilename(title="Selecione o shapefile (.shp)", filetypes=[("Shapefile", "*.shp")])
        if path:
            self.shape_path.set(path)

    def pick_out_dir(self):
        d = filedialog.askdirectory(title="Selecione a pasta de saída")
        if d:
            self.out_dir.set(d)

    def run(self):
        shp = self.shape_path.get().strip()
        out = self.out_dir.get().strip()
        uf = self.uf.get().strip()
        nm = self.municipio.get().strip()

        cap = _safe_int(self.points_per_prancha.get().strip())
        icon_scale = _safe_float(self.icon_scale.get().strip())
        line_cells = _safe_float(self.line_width_cells.get().strip())
        line_mun = _safe_float(self.line_width_mun.get().strip())
        icon_href = self.icon_href.get().strip()

        smooth_m = _safe_float(self.smooth_m.get().strip())
        fix_buf = _safe_float(self.fix_point_buffer_m.get().strip())

        if not shp or not os.path.exists(shp):
            messagebox.showerror("Erro", "Selecione um shapefile (.shp) válido.")
            return
        if not out or not os.path.isdir(out):
            messagebox.showerror("Erro", "Selecione uma pasta de saída válida.")
            return
        if not uf or not nm:
            messagebox.showerror("Erro", "Selecione UF e Município.")
            return
        if cap is None or cap <= 0:
            messagebox.showerror("Erro", "Postes por prancha deve ser inteiro > 0.")
            return
        if icon_scale is None or icon_scale <= 0:
            messagebox.showerror("Erro", "Ícone escala deve ser > 0 (ex.: 0.7).")
            return
        if line_cells is None or line_cells <= 0:
            messagebox.showerror("Erro", "Linha pranchas deve ser > 0 (ex.: 3.5).")
            return
        if line_mun is None or line_mun <= 0:
            messagebox.showerror("Erro", "Linha município deve ser > 0.")
            return
        if smooth_m is None or smooth_m < 0:
            messagebox.showerror("Erro", "Curvas (m) deve ser >= 0.")
            return
        if fix_buf is None or fix_buf < 0:
            messagebox.showerror("Erro", "Fix pontos (m) deve ser >= 0.")
            return

        self.run_btn.configure(state="disabled")
        self.status.set("Processando...")

        def worker():
            try:
                gdf = gpd.read_file(shp)
                if len(gdf) == 0:
                    raise ValueError("O shapefile de pontos não possui feições.")

                gdf = gdf[gdf.geometry.notnull()].copy()
                if len(gdf) == 0:
                    raise ValueError("Todas as geometrias do shape de pontos são nulas.")

                geom_types = set(gdf.geom_type.unique())
                if not geom_types.issubset({"Point", "MultiPoint"}):
                    raise ValueError("O shapefile precisa ser de pontos (Point/MultiPoint).")
                if "MultiPoint" in geom_types:
                    gdf = gdf.explode(index_parts=False).reset_index(drop=True)

                if self.mun_gdf is None:
                    raise ValueError("Base de municípios não carregada.")
                mun_geom, mun_crs = _get_municipio_geom(self.mun_gdf, uf, nm)

                before = len(gdf)
                gdf = _clip_points_to_municipio(gdf, mun_geom, mun_crs)
                after = len(gdf)

                if after == 0:
                    raise ValueError("Após recortar pelo município, não sobrou nenhum ponto.")
                if cap > after:
                    raise ValueError(f"Você pediu {cap} postes por prancha, mas sobrou {after} pontos no município.")

                utm = _guess_utm_crs(gdf)
                gdf_proj = gdf.to_crs(utm)
                mun_proj = gpd.GeoSeries([mun_geom], crs=mun_crs).to_crs(utm).iloc[0].buffer(0)

                # grupos por proximidade (cap fixo)
                gdf_proj_sorted = _sort_by_spatial_morton(gdf_proj)
                gdf_proj_sorted = _assign_group_ids_fixed_capacity(gdf_proj_sorted, cap)

                # partição por Voronoi dos pontos e dissolve por grupo
                cells_proj = _build_partition_polygons_from_points(gdf_proj_sorted, mun_proj, smooth_m=smooth_m)

                # garante cobertura final
                cells_proj = _ensure_group_points_covered(gdf_proj_sorted, cells_proj, mun_proj, fix_buffer_m=fix_buf)

                # reprojeta para WGS84
                cells_wgs84 = [
                    gpd.GeoSeries([p], crs=utm).to_crs(4326).iloc[0] if _is_geom(p) else None
                    for p in cells_proj
                ]
                mun_wgs84 = gpd.GeoSeries([mun_proj], crs=utm).to_crs(4326).iloc[0]

                # pontos em WGS84 com gid
                pts_wgs84 = gdf_proj_sorted.to_crs(4326)

                # salvar
                base = os.path.splitext(os.path.basename(shp))[0]
                out_base = f"{base}_{uf}_{nm}".replace(" ", "_").replace("/", "-")

                excel_path = os.path.join(out, f"{out_base}_pranchas.xlsx")
                _write_excel(pts_wgs84, excel_path)

                kmz_dir = os.path.join(out, f"{out_base}_KMZ")
                os.makedirs(kmz_dir, exist_ok=True)

                n_groups = int(pts_wgs84["__gid"].max()) + 1
                for gid in range(n_groups):
                    grp_pts = pts_wgs84[pts_wgs84["__gid"] == gid].copy()
                    cell_wgs = cells_wgs84[gid] if gid < len(cells_wgs84) else None

                    kmz_path = os.path.join(kmz_dir, f"{_format_prancha(gid+1).replace(' ', '_')}.kmz")
                    _write_kmz_for_group(
                        group_points_wgs84=grp_pts,
                        cell_wgs84=cell_wgs,
                        municipio_wgs84=mun_wgs84,
                        out_kmz=kmz_path,
                        icon_href=icon_href,
                        icon_scale=icon_scale,
                        line_width_cells=line_cells,
                        line_width_mun=line_mun,
                    )

                n_pranchas = math.ceil(after / cap)
                resto = after % cap
                if resto == 0:
                    resto = cap

                msg = (
                    f"Concluído!\n\n"
                    f"Município: {nm}/{uf}\n"
                    f"Pontos lidos: {before}\n"
                    f"Pontos dentro do município: {after}\n"
                    f"Postes por prancha: {cap}\n"
                    f"Pranchas: {n_pranchas} (última com {resto})\n\n"
                    f"Curvas (m): {smooth_m}\n"
                    f"Fix pontos (m): {fix_buf}\n\n"
                    f"Excel: {excel_path}\n"
                    f"KMZs: {kmz_dir}\n\n"
                    f"Obs: os polígonos das pranchas formam uma partição do município (união = limite municipal)."
                )

                self.after(0, lambda m=msg: self.status.set(m))
                self.after(0, lambda: messagebox.showinfo("OK", "Arquivos gerados com sucesso!"))

            except Exception as ex:
                err = f"{ex}\n\nTRACE:\n{traceback.format_exc()}"
                self.after(0, lambda msg=f"Erro: {err}": self.status.set(msg))
                self.after(0, lambda: messagebox.showerror("Erro", err))
            finally:
                self.after(0, lambda: self.run_btn.configure(state="normal"))

        threading.Thread(target=worker, daemon=True).start()


if __name__ == "__main__":
    App().mainloop()
