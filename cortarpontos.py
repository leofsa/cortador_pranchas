import os
import zipfile
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import simplekml


def _find_first_shp(folder: Path) -> Path:
    shps = sorted(folder.rglob("*.shp"))
    if not shps:
        raise FileNotFoundError("Não encontrei nenhum .shp dentro do ZIP.")
    return shps[0]


def _validate_shapefile_set(shp_path: Path):
    base = shp_path.with_suffix("")
    needed = [".shp", ".shx", ".dbf"]
    missing = []
    for ext in needed:
        if not (base.with_suffix(ext)).exists():
            missing.append(ext)
    if missing:
        raise FileNotFoundError(
            f"Shapefile incompleto. Faltando: {', '.join(missing)}. "
            "Envie o ZIP contendo pelo menos .shp + .shx + .dbf (e idealmente .prj)."
        )


def _sort_by_spatial_morton(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # Ordena os pontos para formar pranchas “próximas” (Morton/Z-order)
    xs = gdf.geometry.x.to_numpy()
    ys = gdf.geometry.y.to_numpy()

    xmin, xmax = float(xs.min()), float(xs.max())
    ymin, ymax = float(ys.min()), float(ys.max())

    dx = (xmax - xmin) if (xmax - xmin) != 0 else 1.0
    dy = (ymax - ymin) if (ymax - ymin) != 0 else 1.0

    x_norm = (xs - xmin) / dx
    y_norm = (ys - ymin) / dy

    x16 = np.clip(np.round(x_norm * 65535), 0, 65535).astype(np.uint32)
    y16 = np.clip(np.round(y_norm * 65535), 0, 65535).astype(np.uint32)

    def part1by1(n: np.ndarray) -> np.ndarray:
        n = n.astype(np.uint32)
        n = (n | (n << 8)) & np.uint32(0x00FF00FF)
        n = (n | (n << 4)) & np.uint32(0x0F0F0F0F)
        n = (n | (n << 2)) & np.uint32(0x33333333)
        n = (n | (n << 1)) & np.uint32(0x55555555)
        return n

    morton = part1by1(x16) | (part1by1(y16) << np.uint32(1))

    out = gdf.copy()
    out["__morton"] = morton
    out = out.sort_values("__morton").drop(columns=["__morton"]).reset_index(drop=True)
    return out


def _format_prancha(i: int) -> str:
    return f"Prancha {i:02d}"


def _write_excel(points_wgs84: gpd.GeoDataFrame, out_xlsx: Path):
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        n_groups = int(points_wgs84["__gid"].max()) + 1
        for gid in range(n_groups):
            sheet = _format_prancha(gid + 1)
            g = points_wgs84[points_wgs84["__gid"] == gid].copy()
            df = g.drop(columns=["geometry"])
            df["lon"] = g.geometry.x
            df["lat"] = g.geometry.y
            df.to_excel(writer, sheet_name=sheet[:31], index=False)


def _kml_color(a, r, g, b) -> str:
    # KML: AABBGGRR
    return f"{a:02x}{b:02x}{g:02x}{r:02x}"


def _write_kmz_for_group(points_wgs84: gpd.GeoDataFrame, out_kmz: Path, icon_href: str, icon_scale: float, line_width: float):
    kml = simplekml.Kml()
    fol = kml.newfolder(name="")

    icon_style = simplekml.Style()
    icon_style.iconstyle.icon.href = icon_href
    icon_style.iconstyle.scale = float(icon_scale)
    icon_style.iconstyle.color = _kml_color(0xFF, 0x00, 0x00, 0xFF)  # azul
    icon_style.labelstyle.scale = 0.0

    # opcional: desenha um contorno simples (convex hull) só pra visualizar “área” da prancha
    try:
        hull = points_wgs84.unary_union.convex_hull
        if hull and hull.geom_type == "Polygon":
            poly = fol.newpolygon(name="")
            poly.outerboundaryis = [(float(x), float(y)) for x, y in list(hull.exterior.coords)]
            poly.polystyle.fill = 0
            poly.linestyle.width = float(line_width)
            poly.linestyle.color = _kml_color(0xFF, 0xFF, 0xFF, 0x00)  # amarelo
    except Exception:
        pass

    for _, row in points_wgs84.iterrows():
        geom = row.geometry
        if geom is None:
            continue
        p = fol.newpoint(name="")
        p.coords = [(float(geom.x), float(geom.y))]
        p.style = icon_style

    kml.savekmz(str(out_kmz))


def processar_zip_shapefile(zip_path: Path, params: dict, workdir: Path) -> Path:
    """
    Recebe um ZIP com shapefile e devolve um ZIP com:
    - pranchas.xlsx
    - KMZs (1 por prancha) dentro da pasta KMZ/
    """
    zip_path = Path(zip_path)
    workdir = Path(workdir)

    extract_dir = workdir / "input"
    out_dir = workdir / "output"
    kmz_dir = out_dir / "KMZ"
    extract_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    kmz_dir.mkdir(parents=True, exist_ok=True)

    # Extrai ZIP
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_dir)

    shp_path = _find_first_shp(extract_dir)
    _validate_shapefile_set(shp_path)

    cap = int(params.get("cap", 200))
    icon_scale = float(params.get("icon_scale", 0.7))
    line_cells = float(params.get("line_cells", 3.5))
    icon_href = str(params.get("icon_href", "")).strip() or "http://maps.google.com/mapfiles/kml/paddle/wht-blank.png"

    # Lê shapefile
    gdf = gpd.read_file(shp_path)
    if len(gdf) == 0:
        raise ValueError("O shapefile não possui feições.")

    gdf = gdf[gdf.geometry.notnull()].copy()
    if len(gdf) == 0:
        raise ValueError("Todas as geometrias são nulas.")

    # explode MultiPoint
    if "MultiPoint" in set(gdf.geom_type.unique()):
        gdf = gdf.explode(index_parts=False).reset_index(drop=True)

    # precisa ter CRS
    if gdf.crs is None:
        raise ValueError("O shapefile está sem CRS (.prj ausente). Gere/salve o .prj no QGIS e compacte tudo em ZIP.")

    # Reprojeta para WGS84 (KMZ precisa)
    pts_wgs84 = gdf.to_crs(4326)

    # Ordena e cria grupos
    pts_wgs84 = _sort_by_spatial_morton(pts_wgs84)
    pts_wgs84["__gid"] = (np.arange(len(pts_wgs84)) // cap).astype(int)

    # Excel
    excel_path = out_dir / "pranchas.xlsx"
    _write_excel(pts_wgs84, excel_path)

    # KMZs
    n_groups = int(pts_wgs84["__gid"].max()) + 1
    for gid in range(n_groups):
        grp = pts_wgs84[pts_wgs84["__gid"] == gid].copy()
        kmz_path = kmz_dir / f"{_format_prancha(gid + 1).replace(' ', '_')}.kmz"
        _write_kmz_for_group(grp, kmz_path, icon_href=icon_href, icon_scale=icon_scale, line_width=line_cells)

    # Compacta resultado
    result_zip = workdir / "resultado.zip"
    with zipfile.ZipFile(result_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.write(excel_path, arcname="pranchas.xlsx")
        for kmz in sorted(kmz_dir.glob("*.kmz")):
            z.write(kmz, arcname=f"KMZ/{kmz.name}")

    return result_zip
