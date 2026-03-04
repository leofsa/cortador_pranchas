import os
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd
import simplekml

from shapely.geometry import MultiPoint
from shapely.ops import voronoi_diagram


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
            f"Shapefile incompleto. Faltando: {', '.join(missing)}"
        )


def _sort_by_spatial_morton(gdf: gpd.GeoDataFrame):

    xs = gdf.geometry.x.to_numpy()
    ys = gdf.geometry.y.to_numpy()

    xmin, xmax = xs.min(), xs.max()
    ymin, ymax = ys.min(), ys.max()

    dx = xmax - xmin if xmax != xmin else 1
    dy = ymax - ymin if ymax != ymin else 1

    x_norm = (xs - xmin) / dx
    y_norm = (ys - ymin) / dy

    x16 = (x_norm * 65535).astype(np.uint32)
    y16 = (y_norm * 65535).astype(np.uint32)

    def part1by1(n):
        n = (n | (n << 8)) & 0x00FF00FF
        n = (n | (n << 4)) & 0x0F0F0F0F
        n = (n | (n << 2)) & 0x33333333
        n = (n | (n << 1)) & 0x55555555
        return n

    morton = part1by1(x16) | (part1by1(y16) << 1)

    g = gdf.copy()
    g["__morton"] = morton
    g = g.sort_values("__morton").drop(columns="__morton").reset_index(drop=True)

    return g


def _format_prancha(i):
    return f"Prancha {i:02d}"


def _write_excel(points, out_xlsx):

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:

        n = int(points["__gid"].max()) + 1

        for gid in range(n):

            sheet = _format_prancha(gid + 1)

            g = points[points["__gid"] == gid].copy()

            df = g.drop(columns="geometry")

            df["lon"] = g.geometry.x
            df["lat"] = g.geometry.y

            df.to_excel(writer, sheet_name=sheet, index=False)


def _kml_color(a, r, g, b):
    return f"{a:02x}{b:02x}{g:02x}{r:02x}"


def _build_polygons_voronoi(points):

    mp = MultiPoint(list(points.geometry))

    vor = voronoi_diagram(mp)

    cells = list(vor.geoms)

    gdf_cells = gpd.GeoDataFrame(geometry=cells, crs=points.crs)

    join = gpd.sjoin(points, gdf_cells, predicate="within")

    gdf_cells["__gid"] = join["__gid"].values

    polys = gdf_cells.dissolve(by="__gid")

    return polys


def _smooth_polygon(poly):

    try:
        return poly.buffer(30).buffer(-30)
    except:
        return poly


def _write_kmz_for_group(points, polygon, out_kmz, icon_href, icon_scale, line_width):

    kml = simplekml.Kml()
    fol = kml.newfolder(name="")

    icon_style = simplekml.Style()

    icon_style.iconstyle.icon.href = icon_href
    icon_style.iconstyle.scale = icon_scale
    icon_style.iconstyle.color = _kml_color(255,0,0,255)

    icon_style.labelstyle.scale = 0

    if polygon is not None:

        polygon = _smooth_polygon(polygon)

        if polygon.geom_type == "Polygon":

            pol = fol.newpolygon(name="")

            pol.outerboundaryis = list(polygon.exterior.coords)

            pol.polystyle.fill = 0

            pol.linestyle.width = line_width

            pol.linestyle.color = _kml_color(255,255,255,0)

    for _, row in points.iterrows():

        p = fol.newpoint()

        p.coords = [(row.geometry.x, row.geometry.y)]

        p.style = icon_style

    kml.savekmz(out_kmz)


def processar_zip_shapefile(zip_path: Path, params: dict, workdir: Path):

    extract_dir = workdir / "input"
    out_dir = workdir / "output"
    kmz_dir = out_dir / "KMZ"

    extract_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    kmz_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as z:
        z.extractall(extract_dir)

    shp_path = _find_first_shp(extract_dir)

    _validate_shapefile_set(shp_path)

    cap = int(params.get("cap",200))
    icon_scale = float(params.get("icon_scale",0.7))
    line_cells = float(params.get("line_cells",3.5))

    icon_href = params.get(
        "icon_href",
        "http://maps.google.com/mapfiles/kml/paddle/wht-blank.png"
    )

    gdf = gpd.read_file(shp_path)

    if gdf.crs is None:
        raise ValueError("Shapefile sem CRS")

    pts = gdf.to_crs(4326)

    pts = _sort_by_spatial_morton(pts)

    pts["__gid"] = (np.arange(len(pts)) // cap).astype(int)

    excel_path = out_dir / "pranchas.xlsx"

    _write_excel(pts, excel_path)

    polys = _build_polygons_voronoi(pts)

    n_groups = int(pts["__gid"].max()) + 1

    for gid in range(n_groups):

        grp = pts[pts["__gid"] == gid]

        poly = polys.loc[gid].geometry if gid in polys.index else None

        kmz_path = kmz_dir / f"{_format_prancha(gid+1).replace(' ','_')}.kmz"

        _write_kmz_for_group(
            grp,
            poly,
            kmz_path,
            icon_href,
            icon_scale,
            line_cells
        )

    result_zip = workdir / "resultado.zip"

    with zipfile.ZipFile(result_zip,"w",compression=zipfile.ZIP_DEFLATED) as z:

        z.write(excel_path,"pranchas.xlsx")

        for kmz in kmz_dir.glob("*.kmz"):
            z.write(kmz,f"KMZ/{kmz.name}")

    return result_zip
