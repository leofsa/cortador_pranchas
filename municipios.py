import geopandas as gpd

MUNICIPIOS_PATH = "data/Municípios.geojson"

def carregar_municipios():
    gdf = gpd.read_file(MUNICIPIOS_PATH)

    if gdf.crs is None:
        gdf = gdf.set_crs(4674)

    return gdf


def listar_ufs(gdf):
    return sorted(gdf["SIGLA_UF"].unique())


def listar_municipios(gdf, uf):
    sub = gdf[gdf["SIGLA_UF"] == uf]
    return sorted(sub["NM_MUN"].unique())


def obter_geom_municipio(gdf, uf, nome):

    sel = gdf[(gdf["SIGLA_UF"] == uf) & (gdf["NM_MUN"] == nome)]

    if len(sel) == 0:
        raise ValueError("Município não encontrado")

    return sel.iloc[0].geometry, gdf.crs
