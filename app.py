from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import shutil
import uuid
import os
import geopandas as gpd

from cortarpontos import processar

app = FastAPI()

templates = Jinja2Templates(directory="templates")

UPLOAD = "uploads"
OUTPUT = "outputs"

os.makedirs(UPLOAD, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)


# ------------------------------
# carregar municipios
# ------------------------------

def carregar_base():

    gdf = gpd.read_file("data/Municipios.geojson")

    if gdf.crs is None:
        gdf = gdf.set_crs(4674)

    gdf.columns = [c.upper() for c in gdf.columns]

    return gdf


BASE = carregar_base()


# ------------------------------
# HOME
# ------------------------------

@app.get("/")
def home(request: Request):

    ufs = sorted(BASE["SIGLA_UF"].unique())

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "ufs": ufs
        }
    )


# ------------------------------
# MUNICIPIOS
# ------------------------------

@app.get("/municipios/{uf}")
def listar_municipios(uf: str):

    sub = BASE[BASE["SIGLA_UF"] == uf]

    municipios = sorted(sub["NM_MUN"].unique())

    return municipios


# ------------------------------
# PROCESSAR
# ------------------------------

@app.post("/processar")
async def cortar(
    arquivo: UploadFile = File(...),
    uf: str = Form(...),
    municipio: str = Form(...),
    cap: int = Form(...)
):

    uid = str(uuid.uuid4())

    zip_path = os.path.join(UPLOAD, uid + ".zip")

    with open(zip_path, "wb") as f:
        shutil.copyfileobj(arquivo.file, f)

    out_dir = os.path.join(OUTPUT, uid)

    os.makedirs(out_dir)

    resultado = processar(zip_path, uf, municipio, cap, out_dir)

    return FileResponse(resultado, filename="resultado.zip")
