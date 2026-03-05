import os
import uuid
import shutil
import zipfile
import geopandas as gpd

from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates

from cortarpontos import processar

app = FastAPI()

templates = Jinja2Templates(directory="templates")

UPLOAD = "uploads"
OUTPUT = "outputs"

os.makedirs(UPLOAD, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)


# ---------------------------
# carregar municípios
# ---------------------------

BASE = gpd.read_file("data/Municipios.geojson")

BASE.columns = [c.upper() for c in BASE.columns]

# ativar geometria
if "GEOMETRY" in BASE.columns:
    BASE = BASE.set_geometry("GEOMETRY")

# garantir CRS
if BASE.crs is None:
    BASE = BASE.set_crs(4674)

# ---------------------------
# HOME
# ---------------------------

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


# ---------------------------
# MUNICIPIOS
# ---------------------------

@app.get("/municipios/{uf}")
def listar_municipios(uf: str):

    sub = BASE[BASE["SIGLA_UF"] == uf]

    municipios = sorted(sub["NM_MUN"].unique())

    return municipios


# ---------------------------
# PROCESSAR
# ---------------------------

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

    extract_dir = os.path.join(UPLOAD, uid)

    os.makedirs(extract_dir)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)

    shp_path = None

    for file in os.listdir(extract_dir):
        if file.endswith(".shp"):
            shp_path = os.path.join(extract_dir, file)
            break

    if shp_path is None:
        raise Exception("Shapefile .shp não encontrado dentro do ZIP")

    out_dir = os.path.join(OUTPUT, uid)

    os.makedirs(out_dir)

    resultado = processar(shp_path, uf, municipio, cap, out_dir)

    return FileResponse(resultado, filename="resultado.zip")

