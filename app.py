from fastapi import FastAPI, UploadFile, File, Form, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
import shutil
import uuid
import os
import zipfile

from cortarpontos import processar

app = FastAPI()

templates = Jinja2Templates(directory="templates")

UPLOAD = "uploads"
OUTPUT = "outputs"

os.makedirs(UPLOAD, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/processar")
async def cortar(
    file: UploadFile = File(...),
    uf: str = Form(...),
    municipio: str = Form(...),
    cap: int = Form(...)
):

    uid = str(uuid.uuid4())

    zip_path = os.path.join(UPLOAD, uid + ".zip")

    with open(zip_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    extract_dir = os.path.join(UPLOAD, uid)

    with zipfile.ZipFile(zip_path, 'r') as z:
        z.extractall(extract_dir)

    shp_file = None

    for f in os.listdir(extract_dir):
        if f.endswith(".shp"):
            shp_file = os.path.join(extract_dir, f)

    if shp_file is None:
        return {"erro": "Shapefile não encontrado no ZIP"}

    out_dir = os.path.join(OUTPUT, uid)

    os.makedirs(out_dir)

    resultado = processar(shp_file, uf, municipio, cap, out_dir)

    return FileResponse(resultado, filename="resultado.zip")
