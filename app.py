@app.get("/")
def home():
return {"status": "API Cortador de Pranchas funcionando"}
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse
import shutil
import uuid
import os

# IMPORTA DO SEU SCRIPT REAL
from cortarpontos import processar

app = FastAPI()

UPLOAD = "uploads"
OUTPUT = "outputs"

os.makedirs(UPLOAD, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)


@app.post("/processar")
async def cortar(
    file: UploadFile = File(...),
    uf: str = Form(...),
    municipio: str = Form(...),
    cap: int = Form(...)
):

    uid = str(uuid.uuid4())

    # salvar upload
    shp_path = os.path.join(UPLOAD, f"{uid}.shp")

    with open(shp_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    # pasta de saída
    out_dir = os.path.join(OUTPUT, uid)
    os.makedirs(out_dir, exist_ok=True)

    # chama seu algoritmo
    zip_path = processar(
        shp_path,
        uf,
        municipio,
        cap,
        out_dir
    )

    # retorna download
    return FileResponse(
        zip_path,
        filename="resultado.zip",
        media_type="application/zip"
    )

