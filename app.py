from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse
import shutil
import uuid
import os

from cortador import processar

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

    shp_path = os.path.join(UPLOAD, uid + ".shp")

    with open(shp_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    out_dir = os.path.join(OUTPUT, uid)

    os.makedirs(out_dir)

    zip_path = processar(shp_path, uf, municipio, cap, out_dir)

    return FileResponse(zip_path, filename="resultado.zip")
