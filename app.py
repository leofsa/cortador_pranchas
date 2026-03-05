import os
import re
import uuid
import shutil
import zipfile
import threading
from typing import Optional

import geopandas as gpd
from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates

from cortarpontos import processar

app = FastAPI()
templates = Jinja2Templates(directory="templates")

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

MUNICIPIOS_PATH = "data/Municipios.geojson"

# Cache lazy + lock
_BASE: Optional[gpd.GeoDataFrame] = None
_BASE_LOCK = threading.Lock()


def sanitizar_municipio(municipio: str, uf: str) -> str:
    if municipio is None:
        return ""
    s = str(municipio).strip()
    s = s.replace("\u00A0", " ")  # NBSP
    s = re.sub(r"\s+", " ", s).strip()

    uf2 = (uf or "").strip().upper()
    if uf2:
        s = re.sub(rf"(\s*[-/]\s*{re.escape(uf2)})\s*$", "", s, flags=re.IGNORECASE).strip()
        s = re.sub(rf"\(\s*{re.escape(uf2)}\s*\)\s*$", "", s, flags=re.IGNORECASE).strip()
        if s.upper().endswith(" " + uf2):
            s = s[: -(len(uf2) + 1)].strip()
    return s


def get_base() -> gpd.GeoDataFrame:
    """
    Carrega o GeoJSON somente quando necessário.
    Evita travar o worker no import (que causa WORKER TIMEOUT/502 no Render).
    """
    global _BASE

    if _BASE is not None:
        return _BASE

    with _BASE_LOCK:
        if _BASE is not None:
            return _BASE

        if not os.path.exists(MUNICIPIOS_PATH):
            raise RuntimeError(f"Arquivo não encontrado: {MUNICIPIOS_PATH}")

        df = gpd.read_file(MUNICIPIOS_PATH)
        df.columns = [c.upper() for c in df.columns]

        if "GEOMETRY" in df.columns:
            df = df.set_geometry("GEOMETRY")

        if df.crs is None:
            df = df.set_crs(4674)

        if "SIGLA_UF" not in df.columns or "NM_MUN" not in df.columns:
            raise RuntimeError(f"Colunas esperadas não encontradas. Colunas: {list(df.columns)}")

        df["SIGLA_UF"] = df["SIGLA_UF"].astype(str).str.strip().str.upper()
        df["NM_MUN"] = df["NM_MUN"].astype(str).str.strip()

        _BASE = df
        return _BASE


@app.get("/health", include_in_schema=False)
async def health():
    return PlainTextResponse("ok", status_code=200)


@app.head("/", include_in_schema=False)
async def head_root():
    return PlainTextResponse("", status_code=200)


@app.get("/")
async def home(request: Request):
    try:
        base = get_base()
    except Exception as e:
        return PlainTextResponse(f"Erro: base de municípios não carregada. Detalhe: {e}", status_code=500)

    ufs = sorted([u for u in base["SIGLA_UF"].dropna().unique() if str(u).strip()])
    return templates.TemplateResponse("index.html", {"request": request, "ufs": ufs})


@app.get("/municipios/{uf}")
async def listar_municipios(uf: str):
    try:
        base = get_base()
    except Exception:
        return []

    uf_norm = (uf or "").strip().upper()
    sub = base[base["SIGLA_UF"] == uf_norm]
    municipios = sorted([m for m in sub["NM_MUN"].dropna().unique() if str(m).strip()])
    return municipios


@app.post("/processar")
async def cortar(
    arquivo: UploadFile = File(...),
    uf: str = Form(...),
    municipio: str = Form(...),
    cap: int = Form(...),
):
    uid = str(uuid.uuid4())
    extract_dir = os.path.join(UPLOAD_DIR, uid)
    out_dir = os.path.join(OUTPUT_DIR, uid)

    uf_norm = (uf or "").strip().upper()
    municipio_norm = sanitizar_municipio(municipio, uf_norm)

    temp_zip = os.path.join(UPLOAD_DIR, f"{uid}.zip")
    shp_path = None

    try:
        os.makedirs(extract_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)

        if not uf_norm or len(uf_norm) != 2:
            raise ValueError("UF inválida. Selecione uma UF válida.")
        if not municipio_norm:
            raise ValueError("Município inválido. Selecione um município válido.")

        cap = int(cap)
        if cap <= 0:
            raise ValueError("Postes por prancha (cap) deve ser maior que zero.")

        with open(temp_zip, "wb") as f:
            shutil.copyfileobj(arquivo.file, f)

        with zipfile.ZipFile(temp_zip, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        for root, _, files in os.walk(extract_dir):
            for file in files:
                if file.lower().endswith(".shp"):
                    shp_path = os.path.join(root, file)
                    break
            if shp_path:
                break

        if not shp_path:
            raise ValueError("Arquivo .shp não encontrado dentro do ZIP enviado.")

        resultado_zip = processar(shp_path, uf_norm, municipio_norm, cap, out_dir)

        safe_name = re.sub(r"[^A-Za-z0-9_\-]+", "_", municipio_norm).strip("_") or "resultado"
        return FileResponse(
            resultado_zip,
            media_type="application/zip",
            filename=f"resultado_{safe_name}.zip",
        )

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    finally:
        try:
            if os.path.exists(temp_zip):
                os.remove(temp_zip)
        except Exception:
            pass
        try:
            shutil.rmtree(extract_dir, ignore_errors=True)
        except Exception:
            pass
        # Para não apagar antes de baixar: deixe out_dir (ou apague depois se quiser)
        # shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
