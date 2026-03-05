import os
import re
import uuid
import shutil
import zipfile
import pandas as pd

from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from cortarpontos import processar

app = FastAPI()
templates = Jinja2Templates(directory="templates")

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# CSV leve (só para lista UF/Municípios) — evita OOM no Render
LOOKUP_PATH = "data/Municipios.csv"


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


def get_lookup() -> pd.DataFrame:
    if not os.path.exists(LOOKUP_PATH):
        raise RuntimeError(f"Arquivo não encontrado: {LOOKUP_PATH}")

    # tenta utf-8, se falhar usa latin-1 (Excel/Windows-1252)
    try:
        df = pd.read_csv(LOOKUP_PATH, dtype=str, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(LOOKUP_PATH, dtype=str, encoding="latin-1")

    # garante colunas esperadas (aceita variações de maiúsculas/minúsculas)
    cols = {c.strip().upper(): c for c in df.columns}
    if "SIGLA_UF" not in cols or "NM_MUN" not in cols:
        raise RuntimeError(f"CSV precisa ter colunas NM_MUN e SIGLA_UF. Colunas: {list(df.columns)}")

    # padroniza nomes
    df = df.rename(columns={cols["SIGLA_UF"]: "SIGLA_UF", cols["NM_MUN"]: "NM_MUN"})

    df["SIGLA_UF"] = df["SIGLA_UF"].astype(str).str.strip().str.upper()
    df["NM_MUN"] = df["NM_MUN"].astype(str).str.strip()

    # remove vazios + dupes
    df = df[(df["SIGLA_UF"] != "") & (df["NM_MUN"] != "")]
    df = df.drop_duplicates(subset=["SIGLA_UF", "NM_MUN"])
    return df


@app.get("/health", include_in_schema=False)
async def health():
    return PlainTextResponse("ok", status_code=200)


@app.head("/", include_in_schema=False)
async def head_root():
    return PlainTextResponse("", status_code=200)


# evita erro quando a pessoa abre /processar no navegador (GET)
@app.get("/processar", include_in_schema=False)
async def processar_get():
    return RedirectResponse(url="/", status_code=302)


@app.get("/")
async def home(request: Request):
    try:
        df = get_lookup()
    except Exception as e:
        return PlainTextResponse(f"Erro carregando Municipios.csv: {e}", status_code=500)

    ufs = sorted(df["SIGLA_UF"].dropna().unique().tolist())
    return templates.TemplateResponse("index.html", {"request": request, "ufs": ufs})


@app.get("/municipios/{uf}")
async def listar_municipios(uf: str):
    try:
        df = get_lookup()
    except Exception:
        return []

    uf_norm = (uf or "").strip().upper()
    sub = df[df["SIGLA_UF"] == uf_norm]
    municipios = sorted(sub["NM_MUN"].dropna().unique().tolist())
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

        cap = int(cap)
        if cap <= 0:
            raise ValueError("Postes por prancha (cap) deve ser > 0.")

        # salva ZIP
        with open(temp_zip, "wb") as f:
            shutil.copyfileobj(arquivo.file, f)

        # extrai
        with zipfile.ZipFile(temp_zip, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        # acha .shp
        for root, _, files in os.walk(extract_dir):
            for file in files:
                if file.lower().endswith(".shp"):
                    shp_path = os.path.join(root, file)
                    break
            if shp_path:
                break

        if not shp_path:
            raise ValueError("Arquivo .shp não encontrado dentro do ZIP enviado.")

        # processa (polígono continua vindo do Municipios.geojson no cortarpontos.py)
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
        # limpeza
        try:
            if os.path.exists(temp_zip):
                os.remove(temp_zip)
        except Exception:
            pass
        try:
            shutil.rmtree(extract_dir, ignore_errors=True)
        except Exception:
            pass
        # se quiser limpar outputs também (atenção: não apague antes do download terminar)
        # shutil.rmtree(out_dir, ignore_errors=True)


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
