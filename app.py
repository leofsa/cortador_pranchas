import os
import re
import uuid
import shutil
import zipfile
import geopandas as gpd

from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates

# Importa a função processar do seu arquivo cortarpontos.py
from cortarpontos import processar

app = FastAPI()

templates = Jinja2Templates(directory="templates")

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------------------------------------------
# Normalização robusta do município recebido do FORM
# (corrige casos tipo "Maurilândia-GO", "Maurilândia / GO", "Maurilândia (GO)")
# ---------------------------------------------------
_UF_RX = re.compile(r"\b[A-Z]{2}\b", re.IGNORECASE)

def sanitizar_municipio(municipio: str, uf: str) -> str:
    if municipio is None:
        return ""
    s = str(municipio).strip()

    # normaliza separadores comuns
    s = s.replace("\u00A0", " ")  # NBSP
    s = re.sub(r"\s+", " ", s).strip()

    uf2 = (uf or "").strip().upper()

    # remove padrões no final: "-GO", "/GO", " (GO)", " - GO"
    if uf2:
        s = re.sub(rf"(\s*[-/]\s*{re.escape(uf2)})\s*$", "", s, flags=re.IGNORECASE).strip()
        s = re.sub(rf"\(\s*{re.escape(uf2)}\s*\)\s*$", "", s, flags=re.IGNORECASE).strip()

    # se ainda terminar com UF solta (ex: "MAURILANDIA GO"), remove
    if uf2 and s.upper().endswith(" " + uf2):
        s = s[: -(len(uf2) + 1)].strip()

    return s


# ---------------------------------------------------
# Carregar base de dados (Singleton)
# ---------------------------------------------------
def carregar_base():
    path = "data/Municipios.geojson"
    if not os.path.exists(path):
        print(f"ERRO CRÍTICO: Arquivo {path} não encontrado!")
        return None

    try:
        df = gpd.read_file(path)
        df.columns = [c.upper() for c in df.columns]

        if "GEOMETRY" in df.columns:
            df = df.set_geometry("GEOMETRY")

        if df.crs is None:
            df = df.set_crs(4674)

        # Garantias mínimas
        if "SIGLA_UF" not in df.columns or "NM_MUN" not in df.columns:
            print(f"ERRO CRÍTICO: Colunas esperadas não encontradas. Colunas: {list(df.columns)}")
            return None

        # limpeza leve para listagem
        df["SIGLA_UF"] = df["SIGLA_UF"].astype(str).str.strip().str.upper()
        df["NM_MUN"] = df["NM_MUN"].astype(str).str.strip()

        return df
    except Exception as e:
        print(f"Erro ao ler GeoJSON: {e}")
        return None


BASE = carregar_base()


# ---------------------------------------------------
# Rotas
# ---------------------------------------------------
@app.get("/")
async def home(request: Request):
    if BASE is None:
        return "Erro: Base de dados de municípios não carregada no servidor."

    ufs = sorted([u for u in BASE["SIGLA_UF"].dropna().unique() if str(u).strip()])
    return templates.TemplateResponse("index.html", {"request": request, "ufs": ufs})


@app.get("/municipios/{uf}")
async def listar_municipios(uf: str):
    if BASE is None:
        return []

    uf_norm = (uf or "").strip().upper()
    sub = BASE[BASE["SIGLA_UF"] == uf_norm]
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

    # normaliza UF e MUNICIPIO para evitar o bug do "-GO" vindo no form
    uf_norm = (uf or "").strip().upper()
    municipio_norm = sanitizar_municipio(municipio, uf_norm)

    try:
        os.makedirs(extract_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)

        # 1) salvar ZIP
        temp_zip = os.path.join(UPLOAD_DIR, f"{uid}.zip")
        with open(temp_zip, "wb") as f:
            shutil.copyfileobj(arquivo.file, f)

        # 2) extrair
        with zipfile.ZipFile(temp_zip, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        # 3) achar .shp
        shp_path = None
        for root, _, files in os.walk(extract_dir):
            for file in files:
                if file.lower().endswith(".shp"):
                    shp_path = os.path.join(root, file)
                    break
            if shp_path:
                break

        if not shp_path:
            raise ValueError("Arquivo .shp não encontrado dentro do ZIP enviado.")

        # 4) processar
        resultado_zip = processar(shp_path, uf_norm, municipio_norm, int(cap), out_dir)

        # 5) retorno
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
        # limpeza opcional (deixe comentado se quiser inspecionar outputs no servidor)
        # shutil.rmtree(extract_dir, ignore_errors=True)
        # if os.path.exists(temp_zip): os.remove(temp_zip)
        pass


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
