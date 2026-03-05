import os
import uuid
import shutil
import zipfile
import geopandas as gpd

from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates

# Certifique-se de que o arquivo cortarpontos.py esteja na mesma pasta
from cortarpontos import processar

app = FastAPI()

templates = Jinja2Templates(directory="templates")

UPLOAD = "uploads"
OUTPUT = "outputs"

os.makedirs(UPLOAD, exist_ok=True)
os.makedirs(OUTPUT, exist_ok=True)

# ---------------------------------------------------
# Carregar municípios globalmente
# ---------------------------------------------------
# Dica: Ler o arquivo uma única vez no startup melhora a performance no Render
def carregar_base():
    path = "data/Municipios.geojson"
    if not os.path.exists(path):
        print(f"ERRO: Arquivo {path} não encontrado!")
        return None
    
    df = gpd.read_file(path)
    # Padroniza colunas para maiúsculo para evitar erros de busca
    df.columns = [c.upper() for c in df.columns]
    
    if "GEOMETRY" in df.columns:
        df = df.set_geometry("GEOMETRY")
    
    if df.crs is None:
        df = df.set_crs(4674)
        
    return df

BASE = carregar_base()

# ---------------------------------------------------
# Rotas
# ---------------------------------------------------

@app.get("/")
def home(request: Request):
    # Usamos SIGLA_UF que é o padrão do IBGE
    ufs = sorted(BASE["SIGLA_UF"].unique()) if BASE is not None else []
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "ufs": ufs}
    )

@app.get("/municipios/{uf}")
def listar_municipios(uf: str):
    if BASE is None: return []
    # Filtra por UF e pega a coluna de nome NM_MUN
    sub = BASE[BASE["SIGLA_UF"] == uf.upper()]
    municipios = sorted(sub["NM_MUN"].unique())
    return municipios

@app.post("/processar")
async def cortar(
    arquivo: UploadFile = File(...),
    uf: str = Form(...),
    municipio: str = Form(...),
    cap: int = Form(...)
):
    try:
        uid = str(uuid.uuid4())
        extract_dir = os.path.join(UPLOAD, uid)
        os.makedirs(extract_dir, exist_ok=True)

        # Salvar o arquivo enviado
        zip_path = os.path.join(UPLOAD, uid + ".zip")
        with open(zip_path, "wb") as f:
            shutil.copyfileobj(arquivo.file, f)

        # Extrair
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # Localizar o .shp
        shp_path = None
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.lower().endswith(".shp"):
                    shp_path = os.path.join(root, file)
                    break
        
        if not shp_path:
            raise HTTPException(status_code=400, detail="Shapefile (.shp) não encontrado no ZIP.")

        out_dir = os.path.join(OUTPUT, uid)
        os.makedirs(out_dir, exist_ok=True)

        # CHAMADA DO PROCESSAMENTO
        # Importante: O nome do município vai exatamente como saiu da lista
        resultado = processar(shp_path, uf, municipio, cap, out_dir)

        return FileResponse(resultado, filename=f"resultado_{municipio}.zip")

    except Exception as e:
        # Retorna o erro real para o navegador para facilitar o debug
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Opcional: Limpar arquivos temporários para não estourar o disco do Render
        # shutil.rmtree(extract_dir, ignore_errors=True)
        pass
