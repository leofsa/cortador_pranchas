import os
import uuid
import shutil
import zipfile
import geopandas as gpd

from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# Importa a função processar do seu arquivo cortarpontos.py
from cortarpontos import processar

app = FastAPI()

# Configuração de templates
templates = Jinja2Templates(directory="templates")

# Pastas de trabalho
UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------
# Carregar base de dados (Singleton)
# ---------------------------------------------------
def carregar_base():
    """Carrega o GeoJSON e padroniza colunas conforme visto no QGIS."""
    path = "data/Municipios.geojson"
    if not os.path.exists(path):
        print(f"ERRO CRÍTICO: Arquivo {path} não encontrado!")
        return None
    
    try:
        df = gpd.read_file(path)
        # Força colunas em maiúsculo para evitar conflitos (NM_MUN, SIGLA_UF)
        df.columns = [c.upper() for c in df.columns]
        
        if "GEOMETRY" in df.columns:
            df = df.set_geometry("GEOMETRY")
        
        # Define CRS padrão caso não exista (SIRGAS 2000 / EPSG 4674)
        if df.crs is None:
            df = df.set_crs(4674)
            
        return df
    except Exception as e:
        print(f"Erro ao ler GeoJSON: {e}")
        return None

# Carrega a base uma única vez no início para ganhar performance
BASE = carregar_base()

# ---------------------------------------------------
# Rotas da Aplicação
# ---------------------------------------------------

@app.get("/")
async def home(request: Request):
    """Renderiza a página inicial com a lista de estados."""
    if BASE is None:
        return "Erro: Base de dados de municípios não carregada no servidor."
        
    # Pega as siglas únicas conforme sua imagem do QGIS
    ufs = sorted(BASE["SIGLA_UF"].unique())
    return templates.TemplateResponse(
        "index.html", 
        {"request": request, "ufs": ufs}
    )

@app.get("/municipios/{uf}")
async def listar_municipios(uf: str):
    """Retorna a lista de municípios para o estado selecionado."""
    if BASE is None:
        return []
        
    # Filtro exato pela sigla do estado
    sub = BASE[BASE["SIGLA_UF"] == uf.upper()]
    # Pega os nomes da coluna NM_MUN (conforme sua imagem)
    municipios = sorted(sub["NM_MUN"].unique())
    return municipios

@app.post("/processar")
async def cortar(
    arquivo: UploadFile = File(...),
    uf: str = Form(...),
    municipio: str = Form(...),
    cap: int = Form(...)
):
    """Recebe o ZIP, processa os pontos e retorna o resultado."""
    uid = str(uuid.uuid4())
    extract_dir = os.path.join(UPLOAD_DIR, uid)
    out_dir = os.path.join(OUTPUT_DIR, uid)
    
    try:
        os.makedirs(extract_dir, exist_ok=True)
        os.makedirs(out_dir, exist_ok=True)

        # 1. Salvar o arquivo ZIP enviado
        temp_zip = os.path.join(UPLOAD_DIR, f"{uid}.zip")
        with open(temp_zip, "wb") as f:
            shutil.copyfileobj(arquivo.file, f)

        # 2. Extrair conteúdo
        with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        # 3. Localizar o arquivo .shp (busca em subpastas caso o ZIP seja aninhado)
        shp_path = None
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.lower().endswith(".shp"):
                    shp_path = os.path.join(root, file)
                    break
            if shp_path: break
        
        if not shp_path:
            raise ValueError("Arquivo .shp não encontrado dentro do ZIP enviado.")

        # 4. Chamar a lógica de processamento do cortarpontos.py
        # Passamos UF e Município exatamente como vieram do formulário
        resultado_zip = processar(shp_path, uf, municipio, cap, out_dir)

        # 5. Retornar arquivo para download
        return FileResponse(
            resultado_zip, 
            media_type='application/zip',
            filename=f"resultado_{municipio.replace(' ', '_')}.zip"
        )

    except ValueError as ve:
        # Erros de lógica (ex: município não encontrado) retornam 400
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        # Outros erros retornam 500 com o detalhe da exceção
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    finally:
        # Opcional: Aqui você poderia deletar a pasta extract_dir para economizar espaço
        # shutil.rmtree(extract_dir, ignore_errors=True)
        pass

if __name__ == "__main__":
    import uvicorn
    # Porta padrão do Render ou 8000 local
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
