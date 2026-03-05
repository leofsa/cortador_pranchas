import os
import re
import uuid
import shutil
import zipfile
import pandas as pd
import csv

from fastapi import FastAPI, UploadFile, File, Form, Request, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from cortarpontos import processar

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# =========================================
# Pastas
# =========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs")
DATA_DIR = os.path.join(BASE_DIR, "data")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =========================================
# Escolhe automaticamente o arquivo de lookup
# (funciona mesmo se mudar maiúsculas/minúsculas)
# =========================================
LOOKUP_CANDIDATES = [
    os.path.join(DATA_DIR, "Municipios.csv"),
    os.path.join(DATA_DIR, "municipios.csv"),
    os.path.join(DATA_DIR, "municipios_lookup.csv"),
    os.path.join(DATA_DIR, "Municipios.xlsx"),
    os.path.join(DATA_DIR, "municipios.xlsx"),
]

def _resolve_lookup_path() -> str:
    for p in LOOKUP_CANDIDATES:
        if os.path.exists(p):
            return p
    # mensagem rica para depurar no Render
    raise RuntimeError(
        "Nenhum arquivo de lookup encontrado em /data.\n"
        f"Procurei: {LOOKUP_CANDIDATES}\n"
        f"BASE_DIR={BASE_DIR}\n"
        f"DATA_DIR={DATA_DIR}\n"
        f"Arquivos em data/: {os.listdir(DATA_DIR) if os.path.isdir(DATA_DIR) else 'data/ não existe'}"
    )

LOOKUP_PATH = _resolve_lookup_path()

# Cache para não reler arquivo toda hora
_LOOKUP_CACHE = None
_LOOKUP_MTIME = None


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


def _is_xlsx_file(path: str) -> bool:
    # XLSX é um ZIP: começa com bytes "PK"
    try:
        with open(path, "rb") as f:
            sig = f.read(2)
        return sig == b"PK"
    except Exception:
        return False


def _get_lookup_from_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str, engine="openpyxl")
    df.columns = [str(c).strip().upper() for c in df.columns]

    if "SIGLA_UF" not in df.columns or "NM_MUN" not in df.columns:
        raise RuntimeError(f"Planilha precisa ter colunas NM_MUN e SIGLA_UF. Colunas: {list(df.columns)}")

    df = df[["SIGLA_UF", "NM_MUN"]].copy()
    df["SIGLA_UF"] = df["SIGLA_UF"].astype(str).str.strip().str.upper()
    df["NM_MUN"] = df["NM_MUN"].astype(str).str.strip()
    df = df[(df["SIGLA_UF"] != "") & (df["NM_MUN"] != "")]
    df = df.drop_duplicates(subset=["SIGLA_UF", "NM_MUN"])
    return df


def _detectar_delimitador(sample: str) -> str:
    # tenta sniffer, se falhar escolhe por contagem
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
        return dialect.delimiter
    except Exception:
        header = sample.splitlines()[0] if sample.splitlines() else ""
        if header.count(";") >= header.count(","):
            return ";"
        return ","


def _get_lookup_from_csv(path: str) -> pd.DataFrame:
    # IMPORTANTÍSSIMO: newline='' evita o erro "new-line character seen..."
    # tenta utf-8, depois latin-1
    last_err = None
    for enc in ("utf-8", "latin-1"):
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                # amostra para detectar delimitador
                sample_lines = []
                for _ in range(20):
                    line = f.readline()
                    if not line:
                        break
                    sample_lines.append(line)
                sample = "".join(sample_lines)

                # volta pro início para ler tudo
                f.seek(0)

                delim = _detectar_delimitador(sample)
                reader = csv.reader(f, delimiter=delim)

                header = next(reader, None)
                if not header:
                    raise RuntimeError("CSV vazio.")

                header_norm = [h.strip().upper() for h in header]

                def _idx(nome: str):
                    try:
                        return header_norm.index(nome)
                    except ValueError:
                        return None

                idx_mun = _idx("NM_MUN")
                idx_uf = _idx("SIGLA_UF")
                if idx_mun is None or idx_uf is None:
                    raise RuntimeError(f"CSV precisa ter colunas NM_MUN e SIGLA_UF. Cabeçalho: {header}")

                rows = []
                for row in reader:
                    if not row:
                        continue
                    row = [c.strip() for c in row]
                    if len(row) < 2:
                        continue

                    # se vier com colunas extras: UF = último campo; município = resto juntado
                    if len(row) != len(header):
                        uf = row[-1]
                        mun = " ".join([c for c in row[:-1] if c]).strip()
                    else:
                        mun = row[idx_mun] if idx_mun < len(row) else ""
                        uf = row[idx_uf] if idx_uf < len(row) else ""

                    if not mun or not uf:
                        continue

                    uf = str(uf).strip().upper()
                    mun = str(mun).strip()

                    if len(uf) != 2:
                        continue

                    rows.append((uf, mun))

                if not rows:
                    raise RuntimeError("Não consegui extrair nenhuma linha válida do CSV (UF/Município).")

                df = pd.DataFrame(rows, columns=["SIGLA_UF", "NM_MUN"])
                df["SIGLA_UF"] = df["SIGLA_UF"].astype(str).str.strip().str.upper()
                df["NM_MUN"] = df["NM_MUN"].astype(str).str.strip()
                df = df[(df["SIGLA_UF"] != "") & (df["NM_MUN"] != "")]
                df = df.drop_duplicates(subset=["SIGLA_UF", "NM_MUN"])
                return df

        except Exception as e:
            last_err = e

    raise RuntimeError(f"Falha ao ler CSV ({path}). Último erro: {last_err}")


def get_lookup() -> pd.DataFrame:
    global _LOOKUP_CACHE, _LOOKUP_MTIME

    path = LOOKUP_PATH
    mtime = os.path.getmtime(path)

    if _LOOKUP_CACHE is not None and _LOOKUP_MTIME == mtime:
        return _LOOKUP_CACHE

    if _is_xlsx_file(path) or path.lower().endswith(".xlsx"):
        df = _get_lookup_from_excel(path)
    else:
        df = _get_lookup_from_csv(path)

    _LOOKUP_CACHE = df
    _LOOKUP_MTIME = mtime
    return df


# =========================================
# Rotas auxiliares
# =========================================
@app.get("/health", include_in_schema=False)
async def health():
    return PlainTextResponse("ok", status_code=200)


@app.head("/", include_in_schema=False)
async def head_root():
    return PlainTextResponse("", status_code=200)


@app.get("/processar", include_in_schema=False)
async def processar_get():
    # evita erro se alguém abrir /processar na URL
    return RedirectResponse(url="/", status_code=302)


# =========================================
# UI
# =========================================
@app.get("/")
async def home(request: Request):
    try:
        df = get_lookup()
    except Exception as e:
        return PlainTextResponse(f"Erro carregando lookup: {e}", status_code=500)

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


# =========================================
# Processamento
# =========================================
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


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
