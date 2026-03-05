import os
import re
import uuid
import shutil
import zipfile
import pandas as pd
import csv
from io import StringIO

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

# Pode ser CSV ou XLSX (mesmo que o nome esteja errado)
LOOKUP_PATH = "data/municipios.csv"


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


def _ler_texto_com_fallback(path: str) -> str:
    with open(path, "rb") as f:
        data = f.read()
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("latin-1")


def _detectar_delimitador(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
        return dialect.delimiter
    except Exception:
        header = sample.splitlines()[0] if sample.splitlines() else ""
        if header.count(";") >= header.count(","):
            return ";"
        return ","


def _get_lookup_from_excel(path: str) -> pd.DataFrame:
    # lê a primeira aba por padrão
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


def _get_lookup_from_csv(path: str) -> pd.DataFrame:
    text = _ler_texto_com_fallback(path)
    sample = "\n".join(text.splitlines()[:20])
    delim = _detectar_delimitador(sample)

    sio = StringIO(text)
    reader = csv.reader(sio, delimiter=delim)

    try:
        header = next(reader)
    except StopIteration:
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

        # se linha veio com colunas extras, assume UF como último campo
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
        raise RuntimeError("Não consegui extrair nenhuma linha válida do arquivo (UF/Município).")

    df = pd.DataFrame(rows, columns=["SIGLA_UF", "NM_MUN"])
    df["SIGLA_UF"] = df["SIGLA_UF"].astype(str).str.strip().str.upper()
    df["NM_MUN"] = df["NM_MUN"].astype(str).str.strip()
    df = df[(df["SIGLA_UF"] != "") & (df["NM_MUN"] != "")]
    df = df.drop_duplicates(subset=["SIGLA_UF", "NM_MUN"])
    return df


def get_lookup() -> pd.DataFrame:
    if not os.path.exists(LOOKUP_PATH):
        raise RuntimeError(f"Arquivo não encontrado: {LOOKUP_PATH}")

    # Se for XLSX (mesmo que o nome esteja .csv), lê como Excel
    if _is_xlsx_file(LOOKUP_PATH):
        return _get_lookup_from_excel(LOOKUP_PATH)

    # senão tenta como CSV
    return _get_lookup_from_csv(LOOKUP_PATH)


@app.get("/health", include_in_schema=False)
async def health():
    return PlainTextResponse("ok", status_code=200)


@app.head("/", include_in_schema=False)
async def head_root():
    return PlainTextResponse("", status_code=200)


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

