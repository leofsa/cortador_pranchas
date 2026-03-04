import os
import uuid
import shutil
import tempfile
from pathlib import Path

import geopandas as gpd

from flask import Flask, render_template, request, send_file, abort, jsonify

from cortarpontos import processar_zip_shapefile


app = Flask(__name__)

# ============================================================
# CONFIG
# ============================================================

app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024

BASE_DIR = Path(__file__).parent
MUNICIPIOS_PATH = BASE_DIR / "data" / "Municípios.geojson"


# ============================================================
# CARREGAR MUNICÍPIOS
# ============================================================

def carregar_municipios():

    if not MUNICIPIOS_PATH.exists():
        raise RuntimeError(f"Arquivo não encontrado: {MUNICIPIOS_PATH}")

    gdf = gpd.read_file(MUNICIPIOS_PATH)

    if gdf.crs is None:
        gdf = gdf.set_crs(4674)

    if "SIGLA_UF" not in gdf.columns or "NM_MUN" not in gdf.columns:
        raise RuntimeError("Base municipal precisa ter campos SIGLA_UF e NM_MUN")

    return gdf


municipios_gdf = carregar_municipios()


# ============================================================
# UTILITÁRIOS MUNICÍPIOS
# ============================================================

def listar_ufs():
    return sorted(municipios_gdf["SIGLA_UF"].unique())


def listar_municipios(uf):
    sub = municipios_gdf[municipios_gdf["SIGLA_UF"] == uf]
    return sorted(sub["NM_MUN"].unique())


def obter_municipio_geom(uf, nome):

    sel = municipios_gdf[
        (municipios_gdf["SIGLA_UF"] == uf) &
        (municipios_gdf["NM_MUN"] == nome)
    ]

    if len(sel) == 0:
        raise ValueError("Município não encontrado")

    return sel.iloc[0].geometry, municipios_gdf.crs


# ============================================================
# ROTAS
# ============================================================

@app.get("/")
def index():

    ufs = listar_ufs()

    return render_template(
        "index.html",
        ufs=ufs
    )


# ------------------------------------------------------------
# RETORNA MUNICÍPIOS DA UF
# ------------------------------------------------------------

@app.get("/municipios/<uf>")
def municipios(uf):

    try:
        lista = listar_municipios(uf)
        return jsonify(lista)

    except Exception as e:
        return jsonify({"erro": str(e)}), 500


# ------------------------------------------------------------
# PROCESSAMENTO
# ------------------------------------------------------------

@app.post("/processar")
def processar():

    if "arquivo" not in request.files:
        abort(400, "Arquivo não enviado.")

    f = request.files["arquivo"]

    if not f or not f.filename:
        abort(400, "Nenhum arquivo selecionado.")

    filename = f.filename.lower()

    if not filename.endswith(".zip"):
        abort(400, "Envie um arquivo .ZIP contendo o shapefile completo (.shp, .shx, .dbf, .prj...).")

    # --------------------------------------------------------
    # UF / MUNICÍPIO
    # --------------------------------------------------------

    uf = request.form.get("uf")
    municipio = request.form.get("municipio")

    if not uf or not municipio:
        abort(400, "Selecione UF e município.")

    try:
        mun_geom, mun_crs = obter_municipio_geom(uf, municipio)
    except Exception as e:
        abort(400, str(e))

    # --------------------------------------------------------
    # PARÂMETROS
    # --------------------------------------------------------

    def get_float(name, default):
        try:
            return float(request.form.get(name, default))
        except Exception:
            return float(default)

    def get_int(name, default):
        try:
            return int(request.form.get(name, default))
        except Exception:
            return int(default)

    params = {
        "cap": get_int("cap", 200),
        "icon_scale": get_float("icon_scale", 0.7),
        "line_cells": get_float("line_cells", 3.5),
        "line_mun": get_float("line_mun", 5.0),
        "icon_href": request.form.get(
            "icon_href",
            "http://maps.google.com/mapfiles/kml/paddle/wht-blank.png"
        ).strip(),
        "smooth_m": get_float("smooth_m", 50),
        "fix_buf": get_float("fix_buf", 2),
    }

    # --------------------------------------------------------
    # WORKDIR TEMPORÁRIO
    # --------------------------------------------------------

    job_id = uuid.uuid4().hex[:12]

    workdir = Path(tempfile.mkdtemp(prefix=f"cortador_{job_id}_"))

    try:

        zip_path = workdir / "upload.zip"

        f.save(zip_path)

        # ----------------------------------------------------
        # PROCESSAR
        # ----------------------------------------------------

        out_zip = processar_zip_shapefile(
            zip_path=zip_path,
            params=params,
            workdir=workdir,
            mun_geom=mun_geom,
            mun_crs=mun_crs
        )

        return send_file(
            out_zip,
            as_attachment=True,
            download_name=f"resultado_{job_id}.zip",
            mimetype="application/zip",
        )

    except Exception as e:

        return f"<h2>Erro no processamento</h2><pre>{str(e)}</pre>", 500

    finally:

        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception:
            pass


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "10000"))
    )
