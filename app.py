import os
import uuid
import shutil
import tempfile
from pathlib import Path

from flask import Flask, render_template, request, send_file, abort

from cortarpontos import processar_zip_shapefile

app = Flask(__name__)

# Limite de upload (ajuste se precisar)
# 200 MB:
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024


@app.get("/")
def index():
    return render_template("index.html")


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

    # Lê parâmetros do form (com defaults)
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
        "line_mun": get_float("line_mun", 5.0),  # mantido por compatibilidade visual
        "icon_href": request.form.get("icon_href", "http://maps.google.com/mapfiles/kml/paddle/wht-blank.png").strip(),
        "smooth_m": get_float("smooth_m", 50),  # mantido por compatibilidade (não usado nesta versão simples)
        "fix_buf": get_float("fix_buf", 2),     # mantido por compatibilidade (não usado nesta versão simples)
    }

    job_id = uuid.uuid4().hex[:12]
    workdir = Path(tempfile.mkdtemp(prefix=f"cortador_{job_id}_"))

    try:
        zip_path = workdir / "upload.zip"
        f.save(zip_path)

        out_zip = processar_zip_shapefile(zip_path=zip_path, params=params, workdir=workdir)

        # Baixa automaticamente
        return send_file(
            out_zip,
            as_attachment=True,
            download_name=f"resultado_{job_id}.zip",
            mimetype="application/zip",
        )

    except Exception as e:
        # Mostra erro no navegador (facilita debug)
        return f"<h2>Erro no processamento</h2><pre>{str(e)}</pre>", 500

    finally:
        # Limpa tudo (importante no Render)
        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception:
            pass


if __name__ == "__main__":
    # Em produção (Render) use gunicorn: gunicorn app:app
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "10000")))
