from flask import Flask, render_template, request
import subprocess
import os

app = Flask(__name__, template_folder="templates")

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/processar", methods=["POST"])
def processar():

    arquivo = request.files["arquivo"]

    caminho = os.path.join(UPLOAD_FOLDER, arquivo.filename)
    arquivo.save(caminho)

    subprocess.run(["python", "cortarpontos.py", caminho])

    return """
    <h2>✅ Processamento concluído</h2>
    <a href="/">Voltar</a>
    """


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)

