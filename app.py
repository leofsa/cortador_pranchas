from flask import Flask, render_template, request
import subprocess

app = Flask(__name__)

@app.route("/")
def index():
    return """
    <h2>Cortador de Pranchas</h2>
    <form action="/processar" method="post" enctype="multipart/form-data">
        <input type="file" name="arquivo">
        <button type="submit">Gerar Pranchas</button>
    </form>
    """

@app.route("/processar", methods=["POST"])
def processar():
    arquivo = request.files["arquivo"]
    caminho = arquivo.filename
    arquivo.save(caminho)

    subprocess.run(["python", "cortarpontos.py", caminho])

    return "Processamento concluído"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
