import json
import os
import re
from datetime import datetime

import MySQLdb
from flask import Flask, jsonify, request

# host = "ballast.proxy.rlwy.net"
# user = "root"
# password = "cUxQKiTNIHZUlBQhphYhiESVTcrCJTGO"
# porta = 15192
# banco_padrao = "teste"

host = os.getenv("DB_HOST", "ballast.proxy.rlwy.net")
user = os.getenv("DB_USER", "root")
password = os.getenv("DB_PASSWORD", "cUxQKiTNIHZUlBQhphYhiESVTcrCJTGO")
porta = int(os.getenv("DB_PORT", "15192"))
banco_padrao = os.getenv("DB_NAME", "teste")

app = Flask(__name__)
estrutura_inicializada = set()


SISTEMA_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


def abrir_conexao(nome_banco=None):
    params = {
        "host": host,
        "user": user,
        "passwd": password,
        "port": porta,
        "charset": "utf8mb4",
    }
    if nome_banco:
        params["db"] = nome_banco
    try:
        return MySQLdb.connect(**params)
    except MySQLdb.MySQLError as erro:
        raise RuntimeError(
            "Falha ao conectar no MySQL local. Confira DB_HOST, DB_PORT, DB_USER e DB_PASSWORD."
        ) from erro


def responder_json(payload, status=200):
    resposta = jsonify(payload)
    resposta.status_code = status
    resposta.headers["Access-Control-Allow-Origin"] = "*"
    resposta.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Project-DB"
    resposta.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    return resposta


def nome_banco_valido(nome_banco):
    return bool(re.fullmatch(r"[A-Za-z0-9_]+", nome_banco or ""))


def listar_bancos_disponiveis():
    conn = abrir_conexao()
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    bancos = [linha[0] for linha in cursor.fetchall() if linha[0] not in SISTEMA_DATABASES]
    cursor.close()
    conn.close()
    return bancos


def obter_banco_requisicao():
    nome_banco = request.headers.get("X-Project-DB", banco_padrao)
    if not nome_banco_valido(nome_banco):
        raise ValueError("Nome de banco inválido.")
    if nome_banco not in listar_bancos_disponiveis():
        raise ValueError(f"Banco '{nome_banco}' não encontrado.")
    return nome_banco


def normalizar_anexos(anexos):
    if not anexos:
        return []
    if isinstance(anexos, str):
        try:
            return json.loads(anexos)
        except json.JSONDecodeError:
            return []
    return anexos


def garantir_estrutura(nome_banco):
    if nome_banco in estrutura_inicializada:
        return

    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS usuarios (
            usuario VARCHAR(100) PRIMARY KEY,
            senha VARCHAR(255) NOT NULL,
            tipo VARCHAR(30) NOT NULL,
            nome_completo VARCHAR(255) NULL,
            telefone VARCHAR(60) NULL,
            documento VARCHAR(60) NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chamados (
            id_chamado VARCHAR(30) PRIMARY KEY,
            cliente VARCHAR(255) NOT NULL,
            login_cliente VARCHAR(100) NOT NULL,
            resumo TEXT NOT NULL,
            descricao LONGTEXT,
            prioridade VARCHAR(20) NOT NULL,
            status VARCHAR(30) NOT NULL,
            numero_processo VARCHAR(100),
            parceria TINYINT(1) DEFAULT 0,
            parceria_porcentagem VARCHAR(10),
            parceria_com VARCHAR(255),
            abertura VARCHAR(30),
            ultima_atualizacao VARCHAR(30)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS chamado_atualizacoes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_chamado VARCHAR(30) NOT NULL,
            autor VARCHAR(100) NOT NULL,
            mensagem LONGTEXT NOT NULL,
            data_atualizacao VARCHAR(30) NOT NULL,
            anexos LONGTEXT,
            FOREIGN KEY (id_chamado) REFERENCES chamados(id_chamado) ON DELETE CASCADE
        )
        """
    )

    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE usuario = %s", ("tecnico",))
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            """
            INSERT INTO usuarios (usuario, senha, tipo, nome_completo, telefone, documento)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("tecnico", "tecnico123", "Técnico", "Técnico Padrão", None, None),
        )

    cursor.execute("SELECT COUNT(*) FROM usuarios WHERE usuario = %s", ("cliente",))
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            """
            INSERT INTO usuarios (usuario, senha, tipo, nome_completo, telefone, documento)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("cliente", "cliente123", "Cliente", "Cliente Padrão", "(11) 99999-9999", "000.000.000-00"),
        )

    cursor.execute("SELECT COUNT(*) FROM chamados")


def listar_clientes(nome_banco):
    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(
        """
        SELECT usuario, senha, nome_completo, telefone, documento
        FROM usuarios
        WHERE tipo = 'Cliente'
        ORDER BY usuario
        """
    )
    registros = cursor.fetchall()
    cursor.close()
    conn.close()
    return [
        {
            "nomeCompleto": r["nome_completo"] or "",
            "telefone": r["telefone"] or "",
            "documento": r["documento"] or "",
            "login": r["usuario"],
            "senha": r["senha"],
        }
        for r in registros
    ]


def substituir_clientes(nome_banco, clientes):
    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM usuarios WHERE tipo = 'Cliente'")
    for cliente in clientes:
        cursor.execute(
            """
            INSERT INTO usuarios (usuario, senha, tipo, nome_completo, telefone, documento)
            VALUES (%s, %s, 'Cliente', %s, %s, %s)
            """,
            (
                cliente["login"],
                cliente["senha"],
                cliente.get("nomeCompleto") or None,
                cliente.get("telefone") or None,
                cliente.get("documento") or None,
            ),
        )
    conn.commit()
    cursor.close()
    conn.close()


def listar_chamados(nome_banco):
    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(
        """
        SELECT id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
               numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao
        FROM chamados
        ORDER BY id_chamado DESC
        """
    )
    chamados = cursor.fetchall()
    cursor.execute(
        """
        SELECT id_chamado, autor, mensagem, data_atualizacao, anexos
        FROM chamado_atualizacoes
        ORDER BY id DESC
        """
    )
    atualizacoes = cursor.fetchall()
    cursor.close()
    conn.close()

    mapa = {}
    for atu in atualizacoes:
        mapa.setdefault(atu["id_chamado"], []).append(
            {
                "author": atu["autor"],
                "message": atu["mensagem"],
                "date": atu["data_atualizacao"],
                "attachments": normalizar_anexos(atu["anexos"]),
            }
        )

    return [
        {
            "id": c["id_chamado"],
            "client": c["cliente"],
            "clienteLogin": c["login_cliente"],
            "summary": c["resumo"],
            "description": c["descricao"] or "",
            "priority": c["prioridade"],
            "status": c["status"],
            "processNumber": c["numero_processo"] or "",
            "hasPartnership": bool(c["parceria"]),
            "partnershipPercent": c["parceria_porcentagem"] or "",
            "partnershipWith": c["parceria_com"] or "",
            "openedAt": c["abertura"] or "",
            "lastUpdate": c["ultima_atualizacao"] or "",
            "updates": mapa.get(c["id_chamado"], []),
        }
        for c in chamados
    ]


def substituir_chamados(nome_banco, chamados):
    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM chamado_atualizacoes")
    cursor.execute("DELETE FROM chamados")

    for chamado in chamados:
        cursor.execute(
            """
            INSERT INTO chamados (
                id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
                numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                chamado["id"],
                chamado["client"],
                chamado["clienteLogin"],
                chamado["summary"],
                chamado["description"],
                chamado["priority"],
                chamado["status"],
                chamado["processNumber"],
                1 if chamado.get("hasPartnership") else 0,
                chamado.get("partnershipPercent", ""),
                chamado.get("partnershipWith", ""),
                chamado["openedAt"],
                chamado["lastUpdate"],
            ),
        )

        for atualizacao in chamado.get("updates", []):
            cursor.execute(
                """
                INSERT INTO chamado_atualizacoes (id_chamado, autor, mensagem, data_atualizacao, anexos)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    chamado["id"],
                    atualizacao.get("author", "Técnico"),
                    atualizacao.get("message", ""),
                    atualizacao.get("date", datetime.now().strftime("%d/%m/%Y %H:%M")),
                    json.dumps(atualizacao.get("attachments", []), ensure_ascii=False),
                ),
            )

    conn.commit()
    cursor.close()
    conn.close()


def autenticar_usuario(nome_banco, usuario, senha):
    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT usuario, senha, tipo FROM usuarios WHERE usuario = %s", (usuario,))
    registro = cursor.fetchone()
    cursor.close()
    conn.close()

    if not registro or registro["senha"] != senha:
        return None
    return registro


@app.route("/api/projetos", methods=["GET", "OPTIONS"])
def api_projetos():
    if request.method == "OPTIONS":
        return responder_json({"ok": True})
    try:
        return responder_json({"projetos": listar_bancos_disponiveis(), "padrao": banco_padrao})
    except RuntimeError as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 500)


@app.route("/api/clientes", methods=["GET", "PUT", "OPTIONS"])
def api_clientes():
    if request.method == "OPTIONS":
        return responder_json({"ok": True})
    try:
        nome_banco = obter_banco_requisicao()
        garantir_estrutura(nome_banco)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    if request.method == "GET":
        return responder_json(listar_clientes(nome_banco))

    clientes = request.json or []
    substituir_clientes(nome_banco, clientes)
    return responder_json({"ok": True})


@app.route("/api/chamados", methods=["GET", "PUT", "OPTIONS"])
def api_chamados():
    if request.method == "OPTIONS":
        return responder_json({"ok": True})
    try:
        nome_banco = obter_banco_requisicao()
        garantir_estrutura(nome_banco)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    if request.method == "GET":
        return responder_json(listar_chamados(nome_banco))

    chamados = request.json or []
    substituir_chamados(nome_banco, chamados)
    return responder_json({"ok": True})


@app.route("/api/login", methods=["POST", "OPTIONS"])
def api_login():
    if request.method == "OPTIONS":
        return responder_json({"ok": True})

    dados = request.json or {}
    usuario = (dados.get("usuario") or "").strip()
    senha = (dados.get("senha") or "").strip()

    try:
        nome_banco = dados.get("banco") or obter_banco_requisicao()
        if not nome_banco_valido(nome_banco):
            raise ValueError("Nome de banco inválido.")
        if nome_banco not in listar_bancos_disponiveis():
            raise ValueError(f"Banco '{nome_banco}' não encontrado.")
        garantir_estrutura(nome_banco)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    autenticado = autenticar_usuario(nome_banco, usuario, senha)
    if not autenticado:
        return responder_json({"ok": False, "erro": "Credenciais inválidas."}, 401)

    tipo = autenticado["tipo"]
    redirect = "admin.html" if tipo == "Administrador" else ("index.html" if tipo == "Técnico" else "cliente.html")
    return responder_json({"ok": True, "usuario": usuario, "tipo": tipo, "redirect": redirect, "banco": nome_banco})


if __name__ == "__main__":
    try:
        garantir_estrutura(banco_padrao)
    except RuntimeError as erro:
        print(str(erro))
        raise SystemExit(1)
    app.run(host="0.0.0.0", port=5000, debug=True)
