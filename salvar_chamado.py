import json
import re
from datetime import datetime, timedelta
from threading import Lock, RLock

import MySQLdb
from flask import Flask, jsonify, make_response, request

DB_HOST = "ballast.proxy.rlwy.net"
DB_USER = "root"
DB_PASSWORD = "cUxQKiTNIHZUlBQhphYhiESVTcrCJTGO"
DB_PORT = 15192
DB_NAME_DEFAULT = "teste"

app = Flask(__name__)

SISTEMA_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}
bancos_cache = {"valores": [], "expira_em": datetime.min}

_connection_lock = Lock()
_connections = {}
_db_locks = {}


def _connect(db_name=None):
    params = {
        "host": DB_HOST,
        "user": DB_USER,
        "passwd": DB_PASSWORD,
        "port": DB_PORT,
        "charset": "utf8mb4",
        "connect_timeout": 5,
    }
    if db_name:
        params["db"] = db_name
    try:
        conn = MySQLdb.connect(**params)
    except MySQLdb.MySQLError as erro:
        raise RuntimeError("Falha ao conectar no MySQL. Verifique host, porta, usuário e senha.") from erro
    conn.autocommit(False)
    return conn


def obter_conexao(nome_banco=None):
    chave = nome_banco or "_sem_banco"
    with _connection_lock:
        conn = _connections.get(chave)
        if conn is None:
            conn = _connect(nome_banco)
            _connections[chave] = conn
            return conn
        try:
            conn.ping(True)
        except MySQLdb.MySQLError:
            conn = _connect(nome_banco)
            _connections[chave] = conn
        return conn



def obter_lock_banco(nome_banco=None):
    chave = nome_banco or "_sem_banco"
    with _connection_lock:
        lock = _db_locks.get(chave)
        if lock is None:
            lock = RLock()
            _db_locks[chave] = lock
        return lock

def aplicar_headers_cors(resposta):
    resposta.headers["Access-Control-Allow-Origin"] = "*"
    resposta.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Project-DB"
    resposta.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    return resposta


def responder_json(payload, status=200):
    resposta = jsonify(payload)
    resposta.status_code = status
    return aplicar_headers_cors(resposta)


@app.before_request
def responder_preflight_options():
    if request.method == "OPTIONS":
        return aplicar_headers_cors(make_response("", 200))
    return None


@app.after_request
def garantir_cors_global(resposta):
    return aplicar_headers_cors(resposta)


def nome_banco_valido(nome_banco):
    return bool(re.fullmatch(r"[A-Za-z0-9_]+", nome_banco or ""))


def listar_bancos_disponiveis():
    if datetime.now() < bancos_cache["expira_em"] and bancos_cache["valores"]:
        return bancos_cache["valores"]

    with obter_lock_banco():
        conn = obter_conexao()
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        bancos = [linha[0] for linha in cursor.fetchall() if linha[0] not in SISTEMA_DATABASES]
        cursor.close()

    bancos_cache["valores"] = bancos
    bancos_cache["expira_em"] = datetime.now() + timedelta(seconds=20)
    return bancos


def obter_banco_requisicao():
    nome_banco = request.headers.get("X-Project-DB", DB_NAME_DEFAULT)
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


def parse_int_param(valor, padrao=None, minimo=1, maximo=1000):
    if valor is None or valor == "":
        return padrao
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        raise ValueError("Parâmetro de paginação inválido.")
    if numero < minimo or numero > maximo:
        raise ValueError(f"Parâmetro deve estar entre {minimo} e {maximo}.")
    return numero


def executar_select(nome_banco, sql, params=None, fetch_one=False, dict_cursor=True):
    with obter_lock_banco(nome_banco):
        conn = obter_conexao(nome_banco)
        cursor_cls = MySQLdb.cursors.DictCursor if dict_cursor else None
        cursor = conn.cursor(cursor_cls) if cursor_cls else conn.cursor()
        cursor.execute(sql, params or ())
        dados = cursor.fetchone() if fetch_one else cursor.fetchall()
        cursor.close()
        return dados


def executar_write(nome_banco, sql, params=None):
    with obter_lock_banco(nome_banco):
        conn = obter_conexao(nome_banco)
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        conn.commit()
        cursor.close()


def listar_clientes(nome_banco):
    registros = executar_select(
        nome_banco,
        """
        SELECT usuario, senha, nome_completo, telefone, documento
        FROM usuarios
        WHERE tipo = 'Cliente'
        ORDER BY usuario
        """,
    )
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
    with obter_lock_banco(nome_banco):
        conn = obter_conexao(nome_banco)
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


def inserir_cliente(nome_banco, cliente):
    executar_write(
        nome_banco,
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


def listar_chamados(nome_banco, limite=None, offset=0):
    sql = """
        SELECT id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
               numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao
        FROM chamados
        ORDER BY id_chamado DESC
    """
    params = []
    if limite is not None:
        sql += " LIMIT %s OFFSET %s"
        params.extend([limite, offset])

    chamados = executar_select(nome_banco, sql, tuple(params))
    chamados_ids = [c["id_chamado"] for c in chamados]

    atualizacoes = []
    if chamados_ids:
        placeholders = ", ".join(["%s"] * len(chamados_ids))
        atualizacoes = executar_select(
            nome_banco,
            f"""
            SELECT id_chamado, autor, mensagem, data_atualizacao, anexos
            FROM chamado_atualizacoes
            WHERE id_chamado IN ({placeholders})
            ORDER BY id DESC
            """,
            tuple(chamados_ids),
        )

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
    with obter_lock_banco(nome_banco):
        conn = obter_conexao(nome_banco)
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


def salvar_chamado_individual(nome_banco, chamado):
    with obter_lock_banco(nome_banco):
        conn = obter_conexao(nome_banco)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO chamados (
                id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
                numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                cliente = VALUES(cliente),
                login_cliente = VALUES(login_cliente),
                resumo = VALUES(resumo),
                descricao = VALUES(descricao),
                prioridade = VALUES(prioridade),
                status = VALUES(status),
                numero_processo = VALUES(numero_processo),
                parceria = VALUES(parceria),
                parceria_porcentagem = VALUES(parceria_porcentagem),
                parceria_com = VALUES(parceria_com),
                abertura = VALUES(abertura),
                ultima_atualizacao = VALUES(ultima_atualizacao)
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

        cursor.execute("DELETE FROM chamado_atualizacoes WHERE id_chamado = %s", (chamado["id"],))
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


def excluir_chamado(nome_banco, id_chamado):
    executar_write(nome_banco, "DELETE FROM chamados WHERE id_chamado = %s", (id_chamado,))


def autenticar_usuario(nome_banco, usuario, senha):
    registro = executar_select(
        nome_banco,
        "SELECT usuario, senha, tipo FROM usuarios WHERE usuario = %s",
        (usuario,),
        fetch_one=True,
    )
    if not registro or registro["senha"] != senha:
        return None
    return registro


@app.errorhandler(MySQLdb.MySQLError)
def tratar_erro_mysql(erro):
    return responder_json({"ok": False, "erro": f"Erro de banco de dados: {erro}"}, 500)



@app.route("/api/projetos", methods=["GET"])
def api_projetos_listar():
    try:
        return responder_json({"projetos": listar_bancos_disponiveis(), "padrao": DB_NAME_DEFAULT})
    except RuntimeError as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 500)


@app.route("/api/clientes", methods=["GET"])
def api_clientes_listar():
    try:
        nome_banco = obter_banco_requisicao()
        return responder_json(listar_clientes(nome_banco))
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/clientes", methods=["PUT"])
def api_clientes_substituir():
    try:
        nome_banco = obter_banco_requisicao()
        clientes = request.json or []
        substituir_clientes(nome_banco, clientes)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/clientes", methods=["POST"])
def api_cliente_inserir():
    try:
        nome_banco = obter_banco_requisicao()
        inserir_cliente(nome_banco, request.json or {})
        return responder_json({"ok": True}, 201)
    except (ValueError, RuntimeError, MySQLdb.MySQLError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["GET"])
def api_chamados_listar():
    try:
        nome_banco = obter_banco_requisicao()
        limite = parse_int_param(request.args.get("limit"), padrao=None, minimo=1, maximo=1000)
        offset = parse_int_param(request.args.get("offset"), padrao=0, minimo=0, maximo=1000000)
        return responder_json(listar_chamados(nome_banco, limite=limite, offset=offset))
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["PUT"])
def api_chamados_substituir():
    try:
        nome_banco = obter_banco_requisicao()
        substituir_chamados(nome_banco, request.json or [])
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["POST"])
def api_chamado_inserir():
    try:
        nome_banco = obter_banco_requisicao()
        salvar_chamado_individual(nome_banco, request.json or {})
        return responder_json({"ok": True}, 201)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["PUT"])
def api_chamado_atualizar(id_chamado):
    try:
        nome_banco = obter_banco_requisicao()
        chamado = request.json or {}
        if not chamado.get("id"):
            chamado["id"] = id_chamado
        salvar_chamado_individual(nome_banco, chamado)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["DELETE"])
def api_chamado_remover(id_chamado):
    try:
        nome_banco = obter_banco_requisicao()
        excluir_chamado(nome_banco, id_chamado)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/login", methods=["POST"])
def api_login():
    dados = request.json or {}
    usuario = (dados.get("usuario") or "").strip()
    senha = (dados.get("senha") or "").strip()

    try:
        nome_banco = dados.get("banco") or obter_banco_requisicao()
        if not nome_banco_valido(nome_banco):
            raise ValueError("Nome de banco inválido.")
        if nome_banco not in listar_bancos_disponiveis():
            raise ValueError(f"Banco '{nome_banco}' não encontrado.")
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    autenticado = autenticar_usuario(nome_banco, usuario, senha)
    if not autenticado:
        return responder_json({"ok": False, "erro": "Credenciais inválidas."}, 401)

    tipo = autenticado["tipo"]
    redirect = "admin.html" if tipo == "Administrador" else ("index.html" if tipo == "Técnico" else "cliente.html")
    return responder_json({"ok": True, "usuario": usuario, "tipo": tipo, "redirect": redirect, "banco": nome_banco})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
