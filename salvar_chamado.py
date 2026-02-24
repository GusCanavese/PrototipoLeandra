import json
import re
import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Empty, Queue
from threading import Lock

import MySQLdb
from flask import Flask, jsonify, make_response, request

DB_HOST = "ballast.proxy.rlwy.net"
DB_USER = "root"
DB_PASSWORD = "cUxQKiTNIHZUlBQhphYhiESVTcrCJTGO"
DB_PORT = 15192
DB_NAME_DEFAULT = "teste"
POOL_SIZE = 12
DB_CACHE_TTL_MINUTOS = 20

app = Flask(__name__)

SISTEMA_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}
bancos_cache = {"valores": [], "expira_em": datetime.min}

_connection_lock = Lock()
_pools = {}
_indices_garantidos = set()


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


def _criar_pool(nome_banco=None):
    fila = Queue(maxsize=POOL_SIZE)
    for _ in range(POOL_SIZE):
        fila.put(_connect(nome_banco))
    return fila


@contextmanager
def conexao_pool(nome_banco=None):
    chave = nome_banco or "_sem_banco"
    with _connection_lock:
        if chave not in _pools:
            _pools[chave] = _criar_pool(nome_banco)
        pool = _pools[chave]

    try:
        conn = pool.get(timeout=5)
    except Empty as erro:
        raise RuntimeError("Pool de conexões esgotado. Tente novamente.") from erro

    try:
        yield conn
    finally:
        try:
            pool.put_nowait(conn)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass


def _executar_com_retry(nome_banco, operacao):
    chave = nome_banco or "_sem_banco"
    for tentativa in range(2):
        with conexao_pool(nome_banco) as conn:
            try:
                return operacao(conn)
            except MySQLdb.OperationalError as erro:
                mensagem = str(erro).lower()
                erro_recuperavel = any(
                    termo in mensagem for termo in ["server has gone away", "lost connection", "connection was killed"]
                )
                if tentativa == 1 or not erro_recuperavel:
                    raise
                try:
                    conn.close()
                except Exception:
                    pass
                with _connection_lock:
                    _pools[chave].put(_connect(nome_banco))


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

    with conexao_pool() as conn:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        bancos = [linha[0] for linha in cursor.fetchall() if linha[0] not in SISTEMA_DATABASES]
        cursor.close()

    bancos_cache["valores"] = bancos
    bancos_cache["expira_em"] = datetime.now() + timedelta(minutes=DB_CACHE_TTL_MINUTOS)
    return bancos


def obter_banco_requisicao():
    nome_banco = request.headers.get("X-Project-DB", DB_NAME_DEFAULT)
    if not nome_banco_valido(nome_banco):
        raise ValueError("Nome de banco inválido.")
    try:
        with conexao_pool(nome_banco):
            pass
    except RuntimeError as erro:
        raise ValueError(str(erro)) from erro
    except (MySQLdb.OperationalError, MySQLdb.ProgrammingError) as erro:
        raise ValueError(f"Banco '{nome_banco}' não encontrado.") from erro
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


async def executar_em_thread(funcao, *args, **kwargs):
    return await asyncio.to_thread(funcao, *args, **kwargs)


def executar_select(nome_banco, sql, params=None, fetch_one=False, dict_cursor=True):
    def operacao(conn):
        cursor_cls = MySQLdb.cursors.DictCursor if dict_cursor else None
        cursor = conn.cursor(cursor_cls) if cursor_cls else conn.cursor()
        cursor.execute(sql, params or ())
        dados = cursor.fetchone() if fetch_one else cursor.fetchall()
        cursor.close()
        return dados

    return _executar_com_retry(nome_banco, operacao)


def executar_write(nome_banco, sql, params=None):
    def operacao(conn):
        cursor = conn.cursor()
        cursor.execute(sql, params or ())
        conn.commit()
        cursor.close()

    _executar_com_retry(nome_banco, operacao)


def executar_transacao(nome_banco, callback):
    def operacao(conn):
        try:
            callback(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    _executar_com_retry(nome_banco, operacao)


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
    def transacao(conn):
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
        cursor.close()

    executar_transacao(nome_banco, transacao)


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


def garantir_indices(nome_banco):
    if nome_banco in _indices_garantidos:
        return

    def transacao(conn):
        cursor = conn.cursor()
        cursor.execute("CREATE INDEX idx_chamados_ultima_id ON chamados (ultima_atualizacao, id_chamado)")
        cursor.execute("CREATE INDEX idx_chamado_atualizacoes_chamado_data ON chamado_atualizacoes (id_chamado, data_atualizacao)")
        cursor.close()

    try:
        executar_transacao(nome_banco, transacao)
    except MySQLdb.MySQLError:
        pass
    _indices_garantidos.add(nome_banco)


def listar_chamados(nome_banco, limite=50, offset=0):
    garantir_indices(nome_banco)
    chamados = executar_select(
        nome_banco,
        """
        SELECT id_chamado, cliente, resumo, prioridade, status, abertura, ultima_atualizacao
        FROM chamados
        ORDER BY ultima_atualizacao DESC, id_chamado DESC
        LIMIT %s OFFSET %s
        """,
        (limite, offset),
    )
    return [
        {
            "id": c["id_chamado"],
            "client": c["cliente"],
            "summary": c["resumo"],
            "priority": c["prioridade"],
            "status": c["status"],
            "openedAt": c["abertura"] or "",
            "lastUpdate": c["ultima_atualizacao"] or "",
        }
        for c in chamados
    ]


def obter_chamado_detalhe(nome_banco, id_chamado):
    chamado = executar_select(
        nome_banco,
        """
        SELECT id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
               numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao
        FROM chamados
        WHERE id_chamado = %s
        """,
        (id_chamado,),
        fetch_one=True,
    )
    if not chamado:
        return None

    atualizacoes = executar_select(
        nome_banco,
        """
        SELECT autor, mensagem, data_atualizacao, anexos
        FROM chamado_atualizacoes
        WHERE id_chamado = %s
        ORDER BY id DESC
        """,
        (id_chamado,),
    )

    return {
        "id": chamado["id_chamado"],
        "client": chamado["cliente"],
        "clienteLogin": chamado["login_cliente"],
        "summary": chamado["resumo"],
        "description": chamado["descricao"] or "",
        "priority": chamado["prioridade"],
        "status": chamado["status"],
        "processNumber": chamado["numero_processo"] or "",
        "hasPartnership": bool(chamado["parceria"]),
        "partnershipPercent": chamado["parceria_porcentagem"] or "",
        "partnershipWith": chamado["parceria_com"] or "",
        "openedAt": chamado["abertura"] or "",
        "lastUpdate": chamado["ultima_atualizacao"] or "",
        "updates": [
            {
                "author": atu["autor"],
                "message": atu["mensagem"],
                "date": atu["data_atualizacao"],
                "attachments": normalizar_anexos(atu["anexos"]),
            }
            for atu in atualizacoes
        ],
    }


def substituir_chamados(nome_banco, chamados):
    def transacao(conn):
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

        cursor.close()

    executar_transacao(nome_banco, transacao)


def salvar_chamado_individual(nome_banco, chamado):
    def transacao(conn):
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

        cursor.close()

    executar_transacao(nome_banco, transacao)


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
async def api_projetos_listar():
    try:
        projetos = await executar_em_thread(listar_bancos_disponiveis)
        return responder_json({"projetos": projetos, "padrao": DB_NAME_DEFAULT})
    except RuntimeError as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 500)


@app.route("/api/clientes", methods=["GET"])
async def api_clientes_listar():
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        clientes = await executar_em_thread(listar_clientes, nome_banco)
        return responder_json(clientes)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/clientes", methods=["PUT"])
async def api_clientes_substituir():
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        clientes = request.json or []
        await executar_em_thread(substituir_clientes, nome_banco, clientes)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/clientes", methods=["POST"])
async def api_cliente_inserir():
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        await executar_em_thread(inserir_cliente, nome_banco, request.json or {})
        return responder_json({"ok": True}, 201)
    except (ValueError, RuntimeError, MySQLdb.MySQLError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["GET"])
async def api_chamados_listar():
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        limite = parse_int_param(request.args.get("limit"), padrao=50, minimo=1, maximo=200)
        offset = parse_int_param(request.args.get("offset"), padrao=0, minimo=0, maximo=1000000)
        chamados = await executar_em_thread(listar_chamados, nome_banco, limite, offset)
        return responder_json(chamados)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["GET"])
async def api_chamado_detalhar(id_chamado):
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        chamado = await executar_em_thread(obter_chamado_detalhe, nome_banco, id_chamado)
        if not chamado:
            return responder_json({"ok": False, "erro": "Chamado não encontrado."}, 404)
        return responder_json(chamado)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["PUT"])
async def api_chamados_substituir():
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        await executar_em_thread(substituir_chamados, nome_banco, request.json or [])
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["POST"])
async def api_chamado_inserir():
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        await executar_em_thread(salvar_chamado_individual, nome_banco, request.json or {})
        return responder_json({"ok": True}, 201)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["PUT"])
async def api_chamado_atualizar(id_chamado):
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        chamado = request.json or {}
        if not chamado.get("id"):
            chamado["id"] = id_chamado
        await executar_em_thread(salvar_chamado_individual, nome_banco, chamado)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["DELETE"])
async def api_chamado_remover(id_chamado):
    try:
        nome_banco = await executar_em_thread(obter_banco_requisicao)
        await executar_em_thread(excluir_chamado, nome_banco, id_chamado)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/login", methods=["POST"])
async def api_login():
    dados = request.json or {}
    usuario = (dados.get("usuario") or "").strip()
    senha = (dados.get("senha") or "").strip()

    try:
        nome_banco = dados.get("banco") or await executar_em_thread(obter_banco_requisicao)
        valido = await executar_em_thread(nome_banco_valido, nome_banco)
        if not valido:
            raise ValueError("Nome de banco inválido.")
        bancos_disponiveis = await executar_em_thread(listar_bancos_disponiveis)
        if nome_banco not in bancos_disponiveis:
            raise ValueError(f"Banco '{nome_banco}' não encontrado.")
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    autenticado = await executar_em_thread(autenticar_usuario, nome_banco, usuario, senha)
    if not autenticado:
        return responder_json({"ok": False, "erro": "Credenciais inválidas."}, 401)

    tipo = autenticado["tipo"]
    redirect = "admin.html" if tipo == "Administrador" else ("index.html" if tipo == "Técnico" else "cliente.html")
    return responder_json({"ok": True, "usuario": usuario, "tipo": tipo, "redirect": redirect, "banco": nome_banco})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
