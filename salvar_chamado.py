import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta
from queue import Empty, Queue

import MySQLdb
from flask import Flask, has_request_context, jsonify, request

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
bancos_cache = {"valores": [], "expira_em": datetime.min}

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("db_performance")


SISTEMA_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


class MySQLConnectionPool:
    def __init__(self, params, pool_size=10):
        self.params = params
        self.pool_size = pool_size
        self._queue = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._created = 0

    def _create_connection(self):
        conn = MySQLdb.connect(**self.params)
        conn.autocommit(False)
        return conn

    def acquire(self, timeout=3):
        with self._lock:
            if self._created < self.pool_size:
                self._created += 1
                return self._create_connection()
        try:
            conn = self._queue.get(timeout=timeout)
        except Empty as erro:
            raise RuntimeError("Timeout ao obter conexão do pool MySQL.") from erro
        try:
            conn.ping(True)
        except MySQLdb.MySQLError:
            conn = self._create_connection()
        return conn

    def release(self, conn):
        if conn is None:
            return
        try:
            self._queue.put_nowait(conn)
        except Exception:
            try:
                conn.close()
            except MySQLdb.MySQLError:
                pass


class MySQLPoolManager:
    def __init__(self):
        self._pools = {}
        self._lock = threading.Lock()
        self._pool_size = int(os.getenv("DB_POOL_SIZE", "10"))
        self._pool_timeout = int(os.getenv("DB_POOL_ACQUIRE_TIMEOUT", "3"))

    def get_pool(self, nome_banco=None):
        key = nome_banco or "_sem_banco"
        with self._lock:
            if key in self._pools:
                return self._pools[key]

            params = {
                "host": host,
                "user": user,
                "passwd": password,
                "port": porta,
                "charset": "utf8mb4",
                "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "5")),
                "read_timeout": int(os.getenv("DB_READ_TIMEOUT", "10")),
                "write_timeout": int(os.getenv("DB_WRITE_TIMEOUT", "10")),
            }
            if nome_banco:
                params["db"] = nome_banco

            pool = MySQLConnectionPool(params=params, pool_size=self._pool_size)
            self._pools[key] = pool
            return pool

    def acquire(self, nome_banco=None):
        return self.get_pool(nome_banco).acquire(timeout=self._pool_timeout)

    def release(self, conn, nome_banco=None):
        self.get_pool(nome_banco).release(conn)


pool_manager = MySQLPoolManager()


def log_tempo_db(funcao, query_id, conexao_ms, execucao_ms, fetch_ms, total_ms, linhas=None):
    rota = request.path if has_request_context() else "background"
    logger.info(
        json.dumps(
            {
                "route": rota,
                "function": funcao,
                "query_id": query_id,
                "connection_ms": round(conexao_ms, 2),
                "execution_ms": round(execucao_ms, 2),
                "fetch_ms": round(fetch_ms, 2),
                "total_ms": round(total_ms, 2),
                "rows": linhas,
            },
            ensure_ascii=False,
        )
    )


def abrir_conexao(nome_banco=None):
    try:
        return pool_manager.acquire(nome_banco)
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
    if datetime.now() < bancos_cache["expira_em"] and bancos_cache["valores"]:
        return bancos_cache["valores"]

    conn = abrir_conexao()
    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES")
    bancos = [linha[0] for linha in cursor.fetchall() if linha[0] not in SISTEMA_DATABASES]
    cursor.close()
    pool_manager.release(conn)
    bancos_cache["valores"] = bancos
    bancos_cache["expira_em"] = datetime.now() + timedelta(seconds=20)
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


def garantir_estrutura(nome_banco):
    if nome_banco in estrutura_inicializada:
        return

    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS usuarios (
                usuario VARCHAR(100) PRIMARY KEY,
                senha VARCHAR(255) NOT NULL,
                tipo VARCHAR(30) NOT NULL,
                nome_completo VARCHAR(255) NULL,
                telefone VARCHAR(60) NULL,
                documento VARCHAR(60) NULL,
                email VARCHAR(60) NULL
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
        conn.commit()
        estrutura_inicializada.add(nome_banco)
    finally:
        cursor.close()
        pool_manager.release(conn, nome_banco)


def executar_select(nome_banco, query_id, sql, params=None, fetch_one=False, dict_cursor=True, funcao=""):
    inicio = time.perf_counter()
    inicio_conexao = time.perf_counter()
    conn = abrir_conexao(nome_banco)
    conexao_ms = (time.perf_counter() - inicio_conexao) * 1000
    cursor_cls = MySQLdb.cursors.DictCursor if dict_cursor else None
    cursor = conn.cursor(cursor_cls) if cursor_cls else conn.cursor()
    try:
        inicio_exec = time.perf_counter()
        cursor.execute(sql, params or ())
        execucao_ms = (time.perf_counter() - inicio_exec) * 1000

        inicio_fetch = time.perf_counter()
        dados = cursor.fetchone() if fetch_one else cursor.fetchall()
        fetch_ms = (time.perf_counter() - inicio_fetch) * 1000
        total_ms = (time.perf_counter() - inicio) * 1000
        linhas = 1 if fetch_one and dados else (len(dados) if not fetch_one else 0)
        log_tempo_db(funcao, query_id, conexao_ms, execucao_ms, fetch_ms, total_ms, linhas=linhas)
        return dados
    finally:
        cursor.close()
        pool_manager.release(conn, nome_banco)


def executar_write(nome_banco, query_id, sql, params=None, funcao=""):
    inicio = time.perf_counter()
    inicio_conexao = time.perf_counter()
    conn = abrir_conexao(nome_banco)
    conexao_ms = (time.perf_counter() - inicio_conexao) * 1000
    cursor = conn.cursor()
    try:
        inicio_exec = time.perf_counter()
        cursor.execute(sql, params or ())
        conn.commit()
        execucao_ms = (time.perf_counter() - inicio_exec) * 1000
        total_ms = (time.perf_counter() - inicio) * 1000
        log_tempo_db(funcao, query_id, conexao_ms, execucao_ms, 0, total_ms, linhas=cursor.rowcount)
    finally:
        cursor.close()
        pool_manager.release(conn, nome_banco)


def listar_clientes(nome_banco):
    registros = executar_select(
        nome_banco,
        "listar_clientes",
        """
        SELECT usuario, senha, nome_completo, telefone, documento
        FROM usuarios
        WHERE tipo = 'Cliente'
        ORDER BY usuario
        """,
        funcao="listar_clientes",
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
    pool_manager.release(conn, nome_banco)


def inserir_cliente(nome_banco, cliente):
    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor()
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
    pool_manager.release(conn, nome_banco)


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

    chamados = executar_select(
        nome_banco,
        "listar_chamados_base",
        sql,
        tuple(params),
        funcao="listar_chamados",
    )
    chamados_ids = [c["id_chamado"] for c in chamados]
    if chamados_ids:
        placeholders = ", ".join(["%s"] * len(chamados_ids))
        atualizacoes = executar_select(
            nome_banco,
            "listar_chamados_atualizacoes",
            f"""
            SELECT id_chamado, autor, mensagem, data_atualizacao, anexos
            FROM chamado_atualizacoes
            WHERE id_chamado IN ({placeholders})
            ORDER BY id DESC
            """,
            tuple(chamados_ids),
            funcao="listar_chamados",
        )
    else:
        atualizacoes = []

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
    pool_manager.release(conn, nome_banco)


def salvar_chamado_individual(nome_banco, chamado):
    conn = abrir_conexao(nome_banco)
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
    pool_manager.release(conn, nome_banco)


def excluir_chamado(nome_banco, id_chamado):
    conn = abrir_conexao(nome_banco)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM chamados WHERE id_chamado = %s", (id_chamado,))
    conn.commit()
    cursor.close()
    pool_manager.release(conn, nome_banco)


def autenticar_usuario(nome_banco, usuario, senha):
    registro = executar_select(
        nome_banco,
        "autenticar_usuario",
        "SELECT usuario, senha, tipo FROM usuarios WHERE usuario = %s",
        (usuario,),
        fetch_one=True,
        funcao="autenticar_usuario",
    )

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


@app.route("/api/clientes", methods=["POST"])
def api_cliente_inserir():
    try:
        nome_banco = obter_banco_requisicao()
        garantir_estrutura(nome_banco)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    cliente = request.json or {}
    try:
        inserir_cliente(nome_banco, cliente)
    except MySQLdb.MySQLError as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)
    return responder_json({"ok": True}, 201)


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
        try:
            limite = parse_int_param(request.args.get("limit"), padrao=None, minimo=1, maximo=1000)
            offset = parse_int_param(request.args.get("offset"), padrao=0, minimo=0, maximo=1000000)
        except ValueError as erro:
            return responder_json({"ok": False, "erro": str(erro)}, 400)
        return responder_json(listar_chamados(nome_banco, limite=limite, offset=offset))

    chamados = request.json or []
    substituir_chamados(nome_banco, chamados)
    return responder_json({"ok": True})


@app.route("/api/chamados", methods=["POST"])
def api_chamado_inserir():
    try:
        nome_banco = obter_banco_requisicao()
        garantir_estrutura(nome_banco)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    chamado = request.json or {}
    salvar_chamado_individual(nome_banco, chamado)
    return responder_json({"ok": True}, 201)


@app.route("/api/chamados/<id_chamado>", methods=["PUT", "DELETE", "OPTIONS"])
def api_chamado_individual(id_chamado):
    if request.method == "OPTIONS":
        return responder_json({"ok": True})
    try:
        nome_banco = obter_banco_requisicao()
        garantir_estrutura(nome_banco)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    if request.method == "DELETE":
        excluir_chamado(nome_banco, id_chamado)
        return responder_json({"ok": True})

    chamado = request.json or {}
    if not chamado.get("id"):
        chamado["id"] = id_chamado
    salvar_chamado_individual(nome_banco, chamado)
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
