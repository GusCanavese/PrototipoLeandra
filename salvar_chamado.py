import json
import re
import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Empty, Queue
from threading import Lock

import MySQLdb
from flask import Flask, jsonify, make_response, request

host     = "ballast.proxy.rlwy.net"
user     = "root"
password = "cUxQKiTNIHZUlBQhphYhiESVTcrCJTGO"
db       = "teste"
port     =  15192
nome_banco = "teste"


POOL_SIZE = 1
DB_CACHE_TTL_MINUTOS = 2
SCHEMA_CACHE_TTL_MINUTOS = 5

app = Flask(__name__)

SISTEMA_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}
bancos_cache = {"valores": [], "expira_em": datetime.min}
esquema_cache = {}

_connection_lock = Lock()
_pools = {}


def criar_conexao(nome_banco=None):
    banco_destino = nome_banco or db
    return MySQLdb.connect(host=host, user=user, passwd=password, db=banco_destino, port=port, charset="utf8mb4")


def obter_pool(nome_banco=None):
    chave = nome_banco or db
    with _connection_lock:
        dados_pool = _pools.get(chave)
        if dados_pool is None:
            dados_pool = {"fila": Queue(maxsize=POOL_SIZE), "criadas": 0}
            _pools[chave] = dados_pool
    return dados_pool


@contextmanager
def conexao_pool(nome_banco=None):
    chave = nome_banco or db
    dados_pool = obter_pool(chave)
    pool = dados_pool["fila"]
    conn = None

    try:
        conn = pool.get_nowait()
    except Empty:
        with _connection_lock:
            if dados_pool["criadas"] < POOL_SIZE:
                conn = criar_conexao(chave)
                dados_pool["criadas"] += 1

        if conn is None:
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
            with _connection_lock:
                dados_pool["criadas"] = max(0, dados_pool["criadas"] - 1)


def _executar_com_retry(nome_banco, operacao):
    chave = nome_banco or db
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
                    dados_pool = _pools.get(chave)
                    if dados_pool:
                        dados_pool["criadas"] = max(0, dados_pool["criadas"] - 1)


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

    with conexao_pool(db) as conn:
        cursor = conn.cursor()
        cursor.execute("SHOW DATABASES")
        bancos = [linha[0] for linha in cursor.fetchall() if linha[0] not in SISTEMA_DATABASES]
        cursor.close()

    bancos_cache["valores"] = bancos
    bancos_cache["expira_em"] = datetime.now() + timedelta(minutes=DB_CACHE_TTL_MINUTOS)
    return bancos


def obter_banco_requisicao():
    nome_banco = request.headers.get("X-Project-DB", "teste")
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


def obter_colunas_tabela(nome_banco, tabela):
    chave_cache = (nome_banco or db, tabela)
    cache = esquema_cache.get(chave_cache)
    if cache and cache["expira_em"] > datetime.now():
        return cache["colunas"]

    colunas = executar_select(
        nome_banco,
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        (nome_banco or db, tabela),
        dict_cursor=False,
    )
    valores = {linha[0] for linha in colunas}
    esquema_cache[chave_cache] = {
        "colunas": valores,
        "expira_em": datetime.now() + timedelta(minutes=SCHEMA_CACHE_TTL_MINUTOS),
    }
    return valores


def tabela_existe(nome_banco, tabela):
    return bool(obter_colunas_tabela(nome_banco, tabela))


def coluna_ou_padrao(colunas, coluna, alias=None, padrao="''", prefixo=""):
    alias_sql = alias or coluna
    if coluna in colunas:
        return f"{prefixo}{coluna} AS {alias_sql}"
    return f"{padrao} AS {alias_sql}"


def expressao_ultima_atualizacao(nome_banco, alias_chamados="c"):
    colunas_chamados = obter_colunas_tabela(nome_banco, "chamados")
    possui_tabela_atualizacoes = tabela_existe(nome_banco, "chamado_atualizacoes")

    if "ultima_atualizacao" in colunas_chamados and possui_tabela_atualizacoes:
        return (
            f"COALESCE({alias_chamados}.ultima_atualizacao, atual.data_atualizacao, '')"
        )
    if "ultima_atualizacao" in colunas_chamados:
        return f"COALESCE({alias_chamados}.ultima_atualizacao, '')"
    if possui_tabela_atualizacoes:
        return "COALESCE(atual.data_atualizacao, '')"
    return "''"


def join_ultima_atualizacao(nome_banco, alias_chamados="c"):
    if not tabela_existe(nome_banco, "chamado_atualizacoes"):
        return ""
    return f"""
        LEFT JOIN (
            SELECT ca.id_chamado, ca.data_atualizacao
            FROM chamado_atualizacoes ca
            INNER JOIN (
                SELECT id_chamado, MAX(id) AS max_id
                FROM chamado_atualizacoes
                GROUP BY id_chamado
            ) ult ON ult.id_chamado = ca.id_chamado AND ult.max_id = ca.id
        ) atual ON atual.id_chamado = {alias_chamados}.id_chamado
    """


def listar_clientes(nome_banco):
    colunas = obter_colunas_tabela(nome_banco, "usuarios")
    if not colunas:
        return []

    registros = executar_select(
        nome_banco,
        f"""
        SELECT
            {coluna_ou_padrao(colunas, 'usuario')},
            {coluna_ou_padrao(colunas, 'senha')},
            {coluna_ou_padrao(colunas, 'nome_completo')},
            {coluna_ou_padrao(colunas, 'telefone')},
            {coluna_ou_padrao(colunas, 'documento')}
        FROM usuarios
        WHERE {'tipo = %s' if 'tipo' in colunas else '1=1'}
        ORDER BY usuario
        """,
        ('Cliente',) if 'tipo' in colunas else (),
    )
    return [
        {
            "nomeCompleto": r["nome_completo"] or "",
            "telefone": r["telefone"] or "",
            "documento": r["documento"] or "",
            "login": r["usuario"] or "",
            "senha": r["senha"] or "",
        }
        for r in registros
        if r["usuario"]
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



def listar_chamados(nome_banco, limite=50, offset=0):
    colunas = obter_colunas_tabela(nome_banco, "chamados")
    if not colunas:
        return []

    chamados = executar_select(
        nome_banco,
        f"""
        SELECT
            {coluna_ou_padrao(colunas, 'id_chamado', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'cliente', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'login_cliente', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'resumo', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'prioridade', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'status', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'abertura', prefixo='c.')},
            {expressao_ultima_atualizacao(nome_banco)} AS ultima_atualizacao
        FROM chamados c
        {join_ultima_atualizacao(nome_banco)}
        ORDER BY ultima_atualizacao DESC, id_chamado DESC
        LIMIT %s OFFSET %s
        """,
        (limite, offset),
    )
    return [
        {
            "id": c["id_chamado"],
            "client": c["cliente"],
            "clienteLogin": c["login_cliente"],
            "summary": c["resumo"],
            "priority": c["prioridade"],
            "status": c["status"],
            "openedAt": c["abertura"] or "",
            "lastUpdate": c["ultima_atualizacao"] or "",
        }
        for c in chamados
    ]


def verificar_datas_atualizacao_chamados(nome_banco, referencias):
    colunas = obter_colunas_tabela(nome_banco, "chamados")
    if not colunas or not referencias:
        return {}

    ids = [str(item.get("id") or "").strip() for item in referencias if (item.get("id") or "").strip()]
    if not ids:
        return {}

    placeholders = ", ".join(["%s"] * len(ids))
    registros = executar_select(
        nome_banco,
        f"""
        SELECT c.id_chamado, {expressao_ultima_atualizacao(nome_banco)} AS ultima_atualizacao
        FROM chamados c
        {join_ultima_atualizacao(nome_banco)}
        WHERE c.id_chamado IN ({placeholders})
        """,
        tuple(ids),
    )
    por_id = {registro["id_chamado"]: (registro["ultima_atualizacao"] or "") for registro in registros}

    resultado = {}
    for item in referencias:
        id_chamado = str(item.get("id") or "").strip()
        data_cache = str(item.get("lastUpdate") or "")
        data_banco = por_id.get(id_chamado)
        resultado[id_chamado] = {
            "exists": data_banco is not None,
            "cachedExactDateExists": data_banco == data_cache if data_banco is not None else False,
            "dbLastUpdate": data_banco or "",
            "needsRefresh": data_banco is None or data_banco != data_cache,
        }
    return resultado


def obter_chamado_detalhe(nome_banco, id_chamado):
    colunas = obter_colunas_tabela(nome_banco, "chamados")
    if not colunas:
        return None

    chamado = executar_select(
        nome_banco,
        f"""
        SELECT
            {coluna_ou_padrao(colunas, 'id_chamado', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'cliente', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'login_cliente', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'resumo', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'descricao', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'prioridade', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'status', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'numero_processo', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'parceria', padrao='0', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'parceria_porcentagem', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'parceria_com', prefixo='c.')},
            {coluna_ou_padrao(colunas, 'abertura', prefixo='c.')},
            {expressao_ultima_atualizacao(nome_banco)} AS ultima_atualizacao
        FROM chamados c
        {join_ultima_atualizacao(nome_banco)}
        WHERE c.id_chamado = %s
        """,
        (id_chamado,),
        fetch_one=True,
    )
    if not chamado:
        return None

    atualizacoes = []
    if tabela_existe(nome_banco, "chamado_atualizacoes"):
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

# keep rest of file from original after this marker

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
    chamado_normalizado = dict(chamado or {})

    def transacao(conn):
        cursor = conn.cursor()

        id_chamado = (chamado_normalizado.get("id") or "").strip()
        if not id_chamado:
            cursor.execute(
                """
                SELECT id_chamado
                FROM chamados
                WHERE id_chamado REGEXP '^C-[0-9]+$'
                ORDER BY CAST(SUBSTRING(id_chamado, 3) AS UNSIGNED) DESC
                LIMIT 1
                """
            )
            ultimo = cursor.fetchone()
            proximo_numero = 1
            if ultimo and ultimo[0]:
                try:
                    proximo_numero = int(str(ultimo[0]).split("-")[-1]) + 1
                except (ValueError, TypeError):
                    proximo_numero = 1
            id_chamado = f"C-{proximo_numero}"
            chamado_normalizado["id"] = id_chamado

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
                chamado_normalizado["id"],
                chamado_normalizado["client"],
                chamado_normalizado["clienteLogin"],
                chamado_normalizado["summary"],
                chamado_normalizado["description"],
                chamado_normalizado["priority"],
                chamado_normalizado["status"],
                chamado_normalizado["processNumber"],
                1 if chamado_normalizado.get("hasPartnership") else 0,
                chamado_normalizado.get("partnershipPercent", ""),
                chamado_normalizado.get("partnershipWith", ""),
                chamado_normalizado["openedAt"],
                chamado_normalizado["lastUpdate"],
            ),
        )

        cursor.execute(
            """
            SELECT autor, mensagem, data_atualizacao, anexos
            FROM chamado_atualizacoes
            WHERE id_chamado = %s
            """,
            (chamado_normalizado["id"],),
        )
        existentes = {
            (
                row[0] or "",
                row[1] or "",
                row[2] or "",
                row[3] or "[]",
            )
            for row in cursor.fetchall()
        }

        for atualizacao in chamado_normalizado.get("updates", []):
            anexos_serializados = json.dumps(atualizacao.get("attachments", []), ensure_ascii=False)
            assinatura = (
                atualizacao.get("author", "Técnico") or "",
                atualizacao.get("message", "") or "",
                atualizacao.get("date", datetime.now().strftime("%d/%m/%Y %H:%M")) or "",
                anexos_serializados,
            )
            if assinatura in existentes:
                continue
            cursor.execute(
                """
                INSERT INTO chamado_atualizacoes (id_chamado, autor, mensagem, data_atualizacao, anexos)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    chamado_normalizado["id"],
                    assinatura[0],
                    assinatura[1],
                    assinatura[2],
                    assinatura[3],
                ),
            )

        cursor.close()

    executar_transacao(nome_banco, transacao)
    return chamado_normalizado


def excluir_chamado(nome_banco, id_chamado):
    executar_write(nome_banco, "DELETE FROM chamados WHERE id_chamado = %s", (id_chamado,))


def autenticar_usuario(nome_banco, usuario, senha):
    registro = executar_select(
        nome_banco,
        "SELECT usuario, senha, tipo FROM usuarios WHERE usuario = %s LIMIT 1",
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
        return responder_json({"projetos": projetos, "padrao": "teste"})
    except RuntimeError as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 500)


@app.route("/api/clientes", methods=["GET"])
async def api_clientes_listar():
    try:
        nome_banco = obter_banco_requisicao()
        clientes = await executar_em_thread(listar_clientes, nome_banco)
        return responder_json(clientes)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/clientes", methods=["PUT"])
async def api_clientes_substituir():
    try:
        nome_banco = obter_banco_requisicao()
        clientes = request.json or []
        await executar_em_thread(substituir_clientes, nome_banco, clientes)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/clientes", methods=["POST"])
async def api_cliente_inserir():
    try:
        nome_banco = obter_banco_requisicao()
        await executar_em_thread(inserir_cliente, nome_banco, request.json or {})
        return responder_json({"ok": True}, 201)
    except (ValueError, RuntimeError, MySQLdb.MySQLError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["GET"])
async def api_chamados_listar():
    try:
        nome_banco = obter_banco_requisicao()
        limite = parse_int_param(request.args.get("limit"), padrao=50, minimo=1, maximo=200)
        offset = parse_int_param(request.args.get("offset"), padrao=0, minimo=0, maximo=1000000)
        chamados = await executar_em_thread(listar_chamados, nome_banco, limite, offset)
        return responder_json(chamados)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

@app.route("/api/chamados/validar-cache", methods=["POST"])
async def api_chamados_validar_cache():
    try:
        nome_banco = obter_banco_requisicao()
        chamados_referencia = (request.json or {}).get("chamados") or []
        validacoes = await executar_em_thread(verificar_datas_atualizacao_chamados, nome_banco, chamados_referencia)
        return responder_json({"ok": True, "validacoes": validacoes})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["GET"])
async def api_chamado_detalhar(id_chamado):
    try:
        nome_banco = obter_banco_requisicao()
        chamado = await executar_em_thread(obter_chamado_detalhe, nome_banco, id_chamado)
        if not chamado:
            return responder_json({"ok": False, "erro": "Chamado não encontrado."}, 404)
        return responder_json(chamado)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["PUT"])
async def api_chamados_substituir():
    try:
        nome_banco = obter_banco_requisicao()
        await executar_em_thread(substituir_chamados, nome_banco, request.json or [])
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["POST"])
async def api_chamado_inserir():
    try:
        nome_banco = obter_banco_requisicao()
        chamado_salvo = await executar_em_thread(salvar_chamado_individual, nome_banco, request.json or {})
        return responder_json({"ok": True, "chamado": chamado_salvo}, 201)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["PUT"])
async def api_chamado_atualizar(id_chamado):
    try:
        nome_banco = obter_banco_requisicao()
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
        nome_banco = obter_banco_requisicao()
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
        nome_banco = dados.get("banco") or "teste"
        valido = await executar_em_thread(nome_banco_valido, nome_banco)
        if not valido:
            raise ValueError("Nome de banco inválido.")
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)

    try:
        autenticado = await executar_em_thread(autenticar_usuario, nome_banco, usuario, senha)
    except (MySQLdb.OperationalError, MySQLdb.ProgrammingError):
        return responder_json({"ok": False, "erro": f"Banco '{nome_banco}' não encontrado."}, 400)
    if not autenticado:
        return responder_json({"ok": False, "erro": "Credenciais inválidas."}, 401)

    tipo = autenticado["tipo"]
    redirect = "admin.html" if tipo == "Administrador" else ("index.html" if tipo == "Técnico" else "cliente.html")
    cliente_id = autenticado["usuario"] if tipo == "Cliente" else ""
    return responder_json(
        {
            "ok": True,
            "usuario": usuario,
            "tipo": tipo,
            "clienteId": cliente_id,
            "redirect": redirect,
            "banco": nome_banco,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
