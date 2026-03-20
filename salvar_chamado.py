import asyncio
import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from queue import Empty, Queue
from threading import Lock
from typing import Optional

import pymysql
from flask import Flask, jsonify, make_response, request, send_from_directory

pymysql.install_as_MySQLdb()

import MySQLdb
import MySQLdb.cursors

BASE_DIR = Path(__file__).resolve().parent
STATIC_PAGES = {
    "index.html",
    "login.html",
    "admin.html",
    "cliente.html",
    "cadastro-cliente.html",
    "create.html",
    "details.html",
}

host = os.getenv("MYSQLHOST", os.getenv("DB_HOST", "localhost"))
user = os.getenv("MYSQLUSER", os.getenv("DB_USER", "root"))
password = os.getenv("MYSQLPASSWORD", os.getenv("DB_PASSWORD", ""))
db = os.getenv("MYSQLDATABASE", os.getenv("DB_NAME", "teste"))
port = int(os.getenv("MYSQLPORT", os.getenv("DB_PORT", "3306")))
nome_banco = db


POOL_SIZE = 1
DB_CACHE_TTL_MINUTOS = 2
VALIDACAO_BANCO_TTL_SEGUNDOS = 30

app = Flask(__name__, static_folder="assets", static_url_path="/assets")

SISTEMA_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}
bancos_cache = {"valores": [], "expira_em": datetime.min}
validacao_bancos_cache = {}
tabelas_atualizacoes_cache = {}

_connection_lock = Lock()
_pools = {}



def criar_conexao(nome_banco=None):
    if configuracao_banco_incompleta():
        raise RuntimeError("Configure as variáveis MYSQLHOST, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE e MYSQLPORT no ambiente.")
    banco_destino = nome_banco or db
    return MySQLdb.connect(host=host, user=user, passwd=password, db=banco_destino, port=port, charset="utf8mb4")


def configuracao_banco_incompleta():
    return not all([host, user, db]) or port <= 0


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
    descartar_conexao = False

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
        try:
            conn.ping(True)
        except (AttributeError, MySQLdb.Error):
            _descartar_conexao_pool(chave, conn)
            conn = criar_conexao(chave)
            with _connection_lock:
                dados_pool["criadas"] += 1
        yield conn
    except (MySQLdb.OperationalError, MySQLdb.InterfaceError) as erro:
        descartar_conexao = _erro_mysql_recuperavel(erro)
        raise
    finally:
        if conn is None:
            return
        if descartar_conexao:
            _descartar_conexao_pool(chave, conn)
            return
        try:
            pool.put_nowait(conn)
        except Exception:
            _descartar_conexao_pool(chave, conn)


def _erro_mysql_recuperavel(erro):
    mensagem = str(erro).lower()
    return any(
        termo in mensagem
        for termo in [
            "server has gone away",
            "lost connection",
            "connection was killed",
            "commands out of sync",
            "gone away",
        ]
    )


def _descartar_conexao_pool(chave, conn):
    try:
        conn.close()
    except Exception:
        pass
    with _connection_lock:
        dados_pool = _pools.get(chave)
        if dados_pool:
            dados_pool["criadas"] = max(0, dados_pool["criadas"] - 1)


def _executar_com_retry(nome_banco, operacao):
    for tentativa in range(2):
        with conexao_pool(nome_banco) as conn:
            try:
                return operacao(conn)
            except (MySQLdb.OperationalError, MySQLdb.InterfaceError) as erro:
                if tentativa == 1 or not _erro_mysql_recuperavel(erro):
                    raise


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




def validar_banco_disponivel(nome_banco):
    agora = datetime.now()
    expira_em: Optional[datetime] = validacao_bancos_cache.get(nome_banco)
    if expira_em and agora < expira_em:
        return

    with conexao_pool(nome_banco):
        pass

    validacao_bancos_cache[nome_banco] = agora + timedelta(seconds=VALIDACAO_BANCO_TTL_SEGUNDOS)


def obter_banco_requisicao():
    nome_banco = request.headers.get("X-Project-DB", "teste")
    if not nome_banco_valido(nome_banco):
        raise ValueError("Nome de banco inválido.")
    try:
        validar_banco_disponivel(nome_banco)
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


def normalizar_financeiro(financeiro):
    if not financeiro:
        return []
    if isinstance(financeiro, str):
        try:
            dados = json.loads(financeiro)
        except json.JSONDecodeError:
            return []
    else:
        dados = financeiro

    if not isinstance(dados, list):
        return []

    itens_normalizados = []
    for indice, item in enumerate(dados):
        if not isinstance(item, dict):
            continue
        parcelas = item.get("installments", item.get("parcelas", 1))
        try:
            parcelas = max(1, int(parcelas))
        except (TypeError, ValueError):
            parcelas = 1
        valor = item.get("value", item.get("valor", 0))
        try:
            valor = float(valor)
        except (TypeError, ValueError):
            valor = 0.0
        parcelas_pagas = item.get("paidInstallments", item.get("parcelasPagas", []))
        if not isinstance(parcelas_pagas, list):
            parcelas_pagas = []
        parcelas_pagas = [bool(parcelas_pagas[i]) if i < len(parcelas_pagas) else False for i in range(parcelas)]
        produto = str(item.get("product", item.get("produto", "")) or "").strip()
        if not produto:
            continue
        itens_normalizados.append(
            {
                "id": item.get("id") or f"financeiro-{indice}",
                "product": produto,
                "value": valor,
                "installments": parcelas,
                "description": str(item.get("description", item.get("descricao", "")) or "").strip(),
                "paidInstallments": parcelas_pagas,
            }
        )
    return itens_normalizados


def resolver_tabela_atualizacoes(conn):
    cursor = conn.cursor()
    try:
        for nome_tabela in ("chamados_atualizacoes", "chamado_atualizacoes"):
            cursor.execute("SHOW TABLES LIKE %s", (nome_tabela,))
            if cursor.fetchone():
                return nome_tabela
    finally:
        cursor.close()
    return "chamado_atualizacoes"


def garantir_colunas_financeiras(conn, tabela_atualizacoes):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DATABASE()")
        banco_atual = cursor.fetchone()[0]
        cursor.execute(
            f"""
            SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND COLUMN_NAME IN ('financeiro_cliente', 'financeiro_escritorio')
            """,
            (banco_atual, tabela_atualizacoes),
        )
        colunas_existentes = {
            linha[0]: {
                "tipo": (linha[1] or "").lower(),
                "tamanho": linha[2],
            }
            for linha in cursor.fetchall()
        }
        if "financeiro_cliente" not in colunas_existentes:
            cursor.execute(f"ALTER TABLE {tabela_atualizacoes} ADD COLUMN financeiro_cliente LONGTEXT NULL")
        elif colunas_existentes["financeiro_cliente"]["tipo"] != "longtext":
            cursor.execute(f"ALTER TABLE {tabela_atualizacoes} MODIFY COLUMN financeiro_cliente LONGTEXT NULL")
        if "financeiro_escritorio" not in colunas_existentes:
            cursor.execute(f"ALTER TABLE {tabela_atualizacoes} ADD COLUMN financeiro_escritorio LONGTEXT NULL")
        elif colunas_existentes["financeiro_escritorio"]["tipo"] != "longtext":
            cursor.execute(f"ALTER TABLE {tabela_atualizacoes} MODIFY COLUMN financeiro_escritorio LONGTEXT NULL")
    finally:
        cursor.close()


def preparar_tabela_atualizacoes(nome_banco):
    agora = datetime.now()
    cache = tabelas_atualizacoes_cache.get(nome_banco)
    if cache and agora < cache["expira_em"]:
        return cache["tabela"]

    def operacao(conn):
        tabela_atualizacoes = resolver_tabela_atualizacoes(conn)
        garantir_colunas_financeiras(conn, tabela_atualizacoes)
        return tabela_atualizacoes

    tabela = _executar_com_retry(nome_banco, operacao)
    tabelas_atualizacoes_cache[nome_banco] = {
        "tabela": tabela,
        "expira_em": agora + timedelta(minutes=10),
    }
    return tabela


def preparar_tabela_atualizacoes_em_conexao(nome_banco, conn):
    agora = datetime.now()
    cache = tabelas_atualizacoes_cache.get(nome_banco)
    if cache and agora < cache["expira_em"]:
        return cache["tabela"]

    tabela = resolver_tabela_atualizacoes(conn)
    garantir_colunas_financeiras(conn, tabela)
    tabelas_atualizacoes_cache[nome_banco] = {
        "tabela": tabela,
        "expira_em": agora + timedelta(minutes=10),
    }
    return tabela


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



def listar_chamados(nome_banco, limite=50, offset=0):
    chamados = executar_select(
        nome_banco,
        """
        SELECT id_chamado, cliente, login_cliente, resumo, prioridade, status, abertura, ultima_atualizacao
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
            "clienteLogin": c["login_cliente"],
            "summary": c["resumo"],
            "priority": c["prioridade"],
            "status": c["status"],
            "openedAt": c["abertura"] or "",
            "lastUpdate": c["ultima_atualizacao"] or "",
        }
        for c in chamados
    ]


def obter_chamado_detalhe(nome_banco, id_chamado):
    tabela_atualizacoes = preparar_tabela_atualizacoes(nome_banco)
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
        f"""
        SELECT autor, mensagem, data_atualizacao, anexos, financeiro_cliente, financeiro_escritorio
        FROM {tabela_atualizacoes}
        WHERE id_chamado = %s
        ORDER BY id DESC
        """,
        (id_chamado,),
    )
    financeiro_cliente = normalizar_financeiro(atualizacoes[0]["financeiro_cliente"]) if atualizacoes else []
    financeiro_escritorio = normalizar_financeiro(atualizacoes[0]["financeiro_escritorio"]) if atualizacoes else []

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
        "financialClient": financeiro_cliente,
        "financialOffice": financeiro_escritorio,
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
        tabela_atualizacoes = preparar_tabela_atualizacoes_em_conexao(nome_banco, conn)
        cursor.execute(f"DELETE FROM {tabela_atualizacoes}")
        cursor.execute("DELETE FROM chamados")

        for chamado in chamados:
            financeiro_cliente = json.dumps(normalizar_financeiro(chamado.get("financialClient", [])), ensure_ascii=False)
            financeiro_escritorio = json.dumps(normalizar_financeiro(chamado.get("financialOffice", [])), ensure_ascii=False)
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
                    f"""
                    INSERT INTO {tabela_atualizacoes} (
                        id_chamado, autor, mensagem, data_atualizacao, anexos, financeiro_cliente, financeiro_escritorio
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        chamado["id"],
                        atualizacao.get("author", "Técnico"),
                        atualizacao.get("message", ""),
                        atualizacao.get("date", datetime.now().strftime("%d/%m/%Y %H:%M")),
                        json.dumps(atualizacao.get("attachments", []), ensure_ascii=False),
                        financeiro_cliente,
                        financeiro_escritorio,
                    ),
                )

        cursor.close()

    executar_transacao(nome_banco, transacao)


def salvar_chamado_individual(nome_banco, chamado):
    chamado_normalizado = dict(chamado or {})
    chamado_normalizado["financialClient"] = normalizar_financeiro(chamado_normalizado.get("financialClient", []))
    chamado_normalizado["financialOffice"] = normalizar_financeiro(chamado_normalizado.get("financialOffice", []))

    def transacao(conn):
        cursor = conn.cursor()
        tabela_atualizacoes = preparar_tabela_atualizacoes_em_conexao(nome_banco, conn)

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
            f"""
            SELECT autor, mensagem, data_atualizacao, anexos
            FROM {tabela_atualizacoes}
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
        financeiro_cliente = json.dumps(chamado_normalizado.get("financialClient", []), ensure_ascii=False)
        financeiro_escritorio = json.dumps(chamado_normalizado.get("financialOffice", []), ensure_ascii=False)

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
                f"""
                INSERT INTO {tabela_atualizacoes} (
                    id_chamado, autor, mensagem, data_atualizacao, anexos, financeiro_cliente, financeiro_escritorio
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    chamado_normalizado["id"],
                    assinatura[0],
                    assinatura[1],
                    assinatura[2],
                    assinatura[3],
                    financeiro_cliente,
                    financeiro_escritorio,
                ),
            )

        cursor.execute(
            f"""
            UPDATE {tabela_atualizacoes}
            SET financeiro_cliente = %s,
                financeiro_escritorio = %s
            WHERE id_chamado = %s
            """,
            (
                financeiro_cliente,
                financeiro_escritorio,
                chamado_normalizado["id"],
            ),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                f"""
                INSERT INTO {tabela_atualizacoes} (
                    id_chamado, autor, mensagem, data_atualizacao, anexos, financeiro_cliente, financeiro_escritorio
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    chamado_normalizado["id"],
                    "Sistema",
                    "Registro financeiro inicial.",
                    chamado_normalizado.get("lastUpdate", datetime.now().strftime("%d/%m/%Y %H:%M")),
                    "[]",
                    financeiro_cliente,
                    financeiro_escritorio,
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


@app.route("/")
def servir_raiz():
    return send_from_directory(BASE_DIR, "login.html")


@app.route("/api/health", methods=["GET"])
def healthcheck():
    return responder_json({"ok": True, "status": "healthy"})


@app.route("/<path:arquivo>")
def servir_arquivos_estaticos(arquivo):
    if arquivo in STATIC_PAGES:
        return send_from_directory(BASE_DIR, arquivo)
    return send_from_directory(app.static_folder, arquivo)


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
    porta_http = int(os.getenv("PORT", "5000"))
    debug = os.getenv("FLASK_DEBUG", "").lower() in {"1", "true", "yes"}
    app.run(host="0.0.0.0", port=porta_http, debug=debug)
