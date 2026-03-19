import asyncio
import json
import re
from contextlib import contextmanager
from datetime import datetime, timedelta
from queue import Empty, Queue
from threading import Lock

import MySQLdb
from flask import Flask, jsonify, request

HOST = "ballast.proxy.rlwy.net"
USER = "root"
PASSWORD = "cUxQKiTNIHZUlBQhphYhiESVTcrCJTGO"
DB_PADRAO = "teste"
PORT = 15192
POOL_MAX = 8
CACHE_BANCOS_MIN = 2
BANCOS_SISTEMA = {"information_schema", "mysql", "performance_schema", "sys"}
CAMPOS_CHAMADO = [
    "id", "client", "clienteLogin", "summary", "description", "priority", "status",
    "processNumber", "hasPartnership", "partnershipPercent", "partnershipWith", "openedAt", "lastUpdate",
]

app = Flask(__name__)
cache_bancos = {"valores": [], "expira": datetime.min}
lock_pool = Lock()
pools = {}


@contextmanager
def conectar(nome_banco=DB_PADRAO):
    conexao = None
    with lock_pool:
        pool = pools.setdefault(nome_banco, {"fila": Queue(maxsize=POOL_MAX), "total": 0})

    try:
        conexao = pool["fila"].get_nowait()
    except Empty:
        with lock_pool:
            if pool["total"] < POOL_MAX:
                conexao = MySQLdb.connect(
                    host=HOST, user=USER, passwd=PASSWORD, db=nome_banco, port=PORT, charset="utf8mb4"
                )
                pool["total"] += 1
        if conexao is None:
            try:
                conexao = pool["fila"].get(timeout=5)
            except Empty as erro:
                raise RuntimeError("Pool de conexões esgotado. Tente novamente.") from erro

    try:
        yield conexao
    finally:
        try:
            pool["fila"].put_nowait(conexao)
        except Exception:
            try:
                conexao.close()
            except Exception:
                pass
            with lock_pool:
                pool["total"] = max(0, pool["total"] - 1)


def banco(nome_banco, acao):
    nome_banco = nome_banco or DB_PADRAO
    for tentativa in range(2):
        with conectar(nome_banco) as conexao:
            try:
                return acao(conexao)
            except MySQLdb.OperationalError as erro:
                texto = str(erro).lower()
                if tentativa == 1 or not any(item in texto for item in ["server has gone away", "lost connection", "connection was killed"]):
                    raise
                try:
                    conexao.close()
                except Exception:
                    pass
                with lock_pool:
                    if nome_banco in pools:
                        pools[nome_banco]["total"] = max(0, pools[nome_banco]["total"] - 1)


def consultar(nome_banco, sql, params=(), unico=False, dicionario=True):
    return banco(
        nome_banco,
        lambda conexao: _consultar(conexao, sql, params, unico, dicionario),
    )


def _consultar(conexao, sql, params, unico, dicionario):
    cursor = conexao.cursor(MySQLdb.cursors.DictCursor) if dicionario else conexao.cursor()
    cursor.execute(sql, params)
    resultado = cursor.fetchone() if unico else cursor.fetchall()
    cursor.close()
    return resultado


def executar(nome_banco, sql, params=()):
    banco(nome_banco, lambda conexao: _executar(conexao, sql, params))


def _executar(conexao, sql, params):
    cursor = conexao.cursor()
    cursor.execute(sql, params)
    conexao.commit()
    cursor.close()


def transacao(nome_banco, acao):
    def rodar(conexao):
        try:
            acao(conexao)
            conexao.commit()
        except Exception:
            conexao.rollback()
            raise

    banco(nome_banco, rodar)


def resposta(payload, status=200):
    retorno = jsonify(payload)
    retorno.status_code = status
    retorno.headers["Access-Control-Allow-Origin"] = "*"
    retorno.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Project-DB"
    retorno.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    return retorno


@app.before_request
def preflight():
    return resposta({}, 200) if request.method == "OPTIONS" else None


@app.after_request
def cors(retorno):
    retorno.headers["Access-Control-Allow-Origin"] = "*"
    retorno.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Project-DB"
    retorno.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,DELETE,OPTIONS"
    return retorno


@app.errorhandler(MySQLdb.MySQLError)
def erro_mysql(erro):
    return resposta({"ok": False, "erro": f"Erro de banco de dados: {erro}"}, 500)


async def em_thread(funcao, *args):
    return await asyncio.to_thread(funcao, *args)


def inteiro(valor, padrao=None, minimo=1, maximo=1000):
    if valor in (None, ""):
        return padrao
    try:
        valor = int(valor)
    except (TypeError, ValueError) as erro:
        raise ValueError("Parâmetro de paginação inválido.") from erro
    if valor < minimo or valor > maximo:
        raise ValueError(f"Parâmetro deve estar entre {minimo} e {maximo}.")
    return valor


def banco_valido(nome_banco):
    return bool(re.fullmatch(r"[A-Za-z0-9_]+", nome_banco or ""))


def listar_bancos():
    if datetime.now() < cache_bancos["expira"] and cache_bancos["valores"]:
        return cache_bancos["valores"]
    cache_bancos["valores"] = [
        linha[0] for linha in consultar(DB_PADRAO, "SHOW DATABASES", dicionario=False) if linha[0] not in BANCOS_SISTEMA
    ]
    cache_bancos["expira"] = datetime.now() + timedelta(minutes=CACHE_BANCOS_MIN)
    return cache_bancos["valores"]


def listar_clientes(nome_banco):
    return [
        {
            "nomeCompleto": item["nome_completo"] or "",
            "telefone": item["telefone"] or "",
            "documento": item["documento"] or "",
            "login": item["usuario"],
            "senha": item["senha"],
        }
        for item in consultar(
            nome_banco,
            "SELECT usuario, senha, nome_completo, telefone, documento FROM usuarios WHERE tipo = 'Cliente' ORDER BY usuario",
        )
    ]


def salvar_clientes(nome_banco, clientes):
    def rodar(conexao):
        cursor = conexao.cursor()
        cursor.execute("DELETE FROM usuarios WHERE tipo = 'Cliente'")
        for cliente in clientes:
            cursor.execute(
                "INSERT INTO usuarios (usuario, senha, tipo, nome_completo, telefone, documento) VALUES (%s, %s, 'Cliente', %s, %s, %s)",
                (cliente["login"], cliente["senha"], cliente.get("nomeCompleto") or None, cliente.get("telefone") or None, cliente.get("documento") or None),
            )
        cursor.close()

    transacao(nome_banco, rodar)


def criar_cliente(nome_banco, cliente):
    executar(
        nome_banco,
        "INSERT INTO usuarios (usuario, senha, tipo, nome_completo, telefone, documento) VALUES (%s, %s, 'Cliente', %s, %s, %s)",
        (cliente["login"], cliente["senha"], cliente.get("nomeCompleto") or None, cliente.get("telefone") or None, cliente.get("documento") or None),
    )


def listar_chamados(nome_banco, limite=50, offset=0):
    return [
        {
            "id": item["id_chamado"],
            "client": item["cliente"],
            "clienteLogin": item["login_cliente"],
            "summary": item["resumo"],
            "priority": item["prioridade"],
            "status": item["status"],
            "openedAt": item["abertura"] or "",
            "lastUpdate": item["ultima_atualizacao"] or "",
        }
        for item in consultar(
            nome_banco,
            "SELECT id_chamado, cliente, login_cliente, resumo, prioridade, status, abertura, ultima_atualizacao FROM chamados ORDER BY ultima_atualizacao DESC, id_chamado DESC LIMIT %s OFFSET %s",
            (limite, offset),
        )
    ]


def detalhe_chamado(nome_banco, id_chamado):
    chamado = consultar(
        nome_banco,
        "SELECT id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status, numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao FROM chamados WHERE id_chamado = %s",
        (id_chamado,),
        unico=True,
    )
    if not chamado:
        return None
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
                "author": item["autor"],
                "message": item["mensagem"],
                "date": item["data_atualizacao"],
                "attachments": json.loads(item["anexos"]) if item["anexos"] else [],
            }
            for item in consultar(
                nome_banco,
                "SELECT autor, mensagem, data_atualizacao, anexos FROM chamado_atualizacoes WHERE id_chamado = %s ORDER BY id DESC",
                (id_chamado,),
            )
        ],
    }


def salvar_lista_chamados(nome_banco, chamados):
    def rodar(conexao):
        cursor = conexao.cursor()
        cursor.execute("DELETE FROM chamado_atualizacoes")
        cursor.execute("DELETE FROM chamados")
        for chamado in chamados:
            _salvar_chamado(cursor, normalizar_chamado(chamado), sobrescrever=True)
        cursor.close()

    transacao(nome_banco, rodar)


def normalizar_chamado(chamado):
    dados = {campo: chamado.get(campo, "") for campo in CAMPOS_CHAMADO}
    dados["updates"] = chamado.get("updates", [])
    dados["hasPartnership"] = bool(chamado.get("hasPartnership"))
    return dados


def proximo_id(cursor):
    cursor.execute("SELECT id_chamado FROM chamados WHERE id_chamado REGEXP '^C-[0-9]+$' ORDER BY CAST(SUBSTRING(id_chamado, 3) AS UNSIGNED) DESC LIMIT 1")
    ultimo = cursor.fetchone()
    try:
        return f"C-{int(str(ultimo[0]).split('-')[-1]) + 1}" if ultimo and ultimo[0] else "C-1"
    except (TypeError, ValueError):
        return "C-1"


def _salvar_chamado(cursor, chamado, sobrescrever=False):
    if not (chamado["id"] or "").strip():
        chamado["id"] = proximo_id(cursor)
    cursor.execute(
        """
        INSERT INTO chamados (
            id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
            numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            cliente = VALUES(cliente), login_cliente = VALUES(login_cliente), resumo = VALUES(resumo),
            descricao = VALUES(descricao), prioridade = VALUES(prioridade), status = VALUES(status),
            numero_processo = VALUES(numero_processo), parceria = VALUES(parceria),
            parceria_porcentagem = VALUES(parceria_porcentagem), parceria_com = VALUES(parceria_com),
            abertura = VALUES(abertura), ultima_atualizacao = VALUES(ultima_atualizacao)
        """,
        (
            chamado["id"], chamado["client"], chamado["clienteLogin"], chamado["summary"], chamado["description"],
            chamado["priority"], chamado["status"], chamado["processNumber"], 1 if chamado["hasPartnership"] else 0,
            chamado["partnershipPercent"], chamado["partnershipWith"], chamado["openedAt"], chamado["lastUpdate"],
        ),
    )
    if sobrescrever:
        for update in chamado.get("updates", []):
            cursor.execute(
                "INSERT INTO chamado_atualizacoes (id_chamado, autor, mensagem, data_atualizacao, anexos) VALUES (%s, %s, %s, %s, %s)",
                (chamado["id"], update.get("author", "Técnico"), update.get("message", ""), update.get("date", datetime.now().strftime("%d/%m/%Y %H:%M")), json.dumps(update.get("attachments", []), ensure_ascii=False)),
            )
        return

    cursor.execute("SELECT autor, mensagem, data_atualizacao, anexos FROM chamado_atualizacoes WHERE id_chamado = %s", (chamado["id"],))
    existentes = {(item[0] or "", item[1] or "", item[2] or "", item[3] or "[]") for item in cursor.fetchall()}
    for update in chamado.get("updates", []):
        assinatura = (
            update.get("author", "Técnico") or "",
            update.get("message", "") or "",
            update.get("date", datetime.now().strftime("%d/%m/%Y %H:%M")) or "",
            json.dumps(update.get("attachments", []), ensure_ascii=False),
        )
        if assinatura not in existentes:
            cursor.execute(
                "INSERT INTO chamado_atualizacoes (id_chamado, autor, mensagem, data_atualizacao, anexos) VALUES (%s, %s, %s, %s, %s)",
                (chamado["id"], assinatura[0], assinatura[1], assinatura[2], assinatura[3]),
            )


def salvar_chamado(nome_banco, chamado):
    chamado = normalizar_chamado(chamado)

    def rodar(conexao):
        cursor = conexao.cursor()
        _salvar_chamado(cursor, chamado)
        cursor.close()

    transacao(nome_banco, rodar)
    return chamado


def excluir_chamado(nome_banco, id_chamado):
    executar(nome_banco, "DELETE FROM chamados WHERE id_chamado = %s", (id_chamado,))


def login(nome_banco, usuario, senha):
    registro = consultar(nome_banco, "SELECT usuario, senha, tipo FROM usuarios WHERE usuario = %s LIMIT 1", (usuario,), unico=True)
    return registro if registro and registro["senha"] == senha else None


@app.route("/api/projetos", methods=["GET"])
async def api_projetos():
    try:
        return resposta({"projetos": await em_thread(listar_bancos), "padrao": DB_PADRAO})
    except RuntimeError as erro:
        return resposta({"ok": False, "erro": str(erro)}, 500)


@app.route("/api/clientes", methods=["GET", "PUT", "POST"])
async def api_clientes():
    try:
        if request.method == "GET":
            return resposta(await em_thread(listar_clientes, DB_PADRAO))
        if request.method == "PUT":
            await em_thread(salvar_clientes, DB_PADRAO, request.json or [])
            return resposta({"ok": True})
        await em_thread(criar_cliente, DB_PADRAO, request.json or {})
        return resposta({"ok": True}, 201)
    except (ValueError, RuntimeError, MySQLdb.MySQLError) as erro:
        return resposta({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["GET", "PUT", "POST"])
async def api_chamados():
    try:
        if request.method == "GET":
            return resposta(await em_thread(listar_chamados, DB_PADRAO, inteiro(request.args.get("limit"), 50, 1, 200), inteiro(request.args.get("offset"), 0, 0, 1000000)))
        if request.method == "PUT":
            await em_thread(salvar_lista_chamados, DB_PADRAO, request.json or [])
            return resposta({"ok": True})
        return resposta({"ok": True, "chamado": await em_thread(salvar_chamado, DB_PADRAO, request.json or {})}, 201)
    except (ValueError, RuntimeError) as erro:
        return resposta({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["GET", "PUT", "DELETE"])
async def api_chamado(id_chamado):
    try:
        if request.method == "GET":
            chamado = await em_thread(detalhe_chamado, DB_PADRAO, id_chamado)
            return resposta(chamado) if chamado else resposta({"ok": False, "erro": "Chamado não encontrado."}, 404)
        if request.method == "PUT":
            dados = request.json or {}
            dados["id"] = dados.get("id") or id_chamado
            await em_thread(salvar_chamado, DB_PADRAO, dados)
            return resposta({"ok": True})
        await em_thread(excluir_chamado, DB_PADRAO, id_chamado)
        return resposta({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return resposta({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/login", methods=["POST"])
async def api_login():
    dados = request.json or {}
    nome_banco = dados.get("banco") or DB_PADRAO
    usuario = (dados.get("usuario") or "").strip()
    senha = (dados.get("senha") or "").strip()

    try:
        if not await em_thread(banco_valido, nome_banco):
            raise ValueError("Nome de banco inválido.")
        autenticado = await em_thread(login, nome_banco, usuario, senha)
    except (ValueError, RuntimeError) as erro:
        return resposta({"ok": False, "erro": str(erro)}, 400)
    except (MySQLdb.OperationalError, MySQLdb.ProgrammingError):
        return resposta({"ok": False, "erro": f"Banco '{nome_banco}' não encontrado."}, 400)

    if not autenticado:
        return resposta({"ok": False, "erro": "Credenciais inválidas."}, 401)

    tipo = autenticado["tipo"]
    return resposta({
        "ok": True,
        "usuario": usuario,
        "tipo": tipo,
        "clienteId": autenticado["usuario"] if tipo == "Cliente" else "",
        "redirect": "admin.html" if tipo == "Administrador" else ("index.html" if tipo == "Técnico" else "cliente.html"),
        "banco": nome_banco,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
