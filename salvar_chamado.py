import json
import re
import os
import ssl
import hmac
import hashlib
import secrets
import smtplib
import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta
from email.message import EmailMessage
from typing import Optional
from queue import Empty, Queue
from threading import Lock

from dotenv import load_dotenv
load_dotenv()

import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import MySQLdb.cursors
from flask import Flask, jsonify, make_response, request
from flask import send_from_directory
from werkzeug.security import check_password_hash, generate_password_hash

host       = os.getenv("DB_HOST", "ballast.proxy.rlwy.net")
user       = os.getenv("DB_USER", "root")
password   = os.getenv("DB_PASSWORD", "")
db         = os.getenv("DB_NAME", "teste")
port       = int(os.getenv("DB_PORT", 15192))
nome_banco = db

POOL_SIZE = 8
DB_CACHE_TTL_MINUTOS = 2
VALIDACAO_BANCO_TTL_SEGUNDOS = 30

app = Flask(__name__)

SISTEMA_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}
bancos_cache = {"valores": [], "expira_em": datetime.min}
validacao_bancos_cache = {}
tabelas_atualizacoes_cache = {}
usuarios_cache = {}
rate_limit_cache = {}

_connection_lock = Lock()
_pools = {}
_rate_limit_lock = Lock()

PASSWORD_RESET_TOKEN_TTL_MINUTES = 15
PASSWORD_RESET_REQUEST_LIMIT = 5
PASSWORD_RESET_REQUEST_WINDOW_SECONDS = 15 * 60
PASSWORD_RESET_VALIDATE_LIMIT = 8
PASSWORD_RESET_VALIDATE_WINDOW_SECONDS = 15 * 60
PASSWORD_RESET_MAX_FAILED_ATTEMPTS = 5



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
    resposta.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Project-DB, X-Auth-User"
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

@app.route("/")
def home():
    return send_from_directory(".", "login.html")

@app.route("/<path:nome_arquivo>")
def servir_arquivos(nome_arquivo):
    if os.path.exists(nome_arquivo):
        return send_from_directory(".", nome_arquivo)
    return "Página não encontrada", 404

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


def obter_usuario_requisicao(nome_banco):
    usuario = (request.headers.get("X-Auth-User") or "").strip()
    if not usuario:
        raise ValueError("Usuário autenticado não informado.")
    registro = obter_usuario_por_login(nome_banco, usuario)
    if not registro:
        raise ValueError("Usuário autenticado inválido.")
    return registro["usuario"]


def normalizar_email(email):
    return (email or "").strip().lower()


def hash_senha(senha):
    if not senha:
        raise ValueError("Senha obrigatória.")
    if len(senha) < 8:
        raise ValueError("A senha deve ter pelo menos 8 caracteres.")
    return generate_password_hash(senha)


def senha_usa_hash(valor):
    return isinstance(valor, str) and valor.startswith(("scrypt:", "pbkdf2:"))


def verificar_senha(senha_informada, senha_salva):
    if not senha_informada or not senha_salva:
        return False
    if senha_usa_hash(senha_salva):
        try:
            return check_password_hash(senha_salva, senha_informada)
        except ValueError:
            return False
    return hmac.compare_digest(str(senha_salva), str(senha_informada))


def token_hash(valor):
    return hashlib.sha256((valor or "").encode("utf-8")).hexdigest()


def gerar_codigo_reset():
    return f"{secrets.randbelow(1000000):06d}"


def gerar_token_sessao_reset():
    return secrets.token_urlsafe(32)


def obter_ip_requisicao():
    cabecalho = (request.headers.get("X-Forwarded-For") or "").strip()
    if cabecalho:
        return cabecalho.split(",")[0].strip()
    return (request.remote_addr or "desconhecido").strip() or "desconhecido"


def aplicar_rate_limit(chave, limite, janela_segundos):
    agora = datetime.now()
    with _rate_limit_lock:
        entradas = [
            instante
            for instante in rate_limit_cache.get(chave, [])
            if (agora - instante).total_seconds() < janela_segundos
        ]
        if len(entradas) >= limite:
            rate_limit_cache[chave] = entradas
            raise ValueError("Muitas tentativas. Aguarde alguns minutos e tente novamente.")
        entradas.append(agora)
        rate_limit_cache[chave] = entradas


def limitar_solicitacao_reset(email):
    chave_email = hashlib.sha256(normalizar_email(email).encode("utf-8")).hexdigest()
    chave = f"pwd-reset:request:{obter_ip_requisicao()}:{chave_email}"
    aplicar_rate_limit(chave, PASSWORD_RESET_REQUEST_LIMIT, PASSWORD_RESET_REQUEST_WINDOW_SECONDS)


def limitar_validacao_reset(email):
    chave_email = hashlib.sha256(normalizar_email(email).encode("utf-8")).hexdigest()
    chave = f"pwd-reset:validate:{obter_ip_requisicao()}:{chave_email}"
    aplicar_rate_limit(chave, PASSWORD_RESET_VALIDATE_LIMIT, PASSWORD_RESET_VALIDATE_WINDOW_SECONDS)


def obter_config_email():
    host_email = (os.getenv("SMTP_HOST") or "").strip()
    usuario_email = (os.getenv("SMTP_USERNAME") or "").strip()
    senha_email = os.getenv("SMTP_PASSWORD") or ""
    remetente = (os.getenv("SMTP_FROM_EMAIL") or usuario_email).strip()
    porta = int(os.getenv("SMTP_PORT") or "587")
    usar_ssl = (os.getenv("SMTP_USE_SSL") or "0").strip() == "1"
    usar_tls = (os.getenv("SMTP_USE_TLS") or "1").strip() != "0"
    suppress = (os.getenv("SMTP_SUPPRESS_SEND") or "0").strip() == "1"

    return {
        "host": host_email,
        "port": porta,
        "username": usuario_email,
        "password": senha_email,
        "from_email": remetente,
        "use_ssl": usar_ssl,
        "use_tls": usar_tls and not usar_ssl,
        "suppress_send": suppress,
    }


def enviar_email_codigo_reset(destinatario, codigo):
    config = obter_config_email()
    if config["suppress_send"]:
        print(f"[password-reset] Código para {destinatario}: {codigo}")
        return
    if not config["host"] or not config["from_email"]:
        print(f"[password-reset] SMTP não configurado. Código gerado para {destinatario}: {codigo}")
        return

    mensagem = EmailMessage()
    mensagem["Subject"] = "Redefinição de senha"
    mensagem["From"] = config["from_email"]
    mensagem["To"] = destinatario
    mensagem.set_content(
        (
            "Você solicitou a redefinição da sua senha.\n\n"
            f"Use este código de 6 dígitos: {codigo}\n"
            f"Validade: {PASSWORD_RESET_TOKEN_TTL_MINUTES} minutos.\n\n"
            "Se você não fez essa solicitação, ignore este e-mail."
        )
    )

    contexto_ssl = ssl.create_default_context()
    if config["use_ssl"]:
        with smtplib.SMTP_SSL(config["host"], config["port"], context=contexto_ssl, timeout=15) as servidor:
            if config["username"]:
                servidor.login(config["username"], config["password"])
            servidor.send_message(mensagem)
        return

    with smtplib.SMTP(config["host"], config["port"], timeout=15) as servidor:
        servidor.ehlo()
        if config["use_tls"]:
            servidor.starttls(context=contexto_ssl)
            servidor.ehlo()
        if config["username"]:
            servidor.login(config["username"], config["password"])
        servidor.send_message(mensagem)


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
        datas_parcelas = item.get("installmentDates", item.get("datasParcelas", []))
        if not isinstance(datas_parcelas, list):
            datas_parcelas = []
        datas_parcelas = [str(datas_parcelas[i]).strip() if i < len(datas_parcelas) and datas_parcelas[i] else "" for i in range(parcelas)]
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
                "installmentDates": datas_parcelas,
            }
        )
    return itens_normalizados


def normalizar_evento_financeiro(evento):
    if not evento:
        return None
    if isinstance(evento, str):
        try:
            dados = json.loads(evento)
        except json.JSONDecodeError:
            return None
    else:
        dados = evento

    if not isinstance(dados, dict):
        return None

    produto = str(dados.get("product", dados.get("produto", "")) or "").strip()
    escopo = str(dados.get("scope", dados.get("escopo", "")) or "").strip()
    acao = str(dados.get("action", dados.get("acao", "")) or "").strip()

    try:
        valor = float(dados.get("value", dados.get("valor", 0)) or 0)
    except (TypeError, ValueError):
        valor = 0.0

    try:
        parcelas = max(1, int(dados.get("installments", dados.get("parcelas", 1)) or 1))
    except (TypeError, ValueError):
        parcelas = 1

    if not produto or not escopo:
        return None

    return {
        "action": acao or "create",
        "scope": escopo,
        "product": produto,
        "value": valor,
        "installments": parcelas,
    }


def atualizacao_financeira_placeholder(atualizacao):
    if not isinstance(atualizacao, dict):
        return False
    mensagem = str(atualizacao.get("mensagem", "") or "").strip()
    autor = str(atualizacao.get("autor", "") or "").strip()
    anexos = normalizar_anexos(atualizacao.get("anexos"))
    evento = normalizar_evento_financeiro(atualizacao.get("financeiro_evento"))
    return (
        autor == "Sistema"
        and mensagem == "Registro financeiro inicial."
        and not anexos
        and evento is None
    )


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
              AND COLUMN_NAME IN ('financeiro_cliente', 'financeiro_escritorio', 'financeiro_evento')
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
        if "financeiro_evento" not in colunas_existentes:
            cursor.execute(f"ALTER TABLE {tabela_atualizacoes} ADD COLUMN financeiro_evento LONGTEXT NULL")
        elif colunas_existentes["financeiro_evento"]["tipo"] != "longtext":
            cursor.execute(f"ALTER TABLE {tabela_atualizacoes} MODIFY COLUMN financeiro_evento LONGTEXT NULL")
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


def garantir_coluna_primeiro_acesso(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DATABASE()")
        banco_atual = cursor.fetchone()[0]
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = 'usuarios'
              AND COLUMN_NAME = 'primeiro_acesso'
            """,
            (banco_atual,),
        )
        if not cursor.fetchone():
            cursor.execute(
                """
                ALTER TABLE usuarios
                ADD COLUMN primeiro_acesso TINYINT(1) NOT NULL DEFAULT 0
                """
            )
    finally:
        cursor.close()


def garantir_coluna_email(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DATABASE()")
        banco_atual = cursor.fetchone()[0]
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = 'usuarios'
              AND COLUMN_NAME = 'email'
            """,
            (banco_atual,),
        )
        if not cursor.fetchone():
            cursor.execute(
                """
                ALTER TABLE usuarios
                ADD COLUMN email VARCHAR(255) NULL
                """
            )
    finally:
        cursor.close()


def garantir_tabela_reset_senha(conn):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                usuario_login VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                token_hash CHAR(64) NOT NULL,
                reset_session_hash CHAR(64) NULL,
                expires_at DATETIME NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                used_at DATETIME NULL,
                validation_failures INT NOT NULL DEFAULT 0,
                request_ip VARCHAR(64) NULL,
                INDEX idx_password_reset_email (email),
                INDEX idx_password_reset_usuario (usuario_login),
                INDEX idx_password_reset_expires (expires_at)
            )
            """
        )
    finally:
        cursor.close()


def preparar_tabela_usuarios(nome_banco):
    agora = datetime.now()
    cache = usuarios_cache.get(nome_banco)
    if cache and agora < cache:
        return

    def operacao(conn):
        garantir_coluna_primeiro_acesso(conn)
        garantir_coluna_email(conn)
        garantir_tabela_reset_senha(conn)

    _executar_com_retry(nome_banco, operacao)
    usuarios_cache[nome_banco] = agora + timedelta(minutes=10)


def _coluna_existe(conn, nome_tabela, nome_coluna):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT DATABASE()")
        banco_atual = cursor.fetchone()[0]
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            LIMIT 1
            """,
            (banco_atual, nome_tabela, nome_coluna),
        )
        return bool(cursor.fetchone())
    finally:
        cursor.close()


def preparar_tabela_chamados(nome_banco):
    def operacao(conn):
        cursor = conn.cursor()
        try:
            if not _coluna_existe(conn, "chamados", "criador_login"):
                cursor.execute("ALTER TABLE chamados ADD COLUMN criador_login VARCHAR(255) NULL")
            if not _coluna_existe(conn, "chamados", "parceiro_login"):
                cursor.execute("ALTER TABLE chamados ADD COLUMN parceiro_login VARCHAR(255) NULL")
            cursor.execute(
                """
                UPDATE chamados
                SET criador_login = login_cliente
                WHERE (criador_login IS NULL OR criador_login = '')
                  AND login_cliente IS NOT NULL
                  AND login_cliente <> ''
                """
            )
        finally:
            cursor.close()

    _executar_com_retry(nome_banco, operacao)


def obter_usuario_por_login(nome_banco, login):
    preparar_tabela_usuarios(nome_banco)
    login_normalizado = (login or "").strip().lower()
    if not login_normalizado:
        return None
    return executar_select(
        nome_banco,
        """
        SELECT usuario, tipo
        FROM usuarios
        WHERE LOWER(usuario) = %s
        LIMIT 1
        """,
        (login_normalizado,),
        fetch_one=True,
    )


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
        conn.commit()
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
    preparar_tabela_usuarios(nome_banco)
    registros = executar_select(
        nome_banco,
        """
        SELECT usuario, senha, nome_completo, telefone, documento, email
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
            "email": normalizar_email(r.get("email")),
            "login": r["usuario"],
        }
        for r in registros
    ]


def substituir_clientes(nome_banco, clientes):
    preparar_tabela_usuarios(nome_banco)
    def transacao(conn):
        cursor = conn.cursor()
        cursor.execute("DELETE FROM usuarios WHERE tipo = 'Cliente'")
        for cliente in clientes:
            cursor.execute(
                """
                INSERT INTO usuarios (usuario, senha, tipo, nome_completo, telefone, documento, email, primeiro_acesso)
                VALUES (%s, %s, 'Cliente', %s, %s, %s, %s, %s)
                """,
                (
                    cliente["login"],
                    hash_senha(cliente["senha"]),
                    cliente.get("nomeCompleto") or None,
                    cliente.get("telefone") or None,
                    cliente.get("documento") or None,
                    normalizar_email(cliente.get("email")) or None,
                    1,
                ),
            )
        cursor.close()

    executar_transacao(nome_banco, transacao)


def inserir_cliente(nome_banco, cliente):
    preparar_tabela_usuarios(nome_banco)
    tipo = cliente.get("tipo") or "Cliente"
    if tipo == "Técnico":
        tipo = "Advogado"
    if tipo not in {"Cliente", "Advogado", "Administrador"}:
        raise ValueError("Tipo de usuário inválido.")

    executar_write(
        nome_banco,
        """
        INSERT INTO usuarios (usuario, senha, tipo, nome_completo, telefone, documento, email, primeiro_acesso)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            cliente["login"],
            hash_senha(cliente["senha"]),
            tipo,
            cliente.get("nomeCompleto") or None,
            cliente.get("telefone") or None,
            cliente.get("documento") or None,
            normalizar_email(cliente.get("email")) or None,
            1 if tipo in {"Cliente", "Advogado"} else 0,
        ),
    )


def trocar_senha_primeiro_acesso(nome_banco, usuario, senha_atual, nova_senha):
    preparar_tabela_usuarios(nome_banco)
    registro = executar_select(
        nome_banco,
        """
        SELECT usuario, senha, tipo, primeiro_acesso
        FROM usuarios
        WHERE usuario = %s
        LIMIT 1
        """,
        (usuario,),
        fetch_one=True,
    )
    if not registro or not verificar_senha(senha_atual, registro["senha"]):
        raise ValueError("Credenciais inválidas.")

    tipo_registro = registro["tipo"]
    tipo = "Advogado" if tipo_registro in {"Técnico", "TÃ©cnico"} else tipo_registro
    if tipo not in {"Cliente", "Advogado"}:
        raise ValueError("Somente clientes e advogados podem usar esse fluxo.")
    if not int(registro.get("primeiro_acesso") or 0):
        raise ValueError("A troca obrigatória de senha já foi concluída.")
    if not nova_senha or nova_senha == senha_atual:
        raise ValueError("Informe uma nova senha diferente da atual.")

    executar_write(
        nome_banco,
        """
        UPDATE usuarios
        SET senha = %s,
            primeiro_acesso = 0
        WHERE usuario = %s
        """,
        (hash_senha(nova_senha), usuario),
    )

    return {
        "usuario": usuario,
        "tipo": tipo,
    }



def buscar_usuario_por_email(nome_banco, email):
    preparar_tabela_usuarios(nome_banco)
    return executar_select(
        nome_banco,
        """
        SELECT usuario, email, tipo
        FROM usuarios
        WHERE email IS NOT NULL
          AND LOWER(email) = %s
        LIMIT 1
        """,
        (normalizar_email(email),),
        fetch_one=True,
    )


def solicitar_reset_senha(nome_banco, email, request_ip):
    preparar_tabela_usuarios(nome_banco)
    email_normalizado = normalizar_email(email)
    if not email_normalizado:
        raise ValueError("Informe um e-mail válido.")

    usuario = buscar_usuario_por_email(nome_banco, email_normalizado)
    if not usuario:
        return {"ok": True, "mensagem": "código enviado"}

    codigo = gerar_codigo_reset()
    agora = datetime.now()
    expira_em = agora + timedelta(minutes=PASSWORD_RESET_TOKEN_TTL_MINUTES)
    hash_codigo = token_hash(codigo)

    def transacao(conn):
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE password_reset_tokens
            SET used_at = COALESCE(used_at, %s)
            WHERE usuario_login = %s
              AND used_at IS NULL
            """,
            (agora, usuario["usuario"]),
        )
        cursor.execute(
            """
            INSERT INTO password_reset_tokens (
                usuario_login,
                email,
                token_hash,
                reset_session_hash,
                expires_at,
                request_ip
            ) VALUES (%s, %s, %s, NULL, %s, %s)
            """,
            (
                usuario["usuario"],
                email_normalizado,
                hash_codigo,
                expira_em,
                request_ip or None,
            ),
        )
        cursor.close()

    executar_transacao(nome_banco, transacao)
    try:
        enviar_email_codigo_reset(email_normalizado, codigo)
    except Exception as erro:
        print(f"[password-reset] Falha ao enviar e-mail para {email_normalizado}: {erro}")

    return {"ok": True, "mensagem": "código enviado"}


def validar_codigo_reset_senha(nome_banco, email, codigo):
    preparar_tabela_usuarios(nome_banco)
    email_normalizado = normalizar_email(email)
    codigo_normalizado = re.sub(r"\D", "", codigo or "")
    if not email_normalizado or len(codigo_normalizado) != 6:
        raise ValueError("Código inválido ou expirado.")

    registro = executar_select(
        nome_banco,
        """
        SELECT id, usuario_login, expires_at, used_at, validation_failures, token_hash
        FROM password_reset_tokens
        WHERE email = %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (email_normalizado,),
        fetch_one=True,
    )
    agora = datetime.now()
    if not registro:
        raise ValueError("Código inválido ou expirado.")
    if registro["used_at"] is not None:
        raise ValueError("Código inválido ou expirado.")
    if registro["expires_at"] < agora:
        raise ValueError("Código inválido ou expirado.")
    if int(registro.get("validation_failures") or 0) >= PASSWORD_RESET_MAX_FAILED_ATTEMPTS:
        raise ValueError("Código inválido ou expirado.")

    if not hmac.compare_digest(registro["token_hash"], token_hash(codigo_normalizado)):
        executar_write(
            nome_banco,
            """
            UPDATE password_reset_tokens
            SET validation_failures = validation_failures + 1
            WHERE id = %s
            """,
            (registro["id"],),
        )
        raise ValueError("Código inválido ou expirado.")

    reset_token = gerar_token_sessao_reset()
    executar_write(
        nome_banco,
        """
        UPDATE password_reset_tokens
        SET used_at = %s,
            reset_session_hash = %s
        WHERE id = %s
        """,
        (agora, token_hash(reset_token), registro["id"]),
    )

    return {"resetToken": reset_token, "email": email_normalizado}


def redefinir_senha_com_token(nome_banco, email, reset_token, nova_senha):
    preparar_tabela_usuarios(nome_banco)
    email_normalizado = normalizar_email(email)
    if not email_normalizado or not reset_token:
        raise ValueError("Sessão de redefinição inválida.")
    if not nova_senha:
        raise ValueError("Informe a nova senha.")

    registro = executar_select(
        nome_banco,
        """
        SELECT id, usuario_login, expires_at, reset_session_hash
        FROM password_reset_tokens
        WHERE email = %s
          AND used_at IS NOT NULL
          AND reset_session_hash IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (email_normalizado,),
        fetch_one=True,
    )
    agora = datetime.now()
    if not registro or registro["expires_at"] < agora:
        raise ValueError("Sessão de redefinição inválida.")
    if not hmac.compare_digest(registro["reset_session_hash"], token_hash(reset_token)):
        raise ValueError("Sessão de redefinição inválida.")

    def transacao(conn):
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE usuarios
            SET senha = %s,
                primeiro_acesso = 0
            WHERE usuario = %s
            """,
            (hash_senha(nova_senha), registro["usuario_login"]),
        )
        cursor.execute(
            """
            UPDATE password_reset_tokens
            SET reset_session_hash = NULL
            WHERE id = %s
            """,
            (registro["id"],),
        )
        cursor.close()

    executar_transacao(nome_banco, transacao)
    return {"ok": True}


def listar_chamados(nome_banco, usuario_logado, limite=50, offset=0):
    preparar_tabela_chamados(nome_banco)
    chamados = executar_select(
        nome_banco,
        """
        SELECT id_chamado, cliente, login_cliente, resumo, prioridade, status, abertura, ultima_atualizacao,
               criador_login, parceiro_login
        FROM chamados
        WHERE LOWER(COALESCE(criador_login, '')) = %s
           OR LOWER(COALESCE(parceiro_login, '')) = %s
        ORDER BY ultima_atualizacao DESC, id_chamado DESC
        LIMIT %s OFFSET %s
        """,
        (usuario_logado.lower(), usuario_logado.lower(), limite, offset),
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
            "creatorLogin": c.get("criador_login") or "",
            "partnerLogin": c.get("parceiro_login") or "",
        }
        for c in chamados
    ]


def obter_chamado_detalhe(nome_banco, id_chamado, usuario_logado):
    preparar_tabela_chamados(nome_banco)
    tabela_atualizacoes = preparar_tabela_atualizacoes(nome_banco)
    chamado = executar_select(
        nome_banco,
        """
        SELECT id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
               numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao,
               criador_login, parceiro_login
        FROM chamados
        WHERE id_chamado = %s
          AND (
            LOWER(COALESCE(criador_login, '')) = %s
            OR LOWER(COALESCE(parceiro_login, '')) = %s
          )
        """,
        (id_chamado, usuario_logado.lower(), usuario_logado.lower()),
        fetch_one=True,
    )
    if not chamado:
        return None

    atualizacoes = executar_select(
        nome_banco,
        f"""
        SELECT autor, mensagem, data_atualizacao, anexos, financeiro_cliente, financeiro_escritorio, financeiro_evento
        FROM {tabela_atualizacoes}
        WHERE id_chamado = %s
        ORDER BY id DESC
        """,
        (id_chamado,),
    )
    financeiro_cliente = normalizar_financeiro(atualizacoes[0]["financeiro_cliente"]) if atualizacoes else []
    financeiro_escritorio = normalizar_financeiro(atualizacoes[0]["financeiro_escritorio"]) if atualizacoes else []
    atualizacoes_exibiveis = [atu for atu in atualizacoes if not atualizacao_financeira_placeholder(atu)]

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
        "creatorLogin": chamado.get("criador_login") or "",
        "partnerLogin": chamado.get("parceiro_login") or "",
        "financialClient": financeiro_cliente,
        "financialOffice": financeiro_escritorio,
        "updates": [
            {
                "author": atu["autor"],
                "message": atu["mensagem"],
                "date": atu["data_atualizacao"],
                "attachments": normalizar_anexos(atu["anexos"]),
                "financialEvent": normalizar_evento_financeiro(atu.get("financeiro_evento")),
            }
            for atu in atualizacoes_exibiveis
        ],
    }


def substituir_chamados(nome_banco, chamados, usuario_logado):
    preparar_tabela_chamados(nome_banco)
    def transacao(conn):
        cursor = conn.cursor()
        tabela_atualizacoes = preparar_tabela_atualizacoes_em_conexao(nome_banco, conn)
        cursor.execute(f"DELETE FROM {tabela_atualizacoes}")
        cursor.execute(
            """
            DELETE FROM chamados
            WHERE LOWER(COALESCE(criador_login, '')) = %s
               OR LOWER(COALESCE(parceiro_login, '')) = %s
            """,
            (usuario_logado.lower(), usuario_logado.lower()),
        )

        for chamado in chamados:
            parceiro_login = ""
            parceiro_informado = (chamado.get("partnershipWith") or "").strip()
            if chamado.get("hasPartnership") and parceiro_informado:
                parceiro = obter_usuario_por_login(nome_banco, parceiro_informado)
                if parceiro:
                    parceiro_login = parceiro["usuario"]
            financeiro_cliente = json.dumps(normalizar_financeiro(chamado.get("financialClient", [])), ensure_ascii=False)
            financeiro_escritorio = json.dumps(normalizar_financeiro(chamado.get("financialOffice", [])), ensure_ascii=False)
            cursor.execute(
                """
                INSERT INTO chamados (
                    id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
                    numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao,
                    criador_login, parceiro_login
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    1 if parceiro_login else 0,
                    chamado.get("partnershipPercent", ""),
                    parceiro_login,
                    chamado["openedAt"],
                    chamado["lastUpdate"],
                    usuario_logado,
                    parceiro_login or None,
                ),
            )

            for atualizacao in chamado.get("updates", []):
                financeiro_evento = json.dumps(normalizar_evento_financeiro(atualizacao.get("financialEvent")), ensure_ascii=False)
                cursor.execute(
                    f"""
                    INSERT INTO {tabela_atualizacoes} (
                        id_chamado, autor, mensagem, data_atualizacao, anexos, financeiro_cliente, financeiro_escritorio, financeiro_evento
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        chamado["id"],
                        atualizacao.get("author", "T?cnico"),
                        atualizacao.get("message", ""),
                        atualizacao.get("date", datetime.now().strftime("%d/%m/%Y %H:%M")),
                        json.dumps(atualizacao.get("attachments", []), ensure_ascii=False),
                        financeiro_cliente,
                        financeiro_escritorio,
                        financeiro_evento,
                    ),
                )

        cursor.close()

    executar_transacao(nome_banco, transacao)


def salvar_chamado_individual(nome_banco, chamado, usuario_logado):
    preparar_tabela_chamados(nome_banco)
    chamado_normalizado = dict(chamado or {})
    chamado_normalizado["financialClient"] = normalizar_financeiro(chamado_normalizado.get("financialClient", []))
    chamado_normalizado["financialOffice"] = normalizar_financeiro(chamado_normalizado.get("financialOffice", []))

    def transacao(conn):
        cursor = conn.cursor()
        tabela_atualizacoes = preparar_tabela_atualizacoes_em_conexao(nome_banco, conn)

        id_chamado = (chamado_normalizado.get("id") or "").strip()
        criador_login = usuario_logado
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
        else:
            cursor.execute(
                """
                SELECT criador_login, parceiro_login
                FROM chamados
                WHERE id_chamado = %s
                LIMIT 1
                """,
                (id_chamado,),
            )
            registro_existente = cursor.fetchone()
            if registro_existente:
                criador_existente = (registro_existente[0] or "").strip()
                parceiro_existente = (registro_existente[1] or "").strip().lower()
                if usuario_logado.lower() not in {criador_existente.lower(), parceiro_existente}:
                    raise ValueError("Você não tem permissão para alterar este chamado.")
                if criador_existente:
                    criador_login = criador_existente

        parceiro_login = ""
        parceiro_informado = (chamado_normalizado.get("partnershipWith") or "").strip()
        if chamado_normalizado.get("hasPartnership") and parceiro_informado:
            parceiro = obter_usuario_por_login(nome_banco, parceiro_informado)
            if parceiro:
                parceiro_login = parceiro["usuario"]

        cursor.execute(
            """
            INSERT INTO chamados (
                id_chamado, cliente, login_cliente, resumo, descricao, prioridade, status,
                numero_processo, parceria, parceria_porcentagem, parceria_com, abertura, ultima_atualizacao,
                criador_login, parceiro_login
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                ultima_atualizacao = VALUES(ultima_atualizacao),
                criador_login = VALUES(criador_login),
                parceiro_login = VALUES(parceiro_login)
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
                1 if parceiro_login else 0,
                chamado_normalizado.get("partnershipPercent", ""),
                parceiro_login,
                chamado_normalizado["openedAt"],
                chamado_normalizado["lastUpdate"],
                criador_login,
                parceiro_login or None,
            ),
        )

        cursor.execute(
            f"""
            SELECT autor, mensagem, data_atualizacao, anexos, financeiro_evento
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
                row[4] or "null",
            )
            for row in cursor.fetchall()
        }
        financeiro_cliente = json.dumps(chamado_normalizado.get("financialClient", []), ensure_ascii=False)
        financeiro_escritorio = json.dumps(chamado_normalizado.get("financialOffice", []), ensure_ascii=False)

        for atualizacao in chamado_normalizado.get("updates", []):
            anexos_serializados = json.dumps(atualizacao.get("attachments", []), ensure_ascii=False)
            financeiro_evento = json.dumps(normalizar_evento_financeiro(atualizacao.get("financialEvent")), ensure_ascii=False)
            assinatura = (
                atualizacao.get("author", "Técnico") or "",
                atualizacao.get("message", "") or "",
                atualizacao.get("date", datetime.now().strftime("%d/%m/%Y %H:%M")) or "",
                anexos_serializados,
                financeiro_evento,
            )
            if assinatura in existentes:
                continue
            cursor.execute(
                f"""
                INSERT INTO {tabela_atualizacoes} (
                    id_chamado, autor, mensagem, data_atualizacao, anexos, financeiro_cliente, financeiro_escritorio, financeiro_evento
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    chamado_normalizado["id"],
                    assinatura[0],
                    assinatura[1],
                    assinatura[2],
                    assinatura[3],
                    financeiro_cliente,
                    financeiro_escritorio,
                    assinatura[4],
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
                    id_chamado, autor, mensagem, data_atualizacao, anexos, financeiro_cliente, financeiro_escritorio, financeiro_evento
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    chamado_normalizado["id"],
                    "Sistema",
                    "Registro financeiro inicial.",
                    chamado_normalizado.get("lastUpdate", datetime.now().strftime("%d/%m/%Y %H:%M")),
                    "[]",
                    financeiro_cliente,
                    financeiro_escritorio,
                    json.dumps(None, ensure_ascii=False),
                ),
            )

        cursor.close()

    executar_transacao(nome_banco, transacao)
    return chamado_normalizado


def excluir_chamado(nome_banco, id_chamado, usuario_logado):
    preparar_tabela_chamados(nome_banco)
    executar_write(
        nome_banco,
        """
        DELETE FROM chamados
        WHERE id_chamado = %s
          AND (
            LOWER(COALESCE(criador_login, '')) = %s
            OR LOWER(COALESCE(parceiro_login, '')) = %s
          )
        """,
        (id_chamado, usuario_logado.lower(), usuario_logado.lower()),
    )


def autenticar_usuario(nome_banco, usuario, senha):
    preparar_tabela_usuarios(nome_banco)
    identificador = normalizar_email(usuario)
    registro = executar_select(
        nome_banco,
        """
        SELECT usuario, senha, tipo, primeiro_acesso, email
        FROM usuarios
        WHERE LOWER(usuario) = %s
           OR (email IS NOT NULL AND LOWER(email) = %s)
        LIMIT 1
        """,
        (identificador, identificador),
        fetch_one=True,
    )
    if not registro or not verificar_senha(senha, registro["senha"]):
        return None
    if not senha_usa_hash(registro["senha"]):
        executar_write(
            nome_banco,
            "UPDATE usuarios SET senha = %s WHERE usuario = %s",
            (hash_senha(senha), registro["usuario"]),
        )
        registro["senha"] = ""
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
        usuario_logado = obter_usuario_requisicao(nome_banco)
        limite = parse_int_param(request.args.get("limit"), padrao=50, minimo=1, maximo=200)
        offset = parse_int_param(request.args.get("offset"), padrao=0, minimo=0, maximo=1000000)
        chamados = await executar_em_thread(listar_chamados, nome_banco, usuario_logado, limite, offset)
        return responder_json(chamados)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["GET"])
async def api_chamado_detalhar(id_chamado):
    try:
        nome_banco = obter_banco_requisicao()
        usuario_logado = obter_usuario_requisicao(nome_banco)
        chamado = await executar_em_thread(obter_chamado_detalhe, nome_banco, id_chamado, usuario_logado)
        if not chamado:
            return responder_json({"ok": False, "erro": "Chamado não encontrado."}, 404)
        return responder_json(chamado)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["PUT"])
async def api_chamados_substituir():
    try:
        nome_banco = obter_banco_requisicao()
        usuario_logado = obter_usuario_requisicao(nome_banco)
        await executar_em_thread(substituir_chamados, nome_banco, request.json or [], usuario_logado)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados", methods=["POST"])
async def api_chamado_inserir():
    try:
        nome_banco = obter_banco_requisicao()
        usuario_logado = obter_usuario_requisicao(nome_banco)
        chamado_salvo = await executar_em_thread(salvar_chamado_individual, nome_banco, request.json or {}, usuario_logado)
        return responder_json({"ok": True, "chamado": chamado_salvo}, 201)
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["PUT"])
async def api_chamado_atualizar(id_chamado):
    try:
        nome_banco = obter_banco_requisicao()
        usuario_logado = obter_usuario_requisicao(nome_banco)
        chamado = request.json or {}
        if not chamado.get("id"):
            chamado["id"] = id_chamado
        await executar_em_thread(salvar_chamado_individual, nome_banco, chamado, usuario_logado)
        return responder_json({"ok": True})
    except (ValueError, RuntimeError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/chamados/<id_chamado>", methods=["DELETE"])
async def api_chamado_remover(id_chamado):
    try:
        nome_banco = obter_banco_requisicao()
        usuario_logado = obter_usuario_requisicao(nome_banco)
        await executar_em_thread(excluir_chamado, nome_banco, id_chamado, usuario_logado)
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

    tipo = "Advogado" if autenticado["tipo"] == "Técnico" else autenticado["tipo"]
    redirect = "admin.html" if tipo == "Administrador" else ("index.html" if tipo == "Advogado" else "cliente.html")
    cliente_id = autenticado["usuario"] if tipo == "Cliente" else ""
    return responder_json(
        {
            "ok": True,
            "usuario": autenticado["usuario"],
            "tipo": tipo,
            "clienteId": cliente_id,
            "redirect": redirect,
            "precisaTrocarSenha": bool(autenticado.get("primeiro_acesso")) and tipo in {"Cliente", "Advogado"},
            "banco": nome_banco,
        }
    )


@app.route("/api/usuarios/primeiro-acesso", methods=["POST"])
async def api_primeiro_acesso():
    dados = request.json or {}
    usuario = (dados.get("usuario") or "").strip()
    senha_atual = (dados.get("senhaAtual") or "").strip()
    nova_senha = (dados.get("novaSenha") or "").strip()

    try:
        nome_banco = dados.get("banco") or "teste"
        valido = await executar_em_thread(nome_banco_valido, nome_banco)
        if not valido:
            raise ValueError("Nome de banco inválido.")
        resultado = await executar_em_thread(
            trocar_senha_primeiro_acesso,
            nome_banco,
            usuario,
            senha_atual,
            nova_senha,
        )
        tipo = resultado["tipo"]
        return responder_json(
            {
                "ok": True,
                "usuario": usuario,
                "tipo": tipo,
                "clienteId": usuario if tipo == "Cliente" else "",
                "redirect": "index.html" if tipo == "Advogado" else "cliente.html",
                "banco": nome_banco,
            }
        )
    except (ValueError, RuntimeError, MySQLdb.MySQLError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/usuarios/esqueci-senha/solicitar", methods=["POST"])
async def api_esqueci_senha_solicitar():
    dados = request.json or {}
    email = normalizar_email(dados.get("email"))

    try:
        nome_banco = dados.get("banco") or obter_banco_requisicao()
        valido = await executar_em_thread(nome_banco_valido, nome_banco)
        if not valido:
            raise ValueError("Nome de banco inválido.")
        limitar_solicitacao_reset(email)
        resposta = await executar_em_thread(solicitar_reset_senha, nome_banco, email, obter_ip_requisicao())
        resposta["banco"] = nome_banco
        return responder_json(resposta)
    except ValueError as erro:
        status = 429 if "Muitas tentativas" in str(erro) else 400
        return responder_json({"ok": False, "erro": str(erro)}, status)
    except (RuntimeError, MySQLdb.MySQLError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/usuarios/esqueci-senha/validar", methods=["POST"])
async def api_esqueci_senha_validar():
    dados = request.json or {}
    email = normalizar_email(dados.get("email"))
    codigo = (dados.get("codigo") or "").strip()

    try:
        nome_banco = dados.get("banco") or obter_banco_requisicao()
        valido = await executar_em_thread(nome_banco_valido, nome_banco)
        if not valido:
            raise ValueError("Nome de banco inválido.")
        limitar_validacao_reset(email)
        resposta = await executar_em_thread(validar_codigo_reset_senha, nome_banco, email, codigo)
        resposta["ok"] = True
        resposta["banco"] = nome_banco
        return responder_json(resposta)
    except ValueError as erro:
        status = 429 if "Muitas tentativas" in str(erro) else 400
        return responder_json({"ok": False, "erro": str(erro)}, status)
    except (RuntimeError, MySQLdb.MySQLError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/usuarios/esqueci-senha/redefinir", methods=["POST"])
async def api_esqueci_senha_redefinir():
    dados = request.json or {}
    email = normalizar_email(dados.get("email"))
    reset_token = (dados.get("resetToken") or "").strip()
    nova_senha = (dados.get("novaSenha") or "").strip()

    try:
        nome_banco = dados.get("banco") or obter_banco_requisicao()
        valido = await executar_em_thread(nome_banco_valido, nome_banco)
        if not valido:
            raise ValueError("Nome de banco inválido.")
        await executar_em_thread(redefinir_senha_com_token, nome_banco, email, reset_token, nova_senha)
        return responder_json(
            {
                "ok": True,
                "mensagem": "Senha redefinida com sucesso.",
                "banco": nome_banco,
            }
        )
    except (ValueError, RuntimeError, MySQLdb.MySQLError) as erro:
        return responder_json({"ok": False, "erro": str(erro)}, 400)


@app.route("/api/health", methods=["GET"])
def healthcheck():
    return responder_json({"ok": True, "status": "healthy"})

if __name__ == "__main__":
    porta = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=porta)
