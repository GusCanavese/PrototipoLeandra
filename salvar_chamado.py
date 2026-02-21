import MySQLdb

host = "ballast.proxy.rlwy.net"
user = "root"
password = "cUxQKiTNIHZUlBQhphYhiESVTcrCJTGO"
db = "teste"
port = 15192


def abrir_conexao():
    return MySQLdb.connect(host, user, password, db, port)


def salvar_chamado(chamado):
    conn = abrir_conexao()
    cursor = conn.cursor()

    sql = """
    INSERT INTO chamados (
      id_chamado,
      cliente,
      login_cliente,
      resumo,
      descricao,
      prioridade,
      status,
      numero_processo,
      parceria,
      parceria_porcentagem,
      parceria_com,
      abertura,
      ultima_atualizacao
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    valores = (
        chamado["id"],
        chamado["client"],
        chamado["clienteLogin"],
        chamado["summary"],
        chamado["description"],
        chamado["priority"],
        chamado["status"],
        chamado["processNumber"],
        1 if chamado["hasPartnership"] else 0,
        chamado["partnershipPercent"],
        chamado["partnershipWith"],
        chamado["openedAt"],
        chamado["lastUpdate"],
    )

    cursor.execute(sql, valores)
    conn.commit()
    cursor.close()
    conn.close()


def salvar_atualizacao(id_chamado, autor, mensagem, data, anexos=""):
    conn = abrir_conexao()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO chamado_atualizacoes (id_chamado, autor, mensagem, data_atualizacao, anexos)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (id_chamado, autor, mensagem, data, anexos),
    )
    cursor.execute(
        "UPDATE chamados SET ultima_atualizacao = %s WHERE id_chamado = %s",
        (data, id_chamado),
    )
    conn.commit()
    cursor.close()
    conn.close()


def excluir_chamado(id_chamado):
    conn = abrir_conexao()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM chamado_atualizacoes WHERE id_chamado = %s", (id_chamado,))
    cursor.execute("DELETE FROM chamados WHERE id_chamado = %s", (id_chamado,))
    conn.commit()
    cursor.close()
    conn.close()
