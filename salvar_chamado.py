import MySQLdb

host     = "ballast.proxy.rlwy.net"
user     = "root"
password = "cUxQKiTNIHZUlBQhphYhiESVTcrCJTGO"
db       = "teste"
port     =  15192

conn = MySQLdb.connect(host, user, password, db, port)
cursor = conn.cursor()


def salvar_atualizacao(atualizacao):
    cursor.execute("INSERT INTO chamados (atualizacao) VALUES (%s)", (atualizacao,))
    conn.commit()


if __name__ == "__main__":
    descricao = "Atualizacao de exemplo"
    salvar_atualizacao(descricao)
    cursor.close()
    conn.close()
