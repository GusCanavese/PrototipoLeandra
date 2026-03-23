# PrototipoLeandra

Aplicação Flask que expõe a API em `/api` e serve as páginas HTML pelo mesmo serviço web.
O projeto está preparado para deploy via GitHub no Railway com Railpack + Gunicorn.

## O que o Railway vai usar automaticamente

Ao conectar este repositório no Railway:

- o `railway.json` informa que o builder é `RAILPACK`;
- o comando de start é `gunicorn --bind 0.0.0.0:${PORT:-5000} wsgi:app`;
- o healthcheck é feito em `/api/health`;
- o `requirements.txt` define as dependências Python;
- o `runtime.txt` fixa a versão do Python.

## Variáveis de ambiente

Defina no Railway:

- `MYSQLHOST`
- `MYSQLPORT`
- `MYSQLDATABASE`
- `MYSQLUSER`
- `MYSQLPASSWORD`
- `PORT` (normalmente o Railway injeta automaticamente)

Use `.env.example` como referência.

## Fluxo de deploy

1. Suba este repositório no GitHub.
2. No Railway, crie um novo projeto a partir do repositório.
3. Configure as variáveis de ambiente do MySQL.
4. Faça o deploy.
5. O Railway deve construir a imagem automaticamente e subir o app com o comando definido em `railway.json`.

## Teste local

```bash
python salvar_chamado.py
```

Ou com Gunicorn:

```bash
gunicorn --bind 0.0.0.0:5000 wsgi:app
```
