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
- `SMTP_HOST` (ex.: `smtp.gmail.com`)
- `SMTP_PORT` (ex.: `587`)
- `SMTP_USER` (**coloque aqui o e-mail remetente**)
- `SMTP_PASSWORD` (**coloque aqui a App Password**)
- `SMTP_REMETENTE` (**coloque aqui o e-mail remetente que aparecerá no envio**)
- `APP_BASE_URL` (URL pública da aplicação para montar o link de redefinição)

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

## Recuperação de senha por e-mail

O fluxo de redefinição de senha usa `smtplib` do Python e cria links com token opaco (sem expor o usuário na URL).

Preencha claramente estas variáveis:

```env
SMTP_USER=seuemail@gmail.com          # coloque aqui o e-mail remetente
SMTP_PASSWORD=sua_app_password_aqui   # coloque aqui a App Password
SMTP_REMETENTE=seuemail@gmail.com     # normalmente o mesmo e-mail remetente
APP_BASE_URL=https://seu-dominio.com  # URL usada para gerar o link do e-mail
```

O backend envia o link para `reset-password.html?token=...`, valida se o e-mail já pertence a um usuário cadastrado e só então permite a troca de senha.
