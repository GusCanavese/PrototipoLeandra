# Guia rápido: medir performance MySQL

## 1) Rodar API com logs estruturados

```bash
LOG_LEVEL=INFO DB_POOL_SIZE=10 DB_CONNECT_TIMEOUT=5 DB_READ_TIMEOUT=10 DB_WRITE_TIMEOUT=10 python salvar_chamado.py
```

Cada query registrada inclui:
- `route`
- `function`
- `query_id`
- `connection_ms`
- `execution_ms`
- `fetch_ms`
- `total_ms`
- `rows`

## 2) Exercitar endpoints

```bash
curl -s -H 'X-Project-DB: teste' http://127.0.0.1:5000/api/chamados > /dev/null
curl -s -H 'X-Project-DB: teste' http://127.0.0.1:5000/api/clientes > /dev/null
curl -s -H 'Content-Type: application/json' -d '{"usuario":"tecnico","senha":"tecnico123","banco":"teste"}' http://127.0.0.1:5000/api/login > /dev/null
```

## 3) Top queries mais lentas no log

```bash
python - <<'PY'
import json
from collections import defaultdict

stats = defaultdict(list)
with open('app.log', 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line.startswith('{'):
            continue
        try:
            d = json.loads(line)
        except json.JSONDecodeError:
            continue
        qid = d.get('query_id')
        if qid:
            stats[qid].append(d.get('total_ms', 0))

ranking = sorted(((k, sum(v)/len(v), max(v), len(v)) for k, v in stats.items()), key=lambda x: x[1], reverse=True)
for qid, media, pico, n in ranking[:5]:
    print(f"{qid:35} avg={media:8.2f}ms max={pico:8.2f}ms n={n}")
PY
```

> Dica: redirecione stdout da API para `app.log` (`python salvar_chamado.py > app.log 2>&1`).

## 4) Validar índices com EXPLAIN

```sql
EXPLAIN SELECT usuario, senha, nome_completo, telefone, documento
FROM usuarios WHERE tipo = 'Cliente' ORDER BY usuario;

EXPLAIN SELECT id_chamado, autor, mensagem, data_atualizacao, anexos
FROM chamado_atualizacoes
WHERE id_chamado IN ('CH-1', 'CH-2')
ORDER BY id DESC;
```

Esperado: evitar `type=ALL` nas consultas críticas.
