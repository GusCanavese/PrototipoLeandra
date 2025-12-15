const chamados = [
    {
        id: 'CH-1024',
        client: 'Clínica Horizonte',
        summary: 'Falha no acesso ao prontuário eletrônico',
        lastUpdate: '10/06/2024 14:35',
        openedAt: '08/06/2024',
        priority: 'Alta',
        status: 'Em andamento',
        updates: [
            { author: 'Técnico', message: 'Analisando logs do servidor.', date: '09/06/2024 09:10', attachments: [] },
            { author: 'Cliente', message: 'Erro ocorre ao salvar novos pacientes.', date: '09/06/2024 11:22', attachments: [] },
            { author: 'Técnico', message: 'Reaplicado patch e monitorando.', date: '10/06/2024 14:35', attachments: ['log-servidor.txt'] }
        ]
    },
    {
        id: 'CH-2048',
        client: 'Tech Labs',
        summary: 'Lentidão na VPN corporativa',
        lastUpdate: '11/06/2024 10:05',
        openedAt: '07/06/2024',
        priority: 'Média',
        status: 'Aberto',
        updates: [
            { author: 'Cliente', message: 'Equipe remota não consegue conectar.', date: '08/06/2024 18:50', attachments: [] },
            { author: 'Técnico', message: 'Reiniciada instância de VPN.', date: '11/06/2024 10:05', attachments: [] }
        ]
    },
    {
        id: 'CH-4096',
        client: 'AgroVale',
        summary: 'Integração com fornecedor falhando',
        lastUpdate: '05/06/2024 16:20',
        openedAt: '03/06/2024',
        priority: 'Baixa',
        status: 'Concluído',
        updates: [
            { author: 'Cliente', message: 'Retorno 500 ao enviar pedidos.', date: '03/06/2024 09:12', attachments: [] },
            { author: 'Técnico', message: 'Corrigida chave de API e validado.', date: '05/06/2024 16:20', attachments: ['captura-erro.png'] }
        ]
    },
    {
        id: 'CH-8192',
        client: 'Universidade Nova',
        summary: 'Portal do aluno indisponível',
        lastUpdate: '11/06/2024 08:42',
        openedAt: '10/06/2024',
        priority: 'Alta',
        status: 'Aberto',
        updates: [
            { author: 'Cliente', message: 'Erro 503 em horários de pico.', date: '10/06/2024 10:00', attachments: [] },
            { author: 'Técnico', message: 'Ajustada capacidade de instâncias.', date: '11/06/2024 08:42', attachments: [] }
        ]
    }
];

const filtros = {
    client: '',
    summary: '',
    lastUpdate: '',
    openedAt: '',
    priority: '',
    status: ''
};

function limparFiltros(realizarRenderizacao = true) {
    Object.keys(filtros).forEach((chave) => (filtros[chave] = ''));

    document.querySelectorAll('.filter-input').forEach((campo) => {
        campo.value = '';
    });

    const filtroPrioridade = document.querySelector('.filter-select');
    if (filtroPrioridade) filtroPrioridade.value = '';

    const botoesStatus = document.querySelectorAll('[data-filter="status"]');
    botoesStatus.forEach((botao) => {
        botao.classList.toggle('active', botao.dataset.value === '');
    });

    if (realizarRenderizacao) {
        renderChamadosTabela();
    }
}

function createPriorityBadge(priority) {
    const badge = document.createElement('span');
    badge.classList.add('badge', 'rounded-pill', 'priority-badge');

    switch (priority) {
        case 'Alta':
            badge.classList.add('priority-alta');
            break;
        case 'Média':
            badge.classList.add('priority-media');
            break;
        default:
            badge.classList.add('priority-baixa');
            break;
    }

    badge.textContent = priority;
    return badge;
}

function renderChamadosTabela() {
    const corpoTabela = document.getElementById('lista-chamados');
    if (!corpoTabela) return;

    corpoTabela.innerHTML = '';

    const chamadosFiltrados = chamados.filter((chamado) => {
        const atendeCliente = chamado.client.toLowerCase().includes(filtros.client);
        const atendeResumo = chamado.summary.toLowerCase().includes(filtros.summary);
        const atendeUltima = chamado.lastUpdate.toLowerCase().includes(filtros.lastUpdate);
        const atendeAbertura = chamado.openedAt.toLowerCase().includes(filtros.openedAt);
        const atendePrioridade = filtros.priority ? chamado.priority === filtros.priority : true;
        const atendeStatus = filtros.status ? chamado.status === filtros.status : true;

        return atendCliente && atendeResumo && atendeUltima && atendeAbertura && atendePrioridade && atendeStatus;
    });

    chamadosFiltrados.forEach((chamado) => {
        const linha = document.createElement('tr');

        linha.innerHTML = `
            <td>
                <div class="fw-semibold">${chamado.client}</div>
                <div class="text-muted small">${chamado.id}</div>
            </td>
            <td>${chamado.summary}</td>
            <td>${chamado.lastUpdate}</td>
            <td>${chamado.openedAt}</td>
            <td class="text-center"></td>
            <td class="text-end">
                <a class="btn btn-sm btn-primary" href="details.html?id=${encodeURIComponent(chamado.id)}">Ver mais detalhes</a>
            </td>
        `;

        const prioridadeCelula = linha.querySelector('td:nth-child(5)');
        prioridadeCelula.appendChild(createPriorityBadge(chamado.priority));

        corpoTabela.appendChild(linha);
    });
}

function renderChamadosAbertos() {
    const grid = document.getElementById('grid-chamados-abertos');
    if (!grid) return;

    grid.innerHTML = '';

    chamados
        .filter((chamado) => chamado.status === 'Aberto')
        .forEach((chamado) => {
            const coluna = document.createElement('div');
            coluna.className = 'col-12 col-md-6 col-xl-4';

            coluna.innerHTML = `
                <div class="card ticket-card h-100">
                    <div class="card-body d-flex flex-column">
                        <div class="d-flex justify-content-between align-items-start mb-2">
                            <div>
                                <h3 class="h6 mb-1">${chamado.client}</h3>
                                <p class="text-muted small mb-0">${chamado.id}</p>
                            </div>
                            ${createPriorityBadge(chamado.priority).outerHTML}
                        </div>
                        <p class="mb-2">${chamado.summary}</p>
                        <div class="mt-auto d-flex justify-content-between align-items-center">
                            <div class="text-muted small">
                                <div>Última atualização</div>
                                <strong>${chamado.lastUpdate}</strong>
                            </div>
                            <a class="btn btn-outline-primary btn-sm" href="details.html?id=${encodeURIComponent(chamado.id)}">Detalhes</a>
                        </div>
                    </div>
                </div>
            `;

            grid.appendChild(coluna);
        });
}

function registrarFiltros() {
    const camposTexto = document.querySelectorAll('.filter-input');
    camposTexto.forEach((campo) => {
        campo.addEventListener('input', (evento) => {
            const coluna = evento.target.dataset.column;
            filtros[coluna] = evento.target.value.toLowerCase();
            renderChamadosTabela();
        });
    });

    const selectPrioridade = document.querySelector('.filter-select');
    if (selectPrioridade) {
        selectPrioridade.addEventListener('change', (evento) => {
            filtros.priority = evento.target.value;
            renderChamadosTabela();
        });
    }

    const botoesStatus = document.querySelectorAll('[data-filter="status"]');
    botoesStatus.forEach((botao) => {
        botao.addEventListener('click', (evento) => {
            filtros.status = evento.target.dataset.value;
            botoesStatus.forEach((outroBotao) => outroBotao.classList.toggle('active', outroBotao === botao));
            renderChamadosTabela();
        });
    });

    const botaoBuscar = document.getElementById('btn-buscar-chamados');
    if (botaoBuscar) {
        botaoBuscar.addEventListener('click', () => {
            renderChamadosTabela();
        });
    }

    const botaoLimpar = document.getElementById('btn-limpar-filtros');
    if (botaoLimpar) {
        botaoLimpar.addEventListener('click', () => {
            limparFiltros();
        });
    }
}

function obterChamadoPorId(id) {
    return chamados.find((chamado) => chamado.id === id);
}

function preencherCabecalhoChamado(chamado) {
    const cabecalho = document.getElementById('cabecalho-chamado');
    const badgeStatus = document.getElementById('badge-status');
    if (!cabecalho || !badgeStatus) return;

    const prioridadeBadge = createPriorityBadge(chamado.priority);

    cabecalho.innerHTML = `
        <div class="d-flex justify-content-between align-items-start flex-wrap gap-3">
            <div>
                <p class="text-muted mb-1">${chamado.id}</p>
                <h2 class="h5 mb-1">${chamado.client}</h2>
                <p class="mb-0">${chamado.summary}</p>
            </div>
            <div class="d-flex flex-column align-items-end gap-2">
                <div class="d-flex gap-2 align-items-center">
                    <span class="badge bg-secondary badge-role">Última atualização: ${chamado.lastUpdate}</span>
                    ${prioridadeBadge.outerHTML}
                </div>
                <span class="badge bg-light text-dark border">Aberto em ${chamado.openedAt}</span>
            </div>
        </div>
    `;

    badgeStatus.textContent = chamado.status;
}

function preencherHistorico(chamado) {
    const listaHistorico = document.getElementById('lista-historico');
    if (!listaHistorico) return;

    listaHistorico.innerHTML = '';

    chamado.updates.forEach((atualizacao) => {
        const item = document.createElement('div');
        item.className = 'timeline-item';
        item.innerHTML = `
            <div class="d-flex justify-content-between align-items-center mb-1">
                <strong>${atualizacao.author}</strong>
                <span class="text-muted small">${atualizacao.date}</span>
            </div>
            <p class="mb-1">${atualizacao.message}</p>
            ${atualizacao.attachments && atualizacao.attachments.length ? `<div class="small text-muted">Anexos: ${atualizacao.attachments.join(', ')}</div>` : ''}
        `;
        listaHistorico.appendChild(item);
    });
}

function preencherAnexos(chamado) {
    const listaAnexos = document.getElementById('lista-anexos');
    if (!listaAnexos) return;

    listaAnexos.innerHTML = '';

    const anexos = chamado.updates.flatMap((item) => item.attachments || []);

    if (!anexos.length) {
        listaAnexos.innerHTML = '<li class="list-group-item">Nenhum anexo registrado.</li>';
        return;
    }

    anexos.forEach((anexo) => {
        const item = document.createElement('li');
        item.className = 'list-group-item d-flex justify-content-between align-items-center';
        item.innerHTML = `<span>${anexo}</span><span class="text-muted small">Arquivo</span>`;
        listaAnexos.appendChild(item);
    });
}

function registrarFormularioAtualizacao(chamado) {
    const formulario = document.getElementById('form-atualizacao');
    if (!formulario) return;

    formulario.addEventListener('submit', (evento) => {
        evento.preventDefault();

        const autor = formulario.querySelector('input[name="tipoAutor"]:checked').value;
        const descricao = document.getElementById('descricaoAtualizacao').value.trim();
        const prioridade = document.getElementById('prioridadeAtualizacao').value;
        const anexoArquivo = document.getElementById('anexoAtualizacao');
        const nomeAnexo = anexoArquivo.files[0]?.name;

        if (!descricao) return;

        const novaAtualizacao = {
            author: autor,
            message: descricao,
            date: new Date().toLocaleString('pt-BR'),
            attachments: nomeAnexo ? [nomeAnexo] : []
        };

        chamado.updates.unshift(novaAtualizacao);
        chamado.priority = prioridade;
        chamado.lastUpdate = novaAtualizacao.date;

        preencherCabecalhoChamado(chamado);
        preencherHistorico(chamado);
        preencherAnexos(chamado);

        formulario.reset();
        document.getElementById('autorTecnico').checked = true;
    });
}

function carregarDetalhesChamado() {
    const containerDetalhes = document.getElementById('detalhes-chamado');
    if (!containerDetalhes) return;

    const parametros = new URLSearchParams(window.location.search);
    const idChamado = parametros.get('id') || chamados[0].id;
    const chamado = obterChamadoPorId(idChamado);

    if (!chamado) {
        containerDetalhes.innerHTML = '<div class="alert alert-warning">Chamado não encontrado.</div>';
        return;
    }

    preencherCabecalhoChamado(chamado);
    preencherHistorico(chamado);
    preencherAnexos(chamado);
    registrarFormularioAtualizacao(chamado);
}

function inicializar() {
    renderChamadosAbertos();
    registrarFiltros();
    limparFiltros();
    carregarDetalhesChamado();
}

document.addEventListener('DOMContentLoaded', inicializar);
