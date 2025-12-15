const chamados = [
    {
        id: 'CH-1024',
        client: 'Clínica Horizonte',
        summary: 'Falha no acesso ao prontuário eletrônico',
        lastUpdate: '10/06/2024 14:35',
        openedAt: '08/06/2024',
        priority: 'Alta',
        status: 'Em andamento',
        clienteLogin: 'cliente',
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
        clienteLogin: 'cliente',
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
        clienteLogin: 'agrovale',
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
        clienteLogin: 'universidade',
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

const credenciaisLogin = {
    tecnico: { senha: 'tecnico123', tipo: 'Técnico', redirect: 'index.html' },
    cliente: { senha: 'cliente123', tipo: 'Cliente', redirect: 'cliente.html', clienteId: 'cliente' }
};

const CHAVE_STORAGE_LOGIN = 'usuarioAutenticado';
let usuarioAutenticado = null;

function obterUsuarioSalvo() {
    try {
        const dados = localStorage.getItem(CHAVE_STORAGE_LOGIN);
        return dados ? JSON.parse(dados) : null;
    } catch (erro) {
        console.error('Erro ao ler usuário salvo', erro);
        return null;
    }
}

function salvarUsuarioAutenticado(usuario) {
    usuarioAutenticado = usuario;
    localStorage.setItem(CHAVE_STORAGE_LOGIN, JSON.stringify(usuario));
}

function limparAutenticacao() {
    usuarioAutenticado = null;
    localStorage.removeItem(CHAVE_STORAGE_LOGIN);
}

function definirUsuarioAutenticadoSeSalvo() {
    if (!usuarioAutenticado) {
        usuarioAutenticado = obterUsuarioSalvo();
    }
}

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

function obterChamadosDoCliente(clienteId) {
    return chamados.filter((chamado) => chamado.clienteLogin === clienteId && chamado.status === 'Aberto');
}

function obterNomeClientePorLogin(clienteLogin) {
    const chamado = chamados.find((item) => item.clienteLogin === clienteLogin);
    return chamado?.client || 'Cliente autenticado';
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

function renderChamadosClienteAbertos() {
    const lista = document.getElementById('lista-chamados-cliente');
    if (!lista) return;

    lista.innerHTML = '';

    const chamadosCliente = usuarioAutenticado?.clienteId ? obterChamadosDoCliente(usuarioAutenticado.clienteId) : [];

    if (!chamadosCliente.length) {
        lista.innerHTML = `
            <div class="alert alert-info mb-0">Nenhum chamado em aberto foi encontrado para o seu usuário.</div>
        `;
        return;
    }

    chamadosCliente.forEach((chamado) => {
        const item = document.createElement('div');
        item.className = 'col-12 col-md-6';
        item.innerHTML = `
            <div class="card h-100 shadow-sm">
                <div class="card-body d-flex flex-column">
                    <div class="d-flex justify-content-between align-items-start mb-2">
                        <div>
                            <p class="text-muted small mb-0">${chamado.id}</p>
                            <h3 class="h6 mb-1">${chamado.summary}</h3>
                            <p class="mb-0">${chamado.client}</p>
                        </div>
                        ${createPriorityBadge(chamado.priority).outerHTML}
                    </div>
                    <p class="text-muted small mb-3">Última atualização: ${chamado.lastUpdate}</p>
                    <div class="mt-auto d-flex justify-content-between align-items-center">
                        <span class="badge bg-success">${chamado.status}</span>
                        <a class="btn btn-primary btn-sm" href="details.html?id=${encodeURIComponent(chamado.id)}">Abrir chamado</a>
                    </div>
                </div>
            </div>
        `;

        lista.appendChild(item);
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

function bloquearAtualizacaoPorPermissao(mensagem) {
    const containerDetalhes = document.getElementById('detalhes-chamado');
    const cartaoFormulario = document.getElementById('cartao-atualizacao');

    if (cartaoFormulario) {
        cartaoFormulario.classList.add('d-none');
    }

    if (containerDetalhes && mensagem) {
        const aviso = document.createElement('div');
        aviso.className = 'alert alert-warning';
        aviso.textContent = mensagem;
        containerDetalhes.prepend(aviso);
    }
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

function redirecionarParaLogin() {
    const destino = encodeURIComponent(`${window.location.pathname}${window.location.search}`);
    window.location.href = `login.html?redirect=${destino}`;
}

function obterDestinoPosLogin(destinoParam, credencial) {
    const destinoPreferencial = destinoParam ? decodeURIComponent(destinoParam) : credencial?.redirect || 'index.html';

    if (credencial?.tipo === 'Cliente' && destinoPreferencial.includes('index.html')) {
        return 'cliente.html';
    }

    if (credencial?.tipo === 'Técnico' && destinoPreferencial.includes('cliente.html')) {
        return 'index.html';
    }

    return destinoPreferencial;
}

function atualizarPainelIdentificacao() {
    const textoIdentificacao = document.getElementById('texto-identificacao');
    const badgeIdentificacao = document.getElementById('badge-identificacao');
    const botaoTrocar = document.getElementById('btn-trocar-usuario');

    if (!textoIdentificacao || !badgeIdentificacao) return;

    if (!usuarioAutenticado) {
        textoIdentificacao.textContent = 'Nenhum usuário autenticado.';
        badgeIdentificacao.textContent = '-';
        if (botaoTrocar) {
            botaoTrocar.href = 'login.html';
        }
        return;
    }

    textoIdentificacao.textContent = `Atualizações serão registradas como ${usuarioAutenticado.tipo}.`;
    badgeIdentificacao.textContent = usuarioAutenticado.tipo;

    if (botaoTrocar) {
        botaoTrocar.addEventListener('click', (evento) => {
            evento.preventDefault();
            limparAutenticacao();
            redirecionarParaLogin();
        });
    }
}

function configurarTelaLogin() {
    const formularioLogin = document.getElementById('form-login');
    if (!formularioLogin) return;

    const alertaLogin = document.getElementById('alerta-login');
    const parametros = new URLSearchParams(window.location.search);
    const destino = parametros.get('redirect');

    if (usuarioAutenticado) {
        const credencial = credenciaisLogin[usuarioAutenticado.usuario];
        window.location.href = obterDestinoPosLogin(destino, credencial);
        return;
    }

    formularioLogin.addEventListener('submit', (evento) => {
        evento.preventDefault();

        const usuario = document.getElementById('campo-usuario').value.trim().toLowerCase();
        const senha = document.getElementById('campo-senha').value.trim();
        const credencial = credenciaisLogin[usuario];

        if (credencial && credencial.senha === senha) {
            salvarUsuarioAutenticado({ usuario, tipo: credencial.tipo, clienteId: credencial.clienteId });
            if (alertaLogin) {
                alertaLogin.className = 'alert alert-success';
                alertaLogin.textContent = 'Login realizado com sucesso! Redirecionando...';
            }
            setTimeout(() => {
                window.location.href = obterDestinoPosLogin(destino, credencial);
            }, 400);
            formularioLogin.reset();
            return;
        }

        if (alertaLogin) {
            alertaLogin.className = 'alert alert-danger';
            alertaLogin.textContent = 'Credenciais inválidas. Utilize tecnico/tecnico123 ou cliente/cliente123.';
        }
    });
}

function registrarFormularioAtualizacao(chamado) {
    const formulario = document.getElementById('form-atualizacao');
    if (!formulario) return;

    const campoPrioridade = document.getElementById('prioridadeAtualizacao');
    const containerPrioridade = document.getElementById('container-prioridade');

    if (usuarioAutenticado?.tipo === 'Cliente') {
        if (containerPrioridade) containerPrioridade.classList.add('d-none');
        if (campoPrioridade) campoPrioridade.value = chamado.priority;
    } else if (campoPrioridade) {
        campoPrioridade.value = chamado.priority;
    }

    formulario.addEventListener('submit', (evento) => {
        evento.preventDefault();

        if (!usuarioAutenticado) return;
        const descricao = document.getElementById('descricaoAtualizacao').value.trim();
        const prioridadeSelecionada =
            usuarioAutenticado?.tipo === 'Cliente'
                ? chamado.priority
                : document.getElementById('prioridadeAtualizacao').value;
        const anexoArquivo = document.getElementById('anexoAtualizacao');
        const nomeAnexo = anexoArquivo.files[0]?.name;

        if (!descricao) return;

        const novaAtualizacao = {
            author: usuarioAutenticado.tipo,
            message: descricao,
            date: new Date().toLocaleString('pt-BR'),
            attachments: nomeAnexo ? [nomeAnexo] : []
        };

        chamado.updates.unshift(novaAtualizacao);
        chamado.priority = prioridadeSelecionada;
        chamado.lastUpdate = novaAtualizacao.date;

        preencherCabecalhoChamado(chamado);
        preencherHistorico(chamado);
        preencherAnexos(chamado);

        formulario.reset();
    });
}

function gerarNovoIdChamado() {
    const numeros = chamados
        .map((chamado) => parseInt(chamado.id.split('-')[1], 10))
        .filter((numero) => !Number.isNaN(numero));
    const proximo = Math.max(...numeros, 1024) + 1;
    return `CH-${proximo}`;
}

function ajustarCamposCriacaoParaPerfil() {
    const paginaCriacao = document.getElementById('pagina-criacao');
    if (!paginaCriacao) return;

    const campoCliente = document.getElementById('campo-cliente');
    const campoLoginCliente = document.getElementById('campo-login-cliente');
    const campoStatus = document.getElementById('campo-status');
    const ajudaStatus = document.getElementById('ajuda-status');
    const linkVoltar = document.getElementById('link-voltar-criacao');

    if (linkVoltar) {
        linkVoltar.href = usuarioAutenticado?.tipo === 'Cliente' ? 'cliente.html' : 'index.html';
    }

    if (usuarioAutenticado?.tipo === 'Cliente') {
        const loginCliente = usuarioAutenticado.clienteId || 'cliente';
        if (campoCliente) {
            campoCliente.value = obterNomeClientePorLogin(loginCliente);
            campoCliente.readOnly = true;
        }
        if (campoLoginCliente) {
            campoLoginCliente.value = loginCliente;
            campoLoginCliente.readOnly = true;
        }
        if (campoStatus) {
            campoStatus.value = 'Aberto';
            campoStatus.disabled = true;
        }
        if (ajudaStatus) {
            ajudaStatus.textContent = 'Chamados abertos pelo cliente iniciam como "Aberto".';
        }
    }
}

function registrarFormularioCriacao() {
    const formulario = document.getElementById('form-criar-chamado');
    if (!formulario) return;

    const alerta = document.getElementById('alerta-criacao');

    formulario.addEventListener('submit', (evento) => {
        evento.preventDefault();

        if (!usuarioAutenticado) {
            redirecionarParaLogin();
            return;
        }

        const nomeCliente = document.getElementById('campo-cliente').value.trim();
        const loginCliente = document.getElementById('campo-login-cliente').value.trim();
        const resumo = document.getElementById('campo-resumo').value.trim();
        const prioridade = document.getElementById('campo-prioridade').value;
        const statusSelecionado =
            usuarioAutenticado?.tipo === 'Cliente' ? 'Aberto' : document.getElementById('campo-status').value;
        const descricao = document.getElementById('campo-descricao').value.trim();
        const anexo = document.getElementById('campo-anexo').files[0]?.name;

        if (!nomeCliente || !loginCliente || !resumo || !descricao) return;

        const dataAtual = new Date();
        const dataFormatada = `${dataAtual.toLocaleDateString('pt-BR')} ${dataAtual.toLocaleTimeString('pt-BR')}`;

        const novaAtualizacao = {
            author: usuarioAutenticado.tipo,
            message: descricao,
            date: dataFormatada,
            attachments: anexo ? [anexo] : []
        };

        const novoChamado = {
            id: gerarNovoIdChamado(),
            client: nomeCliente,
            summary: resumo,
            lastUpdate: dataFormatada,
            openedAt: dataAtual.toLocaleDateString('pt-BR'),
            priority: prioridade,
            status: statusSelecionado,
            clienteLogin: loginCliente,
            updates: [novaAtualizacao]
        };

        chamados.unshift(novoChamado);

        if (alerta) {
            alerta.className = 'alert alert-success';
            alerta.textContent = `Chamado ${novoChamado.id} criado com sucesso! Redirecionando...`;
        }

        setTimeout(() => {
            window.location.href = usuarioAutenticado?.tipo === 'Cliente' ? 'cliente.html' : 'index.html';
        }, 600);

        formulario.reset();
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

    if (
        usuarioAutenticado?.tipo === 'Cliente' &&
        (chamado.clienteLogin !== usuarioAutenticado.clienteId || chamado.status !== 'Aberto')
    ) {
        preencherCabecalhoChamado(chamado);
        preencherHistorico(chamado);
        preencherAnexos(chamado);
        bloquearAtualizacaoPorPermissao('Este chamado não está disponível para edição pelo seu usuário.');
        return;
    }

    preencherCabecalhoChamado(chamado);
    preencherHistorico(chamado);
    preencherAnexos(chamado);
    registrarFormularioAtualizacao(chamado);
}

function inicializar() {
    definirUsuarioAutenticadoSeSalvo();
    configurarTelaLogin();

    const paginaDetalhes = document.getElementById('detalhes-chamado');
    const paginaListaTecnico = document.getElementById('table-chamados');
    const paginaCliente = document.getElementById('pagina-cliente');
    const paginaCriacao = document.getElementById('pagina-criacao');

    if (!usuarioAutenticado && (paginaDetalhes || paginaListaTecnico || paginaCliente || paginaCriacao)) {
        redirecionarParaLogin();
        return;
    }

    if (paginaListaTecnico) {
        if (usuarioAutenticado?.tipo !== 'Técnico') {
            window.location.href = 'cliente.html';
            return;
        }

        renderChamadosAbertos();
        registrarFiltros();
        limparFiltros();
    }

    if (paginaCliente) {
        if (usuarioAutenticado?.tipo !== 'Cliente') {
            window.location.href = 'index.html';
            return;
        }

        renderChamadosClienteAbertos();

        const botaoTrocar = document.getElementById('btn-trocar-usuario-cliente');
        if (botaoTrocar) {
            botaoTrocar.addEventListener('click', (evento) => {
                evento.preventDefault();
                limparAutenticacao();
                redirecionarParaLogin();
            });
        }
    }

    if (paginaCriacao) {
        ajustarCamposCriacaoParaPerfil();
        registrarFormularioCriacao();
    }

    if (paginaDetalhes) {
        if (!usuarioAutenticado) {
            redirecionarParaLogin();
            return;
        }

        if (usuarioAutenticado?.tipo === 'Cliente' && !usuarioAutenticado?.clienteId) {
            bloquearAtualizacaoPorPermissao('Seu usuário não possui cliente associado para editar chamados.');
            return;
        }

        atualizarPainelIdentificacao();
        carregarDetalhesChamado();
    }
}

document.addEventListener('DOMContentLoaded', inicializar);
