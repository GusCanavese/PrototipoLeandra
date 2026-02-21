const CHAMADOS_INICIAIS = [
  {
    id: "CH-1024",
    client: "Clínica Horizonte",
    summary: "Falha no acesso ao prontuário eletrônico",
    description: "Usuários não conseguem salvar novos pacientes no sistema.",
    lastUpdate: "10/06/2024 14:35",
    openedAt: "08/06/2024",
    priority: "Alta",
    status: "Em andamento",
    clienteLogin: "cliente",
    processNumber: "0001234-12.2024.8.26.0001",
    hasPartnership: true,
    partnershipPercent: "30",
    partnershipWith: "Escritório Lima",
    updates: [],
  },
  {
    id: "CH-1025",
    client: "Empresa Sol Nascente",
    summary: "Erro ao anexar comprovante no portal",
    description: "O upload finaliza, mas o arquivo não fica visível no histórico.",
    lastUpdate: "11/06/2024 09:20",
    openedAt: "11/06/2024",
    priority: "Média",
    status: "Aberto",
    clienteLogin: "cliente",
    processNumber: "0001250-22.2024.8.26.0001",
    hasPartnership: false,
    partnershipPercent: "",
    partnershipWith: "",
    updates: [
      {
        author: "Cliente",
        message: "Anexo enviado para validação.",
        date: "11/06/2024 09:20",
        attachments: ["comprovante.pdf"],
      },
    ],
  },
  {
    id: "CH-1026",
    client: "Loja Aurora",
    summary: "Consulta de andamento do processo",
    description: "Solicitação de retorno sobre prazo da audiência.",
    lastUpdate: "11/06/2024 10:05",
    openedAt: "11/06/2024",
    priority: "Baixa",
    status: "Aberto",
    clienteLogin: "cliente",
    processNumber: "Sem processo",
    hasPartnership: false,
    partnershipPercent: "",
    partnershipWith: "",
    updates: [],
  },
];

const CHAVE_STORAGE_CHAMADOS = "chamadosRegistrados";
const CANAL_ATUALIZACAO_CHAMADOS = "chamadosAtualizados";
const CHAVE_STORAGE_LOGIN = "usuarioAutenticado";

let chamados = [];
let usuarioAutenticado = null;
const filtros = {
  client: "",
  summary: "",
  lastUpdate: "",
  openedAt: "",
  priority: "",
  status: "",
};

const credenciaisLogin = {
  tecnico: { senha: "tecnico123", tipo: "Técnico", redirect: "index.html" },
  cliente: {
    senha: "cliente123",
    tipo: "Cliente",
    redirect: "cliente.html",
    clienteId: "cliente",
  },
};

function formatarDataHoraAtual() {
  return new Date().toLocaleString("pt-BR");
}

function carregarChamadosSalvos() {
  try {
    const dados = localStorage.getItem(CHAVE_STORAGE_CHAMADOS);
    chamados = dados ? JSON.parse(dados) : [...CHAMADOS_INICIAIS];
  } catch {
    chamados = [...CHAMADOS_INICIAIS];
  }
  salvarChamados(chamados, false);
}

function salvarChamados(chamadosAtualizados = chamados, atualizarTela = true) {
  localStorage.setItem(CHAVE_STORAGE_CHAMADOS, JSON.stringify(chamadosAtualizados));
  if (atualizarTela) atualizarTelaComChamadosAtualizados();
  if (typeof BroadcastChannel !== "undefined") {
    const canal = new BroadcastChannel(CANAL_ATUALIZACAO_CHAMADOS);
    canal.postMessage({ atualizadoEm: Date.now() });
    canal.close();
  }
}

function obterUsuarioSalvo() {
  try {
    const dados = localStorage.getItem(CHAVE_STORAGE_LOGIN);
    return dados ? JSON.parse(dados) : null;
  } catch {
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
  if (!usuarioAutenticado) usuarioAutenticado = obterUsuarioSalvo();
}

function createPriorityBadge(priority) {
  const badge = document.createElement("span");
  badge.classList.add("badge", "rounded-pill", "priority-badge");
  badge.classList.add(
    priority === "Alta" ? "priority-alta" : priority === "Média" ? "priority-media" : "priority-baixa",
  );
  badge.textContent = priority;
  return badge;
}

function renderChamadosTabela() {
  const corpoTabela = document.getElementById("lista-chamados");
  if (!corpoTabela) return;
  corpoTabela.innerHTML = "";

  chamados
    .filter((chamado) => {
      const atendeCliente = chamado.client.toLowerCase().includes(filtros.client);
      const atendeResumo = chamado.summary.toLowerCase().includes(filtros.summary);
      const atendeUltima = chamado.lastUpdate.toLowerCase().includes(filtros.lastUpdate);
      const atendeAbertura = chamado.openedAt.toLowerCase().includes(filtros.openedAt);
      const atendeStatus = chamado.status.toLowerCase().includes(filtros.status);
      const atendePrioridade = !filtros.priority || chamado.priority === filtros.priority;
      return (
        atendeCliente &&
        atendeResumo &&
        atendeUltima &&
        atendeAbertura &&
        atendeStatus &&
        atendePrioridade
      );
    })
    .forEach((chamado) => {
      const linha = document.createElement("tr");
      linha.innerHTML = `
        <td><div class="fw-semibold">${chamado.client}</div><div class="text-muted small">${chamado.id}</div></td>
        <td>${chamado.summary}</td>
        <td><span class="badge bg-light text-dark border">${chamado.status}</span></td>
        <td>${chamado.lastUpdate}</td>
        <td>${chamado.openedAt}</td>
        <td class="text-center"></td>
        <td class="text-end"><a class="btn btn-sm btn-primary" href="details.html?id=${encodeURIComponent(chamado.id)}">Ver</a></td>
      `;
      linha.querySelector("td:nth-child(6)").appendChild(createPriorityBadge(chamado.priority));
      corpoTabela.appendChild(linha);
    });
}

function renderChamadosClienteAbertos() {
  const lista = document.getElementById("lista-chamados-cliente");
  if (!lista) return;
  lista.innerHTML = "";

  const clienteId = usuarioAutenticado?.clienteId;
  const chamadosCliente = chamados.filter((c) => c.clienteLogin === clienteId);
  if (!chamadosCliente.length) {
    lista.innerHTML = '<div class="alert alert-info mb-0">Nenhum chamado encontrado.</div>';
    return;
  }

  chamadosCliente.forEach((chamado) => {
    const item = document.createElement("div");
    item.className = "col-12 col-md-6";
    item.innerHTML = `
      <div class="card h-100 shadow-sm">
        <div class="card-body d-flex flex-column">
          <p class="text-muted small mb-1">${chamado.id}</p>
          <h3 class="h6 mb-1">${chamado.summary}</h3>
          <p class="mb-1">${chamado.client}</p>
          <p class="small text-muted mb-2">Status: ${chamado.status}</p>
          <a class="btn btn-primary btn-sm mt-auto" href="details.html?id=${encodeURIComponent(chamado.id)}">Abrir chamado</a>
        </div>
      </div>`;
    lista.appendChild(item);
  });
}

function renderChamadosAbertos() {
  const grid = document.getElementById("grid-chamados-abertos");
  if (!grid) return;
  grid.innerHTML = "";

  chamados.filter((c) => c.status === "Aberto").forEach((chamado) => {
    const coluna = document.createElement("div");
    coluna.className = "col-12 col-md-6 col-xl-4";
    coluna.innerHTML = `
      <div class="card ticket-card h-100 shadow-sm">
        <div class="card-body d-flex flex-column gap-2">
          <h3 class="h6 mb-0">${chamado.client}</h3>
          <p class="mb-0">${chamado.summary}</p>
          <small class="text-muted">${chamado.lastUpdate}</small>
          <div><span class="badge bg-light text-dark border">${chamado.status}</span></div>
          <div class="container-prioridade-card"></div>
          <a class="btn btn-primary btn-sm mt-auto align-self-start" href="details.html?id=${encodeURIComponent(chamado.id)}">Ver</a>
        </div>
      </div>`;
    coluna.querySelector(".container-prioridade-card").appendChild(createPriorityBadge(chamado.priority));
    grid.appendChild(coluna);
  });
}

function preencherCabecalhoChamado(chamado) {
  const cabecalho = document.getElementById("cabecalho-chamado");
  const badgeStatus = document.getElementById("badge-status");
  if (!cabecalho || !badgeStatus) return;

  const parceria = chamado.hasPartnership
    ? `Sim (${chamado.partnershipPercent || 0}% com ${chamado.partnershipWith || "-"})`
    : "Não";

  cabecalho.innerHTML = `
    <div class="d-flex justify-content-between align-items-start flex-wrap gap-3">
      <div>
        <p class="text-muted mb-1">${chamado.id}</p>
        <h2 class="h5 mb-1">${chamado.client}</h2>
        <p class="mb-2">${chamado.summary}</p>
        <p class="mb-1"><strong>Nº Processo:</strong> ${chamado.processNumber || "Sem processo"}</p>
        <p class="mb-0"><strong>Parceria:</strong> ${parceria}</p>
      </div>
      <div class="d-flex flex-column align-items-end gap-1">
        ${createPriorityBadge(chamado.priority).outerHTML}
        <span class="badge bg-light text-dark border">Aberto em ${chamado.openedAt}</span>
        <span class="badge bg-secondary badge-role">Última atualização: ${chamado.lastUpdate}</span>
      </div>
    </div>`;

  badgeStatus.textContent = chamado.status;
}

function preencherHistorico(chamado) {
  const listaHistorico = document.getElementById("lista-historico");
  if (!listaHistorico) return;
  listaHistorico.innerHTML = "";

  chamado.updates.forEach((u) => {
    const anexos = (u.attachments || []).length
      ? `<div class="small mt-2"><strong>Anexos:</strong> ${(u.attachments || []).join(", ")}</div>`
      : "";
    const item = document.createElement("div");
    item.className = "timeline-item";
    item.innerHTML = `<div class="d-flex justify-content-between"><strong>${u.author}</strong><span class="small text-muted">${u.date}</span></div><p class="mb-1">${u.message}</p>${anexos}`;
    listaHistorico.appendChild(item);
  });
}

function preencherAnexos(chamado) {
  const lista = document.getElementById("lista-anexos");
  if (!lista) return;
  const anexos = chamado.updates.flatMap((u) => u.attachments || []);
  lista.innerHTML = anexos.length
    ? anexos.map((a) => `<li class="list-group-item">${a}</li>`).join("")
    : '<li class="list-group-item">Nenhum anexo registrado.</li>';
}

function registrarFormularioAtualizacao(chamado) {
  const form = document.getElementById("form-atualizacao");
  const btnConcluir = document.getElementById("btn-concluir-chamado");
  const btnExcluir = document.getElementById("btn-excluir-chamado");
  if (!form) return;

  if (usuarioAutenticado?.tipo === "Cliente") {
    btnConcluir?.classList.add("d-none");
    btnExcluir?.classList.add("d-none");
    document.getElementById("container-prioridade")?.classList.add("d-none");
  } else {
    document.getElementById("prioridadeAtualizacao").value = chamado.priority;
  }

  form.addEventListener("submit", (evento) => {
    evento.preventDefault();
    const descricao = document.getElementById("descricaoAtualizacao").value.trim();
    if (!descricao) return;
    const prioridade = document.getElementById("prioridadeAtualizacao").value;
    const arquivo = document.getElementById("anexoAtualizacao").files[0];
    const nova = {
      author: usuarioAutenticado?.tipo || "Técnico",
      message: descricao,
      date: formatarDataHoraAtual(),
      attachments: arquivo ? [arquivo.name] : [],
    };
    chamado.updates.unshift(nova);
    chamado.priority = usuarioAutenticado?.tipo === "Cliente" ? chamado.priority : prioridade;
    chamado.lastUpdate = nova.date;
    salvarChamados();
    preencherCabecalhoChamado(chamado);
    preencherHistorico(chamado);
    preencherAnexos(chamado);
    form.reset();
  });

  btnConcluir?.addEventListener("click", () => {
    chamado.status = "Concluído";
    chamado.lastUpdate = formatarDataHoraAtual();
    salvarChamados();
    preencherCabecalhoChamado(chamado);
    preencherHistorico(chamado);
  });

  btnExcluir?.addEventListener("click", () => {
    chamados = chamados.filter((item) => item.id !== chamado.id);
    salvarChamados();
    window.location.href = "index.html";
  });
}

function gerarNovoIdChamado() {
  const numeros = chamados.map((c) => parseInt((c.id || "").split("-")[1], 10)).filter((n) => !Number.isNaN(n));
  return `CH-${Math.max(...numeros, 1024) + 1}`;
}

function registrarFormularioCriacao() {
  const form = document.getElementById("form-criar-chamado");
  if (!form) return;

  const semProcesso = document.getElementById("campo-sem-processo");
  const campoProcesso = document.getElementById("campo-processo");
  const campoParceria = document.getElementById("campo-parceria");
  const campoParceriaPct = document.getElementById("campo-parceria-porcentagem");
  const campoParceriaCom = document.getElementById("campo-parceria-com");

  semProcesso?.addEventListener("change", () => {
    campoProcesso.disabled = semProcesso.checked;
    if (semProcesso.checked) campoProcesso.value = "";
  });

  campoParceria?.addEventListener("change", () => {
    const ativa = campoParceria.value === "Sim";
    campoParceriaPct.disabled = !ativa;
    campoParceriaCom.disabled = !ativa;
    if (!ativa) {
      campoParceriaPct.value = "";
      campoParceriaCom.value = "";
    }
  });

  form.addEventListener("submit", (evento) => {
    evento.preventDefault();
    if (usuarioAutenticado?.tipo !== "Técnico") return;

    const dataAtual = new Date();
    const dataFormatada = dataAtual.toLocaleString("pt-BR");
    const descricao = document.getElementById("campo-descricao").value.trim();

    const novoChamado = {
      id: gerarNovoIdChamado(),
      client: document.getElementById("campo-cliente").value.trim(),
      clienteLogin: document.getElementById("campo-login-cliente").value.trim(),
      summary: document.getElementById("campo-resumo").value.trim(),
      description: descricao,
      priority: document.getElementById("campo-prioridade").value,
      status: document.getElementById("campo-status").value,
      openedAt: dataAtual.toLocaleDateString("pt-BR"),
      lastUpdate: dataFormatada,
      processNumber: semProcesso.checked ? "Sem processo" : campoProcesso.value.trim(),
      hasPartnership: campoParceria.value === "Sim",
      partnershipPercent: campoParceriaPct.value,
      partnershipWith: campoParceriaCom.value.trim(),
      updates: [
        {
          author: usuarioAutenticado.tipo,
          message: descricao,
          date: dataFormatada,
          attachments: document.getElementById("campo-anexo").files[0]
            ? [document.getElementById("campo-anexo").files[0].name]
            : [],
        },
      ],
    };

    if (!novoChamado.client || !novoChamado.clienteLogin || !novoChamado.summary || !descricao) return;

    chamados.unshift(novoChamado);
    salvarChamados();
    window.location.href = "index.html";
  });
}

function carregarDetalhesChamado() {
  const container = document.getElementById("detalhes-chamado");
  if (!container) return;
  const id = new URLSearchParams(window.location.search).get("id") || chamados[0]?.id;
  const chamado = chamados.find((c) => c.id === id);
  if (!chamado) {
    container.innerHTML = '<div class="alert alert-warning">Chamado não encontrado.</div>';
    return;
  }
  preencherCabecalhoChamado(chamado);
  preencherHistorico(chamado);
  preencherAnexos(chamado);
  registrarFormularioAtualizacao(chamado);
}

function atualizarPainelIdentificacao() {
  const texto = document.getElementById("texto-identificacao");
  const badge = document.getElementById("badge-identificacao");
  const botao = document.getElementById("btn-trocar-usuario");
  if (!texto || !badge) return;
  texto.textContent = usuarioAutenticado
    ? `Atualizações serão registradas como ${usuarioAutenticado.tipo}.`
    : "Nenhum usuário autenticado.";
  badge.textContent = usuarioAutenticado?.tipo || "-";
  botao?.addEventListener("click", (e) => {
    e.preventDefault();
    limparAutenticacao();
    window.location.href = "login.html";
  });
}

function configurarTelaLogin() {
  const form = document.getElementById("form-login");
  if (!form) return;
  if (usuarioAutenticado) {
    window.location.href = usuarioAutenticado.tipo === "Técnico" ? "index.html" : "cliente.html";
    return;
  }
  const alerta = document.getElementById("alerta-login");
  form.addEventListener("submit", (evento) => {
    evento.preventDefault();
    const usuario = document.getElementById("campo-usuario").value.trim().toLowerCase();
    const senha = document.getElementById("campo-senha").value.trim();
    const credencial = credenciaisLogin[usuario];
    if (credencial && credencial.senha === senha) {
      salvarUsuarioAutenticado({ usuario, tipo: credencial.tipo, clienteId: credencial.clienteId });
      window.location.href = credencial.redirect;
      return;
    }
    if (alerta) {
      alerta.className = "alert alert-danger";
      alerta.textContent = "Credenciais inválidas.";
    }
  });
}

function registrarFiltros() {
  document.querySelectorAll(".filter-input").forEach((campo) => {
    campo.addEventListener("input", (evento) => {
      filtros[evento.target.dataset.column] = evento.target.value.toLowerCase();
      renderChamadosTabela();
    });
  });
  const select = document.querySelector(".filter-select");
  if (select) {
    select.addEventListener("change", (evento) => {
      filtros.priority = evento.target.value;
      renderChamadosTabela();
    });
  }
  document.querySelectorAll('[data-filter="status"]').forEach((botao) => {
    botao.addEventListener("click", () => {
      filtros.status = (botao.dataset.value || "").toLowerCase();
      renderChamadosTabela();
    });
  });
}

function atualizarTelaComChamadosAtualizados() {
  if (document.getElementById("table-chamados")) {
    renderChamadosTabela();
    renderChamadosAbertos();
  }
  if (document.getElementById("pagina-cliente")) renderChamadosClienteAbertos();
  if (document.getElementById("detalhes-chamado")) carregarDetalhesChamado();
}

function redirecionarParaLogin() {
  window.location.href = "login.html";
}

function inicializar() {
  definirUsuarioAutenticadoSeSalvo();
  carregarChamadosSalvos();
  configurarTelaLogin();

  const paginaDetalhes = document.getElementById("detalhes-chamado");
  const paginaListaTecnico = document.getElementById("table-chamados");
  const paginaCliente = document.getElementById("pagina-cliente");
  const paginaCriacao = document.getElementById("pagina-criacao");

  if (!usuarioAutenticado && (paginaDetalhes || paginaListaTecnico || paginaCliente || paginaCriacao)) {
    redirecionarParaLogin();
    return;
  }

  if (paginaListaTecnico && usuarioAutenticado?.tipo !== "Técnico") {
    window.location.href = "cliente.html";
    return;
  }

  if (paginaCriacao && usuarioAutenticado?.tipo !== "Técnico") {
    window.location.href = "cliente.html";
    return;
  }

  if (paginaListaTecnico) {
    renderChamadosTabela();
    renderChamadosAbertos();
    registrarFiltros();
  }

  if (paginaCliente) {
    renderChamadosClienteAbertos();
    document.getElementById("btn-trocar-usuario-cliente")?.addEventListener("click", (evento) => {
      evento.preventDefault();
      limparAutenticacao();
      redirecionarParaLogin();
    });
  }

  if (paginaCriacao) registrarFormularioCriacao();
  if (paginaDetalhes) {
    atualizarPainelIdentificacao();
    carregarDetalhesChamado();
  }

  window.addEventListener("storage", (evento) => {
    if (evento.key === CHAVE_STORAGE_CHAMADOS) {
      carregarChamadosSalvos();
      atualizarTelaComChamadosAtualizados();
    }
  });
}

document.addEventListener("DOMContentLoaded", inicializar);
