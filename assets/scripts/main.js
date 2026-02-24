// const CHAMADOS_INICIAIS = [
//   {
//     id: "C-1",
//     client: "Clínica Horizonte",
//     summary: "Falha no acesso ao prontuário eletrônico",
//     description: "Usuários não conseguem salvar novos pacientes no sistema.",
//     lastUpdate: "10/06/2024 14:35",
//     openedAt: "08/06/2024",
//     priority: "Alta",
//     status: "Em andamento",
//     clienteLogin: "cliente",
//     processNumber: "0001234-12.2024.8.26.0001",
//     hasPartnership: true,
//     partnershipPercent: "30",
//     partnershipWith: "Escritório Lima",
//     updates: [],
//   },
//   {
//     id: "C-2",
//     client: "Empresa Sol Nascente",
//     summary: "Erro ao anexar comprovante no portal",
//     description: "O upload finaliza, mas o arquivo não fica visível no histórico.",
//     lastUpdate: "11/06/2024 09:20",
//     openedAt: "11/06/2024",
//     priority: "Média",
//     status: "Aberto",
//     clienteLogin: "cliente",
//     processNumber: "0001250-22.2024.8.26.0001",
//     hasPartnership: false,
//     partnershipPercent: "",
//     partnershipWith: "",
//     updates: [
//       {
//         author: "Cliente",
//         message: "Anexo enviado para validação.",
//         date: "11/06/2024 09:20",
//         attachments: ["comprovante.pdf"],
//       },
//     ],
//   },
//   {
//     id: "C-3",
//     client: "Loja Aurora",
//     summary: "Consulta de andamento do processo",
//     description: "Solicitação de retorno sobre prazo da audiência.",
//     lastUpdate: "11/06/2024 10:05",
//     openedAt: "11/06/2024",
//     priority: "Baixa",
//     status: "Aberto",
//     clienteLogin: "cliente",
//     processNumber: "Sem processo",
//     hasPartnership: false,
//     partnershipPercent: "",
//     partnershipWith: "",
//     updates: [],
//   },
// ];

const CLIENTES_INICIAIS = [
  {
    nomeCompleto: "Cliente Padrão",
    telefone: "(11) 99999-9999",
    email: "cliente@padrao.com",
    documento: "000.000.000-00",
    login: "cliente",
    senha: "cliente123",
  },
];

const API_BASE_URL = `${window.location.protocol}//${window.location.hostname || "localhost"}:5000/api`;
const API_BASE_URLS = Array.from(new Set([API_BASE_URL, "http://localhost:5000/api"]));
const CANAL_ATUALIZACAO_CHAMADOS = "chamadosAtualizados";
const CHAVE_STORAGE_LOGIN = "usuarioAutenticado";
const CHAVE_STORAGE_BANCO = "bancoProjetoAtivo";
const CHAVE_CACHE_CHAMADOS = "cacheChamados";
const CACHE_CHAMADOS_TTL_MS = 5 * 60 * 1000;
const TEMPO_MAXIMO_REQUISICAO_MS = 18000;
const RETRY_BACKOFF_MS = [300, 800, 1500];

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
};

let clientes = [];
let operacoesPendentes = 0;

let bancoProjetoAtivo = localStorage.getItem(CHAVE_STORAGE_BANCO) || "teste";

function obterBancoProjetoAtivo() {
  return bancoProjetoAtivo || "teste";
}

function definirBancoProjetoAtivo(nomeBanco) {
  bancoProjetoAtivo = (nomeBanco || "teste").trim();
  localStorage.setItem(CHAVE_STORAGE_BANCO, bancoProjetoAtivo);
}

function alternarLoadingProcessamento(ativo) {
  const overlay = document.getElementById("overlay-loading-global");
  if (!overlay) return;
  overlay.classList.toggle("ativo", ativo);
  document.body.classList.toggle("ui-bloqueada", ativo);
}

function iniciarOperacaoAssincrona() {
  operacoesPendentes += 1;
  alternarLoadingProcessamento(true);
}

function finalizarOperacaoAssincrona() {
  operacoesPendentes = Math.max(0, operacoesPendentes - 1);
  if (operacoesPendentes === 0) alternarLoadingProcessamento(false);
}

function garantirOverlayLoading() {
  if (document.getElementById("overlay-loading-global")) return;
  const overlay = document.createElement("div");
  overlay.id = "overlay-loading-global";
  overlay.className = "overlay-loading-global";
  overlay.innerHTML = `
    <div class="loading-content" role="status" aria-live="polite" aria-label="Processando ação">
      <div class="loading-spinner"></div>
      <small>Processando...</small>
    </div>
  `;
  document.body.appendChild(overlay);
}

async function carregarProjetosDisponiveis() {
  return requisicaoApi("/projetos", {}, { incluirBancoNoHeader: false });
}

function formatarDataHoraAtual() {
  return new Date().toLocaleString("pt-BR");
}

function lerArquivoComoDataUrl(arquivo) {
  return new Promise((resolve, reject) => {
    const leitor = new FileReader();
    leitor.onload = () => resolve(leitor.result);
    leitor.onerror = () => reject(new Error("Falha ao ler arquivo anexado."));
    leitor.readAsDataURL(arquivo);
  });
}

function normalizarAnexo(anexo) {
  if (!anexo) return null;
  if (typeof anexo === "string") return { name: anexo, content: null };
  if (typeof anexo === "object" && anexo.name) return { name: anexo.name, content: anexo.content || null };
  return null;
}

function renderizarAnexosComDownload(anexos = []) {
  const anexosNormalizados = anexos.map(normalizarAnexo).filter(Boolean);
  if (!anexosNormalizados.length) return "";

  return anexosNormalizados
    .map((anexo) => {
      if (!anexo.content) return `<span class="text-muted">${anexo.name}</span>`;
      return `<a href="${anexo.content}" download="${anexo.name}">${anexo.name}</a>`;
    })
    .join(", ");
}

async function requisicaoApi(caminho, opcoes = {}, opcoesInternas = {}) {
  const incluirBancoNoHeader = opcoesInternas.incluirBancoNoHeader !== false;

  const tentarComBase = async (baseUrl) => {
    for (let tentativa = 0; tentativa <= RETRY_BACKOFF_MS.length; tentativa += 1) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), TEMPO_MAXIMO_REQUISICAO_MS);
      try {
        const resposta = await fetch(`${baseUrl}${caminho}`, {
          headers: {
            "Content-Type": "application/json",
            ...(incluirBancoNoHeader ? { "X-Project-DB": obterBancoProjetoAtivo() } : {}),
            ...(opcoes.headers || {}),
          },
          ...opcoes,
          signal: controller.signal,
        });

        if (!resposta.ok) {
          const erroTexto = await resposta.text();
          if (resposta.status >= 400 && resposta.status < 500) {
            throw new Error(erroTexto || "Falha na requisição.");
          }
          if (tentativa < RETRY_BACKOFF_MS.length) {
            await new Promise((resolve) => setTimeout(resolve, RETRY_BACKOFF_MS[tentativa]));
            continue;
          }
          throw new Error(erroTexto || "Falha na comunicação com o banco de dados.");
        }

        const texto = await resposta.text();
        return texto ? JSON.parse(texto) : null;
      } catch (erro) {
        const erroRepetivel = erro.name === "AbortError" || erro instanceof TypeError;
        if (erroRepetivel && tentativa < RETRY_BACKOFF_MS.length) {
          await new Promise((resolve) => setTimeout(resolve, RETRY_BACKOFF_MS[tentativa]));
          continue;
        }
        if (erro.name === "AbortError") throw new Error("Tempo limite excedido. Tente novamente.");
        throw erro;
      } finally {
        clearTimeout(timeout);
      }
    }
    throw new Error("Falha na comunicação com a API.");
  };

  iniciarOperacaoAssincrona();
  try {
    let ultimoErro = null;
    for (const baseUrl of API_BASE_URLS) {
      try {
        return await tentarComBase(baseUrl);
      } catch (erro) {
        ultimoErro = erro;
      }
    }
    throw ultimoErro || new Error("Falha na comunicação com a API.");
  } finally {
    finalizarOperacaoAssincrona();
  }
}

function lerCacheChamados() {
  try {
    const bruto = sessionStorage.getItem(CHAVE_CACHE_CHAMADOS);
    if (!bruto) return null;
    const cache = JSON.parse(bruto);
    if (!cache?.timestamp || !Array.isArray(cache?.dados)) return null;
    return cache;
  } catch {
    return null;
  }
}

function escreverCacheChamados(dados = chamados) {
  sessionStorage.setItem(CHAVE_CACHE_CHAMADOS, JSON.stringify({ timestamp: Date.now(), banco: obterBancoProjetoAtivo(), dados }));
}

function invalidarCacheChamados() {
  sessionStorage.removeItem(CHAVE_CACHE_CHAMADOS);
}

async function carregarChamadosSalvos(opcoes = {}) {
  const { usarCache = true, revalidar = true } = opcoes;
  const cache = usarCache ? lerCacheChamados() : null;
  const cacheValido = cache && cache.banco === obterBancoProjetoAtivo() && Date.now() - cache.timestamp < CACHE_CHAMADOS_TTL_MS;

  if (cacheValido) {
    chamados = cache.dados;
    if (revalidar) {
      requisicaoApi("/chamados?limit=50&offset=0")
        .then((dadosAtualizados) => {
          chamados = dadosAtualizados || [];
          escreverCacheChamados(chamados);
          atualizarTelaComChamadosAtualizados();
        })
        .catch(() => {});
    }
    return;
  }

  chamados = await requisicaoApi("/chamados?limit=50&offset=0");
  escreverCacheChamados(chamados);
}

async function carregarDetalheChamado(idChamado) {
  if (!idChamado) return null;
  return requisicaoApi(`/chamados/${encodeURIComponent(idChamado)}`);
}

async function carregarClientesSalvos() {
  clientes = await requisicaoApi("/clientes");
}

async function salvarClientes() {
  await requisicaoApi("/clientes", {
    method: "PUT",
    body: JSON.stringify(clientes),
  });
}

async function salvarClienteIndividual(cliente) {
  await requisicaoApi("/clientes", {
    method: "POST",
    body: JSON.stringify(cliente),
  });
}

function obterClientePorLogin(login) {
  if (!login) return null;
  return clientes.find((cliente) => cliente.login.toLowerCase() === login.toLowerCase()) || null;
}

function removerConteudoAnexos(chamadosAtualizados = []) {
  return chamadosAtualizados.map((chamado) => ({
    ...chamado,
    updates: (chamado.updates || []).map((atualizacao) => ({
      ...atualizacao,
      attachments: (atualizacao.attachments || []).map((anexo) => {
        const normalizado = normalizarAnexo(anexo);
        return normalizado ? { name: normalizado.name, content: null } : anexo;
      }),
    })),
  }));
}

function erroDeQuotaStorage(erro) {
  return (
    erro?.name === "QuotaExceededError" ||
    erro?.name === "NS_ERROR_DOM_QUOTA_REACHED" ||
    erro?.code === 22 ||
    erro?.code === 1014
  );
}

async function salvarChamados(chamadosAtualizados = chamados, atualizarTela = true) {
  try {
    await requisicaoApi("/chamados", {
      method: "PUT",
      body: JSON.stringify(chamadosAtualizados),
    });
  } catch (erro) {
    if (!erroDeQuotaStorage(erro)) throw erro;

    const chamadosCompactados = removerConteudoAnexos(chamadosAtualizados);
    try {
      await requisicaoApi("/chamados", {
        method: "PUT",
        body: JSON.stringify(chamadosCompactados),
      });
      chamados = chamadosCompactados;
    } catch {
      throw new Error("Limite de armazenamento excedido. Remova anexos grandes para continuar.");
    }
  }

  if (atualizarTela) atualizarTelaComChamadosAtualizados();
  invalidarCacheChamados();
  notificarAtualizacaoChamados();
}

function notificarAtualizacaoChamados() {
  if (typeof BroadcastChannel !== "undefined") {
    const canal = new BroadcastChannel(CANAL_ATUALIZACAO_CHAMADOS);
    canal.postMessage({ atualizadoEm: Date.now() });
    canal.close();
  }
}

async function salvarChamadoIndividual(chamado) {
  await requisicaoApi(`/chamados/${encodeURIComponent(chamado.id)}`, {
    method: "PUT",
    body: JSON.stringify(chamado),
  });
  invalidarCacheChamados();
  notificarAtualizacaoChamados();
}

async function excluirChamadoIndividual(idChamado) {
  await requisicaoApi(`/chamados/${encodeURIComponent(idChamado)}`, {
    method: "DELETE",
  });
  invalidarCacheChamados();
  notificarAtualizacaoChamados();
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

  const clienteId = (usuarioAutenticado?.clienteId || "").toLowerCase();
  const chamadosCliente = chamados.filter((c) => (c.clienteLogin || "").toLowerCase() === clienteId);
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
        <div class="card-body ticket-card-body d-flex justify-content-between gap-3">
          <div class="ticket-card-coluna-principal d-flex flex-column gap-2">
            <h3 class="h6 mb-0">${chamado.client}</h3>
            <p class="mb-0">${chamado.summary}</p>
            <small class="text-muted">${chamado.openedAt}</small>
          </div>
          <div class="ticket-card-coluna-acoes d-flex flex-column align-items-end gap-2">
            <span class="badge bg-light text-dark border">${chamado.status}</span>
            <div class="container-prioridade-card"></div>
            <a class="btn btn-primary btn-sm" href="details.html?id=${encodeURIComponent(chamado.id)}">Ver</a>
          </div>
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
      ? `<div class="small mt-2"><strong>Anexos:</strong> ${renderizarAnexosComDownload(u.attachments || [])}</div>`
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
  const anexos = chamado.updates.flatMap((u) => u.attachments || []).map(normalizarAnexo).filter(Boolean);
  lista.innerHTML = anexos.length
    ? anexos
        .map((anexo) => {
          if (!anexo.content) return `<li class="list-group-item text-muted">${anexo.name}</li>`;
          return `<li class="list-group-item"><a href="${anexo.content}" download="${anexo.name}">${anexo.name}</a></li>`;
        })
        .join("")
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
    document.getElementById("container-status")?.classList.add("d-none");
  } else {
    document.getElementById("prioridadeAtualizacao").value = chamado.priority;
    document.getElementById("statusAtualizacao").value = chamado.status;
  }

  form.addEventListener("submit", async (evento) => {
    evento.preventDefault();
    const descricao = document.getElementById("descricaoAtualizacao").value.trim();
    if (!descricao) return;
    const prioridade = document.getElementById("prioridadeAtualizacao").value;
    const status = document.getElementById("statusAtualizacao").value;
    const arquivo = document.getElementById("anexoAtualizacao").files[0];
    const anexoSerializado = arquivo
      ? [{ name: arquivo.name, content: await lerArquivoComoDataUrl(arquivo) }]
      : [];
    const nova = {
      author: usuarioAutenticado?.tipo || "Técnico",
      message: descricao,
      date: formatarDataHoraAtual(),
      attachments: anexoSerializado,
    };
    chamado.updates.unshift(nova);
    chamado.priority = usuarioAutenticado?.tipo === "Cliente" ? chamado.priority : prioridade;
    chamado.status = usuarioAutenticado?.tipo === "Cliente" ? chamado.status : status;
    chamado.lastUpdate = nova.date;
    try {
      await salvarChamadoIndividual(chamado);
    } catch (erro) {
      alert(erro.message || "Não foi possível salvar a atualização do chamado.");
      return;
    }
    preencherCabecalhoChamado(chamado);
    preencherHistorico(chamado);
    preencherAnexos(chamado);
    form.reset();
  });

  btnConcluir?.addEventListener("click", async () => {
    chamado.status = "Concluído";
    chamado.lastUpdate = formatarDataHoraAtual();
    try {
      await salvarChamadoIndividual(chamado);
    } catch (erro) {
      alert(erro.message || "Não foi possível concluir o chamado.");
      return;
    }
    preencherCabecalhoChamado(chamado);
    preencherHistorico(chamado);
  });

  btnExcluir?.addEventListener("click", async () => {
    try {
      await excluirChamadoIndividual(chamado.id);
    } catch (erro) {
      alert(erro.message || "Não foi possível excluir o chamado.");
      return;
    }
    window.location.href = "index.html";
  });
}

function gerarNovoIdChamado() {
  if (!chamados || chamados.length === 0) {
    return "C-1";
  }
  const ultimoChamado = chamados[chamados.length - 1];
  const correspondencia = (ultimoChamado.id || "").match(/(\d+)/);
  const ultimoNumero = correspondencia ? parseInt(correspondencia[1], 10) : 0;
  return `C-${ultimoNumero + 1}`;
}

function registrarFormularioCriacao() {
  const form = document.getElementById("form-criar-chamado");
  if (!form) return;

  const semProcesso = document.getElementById("campo-sem-processo");
  const campoProcesso = document.getElementById("campo-processo");
  const campoParceria = document.getElementById("campo-parceria");
  const campoParceriaPct = document.getElementById("campo-parceria-porcentagem");
  const campoParceriaCom = document.getElementById("campo-parceria-com");
  const campoCliente = document.getElementById("campo-cliente");
  const campoLoginCliente = document.getElementById("campo-login-cliente");
  const alertaCriacao = document.getElementById("alerta-criacao");
  const botaoCadastrarCliente = document.getElementById("btn-cadastrar-cliente");

  function validarClienteExistente() {
    const loginInformado = campoLoginCliente.value.trim().toLowerCase();
    if (!loginInformado) {
      botaoCadastrarCliente?.classList.add("d-none");
      return;
    }

    const clienteEncontrado = obterClientePorLogin(loginInformado);
    if (clienteEncontrado) {
      campoCliente.value = clienteEncontrado.nomeCompleto;
      botaoCadastrarCliente?.classList.add("d-none");
      if (alertaCriacao) {
        alertaCriacao.className = "alert alert-success";
        alertaCriacao.textContent = "Cliente encontrado. Você pode seguir com o chamado.";
      }
      return;
    }

    if (alertaCriacao) {
      alertaCriacao.className = "alert alert-warning";
      alertaCriacao.textContent = "Cliente não encontrado para este login. Cadastre o cliente para continuar.";
    }
    botaoCadastrarCliente?.classList.remove("d-none");
    botaoCadastrarCliente.href = `cadastro-cliente.html?login=${encodeURIComponent(loginInformado)}`;
  }

  campoLoginCliente?.addEventListener("blur", validarClienteExistente);
  campoLoginCliente?.addEventListener("input", () => {
    botaoCadastrarCliente?.classList.add("d-none");
    if (alertaCriacao) {
      alertaCriacao.className = "alert alert-info";
      alertaCriacao.textContent = "Informe os dados completos para abertura do chamado.";
    }
  });

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

  form.addEventListener("submit", async (evento) => {
    evento.preventDefault();
    if (usuarioAutenticado?.tipo !== "Técnico") return;

    const dataAtual = new Date();
    const dataFormatada = dataAtual.toLocaleString("pt-BR");
    const descricao = document.getElementById("campo-descricao").value.trim();

    const arquivoAnexado = document.getElementById("campo-anexo").files[0];
    const anexoInicial = arquivoAnexado
      ? [{ name: arquivoAnexado.name, content: await lerArquivoComoDataUrl(arquivoAnexado) }]
      : [];

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
          attachments: anexoInicial,
        },
      ],
    };

    const clienteVinculado = obterClientePorLogin(novoChamado.clienteLogin);
    if (!clienteVinculado) {
      if (alertaCriacao) {
        alertaCriacao.className = "alert alert-danger";
        alertaCriacao.textContent = "Cadastre o cliente antes de abrir o chamado.";
      }
      botaoCadastrarCliente?.classList.remove("d-none");
      botaoCadastrarCliente.href = `cadastro-cliente.html?login=${encodeURIComponent(novoChamado.clienteLogin)}`;
      return;
    }

    novoChamado.client = clienteVinculado.nomeCompleto;

    if (!novoChamado.client || !novoChamado.clienteLogin || !novoChamado.summary || !descricao) return;

    chamados.unshift(novoChamado);
    try {
      await requisicaoApi("/chamados", {
        method: "POST",
        body: JSON.stringify(novoChamado),
      });
      invalidarCacheChamados();
      notificarAtualizacaoChamados();
    } catch (erro) {
      if (alertaCriacao) {
        alertaCriacao.className = "alert alert-danger";
        alertaCriacao.textContent = erro.message || "Não foi possível salvar o chamado.";
      }
      return;
    }
    window.location.href = "index.html";
  });
}

function registrarFormularioCadastroCliente() {
  const form = document.getElementById("form-cadastro-cliente");
  if (!form) return;

  const alerta = document.getElementById("alerta-cadastro-cliente");
  const campoLogin = document.getElementById("campo-cadastro-login");
  const loginPreenchido = new URLSearchParams(window.location.search).get("login");
  if (loginPreenchido) campoLogin.value = loginPreenchido;

  form.addEventListener("submit", async (evento) => {
    evento.preventDefault();

    const novoCliente = {
      nomeCompleto: document.getElementById("campo-cadastro-nome").value.trim(),
      telefone: document.getElementById("campo-cadastro-telefone").value.trim(),
      email: document.getElementById("campo-cadastro-email").value.trim().toLowerCase(),
      documento: document.getElementById("campo-cadastro-documento").value.trim(),
      login: campoLogin.value.trim().toLowerCase(),
      senha: document.getElementById("campo-cadastro-senha").value.trim(),
    };

    if (!novoCliente.nomeCompleto || !novoCliente.telefone || !novoCliente.email || !novoCliente.documento || !novoCliente.login || !novoCliente.senha) {
      return;
    }

    if (credenciaisLogin[novoCliente.login] || obterClientePorLogin(novoCliente.login)) {
      if (alerta) {
        alerta.className = "alert alert-danger";
        alerta.textContent = "Este login já está em uso. Informe outro login.";
      }
      return;
    }

    try {
      clientes.push(novoCliente);
      await salvarClienteIndividual(novoCliente);
    } catch (erro) {
      if (alerta) {
        alerta.className = "alert alert-danger";
        alerta.textContent = erro.message || "Não foi possível cadastrar o cliente.";
      }
      return;
    }

    if (alerta) {
      alerta.className = "alert alert-success";
      alerta.textContent = "Cliente cadastrado com sucesso. Agora você pode abrir o chamado.";
    }

    setTimeout(() => {
      window.location.href = `create.html?clienteLogin=${encodeURIComponent(novoCliente.login)}`;
    }, 800);
  });
}

async function carregarDetalhesChamado() {
  const container = document.getElementById("detalhes-chamado");
  if (!container) return;
  const id = new URLSearchParams(window.location.search).get("id") || chamados[0]?.id;
  let chamado = chamados.find((c) => c.id === id);
  try {
    chamado = await carregarDetalheChamado(id);
  } catch {
    // fallback para o registro já carregado
  }
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
  if (!texto || !badge) return;
  texto.textContent = usuarioAutenticado
    ? `Atualizações serão registradas como ${usuarioAutenticado.tipo}.`
    : "Nenhum usuário autenticado.";
  badge.textContent = usuarioAutenticado?.tipo || "-";
}

function atualizarNomeUsuarioCabecalho() {
  const campo = document.getElementById("nome-usuario-cabecalho");
  if (!campo) return;
  campo.textContent = `Usuário: ${usuarioAutenticado?.usuario || "-"}`;
}

function atualizarAcoesCabecalhoAdministrador() {
  const botoesAdmin = document.querySelectorAll("[data-acao-admin='cadastrar-usuario']");
  const exibir = usuarioAutenticado?.tipo === "Administrador";
  botoesAdmin.forEach((botao) => botao.classList.toggle("d-none", !exibir));
}

function registrarBotoesTrocaUsuario() {
  document.querySelectorAll("#btn-trocar-usuario, #btn-trocar-usuario-cliente").forEach((botao) => {
    botao.addEventListener("click", (e) => {
      e.preventDefault();
      limparAutenticacao();
      redirecionarParaLogin(true);
    });
  });
}

async function configurarTelaLogin() {
  const form = document.getElementById("form-login");
  if (!form) return;
  const params = new URLSearchParams(window.location.search);
  const forcarLogout = params.get("logout") === "1";
  if (forcarLogout) {
    limparAutenticacao();
    params.delete("logout");
    const novaQuery = params.toString();
    window.history.replaceState({}, "", `login.html${novaQuery ? `?${novaQuery}` : ""}`);
  }
  if (usuarioAutenticado) {
    window.location.href = usuarioAutenticado.tipo === "Técnico" ? "index.html" : "cliente.html";
    return;
  }
  const alerta = document.getElementById("alerta-login");
  const seletorProjeto = document.getElementById("campo-projeto-login");
  try {
    const dadosProjetos = await carregarProjetosDisponiveis();
    if (seletorProjeto) {
      seletorProjeto.innerHTML = (dadosProjetos.projetos || [])
        .map((projeto) => `<option value="${projeto}">${projeto}</option>`)
        .join("");
      seletorProjeto.value = obterBancoProjetoAtivo();
      seletorProjeto.addEventListener("change", () => definirBancoProjetoAtivo(seletorProjeto.value));
    }
  } catch {
    if (alerta) {
      alerta.className = "alert alert-warning";
      alerta.textContent = "Não foi possível carregar a lista de projetos do servidor.";
    }
  }

  form.addEventListener("submit", async (evento) => {
    evento.preventDefault();
    const usuario = document.getElementById("campo-usuario").value.trim();
    const senha = document.getElementById("campo-senha").value.trim();
    try {
      const autenticacao = await requisicaoApi("/login", {
        method: "POST",
        body: JSON.stringify({ usuario, senha, banco: obterBancoProjetoAtivo() }),
      });
      if (autenticacao.banco) definirBancoProjetoAtivo(autenticacao.banco);
      salvarUsuarioAutenticado({
        usuario,
        tipo: autenticacao.tipo,
        clienteId: autenticacao.clienteId,
      });
      window.location.href = autenticacao.redirect;
      return;
    } catch {
      // segue para o alerta de erro
    }
    if (alerta) {
      alerta.className = "alert alert-danger";
      alerta.textContent = "Credenciais inválidas.";
    }
  });
}


async function configurarPainelAdministrador() {
  const containerLista = document.getElementById("lista-projetos-admin");
  const atual = document.getElementById("banco-atual-admin");
  if (!containerLista || !atual) return;

  atual.textContent = obterBancoProjetoAtivo();
  const dados = await carregarProjetosDisponiveis();
  const projetos = dados.projetos || [];

  containerLista.innerHTML = "";
  projetos.forEach((projeto) => {
    const item = document.createElement("button");
    item.className = "list-group-item list-group-item-action d-flex justify-content-between align-items-center";
    item.innerHTML = `<span>${projeto}</span><span class="badge bg-primary">Selecionar</span>`;
    item.addEventListener("click", () => {
      definirBancoProjetoAtivo(projeto);
      atual.textContent = projeto;
      window.location.href = "index.html";
    });
    containerLista.appendChild(item);
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

function redirecionarParaLogin(forcarLogout = false) {
  window.location.href = forcarLogout ? "login.html?logout=1" : "login.html";
}

async function inicializar() {
  garantirOverlayLoading();
  definirUsuarioAutenticadoSeSalvo();

  const paginaDetalhes = document.getElementById("detalhes-chamado");
  const paginaListaTecnico = document.getElementById("table-chamados");
  const paginaCliente = document.getElementById("pagina-cliente");
  const paginaCriacao = document.getElementById("pagina-criacao");
  const paginaCadastroCliente = document.getElementById("pagina-cadastro-cliente");
  const paginaAdmin = document.getElementById("pagina-admin");

  await configurarTelaLogin();

  const paginaProtegida = paginaDetalhes || paginaListaTecnico || paginaCliente || paginaCriacao || paginaCadastroCliente || paginaAdmin;
  if (paginaProtegida) {
    try {
      await Promise.all([carregarChamadosSalvos(), carregarClientesSalvos()]);
    } catch {
      alert(`Não foi possível carregar dados do banco '${obterBancoProjetoAtivo()}'. Verifique o backend Python.`);
      return;
    }
  }

  if (!usuarioAutenticado && (paginaDetalhes || paginaListaTecnico || paginaCliente || paginaCriacao || paginaCadastroCliente || paginaAdmin)) {
    redirecionarParaLogin();
    return;
  }

  if (paginaAdmin && usuarioAutenticado?.tipo !== "Administrador") {
    window.location.href = "index.html";
    return;
  }

  if (paginaListaTecnico && !["Técnico", "Administrador"].includes(usuarioAutenticado?.tipo)) {
    window.location.href = "cliente.html";
    return;
  }

  if ((paginaCriacao || paginaCadastroCliente) && !["Técnico", "Administrador"].includes(usuarioAutenticado?.tipo)) {
    window.location.href = "cliente.html";
    return;
  }

  if (paginaListaTecnico) {
    renderChamadosTabela();
    renderChamadosAbertos();
    registrarFiltros();
  }

  if (paginaCliente) renderChamadosClienteAbertos();

  if (paginaCriacao) {
    const loginClientePredefinido = new URLSearchParams(window.location.search).get("clienteLogin");
    if (loginClientePredefinido) {
      const campoLoginCliente = document.getElementById("campo-login-cliente");
      if (campoLoginCliente) campoLoginCliente.value = loginClientePredefinido;
    }
    registrarFormularioCriacao();
  }
  if (paginaCadastroCliente) registrarFormularioCadastroCliente();
  if (paginaAdmin) await configurarPainelAdministrador();
  if (paginaDetalhes) {
    atualizarPainelIdentificacao();
    await carregarDetalhesChamado();
  }

  atualizarNomeUsuarioCabecalho();
  atualizarAcoesCabecalhoAdministrador();
  registrarBotoesTrocaUsuario();

  if (typeof BroadcastChannel !== "undefined") {
    const canalAtualizacao = new BroadcastChannel(CANAL_ATUALIZACAO_CHAMADOS);
    canalAtualizacao.addEventListener("message", async () => {
      await carregarChamadosSalvos({ usarCache: false, revalidar: false });
      atualizarTelaComChamadosAtualizados();
    });
  }
}

document.addEventListener("DOMContentLoaded", () => {
  inicializar();
});
