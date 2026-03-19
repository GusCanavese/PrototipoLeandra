const API_URL = `${window.location.protocol}//${window.location.hostname || "localhost"}:5000/api`;
const CANAL_CHAMADOS = "chamadosAtualizados";
const CHAVE_LOGIN = "usuarioAutenticado";
const CHAVE_BANCO = "bancoProjetoAtivo";
const CHAVE_CHAMADOS = "cacheChamados";
const CHAVE_CLIENTES = "cacheClientes";
const DATABASE_PADRAO = "teste";
const TEMPO_CACHE = 5 * 60 * 1000;
const TIMEOUT = 25000;
const RETRY = [350, 900];
const filtros = { client: "", summary: "", lastUpdate: "", openedAt: "", priority: "", status: "" };
const credenciaisLogin = { tecnico: { senha: "tecnico123", tipo: "Técnico", redirect: "index.html" } };

let chamados = [];
let clientes = [];
let usuario = null;
let carregandoChamados = null;
let operacoes = 0;
let bancoProjetoAtivo = localStorage.getItem(CHAVE_BANCO) || DATABASE_PADRAO;

function salvarBanco(nome) {
  bancoProjetoAtivo = (nome || DATABASE_PADRAO).trim();
  localStorage.setItem(CHAVE_BANCO, bancoProjetoAtivo);
}

function overlay(ativo) {
  let el = document.getElementById("overlay-loading-global");
  if (!el) {
    el = document.createElement("div");
    el.id = "overlay-loading-global";
    el.className = "overlay-loading-global";
    el.innerHTML = '<div class="loading-content" role="status" aria-live="polite" aria-label="Processando ação"><div class="loading-spinner"></div><small>Processando...</small></div>';
    document.body.appendChild(el);
  }
  el.classList.toggle("ativo", ativo);
  document.body.classList.toggle("ui-bloqueada", ativo);
}

async function api(path, options = {}, { semBanco = false } = {}) {
  operacoes += 1;
  overlay(true);

  try {
    let ultimoErro = null;
    for (let tentativa = 0; tentativa <= RETRY.length; tentativa += 1) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), TIMEOUT);
      try {
        const resposta = await fetch(`${API_URL}${path}`, {
          headers: {
            "Content-Type": "application/json",
            ...(semBanco ? {} : { "X-Project-DB": bancoProjetoAtivo }),
            ...(options.headers || {}),
          },
          ...options,
          signal: controller.signal,
        });
        if (!resposta.ok) {
          const textoErro = await resposta.text();
          if (resposta.status >= 400 && resposta.status < 500) throw new Error(textoErro || "Falha na requisição.");
          if (tentativa < RETRY.length) {
            await new Promise((resolve) => setTimeout(resolve, RETRY[tentativa]));
            continue;
          }
          throw new Error(textoErro || "Falha na comunicação com o banco de dados.");
        }
        const texto = await resposta.text();
        return texto ? JSON.parse(texto) : null;
      } catch (erro) {
        ultimoErro = erro.name === "AbortError" ? new Error("Tempo limite excedido. Tente novamente.") : erro;
        if ((erro.name === "AbortError" || erro instanceof TypeError) && tentativa < RETRY.length) {
          await new Promise((resolve) => setTimeout(resolve, RETRY[tentativa]));
          continue;
        }
        break;
      } finally {
        clearTimeout(timeout);
      }
    }
    throw ultimoErro || new Error("Falha na comunicação com a API.");
  } finally {
    operacoes = Math.max(0, operacoes - 1);
    overlay(operacoes > 0);
  }
}

function storageJson(storage, key, value) {
  try {
    if (value === undefined) {
      const raw = storage.getItem(key);
      return raw ? JSON.parse(raw) : null;
    }
    if (value === null) return storage.removeItem(key);
    return storage.setItem(key, JSON.stringify(value));
  } catch {
    return null;
  }
}

function cacheLista(chave, lista, resumir = false) {
  if (lista === undefined) {
    const cache = storageJson(sessionStorage, chave);
    return cache?.banco === bancoProjetoAtivo && Array.isArray(cache?.dados) && Date.now() - cache.timestamp < TEMPO_CACHE ? cache.dados : null;
  }
  storageJson(sessionStorage, chave, lista === null ? null : {
    timestamp: Date.now(),
    banco: bancoProjetoAtivo,
    dados: resumir ? lista.map((item) => ({
      id: item.id,
      client: item.client,
      clienteLogin: item.clienteLogin,
      summary: item.summary,
      priority: item.priority,
      status: item.status,
      openedAt: item.openedAt,
      lastUpdate: item.lastUpdate,
    })) : (lista || []),
  });
}

function arquivoBase64(arquivo) {
  return new Promise((resolve, reject) => {
    const leitor = new FileReader();
    leitor.onload = () => resolve(leitor.result);
    leitor.onerror = () => reject(new Error("Falha ao ler arquivo anexado."));
    leitor.readAsDataURL(arquivo);
  });
}

function notificarChamados() {
  if (typeof BroadcastChannel === "undefined") return;
  const canal = new BroadcastChannel(CANAL_CHAMADOS);
  canal.postMessage({ atualizadoEm: Date.now() });
  canal.close();
}

function syncChamados(chamado, remover = false) {
  if (remover) chamados = chamados.filter((item) => item.id !== chamado);
  else {
    const resumo = {
      id: chamado.id,
      client: chamado.client,
      clienteLogin: chamado.clienteLogin,
      summary: chamado.summary,
      priority: chamado.priority,
      status: chamado.status,
      openedAt: chamado.openedAt,
      lastUpdate: chamado.lastUpdate,
    };
    const index = chamados.findIndex((item) => item.id === resumo.id);
    if (index >= 0) chamados.splice(index, 1, { ...chamados[index], ...resumo });
    else chamados.unshift(resumo);
  }
  cacheLista(CHAVE_CHAMADOS, chamados, true);
}

function clientePorLogin(login) {
  login = (login || "").toLowerCase();
  return login ? clientes.find((item) => item.login.toLowerCase() === login) || null : null;
}

function anexoNormalizado(anexo) {
  if (!anexo) return null;
  if (typeof anexo === "string") return { name: anexo, content: null };
  return typeof anexo === "object" && anexo.name ? { name: anexo.name, content: anexo.content || null } : null;
}

function htmlAnexos(anexos = []) {
  return anexos.map(anexoNormalizado).filter(Boolean).map((anexo) => (
    anexo.content ? `<a href="${anexo.content}" download="${anexo.name}">${anexo.name}</a>` : `<span class="text-muted">${anexo.name}</span>`
  )).join(", ");
}

function badgePrioridade(prioridade) {
  const badge = document.createElement("span");
  badge.className = `badge rounded-pill priority-badge ${prioridade === "Alta" ? "priority-alta" : prioridade === "Média" ? "priority-media" : "priority-baixa"}`;
  badge.textContent = prioridade;
  return badge;
}

async function carregarChamados({ usarCache = true, revalidar = true } = {}) {
  carregandoChamados = (async () => {
    const cache = usarCache ? cacheLista(CHAVE_CHAMADOS) : null;
    if (cache) {
      chamados = cache;
      if (revalidar) {
        api("/chamados?limit=200").then((dados) => {
          chamados = dados || [];
          cacheLista(CHAVE_CHAMADOS, chamados, true);
          atualizarTela();
        }).catch(() => {});
      }
      return;
    }
    chamados = await api("/chamados?limit=200");
    cacheLista(CHAVE_CHAMADOS, chamados, true);
  })();
  try { await carregandoChamados; } finally { carregandoChamados = null; }
}

async function carregarClientes({ usarCache = true, revalidar = true } = {}) {
  const cache = usarCache ? cacheLista(CHAVE_CLIENTES) : null;
  if (cache) {
    clientes = cache;
    if (revalidar) {
      api("/clientes").then((dados) => {
        clientes = dados || [];
        cacheLista(CHAVE_CLIENTES, clientes);
      }).catch(() => {});
    }
    return;
  }
  clientes = await api("/clientes");
  cacheLista(CHAVE_CLIENTES, clientes);
}

async function salvarCliente(cliente) {
  const salvo = await api("/clientes", { method: "POST", body: JSON.stringify(cliente) });
  const clienteSalvo = salvo?.cliente || cliente;
  const index = clientes.findIndex((item) => item.login === clienteSalvo.login);
  if (index >= 0) clientes.splice(index, 1, { ...clientes[index], ...clienteSalvo });
  else clientes.unshift(clienteSalvo);
  cacheLista(CHAVE_CLIENTES, clientes);
  return clienteSalvo;
}

async function salvarChamado(chamado) {
  await api(`/chamados/${encodeURIComponent(chamado.id)}`, { method: "PUT", body: JSON.stringify(chamado) });
  syncChamados(chamado);
  notificarChamados();
}

async function excluirChamado(id) {
  await api(`/chamados/${encodeURIComponent(id)}`, { method: "DELETE" });
  syncChamados(id, true);
  notificarChamados();
}

function renderTabela() {
  const tbody = document.getElementById("lista-chamados");
  if (!tbody) return;
  tbody.innerHTML = "";
  chamados.filter((item) => (
    item.client.toLowerCase().includes(filtros.client)
    && item.summary.toLowerCase().includes(filtros.summary)
    && item.lastUpdate.toLowerCase().includes(filtros.lastUpdate)
    && item.openedAt.toLowerCase().includes(filtros.openedAt)
    && item.status.toLowerCase().includes(filtros.status)
    && (!filtros.priority || item.priority === filtros.priority)
  )).forEach((item) => {
    const linha = document.createElement("tr");
    linha.innerHTML = `
      <td><div class="fw-semibold">${item.client}</div><div class="text-muted small">${item.id}</div></td>
      <td>${item.summary}</td>
      <td><span class="badge bg-light text-dark border">${item.status}</span></td>
      <td>${item.lastUpdate}</td>
      <td>${item.openedAt}</td>
      <td class="text-center"></td>
      <td class="text-end"><a class="btn btn-sm btn-primary" href="details.html?id=${encodeURIComponent(item.id)}">Ver</a></td>`;
    linha.querySelector("td:nth-child(6)").appendChild(badgePrioridade(item.priority));
    tbody.appendChild(linha);
  });
}

function renderCardsCliente() {
  const lista = document.getElementById("lista-chamados-cliente");
  if (!lista) return;
  lista.innerHTML = "";
  const meus = chamados.filter((item) => (item.clienteLogin || "").toLowerCase() === (usuario?.usuario || "").toLowerCase());
  if (!meus.length) {
    lista.innerHTML = '<div class="alert alert-info mb-0">Nenhum chamado encontrado.</div>';
    return;
  }
  meus.forEach((item) => {
    const card = document.createElement("div");
    card.className = "col-12 col-md-6";
    card.innerHTML = `<div class="card h-100 shadow-sm"><div class="card-body d-flex flex-column"><p class="text-muted small mb-1">${item.id}</p><h3 class="h6 mb-1">${item.summary}</h3><p class="mb-1">${item.client}</p><p class="small text-muted mb-2">Status: ${item.status}</p><a class="btn btn-primary btn-sm mt-auto" href="details.html?id=${encodeURIComponent(item.id)}">Abrir chamado</a></div></div>`;
    lista.appendChild(card);
  });
}

function renderAbertos() {
  const grid = document.getElementById("grid-chamados-abertos");
  if (!grid) return;
  grid.innerHTML = "";
  chamados.filter((item) => item.status === "Aberto").forEach((item) => {
    const coluna = document.createElement("div");
    coluna.className = "col-12 col-md-6 col-xl-4";
    coluna.innerHTML = `<div class="card ticket-card h-100 shadow-sm"><div class="card-body ticket-card-body d-flex justify-content-between gap-3"><div class="ticket-card-coluna-principal d-flex flex-column gap-2"><h3 class="h6 mb-0">${item.client}</h3><p class="mb-0">${item.summary}</p><small class="text-muted">${item.openedAt}</small></div><div class="ticket-card-coluna-acoes d-flex flex-column align-items-end gap-2"><span class="badge bg-light text-dark border">${item.status}</span><div class="container-prioridade-card"></div><a class="btn btn-primary btn-sm" href="details.html?id=${encodeURIComponent(item.id)}">Ver</a></div></div></div>`;
    coluna.querySelector(".container-prioridade-card").appendChild(badgePrioridade(item.priority));
    grid.appendChild(coluna);
  });
}

function podeVer(chamado) {
  const idCliente = (usuario?.clienteId || usuario?.usuario || "").toLowerCase();
  return !!chamado && (["Técnico", "Administrador"].includes(usuario?.tipo) || (usuario?.tipo === "Cliente" && (chamado.clienteLogin || "").toLowerCase() === idCliente));
}

function renderDetalhe(chamado) {
  const cabecalho = document.getElementById("cabecalho-chamado");
  const badgeStatus = document.getElementById("badge-status");
  const historico = document.getElementById("lista-historico");
  const anexos = document.getElementById("lista-anexos");
  if (cabecalho && badgeStatus) {
    const parceria = chamado.hasPartnership ? `Sim (${chamado.partnershipPercent || 0}% com ${chamado.partnershipWith || "-"})` : "Não";
    cabecalho.innerHTML = `<div class="d-flex justify-content-between align-items-start flex-wrap gap-3"><div><h2 class="h5 mb-1">${chamado.client}</h2><p class="mb-2">${chamado.summary}</p><p class="mb-1"><strong>Nº Processo:</strong> ${chamado.processNumber || "Sem processo"}</p><p class="mb-0"><strong>Parceria:</strong> ${parceria}</p></div><div class="d-flex flex-column align-items-end gap-1">${badgePrioridade(chamado.priority).outerHTML}<span class="badge bg-light text-dark border">Aberto em ${chamado.openedAt}</span><span class="badge bg-secondary badge-role">Última atualização: ${chamado.lastUpdate}</span></div></div>`;
    badgeStatus.textContent = chamado.status;
  }
  if (historico) {
    historico.innerHTML = "";
    chamado.updates.forEach((item) => {
      const linha = document.createElement("div");
      linha.className = "timeline-item";
      linha.innerHTML = `<div class="d-flex justify-content-between"><strong>${item.author}</strong><span class="small text-muted">${item.date}</span></div><p class="mb-1">${item.message}</p>${(item.attachments || []).length ? `<div class="small mt-2"><strong>Anexos:</strong> ${htmlAnexos(item.attachments)}</div>` : ""}`;
      historico.appendChild(linha);
    });
  }
  if (anexos) {
    const lista = chamado.updates.flatMap((item) => item.attachments || []).map(anexoNormalizado).filter(Boolean);
    anexos.innerHTML = lista.length ? lista.map((item) => item.content ? `<li class="list-group-item"><a href="${item.content}" download="${item.name}">${item.name}</a></li>` : `<li class="list-group-item text-muted">${item.name}</li>`).join("") : '<li class="list-group-item">Nenhum anexo registrado.</li>';
  }
}

async function telaDetalhes() {
  const box = document.getElementById("detalhes-chamado");
  const form = document.getElementById("form-atualizacao");
  if (!box) return;
  const id = new URLSearchParams(window.location.search).get("id") || chamados[0]?.id;
  let chamado = chamados.find((item) => item.id === id);
  try { chamado = id ? await api(`/chamados/${encodeURIComponent(id)}`) : chamado; } catch {}
  if (!chamado) return void (box.innerHTML = '<div class="alert alert-warning">Chamado não encontrado.</div>');
  if (!podeVer(chamado)) return void (box.innerHTML = '<div class="alert alert-danger">Você não tem permissão para visualizar este chamado.</div>');

  renderDetalhe(chamado);
  if (!form) return;
  if (usuario?.tipo === "Cliente") {
    document.getElementById("btn-concluir-chamado")?.classList.add("d-none");
    document.getElementById("btn-excluir-chamado")?.classList.add("d-none");
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
    const arquivo = document.getElementById("anexoAtualizacao").files[0];
    const nova = {
      author: usuario?.tipo || "Técnico",
      message: descricao,
      date: new Date().toLocaleString("pt-BR"),
      attachments: arquivo ? [{ name: arquivo.name, content: await arquivoBase64(arquivo) }] : [],
    };
    chamado.updates.unshift(nova);
    chamado.priority = usuario?.tipo === "Cliente" ? chamado.priority : document.getElementById("prioridadeAtualizacao").value;
    chamado.status = usuario?.tipo === "Cliente" ? chamado.status : document.getElementById("statusAtualizacao").value;
    chamado.lastUpdate = nova.date;
    try {
      await salvarChamado(chamado);
      renderDetalhe(chamado);
      form.reset();
    } catch (erro) {
      alert(erro.message || "Não foi possível salvar a atualização do chamado.");
    }
  });

  document.getElementById("btn-concluir-chamado")?.addEventListener("click", async () => {
    chamado.status = "Concluído";
    chamado.lastUpdate = new Date().toLocaleString("pt-BR");
    try { await salvarChamado(chamado); renderDetalhe(chamado); } catch (erro) { alert(erro.message || "Não foi possível concluir o chamado."); }
  });
  document.getElementById("btn-excluir-chamado")?.addEventListener("click", async () => {
    try { await excluirChamado(chamado.id); window.location.href = usuario?.tipo === "Cliente" ? "cliente.html" : "index.html"; } catch (erro) { alert(erro.message || "Não foi possível excluir o chamado."); }
  });
}

function atualizarTela() {
  if (document.getElementById("table-chamados")) { renderTabela(); renderAbertos(); }
  if (document.getElementById("pagina-cliente")) renderCardsCliente();
  if (document.getElementById("detalhes-chamado")) telaDetalhes();
}

function configurarCriacao() {
  const form = document.getElementById("form-criar-chamado");
  if (!form) return;
  const semProcesso = document.getElementById("campo-sem-processo");
  const processo = document.getElementById("campo-processo");
  const parceria = document.getElementById("campo-parceria");
  const parceriaPct = document.getElementById("campo-parceria-porcentagem");
  const parceriaCom = document.getElementById("campo-parceria-com");
  const campoCliente = document.getElementById("campo-cliente");
  const campoLogin = document.getElementById("campo-login-cliente");
  const alerta = document.getElementById("alerta-criacao");
  const btnCadastro = document.getElementById("btn-cadastrar-cliente");
  const ehCliente = usuario?.tipo === "Cliente";

  if (ehCliente) {
    const cliente = clientePorLogin(usuario?.clienteId || "");
    campoLogin.value = usuario?.clienteId || "";
    campoLogin.readOnly = true;
    if (cliente?.nomeCompleto) { campoCliente.value = cliente.nomeCompleto; campoCliente.readOnly = true; }
    btnCadastro?.classList.add("d-none");
    if (alerta) { alerta.className = "alert alert-info"; alerta.textContent = "Você está abrindo chamado para o seu próprio usuário."; }
  }

  const validar = () => {
    const login = campoLogin.value.trim().toLowerCase();
    const cliente = clientePorLogin(login);
    if (!login) return btnCadastro?.classList.add("d-none");
    if (cliente) {
      campoCliente.value = cliente.nomeCompleto;
      btnCadastro?.classList.add("d-none");
      if (alerta) { alerta.className = "alert alert-success"; alerta.textContent = "Cliente encontrado. Você pode seguir com o chamado."; }
      return;
    }
    if (alerta) { alerta.className = "alert alert-warning"; alerta.textContent = "Cliente não encontrado para este login. Cadastre o cliente para continuar."; }
    btnCadastro?.classList.remove("d-none");
    btnCadastro.href = `cadastro-cliente.html?login=${encodeURIComponent(login)}`;
  };

  if (!ehCliente) {
    campoLogin?.addEventListener("blur", validar);
    campoLogin?.addEventListener("input", () => {
      btnCadastro?.classList.add("d-none");
      if (alerta) { alerta.className = "alert alert-info"; alerta.textContent = "Informe os dados completos para abertura do chamado."; }
    });
  }
  semProcesso?.addEventListener("change", () => { processo.disabled = semProcesso.checked; if (semProcesso.checked) processo.value = ""; });
  parceria?.addEventListener("change", () => {
    const ativa = parceria.value === "Sim";
    parceriaPct.disabled = !ativa;
    parceriaCom.disabled = !ativa;
    if (!ativa) { parceriaPct.value = ""; parceriaCom.value = ""; }
  });

  form.addEventListener("submit", async (evento) => {
    evento.preventDefault();
    if (!["Técnico", "Administrador", "Cliente"].includes(usuario?.tipo)) return;
    const descricao = document.getElementById("campo-descricao").value.trim();
    const arquivo = document.getElementById("campo-anexo").files[0];
    const login = campoLogin.value.trim();
    const cliente = clientePorLogin(login);
    const agora = new Date();
    const chamado = {
      id: "",
      client: campoCliente.value.trim(),
      clienteLogin: login,
      summary: document.getElementById("campo-resumo").value.trim(),
      description: descricao,
      priority: document.getElementById("campo-prioridade").value,
      status: document.getElementById("campo-status").value,
      openedAt: agora.toLocaleDateString("pt-BR"),
      lastUpdate: agora.toLocaleString("pt-BR"),
      processNumber: semProcesso.checked ? "Sem processo" : processo.value.trim(),
      hasPartnership: parceria.value === "Sim",
      partnershipPercent: parceriaPct.value,
      partnershipWith: parceriaCom.value.trim(),
      updates: [{ author: usuario.tipo, message: descricao, date: agora.toLocaleString("pt-BR"), attachments: arquivo ? [{ name: arquivo.name, content: await arquivoBase64(arquivo) }] : [] }],
    };

    if (!cliente) {
      if (alerta) { alerta.className = "alert alert-danger"; alerta.textContent = "Cadastre o cliente antes de abrir o chamado."; }
      btnCadastro?.classList.remove("d-none");
      btnCadastro.href = `cadastro-cliente.html?login=${encodeURIComponent(chamado.clienteLogin)}`;
      return;
    }
    chamado.client = cliente.nomeCompleto;
    if (!chamado.client || !chamado.clienteLogin || !chamado.summary || !descricao) return;

    try {
      const salvo = await api("/chamados", { method: "POST", body: JSON.stringify(chamado) });
      const chamadoSalvo = { ...chamado, ...(salvo?.chamado || {}) };
      syncChamados(chamadoSalvo);
      notificarChamados();
      window.location.href = usuario?.tipo === "Cliente" ? "cliente.html" : "index.html";
    } catch (erro) {
      if (alerta) { alerta.className = "alert alert-danger"; alerta.textContent = erro.message || "Não foi possível salvar o chamado."; }
    }
  });
}

function configurarCadastroCliente() {
  const form = document.getElementById("form-cadastro-cliente");
  const alerta = document.getElementById("alerta-cadastro-cliente");
  const login = document.getElementById("campo-cadastro-login");
  if (!form) return;
  const loginUrl = new URLSearchParams(window.location.search).get("login");
  if (loginUrl) login.value = loginUrl;

  form.addEventListener("submit", async (evento) => {
    evento.preventDefault();
    const cliente = {
      nomeCompleto: document.getElementById("campo-cadastro-nome").value.trim(),
      telefone: document.getElementById("campo-cadastro-telefone").value.trim(),
      email: document.getElementById("campo-cadastro-email").value.trim().toLowerCase(),
      documento: document.getElementById("campo-cadastro-documento").value.trim(),
      login: login.value.trim().toLowerCase(),
      senha: document.getElementById("campo-cadastro-senha").value.trim(),
    };
    if (!cliente.nomeCompleto || !cliente.telefone || !cliente.email || !cliente.documento || !cliente.login || !cliente.senha) return;
    if (credenciaisLogin[cliente.login] || clientePorLogin(cliente.login)) {
      if (alerta) { alerta.className = "alert alert-danger"; alerta.textContent = "Este login já está em uso. Informe outro login."; }
      return;
    }
    try {
      await salvarCliente(cliente);
      if (alerta) { alerta.className = "alert alert-success"; alerta.textContent = "Cliente cadastrado com sucesso. Agora você pode abrir o chamado."; }
      setTimeout(() => { window.location.href = `create.html?clienteLogin=${encodeURIComponent(cliente.login)}`; }, 800);
    } catch (erro) {
      if (alerta) { alerta.className = "alert alert-danger"; alerta.textContent = erro.message || "Não foi possível cadastrar o cliente."; }
    }
  });
}

async function configurarLogin() {
  const form = document.getElementById("form-login");
  if (!form) return;
  const alerta = document.getElementById("alerta-login");
  const projeto = document.getElementById("campo-projeto-login");
  const params = new URLSearchParams(window.location.search);
  if (params.get("logout") === "1") {
    usuario = null;
    storageJson(localStorage, CHAVE_LOGIN, null);
    params.delete("logout");
    window.history.replaceState({}, "", `login.html${params.toString() ? `?${params.toString()}` : ""}`);
  }
  if (usuario) return void (window.location.href = usuario.tipo === "Técnico" ? "index.html" : "cliente.html");

  try {
    const dados = await api("/projetos", {}, { semBanco: true });
    if (projeto) {
      projeto.innerHTML = (dados.projetos || []).map((item) => `<option value="${item}">${item}</option>`).join("");
      projeto.value = bancoProjetoAtivo;
      projeto.addEventListener("change", () => salvarBanco(projeto.value));
    }
  } catch {
    if (alerta) { alerta.className = "alert alert-warning"; alerta.textContent = "Não foi possível carregar a lista de projetos do servidor."; }
  }

  form.addEventListener("submit", async (evento) => {
    evento.preventDefault();
    try {
      const auth = await api("/login", {
        method: "POST",
        body: JSON.stringify({ usuario: document.getElementById("campo-usuario").value.trim(), senha: document.getElementById("campo-senha").value.trim(), banco: bancoProjetoAtivo }),
      });
      if (auth.banco) salvarBanco(auth.banco);
      usuario = { usuario: document.getElementById("campo-usuario").value.trim(), tipo: auth.tipo, clienteId: auth.clienteId };
      storageJson(localStorage, CHAVE_LOGIN, usuario);
      window.location.href = auth.redirect;
    } catch {
      if (alerta) { alerta.className = "alert alert-danger"; alerta.textContent = "Credenciais inválidas."; }
    }
  });
}

async function configurarAdmin() {
  const lista = document.getElementById("lista-projetos-admin");
  const atual = document.getElementById("banco-atual-admin");
  if (!lista || !atual) return;
  atual.textContent = bancoProjetoAtivo;
  const dados = await api("/projetos", {}, { semBanco: true });
  lista.innerHTML = "";
  (dados.projetos || []).forEach((item) => {
    const botao = document.createElement("button");
    botao.className = "list-group-item list-group-item-action d-flex justify-content-between align-items-center";
    botao.innerHTML = `<span>${item}</span><span class="badge bg-primary">Selecionar</span>`;
    botao.addEventListener("click", () => {
      salvarBanco(item);
      atual.textContent = item;
      window.location.href = usuario?.tipo === "Cliente" ? "cliente.html" : "index.html";
    });
    lista.appendChild(botao);
  });
}

async function iniciar() {
  const detalhes = document.getElementById("detalhes-chamado");
  const tecnico = document.getElementById("table-chamados");
  const cliente = document.getElementById("pagina-cliente");
  const criacao = document.getElementById("pagina-criacao");
  const cadastro = document.getElementById("pagina-cadastro-cliente");
  const admin = document.getElementById("pagina-admin");
  const protegida = detalhes || tecnico || cliente || criacao || cadastro || admin;

  usuario = storageJson(localStorage, CHAVE_LOGIN);
  overlay(false);
  await configurarLogin();

  if (protegida) {
    try { await Promise.all([carregarChamados(), carregarClientes()]); } catch { return alert(`Não foi possível carregar dados do banco '${bancoProjetoAtivo}'. Verifique o backend Python.`); }
  }
  if (!usuario && protegida) return void (window.location.href = "login.html");
  if (admin && usuario?.tipo !== "Administrador") return void (window.location.href = usuario?.tipo === "Cliente" ? "cliente.html" : "index.html");
  if (tecnico && !["Técnico", "Administrador"].includes(usuario?.tipo)) return void (window.location.href = "cliente.html");
  if (criacao && !["Técnico", "Administrador", "Cliente"].includes(usuario?.tipo)) return void (window.location.href = "cliente.html");
  if (cadastro && !["Técnico", "Administrador"].includes(usuario?.tipo)) return void (window.location.href = "cliente.html");

  if (tecnico) {
    renderTabela();
    renderAbertos();
    document.querySelectorAll(".filter-input").forEach((campo) => campo.addEventListener("input", (evento) => { filtros[evento.target.dataset.column] = evento.target.value.toLowerCase(); renderTabela(); }));
    document.querySelector(".filter-select")?.addEventListener("change", (evento) => { filtros.priority = evento.target.value; renderTabela(); });
    document.querySelectorAll('[data-filter="status"]').forEach((botao) => botao.addEventListener("click", () => { filtros.status = (botao.dataset.value || "").toLowerCase(); renderTabela(); }));
  }
  if (cliente) renderCardsCliente();
  if (criacao) {
    const aviso = document.getElementById("alerta-criacao");
    const loginPredefinido = new URLSearchParams(window.location.search).get("clienteLogin");
    if (aviso && usuario?.tipo === "Cliente") { aviso.className = "alert alert-info"; aviso.textContent = "Você pode criar chamados para o seu próprio usuário."; }
    if (loginPredefinido && document.getElementById("campo-login-cliente")) document.getElementById("campo-login-cliente").value = loginPredefinido;
    configurarCriacao();
  }
  if (cadastro) configurarCadastroCliente();
  if (admin) await configurarAdmin();
  if (detalhes) {
    const texto = document.getElementById("texto-identificacao");
    const badge = document.getElementById("badge-identificacao");
    if (texto && badge) {
      texto.textContent = usuario ? `Atualizações serão registradas como ${usuario.tipo}.` : "Nenhum usuário autenticado.";
      badge.textContent = usuario?.tipo || "-";
    }
    await telaDetalhes();
  }

  const nome = document.getElementById("nome-usuario-cabecalho");
  if (nome) nome.textContent = `Usuário: ${usuario?.usuario || "-"}`;
  document.querySelectorAll("[data-acao-admin='cadastrar-usuario']").forEach((botao) => botao.classList.toggle("d-none", usuario?.tipo !== "Administrador"));
  document.querySelectorAll("#btn-trocar-usuario, #btn-trocar-usuario-cliente").forEach((botao) => botao.addEventListener("click", (evento) => {
    evento.preventDefault();
    usuario = null;
    storageJson(localStorage, CHAVE_LOGIN, null);
    window.location.href = "login.html?logout=1";
  }));
  if (typeof BroadcastChannel !== "undefined") {
    const canal = new BroadcastChannel(CANAL_CHAMADOS);
    canal.addEventListener("message", async () => { await carregarChamados({ usarCache: false, revalidar: false }); atualizarTela(); });
  }
}

document.addEventListener("DOMContentLoaded", iniciar);
