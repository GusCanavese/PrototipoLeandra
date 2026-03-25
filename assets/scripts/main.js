const API_BASE_URL = `${window.location.origin}/api`;
const API_BASE_URLS = [API_BASE_URL];
const CANAL_ATUALIZACAO_CHAMADOS = "chamadosAtualizados";
const ID_INSTANCIA_ABA = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
const CHAVE_STORAGE_LOGIN = "usuarioAutenticado";
const CHAVE_STORAGE_BANCO = "bancoProjetoAtivo";
const CHAVE_CACHE_CHAMADOS = "cacheChamados";
const CHAVE_CACHE_CLIENTES = "cacheClientes";
const CHAVE_STORAGE_CHAMADO_ATUAL = "chamadoAtualSelecionado";
const CHAVE_STORAGE_LOGIN_PRE_CADASTRO = "loginPreCadastroCliente";
const CHAVE_STORAGE_RETORNO_CADASTRO = "rotaRetornoCadastro";
const CHAVE_STORAGE_ATIVIDADE_USUARIO = "usuarioUltimaAtividade";
const CHAVE_STORAGE_SENHA_TEMPORARIA = "senhaTemporariaPrimeiroAcesso";
const ROTA_PADRAO_POS_CADASTRO = "index.html";
const CACHE_DADOS_TTL_MS = 5 * 60 * 1000;
const TEMPO_LIMITE_INATIVIDADE_MS = 20 * 60 * 1000;
const INTERVALO_VERIFICACAO_INATIVIDADE_MS = 30 * 1000;
const TEMPO_MAXIMO_REQUISICAO_MS = 25000;
const RETRY_BACKOFF_MS = [350, 900];
const filtros = {client:"", summary:"", lastUpdate:"", openedAt:"", priority:"", status:"",};
const credenciaisLogin = {};

let chamados = [];
let clientes = [];
let usuarioAutenticado = null;
let promessaCarregamentoChamados = null;
let promessaCarregamentoClientes = null;
let operacoesPendentes = 0;
let bancoProjetoAtivo = localStorage.getItem(CHAVE_STORAGE_BANCO) || "teste";



function normalizarTipoUsuario(tipo) {
  if (tipo === "Técnico") return "Advogado";
  return tipo || "";
}

function usuarioEhAdministrador() {
  return normalizarTipoUsuario(usuarioAutenticado?.tipo) === "Administrador";
}

function usuarioEhPerfilInterno(tipo = usuarioAutenticado?.tipo) {
  return ["Advogado", "Administrador"].includes(normalizarTipoUsuario(tipo));
}

function usuarioPodeCadastrarUsuarios() {
  return usuarioEhPerfilInterno();
}

function usuarioPodeCriarTipoUsuario(tipo) {
  const tipoNormalizado = normalizarTipoUsuario(tipo);
  if (tipoNormalizado === "Advogado") return usuarioEhAdministrador();
  return usuarioPodeCadastrarUsuarios();
}

function obterRotuloTipoCadastro() {
  return usuarioEhAdministrador() ? "Cliente ou advogado" : "Cliente";
}

function configurarAlternadoresSenha() {
  document.querySelectorAll("[data-toggle-password]").forEach((botao) => {
    botao.addEventListener("click", () => {
      const seletor = botao.getAttribute("data-toggle-password");
      const campo = seletor ? document.querySelector(seletor) : null;
      if (!campo) return;
      const exibindo = campo.type === "text";
      campo.type = exibindo ? "password" : "text";
      botao.setAttribute("aria-pressed", String(!exibindo));
      botao.setAttribute("aria-label", exibindo ? "Mostrar senha" : "Ocultar senha");
      botao.innerHTML = exibindo ? "👁" : "🙈";
    });
  });
}

function obterBancoProjetoAtual() {
  return (bancoProjetoAtivo || "teste").trim() || "teste";
}

function definirBancoProjetoAtivo(nomeBanco) {
  bancoProjetoAtivo = (nomeBanco || "teste").trim();
  localStorage.setItem(CHAVE_STORAGE_BANCO, bancoProjetoAtivo);
}

function obterLoginPreCadastro() {
  return (sessionStorage.getItem(CHAVE_STORAGE_LOGIN_PRE_CADASTRO) || "").trim();
}

function limparLoginPreCadastro() {
  sessionStorage.removeItem(CHAVE_STORAGE_LOGIN_PRE_CADASTRO);
}

function obterRotaRetornoCadastro() {
  return (sessionStorage.getItem(CHAVE_STORAGE_RETORNO_CADASTRO) || "").trim();
}

function limparRotaRetornoCadastro() {
  sessionStorage.removeItem(CHAVE_STORAGE_RETORNO_CADASTRO);
}

function obterChamadoAtualSelecionado() {
  return (sessionStorage.getItem(CHAVE_STORAGE_CHAMADO_ATUAL) || "").trim();
}

function abrirDetalhesChamado(idChamado) {
  const idNormalizado = (idChamado || "").toString().trim();
  if (!idNormalizado) return;
  sessionStorage.setItem(CHAVE_STORAGE_CHAMADO_ATUAL, idNormalizado);
  window.location.href = "details.html";
}

function prepararFluxoCadastroUsuario({ login = "", retorno = ROTA_PADRAO_POS_CADASTRO } = {}) {
  const loginNormalizado = (login || "").trim().toLowerCase();
  const rotaRetorno = (retorno || ROTA_PADRAO_POS_CADASTRO).trim() || ROTA_PADRAO_POS_CADASTRO;

  if (loginNormalizado) sessionStorage.setItem(CHAVE_STORAGE_LOGIN_PRE_CADASTRO, loginNormalizado);
  else limparLoginPreCadastro();

  sessionStorage.setItem(CHAVE_STORAGE_RETORNO_CADASTRO, rotaRetorno);

  const destino = loginNormalizado
    ? `cadastro-cliente.html?login=${encodeURIComponent(loginNormalizado)}`
    : "cadastro-cliente.html";
  window.location.href = destino;
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

function escaparHtml(valor) {
  return String(valor ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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
            ...(incluirBancoNoHeader ? { "X-Project-DB": obterBancoProjetoAtual() } : {}),
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

  escreverCacheSessao(CHAVE_CACHE_CHAMADOS, dados);
}

function invalidarCacheChamados() {
  invalidarCacheSessao(CHAVE_CACHE_CHAMADOS);
}

function invalidarCacheClientes() {
  invalidarCacheSessao(CHAVE_CACHE_CLIENTES);
}

async function carregarChamadosSalvos(opcoes = {}) {
  if (promessaCarregamentoChamados) return promessaCarregamentoChamados;

  promessaCarregamentoChamados = (async () => {
  const { usarCache = true, revalidar = true } = opcoes;
  const cache = usarCache ? lerCacheChamados() : null;
  const cacheValido = cache && cache.banco === obterBancoProjetoAtual() && Date.now() - cache.timestamp < CACHE_DADOS_TTL_MS;

  if (cacheValido) {
    chamados = cache.dados;
    if (revalidar) {
      requisicaoApi("/chamados").then((dadosAtualizados) => {
          chamados = dadosAtualizados || [];
          escreverCacheChamados(chamados);
          atualizarTelaComChamadosAtualizados();
        }).catch(() => {});
    }
    return;
  }

  chamados = await requisicaoApi("/chamados");
  escreverCacheChamados(chamados);
  })();

  try {
    await promessaCarregamentoChamados;
  } finally {
    promessaCarregamentoChamados = null;
  }
}

async function carregarDetalheChamado(idChamado) {
  if (!idChamado) return null;
  const chamado = await requisicaoApi(`/chamados/${encodeURIComponent(idChamado)}`);
  return garantirFinanceiroChamado(chamado);
}

function lerCacheSessao(chave) {
  try {
    const bruto = sessionStorage.getItem(chave);
    if (!bruto) return null;
    const cache = JSON.parse(bruto);
    if (!cache?.timestamp || cache.banco !== obterBancoProjetoAtual()) return null;
    if (Date.now() - cache.timestamp >= CACHE_DADOS_TTL_MS) return null;
    return cache.dados;
  } catch {
    return null;
  }
}

function escreverCacheSessao(chave, dados) {
  sessionStorage.setItem(chave, JSON.stringify({
    timestamp: Date.now(),
    banco: obterBancoProjetoAtual(),
    dados,
  }));
}

function invalidarCacheSessao(chave) {
  sessionStorage.removeItem(chave);
}

async function carregarClientesSalvos(opcoes = {}) {
  if (promessaCarregamentoClientes) return promessaCarregamentoClientes;

  promessaCarregamentoClientes = (async () => {
    const { usarCache = true } = opcoes;
    const cache = usarCache ? lerCacheSessao(CHAVE_CACHE_CLIENTES) : null;
    if (Array.isArray(cache)) {
      clientes = cache;
      return clientes;
    }

    clientes = await requisicaoApi("/clientes");
    escreverCacheSessao(CHAVE_CACHE_CLIENTES, clientes);
    return clientes;
  })();

  try {
    return await promessaCarregamentoClientes;
  } finally {
    promessaCarregamentoClientes = null;
  }
}

async function salvarClientes() {
  await requisicaoApi("/clientes", {
    method: "PUT",
    body: JSON.stringify(clientes),
  });
  escreverCacheSessao(CHAVE_CACHE_CLIENTES, clientes);
}

async function salvarClienteIndividual(cliente) {
  await requisicaoApi("/clientes", {
    method: "POST",
    body: JSON.stringify(cliente),
  });
  invalidarCacheClientes();
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
  const chamadosNormalizados = chamadosAtualizados.map((chamado) => garantirFinanceiroChamado(chamado));
  try {
    await requisicaoApi("/chamados", {
      method: "PUT",
      body: JSON.stringify(chamadosNormalizados),
    });
  } catch (erro) {
    if (!erroDeQuotaStorage(erro)) throw erro;

    const chamadosCompactados = removerConteudoAnexos(chamadosNormalizados);
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
    canal.postMessage({ atualizadoEm: Date.now(), origem: ID_INSTANCIA_ABA });
    canal.close();
  }
}

async function salvarChamadoIndividual(chamado) {
  const chamadoNormalizado = garantirFinanceiroChamado(chamado);
  await requisicaoApi(`/chamados/${encodeURIComponent(chamado.id)}`, {
    method: "PUT",
    body: JSON.stringify(chamadoNormalizado),
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

function criarParcelasFinanceiras(totalParcelas, parcelasExistentes = []) {
  const quantidade = Math.max(1, parseInt(totalParcelas, 10) || 1);
  return Array.from({ length: quantidade }, (_, indice) => Boolean(parcelasExistentes[indice]));
}

function normalizarDataPagamento(valor) {
  if (!valor) return "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(String(valor))) return String(valor);
  const data = new Date(valor);
  if (Number.isNaN(data.getTime())) return "";
  const ano = data.getFullYear();
  const mes = String(data.getMonth() + 1).padStart(2, "0");
  const dia = String(data.getDate()).padStart(2, "0");
  return `${ano}-${mes}-${dia}`;
}

function adicionarMesesDataPagamento(dataBase, quantidadeMeses) {
  const dataNormalizada = normalizarDataPagamento(dataBase);
  if (!dataNormalizada) return "";
  const [ano, mes, dia] = dataNormalizada.split("-").map(Number);
  const data = new Date(ano, mes - 1, dia);
  data.setMonth(data.getMonth() + quantidadeMeses);
  const novoAno = data.getFullYear();
  const novoMes = String(data.getMonth() + 1).padStart(2, "0");
  const novoDia = String(data.getDate()).padStart(2, "0");
  return `${novoAno}-${novoMes}-${novoDia}`;
}

function criarDatasParcelas(totalParcelas, datasExistentes = [], primeiraData = "") {
  const quantidade = Math.max(1, parseInt(totalParcelas, 10) || 1);
  const datasNormalizadas = Array.isArray(datasExistentes)
    ? datasExistentes.map((data) => normalizarDataPagamento(data))
    : [];
  const primeiraDataNormalizada = normalizarDataPagamento(primeiraData) || datasNormalizadas[0] || "";

  return Array.from({ length: quantidade }, (_, indice) => {
    if (datasNormalizadas[indice]) return datasNormalizadas[indice];
    if (!primeiraDataNormalizada) return "";
    return adicionarMesesDataPagamento(primeiraDataNormalizada, indice);
  });
}

function formatarDataPagamento(valor) {
  const dataNormalizada = normalizarDataPagamento(valor);
  if (!dataNormalizada) return "Sem data";
  const [ano, mes, dia] = dataNormalizada.split("-");
  return `${dia}/${mes}/${ano}`;
}

function parcelaEstaVencida(dataPagamento, paga) {
  if (paga) return false;
  const dataNormalizada = normalizarDataPagamento(dataPagamento);
  if (!dataNormalizada) return false;
  const hoje = new Date();
  hoje.setHours(0, 0, 0, 0);
  const [ano, mes, dia] = dataNormalizada.split("-").map(Number);
  const vencimento = new Date(ano, mes - 1, dia);
  vencimento.setHours(0, 0, 0, 0);
  return vencimento < hoje;
}

function obterClasseStatusParcela(dataPagamento, paga) {
  if (paga) return "financeiro-badge-pago";
  if (parcelaEstaVencida(dataPagamento, paga)) return "financeiro-badge-pendente";
  return "financeiro-badge-neutro";
}

function obterTextoStatusParcela(dataPagamento, paga) {
  if (paga) return "Paga";
  if (parcelaEstaVencida(dataPagamento, paga)) return "Vencida";
  return "Agendada";
}

function normalizarItemFinanceiro(item = {}, indice = 0) {
  const totalParcelas = Math.max(1, parseInt(item.installments ?? item.parcelas, 10) || 1);
  const valor = Number(item.value ?? item.valor ?? 0);
  return {
    id: item.id || `financeiro-${Date.now()}-${indice}-${Math.random().toString(36).slice(2, 8)}`,
    product: String(item.product ?? item.produto ?? "").trim(),
    value: Number.isFinite(valor) ? valor : 0,
    installments: totalParcelas,
    description: String(item.description ?? item.descricao ?? "").trim(),
    paidInstallments: criarParcelasFinanceiras(totalParcelas, item.paidInstallments ?? item.parcelasPagas ?? []),
    installmentDates: criarDatasParcelas(totalParcelas, item.installmentDates ?? item.datasParcelas ?? [], item.firstInstallmentDate ?? item.dataPrimeiraParcela ?? ""),
  };
}

function normalizarFinanceiroColecao(itens = []) {
  if (!Array.isArray(itens)) return [];
  return itens.map((item, indice) => normalizarItemFinanceiro(item, indice)).filter((item) => item.product);
}

function garantirFinanceiroChamado(chamado) {
  if (!chamado || typeof chamado !== "object") return chamado;
  chamado.financialOffice = normalizarFinanceiroColecao(chamado.financialOffice ?? chamado.financeiro_escritorio);
  chamado.financialClient = normalizarFinanceiroColecao(chamado.financialClient ?? chamado.financeiro_cliente);
  return chamado;
}

function formatarMoeda(valor) {
  const numero = Number(valor) || 0;
  return numero.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function calcularValorRestanteFinanceiro(item) {
  if (!item) return 0;
  const totalParcelas = Math.max(1, item.installments || 1);
  const valorParcela = (Number(item.value) || 0) / totalParcelas;
  const pagas = (item.paidInstallments || []).filter(Boolean).length;
  return Math.max(0, (totalParcelas - pagas) * valorParcela);
}

function calcularValorParcelaFinanceira(item) {
  if (!item) return 0;
  const totalParcelas = Math.max(1, item.installments || 1);
  return (Number(item.value) || 0) / totalParcelas;
}

function calcularValorRestanteTotalFinanceiro(itens = []) {
  return (itens || []).reduce((total, item) => total + calcularValorRestanteFinanceiro(item), 0);
}

function obterTituloFinanceiro(tipo) {
  return tipo === "escritorio" ? "Pago pelo Escritório" : "Pago pelo Cliente";
}

function obterColecaoFinanceira(chamado, tipo) {
  garantirFinanceiroChamado(chamado);
  return tipo === "escritorio" ? chamado.financialOffice : chamado.financialClient;
}

function usuarioPodeGerenciarFinanceiro() {
  return usuarioEhPerfilInterno(usuarioAutenticado?.tipo);
}

function obterRotuloEscopoFinanceiro(tipo) {
  return tipo === "escritorio" ? "Pago pelo Escritório" : "Pago pelo Cliente";
}

function obterMensagemEventoFinanceiro(acao) {
  if (acao === "update") return "Registro financeiro atualizado.";
  if (acao === "delete") return "Registro financeiro removido.";
  return "Registro financeiro adicionado.";
}

function criarAtualizacaoFinanceira(tipo, item, acao = "create") {
  if (!item) return null;
  return {
    author: "Sistema",
    message: obterMensagemEventoFinanceiro(acao),
    date: formatarDataHoraAtual(),
    attachments: [],
    financialEvent: {
      action: acao,
      scope: tipo,
      product: item.product || "",
      value: Number(item.value) || 0,
      installments: Math.max(1, Number(item.installments) || 1),
    },
  };
}

function obterResumoParcelas(item) {
  const totalParcelas = Math.max(1, item.installments || 1);
  const pagas = (item.paidInstallments || []).filter(Boolean).length;
  return `${pagas}/${totalParcelas} parcela(s) paga(s)`;
}

function abrirModalListaFinanceira(chamado, tipo) {
  const modalElemento = document.getElementById("modal-financeiro-lista");
  const titulo = document.getElementById("titulo-modal-financeiro-lista");
  const conteudo = document.getElementById("conteudo-modal-financeiro-lista");
  if (!modalElemento || !titulo || !conteudo || typeof bootstrap === "undefined") return;

  const itens = obterColecaoFinanceira(chamado, tipo);
  titulo.textContent = obterTituloFinanceiro(tipo);
  conteudo.innerHTML = itens.length
    ? itens
        .map((item) => {
          const parcelasHtml = (item.paidInstallments || [])
            .map((paga, indice) => {
              const dataParcela = item.installmentDates?.[indice] || "";
              const classe = obterClasseStatusParcela(dataParcela, paga);
              const texto = obterTextoStatusParcela(dataParcela, paga);
              return `<span class="badge ${classe} me-2 mb-2">Parcela ${indice + 1} - ${formatarDataPagamento(dataParcela)}: ${texto}</span>`;
            })
            .join("");
          return `
            <div class="financeiro-lista-item">
              <div class="d-flex justify-content-between align-items-start gap-3 flex-wrap">
                <div>
                  <h3 class="h6 mb-1">${item.product}</h3>
                  <p class="mb-1"><strong>Valor:</strong> ${formatarMoeda(item.value)}</p>
                  <p class="mb-1"><strong>Parcelas:</strong> ${item.installments}</p>
                  <p class="mb-0"><strong>Valor restante:</strong> ${formatarMoeda(calcularValorRestanteFinanceiro(item))}</p>
                </div>
                <span class="badge bg-light text-dark border">${obterResumoParcelas(item)}</span>
              </div>
              <p class="text-muted mt-3 mb-2">${item.description || "Sem descrição."}</p>
              <div>${parcelasHtml}</div>
            </div>
          `;
        })
        .join("")
    : '<div class="alert alert-secondary mb-0">Nenhum produto registrado nesta seção.</div>';

  bootstrap.Modal.getOrCreateInstance(modalElemento).show();
}

function configurarPainelFinanceiro(chamado) {
  garantirFinanceiroChamado(chamado);
  const podeGerenciarFinanceiro = usuarioPodeGerenciarFinanceiro();
  const cardFinanceiroEscritorio = document.getElementById("card-financeiro-escritorio");
  const cardFinanceiroCliente = document.getElementById("card-financeiro-cliente");

  if (cardFinanceiroEscritorio) cardFinanceiroEscritorio.classList.toggle("d-none", usuarioAutenticado?.tipo === "Cliente");
  if (cardFinanceiroCliente) cardFinanceiroCliente.classList.remove("d-none");

  const modalCadastroElemento = document.getElementById("modal-financeiro");
  const modalListaElemento = document.getElementById("modal-financeiro-lista");
  const formFinanceiro = document.getElementById("form-financeiro");
  if (!formFinanceiro || !modalCadastroElemento || !modalListaElemento || typeof bootstrap === "undefined") return;

  const modalCadastro = bootstrap.Modal.getOrCreateInstance(modalCadastroElemento);
  bootstrap.Modal.getOrCreateInstance(modalListaElemento);

  const tituloModal = document.getElementById("titulo-modal-financeiro");
  const campoProduto = document.getElementById("modal-financeiro-produto");
  const campoValor = document.getElementById("modal-financeiro-valor");
  const campoParcelas = document.getElementById("modal-financeiro-parcelas");
  const campoDescricao = document.getElementById("modal-financeiro-descricao");
  const botaoSalvarFinanceiro = document.getElementById("btn-salvar-financeiro");
  const botaoExcluirFinanceiro = document.getElementById("btn-excluir-financeiro");
  const selecoes = {
    escritorio: { produtoId: "", parcelaIndice: "" },
    cliente: { produtoId: "", parcelaIndice: "" },
  };
  let tipoModalAtual = "escritorio";
  let modoModalFinanceiro = "adicionar";
  let itemEdicaoAtualId = "";

  function obterElementosTipo(tipo) {
    return {
      produto: document.getElementById(`financeiro-${tipo}-produto`),
      valor: document.getElementById(`financeiro-${tipo}-valor`),
      parcela: document.getElementById(`financeiro-${tipo}-parcela`),
      parcelaPaga: document.getElementById(`financeiro-${tipo}-parcela-paga`),
      descricao: document.getElementById(`financeiro-${tipo}-descricao`),
      restante: document.getElementById(`financeiro-${tipo}-restante`),
      botaoEditar: document.getElementById(`btn-editar-financeiro-${tipo}`),
    };
  }

  function obterItemSelecionado(tipo) {
    const itens = obterColecaoFinanceira(chamado, tipo);
    if (!itens.length) return null;
    const selecaoAtual = selecoes[tipo].produtoId;
    return itens.find((item) => item.id === selecaoAtual) || itens[0];
  }

  function renderizarTipo(tipo) {
    const elementos = obterElementosTipo(tipo);
    if (!elementos.produto) return;

    const itens = obterColecaoFinanceira(chamado, tipo);
    const itemSelecionado = obterItemSelecionado(tipo);
    const produtoIdSelecionado = itemSelecionado?.id || "";
    selecoes[tipo].produtoId = produtoIdSelecionado;

    elementos.produto.innerHTML = itens.length
      ? itens.map((item) => `<option value="${item.id}">${item.product}</option>`).join("")
      : '<option value="">Nenhum produto cadastrado</option>';
    elementos.produto.value = produtoIdSelecionado;
    elementos.produto.disabled = !itens.length || !podeGerenciarFinanceiro;
    if (elementos.botaoEditar) elementos.botaoEditar.disabled = !itemSelecionado || !podeGerenciarFinanceiro;

    if (!itemSelecionado) {
      elementos.valor.textContent = formatarMoeda(0);
      elementos.parcela.innerHTML = '<option value="">Selecione</option>';
      elementos.parcela.disabled = true;
      elementos.parcelaPaga.checked = false;
      elementos.parcelaPaga.disabled = true;
      elementos.descricao.value = "";
      elementos.descricao.disabled = true;
      elementos.restante.textContent = formatarMoeda(calcularValorRestanteTotalFinanceiro(itens));
      return;
    }

    const parcelaIndiceAtual = selecoes[tipo].parcelaIndice === ""
      ? "0"
      : String(Math.min(parseInt(selecoes[tipo].parcelaIndice, 10) || 0, itemSelecionado.installments - 1));

    selecoes[tipo].parcelaIndice = parcelaIndiceAtual;
    elementos.parcela.innerHTML = Array.from({ length: itemSelecionado.installments }, (_, indice) => {
      const dataParcela = itemSelecionado.installmentDates?.[indice] || "";
      const status = obterTextoStatusParcela(dataParcela, itemSelecionado.paidInstallments?.[indice]).toLowerCase();
      return `<option value="${indice}">Parcela ${indice + 1} - ${formatarDataPagamento(dataParcela)} (${status})</option>`;
    }).join("");
    elementos.parcela.disabled = !podeGerenciarFinanceiro;
    elementos.parcela.value = parcelaIndiceAtual;
    elementos.parcelaPaga.disabled = !podeGerenciarFinanceiro;
    elementos.parcelaPaga.checked = Boolean(itemSelecionado.paidInstallments?.[parseInt(parcelaIndiceAtual, 10) || 0]);
    elementos.valor.textContent = formatarMoeda(calcularValorParcelaFinanceira(itemSelecionado));
    elementos.descricao.value = itemSelecionado.description || "";
    elementos.descricao.disabled = !podeGerenciarFinanceiro;
    elementos.descricao.readOnly = !podeGerenciarFinanceiro;
    elementos.restante.textContent = formatarMoeda(calcularValorRestanteTotalFinanceiro(itens));
  }

  async function persistirFinanceiro() {
    chamado.lastUpdate = formatarDataHoraAtual();
    await salvarChamadoIndividual(chamado);
    preencherCabecalhoChamado(chamado);
  }

  function preencherFormularioFinanceiro(item) {
    campoProduto.value = item?.product || "";
    campoValor.value = item ? String(Number(item.value) || 0) : "";
    campoParcelas.value = item ? String(Math.max(1, item.installments || 1)) : "1";
    campoDescricao.value = item?.description || "";
  }

  function configurarModalFinanceiro(modo, tipo, item = null) {
    tipoModalAtual = tipo;
    modoModalFinanceiro = modo;
    itemEdicaoAtualId = item?.id || "";

    if (modo === "editar" && item) {
      tituloModal.textContent = `Editar item - ${obterTituloFinanceiro(tipo)}`;
      if (botaoSalvarFinanceiro) botaoSalvarFinanceiro.textContent = "Salvar alterações";
      if (botaoExcluirFinanceiro) botaoExcluirFinanceiro.classList.remove("d-none");
      preencherFormularioFinanceiro(item);
      return;
    }

    tituloModal.textContent = `Adicionar item - ${obterTituloFinanceiro(tipo)}`;
    if (botaoSalvarFinanceiro) botaoSalvarFinanceiro.textContent = "Salvar produto";
    if (botaoExcluirFinanceiro) botaoExcluirFinanceiro.classList.add("d-none");
    formFinanceiro.reset();
    campoParcelas.value = "1";
    itemEdicaoAtualId = "";
  }

  ["escritorio", "cliente"].forEach((tipo) => {
    const elementos = obterElementosTipo(tipo);
    const botaoAdicionar = document.getElementById(`btn-adicionar-financeiro-${tipo}`);
    const botaoEditar = document.getElementById(`btn-editar-financeiro-${tipo}`);

    if (botaoAdicionar) {
      botaoAdicionar.disabled = !podeGerenciarFinanceiro;
      botaoAdicionar.classList.toggle("d-none", !podeGerenciarFinanceiro);
    }
    if (botaoEditar) {
      botaoEditar.disabled = !podeGerenciarFinanceiro;
      botaoEditar.classList.toggle("d-none", !podeGerenciarFinanceiro);
    }
    botaoAdicionar.onclick = () => {
      if (!podeGerenciarFinanceiro) return;
      configurarModalFinanceiro("adicionar", tipo);
      modalCadastro.show();
    };

    botaoEditar.onclick = () => {
      if (!podeGerenciarFinanceiro) return;
      const item = obterItemSelecionado(tipo);
      if (!item) {
        alert("Selecione ou cadastre um produto antes de editar.");
        return;
      }
      configurarModalFinanceiro("editar", tipo, item);
      modalCadastro.show();
    };

    document.getElementById(`btn-ver-mais-${tipo}`).onclick = () => {
      abrirModalListaFinanceira(chamado, tipo);
    };

    elementos.produto.onchange = () => {
      if (!podeGerenciarFinanceiro) return;
      selecoes[tipo].produtoId = elementos.produto.value;
      selecoes[tipo].parcelaIndice = "0";
      renderizarTipo(tipo);
    };

    elementos.parcela.onchange = () => {
      if (!podeGerenciarFinanceiro) return;
      selecoes[tipo].parcelaIndice = elementos.parcela.value;
      renderizarTipo(tipo);
    };

    elementos.parcelaPaga.onchange = async () => {
      if (!podeGerenciarFinanceiro) return;
      const item = obterItemSelecionado(tipo);
      const indiceParcela = parseInt(selecoes[tipo].parcelaIndice, 10);
      if (!item || Number.isNaN(indiceParcela)) {
        elementos.parcelaPaga.checked = false;
        return;
      }
      item.paidInstallments = criarParcelasFinanceiras(item.installments, item.paidInstallments);
      item.paidInstallments[indiceParcela] = elementos.parcelaPaga.checked;
      try {
        await persistirFinanceiro();
      } catch (erro) {
        item.paidInstallments[indiceParcela] = !elementos.parcelaPaga.checked;
        renderizarTipo(tipo);
        alert(erro.message || "Não foi possível atualizar o pagamento da parcela.");
        return;
      }
      renderizarTipo(tipo);
    };

    elementos.descricao.onblur = async () => {
      if (!podeGerenciarFinanceiro) return;
      const item = obterItemSelecionado(tipo);
      if (!item) return;
      const novaDescricao = elementos.descricao.value.trim();
      if ((item.description || "") === novaDescricao) return;

      const descricaoAnterior = item.description || "";
      item.description = novaDescricao;
      try {
        await persistirFinanceiro();
      } catch (erro) {
        item.description = descricaoAnterior;
        renderizarTipo(tipo);
        alert(erro.message || "Não foi possível salvar a descrição do produto.");
        return;
      }
      renderizarTipo(tipo);
    };

    renderizarTipo(tipo);
  });

  formFinanceiro.onsubmit = async (evento) => {
    evento.preventDefault();
    if (!podeGerenciarFinanceiro) return;
    const produto = campoProduto.value.trim();
    const valor = Number(campoValor.value);
    const parcelas = parseInt(campoParcelas.value, 10);
    const descricao = campoDescricao.value.trim();

    if (!produto || !Number.isFinite(valor) || valor < 0 || !Number.isFinite(parcelas) || parcelas < 1) return;

    const colecao = obterColecaoFinanceira(chamado, tipoModalAtual);
    const itemExistente = modoModalFinanceiro === "editar"
      ? colecao.find((itemAtual) => itemAtual.id === itemEdicaoAtualId)
      : null;

    let item = itemExistente;
    let snapshotAnterior = null;
    let atualizacaoFinanceira = null;

    if (itemExistente) {
      snapshotAnterior = {
        product: itemExistente.product,
        value: itemExistente.value,
        installments: itemExistente.installments,
        description: itemExistente.description,
        paidInstallments: [...(itemExistente.paidInstallments || [])],
        installmentDates: [...(itemExistente.installmentDates || [])],
      };
      itemExistente.product = produto;
      itemExistente.value = valor;
      itemExistente.installments = parcelas;
      itemExistente.description = descricao;
      itemExistente.paidInstallments = criarParcelasFinanceiras(parcelas, itemExistente.paidInstallments);
      itemExistente.installmentDates = criarDatasParcelas(parcelas, itemExistente.installmentDates);
      atualizacaoFinanceira = criarAtualizacaoFinanceira(tipoModalAtual, itemExistente, "update");
    } else {
      item = normalizarItemFinanceiro({
        id: `financeiro-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        product: produto,
        value: valor,
        installments: parcelas,
        description: descricao,
        paidInstallments: Array.from({ length: parcelas }, () => false),
        installmentDates: criarDatasParcelas(parcelas),
      });
      colecao.push(item);
      atualizacaoFinanceira = criarAtualizacaoFinanceira(tipoModalAtual, item, "create");
    }

    selecoes[tipoModalAtual].produtoId = item.id;
    selecoes[tipoModalAtual].parcelaIndice = "0";
    if (atualizacaoFinanceira) {
      chamado.updates = chamado.updates || [];
      chamado.updates.unshift(atualizacaoFinanceira);
    }

    try {
      await persistirFinanceiro();
    } catch (erro) {
      if (atualizacaoFinanceira) chamado.updates.shift();
      if (itemExistente && snapshotAnterior) {
        itemExistente.product = snapshotAnterior.product;
        itemExistente.value = snapshotAnterior.value;
        itemExistente.installments = snapshotAnterior.installments;
        itemExistente.description = snapshotAnterior.description;
        itemExistente.paidInstallments = snapshotAnterior.paidInstallments;
        itemExistente.installmentDates = snapshotAnterior.installmentDates;
      } else {
        const indice = colecao.findIndex((registro) => registro.id === item.id);
        if (indice >= 0) colecao.splice(indice, 1);
      }
      alert(erro.message || "Não foi possível salvar o item financeiro.");
      return;
    }

    renderizarTipo(tipoModalAtual);
    modalCadastro.hide();
    formFinanceiro.reset();
  };

  if (botaoExcluirFinanceiro) {
    botaoExcluirFinanceiro.onclick = async () => {
      if (!podeGerenciarFinanceiro) return;
      if (modoModalFinanceiro !== "editar" || !itemEdicaoAtualId) return;

      const colecao = obterColecaoFinanceira(chamado, tipoModalAtual);
      const indice = colecao.findIndex((itemAtual) => itemAtual.id === itemEdicaoAtualId);
      if (indice < 0) return;
      if (!window.confirm("Deseja excluir este item financeiro?")) return;

      const [itemRemovido] = colecao.splice(indice, 1);
      const proximoItem = colecao[0] || null;
      const atualizacaoFinanceira = criarAtualizacaoFinanceira(tipoModalAtual, itemRemovido, "delete");
      selecoes[tipoModalAtual].produtoId = proximoItem?.id || "";
      selecoes[tipoModalAtual].parcelaIndice = proximoItem ? "0" : "";
      if (atualizacaoFinanceira) {
        chamado.updates = chamado.updates || [];
        chamado.updates.unshift(atualizacaoFinanceira);
      }

      try {
        await persistirFinanceiro();
      } catch (erro) {
        if (atualizacaoFinanceira) chamado.updates.shift();
        colecao.splice(indice, 0, itemRemovido);
        selecoes[tipoModalAtual].produtoId = itemRemovido.id;
        selecoes[tipoModalAtual].parcelaIndice = "0";
        renderizarTipo(tipoModalAtual);
        alert(erro.message || "Não foi possível excluir o item financeiro.");
        return;
      }

      renderizarTipo(tipoModalAtual);
      modalCadastro.hide();
      formFinanceiro.reset();
    };
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
  usuarioAutenticado = { ...usuario, tipo: normalizarTipoUsuario(usuario?.tipo) };
  localStorage.setItem(CHAVE_STORAGE_LOGIN, JSON.stringify(usuarioAutenticado));
}

function limparAutenticacao() {
  usuarioAutenticado = null;
  localStorage.removeItem(CHAVE_STORAGE_LOGIN);
  localStorage.removeItem(CHAVE_STORAGE_ATIVIDADE_USUARIO);
  sessionStorage.removeItem(CHAVE_STORAGE_SENHA_TEMPORARIA);
}

function registrarAtividadeUsuario() {
  if (!usuarioAutenticado) return;
  localStorage.setItem(CHAVE_STORAGE_ATIVIDADE_USUARIO, String(Date.now()));
}

function obterUltimaAtividadeUsuario() {
  const valor = Number(localStorage.getItem(CHAVE_STORAGE_ATIVIDADE_USUARIO));
  return Number.isFinite(valor) && valor > 0 ? valor : 0;
}

function sessaoExpiradaPorInatividade() {
  if (!usuarioAutenticado) return false;
  const ultimaAtividade = obterUltimaAtividadeUsuario();
  if (!ultimaAtividade) return false;
  return (Date.now() - ultimaAtividade) > TEMPO_LIMITE_INATIVIDADE_MS;
}

function encerrarSessaoPorInatividade() {
  if (!usuarioAutenticado) return;
  limparAutenticacao();
  redirecionarParaLogin(true, { sessaoExpirada: true });
}

function definirUsuarioAutenticadoSeSalvo() {
  if (!usuarioAutenticado) {
    const salvo = obterUsuarioSalvo();
    if (salvo) usuarioAutenticado = { ...salvo, tipo: normalizarTipoUsuario(salvo.tipo) };
  }
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

  chamados.filter((chamado) => {
      const atendeCliente = chamado.client.toLowerCase().includes(filtros.client);
      const atendeResumo = chamado.summary.toLowerCase().includes(filtros.summary);
      const atendeUltima = chamado.lastUpdate.toLowerCase().includes(filtros.lastUpdate);
      const atendeAbertura = chamado.openedAt.toLowerCase().includes(filtros.openedAt);
      const atendeStatus = chamado.status.toLowerCase().includes(filtros.status);
      const atendePrioridade = !filtros.priority || chamado.priority === filtros.priority;
      return (atendeCliente && atendeResumo && atendeUltima && atendeAbertura && atendeStatus && atendePrioridade);
    }).forEach((chamado) => {
      const linha = document.createElement("tr");
      linha.innerHTML = `
        <td><div class="fw-semibold">${chamado.client}</div><div class="text-muted small">${chamado.id}</div></td>
        <td>${chamado.summary}</td>
        <td><span class="badge bg-light text-dark border">${chamado.status}</span></td>
        <td>${chamado.lastUpdate}</td>
        <td>${chamado.openedAt}</td>
        <td class="text-center"></td>
        <td class="text-end"><button type="button" class="btn btn-sm btn-primary" data-abrir-chamado="${escaparHtml(chamado.id)}">Ver</button></td>
      `;
      linha.querySelector("td:nth-child(6)").appendChild(createPriorityBadge(chamado.priority));
      linha.querySelector("[data-abrir-chamado]")?.addEventListener("click", () => abrirDetalhesChamado(chamado.id));
      corpoTabela.appendChild(linha);
    });
}

function renderChamadosClienteAbertos() {
  const lista = document.getElementById("lista-chamados-cliente");
  if (!lista) return;
  lista.innerHTML = "";

  const usuarioCliente = (usuarioAutenticado?.usuario || "").toLowerCase();
  const chamadosCliente = chamados.filter((c) => (c.clienteLogin || "").toLowerCase() === usuarioCliente);
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
          <button type="button" class="btn btn-primary btn-sm mt-auto" data-abrir-chamado="${escaparHtml(chamado.id)}">Abrir chamado</button>
        </div>
      </div>`;
    item.querySelector("[data-abrir-chamado]")?.addEventListener("click", () => abrirDetalhesChamado(chamado.id));
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
            <button type="button" class="btn btn-primary btn-sm" data-abrir-chamado="${escaparHtml(chamado.id)}">Ver</button>
          </div>
        </div>
      </div>`;
    coluna.querySelector(".container-prioridade-card").appendChild(createPriorityBadge(chamado.priority));
    coluna.querySelector("[data-abrir-chamado]")?.addEventListener("click", () => abrirDetalhesChamado(chamado.id));
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
      <div class="d-flex flex-column align-items-end gap-1 ms-auto text-end">
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

  const atualizacoesVisiveis = (chamado.updates || []).filter((u) => {
    if (usuarioAutenticado?.tipo !== "Cliente") return true;
    return u?.financialEvent?.scope !== "escritorio";
  });

  atualizacoesVisiveis.forEach((u) => {
    const anexos = (u.attachments || []).length
      ? `<div class="small mt-2"><strong>Anexos:</strong> ${renderizarAnexosComDownload(u.attachments || [])}</div>`
      : "";
    const item = document.createElement("div");
    item.className = "timeline-item";
    item.innerHTML = `<div class="d-flex justify-content-between"><strong>${u.author}</strong><span class="small text-muted">${u.date}</span></div><p class="mb-1">${u.message}</p>${anexos}`;
    listaHistorico.appendChild(item);
  });
}

function configurarImpressaoHistorico(chamado) {
  const botaoImprimir = document.getElementById("btn-imprimir-historico");
  if (!botaoImprimir) return;

  const podeImprimir = usuarioEhPerfilInterno(usuarioAutenticado?.tipo);
  botaoImprimir.classList.toggle("d-none", !podeImprimir);
  if (!podeImprimir) {
    botaoImprimir.onclick = null;
    return;
  }

  botaoImprimir.onclick = () => {
    const ultimasAtualizacoes = (chamado.updates || []).slice(0, 10);
    const itensHistorico = ultimasAtualizacoes.length
      ? ultimasAtualizacoes
          .map((atualizacao, indice) => {
            const anexos = (atualizacao.attachments || [])
              .map(normalizarAnexo)
              .filter(Boolean);
            const eventoFinanceiro = atualizacao.financialEvent;
            const anexosHtml = anexos.length
              ? `
                <div class="anexos">
                  <strong>Anexos:</strong> ${anexos.map((anexo) => escaparHtml(anexo.name)).join(", ")}
                </div>
              `
              : "";
            const financeiroHtml = eventoFinanceiro
              ? `
                <div class="anexos">
                  <strong>Registro financeiro:</strong> ${escaparHtml(obterRotuloEscopoFinanceiro(eventoFinanceiro.scope || "cliente"))}<br />
                  <strong>Produto:</strong> ${escaparHtml(eventoFinanceiro.product || "-")}<br />
                  <strong>Valor:</strong> ${escaparHtml(formatarMoeda(eventoFinanceiro.value || 0))}<br />
                  <strong>Parcelas:</strong> ${escaparHtml(String(eventoFinanceiro.installments || 1))}
                </div>
              `
              : "";

            return `
              <section class="item">
                <div class="item-topo">
                  <strong>${escaparHtml(atualizacao.author || "Sistema")}</strong>
                  <span>${escaparHtml(atualizacao.date || "-")}</span>
                </div>
                <div>${escaparHtml(atualizacao.message || "")}</div>
                ${anexosHtml}
                ${financeiroHtml}
                <small>Atualização ${indice + 1}</small>
              </section>
            `;
          })
          .join("")
      : '<p>Nenhuma atualização registrada.</p>';

    const janelaImpressao = window.open("", "_blank", "width=900,height=700");
    if (!janelaImpressao) {
      alert("Não foi possível abrir a janela de impressão.");
      return;
    }

    const htmlRelatorio = `
      <!doctype html>
      <html lang="pt-BR">
        <head>
          <meta charset="UTF-8" />
          <title>Relatório do Chamado ${escaparHtml(chamado.id || "")}</title>
          <style>
            body {
              font-family: Arial, sans-serif;
              color: #1f2937;
              margin: 32px;
              line-height: 1.5;
            }
            h1, h2, p {
              margin: 0 0 12px;
            }
            .meta {
              margin-bottom: 24px;
            }
            .item {
              border: 1px solid #d1d5db;
              border-radius: 8px;
              padding: 12px 14px;
              margin-bottom: 12px;
              page-break-inside: avoid;
            }
            .item-topo {
              display: flex;
              justify-content: space-between;
              gap: 12px;
              margin-bottom: 8px;
              font-size: 14px;
            }
            small {
              display: block;
              margin-top: 10px;
              color: #6b7280;
            }
            .anexos {
              margin-top: 10px;
              font-size: 14px;
            }
            @media print {
              body {
                margin: 18px;
              }
            }
          </style>
        </head>
        <body>
          <h1>Relatório de Atualizações</h1>
          <div class="meta">
            <p><strong>Chamado:</strong> ${escaparHtml(chamado.id || "-")}</p>
            <p><strong>Cliente:</strong> ${escaparHtml(chamado.client || "-")}</p>
            <p><strong>Resumo:</strong> ${escaparHtml(chamado.summary || "-")}</p>
            <p><strong>Status:</strong> ${escaparHtml(chamado.status || "-")}</p>
            <p><strong>Gerado em:</strong> ${escaparHtml(formatarDataHoraAtual())}</p>
            <p><strong>Conteúdo:</strong> 10 últimas atualizações</p>
          </div>
          <h2>Histórico</h2>
          ${itensHistorico}
        </body>
      </html>
    `;
    janelaImpressao.document.open();
    janelaImpressao.document.write(htmlRelatorio);
    janelaImpressao.document.close();
    janelaImpressao.focus();
    janelaImpressao.onload = () => {
      janelaImpressao.print();
    };
  };
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

  form.onsubmit = async (evento) => {
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
      author: usuarioAutenticado?.tipo || "Advogado",
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
  };

  if (btnConcluir) btnConcluir.onclick = async () => {
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
  };

  if (btnExcluir) btnExcluir.onclick = async () => {
    try {
      await excluirChamadoIndividual(chamado.id);
    } catch (erro) {
      alert(erro.message || "Não foi possível excluir o chamado.");
      return;
    }
    window.location.href = normalizarTipoUsuario(usuarioAutenticado?.tipo) === "Cliente" ? "cliente.html" : "index.html";
  };
}

function gerarNovoIdChamado() {
  if (!chamados || chamados.length === 0) {
    return "C-1";
  }
  const maiorIdAtual = chamados.reduce((maior, chamadoAtual) => {
    const correspondencia = (chamadoAtual.id || "").match(/(\d+)/);
    const numeroAtual = correspondencia ? parseInt(correspondencia[1], 10) : 0;
    return Math.max(maior, numeroAtual);
  }, 0);
  return `C-${maiorIdAtual + 1}`;
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
  const campoValorInicial = document.getElementById("campo-valor-inicial");
  const campoParcelasIniciais = document.getElementById("campo-parcelas-iniciais");
  const campoPrimeiraParcela = document.getElementById("campo-primeira-parcela");
  const alertaCriacao = document.getElementById("alerta-criacao");
  const botaoCadastrarCliente = document.getElementById("btn-cadastrar-cliente");
  const usuarioEhCliente = usuarioAutenticado?.tipo === "Cliente";

  if (usuarioEhCliente) {
    const clienteAtual = obterClientePorLogin(usuarioAutenticado?.clienteId || "");
    campoLoginCliente.value = usuarioAutenticado?.clienteId || "";
    campoLoginCliente.readOnly = true;
    if (clienteAtual?.nomeCompleto) {
      campoCliente.value = clienteAtual.nomeCompleto;
      campoCliente.readOnly = true;
    }
    botaoCadastrarCliente?.classList.add("d-none");
    if (alertaCriacao) {
      alertaCriacao.className = "alert alert-info";
      alertaCriacao.textContent = "Você está abrindo chamado para o seu próprio usuário.";
    }
  }

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
    botaoCadastrarCliente.onclick = () => prepararFluxoCadastroUsuario({ login: loginInformado, retorno: "index.html" });
  }

  if (!usuarioEhCliente) campoLoginCliente?.addEventListener("blur", validarClienteExistente);
  if (!usuarioEhCliente) campoLoginCliente?.addEventListener("input", () => {
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
    const usuarioPodeCriar = ["Advogado", "Administrador", "Cliente"].includes(normalizarTipoUsuario(usuarioAutenticado?.tipo));
    if (!usuarioPodeCriar) return;

    const dataAtual = new Date();
    const dataFormatada = dataAtual.toLocaleString("pt-BR");
    const resumo = document.getElementById("campo-resumo").value.trim();
    const descricao = document.getElementById("campo-descricao").value.trim();
    const valorInicialInformado = campoValorInicial?.value?.trim() || "";
    const parcelasIniciaisInformadas = campoParcelasIniciais?.value?.trim() || "";
    const valorInicial = valorInicialInformado === "" ? null : Number(campoValorInicial.value);
    const parcelasIniciais = parcelasIniciaisInformadas === "" ? null : parseInt(campoParcelasIniciais.value, 10);
    const primeiraParcela = normalizarDataPagamento(campoPrimeiraParcela?.value || "");

    if ((valorInicialInformado && !parcelasIniciaisInformadas) || (!valorInicialInformado && parcelasIniciaisInformadas)) {
      if (alertaCriacao) {
        alertaCriacao.className = "alert alert-warning";
        alertaCriacao.textContent = "Preencha valor e parcelas juntos para criar o financeiro inicial.";
      }
      return;
    }

    if (valorInicialInformado && (!Number.isFinite(valorInicial) || valorInicial < 0 || !Number.isFinite(parcelasIniciais) || parcelasIniciais < 1)) {
      if (alertaCriacao) {
        alertaCriacao.className = "alert alert-warning";
        alertaCriacao.textContent = "Informe um valor válido e pelo menos 1 parcela.";
      }
      return;
    }

    if (valorInicialInformado && !primeiraParcela) {
      if (alertaCriacao) {
        alertaCriacao.className = "alert alert-warning";
        alertaCriacao.textContent = "Informe a data de pagamento da primeira parcela.";
      }
      return;
    }

    const arquivoAnexado = document.getElementById("campo-anexo").files[0];
    const anexoInicial = arquivoAnexado
      ? [{ name: arquivoAnexado.name, content: await lerArquivoComoDataUrl(arquivoAnexado) }]
      : [];

    const novoChamado = {
      id: "",
      client: document.getElementById("campo-cliente").value.trim(),
      clienteLogin: document.getElementById("campo-login-cliente").value.trim(),
      summary: resumo,
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
          financialEvent: valorInicialInformado
            ? {
                action: "create",
                scope: "cliente",
                product: resumo,
                value: valorInicial,
                installments: parcelasIniciais,
                firstInstallmentDate: primeiraParcela,
              }
            : null,
        },
      ],
      financialOffice: [],
      financialClient: valorInicialInformado
        ? [
            normalizarItemFinanceiro({
              product: resumo,
              value: valorInicial,
              installments: parcelasIniciais,
              description: descricao,
              paidInstallments: Array.from({ length: parcelasIniciais }, () => false),
              installmentDates: criarDatasParcelas(parcelasIniciais, [], primeiraParcela),
            }),
          ]
        : [],
    };

    const clienteVinculado = obterClientePorLogin(novoChamado.clienteLogin);
    if (!clienteVinculado) {
      if (alertaCriacao) {
        alertaCriacao.className = "alert alert-danger";
        alertaCriacao.textContent = "Cadastre o cliente antes de abrir o chamado.";
      }
      botaoCadastrarCliente?.classList.remove("d-none");
      botaoCadastrarCliente.onclick = () => prepararFluxoCadastroUsuario({ login: novoChamado.clienteLogin, retorno: "index.html" });
      return;
    }

    novoChamado.client = clienteVinculado.nomeCompleto;

    if (!novoChamado.client || !novoChamado.clienteLogin || !novoChamado.summary || !descricao) return;

    chamados.unshift(novoChamado);
    try {
      const respostaCriacao = await requisicaoApi("/chamados", {
        method: "POST",
        body: JSON.stringify(novoChamado),
      });
      if (respostaCriacao?.chamado?.id) novoChamado.id = respostaCriacao.chamado.id;
      invalidarCacheChamados();
      notificarAtualizacaoChamados();
    } catch (erro) {
      if (alertaCriacao) {
        alertaCriacao.className = "alert alert-danger";
        alertaCriacao.textContent = erro.message || "Não foi possível salvar o chamado.";
      }
      return;
    }
    window.location.href = normalizarTipoUsuario(usuarioAutenticado?.tipo) === "Cliente" ? "cliente.html" : "index.html";
  });
}

function registrarFormularioCadastroCliente() {
  const form = document.getElementById("form-cadastro-cliente");
  if (!form) return;

  const alerta = document.getElementById("alerta-cadastro-cliente");
  const campoLogin = document.getElementById("campo-cadastro-login");
  const campoTipo = document.getElementById("campo-cadastro-tipo");
  const textoAjudaTipo = document.getElementById("texto-ajuda-tipo-cadastro");
  const botaoSubmit = form.querySelector('button[type="submit"]');
  const loginPreenchido = new URLSearchParams(window.location.search).get("login");
  if (loginPreenchido) campoLogin.value = loginPreenchido;

  function exibirAlertaCadastro(tipo, mensagem) {
    if (!alerta) return;
    alerta.className = `alert alert-${tipo}`;
    alerta.classList.remove("d-none");
    alerta.textContent = mensagem;
  }

  if (campoTipo) {
    const tiposPermitidos = [
      { valor: "Cliente", label: "Cliente" },
      ...(usuarioEhAdministrador() ? [{ valor: "Advogado", label: "Advogado" }] : []),
    ];
    campoTipo.innerHTML = tiposPermitidos
      .map((tipo) => `<option value="${tipo.valor}">${tipo.label}</option>`)
      .join("");
  }
  if (textoAjudaTipo) textoAjudaTipo.textContent = `Perfis disponíveis: ${obterRotuloTipoCadastro()}.`;

  form.addEventListener("submit", async (evento) => {
    evento.preventDefault();

    const tipoSelecionado = normalizarTipoUsuario(campoTipo?.value || "Cliente");
    const novoUsuario = {
      nomeCompleto: document.getElementById("campo-cadastro-nome").value.trim(),
      telefone: document.getElementById("campo-cadastro-telefone").value.trim(),
      email: document.getElementById("campo-cadastro-email").value.trim().toLowerCase(),
      documento: document.getElementById("campo-cadastro-documento").value.trim(),
      login: campoLogin.value.trim().toLowerCase(),
      senha: document.getElementById("campo-cadastro-senha").value.trim(),
      tipo: tipoSelecionado,
    };

    if (!novoUsuario.nomeCompleto || !novoUsuario.telefone || !novoUsuario.email || !novoUsuario.documento || !novoUsuario.login || !novoUsuario.senha) {
      exibirAlertaCadastro("danger", "Preencha todos os campos obrigatórios antes de salvar.");
      return;
    }

    if (!usuarioPodeCriarTipoUsuario(tipoSelecionado)) {
      exibirAlertaCadastro("danger", "Você não tem permissão para criar esse tipo de usuário.");
      return;
    }

    if (credenciaisLogin[novoUsuario.login] || obterClientePorLogin(novoUsuario.login)) {
      exibirAlertaCadastro("danger", "Usuário já cadastrado no sistema. Informe outro login.");
      return;
    }

    if (botaoSubmit) botaoSubmit.disabled = true;

    try {
      await salvarClienteIndividual(novoUsuario);
      if (tipoSelecionado === "Cliente") {
        clientes.push(novoUsuario);
        escreverCacheSessao(CHAVE_CACHE_CLIENTES, clientes);
      }
    } catch (erro) {
      exibirAlertaCadastro("danger", erro.message || "Não foi possível cadastrar o usuário.");
      if (botaoSubmit) botaoSubmit.disabled = false;
      return;
    }

    exibirAlertaCadastro(
      "success",
      tipoSelecionado === "Cliente"
        ? "Cliente cadastrado com sucesso. Agora você pode abrir o chamado."
        : "Usuário cadastrado com sucesso.",
    );

    limparLoginPreCadastro();
    const rotaRetorno = obterRotaRetornoCadastro();
    limparRotaRetornoCadastro();

    setTimeout(() => {
      if (rotaRetorno) {
        window.location.href = rotaRetorno;
        return;
      }
      window.location.href = tipoSelecionado === "Cliente"
        ? `create.html?clienteLogin=${encodeURIComponent(novoUsuario.login)}`
        : "index.html";
    }, 800);
  });
}

function usuarioPodeAcessarChamado(chamado) {
  if (!chamado) return false;
  if (usuarioEhPerfilInterno(usuarioAutenticado?.tipo)) return true;
  if (usuarioAutenticado?.tipo !== "Cliente") return false;

  const loginClienteChamado = (chamado.clienteLogin || "").toLowerCase();
  const identificadorCliente = (usuarioAutenticado?.clienteId || usuarioAutenticado?.usuario || "").toLowerCase();
  return loginClienteChamado === identificadorCliente;
}

async function carregarDetalhesChamado() {
  const container = document.getElementById("detalhes-chamado");
  if (!container) return;
  const id = obterChamadoAtualSelecionado();
  let chamado = chamados.find((c) => c.id === id);
  try {
    chamado = await carregarDetalheChamado(id);
  } catch {
    // fallback para o registro já carregado
  }
  if (!id || !chamado) {
    container.innerHTML = '<div class="alert alert-warning">Chamado não encontrado.</div>';
    return;
  }
  if (!usuarioPodeAcessarChamado(chamado)) {
    container.innerHTML = '<div class="alert alert-danger">Você não tem permissão para visualizar este chamado.</div>';
    return;
  }
  preencherCabecalhoChamado(chamado);
  preencherHistorico(chamado);
  configurarImpressaoHistorico(chamado);
  preencherAnexos(chamado);
  configurarPainelFinanceiro(chamado);
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
  const exibir = usuarioPodeCadastrarUsuarios();
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
  const alerta = document.getElementById("alerta-login");
  const cardPrimeiroAcesso = document.getElementById("card-primeiro-acesso");
  const formPrimeiroAcesso = document.getElementById("form-primeiro-acesso");
  const alertaPrimeiroAcesso = document.getElementById("alerta-primeiro-acesso");
  const params = new URLSearchParams(window.location.search);
  const forcarLogout = params.get("logout") === "1";
  if (forcarLogout) {
    limparAutenticacao();
    params.delete("logout");
  }

  if (params.get("sessao_expirada") === "1" && alerta) {
    alerta.className = "alert alert-warning";
    alerta.classList.remove("d-none");
    alerta.textContent = "Sua sessão expirou após mais de 20 minutos sem atividade. Faça login novamente.";
    params.delete("sessao_expirada");
  }

  if (forcarLogout || !params.get("sessao_expirada")) {
    const novaQuery = params.toString();
    window.history.replaceState({}, "", `login.html${novaQuery ? `?${novaQuery}` : ""}`);
  }
  if (usuarioAutenticado && !usuarioAutenticado?.precisaTrocarSenha) {
    window.location.href = normalizarTipoUsuario(usuarioAutenticado.tipo) === "Cliente" ? "cliente.html" : "index.html";
    return;
  }
  function exibirFluxoPrimeiroAcesso() {
    if (!usuarioAutenticado?.precisaTrocarSenha) return;
    form.classList.add("d-none");
    if (cardPrimeiroAcesso) cardPrimeiroAcesso.classList.remove("d-none");
    const campoUsuarioPrimeiroAcesso = document.getElementById("campo-usuario-primeiro-acesso");
    if (campoUsuarioPrimeiroAcesso) campoUsuarioPrimeiroAcesso.value = usuarioAutenticado.usuario || "";
  }

  function ocultarFluxoPrimeiroAcesso() {
    form.classList.remove("d-none");
    if (cardPrimeiroAcesso) cardPrimeiroAcesso.classList.add("d-none");
  }

  if (usuarioAutenticado?.precisaTrocarSenha && !sessionStorage.getItem(CHAVE_STORAGE_SENHA_TEMPORARIA)) {
    limparAutenticacao();
  }

  if (usuarioAutenticado?.precisaTrocarSenha) exibirFluxoPrimeiroAcesso();
  else ocultarFluxoPrimeiroAcesso();
  const seletorProjeto = document.getElementById("campo-projeto-login");
  try {
    const dadosProjetos = await carregarProjetosDisponiveis();
    if (seletorProjeto) {
      seletorProjeto.innerHTML = (dadosProjetos.projetos || [])
        .map((projeto) => `<option value="${projeto}">${projeto}</option>`)
        .join("");
      seletorProjeto.value = obterBancoProjetoAtual();
      seletorProjeto.addEventListener("change", () => definirBancoProjetoAtivo(seletorProjeto.value));
    }
  } catch {
    if (alerta) {
      alerta.className = "alert alert-warning";
      alerta.classList.remove("d-none");
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
        body: JSON.stringify({ usuario, senha, banco: obterBancoProjetoAtual()}),
      });
      if (autenticacao.banco) definirBancoProjetoAtivo(autenticacao.banco);
      salvarUsuarioAutenticado({
        usuario,
        tipo: normalizarTipoUsuario(autenticacao.tipo),
        clienteId: autenticacao.clienteId,
        precisaTrocarSenha: Boolean(autenticacao.precisaTrocarSenha),
      });
      if (autenticacao.precisaTrocarSenha) {
        sessionStorage.setItem(CHAVE_STORAGE_SENHA_TEMPORARIA, senha);
        if (alerta) {
          alerta.className = "alert alert-warning";
          alerta.classList.remove("d-none");
          alerta.textContent = "No primeiro acesso, a troca de senha é obrigatória.";
        }
        exibirFluxoPrimeiroAcesso();
        return;
      }
      registrarAtividadeUsuario();
      window.location.href = autenticacao.redirect;
      return;
    } catch {
      // segue para o alerta de erro
    }
    if (alerta) {
      alerta.className = "alert alert-danger";
      alerta.classList.remove("d-none");
      alerta.textContent = "Usuário ou senha incorretos.";
    }
  });

  formPrimeiroAcesso?.addEventListener("submit", async (evento) => {
    evento.preventDefault();
    const novaSenha = document.getElementById("campo-nova-senha").value.trim();
    const confirmarSenha = document.getElementById("campo-confirmar-nova-senha").value.trim();
    const senhaAtual = sessionStorage.getItem(CHAVE_STORAGE_SENHA_TEMPORARIA) || "";

    if (!novaSenha || !confirmarSenha) return;
    if (novaSenha !== confirmarSenha) {
      if (alertaPrimeiroAcesso) {
        alertaPrimeiroAcesso.className = "alert alert-danger";
        alertaPrimeiroAcesso.classList.remove("d-none");
        alertaPrimeiroAcesso.textContent = "A confirmação da nova senha não confere.";
      }
      return;
    }

    try {
      const resposta = await requisicaoApi("/usuarios/primeiro-acesso", {
        method: "POST",
        body: JSON.stringify({
          usuario: usuarioAutenticado?.usuario,
          senhaAtual,
          novaSenha,
          banco: obterBancoProjetoAtual(),
        }),
      });
      if (resposta.banco) definirBancoProjetoAtivo(resposta.banco);
      salvarUsuarioAutenticado({
        usuario: resposta.usuario,
        tipo: normalizarTipoUsuario(resposta.tipo),
        clienteId: resposta.clienteId,
        precisaTrocarSenha: false,
      });
      sessionStorage.removeItem(CHAVE_STORAGE_SENHA_TEMPORARIA);
      registrarAtividadeUsuario();
      window.location.href = resposta.redirect;
    } catch (erro) {
      if (alertaPrimeiroAcesso) {
        alertaPrimeiroAcesso.className = "alert alert-danger";
        alertaPrimeiroAcesso.classList.remove("d-none");
        alertaPrimeiroAcesso.textContent = erro.message || "Não foi possível atualizar a senha.";
      }
    }
  });
}


async function configurarPainelAdministrador() {
  const containerLista = document.getElementById("lista-projetos-admin");
  const atual = document.getElementById("banco-atual-admin");
  if (!containerLista || !atual) return;

  atual.textContent = obterBancoProjetoAtual();
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
      window.location.href = normalizarTipoUsuario(usuarioAutenticado?.tipo) === "Cliente" ? "cliente.html" : "index.html";
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

function monitorarSessaoPorInatividade() {
  if (!usuarioAutenticado) return;

  if (sessaoExpiradaPorInatividade()) {
    encerrarSessaoPorInatividade();
    return;
  }

  const eventosAtividade = ["click", "keydown", "mousemove", "mousedown", "scroll", "touchstart"];
  const atualizarAtividade = () => registrarAtividadeUsuario();
  eventosAtividade.forEach((evento) => {
    window.addEventListener(evento, atualizarAtividade, { passive: true });
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      if (sessaoExpiradaPorInatividade()) {
        encerrarSessaoPorInatividade();
        return;
      }
      registrarAtividadeUsuario();
    }
  });

  window.addEventListener("focus", () => {
    if (sessaoExpiradaPorInatividade()) {
      encerrarSessaoPorInatividade();
      return;
    }
    registrarAtividadeUsuario();
  });

  window.setInterval(() => {
    if (sessaoExpiradaPorInatividade()) encerrarSessaoPorInatividade();
  }, INTERVALO_VERIFICACAO_INATIVIDADE_MS);
}

function redirecionarParaLogin(forcarLogout = false, opcoes = {}) {
  const params = new URLSearchParams();
  if (forcarLogout) params.set("logout", "1");
  if (opcoes.sessaoExpirada) params.set("sessao_expirada", "1");
  const query = params.toString();
  window.location.href = `login.html${query ? `?${query}` : ""}`;
}

async function inicializar() {
  garantirOverlayLoading();
  configurarAlternadoresSenha();
  definirUsuarioAutenticadoSeSalvo();

  const paginaDetalhes = document.getElementById("detalhes-chamado");
  const paginaListaTecnico = document.getElementById("table-chamados");
  const paginaCliente = document.getElementById("pagina-cliente");
  const paginaCriacao = document.getElementById("pagina-criacao");
  const paginaCadastroCliente = document.getElementById("pagina-cadastro-cliente");
  const paginaAdmin = document.getElementById("pagina-admin");

  await configurarTelaLogin();

  const carregamentosIniciais = [];

  if (paginaListaTecnico || paginaCliente) carregamentosIniciais.push(carregarChamadosSalvos());
  if (paginaCriacao || paginaCadastroCliente) carregamentosIniciais.push(carregarClientesSalvos());

  if (carregamentosIniciais.length) {
    try {
      await Promise.all(carregamentosIniciais);
    } catch {
      alert(`Não foi possível carregar dados do banco '${obterBancoProjetoAtual()}'. Verifique o backend Python.`);
      return;
    }
  }

  if (usuarioAutenticado && sessaoExpiradaPorInatividade()) {
    encerrarSessaoPorInatividade();
    return;
  }

  if (!usuarioAutenticado && (paginaDetalhes || paginaListaTecnico || paginaCliente || paginaCriacao || paginaCadastroCliente || paginaAdmin)) {
    redirecionarParaLogin();
    return;
  }

  if (
    usuarioAutenticado?.precisaTrocarSenha
    && (paginaDetalhes || paginaListaTecnico || paginaCliente || paginaCriacao || paginaCadastroCliente || paginaAdmin)
  ) {
    window.location.href = "login.html";
    return;
  }

  if (paginaAdmin && usuarioAutenticado?.tipo !== "Administrador") {
    window.location.href = normalizarTipoUsuario(usuarioAutenticado?.tipo) === "Cliente" ? "cliente.html" : "index.html";
    return;
  }

  if (paginaListaTecnico && !usuarioEhPerfilInterno(usuarioAutenticado?.tipo)) {
    window.location.href = "cliente.html";
    return;
  }

  if (paginaCriacao && !["Advogado", "Administrador", "Cliente"].includes(normalizarTipoUsuario(usuarioAutenticado?.tipo))) {
    window.location.href = "cliente.html";
    return;
  }

  if (paginaCadastroCliente && !usuarioPodeCadastrarUsuarios()) {
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
    const avisoCriacao = document.getElementById("alerta-criacao");
    if (avisoCriacao && usuarioAutenticado?.tipo === "Cliente") {
      avisoCriacao.className = "alert alert-info";
      avisoCriacao.textContent = "Você pode criar chamados para o seu próprio usuário.";
    }
    const loginClientePredefinido = obterLoginPreCadastro();
    if (loginClientePredefinido) {
      const campoLoginCliente = document.getElementById("campo-login-cliente");
      if (campoLoginCliente) campoLoginCliente.value = loginClientePredefinido;
      limparLoginPreCadastro();
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
  monitorarSessaoPorInatividade();

  if (typeof BroadcastChannel !== "undefined") {
    const canalAtualizacao = new BroadcastChannel(CANAL_ATUALIZACAO_CHAMADOS);
    canalAtualizacao.addEventListener("message", async (evento) => {
      if (evento.data?.origem === ID_INSTANCIA_ABA) return;
      await carregarChamadosSalvos({ usarCache: false, revalidar: false });
      atualizarTelaComChamadosAtualizados();
    });
  }
}

document.addEventListener("DOMContentLoaded", () => {
  inicializar();
});
