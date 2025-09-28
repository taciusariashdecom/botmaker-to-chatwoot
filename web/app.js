/* eslint-disable no-console */
(function () {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  const DATASETS = [
    { key: 'contacts', label: 'Contatos' },
    { key: 'chats', label: 'Conversas' },
    { key: 'messages', label: 'Mensagens' },
  ];

  const state = {
    summary: null,
    contacts: [],
    chats: [],
    messages: [],
    dataset: 'contacts',
  };

  const logLines = [];

  function pushLog(message, level = 'info', details) {
    const timestamp = new Date().toLocaleTimeString('pt-BR', { hour12: false });
    let line = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
    if (details) {
      line += `\n${details}`;
    }
    logLines.push(line);
    if (logLines.length > 400) {
      logLines.shift();
    }
    const output = $('#logOutput');
    if (output) {
      output.textContent = logLines.join('\n\n');
      output.scrollTop = output.scrollHeight;
    }
  }

  function getQueryParam(name) {
    const url = new URL(window.location.href);
    return url.searchParams.get(name);
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function formatValue(value) {
    if (value === null || value === undefined) {
      return '<span class="muted">—</span>';
    }
    if (typeof value === 'object') {
      try {
        return `<code>${escapeHtml(JSON.stringify(value))}</code>`;
      } catch (err) {
        return `<code>${escapeHtml(String(value))}</code>`;
      }
    }
    if (typeof value === 'boolean') {
      return value ? 'true' : 'false';
    }
    if (value instanceof Date) {
      return escapeHtml(value.toISOString());
    }
    return escapeHtml(String(value));
  }

  async function fetchJSON(url) {
    try {
      const res = await fetch(url, { cache: 'no-store' });
      if (!res.ok) return null;
      return await res.json();
    } catch (e) {
      pushLog(`Falha ao buscar JSON em ${url}`, 'warn', e.message);
      return null;
    }
  }

  async function fetchText(url) {
    try {
      const res = await fetch(url, { cache: 'no-store' });
      if (!res.ok) return null;
      return await res.text();
    } catch (e) {
      pushLog(`Falha ao buscar texto em ${url}`, 'warn', e.message);
      return null;
    }
  }

  function parseNDJSON(text, sourceLabel) {
    if (!text) return [];
    const out = [];
    const lines = text.split(/\r?\n/);
    lines.forEach((line, index) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      try {
        out.push(JSON.parse(trimmed));
      } catch (err) {
        pushLog(
          `Não foi possível interpretar uma linha em ${sourceLabel || 'NDJSON'}`,
          'error',
          `Linha ${index + 1}: ${err.message}`,
        );
      }
    });
    return out;
  }

  function readFileAsText(file) {
    return new Promise((resolve, reject) => {
      const fr = new FileReader();
      fr.onload = () => resolve(fr.result);
      fr.onerror = () => reject(fr.error);
      fr.readAsText(file);
    });
  }

  function updateStatus(el, text, statusClass) {
    if (!el) return;
    el.className = 'status-text';
    if (statusClass) {
      el.classList.add(statusClass);
    }
    el.textContent = text;
  }

  function updateDatasetButtons() {
    const buttons = $$('.dataset-toggle [data-dataset]');
    buttons.forEach((btn) => {
      const key = btn.getAttribute('data-dataset');
      const meta = DATASETS.find((d) => d.key === key);
      if (!meta) return;
      const count = state[key]?.length ?? 0;
      btn.textContent = `${meta.label} (${count})`;
      btn.classList.toggle('active', state.dataset === key);
      btn.classList.toggle('empty', count === 0);
    });
  }

  function updateSummaryCard() {
    const card = $('#summaryCard');
    const container = $('#summary');
    if (!card || !container) return;
    const hasSummary = !!state.summary;
    const hasRecords = DATASETS.some((d) => state[d.key].length > 0);
    if (!hasSummary && !hasRecords) {
      card.hidden = true;
      container.innerHTML = '';
      return;
    }
    card.hidden = false;
    const summaryData = state.summary || {
      info: 'Sem summary carregado. Use o teste rápido ou faça upload dos arquivos.',
    };
    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(summaryData, null, 2);
    container.innerHTML = '';
    container.appendChild(pre);
  }

  function updateMetricsCard() {
    const card = $('#statsCard');
    const list = $('#metrics');
    if (!card || !list) return;
    const counts = {
      contacts: state.contacts.length,
      chats: state.chats.length,
      messages: state.messages.length,
    };
    const hasRecords = Object.values(counts).some((value) => value > 0);
    if (!hasRecords) {
      card.hidden = true;
      list.innerHTML = '';
      return;
    }
    card.hidden = false;
    const exportedContacts = state.contacts.filter((item) => item.exported_to_chatwoot === true).length;
    const exportedChats = state.chats.filter((item) => item.exported_to_chatwoot === true).length;
    const exportedMessages = state.messages.filter((item) => item.exported_to_chatwoot === true).length;
    list.innerHTML = '';
    list.insertAdjacentHTML(
      'beforeend',
      `<li><strong>Contatos</strong>: ${counts.contacts} (exportados: ${exportedContacts})</li>`,
    );
    list.insertAdjacentHTML(
      'beforeend',
      `<li><strong>Conversas</strong>: ${counts.chats} (exportadas: ${exportedChats})</li>`,
    );
    list.insertAdjacentHTML(
      'beforeend',
      `<li><strong>Mensagens</strong>: ${counts.messages} (exportadas: ${exportedMessages})</li>`,
    );
  }

  function updateSamplesCard() {
    const card = $('#samplesCard');
    const hasRecords = DATASETS.some((d) => state[d.key].length > 0);
    if (!card) return;
    if (!hasRecords) {
      card.hidden = true;
    } else {
      card.hidden = false;
      $('#sampleContacts').textContent = state.contacts.length
        ? JSON.stringify(state.contacts.slice(0, 5), null, 2)
        : 'Sem dados carregados.';
      $('#sampleChats').textContent = state.chats.length
        ? JSON.stringify(state.chats.slice(0, 5), null, 2)
        : 'Sem dados carregados.';
      $('#sampleMessages').textContent = state.messages.length
        ? JSON.stringify(state.messages.slice(0, 5), null, 2)
        : 'Sem dados carregados.';
    }
  }

  function updateDataTable() {
    const card = $('#dataTableCard');
    const table = $('#dataTable');
    const emptyState = $('#tableEmptyState');
    if (!card || !table || !emptyState) return;

    const hasAnyRecords = DATASETS.some((d) => state[d.key].length > 0);
    if (!hasAnyRecords) {
      card.hidden = true;
      emptyState.hidden = false;
      emptyState.textContent = 'Carregue dados para visualizar os campos aqui.';
      table.hidden = true;
      table.querySelector('thead').innerHTML = '';
      table.querySelector('tbody').innerHTML = '';
      return;
    }

    card.hidden = false;

    const activeMeta = DATASETS.find((d) => d.key === state.dataset) || DATASETS[0];
    const records = state[activeMeta.key] || [];

    updateDatasetButtons();

    if (!records.length) {
      emptyState.hidden = false;
      emptyState.textContent = `Nenhum registro carregado para ${activeMeta.label}.`;
      table.hidden = true;
      table.querySelector('thead').innerHTML = '';
      table.querySelector('tbody').innerHTML = '';
      return;
    }

    emptyState.hidden = true;
    table.hidden = false;

    const columns = new Set();
    records.forEach((record) => {
      if (record && typeof record === 'object' && !Array.isArray(record)) {
        Object.keys(record).forEach((key) => columns.add(key));
      }
    });
    const columnList = Array.from(columns).sort();

    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    thead.innerHTML = `<tr>${columnList.map((col) => `<th>${escapeHtml(col)}</th>`).join('')}</tr>`;

    const rowsHtml = records.map((record) => {
      const cells = columnList.map((col) => {
        const value = record ? record[col] : undefined;
        return `<td>${formatValue(value)}</td>`;
      });
      return `<tr>${cells.join('')}</tr>`;
    });

    tbody.innerHTML = rowsHtml.join('');
  }

  function refreshUI() {
    updateDatasetButtons();
    updateSummaryCard();
    updateMetricsCard();
    updateDataTable();
    updateSamplesCard();
  }

  function applyData(payload, sourceLabel = 'Atualização') {
    const summary = payload?.summary ?? null;
    const contacts = Array.isArray(payload?.contacts) ? payload.contacts : [];
    const chats = Array.isArray(payload?.chats) ? payload.chats : [];
    const messages = Array.isArray(payload?.messages) ? payload.messages : [];

    state.summary = summary;
    state.contacts = contacts;
    state.chats = chats;
    state.messages = messages;

    if (!state[state.dataset] || state[state.dataset].length === 0) {
      const fallback = DATASETS.find((d) => state[d.key].length > 0);
      state.dataset = fallback ? fallback.key : 'contacts';
    }

    refreshUI();

    const counts = DATASETS.map((d) => `${d.label}: ${state[d.key].length}`).join(' | ');
    pushLog(`${sourceLabel}: dados atualizados`, 'success', counts);
  }

  async function loadByPrefix(prefix) {
    const trimmed = (prefix || '').trim();
    if (!trimmed) {
      alert('Informe um prefixo');
      return;
    }
    pushLog(`Carregando dados a partir do prefixo ${trimmed}`, 'info');
    const base = `../data/${trimmed}`;
    try {
      let summary = await fetchJSON(`${base}/summary.json`);
      if (!summary) summary = await fetchJSON(`${base}/load_summary.json`);

      const contactsText =
        (await fetchText(`${base}/contacts_export_status.ndjson`)) ||
        (await fetchText(`${base}/contacts.ndjson`));
      const chatsText =
        (await fetchText(`${base}/chats_export_status.ndjson`)) ||
        (await fetchText(`${base}/chats.ndjson`));
      const messagesText =
        (await fetchText(`${base}/messages_export_status.ndjson`)) ||
        (await fetchText(`${base}/messages.ndjson`));

      if (!summary && !contactsText && !chatsText && !messagesText) {
        pushLog(`Nenhum arquivo encontrado em ${base}`, 'warn');
      }

      const contacts = parseNDJSON(contactsText, 'Contatos (prefixo)');
      const chats = parseNDJSON(chatsText, 'Conversas (prefixo)');
      const messages = parseNDJSON(messagesText, 'Mensagens (prefixo)');

      applyData({ summary, contacts, chats, messages }, `Prefixo ${trimmed}`);
    } catch (err) {
      pushLog(`Erro ao carregar prefixo ${trimmed}`, 'error', err.stack || String(err));
      alert(`Falha ao carregar prefixo: ${err.message}`);
    }
  }

  async function loadFromFiles() {
    const summaryFile = $('#fileSummary').files[0];
    const contactsFile = $('#fileContacts').files[0];
    const chatsFile = $('#fileChats').files[0];
    const messagesFile = $('#fileMessages').files[0];

    if (!summaryFile && !contactsFile && !chatsFile && !messagesFile) {
      alert('Selecione ao menos um arquivo para carregar.');
      return;
    }

    pushLog('Carregando arquivos locais selecionados', 'info');

    let summary = null;
    if (summaryFile) {
      try {
        summary = JSON.parse(await readFileAsText(summaryFile));
      } catch (err) {
        pushLog('Não foi possível ler o arquivo de resumo', 'error', err.message);
      }
    }

    let contacts = [];
    if (contactsFile) {
      try {
        contacts = parseNDJSON(await readFileAsText(contactsFile), 'Contatos (arquivo local)');
      } catch (err) {
        pushLog('Falha ao ler contatos locais', 'error', err.message);
      }
    }

    let chats = [];
    if (chatsFile) {
      try {
        chats = parseNDJSON(await readFileAsText(chatsFile), 'Conversas (arquivo local)');
      } catch (err) {
        pushLog('Falha ao ler conversas locais', 'error', err.message);
      }
    }

    let messages = [];
    if (messagesFile) {
      try {
        messages = parseNDJSON(await readFileAsText(messagesFile), 'Mensagens (arquivo local)');
      } catch (err) {
        pushLog('Falha ao ler mensagens locais', 'error', err.message);
      }
    }

    applyData({ summary, contacts, chats, messages }, 'Arquivos locais');
  }

  function setActiveDataset(key) {
    if (!key || !DATASETS.some((dataset) => dataset.key === key)) return;
    state.dataset = key;
    pushLog(`Visualização alterada para ${key}`, 'info');
    updateDataTable();
  }

  async function runTest() {
    const button = $('#btnTestRun');
    const statusEl = $('#testStatus');
    if (!button) return;

    button.disabled = true;
    button.classList.add('is-loading');
    updateStatus(statusEl, 'Executando teste rápido…', 'loading');
    pushLog('Executando teste rápido na função serverless…', 'info');

    try {
      const response = await fetch('/.netlify/functions/test_run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      });
      const text = await response.text();
      let payload = null;
      try {
        payload = JSON.parse(text || '{}');
      } catch (err) {
        throw new Error('Resposta inválida da função (JSON malformado).');
      }

      if (!response.ok) {
        const errorMessage = payload.error || 'Falha na função serverless';
        const hint = payload.hint ? `\nDica: ${payload.hint}` : '';
        throw new Error(`${errorMessage}${hint}`);
      }

      applyData(payload, 'Teste rápido');
      updateStatus(statusEl, 'Teste concluído com sucesso.', 'success');
      pushLog(
        'Teste rápido concluído',
        'success',
        `Contatos: ${payload.contacts?.length ?? 0} | Conversas: ${payload.chats?.length ?? 0} | Mensagens: ${payload.messages?.length ?? 0}`,
      );
    } catch (err) {
      updateStatus(statusEl, `Erro: ${err.message}`, 'error');
      pushLog('Erro ao executar teste rápido', 'error', err.stack || String(err));
    } finally {
      button.disabled = false;
      button.classList.remove('is-loading');
    }
  }

  function init() {
    pushLog('Interface carregada. Pronta para receber dados.', 'info');

    $('#btnLoadPrefix').addEventListener('click', () => {
      const prefix = $('#prefix').value;
      loadByPrefix(prefix);
    });

    $('#btnLoadFiles').addEventListener('click', () => {
      loadFromFiles();
    });

    const testButton = $('#btnTestRun');
    if (testButton) {
      testButton.addEventListener('click', () => {
        runTest();
      });
    }

    $$('.dataset-toggle [data-dataset]').forEach((btn) => {
      btn.addEventListener('click', () => {
        const key = btn.getAttribute('data-dataset');
        setActiveDataset(key);
      });
    });

    updateDatasetButtons();

    const qp = getQueryParam('prefix');
    if (qp) {
      $('#prefix').value = qp;
      pushLog(`Prefixo detectado pela URL (${qp})`, 'info');
      loadByPrefix(qp);
    }
  }

  document.addEventListener('DOMContentLoaded', init);
})();
