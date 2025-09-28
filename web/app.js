/* eslint-disable no-console */
(function () {
  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));

  function getQueryParam(name) {
    const url = new URL(window.location.href);
    return url.searchParams.get(name);
  }

  async function fetchJSON(url) {
    try {
      const res = await fetch(url, { cache: 'no-store' });
      if (!res.ok) return null;
      return await res.json();
    } catch (e) {
      return null;
    }
  }

  async function fetchText(url) {
    try {
      const res = await fetch(url, { cache: 'no-store' });
      if (!res.ok) return null;
      return await res.text();
    } catch (e) {
      return null;
    }
  }

  function parseNDJSON(text) {
    if (!text) return [];
    const out = [];
    for (const line of text.split(/\r?\n/)) {
      const t = line.trim();
      if (!t) continue;
      try { out.push(JSON.parse(t)); } catch (_) {}
    }
    return out;
  }

  function renderSummary(summary, contacts, chats, messages) {
    const sm = $('#summary');
    const mc = $('#metrics');
    const sc = $('#summaryCard');
    const st = $('#statsCard');
    const sp = $('#samplesCard');

    const counts = {
      contacts: contacts.length,
      chats: chats.length,
      messages: messages.length,
    };

    sm.innerHTML = `<pre>${JSON.stringify(summary || { info: 'Sem summary.json' }, null, 2)}</pre>`;

    const expContacts = contacts.filter(c => c.exported_to_chatwoot === true).length;
    const expChats = chats.filter(c => c.exported_to_chatwoot === true).length;
    const expMsgs = messages.filter(m => m.exported_to_chatwoot === true).length;

    mc.innerHTML = '';
    mc.insertAdjacentHTML('beforeend', `<li><strong>Contatos</strong>: ${counts.contacts} (exportados: ${expContacts})</li>`);
    mc.insertAdjacentHTML('beforeend', `<li><strong>Conversas</strong>: ${counts.chats} (exportadas: ${expChats})</li>`);
    mc.insertAdjacentHTML('beforeend', `<li><strong>Mensagens</strong>: ${counts.messages} (exportadas: ${expMsgs})</li>`);

    $('#sampleContacts').textContent = JSON.stringify(contacts.slice(0, 5), null, 2);
    $('#sampleChats').textContent = JSON.stringify(chats.slice(0, 5), null, 2);
    $('#sampleMessages').textContent = JSON.stringify(messages.slice(0, 5), null, 2);

    sc.hidden = false;
    st.hidden = false;
    sp.hidden = false;
  }

  async function loadByPrefix(prefix) {
    const base = `../data/${prefix}`;
    // Try extract summary first, then load summary
    let summary = await fetchJSON(`${base}/summary.json`);
    if (!summary) summary = await fetchJSON(`${base}/load_summary.json`);

    // Prefer export status files, else raw ndjson
    let contactsText = await fetchText(`${base}/contacts_export_status.ndjson`);
    if (!contactsText) contactsText = await fetchText(`${base}/contacts.ndjson`);

    let chatsText = await fetchText(`${base}/chats_export_status.ndjson`);
    if (!chatsText) chatsText = await fetchText(`${base}/chats.ndjson`);

    let messagesText = await fetchText(`${base}/messages_export_status.ndjson`);
    if (!messagesText) messagesText = await fetchText(`${base}/messages.ndjson`);

    const contacts = parseNDJSON(contactsText);
    const chats = parseNDJSON(chatsText);
    const messages = parseNDJSON(messagesText);

    renderSummary(summary, contacts, chats, messages);
  }

  function readFileAsText(file) {
    return new Promise((resolve, reject) => {
      const fr = new FileReader();
      fr.onload = () => resolve(fr.result);
      fr.onerror = reject;
      fr.readAsText(file);
    });
  }

  async function loadFromFiles() {
    const sumF = $('#fileSummary').files[0];
    const cF = $('#fileContacts').files[0];
    const chF = $('#fileChats').files[0];
    const mF = $('#fileMessages').files[0];

    let summary = null;
    if (sumF) {
      try { summary = JSON.parse(await readFileAsText(sumF)); } catch (_) { summary = null; }
    }
    const contacts = cF ? parseNDJSON(await readFileAsText(cF)) : [];
    const chats = chF ? parseNDJSON(await readFileAsText(chF)) : [];
    const messages = mF ? parseNDJSON(await readFileAsText(mF)) : [];

    renderSummary(summary, contacts, chats, messages);
  }

  function init() {
    $('#btnLoadPrefix').addEventListener('click', () => {
      const prefix = ($('#prefix').value || '').trim();
      if (!prefix) return alert('Informe um prefixo');
      loadByPrefix(prefix);
    });

    $('#btnLoadFiles').addEventListener('click', () => {
      loadFromFiles();
    });

    const qp = getQueryParam('prefix');
    if (qp) {
      $('#prefix').value = qp;
      loadByPrefix(qp);
    }
  }

  document.addEventListener('DOMContentLoaded', init);
})();
