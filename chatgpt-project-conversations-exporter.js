(() => {
  // ChatGPT Project Conversations Exporter
  // Scrapes project gizmos, lists conversations by gizmo, converts to Markdown, and saves.
  // Built to mirror auth, UI, conversion, and saving logic used in chatgpt-conversation-downloader.js.

  // ---------- Re-inject cleanly ----------
  const EXISTING_ID = 'chatgpt-projconv-exporter-root';
  const existing = document.getElementById(EXISTING_ID);
  if (existing) existing.remove();

  const host = document.createElement('div');
  host.id = EXISTING_ID;
  const shadow = host.attachShadow({ mode: 'open' });
  document.body.appendChild(host);

  // ---------- State ----------
  const state = {
    gizmos: [],                    // [{ id, name }]
    items: [],                     // [{ id, title, create_time, update_time, gizmo_id, gizmo_name }]
    totalConversations: 0,
    sort: { key: 'update_time', dir: 'desc' },
    filterText: '',
    filterLower: '',
    rowsMap: new Map(),            // id -> { tr, cells... }
    mdMap: new Map(),              // id -> markdown
    convertStatus: new Map(),      // id -> 'pending'|'done'|'error'
    downloadStatus: new Map(),     // id -> 'downloaded'|undefined
    errors: new Map(),             // id -> error message
    detailMeta: new Map(),         // id -> { update_time?, create_time? }
    concurrency: 3,
    isFetching: false,
    isConverting: false,
    dirHandle: null,
    cancelFlag: false,
    persistKey: null,
  };

  // ---------- Utilities ----------
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const clamp = (v, min, max) => Math.max(min, Math.min(max, v));
  const truncateMiddle = (s, head = 6, tail = 4) => (s && s.length > head + tail + 1) ? `${s.slice(0, head)}…${s.slice(-tail)}` : s || '';
  const htmlEscape = (s) => String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[m]));
  const ORIGIN = location.origin.includes('chat.openai.com') ? 'https://chatgpt.com' : location.origin;
  const apiUrl = (path) => path.startsWith('http') ? path : `${ORIGIN}${path}`;

  function supportsFS() { return !!(window.showDirectoryPicker && window.FileSystemWritableFileStream); }
  async function pickDirectory() { try { return await window.showDirectoryPicker({ mode: 'readwrite' }); } catch { return null; } }

  function loadPersist() { try { return JSON.parse(localStorage.getItem(state.persistKey) || '{}'); } catch { return {}; } }
  function savePersist(map) { try { localStorage.setItem(state.persistKey, JSON.stringify(map || {})); } catch {} }
  async function withBackoff(task, tries = 5, base = 500) {
    let attempt = 0;
    for (;;) {
      try { return await task(); }
      catch (e) {
        attempt++;
        if (attempt >= tries) throw e;
        const delay = base * Math.pow(2, attempt - 1) + Math.floor(Math.random() * 250);
        await sleep(delay);
      }
    }
  }

  let persisted = new Map();
  function persistNow() {
    const dump = {};
    for (const it of state.items) {
      const id = it.id;
      if (state.downloadStatus.get(id) === 'downloaded') {
        dump[id] = { status: 'downloaded' };
      } else if (state.errors.get(id)) {
        dump[id] = { status: 'error', error: state.errors.get(id) };
      } else {
        const prev = persisted.get(id);
        if (prev?.status) dump[id] = prev;
      }
    }
    savePersist(dump);
    persisted = new Map(Object.entries(dump));
  }

  state.persistKey = `cgpt:projconv:${location.host}`;
  persisted = new Map(Object.entries(loadPersist()));

  const isoFromAny = (v) => {
    if (v == null || v === '') return '-';
    const n = Number(v);
    if (!Number.isNaN(n)) {
      const ms = n < 1e12 ? n * 1000 : n;
      const d = new Date(ms);
      return Number.isNaN(d.getTime()) ? '-' : d.toISOString();
    }
    const d = new Date(v);
    return Number.isNaN(d.getTime()) ? '-' : d.toISOString();
  };
  const todayDateStr = () => {
    const d = new Date();
    const mm = `${d.getMonth()+1}`.padStart(2,'0');
    const dd = `${d.getDate()}`.padStart(2,'0');
    return `${d.getFullYear()}-${mm}-${dd}`;
  };
  const slugify = (t) => {
    const s = String(t || 'conversation').toLowerCase()
      .replace(/\s+/g,'-').replace(/[^a-z0-9-]/g,'-').replace(/-+/g,'-').replace(/^-+|-+$/g,'');
    return s.slice(0, 80) || 'conversation';
  };
  const dateStrFromTs = (ts) => {
    if (ts === undefined || ts === null || ts === '') return null;
    try {
      let d;
      if (ts instanceof Date) d = ts;
      else {
        const n = Number(ts);
        const ms = Number.isNaN(n) ? NaN : (n < 1e12 ? n * 1000 : n);
        d = new Date(ms);
        if (Number.isNaN(ms)) d = new Date(ts);
      }
      if (Number.isNaN(d.getTime())) return null;
      const mm = `${d.getMonth()+1}`.padStart(2,'0');
      const dd = `${d.getDate()}`.padStart(2,'0');
      return `${d.getFullYear()}-${mm}-${dd}`;
    } catch { return null; }
  };
  const filenameFor = (item) => {
    const detail = state.detailMeta.get(item.id) || null;
    const dateStr =
      [detail?.update_time, item.update_time, detail?.create_time, item.create_time]
        .map(dateStrFromTs)
        .find(Boolean) || todayDateStr();
    const giz = item.gizmo_name ? `-${slugify(item.gizmo_name)}` : '';
    const slug = slugify(item.title);
    return `${dateStr}${giz}-${slug || 'conversation'}.md`;
  };

  const icon = {
    check: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M20 6L9 17l-5-5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
    warn:  `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
    copy:  `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M8 8h12v12H8zM4 4h12v4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
    dot:   `<span class="dot" aria-hidden="true"></span>`
  };

  // Cookie helpers
  const getCookie = (name) => {
    const pair = document.cookie.split('; ').find(row => row.startsWith(name + '='));
    if (!pair) return null;
    const raw = pair.split('=').slice(1).join('=');
    try { return decodeURIComponent(raw).replace(/^"|"$/g, ''); }
    catch { return raw.replace(/^"|"$/g, ''); }
  };
  const getAccountIdFromCookie = () => {
    const v = getCookie('_account');
    return v && v.trim() ? v.trim() : null;
  };

  // ---------- Styles + UI ----------
  const style = document.createElement('style');
  style.textContent = `
:host { all: initial; --bg-0:#1E2326; --bg-1:#272E33; --bg-2:#2E383C; --bg-3:#374145; --bg-4:#414B50; --fg:#D3C6AA; --panel-br:rgba(255,255,255,0.08); }
:host, * { box-sizing: border-box; }
:host { font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }
.panel { position: fixed; right: 16px; bottom: 16px; width: min(960px, calc(100vw - 24px)); max-height: min(78vh, 800px); display: flex; flex-direction: column; background: color-mix(in srgb, var(--bg-1) 68%, transparent); backdrop-filter: blur(12px) saturate(150%); -webkit-backdrop-filter: blur(12px) saturate(150%); border: 1px solid var(--panel-br); border-radius: 16px; color: var(--fg); overflow: hidden; z-index: 2147483647; }
.header { display:flex; align-items:center; justify-content:space-between; padding:10px 12px; cursor:move; border-bottom:1px solid var(--panel-br); }
.title { font-weight: 600; }
.badge { margin-left: 8px; background: var(--bg-3); color: var(--fg); padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid var(--panel-br); }
.header-actions { display:flex; gap:8px; }
.btn { appearance:none; border:1px solid var(--panel-br); color:var(--fg); background: linear-gradient(180deg, color-mix(in srgb, var(--bg-2) 70%, transparent), var(--bg-1)); padding:6px 10px; border-radius:10px; font-size:12px; cursor:pointer; }
.btn[disabled]{ opacity:.6; cursor:not-allowed; }
.btn.primary { border-color: rgba(127,187,179,0.25); }
.btn.danger  { border-color: rgba(230,126,128,0.25); }
.toolbar { display:flex; gap:8px; align-items:center; padding:8px 12px; border-top:1px solid var(--panel-br); border-bottom:1px solid var(--panel-br); background: color-mix(in srgb, var(--bg-2) 60%, transparent); }
.input, .number { background: color-mix(in srgb, var(--bg-2) 90%, transparent); color:var(--fg); border:1px solid var(--panel-br); padding:6px 8px; border-radius:10px; font-size:12px; }
.number { width:72px; min-width:72px; text-align:center; }
.table-wrap { overflow:auto; flex:1 1 auto; }
table { width:100%; border-collapse: separate; border-spacing: 0; }
thead th { position:sticky; top:0; z-index:1; background: color-mix(in srgb, var(--bg-3) 92%, transparent); color:var(--fg); padding:8px; text-align:left; border-bottom:1px solid var(--panel-br); cursor:pointer; white-space:nowrap; }
tbody td { padding:8px; border-bottom:1px solid var(--panel-br); vertical-align:top; color:var(--fg); }
tbody tr:nth-child(2n){ background: color-mix(in srgb, var(--bg-2) 35%, transparent); }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }
.id-col { width: 28ch; max-width: 28ch; }
.ws-col { width: 38ch; max-width: 38ch; }
.api-link a { color: #7FBBB3; text-decoration: none; }
.dl { display:inline-flex; align-items:center; gap:6px; }
.dot { width:8px; height:8px; border-radius:50%; background:#859289; display:inline-block; }
.progress { display:flex; align-items:center; gap:10px; padding:10px 12px; border-top:1px solid var(--panel-br); }
.pbar { position:relative; flex:1; height:10px; background: var(--bg-3); border-radius:999px; overflow:hidden; }
.pbar > .fill { position:absolute; left:0; top:0; bottom:0; width:0%; background: linear-gradient(90deg, #7FBBB3, #83C092); transition: width .2s ease; }
.progress .text { font-size:12px; color:#9DA9A0; }
.footer { padding:8px 12px; color:#9DA9A0; border-top:1px solid var(--panel-br); font-size:12px; background: color-mix(in srgb, var(--bg-2) 60%, transparent); }
.load-more { padding:8px 12px; display:flex; justify-content:center; border-top:1px solid var(--panel-br); }
.id-row { display:flex; align-items:center; gap:6px; }
.id-copy { display:inline-flex; align-items:center; justify-content:center; padding:2px; border-radius:8px; border:1px solid var(--panel-br); background: var(--bg-2); color: var(--fg); cursor: pointer; }
`;
  shadow.appendChild(style);

  const panel = document.createElement('div');
  panel.className = 'panel';
  panel.innerHTML = `
    <div class="header" id="drag-handle" aria-label="Drag to move">
      <div class="lh">
        <span class="title">ChatGPT Project Conversations Exporter</span>
        <span class="badge" id="count-badge">0</span>
      </div>
      <div class="header-actions">
        <button class="btn primary" id="btn-download">Convert & Download All</button>
        <button class="btn" id="btn-retry">Retry Failed</button>
        <button class="btn danger" id="btn-close">Close</button>
      </div>
    </div>
    <div class="toolbar">
      <button class="btn" id="btn-refresh">Refresh List</button>
      <input class="input" id="search" type="search" placeholder="Filter by title…" aria-label="Filter by title">
      <span class="label">Concurrency</span>
      <input class="number" id="concurrency" type="number" min="1" max="5" value="3" aria-label="Concurrency (1-5)">
    </div>
    <div class="table-wrap">
      <table id="table">
        <thead>
          <tr>
            <th data-key="index" aria-sort="none">#</th>
            <th data-key="id" aria-sort="none">id</th>
            <th data-key="title" aria-sort="none">title</th>
            <th data-key="create_time" aria-sort="none">create_time</th>
            <th data-key="update_time" aria-sort="descending">update_time</th>
            <th data-key="gizmo_id" aria-sort="none">gizmo_id</th>
            <th data-key="gizmo_name" aria-sort="none">gizmo_name</th>
            <th data-key="api" aria-sort="none">conversation URL API</th>
            <th data-key="dl" aria-sort="none">DL checker</th>
          </tr>
        </thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
    <div class="load-more" id="pager" style="display:none;">
      <button class="btn" id="btn-more">Load more</button>
    </div>
    <div class="summary" id="fetch-summary">Loaded 0 conversations across 0 projects</div>
    <div class="progress" id="fetch-progress">
      <div class="pbar"><div class="fill" id="fetch-pfill" style="width:0%"></div></div>
      <div class="text" id="fetch-ptext" aria-live="polite">0 conversations loaded</div>
    </div>
    <div class="progress" id="convert-progress">
      <div class="pbar"><div class="fill" id="convert-pfill" style="width:0%"></div></div>
      <div class="text" id="convert-ptext" aria-live="polite">0 of 0 converted</div>
    </div>
    <div class="progress" id="save-progress">
      <div class="pbar"><div class="fill" id="save-pfill" style="width:0%"></div></div>
      <div class="text" id="save-ptext" aria-live="polite">Waiting to save…</div>
    </div>
    <div class="footer">
      Note: Uses same auth headers, cookies, and conversion logic as the conversation downloader.
    </div>
  `;
  shadow.appendChild(panel);

  const dom = {
    tbody: shadow.getElementById('tbody'),
    countBadge: shadow.getElementById('count-badge'),
    fetchSummary: shadow.querySelector('.summary'),
    fetchPFill: shadow.getElementById('fetch-pfill'),
    fetchPText: shadow.getElementById('fetch-ptext'),
    convertPFill: shadow.getElementById('convert-pfill'),
    convertPText: shadow.getElementById('convert-ptext'),
    savePFill: shadow.getElementById('save-pfill'),
    savePText: shadow.getElementById('save-ptext'),
    btnRefresh: shadow.getElementById('btn-refresh'),
    btnDownload: shadow.getElementById('btn-download'),
    btnRetry: shadow.getElementById('btn-retry'),
    btnClose: shadow.getElementById('btn-close'),
    btnMore: shadow.getElementById('btn-more'),
    pager: shadow.getElementById('pager'),
    searchInput: shadow.getElementById('search'),
    concurrencyInput: shadow.getElementById('concurrency'),
    sortHeaders: Array.from(shadow.querySelectorAll('thead th')),
  };

  // ---------- Draggable panel ----------
  (() => {
    const handle = panel.querySelector('#drag-handle');
    let dragging = false, startX=0, startY=0, startRight=16, startBottom=16;
    let lastPos = { right: 16, bottom: 16 };
    const onDown = (e) => {
      dragging = true;
      const rect = panel.getBoundingClientRect();
      startX = e.clientX; startY = e.clientY;
      startRight = window.innerWidth - rect.right;
      startBottom = window.innerHeight - rect.bottom;
      panel.style.right = `${lastPos.right}px`;
      panel.style.bottom = `${lastPos.bottom}px`;
      panel.style.left = 'auto';
      panel.style.top = 'auto';
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    };
    const onMove = (e) => {
      if (!dragging) return;
      const dx = e.clientX - startX;
      const dy = e.clientY - startY;
      const newRight = clamp(startRight - dx, 8, window.innerWidth - 120);
      const newBottom = clamp(startBottom - dy, 8, window.innerHeight - 120);
      lastPos = { right: newRight, bottom: newBottom };
      panel.style.right = `${newRight}px`;
      panel.style.bottom = `${newBottom}px`;
    };
    const onUp = () => {
      dragging = false;
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    };
    handle.addEventListener('mousedown', onDown);
  })();

  // ---------- Auth + fetch with retries (same pattern as base) ----------
  let __accessToken = null;
  async function getAccessToken() {
    if (__accessToken) return __accessToken;
    const resp = await fetch(apiUrl('/api/auth/session'), { credentials: 'include' });
    if (!resp.ok) throw new Error(`Auth session failed: HTTP ${resp.status}`);
    const data = await resp.json();
    const t = data?.accessToken || data?.access_token;
    if (!t) throw new Error('No access token in /api/auth/session');
    __accessToken = t;
    return __accessToken;
  }
  async function fetchJSONWithRetries(url, opts = {}, maxRetries = 3) {
    let attempt = 0;
    for (;;) {
      try {
        const token = await getAccessToken();
        const headers = new Headers(opts.headers || {});
        if (!headers.has('accept')) headers.set('accept', 'application/json');
        if (!headers.has('authorization')) headers.set('authorization', `Bearer ${token}`);
        if (!headers.has('chatgpt-account-id')) {
          const accId = getAccountIdFromCookie();
          if (accId) headers.set('chatgpt-account-id', accId);
        }
        const resp = await fetch(apiUrl(url), { ...opts, headers, credentials: 'include' });
        if (resp.ok) return await resp.json();
        if ((resp.status === 401 || resp.status === 403) && attempt < maxRetries) {
          __accessToken = null;
          attempt++;
          continue;
        }
        if ([429, 500, 502, 503, 504].includes(resp.status) && attempt < maxRetries) {
          await sleep(800 * Math.pow(2, attempt++));
          continue;
        }
        throw new Error(`HTTP ${resp.status} ${resp.statusText} - ${url}`);
      } catch (err) {
        if (attempt < maxRetries) { await sleep(800 * Math.pow(2, attempt++)); continue; }
        const msg = err && err.message ? err.message : String(err);
        throw new Error(`Fetch failed for ${opts.method || 'GET'} ${url}: ${msg}`);
      }
    }
  }

  // ---------- Fetch project gizmos and conversations ----------
  function setFetchProgress(done, total, label) {
    const fill = dom.fetchPFill;
    const text = dom.fetchPText;
    const pct = total ? Math.min(100, Math.round((done / total) * 100)) : 0;
    if (fill) fill.style.width = `${pct}%`;
    if (text) text.textContent = label || `${done} of ${total || '…'} conversations loaded`;
  }
  function updateCount() {
    if (dom.countBadge) dom.countBadge.textContent = `${state.items.length}`;
    if (dom.fetchSummary) {
      const giz = state.gizmos.length;
      const conv = state.items.length;
      dom.fetchSummary.textContent = `Loaded ${conv} conversations across ${giz} projects`;
    }
  }

  async function fetchGizmos() {
    const url = '/backend-api/gizmos/snorlax/sidebar?conversations_per_gizmo=5&owned_only=true';
    const data = await fetchJSONWithRetries(url, { method: 'GET' });
    const arr = Array.isArray(data?.items) ? data.items : [];
    const out = [];
    for (let i = 0; i < arr.length; i++) {
      const g = arr[i]?.gizmo?.gizmo;
      const id = g?.id || null;
      const name = g?.display?.name || null;
      if (id && name) out.push({ id, name });
    }
    return out;
  }

  async function fetchConversationsForGizmo(gizmo, onProgress) {
    const results = [];
    let cursor = 0;
    let pages = 0;
    while (true) {
      const url = `/backend-api/gizmos/${encodeURIComponent(gizmo.id)}/conversations?cursor=${encodeURIComponent(cursor)}`;
      const data = await fetchJSONWithRetries(url, { method: 'GET' });
      const items = Array.isArray(data?.items) ? data.items : [];
      for (const it of items) {
        results.push({
          id: it.id,
          title: it.title || 'untitled',
          create_time: it.create_time || null,
          update_time: it.update_time || null,
          gizmo_id: it.gizmo_id || gizmo.id,
          gizmo_name: gizmo.name
        });
      }
      pages++;
      if (typeof onProgress === 'function') onProgress(results.length);
      // Try common cursor fields
      const nextCur = (data && (data.cursor ?? data.next_cursor ?? data.next)) ?? null;
      if (nextCur == null || nextCur === '' || nextCur === cursor || items.length === 0) break;
      cursor = nextCur;
      // avoid hammering
      await sleep(120);
    }
    return results;
  }

  function sortItems() {
    const { key, dir } = state.sort;
    const mult = dir === 'asc' ? 1 : -1;
    const dateKeys = new Set(['create_time','update_time']);
    state.items.sort((a, b) => {
      if (dateKeys.has(key)) {
        const va = a[key] ? new Date(a[key]).getTime() : 0;
        const vb = b[key] ? new Date(b[key]).getTime() : 0;
        return (va - vb) * mult;
      }
      const va = String(a[key] || '');
      const vb = String(b[key] || '');
      return va.localeCompare(vb) * mult;
    });
  }

  function applyFilterToRow(tr, title) {
    if (!state.filterLower) { tr.style.display = ''; return; }
    const show = String(title || '').toLowerCase().includes(state.filterLower);
    tr.style.display = show ? '' : 'none';
  }

  function renderRows() {
    const tbody = dom.tbody;
    if (!tbody) return;
    tbody.innerHTML = '';
    const frag = document.createDocumentFragment();
    state.rowsMap.clear();
    state.items.forEach((item, idx) => {
      const tr = document.createElement('tr');

      const tdIndex = document.createElement('td');
      tdIndex.className = 'mono';
      tdIndex.textContent = `${idx + 1}`;

      const tdId = document.createElement('td');
      tdId.className = 'id-col mono';
      const idWrap = document.createElement('div'); idWrap.className = 'id-row';
      const idText = document.createElement('span'); idText.textContent = truncateMiddle(item.id, 8, 4); idText.title = item.id;
      const copyBtn = document.createElement('button');
      copyBtn.className = 'id-copy'; copyBtn.type = 'button'; copyBtn.title = 'Copy ID'; copyBtn.innerHTML = icon.copy;
      copyBtn.onclick = async () => { try { await navigator.clipboard.writeText(item.id); } catch {} };
      idWrap.appendChild(idText); idWrap.appendChild(copyBtn); tdId.appendChild(idWrap);

      const tdTitle = document.createElement('td'); tdTitle.textContent = item.title || 'untitled';
      const tdCT = document.createElement('td'); tdCT.textContent = isoFromAny(item.create_time);
      const tdUT = document.createElement('td'); tdUT.textContent = isoFromAny(item.update_time);

      const tdGZ = document.createElement('td'); tdGZ.className = 'ws-col mono'; tdGZ.textContent = item.gizmo_id || '-';
      const tdGN = document.createElement('td'); tdGN.textContent = item.gizmo_name || '-';

      const tdAPI = document.createElement('td');
      tdAPI.className = 'api-link';
      const a = document.createElement('a');
      a.href = apiUrl(`/backend-api/conversation/${encodeURIComponent(item.id)}`);
      a.textContent = a.href; a.target = '_blank'; a.rel = 'noopener';
      tdAPI.appendChild(a);

      const tdDL = document.createElement('td'); tdDL.className = 'dl'; tdDL.innerHTML = icon.dot + '<span class="label"></span>';

      tr.appendChild(tdIndex);
      tr.appendChild(tdId);
      tr.appendChild(tdTitle);
      tr.appendChild(tdCT);
      tr.appendChild(tdUT);
      tr.appendChild(tdGZ);
      tr.appendChild(tdGN);
      tr.appendChild(tdAPI);
      tr.appendChild(tdDL);
      frag.appendChild(tr);

      state.rowsMap.set(item.id, { tr, tdDL, tdTitle, tdUT });
      applyFilterToRow(tr, item.title || '');
      refreshRowDL(item.id);
    });
    tbody.appendChild(frag);
    updateSortHeaders();
  }

  function updateSortHeaders() {
    dom.sortHeaders.forEach(th => {
      const key = th.getAttribute('data-key');
      let aria = 'none';
      if (key === state.sort.key) aria = state.sort.dir === 'asc' ? 'ascending' : 'descending';
      th.setAttribute('aria-sort', aria);
    });
  }

  function refreshRowDL(id) {
    const row = state.rowsMap.get(id);
    if (!row) return;
    const cell = row.tdDL;
    cell.innerHTML = '';
    const persistedStatus = persisted.get(id)?.status;
    if (state.downloadStatus.get(id) === 'downloaded' || persistedStatus === 'downloaded') {
      cell.innerHTML = `<span class="ok">${icon.check}</span><span>downloaded</span>`;
      return;
    }
    const err = state.errors.get(id) || persisted.get(id)?.error;
    if (err) {
      cell.innerHTML = `<span class="warn" title="${htmlEscape(String(err))}">${icon.warn}</span><span>error</span>`;
      return;
    }
    cell.innerHTML = icon.dot + '<span class="label"></span>';
  }

  dom.sortHeaders.forEach(th => {
    th.addEventListener('click', () => {
      const key = th.getAttribute('data-key');
      if (key === 'api' || key === 'dl' || key === 'index') return;
      if (state.sort.key === key) state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
      else { state.sort.key = key; state.sort.dir = 'asc'; }
      sortItems();
      renderRows();
    });
  });

  dom.searchInput?.addEventListener('input', (e) => {
    state.filterText = e.target.value || '';
    state.filterLower = state.filterText.toLowerCase();
    for (const it of state.items) {
      const row = state.rowsMap.get(it.id);
      if (row) applyFilterToRow(row.tr, it.title || '');
    }
  });

  dom.btnClose?.addEventListener('click', () => { host.remove(); });
  dom.concurrencyInput?.addEventListener('change', () => { const v = parseInt(dom.concurrencyInput.value, 10); state.concurrency = clamp(Number.isNaN(v) ? 3 : v, 1, 5); });
  state.concurrency = clamp(parseInt(dom.concurrencyInput?.value, 10) || 3, 1, 5);

  // ---------- Fetch orchestration ----------
  async function refreshList() {
    state.isFetching = true;
    state.items = [];
    state.gizmos = [];
    state.totalConversations = 0;
    renderRows();
    updateCount();
    setFetchProgress(0, 0, 'Fetching projects…');

    try {
      const gizmos = await fetchGizmos();
      state.gizmos = gizmos;
      updateCount();

      let loaded = 0;
      const out = [];
      // Concurrency for gizmo conversations
      let idx = 0;
      const workers = state.concurrency;
      async function worker() {
        while (true) {
          const i = idx++;
          if (i >= gizmos.length) break;
          const g = gizmos[i];
          const convs = await fetchConversationsForGizmo(g, (n) => {
            setFetchProgress(loaded + n, undefined, `Fetching conversations… ${loaded + n}`);
          });
          loaded += convs.length;
          out.push(...convs);
          setFetchProgress(loaded, undefined, `Fetching conversations… ${loaded}`);
        }
      }
      await Promise.all(Array.from({ length: workers }, worker));

      // Deduplicate by conversation id across gizmos
      const uniq = new Map();
      for (const it of out) if (!uniq.has(it.id)) uniq.set(it.id, it);
      state.items = Array.from(uniq.values());
      sortItems();
      renderRows();
      updateCount();
      setFetchProgress(state.items.length, state.items.length, `${state.items.length} conversations loaded`);
    } catch (err) {
      setFetchProgress(0, 0, `Fetch failed: ${err && err.message ? err.message : String(err)}`);
      console.error(err);
    } finally {
      state.isFetching = false;
    }
  }

  dom.btnRefresh?.addEventListener('click', async () => {
    try { dom.btnRefresh.disabled = true; await refreshList(); }
    finally { dom.btnRefresh.disabled = false; }
  });

  // ---------- Detail/meta + conversion + saving (ported behavior) ----------
  function fmtTime(secEpoch) {
    try {
      if (!secEpoch && secEpoch !== 0) return '';
      const ms = (typeof secEpoch === 'number') ? secEpoch * 1000 : Number(secEpoch) * 1000;
      const d = new Date(ms);
      return isNaN(d.getTime()) ? '' : d.toISOString();
    } catch { return ''; }
  }
  function shouldInclude(node) {
    const m = node?.message;
    if (!m) return false;
    const role = m.author?.role;
    if (!['user','assistant','system'].includes(role)) return false;
    if (m.metadata?.is_hidden) return false;
    const c = m.content || {};
    const type = c.content_type || c.type || '';
    const parts = Array.isArray(c.parts) ? c.parts : [];
    const text = c.text ?? (typeof c === 'string' ? c : '');
    const hasText = (type === 'text' || type === 'multimodal_text') &&
      (parts.some(p => typeof p === 'string' && p.trim()) || String(text || '').trim());
    const hasCode = (type === 'code') && (c.code || c.text);
    return hasText || hasCode;
  }
  function messageTextOrCode(node) {
    const m = node?.message || {};
    const c = m.content || {};
    const type = c.content_type || c.type || '';
    if (type === 'code') {
      const lang = (c.language || c.lang || '').toLowerCase() || '';
      const code = String(c.text ?? c.code ?? '').trim();
      return { kind: 'code', lang, body: code };
    }
    let body = '';
    if (Array.isArray(c.parts) && c.parts.length) {
      body = c.parts.map(p => (typeof p === 'string' ? p : (p?.text || ''))).join('\n\n');
    } else {
      body = String(c.text ?? '').trim();
    }
    return { kind: 'text', lang: '', body };
  }
  function clean(s) {
    const t = String(s || '').trim();
    return t.replace(/\u3010.*?\u3011/g, '').replace(/\[\^\d+\]/g, '').replace(/\[\d+\]/g, '');
  }
  function renderMessage(node) {
    const m = node.message;
    const role = m.author?.role || 'assistant';
    const when = fmtTime(m.create_time);
    const head = `### ${role} ${when ? '· ' + when : ''}`.trim();
    const { kind, lang, body } = messageTextOrCode(node);
    if (!body) return head;
    if (kind === 'code') {
      const fence = '```';
      return `${head}\n\n${fence}${lang ? lang : ''}\n${clean(body)}\n${fence}\n`;
    }
    return `${head}\n\n${clean(body)}\n`;
  }
  function dfsOrder(mapping) {
    const nodes = mapping || {};
    const roots = [];
    for (const [id, n] of Object.entries(nodes)) if (!n?.parent) roots.push(id);
    roots.sort();
    const out = [];
    const seen = new Set();
    function visit(id) {
      if (!id || seen.has(id)) return;
      seen.add(id);
      const n = nodes[id];
      if (n) out.push(n);
      const kids = (n?.children || []).slice().sort();
      for (const k of kids) visit(k);
    }
    for (const r of roots) visit(r);
    return out;
  }
  function collectUrlsFromText(s) {
    if (!s) return [];
    const urls = [];
    const re = /https?:\/\/[^\s<()>\[\]{}"']+/g;
    let m;
    while ((m = re.exec(s)) !== null) {
      let u = m[0].replace(/[.,)>\]]+$/, '');
      urls.push(u);
    }
    return urls;
  }
  function normalizeUrl(u) {
    try {
      const url = new URL(u, location.origin);
      if (!/^https?:$/.test(url.protocol)) return null;
      url.hash = '';
      const toDelete = [];
      url.searchParams.forEach((_, k) => { if (/^utm_/i.test(k) || /^gclid$/i.test(k) || /^fbclid$/i.test(k)) toDelete.push(k); });
      for (const k of toDelete) url.searchParams.delete(k);
      if (url.pathname.length > 1 && url.pathname.endsWith('/')) url.pathname = url.pathname.replace(/\/+$/, '');
      return url.toString();
    } catch { return null; }
  }
  function collectSources(data) {
    const set = new Set();
    const rootSafe = data?.metadata?.safe_urls;
    if (Array.isArray(rootSafe)) for (const u of rootSafe) { const n = normalizeUrl(u); if (n) set.add(n); }
    const srg = data?.metadata?.search_result_groups || data?.search_result_groups;
    if (Array.isArray(srg)) for (const g of srg) for (const r of g?.results || []) {
      if (r?.url) { const n = normalizeUrl(r.url); if (n) set.add(n); }
      if (Array.isArray(r?.safe_urls)) for (const u of r.safe_urls) { const n = normalizeUrl(u); if (n) set.add(n); }
    }
    const crItems = data?.content_references?.items;
    if (Array.isArray(crItems)) for (const it of crItems) {
      if (Array.isArray(it?.safe_urls)) for (const u of it.safe_urls) { const n = normalizeUrl(u); if (n) set.add(n); }
      if (it?.url) { const n = normalizeUrl(it.url); if (n) set.add(n); }
    }
    const mapping = data?.mapping || {};
    for (const n of Object.values(mapping)) {
      const m = n?.message; if (!m) continue;
      const metaSafe = m?.metadata?.safe_urls;
      if (Array.isArray(metaSafe)) for (const u of metaSafe) { const nn = normalizeUrl(u); if (nn) set.add(nn); }
      const { body } = messageTextOrCode(n);
      collectUrlsFromText(body).forEach(u => { const nn = normalizeUrl(u); if (nn) set.add(nn); });
    }
    return Array.from(set);
  }
  function convertJsonToMarkdown(input) {
    const title = input?.title || 'Conversation';
    const ordered = dfsOrder(input?.mapping || {});
    const visible = ordered.filter(shouldInclude);
    const lines = [];
    lines.push(`# ${title}`); lines.push('');
    if (visible.length) {
      for (const node of visible) { lines.push(renderMessage(node).trimEnd()); lines.push(''); }
    } else {
      const snippet = { id: input?.id || null, title, update_time: input?.update_time || null, create_time: input?.create_time || null, note: 'No visible messages; raw snippet follows' };
      lines.push('```json'); lines.push(JSON.stringify(snippet, null, 2)); lines.push('```'); lines.push('');
    }
    const sources = collectSources(input);
    lines.push('## Sources');
    if (sources.length) for (const u of sources) lines.push(`- ${u}`); else lines.push('- (none)');
    lines.push('');
    return lines.join('\n');
  }

  async function ensureDetailMeta(ids, onProgress) {
    const pending = ids.filter(id => !state.detailMeta.has(id));
    if (!pending.length) return;
    let idx = 0, done = 0;
    const workers = state.concurrency;
    async function worker() {
      while (true) {
        const i = idx++;
        if (i >= pending.length) break;
        const id = pending[i];
        try {
          const data = await fetchJSONWithRetries(`/backend-api/conversation/${encodeURIComponent(id)}`, { method: 'GET' });
          const detailUpdate = data?.update_time ?? data?.conversation?.update_time ?? null;
          const detailCreate = data?.create_time ?? data?.conversation?.create_time ?? null;
          state.detailMeta.set(id, { update_time: detailUpdate ?? null, create_time: detailCreate ?? null });
          const item = state.items.find(it => it.id === id);
          if (item) {
            if (detailUpdate != null) item.update_time = detailUpdate;
            if (detailCreate != null && item.create_time == null) item.create_time = detailCreate;
            const row = state.rowsMap.get(id);
            if (row) row.tdUT.textContent = isoFromAny(item.update_time);
          }
        } catch {}
        finally { done++; if (onProgress) onProgress(done); }
      }
    }
    await Promise.all(Array.from({ length: workers }, worker));
  }

  async function convertMissing(ids) {
    const pending = ids.filter(id => !state.mdMap.get(id));
    if (!pending.length) return;
    state.isConverting = true;
    const btn = dom.btnDownload; if (btn) btn.disabled = true;

    let done = 0; const total = pending.length;
    dom.convertPFill.style.width = '0%';
    dom.convertPText.textContent = `0 of ${total} converted`;

    let idx = 0;
    const workers = state.concurrency;
    async function worker() {
      while (true) {
        const i = idx++;
        if (i >= pending.length) break;
        const id = pending[i];
        try {
          const data = await fetchJSONWithRetries(`/backend-api/conversation/${encodeURIComponent(id)}`, { method: 'GET' });
          const detailUpdate = data?.update_time ?? data?.conversation?.update_time ?? null;
          const detailCreate = data?.create_time ?? data?.conversation?.create_time ?? null;
          const prev = state.detailMeta.get(id) || {};
          state.detailMeta.set(id, { update_time: detailUpdate ?? prev.update_time ?? null, create_time: detailCreate ?? prev.create_time ?? null });

          const item = state.items.find(it => it.id === id);
          if (item) {
            if (detailUpdate != null) item.update_time = detailUpdate;
            if (detailCreate != null && (item.create_time == null)) item.create_time = detailCreate;
            const row = state.rowsMap.get(id);
            if (row) row.tdUT.textContent = isoFromAny(item.update_time);
          }

          const md = convertJsonToMarkdown(data);
          state.mdMap.set(id, md);
          state.convertStatus.set(id, 'done');
        } catch (e) {
          state.errors.set(id, e.message || String(e));
          state.convertStatus.set(id, 'error');
        } finally {
          done++;
          refreshRowDL(id);
          const pct = total ? Math.round((done / total) * 100) : 0;
          dom.convertPFill.style.width = `${pct}%`;
          dom.convertPText.textContent = `${done} of ${total} converted`;
        }
      }
    }
    await Promise.all(Array.from({ length: workers }, worker));
    state.isConverting = false;
    if (btn) btn.disabled = false;
  }

  async function ensureDir() { if (state.dirHandle) return state.dirHandle; state.dirHandle = await pickDirectory(); return state.dirHandle; }

  async function downloadAllToFolder(items, onProgress) {
    const dir = await ensureDir();
    if (!dir) throw new Error('No folder selected');
    let done = 0;
    for (const it of items) {
      if (persisted.get(it.id)?.status === 'downloaded') {
        state.downloadStatus.set(it.id, 'downloaded');
        refreshRowDL(it.id);
        if (onProgress) onProgress(++done);
        continue;
      }
      const md = state.mdMap.get(it.id);
      if (!md) { if (onProgress) onProgress(++done); continue; }
      const name = filenameFor(it);
      try {
        await withBackoff(async () => {
          const fh = await dir.getFileHandle(name, { create: true });
          const w = await fh.createWritable();
          await w.write(md);
          await w.close();
        });
        state.downloadStatus.set(it.id, 'downloaded');
        state.errors.delete(it.id);
      } catch (e) {
        state.errors.set(it.id, e.message || String(e));
      }
      refreshRowDL(it.id); persistNow();
      await sleep(10);
      if (onProgress) onProgress(++done);
    }
  }

  async function downloadAllViaAnchors(items, onProgress) {
    const BATCH = 5, PAUSE_MS = 2000;
    let launched = 0, done = 0;
    for (const it of items) {
      if (persisted.get(it.id)?.status === 'downloaded') {
        state.downloadStatus.set(it.id, 'downloaded'); refreshRowDL(it.id);
        if (onProgress) onProgress(++done); continue;
      }
      const md = state.mdMap.get(it.id);
      if (!md) { if (onProgress) onProgress(++done); continue; }
      try {
        await withBackoff(async () => {
          const blob = new Blob([md], { type: 'text/markdown;charset=utf-8' });
          const url = URL.createObjectURL(blob);
          const a = document.createElement('a'); a.href = url; a.download = filenameFor(it);
          document.body.appendChild(a); a.click(); a.remove();
          setTimeout(() => URL.revokeObjectURL(url), 10000);
        });
        state.downloadStatus.set(it.id, 'downloaded'); state.errors.delete(it.id);
        refreshRowDL(it.id);
        launched++;
        if (launched % BATCH === 0) await sleep(PAUSE_MS); else await sleep(40);
      } catch (e) {
        state.errors.set(it.id, e.message || String(e));
        refreshRowDL(it.id);
      }
      persistNow();
      if (onProgress) onProgress(++done);
    }
  }

  async function saveAllPreferFS(items) {
    const savable = items.filter(it => {
      const persistedStatus = persisted.get(it.id)?.status;
      const already = persistedStatus === 'downloaded' || state.downloadStatus.get(it.id) === 'downloaded';
      return !already && !!state.mdMap.get(it.id);
    });
    const total = savable.length;
    dom.savePFill.style.width = '0%';
    dom.savePText.textContent = total ? `Ready to save ${total} file${total === 1 ? '' : 's'}` : 'Nothing to save';

    let progressed = 0;
    const onProgress = (n) => {
      progressed = n;
      const pct = total ? Math.min(100, Math.round((progressed / total) * 100)) : 0;
      dom.savePFill.style.width = `${pct}%`;
      dom.savePText.textContent = `Saving… (${progressed} of ${total})`;
    };

    if (supportsFS()) {
      const dir = await ensureDir();
      if (dir) { await downloadAllToFolder(savable, onProgress); dom.savePText.textContent = `Saved ${progressed} of ${total} files`; return; }
    }
    await downloadAllViaAnchors(savable, onProgress);
    dom.savePText.textContent = `Saved ${progressed} of ${total} files`;
  }

  dom.btnDownload?.addEventListener('click', async () => {
    const ids = state.items.map(it => it.id);
    const btn = dom.btnDownload; if (btn) btn.disabled = true;
    try {
      dom.convertPFill.style.width = '0%';
      dom.convertPText.textContent = `Preparing… (0 of ${ids.length})`;
      let prepDone = 0;
      await ensureDetailMeta(ids, (n) => {
        prepDone = n;
        dom.convertPText.textContent = `Preparing… (${prepDone} of ${ids.length})`;
      });

      await convertMissing(ids);

      const totalSavable = state.items.filter(it => state.mdMap.get(it.id) && state.downloadStatus.get(it.id) !== 'downloaded' && persisted.get(it.id)?.status !== 'downloaded').length;
      dom.savePFill.style.width = '0%';
      dom.savePText.textContent = totalSavable ? `Ready to save ${totalSavable} files` : 'Nothing to save';
      await saveAllPreferFS(state.items);
    } finally {
      if (btn) btn.disabled = false;
    }
  });

  dom.btnRetry?.addEventListener('click', async () => {
    const failed = state.items.filter(it => state.errors.get(it.id) || (persisted.get(it.id)?.status === 'error'));
    if (!failed.length) return;
    const totalSavable = failed.filter(it => state.mdMap.get(it.id)).length;
    dom.savePFill.style.width = '0%';
    dom.savePText.textContent = totalSavable ? `Ready to save ${totalSavable} files` : 'Nothing to save';
    await saveAllPreferFS(failed);
  });

  // ---------- Init ----------
  sortItems();
  renderRows();
  for (const it of state.items) {
    if (persisted.get(it.id)?.status === 'downloaded') state.downloadStatus.set(it.id, 'downloaded');
    refreshRowDL(it.id);
  }
  persistNow();
  dom.convertPText.textContent = '0 of 0 converted';
  dom.savePText.textContent = 'Waiting to save…';
  setFetchProgress(0, 0, 'Ready');

  // Kick off
  refreshList().catch(err => {
    console.error('Failed to fetch project conversations:', err);
    dom.fetchPText.textContent = 'Auth failed. Open any chat to refresh your session, then click "Refresh List".';
  });
})();
