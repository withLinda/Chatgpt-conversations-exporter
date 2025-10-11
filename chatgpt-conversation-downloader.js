(() => {
  // ChatGPT Conversation Downloader - Shadow DOM UI + JSON→Markdown + Downloads
  // 12 concise comments included to explain key steps.

  // 1) Re-inject cleanly if already present
  const EXISTING_ID = 'chatgpt-conv-dl-root';
  const existing = document.getElementById(EXISTING_ID);
  if (existing) existing.remove();

  // 2) Create root and Shadow DOM to fully isolate styles/markup
  const host = document.createElement('div');
  host.id = EXISTING_ID;
  const shadow = host.attachShadow({ mode: 'open' });
  document.body.appendChild(host);

  // --------- State (module-scope, now with localStorage persistence) ----------
  const state = {
    items: [],
    total: 0,
    limit: 100,
    offset: 0,
    sort: { key: 'update_time', dir: 'desc' },
    filterText: '',
    rowsMap: new Map(),           // id -> { tr, cells... }
    mdMap: new Map(),             // id -> markdown
    convertStatus: new Map(),     // id -> 'pending'|'done'|'error'
    downloadStatus: new Map(),    // id -> 'downloaded'|undefined
    errors: new Map(),            // id -> error message
    detailMeta: new Map(),        // id -> { update_time?, create_time? }
    concurrency: 3,
    isConverting: false,
    isFetching: false,
    // NEW:
    dirHandle: null,              // chosen directory handle (FS Access API)
    cancelFlag: false,            // reserved for future cancel support
    persistKey: null,             // localStorage key for statuses
  };

  // --------- Utilities ----------
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const clamp = (v, min, max) => Math.max(min, Math.min(max, v));
  const truncateMiddle = (s, head = 6, tail = 4) => (s && s.length > head + tail + 1) ? `${s.slice(0, head)}…${s.slice(-tail)}` : s || '';
  const htmlEscape = (s) => String(s ?? '').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;','\'':'&#39;'}[m]));
  const ORIGIN = location.origin.includes('chat.openai.com') ? 'https://chatgpt.com' : location.origin;
  const apiUrl = (path) => path.startsWith('http') ? path : `${ORIGIN}${path}`;

  // NEW: persistence helpers and backoff + FS utils
  function supportsFS() {
    return !!(window.showDirectoryPicker && window.FileSystemWritableFileStream);
  }
  async function pickDirectory() {
    try { return await window.showDirectoryPicker({ mode: 'readwrite' }); }
    catch { return null; }
  }
  function loadPersist() {
    try { return JSON.parse(localStorage.getItem(state.persistKey) || '{}'); } catch { return {}; }
  }
  function savePersist(map) {
    try { localStorage.setItem(state.persistKey, JSON.stringify(map || {})); } catch {}
  }
  async function withBackoff(task, tries = 5, base = 500) {
    let attempt = 0;
    while (true) {
      try { return await task(); }
      catch (e) {
        attempt++;
        if (attempt >= tries) throw e;
        const delay = base * Math.pow(2, attempt - 1) + Math.floor(Math.random() * 250);
        await sleep(delay);
      }
    }
  }

  // NEW: persisted map in memory (hydrated after persistKey is set)
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
        if (prev?.status) dump[id] = prev; // keep previous state if any
      }
    }
    savePersist(dump);
    persisted = new Map(Object.entries(dump));
  }

  // Set persist key and hydrate storage once DOM exists
  state.persistKey = `cgpt:dl:${location.host}`;
  persisted = new Map(Object.entries(loadPersist()));

  // Robust ISO formatter: accepts ISO string, epoch seconds, or epoch ms
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
      if (ts instanceof Date) {
        d = ts;
      } else {
        const n = Number(ts);
        if (!Number.isNaN(n)) {
          const ms = n < 1e12 ? n * 1000 : n;
          d = new Date(ms);
        } else {
          d = new Date(ts);
        }
      }
      if (Number.isNaN(d.getTime())) return null;
      const mm = `${d.getMonth()+1}`.padStart(2,'0');
      const dd = `${d.getDate()}`.padStart(2,'0');
      return `${d.getFullYear()}-${mm}-${dd}`;
    } catch (_) {
      return null;
    }
  };

  const filenameFor = (item) => {
    const detail = state.detailMeta.get(item.id) || null;
    // Strict preference: detail.update_time -> item.update_time -> detail.create_time -> item.create_time
    // Avoid falling back to today unless all above fail.
    const dateStr =
      [detail?.update_time, item.update_time, detail?.create_time, item.create_time]
        .map(dateStrFromTs)
        .find(Boolean) || todayDateStr();
    const slug = slugify(item.title);
    return `${dateStr}-${slug || 'conversation'}.md`;
  };
  const icon = {
    check: `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M20 6L9 17l-5-5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
    warn:  `<svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M12 9v4m0 4h.01M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
    copy:  `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M8 8h12v12H8zM4 4h12v4" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
    dot:   `<span class="dot" aria-hidden="true"></span>`
  };

  // Cookie helpers: read "_account" and expose its value
  const getCookie = (name) => {
    const pair = document.cookie.split('; ').find(row => row.startsWith(name + '='));
    if (!pair) return null;
    const raw = pair.split('=').slice(1).join('=');
    try {
      return decodeURIComponent(raw).replace(/^"|"$/g, '');
    } catch (_) {
      return raw.replace(/^"|"$/g, '');
    }
  };
  const getAccountIdFromCookie = () => {
    const v = getCookie('_account');
    return v && v.trim() ? v.trim() : null;
  };

  // 3) Styles inside Shadow DOM (palette + glass UI)
  const style = document.createElement('style');
  style.textContent = `
:host {
  all: initial;
  /* Design tokens must live on :host so they exist inside the Shadow DOM */
  /* Backgrounds (palette1) */
  --bg-0:#1E2326; --bg-1:#272E33; --bg-2:#2E383C; --bg-3:#374145;
  --bg-4:#414B50; --bg-5:#495156; --bg-6:#4F5B58; --bg-a:#4C3743;
  --bg-b:#493B40; --bg-c:#45443c; --bg-d:#3C4841; --bg-e:#384B55; --bg-f:#463F48;

  /* Foreground (palette2) */
  --fg:#D3C6AA;
  --acc-red:#E67E80; --acc-orange:#E69875; --acc-yellow:#DBBC7F;
  --acc-green:#83C092; --acc-aqua:#7FBBB3; --acc-pink:#D699B6;
  --gray-0:#7A8478; --gray-1:#859289; --gray-2:#9DA9A0;
  --status-fg:#D3C6AA; --status-err:#E67E80;

  --panel-bg: color-mix(in srgb, var(--bg-1) 70%, transparent);
  --panel-br: rgba(255,255,255,0.08);
  --shadow: 0 10px 30px rgba(0,0,0,0.35);
  --focus: 0 0 0 2px color-mix(in srgb, var(--acc-aqua) 50%, transparent);
}
:host, * { box-sizing: border-box; }
:host { font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; }

.panel {
  position: fixed; right: 16px; bottom: 16px;
  width: min(960px, calc(100vw - 24px));
  max-height: min(78vh, 800px);
  display: flex; flex-direction: column;
  /* Fallback background if color-mix is unsupported */
  background: var(--panel-bg, rgba(39,46,51,0.92));
  backdrop-filter: blur(10px) saturate(140%);
  border: 1px solid var(--panel-br);
  border-radius: 14px; box-shadow: var(--shadow); color: var(--fg, #D3C6AA);
  overflow: hidden; z-index: 2147483647;
}

.header {
  position: relative;
  display: flex; align-items: center; justify-content: space-between;
  padding: 10px 12px; user-select: none; cursor: move;
}
.header::before {
  content:""; position:absolute; inset:0; pointer-events:none;
  border-bottom: 1px solid transparent;
  background: linear-gradient(120deg, var(--bg-e), var(--bg-3), var(--bg-a));
  -webkit-mask: linear-gradient(#000, #000) content-box, linear-gradient(#000, #000);
  -webkit-mask-composite: xor; mask-composite: exclude;
  animation: hue 9s linear infinite;
  opacity: 0.25;
}
@keyframes hue { from { filter: hue-rotate(0deg) } to { filter: hue-rotate(360deg) } }

.title { font-weight: 600; letter-spacing: .2px; }
.badge {
  margin-left: 8px; background: var(--bg-3); color: var(--fg);
  padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid var(--panel-br);
}

.header-actions { display: flex; gap: 8px; }
.btn {
  appearance: none; border: 1px solid var(--panel-br); color: var(--fg);
  background: linear-gradient(180deg, color-mix(in srgb, var(--bg-2) 70%, transparent), var(--bg-1));
  padding: 6px 10px; border-radius: 8px; font-size: 12px; cursor: pointer;
}
.btn:hover { filter: brightness(1.08); }
.btn:focus-visible { outline: none; box-shadow: var(--focus); }
.btn.primary { border-color: color-mix(in srgb, var(--acc-aqua) 25%, var(--panel-br)); }
.btn.danger { border-color: color-mix(in srgb, var(--acc-red) 25%, var(--panel-br)); }

.toolbar {
  display: flex; gap: 8px; align-items: center; padding: 8px 12px;
  border-top: 1px solid var(--panel-br); border-bottom: 1px solid var(--panel-br);
}
.input, .number {
  background: var(--bg-2); color: var(--fg); border: 1px solid var(--panel-br);
  padding: 6px 8px; border-radius: 8px; font-size: 12px; min-width: 220px;
}
.number { width: 72px; min-width: 72px; text-align: center; }
.label { color: var(--gray-2); font-size: 12px; }

.table-wrap { overflow: auto; flex: 1 1 auto; }
table { width: 100%; border-collapse: separate; border-spacing: 0; }
thead th {
  position: sticky; top: 0; z-index: 1;
  background: var(--bg-3); color: var(--fg); padding: 8px; text-align: left;
  border-bottom: 1px solid var(--panel-br); cursor: pointer; white-space: nowrap;
}
tbody td {
  padding: 8px; border-bottom: 1px solid var(--panel-br);
  vertical-align: top; color: var(--fg);
}
tbody tr:hover { background: color-mix(in srgb, var(--bg-2) 70%, transparent); }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; }

.id-col { width: 28ch; max-width: 28ch; }
.index-col { width: 4ch; text-align: right; color: var(--gray-2); }
.id-row { display: flex; align-items: center; gap: 6px; }
.id-copy { display: inline-flex; align-items: center; justify-content: center; padding: 2px; border-radius: 6px; border: 1px solid var(--panel-br); background: var(--bg-2); color: var(--fg); cursor: pointer; }
.id-copy:hover { filter: brightness(1.1); }

.ws-col { width: 38ch; max-width: 38ch; }
.api-link a { color: var(--acc-aqua); text-decoration: none; }
.api-link a:hover { text-decoration: underline; }

.dl { display: inline-flex; align-items: center; gap: 6px; }
.dl .ok { color: var(--acc-green); display: inline-flex; align-items:center; }
.dl .warn { color: var(--acc-red); display: inline-flex; align-items:center; }
.dot { width: 8px; height: 8px; border-radius: 50%; background: var(--gray-1); display: inline-block; }

.progress {
  display: flex; align-items: center; gap: 10px; padding: 10px 12px; border-top: 1px solid var(--panel-br);
}
.pbar {
  position: relative; flex: 1; height: 8px; background: var(--bg-3); border-radius: 999px; overflow: hidden;
}
.pbar > .fill {
  position: absolute; left: 0; top: 0; bottom: 0; width: 0%;
  background: linear-gradient(90deg, var(--acc-aqua), var(--acc-green)); transition: width .2s ease;
}
.progress .text { font-size: 12px; color: var(--gray-2); }
.summary {
  display: flex; align-items: center; justify-content: space-between;
  padding: 8px 12px; border-top: 1px solid var(--panel-br);
  font-size: 12px; color: var(--gray-2);
}

.footer { padding: 8px 12px; color: var(--gray-2); border-top: 1px solid var(--panel-br); font-size: 12px; }
.load-more { padding: 8px 12px; display:flex; justify-content:center; border-top: 1px solid var(--panel-br); }
  `;
  shadow.appendChild(style);

  // 4) Build UI skeleton
  const panel = document.createElement('div');
  panel.className = 'panel';
  panel.innerHTML = `
    <div class="header" id="drag-handle" aria-label="Drag to move">
      <div class="lh">
        <span class="title">ChatGPT Conversation Downloader</span>
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
            <th data-key="workspace_id" aria-sort="none">workspace_id</th>
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
    <div class="summary" id="fetch-summary">Loaded 0 of 0 conversations</div>
    <div class="progress" id="fetch-progress">
      <div class="pbar"><div class="fill" id="fetch-pfill" style="width:0%"></div></div>
      <div class="text" id="fetch-ptext" aria-live="polite">0 of 0 conversations loaded</div>
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
      Note: Heavy usage may hit rate limits. Internal endpoints with cookies only; no tokens embedded.
    </div>
  `;
  shadow.appendChild(panel);

  // 5) Make panel draggable by header (no external libs)
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

  // 6) Auth + Fetch with retries (401→refresh token)
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

  // 7) List fetching + pager
  async function fetchList(offset = 0, append = false) {
    const url = `/backend-api/conversations?offset=${offset}&limit=${state.limit}&order=updated&is_archived=false&is_starred=false`;
    state.isFetching = true;
    updateSummary();
    setFetchProgress(state.items.length, state.total || state.items.length, { status: 'fetching' });
    try {
      const data = await fetchJSONWithRetries(url, { method: 'GET' });
      const items = Array.isArray(data?.items) ? data.items : [];
      state.total = typeof data?.total === 'number' ? data.total : items.length;
      state.offset = offset;
      const normalized = items.map(it => ({
        id: it.id,
        title: it.title || 'untitled',
        create_time: it.create_time || null,
        update_time: it.update_time || null,
        workspace_id: it.workspace_id || '-'
      }));
      if (append) state.items.push(...normalized);
      else state.items = normalized;
      updateCount();
      sortItems();
      renderRows();
      // NEW: hydrate persisted statuses on fetched items
      for (const it of state.items) {
        if (persisted.get(it.id)?.status === 'downloaded') {
          state.downloadStatus.set(it.id, 'downloaded');
        }
        refreshRowDL(it.id);
      }
      persistNow();
      updatePager();
      state.isFetching = false;
      updateSummary();
      setFetchProgress(state.items.length, state.total, { status: 'idle' });
    } catch (err) {
      state.isFetching = false;
      const message = err && err.message ? err.message : String(err);
      setFetchProgress(state.items.length, state.total || state.items.length, { status: 'error', message });
      updateSummary('Fetch failed. See console for details.');
      throw err;
    }
  }

  function updatePager() {
    const pager = shadow.getElementById('pager');
    const btn = shadow.getElementById('btn-more');
    if (state.items.length < state.total) {
      pager.style.display = '';
      btn.onclick = async () => {
        btn.disabled = true;
        try { await fetchList(state.items.length, true); }
        finally { btn.disabled = false; }
      };
    } else {
      pager.style.display = 'none';
    }
  }

  function updateCount() {
    shadow.getElementById('count-badge').textContent = `${state.items.length}`;
  }

  function updateSummary(message) {
    const summary = shadow.getElementById('fetch-summary');
    if (!summary) return;
    if (message) {
      summary.textContent = message;
      return;
    }
    if (state.isFetching) {
      summary.textContent = 'Fetching conversations…';
      return;
    }
    const knownTotal = typeof state.total === 'number' && state.total > 0 ? state.total : null;
    if (knownTotal) {
      summary.textContent = `Loaded ${state.items.length} of ${knownTotal} conversations`;
    } else {
      summary.textContent = `Loaded ${state.items.length} conversations`;
    }
  }

  function setFetchProgress(done, total, options = {}) {
    const { status = 'idle', message } = options;
    const fill = shadow.getElementById('fetch-pfill');
    const text = shadow.getElementById('fetch-ptext');
    if (!fill || !text) return;
    const max = total && total > 0 ? total : (done > 0 ? done : 0);
    const pct = max ? Math.min(100, Math.round((done / max) * 100)) : 0;
    fill.style.width = `${pct}%`;

    if (status === 'fetching') {
      const totalLabel = total && total > 0 ? `${total}` : '…';
      text.textContent = `Fetching conversations… (${done} of ${totalLabel})`;
    } else if (status === 'error') {
      text.textContent = message ? `Fetch failed: ${message}` : 'Fetch failed';
    } else {
      if (total && total > 0) {
        text.textContent = `${done} of ${total} conversations loaded`;
      } else {
        text.textContent = `${done} conversations loaded`;
      }
    }
  }

  // 8) Table rendering / sorting / filtering
  function sortItems() {
    const { key, dir } = state.sort;
    const mult = dir === 'asc' ? 1 : -1;
    state.items.sort((a, b) => {
      let va, vb;
      if (key === 'create_time' || key === 'update_time') {
        va = a[key] ? new Date(a[key]).getTime() : 0;
        vb = b[key] ? new Date(b[key]).getTime() : 0;
        return (va - vb) * mult;
      } else if (key === 'id' || key === 'workspace_id' || key === 'title') {
        va = String(a[key] || '');
        vb = String(b[key] || '');
        return va.localeCompare(vb) * mult;
      }
      return 0;
    });
  }

  function applyFilterToRow(tr, title) {
    if (!state.filterText) { tr.style.display = ''; return; }
    const show = title.toLowerCase().includes(state.filterText.toLowerCase());
    tr.style.display = show ? '' : 'none';
  }

  function renderRows() {
    const tbody = shadow.getElementById('tbody');
    tbody.innerHTML = '';
    state.rowsMap.clear();
    state.items.forEach((item, idx) => {
      const tr = document.createElement('tr');
      const tdIndex = document.createElement('td');
      tdIndex.className = 'index-col mono';
      tdIndex.textContent = `${idx + 1}`;

      // id column with middle truncation + copy
      const tdId = document.createElement('td');
      tdId.className = 'id-col mono';
      const idWrap = document.createElement('div');
      idWrap.className = 'id-row';
      const idText = document.createElement('span');
      idText.textContent = truncateMiddle(item.id, 8, 4);
      idText.title = item.id;
      const copyBtn = document.createElement('button');
      copyBtn.className = 'id-copy';
      copyBtn.type = 'button';
      copyBtn.title = 'Copy ID';
      copyBtn.innerHTML = icon.copy;
      copyBtn.onclick = async () => {
        try { await navigator.clipboard.writeText(item.id); copyBtn.style.filter = 'brightness(1.3)'; setTimeout(()=>copyBtn.style.filter='', 400); } catch(_) {}
      };
      idWrap.appendChild(idText);
      idWrap.appendChild(copyBtn);
      tdId.appendChild(idWrap);

      const tdTitle = document.createElement('td');
      tdTitle.textContent = item.title || 'untitled';

      const tdCT = document.createElement('td');
      tdCT.textContent = isoFromAny(item.create_time);

      const tdUT = document.createElement('td');
      tdUT.textContent = isoFromAny(item.update_time);

      const tdWS = document.createElement('td');
      tdWS.className = 'ws-col mono';
      tdWS.textContent = item.workspace_id || '-';

      const tdAPI = document.createElement('td');
      tdAPI.className = 'api-link';
      const a = document.createElement('a');
      a.href = apiUrl(`/backend-api/conversation/${encodeURIComponent(item.id)}`);
      a.textContent = a.href;
      a.target = '_blank';
      a.rel = 'noopener';
      tdAPI.appendChild(a);

      const tdDL = document.createElement('td');
      tdDL.className = 'dl';
      tdDL.innerHTML = icon.dot + '<span class="label"></span>';

      tr.appendChild(tdIndex);
      tr.appendChild(tdId);
      tr.appendChild(tdTitle);
      tr.appendChild(tdCT);
      tr.appendChild(tdUT);
      tr.appendChild(tdWS);
      tr.appendChild(tdAPI);
      tr.appendChild(tdDL);
      tbody.appendChild(tr);

      state.rowsMap.set(item.id, { tr, tdDL, tdTitle, tdUT });
      applyFilterToRow(tr, item.title || '');
      refreshRowDL(item.id);
    });
    updateSortHeaders();
  }

  function updateSortHeaders() {
    const thead = shadow.querySelectorAll('thead th');
    thead.forEach(th => {
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
    cell.innerHTML = ''; // reset
    // NEW: consider persisted status too
    const persistedStatus = persisted.get(id)?.status;
    if (state.downloadStatus.get(id) === 'downloaded' || persistedStatus === 'downloaded') {
      const ok = document.createElement('span'); ok.className = 'ok'; ok.innerHTML = icon.check;
      const t = document.createElement('span'); t.textContent = 'downloaded';
      cell.appendChild(ok); cell.appendChild(t);
      return;
    }
    const err = state.errors.get(id) || persisted.get(id)?.error;
    if (err) {
      const w = document.createElement('span'); w.className='warn'; w.title = String(err);
      w.innerHTML = icon.warn;
      const t = document.createElement('span'); t.textContent = 'error';
      cell.appendChild(w); cell.appendChild(t);
      return;
    }
    const dot = document.createElement('span'); dot.innerHTML = icon.dot;
    const t = document.createElement('span'); t.textContent = '';
    cell.appendChild(dot); cell.appendChild(t);
  }

  // 9) Events: sort, filter, refresh, close
  shadow.querySelectorAll('thead th').forEach(th => {
    th.addEventListener('click', () => {
      const key = th.getAttribute('data-key');
      if (key === 'api' || key === 'dl' || key === 'index') return;
      if (state.sort.key === key) state.sort.dir = state.sort.dir === 'asc' ? 'desc' : 'asc';
      else { state.sort.key = key; state.sort.dir = 'asc'; }
      sortItems();
      renderRows();
    });
  });

  shadow.getElementById('search').addEventListener('input', (e) => {
    state.filterText = e.target.value || '';
    for (const it of state.items) {
      const row = state.rowsMap.get(it.id);
      if (row) applyFilterToRow(row.tr, it.title || '');
    }
  });

  shadow.getElementById('btn-refresh').addEventListener('click', async () => {
    try {
      shadow.getElementById('btn-refresh').disabled = true;
      await fetchList(0, false);
    } finally {
      shadow.getElementById('btn-refresh').disabled = false;
    }
  });

  shadow.getElementById('btn-close').addEventListener('click', () => { host.remove(); });

  shadow.getElementById('concurrency').addEventListener('change', (e) => {
    const v = parseInt(e.target.value, 10);
    state.concurrency = clamp(isNaN(v) ? 3 : v, 1, 5);
    e.target.value = String(state.concurrency);
  });

  // 10) Conversion queue with adjustable concurrency
  function setConvertProgress(done, total) {
    const pfill = shadow.getElementById('convert-pfill');
    const ptext = shadow.getElementById('convert-ptext');
    const safeTotal = total || 0;
    const pct = safeTotal ? Math.round((done / safeTotal) * 100) : 0;
    pfill.style.width = `${pct}%`;
    ptext.textContent = `${done} of ${safeTotal} converted`;
  }

  // Save/Download progress (ready → waiting → saving → done/error)
  function setSaveProgress(done, total, options = {}) {
    const { status = 'idle', message } = options;
    const pfill = shadow.getElementById('save-pfill');
    const ptext = shadow.getElementById('save-ptext');
    if (!pfill || !ptext) return;
    const safeTotal = total || 0;
    const pct = safeTotal ? Math.min(100, Math.round((done / safeTotal) * 100)) : 0;
    pfill.style.width = `${pct}%`;

    if (status === 'ready') {
      ptext.textContent = safeTotal ? `Ready to save ${safeTotal} file${safeTotal === 1 ? '' : 's'}` : 'Nothing to save';
    } else if (status === 'waiting') {
      ptext.textContent = 'Waiting for folder selection…';
    } else if (status === 'saving') {
      const totalLabel = safeTotal || '…';
      ptext.textContent = `Saving… (${done} of ${totalLabel})`;
    } else if (status === 'done') {
      ptext.textContent = `Saved ${done} of ${safeTotal} file${safeTotal === 1 ? '' : 's'}`;
    } else if (status === 'error') {
      ptext.textContent = message ? `Save failed: ${message}` : 'Save failed';
    } else {
      ptext.textContent = safeTotal ? `${done} of ${safeTotal} saved` : 'Ready';
    }
  }

  // Helper: infer update_time from messages when API lacks it
  function inferUpdateTimeFromMessages(data) {
    try {
      const mapping = data?.mapping || {};
      let max = null;
      for (const n of Object.values(mapping)) {
        const ct = n?.message?.create_time;
        const num = typeof ct === 'number' ? ct : Number(ct);
        if (!Number.isNaN(num)) max = (max == null) ? num : Math.max(max, num);
      }
      return max;
    } catch {
      return null;
    }
  }

  function extractDetailMeta(data) {
    const detailUpdate = data?.update_time ?? data?.conversation?.update_time ?? null;
    const detailCreate = data?.create_time ?? data?.conversation?.create_time ?? null;
    const inferred = detailUpdate != null ? null : inferUpdateTimeFromMessages(data);
    return { update_time: detailUpdate ?? inferred ?? null, create_time: detailCreate ?? null };
  }

  async function ensureDetailMeta(ids) {
    const pending = ids.filter(id => !state.detailMeta.has(id));
    if (!pending.length) return;
    let idx = 0;
    const workers = Math.max(1, Math.min(5, parseInt(shadow.getElementById('concurrency').value, 10) || state.concurrency));
    async function worker() {
      while (true) {
        const i = idx++;
        if (i >= pending.length) break;
        const id = pending[i];
        try {
          const data = await fetchJSONWithRetries(`/backend-api/conversation/${encodeURIComponent(id)}`, { method: 'GET' });
          const meta = extractDetailMeta(data);
          const prev = state.detailMeta.get(id) || {};
          state.detailMeta.set(id, {
            update_time: meta.update_time ?? prev.update_time ?? null,
            create_time: meta.create_time ?? prev.create_time ?? null,
          });
          const item = state.items.find(it => it.id === id);
          if (item) {
            if (meta.update_time != null) item.update_time = meta.update_time;
            if (meta.create_time != null && (item.create_time == null)) item.create_time = meta.create_time;
            const row = state.rowsMap.get(id);
            if (row) row.tdUT.textContent = isoFromAny(item.update_time);
          }
        } catch (_) {
          /* ignore detail fetch failure here */
        }
      }
    }
    await Promise.all(Array.from({length: workers}, worker));
  }

  async function convertMissing(ids) {
    const pending = ids.filter(id => !state.mdMap.get(id));
    if (!pending.length) return;
    state.isConverting = true;
    const btn = shadow.getElementById('btn-download');
    if (btn) btn.disabled = true;

    let done = 0;
    const total = pending.length;
    setConvertProgress(0, total);
    let idx = 0;
    const workers = Math.max(1, Math.min(5, parseInt(shadow.getElementById('concurrency').value, 10) || state.concurrency));
    async function worker() {
      while (true) {
        const i = idx++;
        if (i >= pending.length) break;
        const id = pending[i];
        try {
          const data = await fetchJSONWithRetries(`/backend-api/conversation/${encodeURIComponent(id)}`, { method: 'GET' });
          const meta = extractDetailMeta(data);
          const prev = state.detailMeta.get(id) || {};
          state.detailMeta.set(id, {
            update_time: meta.update_time ?? prev.update_time ?? null,
            create_time: meta.create_time ?? prev.create_time ?? null,
          });
          const item = state.items.find(it => it.id === id);
          if (item) {
            if (meta.update_time != null) item.update_time = meta.update_time;
            if (meta.create_time != null && (item.create_time == null)) item.create_time = meta.create_time;
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
          setConvertProgress(done, total);
        }
      }
    }
    await Promise.all(Array.from({length: workers}, worker));
    state.isConverting = false;
    if (btn) btn.disabled = false;
  }


  // 11) Download helpers: File System Access API and throttled anchor fallback
  async function ensureDir() {
    if (state.dirHandle) return state.dirHandle;
    state.dirHandle = await pickDirectory();
    return state.dirHandle;
  }

  async function downloadAllToFolder(items, onProgress) {
    const dir = await ensureDir();
    if (!dir) throw new Error('No folder selected');
    let done = 0;
    for (const it of items) {
      // skip if already persisted as downloaded
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
      refreshRowDL(it.id);
      persistNow();
      await sleep(10);
      if (onProgress) onProgress(++done);
    }
  }

  async function downloadAllViaAnchors(items, onProgress) {
    // Smaller batches and longer pauses reduce throttling
    const BATCH = 5;
    const PAUSE_MS = 2000;
    let launched = 0;
    let done = 0;
    for (let i = 0; i < items.length; i++) {
      const it = items[i];
      if (persisted.get(it.id)?.status === 'downloaded') {
        state.downloadStatus.set(it.id, 'downloaded');
        refreshRowDL(it.id);
        if (onProgress) onProgress(++done);
        continue;
      }
      const md = state.mdMap.get(it.id);
      if (!md) { if (onProgress) onProgress(++done); continue; }
      try {
        await withBackoff(async () => {
          const blob = new Blob([md], { type: 'text/markdown;charset=utf-8' });
          const url = URL.createObjectURL(blob);
          const a = document.createElement('a');
          a.href = url; a.download = filenameFor(it);
          document.body.appendChild(a);
          a.click();
          a.remove();
          // longer revoke window to avoid premature revocation
          setTimeout(() => URL.revokeObjectURL(url), 10000);
        });
        state.downloadStatus.set(it.id, 'downloaded');
        state.errors.delete(it.id);
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

  // NEW: unified entry. Prefer FS, fall back to anchors.
  async function saveAllPreferFS(items) {
    // compute savable items (skip already-downloaded or without md)
    const savable = items.filter(it => !persisted.get(it.id)?.status === 'downloaded' ? true : state.downloadStatus.get(it.id) !== 'downloaded')
      .filter(it => state.mdMap.get(it.id));
    const total = savable.length;
    setSaveProgress(0, total, { status: 'waiting' });

    let progressed = 0;
    const onProgress = (n) => {
      progressed = n;
      setSaveProgress(progressed, total, { status: 'saving' });
    };

    if (supportsFS()) {
      const dir = await ensureDir();
      if (dir) { await downloadAllToFolder(savable, onProgress); setSaveProgress(progressed, total, { status: 'done' }); return; }
    }
    await downloadAllViaAnchors(savable, onProgress);
    setSaveProgress(progressed, total, { status: 'done' });
  }

  // 11) Download all: ensure detail meta, convert, then save
  shadow.getElementById('btn-download').addEventListener('click', async () => {
    const ids = state.items.map(it => it.id);
    await ensureDetailMeta(ids);
    await convertMissing(ids);
    // Indicate readiness before prompting for folder
    const totalSavable = state.items.filter(it => state.mdMap.get(it.id) && state.downloadStatus.get(it.id) !== 'downloaded' && persisted.get(it.id)?.status !== 'downloaded').length;
    setSaveProgress(0, totalSavable, { status: 'ready' });
    await saveAllPreferFS(state.items);
  });

  // Removed dedicated "Save All to Folder" button in favor of a single unified action

  // NEW: Retry Failed
  shadow.getElementById('btn-retry').addEventListener('click', async () => {
    const failed = state.items.filter(it => {
      const p = persisted.get(it.id);
      return state.errors.get(it.id) || (p && p.status === 'error');
    });
    if (!failed.length) return;
    const totalSavable = failed.filter(it => state.mdMap.get(it.id)).length;
    setSaveProgress(0, totalSavable, { status: 'ready' });
    await saveAllPreferFS(failed);
  });

  // 12) Converter functions (ported structure/behavior)
  function clean(s) {
    const t = String(s || '').trim();
    // remove citation-like markers 【...】 and common tracking footnotes like [^1], [#], etc.
    return t.replace(/\u3010.*?\u3011/g, '') // 【...】
            .replace(/\[\^\d+\]/g, '')
            .replace(/\[\d+\]/g, '');
  }

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
    if (m.metadata?.model_slug === 'tool' || role === 'tool' || role === 'function') return false;

    const c = m.content || {};
    const type = c.content_type || c.type || '';
    const parts = Array.isArray(c.parts) ? c.parts : [];
    const text = c.text ?? (typeof c === 'string' ? c : '');
    const hasText = (type === 'text' || type === 'multimodal_text') && (parts.some(p => typeof p === 'string' && p.trim()) || String(text || '').trim());
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
    // text / multimodal_text
    let body = '';
    if (Array.isArray(c.parts) && c.parts.length) {
      body = c.parts.map(p => (typeof p === 'string' ? p : (p?.text || ''))).join('\n\n');
    } else {
      body = String(c.text ?? '').trim();
    }
    return { kind: 'text', lang: '', body };
  }

  function renderMessage(node) {
    const m = node.message;
    const role = m.author?.role || 'assistant';
    const when = fmtTime(m.create_time);
    const head = `### ${role} ${when ? '· ' + when : ''}`.trim();
    const { kind, lang, body } = messageTextOrCode(node);
    if (!body) return head; // header only if body empty for any reason
    if (kind === 'code') {
      const fence = '```';
      return `${head}\n\n${fence}${lang ? lang : ''}\n${clean(body)}\n${fence}\n`;
    }
    return `${head}\n\n${clean(body)}\n`;
  }

  function dfsOrder(mapping) {
    // Build deterministic DFS over roots (no parent) and child-id lexical order
    const nodes = mapping || {};
    const roots = [];
    for (const [id, n] of Object.entries(nodes)) {
      if (!n?.parent) roots.push(id);
    }
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
      let u = m[0].replace(/[.,)>\]]+$/, ''); // strip trailing punctuation
      urls.push(u);
    }
    return urls;
  }

  function normalizeUrl(u) {
    try {
      const url = new URL(u, location.origin);
      if (!/^https?:$/.test(url.protocol)) return null;
      // remove utm_*, gclid, fbclid, strip hash
      url.hash = '';
      const toDelete = [];
      url.searchParams.forEach((_, k) => {
        if (/^utm_/i.test(k) || /^gclid$/i.test(k) || /^fbclid$/i.test(k)) toDelete.push(k);
      });
      for (const k of toDelete) url.searchParams.delete(k);
      // remove trailing slash (if not root)
      if (url.pathname.length > 1 && url.pathname.endsWith('/')) {
        url.pathname = url.pathname.replace(/\/+$/, '');
      }
      return url.toString();
    } catch { return null; }
  }

  function collectSources(data) {
    const set = new Set();

    // root metadata safe_urls
    const rootSafe = data?.metadata?.safe_urls;
    if (Array.isArray(rootSafe)) for (const u of rootSafe) {
      const n = normalizeUrl(u); if (n) set.add(n);
    }

    // root search_result_groups[*].results[*].(url|safe_urls[*])
    const srg = data?.metadata?.search_result_groups || data?.search_result_groups;
    if (Array.isArray(srg)) {
      for (const g of srg) {
        const results = g?.results || [];
        for (const r of results) {
          if (r?.url) { const n = normalizeUrl(r.url); if (n) set.add(n); }
          if (Array.isArray(r?.safe_urls)) for (const u of r.safe_urls) { const n = normalizeUrl(u); if (n) set.add(n); }
        }
      }
    }

    // content_references.items[*].(safe_urls|url)
    const crItems = data?.content_references?.items;
    if (Array.isArray(crItems)) {
      for (const it of crItems) {
        if (Array.isArray(it?.safe_urls)) for (const u of it.safe_urls) { const n = normalizeUrl(u); if (n) set.add(n); }
        if (it?.url) { const n = normalizeUrl(it.url); if (n) set.add(n); }
      }
    }

    // message-level metadata and text/code body
    const mapping = data?.mapping || {};
    for (const n of Object.values(mapping)) {
      const m = n?.message;
      if (!m) continue;
      const metaSafe = m?.metadata?.safe_urls;
      if (Array.isArray(metaSafe)) for (const u of metaSafe) {
        const nn = normalizeUrl(u); if (nn) set.add(nn);
      }
      const { kind, body } = messageTextOrCode(n);
      collectUrlsFromText(body).forEach(u => { const nn = normalizeUrl(u); if (nn) set.add(nn); });
    }

    return Array.from(set);
  }

  function convertJsonToMarkdown(input) {
    const title = input?.title || 'Conversation';
    const ordered = dfsOrder(input?.mapping || {});
    const visible = ordered.filter(shouldInclude);
    const lines = [];
    lines.push(`# ${title}`);
    lines.push('');

    if (visible.length) {
      for (const node of visible) {
        lines.push(renderMessage(node).trimEnd());
        lines.push('');
      }
    } else {
      // Fallback JSON snippet when no visible messages
      const snippet = {
        id: input?.id || null,
        title,
        update_time: input?.update_time || null,
        create_time: input?.create_time || null,
        note: 'No visible messages; raw snippet follows'
      };
      lines.push('```json');
      lines.push(JSON.stringify(snippet, null, 2));
      lines.push('```');
      lines.push('');
    }

    const sources = collectSources(input);
    lines.push('## Sources');
    if (sources.length) {
      for (const u of sources) lines.push(`- ${u}`);
    } else {
      lines.push('- (none)');
    }
    lines.push('');
    return lines.join('\n');
  }

  // INIT: default sort by update_time desc
  sortItems();
  renderRows();
  // Also reflect any persisted statuses on first paint
  for (const it of state.items) {
    if (persisted.get(it.id)?.status === 'downloaded') {
      state.downloadStatus.set(it.id, 'downloaded');
    }
    refreshRowDL(it.id);
  }
  persistNow();
  setConvertProgress(0, 0);
  setSaveProgress(0, 0, { status: 'idle' });
  setFetchProgress(0, 0, { status: 'idle' });
  updateSummary();

  // Initial fetch
  fetchList(0, false).catch(err => {
    console.error('Failed to fetch conversations:', err);
    try { shadow.getElementById('convert-ptext').textContent = 'Auth failed. Open any chat to refresh your session, then click "Refresh List".'; } catch {}
  });

})();
