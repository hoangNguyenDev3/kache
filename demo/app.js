/* =============================================
   Kache Live Demo — JavaScript
   ============================================= */

const API_BASE_URL = window.API_BASE_URL || 'https://kache.onrender.com';

let healthInterval = null;

/* =============================================
   Initialization
   ============================================= */

document.addEventListener('DOMContentLoaded', () => {
    setupTabs();
    setupAccordion();
    checkHealth();
    healthInterval = setInterval(checkHealth, 10000);
});

function setupTabs() {
    const tabs = document.querySelectorAll('.console-tab');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            const target = tab.dataset.tab;
            document.querySelectorAll('.tab-content').forEach(content => {
                content.classList.toggle('active', content.id === `tab-${target}`);
            });
        });
    });
}

function setupAccordion() {
    const headers = document.querySelectorAll('.accordion-header');
    headers.forEach(header => {
        header.addEventListener('click', () => {
            const item = header.closest('.accordion-item');
            const isOpen = item.classList.contains('open');

            // Close all items (accordion mode — only one open at a time)
            document.querySelectorAll('.accordion-item').forEach(i => i.classList.remove('open'));

            // Toggle the clicked item
            if (!isOpen) {
                item.classList.add('open');
            }
        });
    });
}

/* =============================================
   Health & Connection
   ============================================= */

async function checkHealth() {
    try {
        const res = await fetch(`${API_BASE_URL}/health`, { method: 'GET' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        // Health check still running to keep connection alive or log errors
        await res.json();
    } catch (err) {
        console.warn('Health check failed:', err.message);
    }
}

/* =============================================
   HTTP Helpers
   ============================================= */

async function apiRequest(method, endpoint, body = null) {
    const url = `${API_BASE_URL}${endpoint}`;
    const options = {
        method,
        headers: { 'Content-Type': 'application/json' }
    };
    if (body !== null) {
        options.body = JSON.stringify(body);
    }

    let status, responseText, responseData;
    const startTime = performance.now();

    try {
        const res = await fetch(url, options);
        status = res.status;
        responseText = await res.text();
        try {
            responseData = responseText ? JSON.parse(responseText) : null;
        } catch {
            responseData = responseText;
        }
    } catch (err) {
        status = 0;
        responseData = err.message;
        responseText = err.message;
    }

    const elapsed = (performance.now() - startTime).toFixed(1);
    addToHistory(method, endpoint, status, responseData, elapsed);
    return { status, data: responseData };
}

/* =============================================
   Command History
   ============================================= */

function addToHistory(method, url, status, response, elapsed) {
    const empty = document.getElementById('historyEmpty');
    if (empty) empty.style.display = 'none';

    const list = document.getElementById('historyList');
    const entry = document.createElement('div');
    entry.className = 'history-entry';

    const statusClass = status >= 200 && status < 300 ? 'status-success'
        : status >= 400 ? 'status-error'
            : status === 0 ? 'status-error'
                : 'status-warn';

    const statusLabel = status === 0 ? 'NETWORK ERROR'
        : status >= 200 && status < 300 ? `${status} OK`
            : `${status}`;

    const bodyText = typeof response === 'object'
        ? JSON.stringify(response, null, 2)
        : String(response);

    entry.innerHTML = `
    <div class="history-meta">
      <span class="history-time">${new Date().toLocaleTimeString()}</span>
      <span class="history-method method-${method.toLowerCase()}">${method}</span>
      <span class="history-url">${url}</span>
      <span class="history-status ${statusClass}">${statusLabel}</span>
      <span style="color:var(--text-muted);font-size:0.68rem;margin-left:4px;">${elapsed}ms</span>
    </div>
    <div class="history-body ${status >= 400 || status === 0 ? 'error' : ''}">${escapeHtml(bodyText)}</div>
  `;

    list.prepend(entry);
}

function clearHistory() {
    const list = document.getElementById('historyList');
    list.innerHTML = '';
    const empty = document.getElementById('historyEmpty');
    if (empty) empty.style.display = 'block';
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/* =============================================
   String Operations
   ============================================= */

function execSet() {
    const key = document.getElementById('strKey').value.trim();
    const value = document.getElementById('strValue').value;
    if (!key) { showToast('Key is required', 'error'); return; }
    apiRequest('POST', `/v1/kv/${encodeURIComponent(key)}`, value);
}

function execGet() {
    const key = document.getElementById('strKey').value.trim();
    if (!key) { showToast('Key is required', 'error'); return; }
    apiRequest('GET', `/v1/kv/${encodeURIComponent(key)}`);
}

function execDel() {
    const key = document.getElementById('strKey').value.trim();
    if (!key) { showToast('Key is required', 'error'); return; }
    apiRequest('DELETE', `/v1/kv/${encodeURIComponent(key)}`);
}

/* =============================================
   List Operations
   ============================================= */

function execLPush() {
    const key = document.getElementById('listKey').value.trim();
    const raw = document.getElementById('listValue').value;
    if (!key) { showToast('Key is required', 'error'); return; }
    const values = raw.split(',').map(v => v.trim()).filter(Boolean);
    if (values.length === 0) { showToast('Value(s) are required', 'error'); return; }
    apiRequest('POST', `/v1/list/${encodeURIComponent(key)}/lpush`, { values });
}

function execRPush() {
    const key = document.getElementById('listKey').value.trim();
    const raw = document.getElementById('listValue').value;
    if (!key) { showToast('Key is required', 'error'); return; }
    const values = raw.split(',').map(v => v.trim()).filter(Boolean);
    if (values.length === 0) { showToast('Value(s) are required', 'error'); return; }
    apiRequest('POST', `/v1/list/${encodeURIComponent(key)}/rpush`, { values });
}

function execLPop() {
    const key = document.getElementById('listKey').value.trim();
    if (!key) { showToast('Key is required', 'error'); return; }
    apiRequest('POST', `/v1/list/${encodeURIComponent(key)}/lpop`);
}

function execRPop() {
    const key = document.getElementById('listKey').value.trim();
    if (!key) { showToast('Key is required', 'error'); return; }
    apiRequest('POST', `/v1/list/${encodeURIComponent(key)}/rpop`);
}

function execLRange() {
    const key = document.getElementById('listKey').value.trim();
    const start = document.getElementById('listStart').value;
    const stop = document.getElementById('listStop').value;
    if (!key) { showToast('Key is required', 'error'); return; }
    const qs = new URLSearchParams({ start: start || '0', stop: stop || '-1' });
    apiRequest('GET', `/v1/list/${encodeURIComponent(key)}/range?${qs}`);
}

function execLLen() {
    const key = document.getElementById('listKey').value.trim();
    if (!key) { showToast('Key is required', 'error'); return; }
    apiRequest('GET', `/v1/list/${encodeURIComponent(key)}/len`);
}

/* =============================================
   Hash Operations
   ============================================= */

function execHSet() {
    const key = document.getElementById('hashKey').value.trim();
    const field = document.getElementById('hashField').value.trim();
    const value = document.getElementById('hashValue').value;
    if (!key || !field) { showToast('Key and Field are required', 'error'); return; }
    apiRequest('POST', `/v1/hash/${encodeURIComponent(key)}`, { field, value });
}

function execHGet() {
    const key = document.getElementById('hashKey').value.trim();
    const field = document.getElementById('hashField').value.trim();
    if (!key || !field) { showToast('Key and Field are required', 'error'); return; }
    apiRequest('GET', `/v1/hash/${encodeURIComponent(key)}/${encodeURIComponent(field)}`);
}

/* =============================================
   TTL Operations (not exposed via HTTP)
   ============================================= */

function execExpire() {
    showToast('EXPIRE is not exposed via the HTTP API. Use TCP/RESP protocol.', 'warn');
}

function execTtl() {
    showToast('TTL is not exposed via the HTTP API. Use TCP/RESP protocol.', 'warn');
}

/* =============================================
   Install Copy
   ============================================= */

function copyInstall() {
    const code = document.querySelector('.install-code code');
    navigator.clipboard.writeText(code.textContent).then(() => {
        showToast('Copied to clipboard!', 'success');
    }).catch(() => {
        showToast('Failed to copy', 'error');
    });
}

/* =============================================
   Toast Notifications
   ============================================= */

function showToast(message, type = 'info') {
    const existing = document.querySelector('.demo-toast');
    if (existing) existing.remove();

    const toast = document.createElement('div');
    toast.className = 'demo-toast';
    toast.style.cssText = `
    position: fixed;
    bottom: 24px;
    right: 24px;
    padding: 12px 20px;
    border-radius: 8px;
    font-size: 0.85rem;
    font-weight: 500;
    z-index: 1000;
    animation: slideUp 0.25s ease;
    max-width: 360px;
    line-height: 1.4;
  `;

    const colors = {
        error: { bg: 'rgba(248, 81, 73, 0.15)', border: 'rgba(248, 81, 73, 0.4)', color: '#f87171' },
        warn: { bg: 'rgba(210, 153, 34, 0.15)', border: 'rgba(210, 153, 34, 0.4)', color: '#facc15' },
        info: { bg: 'rgba(0, 212, 170, 0.15)', border: 'rgba(0, 212, 170, 0.4)', color: '#00d4aa' },
        success: { bg: 'rgba(59, 130, 246, 0.15)', border: 'rgba(59, 130, 246, 0.4)', color: '#60a5fa' }
    };

    const c = colors[type] || colors.info;
    toast.style.background = c.bg;
    toast.style.border = `1px solid ${c.border}`;
    toast.style.color = c.color;
    toast.textContent = message;

    document.body.appendChild(toast);
    setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateY(10px)';
        toast.style.transition = 'opacity 0.3s, transform 0.3s';
        setTimeout(() => toast.remove(), 300);
    }, 3500);
}

/* Add toast keyframe */
const toastStyle = document.createElement('style');
toastStyle.textContent = `
  @keyframes slideUp {
    from { opacity: 0; transform: translateY(12px); }
    to { opacity: 1; transform: translateY(0); }
  }
`;
document.head.appendChild(toastStyle);
