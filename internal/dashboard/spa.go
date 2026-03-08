package dashboard

const dashboardSPA = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>intu Dashboard</title>
  <style>
    :root {
      --bg-primary: #0f172a; --bg-secondary: #1e293b; --bg-tertiary: #334155;
      --text-primary: #f1f5f9; --text-secondary: #94a3b8; --text-muted: #64748b;
      --accent: #38bdf8; --accent-hover: #7dd3fc;
      --success: #22c55e; --error: #ef4444; --warning: #f59e0b;
      --border: #334155; --radius: 12px;
    }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: var(--bg-primary); color: var(--text-primary); min-height: 100vh; }
    .header { background: var(--bg-secondary); padding: 16px 32px; border-bottom: 1px solid var(--border); display: flex; align-items: center; justify-content: space-between; position: sticky; top: 0; z-index: 100; }
    .header h1 { font-size: 1.4rem; color: var(--accent); font-weight: 700; }
    .header nav { display: flex; gap: 8px; }
    .header nav button { background: transparent; border: 1px solid var(--border); color: var(--text-secondary); padding: 6px 16px; border-radius: 6px; cursor: pointer; font-size: 0.85rem; transition: all 0.2s; }
    .header nav button:hover, .header nav button.active { background: var(--accent); color: var(--bg-primary); border-color: var(--accent); }
    .container { max-width: 1400px; margin: 0 auto; padding: 24px 32px; }
    .page { display: none; }
    .page.active { display: block; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(380px, 1fr)); gap: 16px; }
    .card { background: var(--bg-secondary); border-radius: var(--radius); padding: 20px; border: 1px solid var(--border); transition: border-color 0.2s; }
    .card:hover { border-color: var(--accent); }
    .card-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
    .card-header h3 { color: var(--accent); font-size: 1rem; font-weight: 600; }
    .badge { display: inline-block; padding: 2px 10px; border-radius: 20px; font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }
    .badge-enabled { background: rgba(34,197,94,0.15); color: var(--success); }
    .badge-disabled { background: rgba(239,68,68,0.15); color: var(--error); }
    .badge-error { background: rgba(239,68,68,0.15); color: var(--error); }
    .badge-sent { background: rgba(34,197,94,0.15); color: var(--success); }
    .badge-received { background: rgba(56,189,248,0.15); color: var(--accent); }
    .badge-transformed { background: rgba(168,85,247,0.15); color: #a855f7; }
    .badge-filtered { background: rgba(245,158,11,0.15); color: var(--warning); }
    .badge-reprocessed { background: rgba(56,189,248,0.15); color: var(--accent); }
    .detail { color: var(--text-secondary); font-size: 0.85rem; margin-top: 6px; }
    .detail strong { color: var(--text-primary); }
    .section { margin-top: 28px; }
    .section-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
    .section h2 { color: var(--text-primary); font-size: 1.2rem; }
    .btn { background: var(--accent); color: var(--bg-primary); border: none; padding: 8px 16px; border-radius: 6px; cursor: pointer; font-size: 0.85rem; font-weight: 600; transition: background 0.2s; }
    .btn:hover { background: var(--accent-hover); }
    .btn-sm { padding: 4px 12px; font-size: 0.75rem; }
    .btn-danger { background: var(--error); color: white; }
    .btn-danger:hover { background: #dc2626; }
    .btn-outline { background: transparent; border: 1px solid var(--border); color: var(--text-secondary); }
    .btn-outline:hover { border-color: var(--accent); color: var(--accent); }
    .btn-group { display: flex; gap: 6px; }
    table { width: 100%; border-collapse: collapse; background: var(--bg-secondary); border-radius: var(--radius); overflow: hidden; border: 1px solid var(--border); }
    th { text-align: left; padding: 12px 16px; font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; background: var(--bg-tertiary); border-bottom: 1px solid var(--border); }
    td { padding: 10px 16px; font-size: 0.85rem; border-bottom: 1px solid var(--border); color: var(--text-secondary); }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(56,189,248,0.05); }
    .filters { display: flex; gap: 12px; margin-bottom: 16px; flex-wrap: wrap; align-items: center; }
    .filters select, .filters input { background: var(--bg-secondary); border: 1px solid var(--border); color: var(--text-primary); padding: 8px 12px; border-radius: 6px; font-size: 0.85rem; }
    .filters select:focus, .filters input:focus { outline: none; border-color: var(--accent); }
    pre { background: var(--bg-primary); padding: 16px; border-radius: 8px; overflow-x: auto; font-size: 0.8rem; color: var(--text-secondary); border: 1px solid var(--border); }
    .metric-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 12px; }
    .metric-card { background: var(--bg-secondary); padding: 16px; border-radius: var(--radius); border: 1px solid var(--border); text-align: center; }
    .metric-card .value { font-size: 2rem; font-weight: 700; color: var(--accent); }
    .metric-card .label { font-size: 0.75rem; color: var(--text-muted); margin-top: 4px; text-transform: uppercase; }
    .tabs { display: flex; gap: 4px; border-bottom: 1px solid var(--border); margin-bottom: 16px; }
    .tab { background: none; border: none; color: var(--text-muted); padding: 8px 16px; cursor: pointer; font-size: 0.85rem; border-bottom: 2px solid transparent; transition: all 0.2s; }
    .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
    .empty { text-align: center; padding: 40px; color: var(--text-muted); }
    .modal-overlay { display: none; position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.6); z-index: 200; justify-content: center; align-items: center; }
    .modal-overlay.show { display: flex; }
    .modal { background: var(--bg-secondary); border-radius: var(--radius); padding: 24px; max-width: 700px; width: 90%; max-height: 80vh; overflow-y: auto; border: 1px solid var(--border); }
    .modal h3 { color: var(--accent); margin-bottom: 16px; }
    .auto-refresh { font-size: 0.75rem; color: var(--text-muted); }
  </style>
</head>
<body>
  <div class="header">
    <h1>intu Dashboard</h1>
    <nav>
      <button class="active" onclick="showPage('channels')">Channels</button>
      <button onclick="showPage('messages')">Messages</button>
      <button onclick="showPage('metrics')">Metrics</button>
    </nav>
  </div>

  <div class="container">
    <div id="page-channels" class="page active">
      <div class="section">
        <div class="section-header">
          <h2>Channels</h2>
          <span class="auto-refresh">Auto-refreshes every 10s</span>
        </div>
        <div class="grid" id="channel-list"></div>
      </div>
    </div>

    <div id="page-messages" class="page">
      <div class="section">
        <h2>Message Browser</h2>
        <div class="filters">
          <select id="msg-channel"><option value="">All Channels</option></select>
          <select id="msg-status">
            <option value="">All Statuses</option>
            <option value="RECEIVED">Received</option>
            <option value="TRANSFORMED">Transformed</option>
            <option value="SENT">Sent</option>
            <option value="ERROR">Error</option>
            <option value="FILTERED">Filtered</option>
            <option value="REPROCESSED">Reprocessed</option>
          </select>
          <input type="date" id="msg-since" placeholder="Since">
          <input type="number" id="msg-limit" value="50" min="1" max="500" style="width:80px">
          <button class="btn btn-sm" onclick="loadMessages()">Search</button>
        </div>
        <div id="message-list"></div>
      </div>
    </div>

    <div id="page-metrics" class="page">
      <div class="section">
        <div class="section-header">
          <h2>Metrics</h2>
          <span class="auto-refresh">Auto-refreshes every 5s</span>
        </div>
        <div class="metric-grid" id="metric-cards"></div>
        <div class="section" style="margin-top:24px">
          <h2>Raw Metrics</h2>
          <pre id="metrics-raw">Loading...</pre>
        </div>
      </div>
    </div>
  </div>

  <div class="modal-overlay" id="msg-modal">
    <div class="modal">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <h3>Message Detail</h3>
        <button class="btn btn-sm btn-outline" onclick="closeModal()">Close</button>
      </div>
      <div id="msg-detail"></div>
    </div>
  </div>

  <script>
    let channels = [];
    let metricsData = {};
    let refreshInterval;

    function showPage(page) {
      document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
      document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
      document.getElementById('page-' + page).classList.add('active');
      event.target.classList.add('active');
      if (page === 'messages') loadMessages();
      if (page === 'metrics') loadMetrics();
    }

    function statusBadge(status) {
      const cls = {
        'ERROR': 'badge-error', 'SENT': 'badge-sent', 'RECEIVED': 'badge-received',
        'TRANSFORMED': 'badge-transformed', 'FILTERED': 'badge-filtered', 'REPROCESSED': 'badge-reprocessed'
      };
      return '<span class="badge ' + (cls[status] || '') + '">' + status + '</span>';
    }

    async function loadChannels() {
      try {
        const r = await fetch('/api/channels');
        channels = await r.json();
        const el = document.getElementById('channel-list');
        if (!channels || !channels.length) { el.innerHTML = '<div class="empty">No channels found.</div>'; return; }
        el.innerHTML = channels.map(c =>
          '<div class="card">' +
            '<div class="card-header">' +
              '<h3>' + c.id + '</h3>' +
              '<span class="badge ' + (c.enabled ? 'badge-enabled' : 'badge-disabled') + '">' + (c.enabled ? 'enabled' : 'disabled') + '</span>' +
            '</div>' +
            '<div class="detail"><strong>Listener:</strong> ' + (c.listener || 'n/a') + '</div>' +
            '<div class="detail"><strong>Destinations:</strong> ' + ((c.destinations || []).join(', ') || 'none') + '</div>' +
            (c.tags ? '<div class="detail"><strong>Tags:</strong> ' + c.tags.join(', ') + '</div>' : '') +
            (c.group ? '<div class="detail"><strong>Group:</strong> ' + c.group + '</div>' : '') +
            '<div class="btn-group" style="margin-top:12px">' +
              (c.enabled
                ? '<button class="btn btn-sm btn-danger" onclick="channelAction(\'' + c.id + '\',\'undeploy\')">Undeploy</button>'
                  + '<button class="btn btn-sm" onclick="channelAction(\'' + c.id + '\',\'deploy\')">Deploy</button>'
                : '<button class="btn btn-sm" onclick="channelAction(\'' + c.id + '\',\'deploy\')">Deploy</button>') +
              '<button class="btn btn-sm btn-outline" onclick="channelAction(\'' + c.id + '\',\'restart\')">Restart</button>' +
            '</div>' +
          '</div>'
        ).join('');

        const sel = document.getElementById('msg-channel');
        const current = sel.value;
        sel.innerHTML = '<option value="">All Channels</option>' + channels.map(c => '<option value="' + c.id + '">' + c.id + '</option>').join('');
        sel.value = current;
      } catch(e) { console.error('Failed to load channels', e); }
    }

    async function channelAction(id, action) {
      try {
        await fetch('/api/channels/' + id + '/' + action, { method: 'POST' });
        loadChannels();
      } catch(e) { alert('Action failed: ' + e.message); }
    }

    async function loadMessages() {
      const ch = document.getElementById('msg-channel').value;
      const st = document.getElementById('msg-status').value;
      const since = document.getElementById('msg-since').value;
      const limit = document.getElementById('msg-limit').value || '50';
      let url = '/api/messages?limit=' + limit;
      if (ch) url += '&channel=' + ch;
      if (st) url += '&status=' + st;
      if (since) url += '&since=' + since + 'T00:00:00Z';
      try {
        const r = await fetch(url);
        const msgs = await r.json();
        const el = document.getElementById('message-list');
        if (!msgs || !msgs.length) { el.innerHTML = '<div class="empty">No messages found.</div>'; return; }
        el.innerHTML = '<table><thead><tr><th>ID</th><th>Channel</th><th>Stage</th><th>Status</th><th>Time</th><th>Actions</th></tr></thead><tbody>' +
          msgs.map(m =>
            '<tr>' +
              '<td style="font-family:monospace;font-size:0.75rem">' + (m.ID || '').substring(0, 12) + '...</td>' +
              '<td>' + (m.ChannelID || '') + '</td>' +
              '<td>' + (m.Stage || '') + '</td>' +
              '<td>' + statusBadge(m.Status || '') + '</td>' +
              '<td style="font-size:0.75rem">' + new Date(m.Timestamp).toLocaleString() + '</td>' +
              '<td><div class="btn-group">' +
                '<button class="btn btn-sm btn-outline" onclick="viewMessage(\'' + m.ID + '\')">View</button>' +
                (m.Status === 'ERROR' ? '<button class="btn btn-sm" onclick="reprocessMessage(\'' + m.ID + '\')">Reprocess</button>' : '') +
              '</div></td>' +
            '</tr>'
          ).join('') +
          '</tbody></table>';
      } catch(e) { console.error('Failed to load messages', e); }
    }

    async function viewMessage(id) {
      try {
        const r = await fetch('/api/messages/' + id);
        const data = await r.json();
        const el = document.getElementById('msg-detail');
        const msg = data.message || data;
        let html = '<div style="margin-top:12px">';
        html += '<div class="detail"><strong>ID:</strong> ' + (msg.ID || '') + '</div>';
        html += '<div class="detail"><strong>Correlation:</strong> ' + (msg.CorrelationID || '') + '</div>';
        html += '<div class="detail"><strong>Channel:</strong> ' + (msg.ChannelID || '') + '</div>';
        html += '<div class="detail"><strong>Stage:</strong> ' + (msg.Stage || '') + '</div>';
        html += '<div class="detail"><strong>Status:</strong> ' + statusBadge(msg.Status || '') + '</div>';
        html += '<div class="detail"><strong>Timestamp:</strong> ' + new Date(msg.Timestamp).toLocaleString() + '</div>';
        if (msg.Content) {
          let content;
          try { content = atob(msg.Content); } catch(e) { content = msg.Content; }
          html += '<div style="margin-top:12px"><strong>Content:</strong><pre>' + escapeHtml(content) + '</pre></div>';
        }
        if (data.stages && data.stages.length) {
          html += '<div style="margin-top:16px"><strong>All Stages:</strong>';
          html += '<table style="margin-top:8px"><thead><tr><th>Stage</th><th>Status</th><th>Time</th></tr></thead><tbody>';
          data.stages.forEach(s => {
            html += '<tr><td>' + s.Stage + '</td><td>' + statusBadge(s.Status) + '</td><td style="font-size:0.75rem">' + new Date(s.Timestamp).toLocaleString() + '</td></tr>';
          });
          html += '</tbody></table></div>';
        }
        if (msg.Status === 'ERROR') {
          html += '<div style="margin-top:12px"><button class="btn" onclick="reprocessMessage(\'' + msg.ID + '\');closeModal();">Reprocess</button></div>';
        }
        html += '</div>';
        el.innerHTML = html;
        document.getElementById('msg-modal').classList.add('show');
      } catch(e) { alert('Failed to load message: ' + e.message); }
    }

    async function reprocessMessage(id) {
      if (!confirm('Reprocess message ' + id + '?')) return;
      try {
        const r = await fetch('/api/messages/' + id + '/reprocess', { method: 'POST' });
        const data = await r.json();
        if (data.reprocessed) { alert('Message reprocessed. New ID: ' + data.new_message_id); loadMessages(); }
        else { alert('Reprocess failed'); }
      } catch(e) { alert('Reprocess failed: ' + e.message); }
    }

    function closeModal() { document.getElementById('msg-modal').classList.remove('show'); }

    async function loadMetrics() {
      try {
        const r = await fetch('/api/metrics');
        metricsData = await r.json();
        const el = document.getElementById('metric-cards');
        const raw = document.getElementById('metrics-raw');
        raw.textContent = JSON.stringify(metricsData, null, 2);

        let cards = '';
        const keys = Object.keys(metricsData).sort();
        for (const k of keys) {
          const v = metricsData[k];
          if (typeof v === 'number') {
            cards += '<div class="metric-card"><div class="value">' + formatNumber(v) + '</div><div class="label">' + k + '</div></div>';
          }
        }
        if (!cards) cards = '<div class="empty">No numeric metrics available.</div>';
        el.innerHTML = cards;
      } catch(e) { console.error('Failed to load metrics', e); }
    }

    function formatNumber(n) {
      if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
      if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
      return n.toString();
    }

    function escapeHtml(str) {
      return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    }

    loadChannels();
    loadMetrics();
    setInterval(loadChannels, 10000);
    setInterval(loadMetrics, 5000);
  </script>
</body>
</html>`
