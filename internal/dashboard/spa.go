package dashboard

const dashboardSPA = `<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>intu Dashboard</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg width='32' height='32' viewBox='0 0 100 100' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Crect width='100' height='100' rx='24' fill='%230ea5e9'/%3E%3Cpath d='M50 15C50 15 52 30 65 32C52 34 50 49 50 49C50 49 48 34 35 32C48 30 50 15 50 15Z' fill='white'/%3E%3Crect x='42' y='55' width='16' height='30' rx='5' fill='white'/%3E%3C/svg%3E">
  <script>
    (function(){var t=localStorage.getItem('intu-theme');if(t==='light')document.documentElement.classList.remove('dark');})();
  </script>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          fontFamily: { sans: ['Inter', 'system-ui', '-apple-system', 'sans-serif'] }
        }
      }
    };
  </script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    @keyframes fadeUp { from { opacity: 0; transform: translateY(12px); } to { opacity: 1; transform: translateY(0); } }
    @keyframes shimmer { 0% { background-position: -200% 0; } 100% { background-position: 200% 0; } }
    @keyframes pulseLine { 0%,100% { opacity: 0.3; } 50% { opacity: 1; } }
    .fade-up { animation: fadeUp 0.35s ease-out both; }
    .dark .skeleton { background: linear-gradient(90deg, #1e293b 25%, #334155 50%, #1e293b 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 8px; }
    .skeleton { background: linear-gradient(90deg, #f1f5f9 25%, #e2e8f0 50%, #f1f5f9 75%); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 8px; }
    .pulse-line { animation: pulseLine 2s ease-in-out infinite; }
    .dark ::-webkit-scrollbar { width: 6px; }
    .dark ::-webkit-scrollbar-track { background: transparent; }
    .dark ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
    .dark ::-webkit-scrollbar-thumb:hover { background: #475569; }
  </style>
</head>
<body class="bg-gray-50 dark:bg-slate-950 text-gray-700 dark:text-slate-200 min-h-screen font-sans transition-colors duration-200">

  <!-- Header -->
  <header class="sticky top-0 z-50 bg-white/80 dark:bg-slate-900/80 backdrop-blur-xl border-b border-gray-200/80 dark:border-slate-800/80 transition-colors duration-200">
    <div class="max-w-[1600px] mx-auto px-6 h-14 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <svg width="32" height="32" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg">
          <rect width="100" height="100" rx="24" fill="#0ea5e9"/>
          <path d="M50 15C50 15 52 30 65 32C52 34 50 49 50 49C50 49 48 34 35 32C48 30 50 15 50 15Z" fill="white"/>
          <rect x="42" y="55" width="16" height="30" rx="5" fill="white"/>
        </svg>
        <span class="text-xl font-extrabold tracking-tight text-gray-900 dark:text-white">intu<span class="text-sky-500">.dev</span></span>
      </div>
      <nav class="flex items-center gap-1" id="main-nav">
        <button onclick="navigate('home')" data-page="home" class="nav-btn px-4 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 text-gray-500 dark:text-slate-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-slate-800">Home</button>
        <button onclick="navigate('channels')" data-page="channels" class="nav-btn px-4 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 text-gray-500 dark:text-slate-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-slate-800">Channels</button>
        <button onclick="navigate('messages')" data-page="messages" class="nav-btn px-4 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 text-gray-500 dark:text-slate-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-slate-800">Messages</button>
        <div class="w-px h-5 bg-gray-200 dark:bg-slate-700 mx-2"></div>
        <button onclick="toggleTheme()" id="theme-toggle" class="p-2 rounded-lg text-gray-400 dark:text-slate-500 hover:text-gray-600 dark:hover:text-slate-300 hover:bg-gray-100 dark:hover:bg-slate-800 transition-all duration-200" title="Toggle theme">
          <svg id="icon-sun" class="w-4 h-4 hidden" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="12" cy="12" r="5"/><path d="M12 1v2m0 18v2M4.22 4.22l1.42 1.42m12.72 12.72l1.42 1.42M1 12h2m18 0h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>
          <svg id="icon-moon" class="w-4 h-4 hidden" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z"/></svg>
        </button>
        <div class="w-px h-5 bg-gray-200 dark:bg-slate-700 mx-1"></div>
        <a href="/logout" class="px-3 py-1.5 rounded-lg text-sm text-gray-400 dark:text-slate-500 hover:text-red-500 dark:hover:text-red-400 hover:bg-red-50 dark:hover:bg-red-400/10 transition-all duration-200">Logout</a>
      </nav>
    </div>
  </header>

  <!-- Main content -->
  <main class="max-w-[1600px] mx-auto px-6 py-6">
    <!-- Home Page -->
    <div id="page-home" class="page hidden">
      <div class="mb-6">
        <h1 class="text-2xl font-bold text-gray-900 dark:text-white">Dashboard</h1>
        <p class="text-gray-400 dark:text-slate-500 text-sm mt-1">System overview and message activity</p>
      </div>
      <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-8" id="stats-grid">
        <div class="skeleton h-24 rounded-xl"></div><div class="skeleton h-24 rounded-xl"></div>
        <div class="skeleton h-24 rounded-xl"></div><div class="skeleton h-24 rounded-xl"></div>
        <div class="skeleton h-24 rounded-xl"></div><div class="skeleton h-24 rounded-xl"></div>
      </div>
      <div class="bg-white dark:bg-slate-900/50 border border-gray-200 dark:border-slate-800/80 rounded-2xl p-6 transition-colors duration-200">
        <h2 class="text-base font-semibold text-gray-900 dark:text-white mb-4">Channel Message Volume</h2>
        <div class="h-72" id="chart-container">
          <canvas id="volume-chart"></canvas>
        </div>
      </div>
    </div>

    <!-- Channels Page -->
    <div id="page-channels" class="page hidden">
      <div class="flex items-center justify-between mb-6">
        <div>
          <h1 class="text-2xl font-bold text-gray-900 dark:text-white">Channels</h1>
          <p class="text-gray-400 dark:text-slate-500 text-sm mt-1">Manage and monitor integration channels</p>
        </div>
        <span class="text-xs text-gray-400 dark:text-slate-600">Auto-refreshes every 10s</span>
      </div>
      <div class="flex flex-wrap gap-3 mb-6">
        <input type="text" id="ch-search" placeholder="Search channels..." oninput="filterChannels()"
          class="px-4 py-2 bg-white dark:bg-slate-900/60 border border-gray-300 dark:border-slate-700/50 rounded-xl text-sm text-gray-800 dark:text-white placeholder-gray-400 dark:placeholder-slate-500 focus:outline-none focus:border-sky-400/50 w-64 transition-all">
        <select id="ch-source-filter" onchange="filterChannels()"
          class="px-4 py-2 bg-white dark:bg-slate-900/60 border border-gray-300 dark:border-slate-700/50 rounded-xl text-sm text-gray-600 dark:text-slate-300 focus:outline-none focus:border-sky-400/50 transition-all cursor-pointer">
          <option value="">All Sources</option>
        </select>
        <select id="ch-dest-filter" onchange="filterChannels()"
          class="px-4 py-2 bg-white dark:bg-slate-900/60 border border-gray-300 dark:border-slate-700/50 rounded-xl text-sm text-gray-600 dark:text-slate-300 focus:outline-none focus:border-sky-400/50 transition-all cursor-pointer">
          <option value="">All Destinations</option>
        </select>
        <select id="ch-status-filter" onchange="filterChannels()"
          class="px-4 py-2 bg-white dark:bg-slate-900/60 border border-gray-300 dark:border-slate-700/50 rounded-xl text-sm text-gray-600 dark:text-slate-300 focus:outline-none focus:border-sky-400/50 transition-all cursor-pointer">
          <option value="">All Status</option>
          <option value="enabled">Enabled</option>
          <option value="disabled">Disabled</option>
        </select>
      </div>
      <div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4" id="channel-list"></div>
    </div>

    <!-- Messages Page -->
    <div id="page-messages" class="page hidden">
      <div class="flex items-center justify-between mb-6">
        <div>
          <h1 class="text-2xl font-bold text-gray-900 dark:text-white">Messages</h1>
          <p class="text-gray-400 dark:text-slate-500 text-sm mt-1">Browse and inspect message flow</p>
        </div>
      </div>
      <div class="flex flex-wrap gap-3 mb-6">
        <select id="msg-channel" onchange="loadMessages()"
          class="px-4 py-2 bg-white dark:bg-slate-900/60 border border-gray-300 dark:border-slate-700/50 rounded-xl text-sm text-gray-600 dark:text-slate-300 focus:outline-none focus:border-sky-400/50 transition-all cursor-pointer">
          <option value="">All Channels</option>
        </select>
        <select id="msg-limit" onchange="loadMessages()"
          class="px-4 py-2 bg-white dark:bg-slate-900/60 border border-gray-300 dark:border-slate-700/50 rounded-xl text-sm text-gray-600 dark:text-slate-300 focus:outline-none focus:border-sky-400/50 transition-all cursor-pointer">
          <option value="10">Last 10</option>
          <option value="25">Last 25</option>
          <option value="50" selected>Last 50</option>
          <option value="100">Last 100</option>
        </select>
      </div>
      <div id="message-list"></div>
    </div>
  </main>

  <!-- Channel detail slide-over -->
  <div id="slideover-backdrop" class="fixed inset-0 bg-black/50 dark:bg-black/60 backdrop-blur-sm z-40 hidden transition-opacity duration-300 opacity-0" onclick="closeSlideOver()"></div>
  <div id="slideover-panel" class="fixed right-0 top-0 h-full w-[640px] max-w-[92vw] bg-white dark:bg-slate-900 border-l border-gray-200 dark:border-slate-800 z-50 transform translate-x-full transition-transform duration-300 overflow-y-auto">
    <div id="slideover-content" class="p-6"></div>
  </div>

  <!-- Toast -->
  <div id="toast" class="fixed bottom-6 right-6 z-[999] flex flex-col gap-2 pointer-events-none"></div>

<script>
var state = { page: 'home', channels: [], stats: {}, volumeChart: null };

// --- Theme ---
function toggleTheme() {
  var html = document.documentElement;
  if (html.classList.contains('dark')) {
    html.classList.remove('dark');
    localStorage.setItem('intu-theme', 'light');
  } else {
    html.classList.add('dark');
    localStorage.setItem('intu-theme', 'dark');
  }
  updateThemeIcon();
  updateChartTheme();
}

function updateThemeIcon() {
  var isDark = document.documentElement.classList.contains('dark');
  document.getElementById('icon-sun').style.display = isDark ? 'block' : 'none';
  document.getElementById('icon-moon').style.display = isDark ? 'none' : 'block';
}

function updateChartTheme() {
  if (!state.volumeChart) return;
  var isDark = document.documentElement.classList.contains('dark');
  var gridColor = isDark ? 'rgba(51,65,85,0.3)' : 'rgba(226,232,240,0.7)';
  var tickColor = isDark ? '#64748b' : '#94a3b8';
  var legendColor = isDark ? '#94a3b8' : '#6b7280';
  state.volumeChart.options.scales.x.ticks.color = tickColor;
  state.volumeChart.options.scales.x.grid.color = 'transparent';
  state.volumeChart.options.scales.y.ticks.color = tickColor;
  state.volumeChart.options.scales.y.grid.color = gridColor;
  state.volumeChart.options.plugins.legend.labels.color = legendColor;
  state.volumeChart.update('none');
}

updateThemeIcon();

// --- Navigation ---
function navigate(page) {
  state.page = page;
  document.querySelectorAll('.page').forEach(function(p) { p.classList.add('hidden'); });
  var el = document.getElementById('page-' + page);
  if (el) { el.classList.remove('hidden'); el.classList.add('fade-up'); }
  document.querySelectorAll('.nav-btn').forEach(function(b) {
    if (b.getAttribute('data-page') === page) {
      b.className = 'nav-btn px-4 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 text-sky-500 dark:text-sky-400 bg-sky-50 dark:bg-sky-400/10';
    } else {
      b.className = 'nav-btn px-4 py-1.5 rounded-lg text-sm font-medium transition-all duration-200 text-gray-500 dark:text-slate-400 hover:text-gray-900 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-slate-800';
    }
  });
  if (page === 'home') fetchStats();
  if (page === 'channels') fetchChannels();
  if (page === 'messages') { populateChannelFilter(); loadMessages(); }
}

// --- Stats / Home ---
function fetchStats() {
  fetch('/api/stats').then(function(r) { return r.json(); }).then(function(data) {
    state.stats = data;
    renderStats(data);
  }).catch(function(e) { console.error('Stats fetch failed', e); });
}

function renderStats(d) {
  var mc = d.message_counts || {};
  var cards = [
    { label: 'Total Channels', value: d.total_channels || 0, color: 'sky' },
    { label: 'Active Channels', value: d.active_channels || 0, color: 'emerald' },
    { label: 'Messages / 60s', value: mc.last_60s || 0, color: 'violet' },
    { label: 'Messages / 5m', value: mc.last_5m || 0, color: 'amber' },
    { label: 'Messages / 1h', value: mc.last_1h || 0, color: 'blue' },
    { label: 'Messages / 24h', value: mc.last_24h || 0, color: 'rose' }
  ];
  var colorMap = {
    sky:     'bg-sky-50 dark:bg-sky-400/10 text-sky-600 dark:text-sky-400 border border-sky-200 dark:border-sky-400/20',
    emerald: 'bg-emerald-50 dark:bg-emerald-400/10 text-emerald-600 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-400/20',
    violet:  'bg-violet-50 dark:bg-violet-400/10 text-violet-600 dark:text-violet-400 border border-violet-200 dark:border-violet-400/20',
    amber:   'bg-amber-50 dark:bg-amber-400/10 text-amber-600 dark:text-amber-400 border border-amber-200 dark:border-amber-400/20',
    blue:    'bg-blue-50 dark:bg-blue-400/10 text-blue-600 dark:text-blue-400 border border-blue-200 dark:border-blue-400/20',
    rose:    'bg-rose-50 dark:bg-rose-400/10 text-rose-600 dark:text-rose-400 border border-rose-200 dark:border-rose-400/20'
  };
  var html = '';
  cards.forEach(function(c, i) {
    html += '<div class="' + colorMap[c.color] + ' rounded-xl p-4 fade-up transition-colors duration-200" style="animation-delay:' + (i * 50) + 'ms">' +
      '<div class="text-2xl font-bold">' + formatNum(c.value) + '</div>' +
      '<div class="text-xs mt-1 opacity-70">' + c.label + '</div>' +
    '</div>';
  });
  document.getElementById('stats-grid').innerHTML = html;
  renderVolumeChart(d.channel_volume || []);
}

function renderVolumeChart(volume) {
  var ctx = document.getElementById('volume-chart');
  if (!ctx) return;
  if (state.volumeChart) { state.volumeChart.destroy(); state.volumeChart = null; }
  if (!volume || !volume.length) {
    document.getElementById('chart-container').innerHTML = '<div class="flex items-center justify-center h-full text-gray-400 dark:text-slate-600 text-sm">No channel volume data yet</div>';
    return;
  }
  if (!document.getElementById('volume-chart')) {
    document.getElementById('chart-container').innerHTML = '<canvas id="volume-chart"></canvas>';
    ctx = document.getElementById('volume-chart');
  }
  var isDark = document.documentElement.classList.contains('dark');
  var gridColor = isDark ? 'rgba(51,65,85,0.3)' : 'rgba(226,232,240,0.7)';
  var tickColor = isDark ? '#64748b' : '#94a3b8';
  var legendColor = isDark ? '#94a3b8' : '#6b7280';
  var labels = volume.map(function(v) { return v.channel; });
  var received = volume.map(function(v) { return v.received || 0; });
  var processed = volume.map(function(v) { return v.processed || 0; });
  var errored = volume.map(function(v) { return v.errored || 0; });
  state.volumeChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        { label: 'Received', data: received, backgroundColor: 'rgba(56, 189, 248, 0.6)', borderRadius: 4 },
        { label: 'Processed', data: processed, backgroundColor: 'rgba(52, 211, 153, 0.6)', borderRadius: 4 },
        { label: 'Errored', data: errored, backgroundColor: 'rgba(248, 113, 113, 0.6)', borderRadius: 4 }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'top', labels: { color: legendColor, usePointStyle: true, pointStyle: 'circle', padding: 20, font: { size: 12 } } } },
      scales: {
        x: { grid: { display: false }, ticks: { color: tickColor, font: { size: 11 } } },
        y: { grid: { color: gridColor }, ticks: { color: tickColor, font: { size: 11 } }, beginAtZero: true }
      }
    }
  });
}

// --- Channels ---
function fetchChannels() {
  fetch('/api/channels').then(function(r) { return r.json(); }).then(function(data) {
    state.channels = data || [];
    populateFilterDropdowns();
    filterChannels();
  }).catch(function(e) { console.error('Channels fetch failed', e); });
}

function populateFilterDropdowns() {
  var sources = {}, dests = {};
  (state.channels || []).forEach(function(c) {
    if (c.listener_type) sources[c.listener_type] = true;
    (c.destination_types || []).forEach(function(dt) { dests[dt] = true; });
  });
  var srcSel = document.getElementById('ch-source-filter');
  var curSrc = srcSel.value;
  srcSel.innerHTML = '<option value="">All Sources</option>';
  Object.keys(sources).sort().forEach(function(s) {
    srcSel.innerHTML += '<option value="' + s + '">' + s.toUpperCase() + '</option>';
  });
  srcSel.value = curSrc;
  var dstSel = document.getElementById('ch-dest-filter');
  var curDst = dstSel.value;
  dstSel.innerHTML = '<option value="">All Destinations</option>';
  Object.keys(dests).sort().forEach(function(d) {
    dstSel.innerHTML += '<option value="' + d + '">' + d.toUpperCase() + '</option>';
  });
  dstSel.value = curDst;
}

function filterChannels() {
  var search = (document.getElementById('ch-search').value || '').toLowerCase();
  var srcFilter = document.getElementById('ch-source-filter').value;
  var dstFilter = document.getElementById('ch-dest-filter').value;
  var statusFilter = document.getElementById('ch-status-filter').value;
  var filtered = (state.channels || []).filter(function(c) {
    if (search && (c.id || '').toLowerCase().indexOf(search) === -1) return false;
    if (srcFilter && c.listener_type !== srcFilter) return false;
    if (dstFilter && (!c.destination_types || c.destination_types.indexOf(dstFilter) === -1)) return false;
    if (statusFilter === 'enabled' && !c.enabled) return false;
    if (statusFilter === 'disabled' && c.enabled) return false;
    return true;
  });
  renderChannelList(filtered);
}

function renderChannelList(channels) {
  var el = document.getElementById('channel-list');
  if (!channels || !channels.length) {
    el.innerHTML = '<div class="col-span-full flex flex-col items-center justify-center py-16 text-gray-400 dark:text-slate-600"><svg class="w-12 h-12 mb-3 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20 13V6a2 2 0 00-2-2H6a2 2 0 00-2 2v7m16 0v5a2 2 0 01-2 2H6a2 2 0 01-2-2v-5m16 0h-2.586a1 1 0 00-.707.293l-2.414 2.414a1 1 0 01-.707.293h-3.172a1 1 0 01-.707-.293l-2.414-2.414A1 1 0 006.586 13H4"/></svg><p class="text-sm">No channels found</p></div>';
    return;
  }
  el.innerHTML = channels.map(function(c, i) {
    var badge = c.enabled
      ? '<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold uppercase tracking-wider bg-emerald-50 dark:bg-emerald-400/10 text-emerald-600 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-400/20"><span class="w-1.5 h-1.5 rounded-full bg-emerald-500 dark:bg-emerald-400"></span>Enabled</span>'
      : '<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[10px] font-semibold uppercase tracking-wider bg-gray-100 dark:bg-slate-500/10 text-gray-500 dark:text-slate-500 border border-gray-200 dark:border-slate-500/20"><span class="w-1.5 h-1.5 rounded-full bg-gray-400 dark:bg-slate-500"></span>Disabled</span>';
    var listenerBadge = c.listener_type
      ? '<span class="px-2 py-0.5 rounded-md text-[10px] font-medium uppercase bg-sky-50 dark:bg-sky-400/10 text-sky-600 dark:text-sky-400 border border-sky-200 dark:border-sky-400/20">' + esc(c.listener_type) + '</span>'
      : '';
    var dests = (c.destinations || []).join(', ') || 'none';
    var tags = (c.tags || []).map(function(t) {
      return '<span class="px-1.5 py-0.5 rounded text-[10px] bg-gray-100 dark:bg-slate-700/50 text-gray-500 dark:text-slate-400">' + esc(t) + '</span>';
    }).join(' ');
    return '<div class="bg-white dark:bg-slate-900/50 border border-gray-200 dark:border-slate-800/60 rounded-xl p-5 hover:border-sky-300 dark:hover:border-sky-400/30 transition-all duration-200 cursor-pointer group fade-up" style="animation-delay:' + (i * 30) + 'ms" onclick="openChannelDetail(\'' + esc(c.id) + '\')">' +
      '<div class="flex items-start justify-between mb-3">' +
        '<div class="flex items-center gap-2">' +
          '<h3 class="text-sm font-semibold text-gray-900 dark:text-white group-hover:text-sky-600 dark:group-hover:text-sky-400 transition-colors">' + esc(c.id) + '</h3>' +
          listenerBadge +
        '</div>' +
        badge +
      '</div>' +
      '<div class="text-xs text-gray-500 dark:text-slate-500 mb-1"><span class="text-gray-600 dark:text-slate-400">Destinations:</span> ' + esc(dests) + '</div>' +
      (c.group ? '<div class="text-xs text-gray-500 dark:text-slate-500"><span class="text-gray-600 dark:text-slate-400">Group:</span> ' + esc(c.group) + '</div>' : '') +
      (tags ? '<div class="flex flex-wrap gap-1 mt-2">' + tags + '</div>' : '') +
      '<div class="flex gap-2 mt-3 opacity-0 group-hover:opacity-100 transition-opacity" onclick="event.stopPropagation()">' +
        (c.enabled
          ? '<button class="px-2.5 py-1 rounded-lg text-[11px] font-medium bg-red-50 dark:bg-red-400/10 text-red-600 dark:text-red-400 border border-red-200 dark:border-red-400/20 hover:bg-red-100 dark:hover:bg-red-400/20 transition-all" onclick="channelAction(\'' + esc(c.id) + '\',\'undeploy\')">Undeploy</button>' +
            '<button class="px-2.5 py-1 rounded-lg text-[11px] font-medium bg-sky-50 dark:bg-sky-400/10 text-sky-600 dark:text-sky-400 border border-sky-200 dark:border-sky-400/20 hover:bg-sky-100 dark:hover:bg-sky-400/20 transition-all" onclick="channelAction(\'' + esc(c.id) + '\',\'restart\')">Restart</button>'
          : '<button class="px-2.5 py-1 rounded-lg text-[11px] font-medium bg-emerald-50 dark:bg-emerald-400/10 text-emerald-600 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-400/20 hover:bg-emerald-100 dark:hover:bg-emerald-400/20 transition-all" onclick="channelAction(\'' + esc(c.id) + '\',\'deploy\')">Deploy</button>') +
      '</div>' +
    '</div>';
  }).join('');
}

function openChannelDetail(id) {
  var panel = document.getElementById('slideover-content');
  panel.innerHTML = '<div class="space-y-4"><div class="skeleton h-8 w-48"></div><div class="skeleton h-4 w-32 mt-2"></div><div class="skeleton h-32 mt-6"></div><div class="skeleton h-32 mt-4"></div></div>';
  showSlideOver();
  fetch('/api/channels/' + encodeURIComponent(id)).then(function(r) { return r.json(); }).then(function(data) {
    renderChannelDetail(data);
  }).catch(function(e) { panel.innerHTML = '<p class="text-red-500 dark:text-red-400">Failed to load channel details.</p>'; });
}

function renderChannelDetail(d) {
  var panel = document.getElementById('slideover-content');
  var statusBadge = d.enabled
    ? '<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-emerald-50 dark:bg-emerald-400/10 text-emerald-600 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-400/20"><span class="w-2 h-2 rounded-full bg-emerald-500 dark:bg-emerald-400"></span>Enabled</span>'
    : '<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-gray-100 dark:bg-slate-500/10 text-gray-500 dark:text-slate-500 border border-gray-200 dark:border-slate-500/20"><span class="w-2 h-2 rounded-full bg-gray-400 dark:bg-slate-500"></span>Disabled</span>';

  var html = '<div class="flex items-center justify-between mb-6">' +
    '<div><h2 class="text-xl font-bold text-gray-900 dark:text-white">' + esc(d.id) + '</h2>' +
    '<div class="flex items-center gap-2 mt-2">' + statusBadge + '</div></div>' +
    '<button onclick="closeSlideOver()" class="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-slate-800 text-gray-400 dark:text-slate-400 hover:text-gray-600 dark:hover:text-white transition-all"><svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg></button>' +
  '</div>';

  var sectionCls = 'bg-gray-50 dark:bg-slate-800/50 border border-gray-200 dark:border-slate-700/50 rounded-xl p-4 mb-4 transition-colors duration-200';
  var labelCls = 'text-xs font-semibold text-gray-400 dark:text-slate-400 uppercase tracking-wider mb-3';
  var kvLabelCls = 'text-gray-400 dark:text-slate-500';
  var kvValueCls = 'text-gray-800 dark:text-slate-200 font-medium mt-0.5';

  if (d.listener) {
    html += '<div class="' + sectionCls + '">' +
      '<h3 class="' + labelCls + '">Listener</h3>' +
      '<div class="flex items-center gap-2 mb-3"><span class="px-2 py-0.5 rounded-md text-xs font-medium bg-sky-50 dark:bg-sky-400/10 text-sky-600 dark:text-sky-400 border border-sky-200 dark:border-sky-400/20">' + esc(d.listener.type || 'unknown') + '</span></div>';
    if (d.listener.config) {
      html += '<div class="grid grid-cols-2 gap-2">';
      Object.keys(d.listener.config).forEach(function(k) {
        var v = d.listener.config[k];
        if (Array.isArray(v)) v = v.join(', ');
        html += '<div class="text-xs"><span class="' + kvLabelCls + '">' + esc(k.replace(/_/g, ' ')) + '</span><div class="' + kvValueCls + '">' + esc(String(v)) + '</div></div>';
      });
      html += '</div>';
    }
    html += '</div>';
  }

  if (d.destinations && d.destinations.length) {
    html += '<div class="' + sectionCls + '">' +
      '<h3 class="' + labelCls + '">Destinations</h3>';
    d.destinations.forEach(function(dest) {
      html += '<div class="mb-3 last:mb-0 p-3 bg-white dark:bg-slate-900/50 rounded-lg border border-gray-100 dark:border-slate-700/30 transition-colors duration-200">' +
        '<div class="flex items-center gap-2 mb-2">' +
          '<span class="text-sm font-medium text-gray-900 dark:text-white">' + esc(dest.name || 'Unnamed') + '</span>' +
          (dest.type ? '<span class="px-1.5 py-0.5 rounded text-[10px] font-medium bg-violet-50 dark:bg-violet-400/10 text-violet-600 dark:text-violet-400">' + esc(dest.type) + '</span>' : '') +
        '</div>';
      if (dest.config && Object.keys(dest.config).length) {
        html += '<div class="grid grid-cols-2 gap-1.5">';
        Object.keys(dest.config).forEach(function(k) {
          var v = dest.config[k];
          if (Array.isArray(v)) v = v.join(', ');
          html += '<div class="text-xs"><span class="' + kvLabelCls + '">' + esc(k.replace(/_/g, ' ')) + '</span><div class="text-gray-700 dark:text-slate-300 mt-0.5">' + esc(String(v)) + '</div></div>';
        });
        html += '</div>';
      }
      html += '</div>';
    });
    html += '</div>';
  }

  if (d.pipeline) {
    html += '<div class="' + sectionCls + '">' +
      '<h3 class="' + labelCls + '">Pipeline</h3>' +
      '<div class="space-y-1.5">';
    Object.keys(d.pipeline).forEach(function(k) {
      html += '<div class="flex items-center justify-between text-xs p-2 rounded-lg bg-white dark:bg-slate-900/30 border border-gray-100 dark:border-transparent transition-colors duration-200">' +
        '<span class="text-gray-500 dark:text-slate-400">' + esc(k) + '</span>' +
        '<span class="text-gray-800 dark:text-slate-200 font-mono text-[11px]">' + esc(d.pipeline[k]) + '</span></div>';
    });
    html += '</div></div>';
  }

  if (d.tags || d.group || d.priority || d.data_types) {
    html += '<div class="' + sectionCls + '">' +
      '<h3 class="' + labelCls + '">Metadata</h3>' +
      '<div class="space-y-2">';
    if (d.tags && d.tags.length) {
      html += '<div class="flex items-center gap-2"><span class="text-xs ' + kvLabelCls + ' w-16">Tags</span><div class="flex flex-wrap gap-1">';
      d.tags.forEach(function(t) { html += '<span class="px-2 py-0.5 rounded-full text-[10px] bg-sky-50 dark:bg-sky-400/10 text-sky-600 dark:text-sky-400">' + esc(t) + '</span>'; });
      html += '</div></div>';
    }
    if (d.group) html += '<div class="flex items-center gap-2 text-xs"><span class="' + kvLabelCls + ' w-16">Group</span><span class="text-gray-800 dark:text-slate-200">' + esc(d.group) + '</span></div>';
    if (d.priority) html += '<div class="flex items-center gap-2 text-xs"><span class="' + kvLabelCls + ' w-16">Priority</span><span class="text-gray-800 dark:text-slate-200">' + esc(d.priority) + '</span></div>';
    if (d.data_types) {
      if (d.data_types.inbound) html += '<div class="flex items-center gap-2 text-xs"><span class="' + kvLabelCls + ' w-16">Inbound</span><span class="text-gray-800 dark:text-slate-200">' + esc(d.data_types.inbound) + '</span></div>';
      if (d.data_types.outbound) html += '<div class="flex items-center gap-2 text-xs"><span class="' + kvLabelCls + ' w-16">Outbound</span><span class="text-gray-800 dark:text-slate-200">' + esc(d.data_types.outbound) + '</span></div>';
    }
    html += '</div></div>';
  }

  if (d.metrics && Object.keys(d.metrics).length) {
    html += '<div class="' + sectionCls + '">' +
      '<h3 class="' + labelCls + '">Metrics</h3>' +
      '<div class="grid grid-cols-2 gap-2">';
    Object.keys(d.metrics).forEach(function(k) {
      var v = d.metrics[k];
      var label = k.replace(d.id, '').replace(/^\./, '').replace(/_/g, ' ');
      html += '<div class="p-2 rounded-lg bg-white dark:bg-slate-900/30 border border-gray-100 dark:border-transparent text-center transition-colors duration-200"><div class="text-lg font-bold text-sky-600 dark:text-sky-400">' + esc(formatNum(typeof v === 'number' ? v : 0)) + '</div><div class="text-[10px] text-gray-400 dark:text-slate-500 mt-0.5">' + esc(label) + '</div></div>';
    });
    html += '</div></div>';
  }

  html += '<div class="flex gap-2 mt-6">';
  if (d.enabled) {
    html += '<button class="px-4 py-2 rounded-xl text-xs font-medium bg-red-50 dark:bg-red-400/10 text-red-600 dark:text-red-400 border border-red-200 dark:border-red-400/20 hover:bg-red-100 dark:hover:bg-red-400/20 transition-all" onclick="channelAction(\'' + esc(d.id) + '\',\'undeploy\');closeSlideOver()">Undeploy</button>';
    html += '<button class="px-4 py-2 rounded-xl text-xs font-medium bg-sky-50 dark:bg-sky-400/10 text-sky-600 dark:text-sky-400 border border-sky-200 dark:border-sky-400/20 hover:bg-sky-100 dark:hover:bg-sky-400/20 transition-all" onclick="channelAction(\'' + esc(d.id) + '\',\'restart\');closeSlideOver()">Restart</button>';
  } else {
    html += '<button class="px-4 py-2 rounded-xl text-xs font-medium bg-emerald-50 dark:bg-emerald-400/10 text-emerald-600 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-400/20 hover:bg-emerald-100 dark:hover:bg-emerald-400/20 transition-all" onclick="channelAction(\'' + esc(d.id) + '\',\'deploy\');closeSlideOver()">Deploy</button>';
  }
  html += '</div>';
  panel.innerHTML = html;
}

function showSlideOver() {
  var backdrop = document.getElementById('slideover-backdrop');
  var panel = document.getElementById('slideover-panel');
  backdrop.classList.remove('hidden');
  setTimeout(function() {
    backdrop.classList.remove('opacity-0');
    panel.classList.remove('translate-x-full');
  }, 10);
}

function closeSlideOver() {
  var backdrop = document.getElementById('slideover-backdrop');
  var panel = document.getElementById('slideover-panel');
  backdrop.classList.add('opacity-0');
  panel.classList.add('translate-x-full');
  setTimeout(function() { backdrop.classList.add('hidden'); }, 300);
}

function channelAction(id, action) {
  fetch('/api/channels/' + encodeURIComponent(id) + '/' + action, { method: 'POST' })
    .then(function() { showToast(action.charAt(0).toUpperCase() + action.slice(1) + ': ' + id, 'success'); fetchChannels(); })
    .catch(function(e) { showToast('Action failed: ' + e.message, 'error'); });
}

// --- Messages ---
function populateChannelFilter() {
  var sel = document.getElementById('msg-channel');
  var cur = sel.value;
  sel.innerHTML = '<option value="">All Channels</option>';
  (state.channels || []).forEach(function(c) {
    sel.innerHTML += '<option value="' + esc(c.id) + '">' + esc(c.id) + '</option>';
  });
  sel.value = cur;
}

function loadMessages() {
  var ch = document.getElementById('msg-channel').value;
  var limit = document.getElementById('msg-limit').value || '50';
  var url = '/api/messages?limit=' + limit;
  if (ch) url += '&channel=' + encodeURIComponent(ch);

  var el = document.getElementById('message-list');
  el.innerHTML = '<div class="space-y-3"><div class="skeleton h-20 rounded-xl"></div><div class="skeleton h-20 rounded-xl"></div><div class="skeleton h-20 rounded-xl"></div></div>';

  fetch(url).then(function(r) { return r.json(); }).then(function(msgs) {
    if (!msgs || !msgs.length) {
      el.innerHTML = '<div class="flex flex-col items-center justify-center py-16 text-gray-400 dark:text-slate-600"><svg class="w-12 h-12 mb-3 opacity-30" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg><p class="text-sm">No messages found</p></div>';
      return;
    }
    renderGroupedMessages(msgs);
  }).catch(function(e) {
    el.innerHTML = '<p class="text-red-500 dark:text-red-400 text-sm">Failed to load messages.</p>';
  });
}

function renderGroupedMessages(msgs) {
  var groups = {};
  var groupOrder = [];
  msgs.forEach(function(m) {
    var key = m.CorrelationID || m.ID;
    if (!groups[key]) { groups[key] = []; groupOrder.push(key); }
    groups[key].push(m);
  });

  var html = '';
  var bandColors = [
    'bg-white dark:bg-slate-900/40 border-gray-200 dark:border-slate-800/60',
    'bg-gray-50 dark:bg-slate-800/20 border-gray-100 dark:border-slate-700/40'
  ];
  groupOrder.forEach(function(key, gi) {
    var items = groups[key];
    items.sort(function(a, b) {
      var order = { RECEIVED: 0, TRANSFORMED: 1, FILTERED: 2, SENT: 3, ERROR: 4, REPROCESSED: 5 };
      return (order[a.Status] || 99) - (order[b.Status] || 99);
    });
    var bandClass = bandColors[gi % 2];
    html += '<div class="' + bandClass + ' border rounded-xl mb-3 overflow-hidden fade-up transition-colors duration-200" style="animation-delay:' + (gi * 40) + 'ms">';
    html += '<div class="px-4 py-2 border-b border-gray-100 dark:border-slate-800/40 flex items-center justify-between">' +
      '<div class="flex items-center gap-2">' +
        '<span class="text-[10px] text-gray-400 dark:text-slate-500 font-mono">' + esc(key.substring(0, 16)) + '...</span>' +
        '<span class="text-[10px] text-gray-400 dark:text-slate-600">' + esc(items[0].ChannelID || '') + '</span>' +
      '</div>' +
      '<span class="text-[10px] text-gray-400 dark:text-slate-600">' + items.length + ' stage' + (items.length > 1 ? 's' : '') + '</span>' +
    '</div>';

    items.forEach(function(m) {
      var statusCls = {
        'RECEIVED':    'bg-sky-50 dark:bg-sky-400/10 text-sky-600 dark:text-sky-400 border-sky-200 dark:border-sky-400/20',
        'TRANSFORMED': 'bg-violet-50 dark:bg-violet-400/10 text-violet-600 dark:text-violet-400 border-violet-200 dark:border-violet-400/20',
        'SENT':        'bg-emerald-50 dark:bg-emerald-400/10 text-emerald-600 dark:text-emerald-400 border-emerald-200 dark:border-emerald-400/20',
        'ERROR':       'bg-red-50 dark:bg-red-400/10 text-red-600 dark:text-red-400 border-red-200 dark:border-red-400/20',
        'FILTERED':    'bg-amber-50 dark:bg-amber-400/10 text-amber-600 dark:text-amber-400 border-amber-200 dark:border-amber-400/20',
        'REPROCESSED': 'bg-blue-50 dark:bg-blue-400/10 text-blue-600 dark:text-blue-400 border-blue-200 dark:border-blue-400/20'
      };
      var cls = statusCls[m.Status] || 'bg-gray-100 dark:bg-slate-700/10 text-gray-500 dark:text-slate-400 border-gray-200 dark:border-slate-600/20';
      var content = '';
      if (m.Content) { try { content = atob(m.Content); } catch(e) { content = m.Content; } }
      var contentPreview = content.length > 120 ? content.substring(0, 120) + '...' : content;
      var ts = m.Timestamp ? new Date(m.Timestamp).toLocaleString() : '';

      html += '<div class="px-4 py-3 border-b border-gray-100/50 dark:border-slate-800/20 last:border-b-0 hover:bg-gray-50/50 dark:hover:bg-white/[0.02] transition-colors">' +
        '<div class="flex items-center justify-between mb-2">' +
          '<div class="flex items-center gap-2">' +
            '<span class="inline-flex items-center px-2 py-0.5 rounded-md text-[10px] font-semibold uppercase border ' + cls + '">' + esc(m.Status || '') + '</span>' +
            '<span class="text-[10px] text-gray-400 dark:text-slate-500">' + esc(m.Stage || '') + '</span>' +
            '<span class="text-[10px] text-gray-300 dark:text-slate-600">' + esc(ts) + '</span>' +
          '</div>' +
          '<div class="flex items-center gap-1.5">' +
            '<button class="px-2 py-1 rounded-md text-[10px] font-medium bg-sky-50 dark:bg-sky-400/10 text-sky-600 dark:text-sky-400 border border-sky-200 dark:border-sky-400/20 hover:bg-sky-100 dark:hover:bg-sky-400/20 transition-all" onclick="reprocessMessage(\'' + esc(m.ID) + '\')">Reprocess</button>' +
            (content ? '<button class="px-2 py-1 rounded-md text-[10px] font-medium bg-gray-100 dark:bg-slate-700/50 text-gray-500 dark:text-slate-400 border border-gray-200 dark:border-slate-600/30 hover:bg-gray-200 dark:hover:bg-slate-700 hover:text-gray-700 dark:hover:text-white transition-all" onclick="copyPayload(\'' + esc(m.ID) + '\')">Copy</button>' : '') +
          '</div>' +
        '</div>';
      if (content) {
        html += '<div class="relative"><pre id="payload-' + esc(m.ID) + '" class="text-[11px] text-gray-500 dark:text-slate-400 font-mono bg-gray-100 dark:bg-slate-950/50 rounded-lg p-3 overflow-x-auto max-h-28 border border-gray-200 dark:border-slate-800/30 transition-colors duration-200">' + esc(contentPreview) + '</pre>';
        if (content.length > 120) {
          html += '<button class="text-[10px] text-sky-600 dark:text-sky-400 hover:text-sky-500 dark:hover:text-sky-300 mt-1" onclick="togglePayload(this, \'' + esc(m.ID) + '\')">Show more</button>';
        }
        html += '<textarea id="full-' + esc(m.ID) + '" class="hidden">' + esc(content) + '</textarea></div>';
      }
      html += '</div>';
    });
    html += '</div>';
  });
  document.getElementById('message-list').innerHTML = html;
}

function togglePayload(btn, id) {
  var pre = document.getElementById('payload-' + id);
  var full = document.getElementById('full-' + id);
  if (!pre || !full) return;
  if (btn.textContent === 'Show more') {
    pre.textContent = full.value;
    pre.classList.remove('max-h-28');
    btn.textContent = 'Show less';
  } else {
    pre.textContent = full.value.substring(0, 120) + '...';
    pre.classList.add('max-h-28');
    btn.textContent = 'Show more';
  }
}

function copyPayload(id) {
  var full = document.getElementById('full-' + id);
  if (!full) return;
  navigator.clipboard.writeText(full.value).then(function() {
    showToast('Copied to clipboard', 'success');
  }).catch(function() {
    var ta = document.createElement('textarea');
    ta.value = full.value;
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    document.body.removeChild(ta);
    showToast('Copied to clipboard', 'success');
  });
}

function reprocessMessage(id) {
  fetch('/api/messages/' + encodeURIComponent(id) + '/reprocess', { method: 'POST' })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.reprocessed) {
        showToast('Message reprocessed successfully', 'success');
        loadMessages();
      } else if (data.error) {
        showToast('Reprocess failed: ' + data.error, 'error');
      }
    })
    .catch(function(e) { showToast('Reprocess failed: ' + e.message, 'error'); });
}

// --- Utilities ---
function formatNum(n) {
  if (n === undefined || n === null) return '0';
  if (typeof n !== 'number') n = parseInt(n, 10) || 0;
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
  if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
  return n.toString();
}

function esc(str) {
  if (!str) return '';
  var d = document.createElement('div');
  d.appendChild(document.createTextNode(String(str)));
  return d.innerHTML;
}

function showToast(msg, type) {
  var container = document.getElementById('toast');
  var colorCls = type === 'error'
    ? 'bg-red-600 dark:bg-red-500/90 border-red-500 dark:border-red-400/30'
    : 'bg-emerald-600 dark:bg-emerald-500/90 border-emerald-500 dark:border-emerald-400/30';
  var iconSvg = type === 'error'
    ? '<svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>'
    : '<svg class="w-4 h-4 shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>';
  var toast = document.createElement('div');
  toast.className = 'pointer-events-auto flex items-center gap-2 px-4 py-2.5 rounded-xl text-white text-sm font-medium border shadow-xl ' + colorCls + ' transform translate-y-4 opacity-0 transition-all duration-300';
  toast.innerHTML = iconSvg + '<span>' + esc(msg) + '</span>';
  container.appendChild(toast);
  setTimeout(function() { toast.classList.remove('translate-y-4', 'opacity-0'); }, 20);
  setTimeout(function() {
    toast.classList.add('translate-y-4', 'opacity-0');
    setTimeout(function() { toast.remove(); }, 300);
  }, 3000);
}

// --- Init ---
navigate('home');
fetchChannels();
setInterval(function() { if (state.page === 'home') fetchStats(); }, 10000);
setInterval(function() { if (state.page === 'channels') fetchChannels(); }, 10000);
</script>
</body>
</html>`
