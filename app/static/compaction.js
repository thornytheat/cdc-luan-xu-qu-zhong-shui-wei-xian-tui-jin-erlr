// 模块 7 墓碑压缩页：来源登记、发起压缩、中断续跑（原生 JS，无框架）。
(function () {
  function qs(sel, root) { return (root || document).querySelector(sel); }
  function qsa(sel, root) { return Array.prototype.slice.call((root || document).querySelectorAll(sel)); }

  function showError(host, err) {
    if (!host) return;
    host.hidden = false;
    host.textContent = '执行失败：' + (err && err.message ? err.message : String(err));
  }

  async function postJSON(url, payload) {
    var res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {})
    });
    var data = null;
    try { data = await res.json(); } catch (e) { data = { detail: '响应不是 JSON' }; }
    if (!res.ok) {
      var msg = (data && (data.detail || data.message)) || ('请求失败：' + res.status);
      if (typeof msg !== 'string') msg = JSON.stringify(msg);
      throw new Error(msg);
    }
    return data;
  }

  // 来源水位线登记
  var sourceForm = qs('#source-form');
  if (sourceForm) {
    sourceForm.addEventListener('submit', async function (event) {
      event.preventDefault();
      var errBox = qs('#source-error');
      errBox.hidden = true;
      try {
        await postJSON('/api/retention/sources', {
          source: qs('input[name="source"]', sourceForm).value.trim(),
          watermark: qs('input[name="watermark"]', sourceForm).value.trim(),
          note: qs('input[name="note"]', sourceForm).value.trim()
        });
        window.location.reload();
      } catch (err) {
        showError(errBox, err);
      }
    });
  }

  // 发起压缩作业
  var compactionForm = qs('#compaction-form');
  if (compactionForm) {
    compactionForm.addEventListener('submit', async function (event) {
      event.preventDefault();
      var errBox = qs('#compaction-error');
      errBox.hidden = true;
      var tables = qsa('input[name="tables"]:checked', compactionForm).map(function (i) { return i.value; });
      var btn = qs('button[type="submit"]', compactionForm);
      btn.disabled = true;
      var label = btn.textContent;
      btn.textContent = '校验与压缩中…';
      try {
        var data = await postJSON('/api/compaction/jobs', { tables: tables });
        window.location.href = data.result_url;
      } catch (err) {
        showError(errBox, err);
        btn.disabled = false;
        btn.textContent = label;
      }
    });
  }

  // 中断续跑
  qsa('.resume-link, #resume-btn').forEach(function (link) {
    link.addEventListener('click', async function (event) {
      event.preventDefault();
      var jobId = link.getAttribute('data-job');
      try {
        var data = await postJSON('/api/compaction/jobs/' + encodeURIComponent(jobId) + '/resume', {});
        window.location.href = data.result_url;
      } catch (err) {
        window.alert('续跑失败：' + (err.message || err));
      }
    });
  });

  // 现网口径：分表归档/拦截计数（页面加载时拉取，避免模板二次查库）
  async function fillCounts() {
    try {
      var [archivedResp, blockedResp] = await Promise.all([
        fetch('/api/compaction/archived'),
        fetch('/api/compaction/blocked-replays')
      ]);
      var archived = (await archivedResp.json()).archived || [];
      var blocked = (await blockedResp.json()).blocked_replays || [];
      var archivedByTable = {};
      var blockedByTable = {};
      archived.forEach(function (row) {
        archivedByTable[row.table] = (archivedByTable[row.table] || 0) + 1;
      });
      blocked.forEach(function (row) {
        blockedByTable[row.table] = (blockedByTable[row.table] || 0) + 1;
      });
      Object.keys(archivedByTable).forEach(function (table) {
        var cell = qs('#archived-' + table);
        if (cell) cell.textContent = archivedByTable[table];
      });
      Object.keys(blockedByTable).forEach(function (table) {
        var cell = qs('#blocked-' + table);
        if (cell) cell.textContent = blockedByTable[table];
      });
      qsa('[id^="archived-"]').forEach(function (cell) { if (cell.textContent === '—') cell.textContent = 0; });
      qsa('[id^="blocked-"]').forEach(function (cell) { if (cell.textContent === '—') cell.textContent = 0; });
    } catch (e) {
      // 计数拉取失败不阻塞页面
    }
  }
  fillCounts();
})();
