(() => {
  const panel = document.getElementById('model-tests');
  if (!panel) return;
  const start = document.getElementById('probe-start');
  const stop = document.getElementById('probe-stop');
  const summary = document.getElementById('probe-summary');
  const rows = document.getElementById('probe-rows');
  const table = document.getElementById('probe-table');
  let running = false;
  let stopping = false;
  const labels = { pending: '等待测试', running: '测试中', available: '可用', unavailable: '不可用', client_rejected: '客户端受限', empty: '空响应', cancelled: '结果未知', skipped: '未测试' };

  async function request(action, ticket) {
    const url = new URL(window.location.href);
    url.search = '';
    url.hash = '';
    url.searchParams.set('action', action);
    if (ticket) url.searchParams.set('ticket', ticket);
    const response = await fetch(url, {
      method: 'GET', cache: 'no-store', credentials: 'same-origin',
      headers: { 'X-Mirasim-Probe': panel.dataset.token, Accept: 'application/json' }
    });
    if (!response.headers.get('content-type')?.includes('application/json')) {
      throw new Error('页面会话已失效，请刷新页面后重试。');
    }
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || ('HTTP ' + response.status));
    return result;
  }

  function cell(row, className) {
    const td = document.createElement('td');
    if (className) td.className = className;
    row.append(td);
    return td;
  }

  function createRow(item) {
    const tr = document.createElement('tr');
    cell(tr).textContent = '账户 ' + item.account;
    const name = cell(tr, 'model-name');
    name.textContent = item.model || '—';
    if (item.kind === 'image' || item.aliases?.length) {
      const hint = document.createElement('div');
      hint.className = 'aliases';
      hint.textContent = item.kind === 'image' ? '图像生成 · 1 张' : ('含别名：' + item.aliases.join('、'));
      name.append(hint);
    }
    const badge = document.createElement('span');
    badge.className = 'test-badge';
    cell(tr).append(badge);
    const duration = cell(tr, 'probe-time');
    duration.textContent = '—';
    const response = cell(tr, 'response-preview');
    rows.append(tr);
    const row = { item, tr, badge, duration, response };
    setState(row, item.problem ? 'skipped' : 'pending');
    response.textContent = item.problem || '—';
    return row;
  }

  function setState(row, state) {
    row.badge.dataset.state = state;
    row.badge.textContent = labels[state] || state;
  }

  function showResult(row, result) {
    setState(row, result.status);
    if (result.http_status) row.badge.textContent += ' · ' + result.http_status;
    row.duration.textContent = (Number(result.duration_ms || 0) / 1000).toFixed(2) + ' s';
    const text = result.response || '—';
    row.response.replaceChildren();
    if (text.length > 120) {
      const details = document.createElement('details');
      const title = document.createElement('summary');
      title.textContent = text.slice(0, 90) + '… 查看响应';
      const full = document.createElement('pre');
      full.textContent = text;
      details.append(title, full);
      row.response.append(details);
    } else {
      row.response.textContent = text;
    }
    const checked = new Date(result.checked_at);
    if (Number.isFinite(checked.getTime())) row.tr.title = '测试时间：' + checked.toLocaleString();
  }

  stop.addEventListener('click', () => {
    stopping = true;
    stop.disabled = true;
    summary.textContent = '正在完成当前请求，之后停止。';
  });

  start.addEventListener('click', async () => {
    if (running) return;
    running = true;
    stopping = false;
    start.disabled = true;
    stop.disabled = false;
    rows.replaceChildren();
    table.hidden = true;
    summary.textContent = '正在读取各账户的模型目录…';
    let rendered = [];
    let completed = 0;
    try {
      const plan = await request('models');
      rendered = plan.rows.map(createRow);
      table.hidden = rendered.length === 0;
      const tasks = rendered.filter(row => row.item.ticket);
      for (const row of tasks) {
        if (stopping) break;
        setState(row, 'running');
        row.response.textContent = '等待模型响应…';
        summary.textContent = '测试中 ' + completed + ' / ' + tasks.length + ' · ' + row.item.model;
        try {
          showResult(row, await request('probe', row.item.ticket));
        } catch (error) {
          setState(row, 'cancelled');
          row.response.textContent = error.message || '请求失败，结果未知。';
        }
        completed++;
      }
      for (const row of tasks) {
        if (row.badge.dataset.state === 'pending') {
          setState(row, 'skipped');
          row.response.textContent = '已停止，未发送请求。';
        }
      }
      const count = state => tasks.filter(row => row.badge.dataset.state === state).length;
      summary.textContent = tasks.length === 0 ? '没有可测试的模型。' :
        (stopping ? '已停止' : '测试完成') + ' · ' + completed + '/' + tasks.length +
        ' · 可用 ' + count('available') + ' · 不可用 ' + count('unavailable') +
        ' · 客户端受限 ' + count('client_rejected') +
        ' · 空响应 ' + count('empty') + ' · 结果未知 ' + count('cancelled');
    } catch (error) {
      summary.textContent = error.message || '读取模型列表失败。';
    } finally {
      running = false;
      start.disabled = false;
      stop.disabled = true;
      start.textContent = '重新测试所有模型';
    }
  });
})();
