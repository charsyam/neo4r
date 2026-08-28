pub(crate) const WEB_INDEX_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>neo4r graph console</title>
  <style>
    html, body { margin: 0; width: 100%; height: 100%; overflow: hidden; font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #151515; color: #f4f7f5; }
    #scene { position: fixed; inset: 0; z-index: 0; }
    #labels { position: fixed; inset: 0; z-index: 1; overflow: hidden; pointer-events: none; }
    .graph-label { position: absolute; max-width: 190px; padding: 4px 8px; border: 1px solid rgba(255,255,255,0.28); border-radius: 6px; background: rgba(8,10,10,0.88); color: #ffffff; font-size: 12px; line-height: 1.2; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; text-shadow: 0 1px 3px rgba(0,0,0,0.95); box-shadow: 0 5px 18px rgba(0,0,0,0.34); transform: translate(-50%, -50%); will-change: transform, opacity; }
    .graph-label.node { font-weight: 700; }
    .graph-label.edge { color: #ffffff; background: rgba(32,115,101,0.92); font-size: 11px; }
    #panel { position: fixed; z-index: 2; left: 16px; top: 16px; bottom: 16px; width: min(380px, calc(100vw - 32px)); display: flex; flex-direction: column; gap: 10px; pointer-events: none; }
    .bar, .detail { border: 1px solid rgba(255,255,255,0.16); background: rgba(29,34,32,0.84); backdrop-filter: blur(12px); border-radius: 8px; box-shadow: 0 16px 50px rgba(0,0,0,0.28); pointer-events: auto; }
    .bar { padding: 12px; display: grid; gap: 8px; }
    h1 { font-size: 17px; line-height: 1.2; margin: 0; font-weight: 700; letter-spacing: 0; }
    .row { display: flex; gap: 8px; min-width: 0; }
    input, textarea, select { width: 100%; box-sizing: border-box; color: #f4f7f5; background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.18); border-radius: 6px; padding: 8px 10px; font: inherit; outline: none; }
    textarea { min-height: 64px; resize: vertical; }
    #params { min-height: 44px; font-size: 12px; }
    button { appearance: none; border: 1px solid rgba(255,255,255,0.22); background: #287d6f; color: white; border-radius: 6px; padding: 8px 11px; font: inherit; font-weight: 650; white-space: nowrap; cursor: pointer; }
    button.secondary { background: rgba(255,255,255,0.08); }
    .stats { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; font-size: 12px; }
    .stat { padding: 8px; border-radius: 6px; background: rgba(255,255,255,0.08); }
    .stat strong { display: block; font-size: 18px; margin-top: 2px; }
    .detail { min-height: 0; overflow: auto; padding: 12px; font-size: 12px; line-height: 1.4; }
    pre { white-space: pre-wrap; overflow-wrap: anywhere; margin: 0; }
    #status { min-height: 18px; color: #f0c36a; font-size: 12px; }
  </style>
</head>
<body>
  <canvas id="scene"></canvas>
  <div id="labels"></div>
  <div id="panel">
    <div class="bar">
      <h1>neo4r graph console</h1>
      <div class="row">
        <input id="authToken" type="password" aria-label="login token" placeholder="login token">
        <button class="secondary" id="login">Login</button>
      </div>
      <div class="row">
        <select id="database" aria-label="database"></select>
        <input id="newDatabase" type="text" value="tenant_a" aria-label="new database">
        <button class="secondary" id="createDatabase">Create DB</button>
        <button class="secondary" id="disableDatabase">Disable</button>
        <button class="secondary" id="enableDatabase">Enable</button>
        <button class="secondary" id="deleteDatabase">Delete</button>
      </div>
      <div class="row">
        <input id="limit" type="number" min="1" value="1000" aria-label="graph limit">
        <button id="refresh">Refresh</button>
      </div>
      <div class="row">
        <select id="examples" aria-label="query examples"></select>
        <select id="history" aria-label="query history"></select>
      </div>
      <textarea id="query" spellcheck="false">MATCH (n) RETURN n</textarea>
      <textarea id="params" spellcheck="false" aria-label="query params">{"name":"Grace","role":"Backend Engineer","age":31,"company":"Neo4r Labs","since":2026}</textarea>
      <div class="row">
        <button id="run">Run Query</button>
        <button class="secondary" id="plan">Plan</button>
        <button class="secondary" id="profile">Profile</button>
      </div>
      <div class="row">
        <button class="secondary" id="storage">Storage</button>
        <button class="secondary" id="stats">Stats</button>
        <button class="secondary" id="metrics">Metrics</button>
      </div>
      <div class="row">
        <button class="secondary" id="cluster">Cluster</button>
        <button class="secondary" id="topology">Topology</button>
        <button class="secondary" id="rebalance">Rebalance</button>
        <button class="secondary" id="slow">Slow</button>
      </div>
      <div class="row">
        <input id="backupPath" type="text" value="./backup/default" aria-label="backup path">
        <button class="secondary" id="backup">Backup</button>
        <button class="secondary" id="restoreDryRun">Verify Restore</button>
      </div>
      <div class="row">
        <input id="restoreConfirm" type="text" aria-label="restore confirmation" placeholder="RESTORE">
        <button class="secondary" id="restoreApply">Restore</button>
      </div>
      <div class="row">
        <button class="secondary" id="maintenanceOn">Maintenance On</button>
        <button class="secondary" id="maintenanceOff">Maintenance Off</button>
      </div>
      <div class="row">
        <button class="secondary" id="raftStatus">Raft</button>
        <button class="secondary" id="snapshotNow">Snapshot</button>
        <button class="secondary" id="verifyInvariants">Verify</button>
        <button class="secondary" id="repairInvariants">Repair</button>
      </div>
      <div class="row">
        <input id="adminUser" type="text" value="operator" aria-label="admin user">
        <select id="adminRole" aria-label="admin role">
          <option value="reader">reader</option>
          <option value="writer">writer</option>
          <option value="admin">admin</option>
        </select>
      </div>
      <div class="row">
        <input id="adminTokenId" type="text" value="main" aria-label="admin token id">
        <input id="adminExpiredAt" type="datetime-local" aria-label="admin token expiration">
      </div>
      <div class="row">
        <input id="adminToken" type="text" value="writer:operator-token" aria-label="admin token">
      </div>
      <div class="row">
        <select id="adminDatabaseRole" aria-label="admin database role">
          <option value="reader">reader on DB</option>
          <option value="writer" selected>writer on DB</option>
          <option value="admin">admin on DB</option>
        </select>
        <button class="secondary" id="users">Users</button>
        <button class="secondary" id="databases">DBs</button>
        <button class="secondary" id="auditLog">Audit</button>
        <button class="secondary" id="invokeToken">Invoke</button>
        <button class="secondary" id="revokeToken">Revoke</button>
        <button class="secondary" id="cleanupTokens">Cleanup</button>
        <button class="secondary" id="deleteUser">Delete</button>
      </div>
      <div class="stats">
        <div class="stat">Nodes<strong id="nodeCount">0</strong></div>
        <div class="stat">Edges<strong id="edgeCount">0</strong></div>
        <div class="stat">Selected<strong id="selectedKind">none</strong></div>
      </div>
      <div id="status"></div>
    </div>
    <div class="detail"><pre id="detail">Select a node or run a query.</pre></div>
  </div>
  <script type="module">
    import * as THREE from 'https://cdn.jsdelivr.net/npm/three@0.166.1/build/three.module.js';

    const canvas = document.getElementById('scene');
    const labelLayer = document.getElementById('labels');
    const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x151515);
    const camera = new THREE.PerspectiveCamera(60, 1, 0.1, 5000);
    camera.position.set(0, 90, 210);
    camera.lookAt(95, 0, 0);
    scene.add(new THREE.AmbientLight(0xffffff, 0.55));
    const light = new THREE.DirectionalLight(0xffffff, 1.25);
    light.position.set(120, 180, 100);
    scene.add(light);

    const nodesGroup = new THREE.Group();
    const edgesGroup = new THREE.Group();
    nodesGroup.position.x = 95;
    edgesGroup.position.x = 95;
    scene.add(edgesGroup, nodesGroup);
    const raycaster = new THREE.Raycaster();
    const pointer = new THREE.Vector2();
    const nodeMeshes = [];
    const graphLabels = [];
    const history = JSON.parse(localStorage.getItem('neo4r.queryHistory') || '[]');
    const savedAuthToken = localStorage.getItem('neo4r.authToken') || sessionCookieToken() || new URLSearchParams(window.location.search).get('token') || '';
    let graph = { nodes: [], relationships: [] };
    let selected = null;
    let dragging = false;
    let last = { x: 0, y: 0 };
    const palette = [0x2f80ed, 0x27ae60, 0xf2c94c, 0xeb5757, 0x9b51e0, 0x56ccf2, 0xf2994a];

    function resize() {
      const width = window.innerWidth;
      const height = window.innerHeight;
      renderer.setSize(width, height, false);
      camera.aspect = width / height;
      camera.updateProjectionMatrix();
    }

    function labelColor(labels) {
      const label = labels && labels.length ? labels[0] : 'Node';
      let hash = 0;
      for (const ch of label) hash = (hash * 31 + ch.charCodeAt(0)) >>> 0;
      return palette[hash % palette.length];
    }

    function nodeLabel(node) {
      return node.properties.name || node.properties.title || node.properties.email || (node.labels[0] || 'Node') + ' #' + node.id;
    }

    function authToken() {
      return document.getElementById('authToken').value.trim();
    }

    function sessionCookieToken() {
      const prefix = 'neo4r.session=';
      const cookie = document.cookie.split(';').map((part) => part.trim()).find((part) => part.startsWith(prefix));
      return cookie ? decodeURIComponent(cookie.slice(prefix.length)) : '';
    }

    function selectedDatabase() {
      return document.getElementById('database').value || 'default';
    }

    function authHeaders(extra = {}) {
      const headers = { ...extra };
      const token = authToken();
      if (token) headers.authorization = 'Bearer ' + token;
      headers['x-neo4r-database'] = selectedDatabase();
      return headers;
    }

    function saveLogin() {
      localStorage.setItem('neo4r.authToken', authToken());
      document.cookie = 'neo4r.session=' + encodeURIComponent(authToken()) + '; SameSite=Strict; path=/';
      setStatus('Login saved.');
    }

    function createGraphLabel(kind, text, position, group) {
      const element = document.createElement('div');
      element.className = 'graph-label ' + kind;
      element.textContent = text;
      labelLayer.appendChild(element);
      graphLabels.push({ element, position, group });
    }

    function clearGraphLabels() {
      graphLabels.length = 0;
      labelLayer.replaceChildren();
    }

    function rebuildScene() {
      nodesGroup.clear();
      edgesGroup.clear();
      clearGraphLabels();
      nodeMeshes.length = 0;
      const nodeById = new Map();
      const count = Math.max(graph.nodes.length, 1);
      const radius = Math.max(70, Math.sqrt(count) * 18);
      graph.nodes.forEach((node, index) => {
        const angle = index * 2.399963229728653;
        const y = ((index % 17) - 8) * 8;
        const r = radius * Math.sqrt((index + 0.5) / count);
        node.position = new THREE.Vector3(Math.cos(angle) * r, y, Math.sin(angle) * r);
        nodeById.set(node.id, node);
        const geometry = new THREE.SphereGeometry(9, 28, 18);
        const material = new THREE.MeshStandardMaterial({ color: labelColor(node.labels), emissive: labelColor(node.labels), emissiveIntensity: 0.16, roughness: 0.42, metalness: 0.06 });
        const mesh = new THREE.Mesh(geometry, material);
        mesh.position.copy(node.position);
        mesh.userData = { kind: 'node', data: node };
        nodeMeshes.push(mesh);
        nodesGroup.add(mesh);
        createGraphLabel('node', nodeLabel(node), node.position.clone().add(new THREE.Vector3(0, 14, 0)), nodesGroup);
      });
      graph.relationships.forEach((rel) => {
        const from = nodeById.get(rel.from);
        const to = nodeById.get(rel.to);
        if (!from || !to) return;
        const points = [from.position, to.position];
        const geometry = new THREE.BufferGeometry().setFromPoints(points);
        const material = new THREE.LineBasicMaterial({ color: 0xb7c4cc, transparent: true, opacity: 0.62 });
        const line = new THREE.Line(geometry, material);
        line.userData = { kind: 'relationship', data: rel };
        edgesGroup.add(line);
        const midpoint = from.position.clone().add(to.position).multiplyScalar(0.5);
        createGraphLabel('edge', rel.type, midpoint, edgesGroup);
      });
      document.getElementById('nodeCount').textContent = graph.nodes.length;
      document.getElementById('edgeCount').textContent = graph.relationships.length;
    }

    async function loadGraph() {
      const limit = encodeURIComponent(document.getElementById('limit').value || '1000');
      setStatus('Loading graph...');
      const response = await fetch('/api/graph?limit=' + limit, { headers: authHeaders() });
      const payload = await response.json();
      if (!response.ok) {
        document.getElementById('detail').textContent = JSON.stringify(payload, null, 2);
        setStatus('Graph load failed.');
        return;
      }
      graph = payload;
      rebuildScene();
      setStatus('Graph loaded.');
    }

    function queryPayload() {
      const query = document.getElementById('query').value;
      const rawParams = document.getElementById('params').value.trim();
      let params = {};
      if (rawParams) params = JSON.parse(rawParams);
      return { database: selectedDatabase(), query, params };
    }

    function rememberQuery(query) {
      const trimmed = query.trim();
      if (!trimmed) return;
      const index = history.indexOf(trimmed);
      if (index >= 0) history.splice(index, 1);
      history.unshift(trimmed);
      history.splice(20);
      localStorage.setItem('neo4r.queryHistory', JSON.stringify(history));
      renderHistory();
    }

    async function postJson(path, payload) {
      const response = await fetch(path, {
        method: 'POST',
        headers: authHeaders({ 'content-type': 'application/json' }),
        body: JSON.stringify(payload)
      });
      const body = await response.json();
      return { response, payload: body };
    }

    async function runQuery() {
      setStatus('Running query...');
      const payload = queryPayload();
      const result = await postJson('/api/query', payload);
      rememberQuery(payload.query);
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'Query complete.' : 'Query failed.');
      await loadGraph();
    }

    async function runPlan(path, status) {
      setStatus(status + '...');
      const payload = queryPayload();
      const result = await postJson(path, payload);
      rememberQuery(payload.query);
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? status + ' complete.' : status + ' failed.');
    }

    async function runClusterAction(path, label) {
      setStatus(label + '...');
      const result = await postJson(path, {});
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? label + ' complete.' : label + ' failed.');
    }

    async function runBackup() {
      setStatus('Backup...');
      const result = await postJson('/api/backup', { database: selectedDatabase(), path: document.getElementById('backupPath').value.trim() });
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'Backup complete.' : 'Backup failed.');
    }

    async function runRestoreDryRun() {
      setStatus('Verify restore...');
      const result = await postJson('/api/restore', { database: selectedDatabase(), path: document.getElementById('backupPath').value.trim(), verify_only: true });
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'Verify restore complete.' : 'Verify restore failed.');
    }

    async function runRestoreApply() {
      setStatus('Restore...');
      const result = await postJson('/api/restore', {
        database: selectedDatabase(),
        path: document.getElementById('backupPath').value.trim(),
        dry_run: false,
        confirm: document.getElementById('restoreConfirm').value.trim()
      });
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'Restore complete.' : 'Restore failed.');
    }

    async function setMaintenanceMode(enabled) {
      setStatus(enabled ? 'Maintenance on...' : 'Maintenance off...');
      const result = await postJson('/api/admin/maintenance-mode', { database: selectedDatabase(), enabled });
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'Maintenance updated.' : 'Maintenance update failed.');
    }

    function userPayload() {
      const expires = document.getElementById('adminExpiredAt').value;
      const expiredAt = expires ? String(Math.floor(new Date(expires).getTime() / 1000)) : '0';
      return {
        name: document.getElementById('adminUser').value.trim(),
        token_id: document.getElementById('adminTokenId').value.trim(),
        role: document.getElementById('adminRole').value,
        token: document.getElementById('adminToken').value.trim(),
        expired_at: expiredAt,
        database: selectedDatabase(),
        database_role: document.getElementById('adminDatabaseRole').value
      };
    }

    async function loadDatabases() {
      const response = await fetch('/api/admin/databases', { headers: authHeaders() });
      const payload = await response.json();
      const select = document.getElementById('database');
      const current = select.value || localStorage.getItem('neo4r.database') || 'default';
      select.replaceChildren();
      for (const database of payload.databases || [{ name: 'default' }]) {
        select.appendChild(new Option(database.name, database.name));
      }
      select.value = [...select.options].some((option) => option.value === current) ? current : 'default';
    }

    async function createDatabase() {
      setStatus('Creating database...');
      const name = document.getElementById('newDatabase').value.trim();
      const result = await postJson('/api/admin/databases', { name });
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      if (result.response.ok) {
        await loadDatabases();
        document.getElementById('database').value = name;
        localStorage.setItem('neo4r.database', name);
        setStatus('Database created.');
        await loadGraph();
      } else {
        setStatus('Database create failed.');
      }
    }

    async function updateDatabaseLifecycle(path, statusText) {
      const name = selectedDatabase();
      setStatus(statusText + '...');
      const result = await postJson(path, { name });
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      await loadDatabases();
      setStatus(result.response.ok ? statusText + ' complete.' : statusText + ' failed.');
    }

    async function invokeToken() {
      setStatus('Invoking token...');
      const result = await postJson('/api/admin/invoke-token', userPayload());
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'Token invoked.' : 'Token invoke failed.');
    }

    async function revokeToken() {
      setStatus('Revoking token...');
      const result = await postJson('/api/admin/revoke-token', {
        name: document.getElementById('adminUser').value.trim(),
        token_id: document.getElementById('adminTokenId').value.trim()
      });
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'Token revoked.' : 'Token revoke failed.');
    }

    async function cleanupTokens() {
      setStatus('Cleaning tokens...');
      const result = await postJson('/api/admin/cleanup-expired-tokens', {});
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'Token cleanup complete.' : 'Token cleanup failed.');
    }

    async function deleteUser() {
      setStatus('Deleting user...');
      const result = await postJson('/api/admin/delete-user', { name: document.getElementById('adminUser').value.trim() });
      document.getElementById('detail').textContent = JSON.stringify(result.payload, null, 2);
      setStatus(result.response.ok ? 'User deleted.' : 'User delete failed.');
    }

    async function loadExamples() {
      const response = await fetch('/api/examples', { headers: authHeaders() });
      const payload = await response.json();
      const select = document.getElementById('examples');
      select.replaceChildren(new Option('Examples', ''));
      for (const example of payload.examples || []) {
        select.appendChild(new Option(example.name, example.query));
      }
    }

    function renderHistory() {
      const select = document.getElementById('history');
      select.replaceChildren(new Option('History', ''));
      for (const query of history) {
        select.appendChild(new Option(query.split('\n')[0].slice(0, 42), query));
      }
    }

    async function showManagement(path) {
      const response = await fetch(path, { headers: authHeaders() });
      const payload = await response.json();
      document.getElementById('detail').textContent = JSON.stringify(payload, null, 2);
      setStatus(response.ok ? path + ' loaded.' : path + ' failed.');
    }

    async function showTopology() {
      setStatus('Loading topology...');
      const [registryResponse, raftResponse] = await Promise.all([
        fetch('/api/cluster/registry', { headers: authHeaders() }),
        fetch('/api/admin/raft-status', { headers: authHeaders() })
      ]);
      const registry = await registryResponse.json();
      const raft = await raftResponse.json();
      document.getElementById('detail').textContent = JSON.stringify({
        registry,
        raft,
        migration_state: registry.migration_state || 'idle',
        ownership_epoch: registry.ownership_epoch || 0,
        registry_expires_at_ms: (registry.generated_at_ms || 0) + (registry.ttl_ms || 0)
      }, null, 2);
      setStatus(registryResponse.ok && raftResponse.ok ? 'Topology loaded.' : 'Topology failed.');
    }

    function setStatus(value) {
      document.getElementById('status').textContent = value;
    }

    function pick(event) {
      const rect = renderer.domElement.getBoundingClientRect();
      pointer.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
      pointer.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;
      raycaster.setFromCamera(pointer, camera);
      const hit = raycaster.intersectObjects(nodeMeshes, false)[0];
      if (!hit) return;
      selected = hit.object.userData;
      document.getElementById('selectedKind').textContent = selected.kind;
      document.getElementById('detail').textContent = JSON.stringify(selected.data, null, 2);
    }

    function updateGraphLabels() {
      const width = renderer.domElement.clientWidth;
      const height = renderer.domElement.clientHeight;
      for (const label of graphLabels) {
        const world = label.position.clone();
        label.group.localToWorld(world);
        world.project(camera);
        const x = (world.x * 0.5 + 0.5) * width;
        const y = (-world.y * 0.5 + 0.5) * height;
        const hidden = world.z < -1 || world.z > 1 || x < 0 || x > width || y < 0 || y > height;
        label.element.style.opacity = hidden ? '0' : '1';
        label.element.style.transform = `translate(${x}px, ${y}px) translate(-50%, -50%)`;
      }
    }

    function animate() {
      nodesGroup.rotation.y += 0.0018;
      edgesGroup.rotation.y = nodesGroup.rotation.y;
      updateGraphLabels();
      renderer.render(scene, camera);
      requestAnimationFrame(animate);
    }

    window.addEventListener('resize', resize);
    canvas.addEventListener('click', pick);
    canvas.addEventListener('pointerdown', (event) => { dragging = true; last = { x: event.clientX, y: event.clientY }; });
    canvas.addEventListener('pointerup', () => { dragging = false; });
    canvas.addEventListener('pointermove', (event) => {
      if (!dragging) return;
      const dx = event.clientX - last.x;
      const dy = event.clientY - last.y;
      nodesGroup.rotation.y += dx * 0.006;
      nodesGroup.rotation.x += dy * 0.004;
      edgesGroup.rotation.copy(nodesGroup.rotation);
      last = { x: event.clientX, y: event.clientY };
    });
    canvas.addEventListener('wheel', (event) => {
      event.preventDefault();
      camera.position.z = Math.max(40, Math.min(900, camera.position.z + event.deltaY * 0.25));
    }, { passive: false });
    document.getElementById('authToken').value = savedAuthToken;
    document.getElementById('login').addEventListener('click', saveLogin);
    document.getElementById('database').addEventListener('change', () => {
      localStorage.setItem('neo4r.database', selectedDatabase());
      loadGraph().catch((err) => setStatus(String(err)));
    });
    document.getElementById('createDatabase').addEventListener('click', createDatabase);
    document.getElementById('disableDatabase').addEventListener('click', () => updateDatabaseLifecycle('/api/admin/disable-database', 'Disable database'));
    document.getElementById('enableDatabase').addEventListener('click', () => updateDatabaseLifecycle('/api/admin/enable-database', 'Enable database'));
    document.getElementById('deleteDatabase').addEventListener('click', () => updateDatabaseLifecycle('/api/admin/delete-database', 'Delete database'));
    document.getElementById('refresh').addEventListener('click', loadGraph);
    document.getElementById('run').addEventListener('click', runQuery);
    document.getElementById('plan').addEventListener('click', () => runPlan('/api/query-plan', 'Plan'));
    document.getElementById('profile').addEventListener('click', () => runPlan('/api/profile', 'Profile'));
    document.getElementById('storage').addEventListener('click', () => showManagement('/api/storage'));
    document.getElementById('stats').addEventListener('click', () => showManagement('/api/statistics'));
    document.getElementById('metrics').addEventListener('click', () => showManagement('/api/metrics'));
    document.getElementById('cluster').addEventListener('click', () => showManagement('/api/cluster'));
    document.getElementById('topology').addEventListener('click', showTopology);
    document.getElementById('rebalance').addEventListener('click', () => runClusterAction('/api/cluster/advance-rebalance', 'Rebalance'));
    document.getElementById('slow').addEventListener('click', () => showManagement('/api/slow-queries'));
    document.getElementById('backup').addEventListener('click', runBackup);
    document.getElementById('restoreDryRun').addEventListener('click', runRestoreDryRun);
    document.getElementById('restoreApply').addEventListener('click', runRestoreApply);
    document.getElementById('maintenanceOn').addEventListener('click', () => setMaintenanceMode(true));
    document.getElementById('maintenanceOff').addEventListener('click', () => setMaintenanceMode(false));
    document.getElementById('raftStatus').addEventListener('click', () => showManagement('/api/admin/raft-status'));
    document.getElementById('snapshotNow').addEventListener('click', () => runClusterAction('/api/admin/snapshot-now', 'Snapshot'));
    document.getElementById('verifyInvariants').addEventListener('click', () => runClusterAction('/api/admin/verify-invariants', 'Verify invariants'));
    document.getElementById('repairInvariants').addEventListener('click', () => runClusterAction('/api/admin/repair-invariants', 'Repair invariants'));
    document.getElementById('users').addEventListener('click', () => showManagement('/api/admin/users'));
    document.getElementById('databases').addEventListener('click', () => showManagement('/api/admin/databases'));
    document.getElementById('auditLog').addEventListener('click', () => showManagement('/api/admin/audit-log'));
    document.getElementById('invokeToken').addEventListener('click', invokeToken);
    document.getElementById('revokeToken').addEventListener('click', revokeToken);
    document.getElementById('cleanupTokens').addEventListener('click', cleanupTokens);
    document.getElementById('deleteUser').addEventListener('click', deleteUser);
    document.getElementById('examples').addEventListener('change', (event) => {
      if (event.target.value) document.getElementById('query').value = event.target.value;
    });
    document.getElementById('history').addEventListener('change', (event) => {
      if (event.target.value) document.getElementById('query').value = event.target.value;
    });
    resize();
    renderHistory();
    loadDatabases()
      .catch((err) => setStatus(String(err)))
      .finally(() => {
        loadExamples().catch((err) => setStatus(String(err)));
        loadGraph().catch((err) => setStatus(String(err)));
      });
    animate();
  </script>
</body>
</html>"#;
