const express = require('express');
const http = require('http');
const https = require('https');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3000;

app.use(express.static(path.join(__dirname, 'public')));

// ============================================================
// PRE-CACHE SYSTEM
// All datasets are fetched on startup and refreshed periodically.
// Client requests are always served from memory → instant response.
// ============================================================

const cache = {};  // key -> { data, contentType, ts }

function httpGet(url) {
  const mod = url.startsWith('https') ? https : http;
  return new Promise((resolve, reject) => {
    mod.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    }).on('error', reject);
  });
}

// Fetch ALL pages of a datos.santander.es dataset
async function fetchAllPages(dataset, pageSize = 500) {
  const firstRaw = await httpGet(`http://datos.santander.es/api/rest/datasets/${dataset}.json?items=${pageSize}&page=1`);
  const first = JSON.parse(firstRaw);
  const totalPages = (first.summary && first.summary.pages) || 1;
  let allResources = first.resources || [];

  if (totalPages > 1) {
    for (let batch = 2; batch <= totalPages; batch += 10) {
      const promises = [];
      for (let p = batch; p <= Math.min(batch + 9, totalPages); p++) {
        promises.push(httpGet(`http://datos.santander.es/api/rest/datasets/${dataset}.json?items=${pageSize}&page=${p}`));
      }
      const results = await Promise.all(promises);
      for (const raw of results) {
        try {
          const json = JSON.parse(raw);
          if (json.resources) allResources = allResources.concat(json.resources);
        } catch {}
      }
    }
  }

  return {
    summary: { items: allResources.length, items_per_page: allResources.length, pages: 1, current_page: 1 },
    resources: allResources
  };
}

// Fetch a single page dataset
async function fetchSinglePage(dataset, items = 500) {
  const raw = await httpGet(`http://datos.santander.es/api/rest/datasets/${dataset}.json?items=${items}&page=1`);
  return JSON.parse(raw);
}

// Cache a datos.santander.es dataset (all pages merged)
async function cacheDataset(key, dataset, opts = {}) {
  const { singlePage, items } = opts;
  try {
    const data = singlePage
      ? await fetchSinglePage(dataset, items || 500)
      : await fetchAllPages(dataset);
    // Retry once if we got 0 resources unexpectedly (API rate-limit on startup)
    if (!data.resources || data.resources.length === 0) {
      console.log(`[cache] ${key}: 0 resources, retrying in 5s...`);
      await new Promise(r => setTimeout(r, 5000));
      const retry = singlePage
        ? await fetchSinglePage(dataset, items || 500)
        : await fetchAllPages(dataset);
      if (retry.resources && retry.resources.length > 0) {
        cache[key] = { data: JSON.stringify(retry), contentType: 'application/json', ts: Date.now() };
        console.log(`[cache] ${key}: ${retry.resources.length} resources (retry)`);
        return;
      }
    }
    cache[key] = { data: JSON.stringify(data), contentType: 'application/json', ts: Date.now() };
    console.log(`[cache] ${key}: ${data.resources?.length || 0} resources`);
  } catch (e) {
    console.error(`[cache] ${key} failed:`, e.message);
  }
}

// Cache an external URL
async function cacheExternal(key, url, contentType = 'application/json') {
  try {
    const data = await httpGet(url);
    cache[key] = { data, contentType, ts: Date.now() };
    console.log(`[cache] ${key}: ${(data.length / 1024).toFixed(1)}KB`);
  } catch (e) {
    console.error(`[cache] ${key} failed:`, e.message);
  }
}

// Serve from cache; if page/items query params, slice from full cache
function serveCache(key, req, res, maxAge = 60) {
  const entry = cache[key];
  if (!entry) return res.status(503).json({ error: 'Data loading, try again shortly' });

  res.setHeader('Content-Type', entry.contentType);
  res.setHeader('Cache-Control', `public, max-age=${maxAge}`);

  // Support pagination from cached full dataset
  if (entry.contentType === 'application/json' && (req.query.page || req.query.items)) {
    try {
      const full = JSON.parse(entry.data);
      if (full.resources) {
        const pageSize = parseInt(req.query.items) || 200;
        const page = parseInt(req.query.page) || 1;
        const start = (page - 1) * pageSize;
        const sliced = full.resources.slice(start, start + pageSize);
        return res.json({
          summary: { items: full.resources.length, items_per_page: pageSize, pages: Math.ceil(full.resources.length / pageSize), current_page: page },
          resources: sliced
        });
      }
    } catch {}
  }

  res.send(entry.data);
}

// ============================================================
// DATASET DEFINITIONS & REFRESH INTERVALS
// ============================================================

const DATASETS = {
  // Waste (2 min refresh - live fill levels)
  contenedores:   { dataset: 'residuos_contenedores', refresh: 120000 },
  papeleras:      { dataset: 'residuos_papeleras', refresh: 3600000 },
  vehiculos:      { dataset: 'residuos_vehiculos', refresh: 120000 },

  // Transport
  paradas:        { dataset: 'paradas_bus', refresh: 86400000 },
  lineas:         { dataset: 'lineas_bus', refresh: 86400000 },
  'buses-eta':    { dataset: 'control_flotas_estimaciones', refresh: 60000 },

  // Parking & Mobility
  'zonas-ola':    { dataset: 'zonas_ola', singlePage: true, items: 50, refresh: 86400000 },
  'plazas-motos': { dataset: 'plazas_motos', refresh: 86400000 },
  'plazas-pmr':   { dataset: 'plazas_pmr', refresh: 86400000 },
  'zonas-carga':  { dataset: 'zonas_carga', refresh: 86400000 },
  'carril-bici':  { dataset: 'carril_bici', refresh: 86400000 },
  'zonas-30':     { dataset: 'zonas_30', refresh: 86400000 },

  // Districts
  distritos:      { dataset: 'distritos', singlePage: true, items: 20, refresh: 86400000 },
  secciones:      { dataset: 'secciones', refresh: 86400000 },

  // Environment
  sensores:       { dataset: 'sensores_smart_env_monitoring', refresh: 300000 },
  'agua-calidad': { dataset: 'agua_calidad', singlePage: true, items: 50, refresh: 3600000 },
  saneamiento:    { dataset: 'agua_estado_red_saneamiento', refresh: 3600000 },

  // Commerce
  comercios:      { dataset: 'comercios_comercios', refresh: 86400000 },
  mercados:       { dataset: 'mercados_mercados', singlePage: true, items: 50, refresh: 86400000 },
  hosteleria:     { dataset: 'establecimientos_hosteleros', refresh: 86400000 },

  // Parks
  parques:        { dataset: 'parques', singlePage: true, items: 20, refresh: 86400000 },
  jardines:       { dataset: 'jardines', refresh: 86400000 },

  // Tourism
  playas:         { dataset: 'puntos_interes_playa', singlePage: true, items: 100, refresh: 86400000 },
  museos:         { dataset: 'puntos_interes_museos', singlePage: true, items: 100, refresh: 86400000 },
  monumentos:     { dataset: 'puntos_interes_monumento', singlePage: true, items: 100, refresh: 86400000 },
  'edificios-interes': { dataset: 'puntos_interes_edificio_interes', singlePage: true, items: 100, refresh: 86400000 },
  deporte:        { dataset: 'puntos_interes_deporte', singlePage: true, items: 100, refresh: 86400000 },
  bibliotecas:    { dataset: 'puntos_interes_bibliotecas', singlePage: true, items: 100, refresh: 86400000 },
};

const EXTERNALS = {
  tusbic:    { url: 'https://api.nextbike.net/maps/nextbike-live.json?city=914', refresh: 30000, contentType: 'application/json' },
  aemet:     { url: 'https://www.aemet.es/xml/municipios/localidad_39075.xml', refresh: 1800000, contentType: 'application/xml' },
  polen:     { url: 'https://air-quality-api.open-meteo.com/v1/air-quality?latitude=43.46&longitude=-3.81&current=alder_pollen,birch_pollen,grass_pollen,mugwort_pollen,olive_pollen,ragweed_pollen&timezone=Europe/Madrid', refresh: 3600000, contentType: 'application/json' },
  sismos:    { url: 'http://www.ign.es/ign/RssTools/sismologia.xml', refresh: 300000, contentType: 'application/xml' },
  noticias:  { url: 'https://santander.es/rss/noticia/rss.xml', refresh: 1800000, contentType: 'application/xml' },
};

// ============================================================
// ROUTES — all served from cache
// ============================================================

// Waste
app.get('/api/contenedores', (req, res) => serveCache('contenedores', req, res, 60));
app.get('/api/papeleras', (req, res) => serveCache('papeleras', req, res, 300));
app.get('/api/vehiculos', (req, res) => serveCache('vehiculos', req, res, 60));

// Transport
app.get('/api/paradas', (req, res) => serveCache('paradas', req, res, 300));
app.get('/api/lineas', (req, res) => serveCache('lineas', req, res, 300));
app.get('/api/buses-eta', (req, res) => serveCache('buses-eta', req, res, 30));

// Parking & Mobility
app.get('/api/zonas-ola', (req, res) => serveCache('zonas-ola', req, res, 300));
app.get('/api/plazas-motos', (req, res) => serveCache('plazas-motos', req, res, 300));
app.get('/api/plazas-pmr', (req, res) => serveCache('plazas-pmr', req, res, 300));
app.get('/api/zonas-carga', (req, res) => serveCache('zonas-carga', req, res, 300));
app.get('/api/carril-bici', (req, res) => serveCache('carril-bici', req, res, 300));
app.get('/api/zonas-30', (req, res) => serveCache('zonas-30', req, res, 300));

// Districts
app.get('/api/distritos', (req, res) => serveCache('distritos', req, res, 300));
app.get('/api/secciones', (req, res) => serveCache('secciones', req, res, 300));

// Environment
app.get('/api/sensores', (req, res) => serveCache('sensores', req, res, 120));
app.get('/api/agua-calidad', (req, res) => serveCache('agua-calidad', req, res, 300));
app.get('/api/saneamiento', (req, res) => serveCache('saneamiento', req, res, 300));

// Commerce
app.get('/api/comercios', (req, res) => serveCache('comercios', req, res, 300));
app.get('/api/mercados', (req, res) => serveCache('mercados', req, res, 300));
app.get('/api/hosteleria', (req, res) => serveCache('hosteleria', req, res, 300));

// Parks
app.get('/api/parques', (req, res) => serveCache('parques', req, res, 300));
app.get('/api/jardines', (req, res) => serveCache('jardines', req, res, 300));

// Tourism
app.get('/api/playas', (req, res) => serveCache('playas', req, res, 300));
app.get('/api/museos', (req, res) => serveCache('museos', req, res, 300));
app.get('/api/monumentos', (req, res) => serveCache('monumentos', req, res, 300));
app.get('/api/edificios-interes', (req, res) => serveCache('edificios-interes', req, res, 300));
app.get('/api/deporte', (req, res) => serveCache('deporte', req, res, 300));
app.get('/api/bibliotecas', (req, res) => serveCache('bibliotecas', req, res, 300));

// External APIs
app.get('/api/tusbic', (req, res) => serveCache('tusbic', req, res, 15));
app.get('/api/aemet', (req, res) => serveCache('aemet', req, res, 600));
app.get('/api/polen', (req, res) => serveCache('polen', req, res, 600));
app.get('/api/sismos', (req, res) => serveCache('sismos', req, res, 120));
app.get('/api/noticias', (req, res) => serveCache('noticias', req, res, 600));

// ============================================================
// BUS LIVE TRACKING (special handling — aggregation logic)
// ============================================================

function fetchDataset(dataset, items, page) {
  return new Promise((resolve) => {
    const url = `http://datos.santander.es/api/rest/datasets/${dataset}.json?items=${items}&page=${page}`;
    http.get(url, (apiRes) => {
      let data = '';
      apiRes.on('data', chunk => data += chunk);
      apiRes.on('end', () => { try { resolve(JSON.parse(data)); } catch { resolve({ resources: [] }); } });
    }).on('error', () => resolve({ resources: [] }));
  });
}

// Bus lines cache
let linesCache = { data: null, ts: 0 };
async function getBusLines() {
  if (linesCache.data && Date.now() - linesCache.ts < 86400000) return linesCache.data;
  try {
    const full = cache['lineas'] ? JSON.parse(cache['lineas'].data) : await fetchDataset('lineas_bus', 500, 1);
    const map = {};
    (full.resources || []).forEach(l => {
      const id = l['dc:identifier'], num = l['ayto:numero'] || '', name = l['dc:name'] || '';
      if (id) map[id] = { num, name };
    });
    linesCache = { data: map, ts: Date.now() };
    return map;
  } catch { return linesCache.data || {}; }
}

const SANT_BOUNDS = { latMin: 43.4, latMax: 43.52, lngMin: -3.92, lngMax: -3.74 };
function validCoords(lat, lng) {
  return lat >= SANT_BOUNDS.latMin && lat <= SANT_BOUNDS.latMax &&
         lng >= SANT_BOUNDS.lngMin && lng <= SANT_BOUNDS.lngMax;
}

let busCache = { data: null, ts: 0 };

async function refreshBuses() {
  try {
    const latest = new Map();
    function processResources(resources) {
      if (!resources) return;
      resources.forEach(b => {
        const vid = b['ayto:vehiculo'];
        if (!vid) return;
        const lat = parseFloat(b['wgs84_pos:lat']), lng = parseFloat(b['wgs84_pos:long']);
        if (isNaN(lat) || isNaN(lng) || !validCoords(lat, lng)) return;
        const entry = latest.get(vid);
        if (!entry) {
          latest.set(vid, { current: b, previous: null });
        } else if (b['ayto:instante'] > entry.current['ayto:instante']) {
          latest.set(vid, { current: b, previous: entry.current });
        } else if (!entry.previous || b['ayto:instante'] > entry.previous['ayto:instante']) {
          entry.previous = b;
        }
      });
    }

    const firstPage = await fetchDataset('control_flotas_posiciones', 500, 1);
    processResources(firstPage.resources);
    const totalPages = (firstPage.summary && firstPage.summary.pages) || 1;

    for (let batch = 2; batch <= totalPages; batch += 10) {
      const promises = [];
      for (let p = batch; p <= Math.min(batch + 9, totalPages); p++) {
        promises.push(fetchDataset('control_flotas_posiciones', 500, p));
      }
      const results = await Promise.all(promises);
      results.forEach(json => processResources(json.resources));
    }

    let maxInstante = '';
    latest.forEach(({ current }) => { if (current['ayto:instante'] > maxInstante) maxInstante = current['ayto:instante']; });
    const cutoff = new Date(new Date(maxInstante).getTime() - 30 * 60000).toISOString();

    const lineNames = await getBusLines();
    const activeLines = new Set();
    const active = [];
    const staleCutoff = new Date(new Date(maxInstante).getTime() - 5 * 60000).toISOString();
    latest.forEach(({ current, previous }) => {
      if (current['ayto:instante'] < cutoff) return;
      const enriched = { ...current };
      if (previous) { enriched._prevLat = previous['wgs84_pos:lat']; enriched._prevLng = previous['wgs84_pos:long']; }
      const line = current['ayto:linea'] || '';
      const lineInfo = lineNames[line] || {};
      enriched._lineNum = lineInfo.num || line;
      enriched._routeName = lineInfo.name || '';
      enriched._stale = current['ayto:instante'] < staleCutoff;
      activeLines.add(lineInfo.num || line);
      active.push(enriched);
    });

    busCache = {
      data: {
        resources: active,
        meta: { activeCount: active.length, totalVehicles: latest.size, lines: [...activeLines].sort(), dataTimestamp: maxInstante, totalPages }
      },
      ts: Date.now()
    };
    console.log(`[cache] buses: ${active.length} active vehicles`);
  } catch (e) {
    console.error('[cache] buses failed:', e.message);
  }
}

app.get('/api/buses', (req, res) => {
  if (!busCache.data) return res.status(503).json({ error: 'Bus data loading, try again shortly' });
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'public, max-age=10');
  res.json(busCache.data);
});

// ============================================================
// BUS ROUTES (OSRM map matching — kept as-is with its own cache)
// ============================================================

function haversineMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000, toRad = d => d * Math.PI / 180;
  const dLat = toRad(lat2 - lat1), dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

const BAY_POLYGON = [
  [43.4730, -3.7920], [43.4650, -3.7700], [43.4500, -3.7650],
  [43.4380, -3.7800], [43.4380, -3.8050], [43.4500, -3.8200],
  [43.4580, -3.8150], [43.4650, -3.8050], [43.4730, -3.7920],
];
function pointInBay(lat, lng) {
  let inside = false;
  for (let i = 0, j = BAY_POLYGON.length - 1; i < BAY_POLYGON.length; j = i++) {
    const [yi, xi] = BAY_POLYGON[i], [yj, xj] = BAY_POLYGON[j];
    if (((yi > lat) !== (yj > lat)) && (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi)) inside = !inside;
  }
  return inside;
}

function decodePolyline(str) {
  const coords = [];
  let index = 0, lat = 0, lng = 0;
  while (index < str.length) {
    let shift = 0, result = 0, byte;
    do { byte = str.charCodeAt(index++) - 63; result |= (byte & 0x1f) << shift; shift += 5; } while (byte >= 0x20);
    lat += (result & 1) ? ~(result >> 1) : (result >> 1);
    shift = 0; result = 0;
    do { byte = str.charCodeAt(index++) - 63; result |= (byte & 0x1f) << shift; shift += 5; } while (byte >= 0x20);
    lng += (result & 1) ? ~(result >> 1) : (result >> 1);
    coords.push([lat / 1e5, lng / 1e5]);
  }
  return coords;
}

const OSRM_CACHE_FILE = path.join(__dirname, 'data', 'osrm-cache.json');
const OSRM_CACHE_TTL = 24 * 60 * 60 * 1000;
let osrmCache = {};
try {
  if (fs.existsSync(OSRM_CACHE_FILE)) osrmCache = JSON.parse(fs.readFileSync(OSRM_CACHE_FILE, 'utf8'));
} catch (e) { osrmCache = {}; }

function saveOsrmCache() {
  try { fs.writeFileSync(OSRM_CACHE_FILE, JSON.stringify(osrmCache), 'utf8'); } catch {}
}

function osrmRouteChunk(coords) {
  return new Promise((resolve) => {
    const coordStr = coords.map(p => `${Number(p.lng).toFixed(6)},${Number(p.lat).toFixed(6)}`).join(';');
    const url = `https://router.project-osrm.org/route/v1/driving/${coordStr}?overview=full&geometries=polyline`;
    if (url.length > 8000) return resolve(null);
    const req = https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          resolve(json.code === 'Ok' && json.routes?.length > 0 ? decodePolyline(json.routes[0].geometry) : null);
        } catch { resolve(null); }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(10000, () => { req.destroy(); resolve(null); });
  });
}

const delay = ms => new Promise(r => setTimeout(r, ms));

function distPointToLine(p, a, b) {
  const dx = b[1] - a[1], dy = b[0] - a[0], lenSq = dx * dx + dy * dy;
  if (lenSq === 0) return haversineMeters(p[0], p[1], a[0], a[1]);
  let t = Math.max(0, Math.min(1, ((p[1] - a[1]) * dx + (p[0] - a[0]) * dy) / lenSq));
  return haversineMeters(p[0], p[1], a[0] + dy * t, a[1] + dx * t);
}

async function matchTrailToRoads(trail) {
  const CHUNK_SIZE = 100, OVERLAP = 5;
  if (trail.length < 2) return null;
  if (trail.length <= CHUNK_SIZE) return await osrmRouteChunk(trail);

  const allCoords = [];
  for (let start = 0; start < trail.length; start += CHUNK_SIZE - OVERLAP) {
    const chunk = trail.slice(start, start + CHUNK_SIZE);
    if (chunk.length < 2) break;
    if (start > 0) await delay(200);
    const matched = await osrmRouteChunk(chunk);
    if (!matched) return null;
    if (allCoords.length === 0) { allCoords.push(...matched); }
    else {
      let skipIdx = 0;
      const lastPt = allCoords[allCoords.length - 1];
      for (let i = 0; i < Math.min(matched.length, 20); i++) {
        if (haversineMeters(lastPt[0], lastPt[1], matched[i][0], matched[i][1]) < 30) skipIdx = i + 1;
      }
      allCoords.push(...matched.slice(skipIdx));
    }
    if (start + CHUNK_SIZE >= trail.length) break;
  }

  if (allCoords.length > 3) {
    const simplified = [allCoords[0]];
    for (let i = 1; i < allCoords.length - 1; i++) {
      if (distPointToLine(allCoords[i], simplified[simplified.length - 1], allCoords[i + 1]) > 5) simplified.push(allCoords[i]);
    }
    simplified.push(allCoords[allCoords.length - 1]);
    return simplified;
  }
  return allCoords.length > 0 ? allCoords : null;
}

let routesCache = { data: null, ts: 0 };

async function refreshBusRoutes() {
  if (routesCache.data && Date.now() - routesCache.ts < 600000) return;
  try {
    const byLineVehicle = new Map();
    function collectPositions(resources) {
      if (!resources) return;
      resources.forEach(b => {
        const lineId = b['ayto:linea'], vid = b['ayto:vehiculo'];
        if (!lineId || !vid) return;
        const lat = parseFloat(b['wgs84_pos:lat']), lng = parseFloat(b['wgs84_pos:long']);
        if (isNaN(lat) || isNaN(lng) || !validCoords(lat, lng) || pointInBay(lat, lng)) return;
        const key = `${lineId}:${vid}`;
        if (!byLineVehicle.has(key)) byLineVehicle.set(key, { lineId, pts: [] });
        byLineVehicle.get(key).pts.push({ lat, lng, ts: b['ayto:instante'] || '' });
      });
    }

    const firstPage = await fetchDataset('control_flotas_posiciones', 500, 1);
    collectPositions(firstPage.resources);
    const totalPages = (firstPage.summary && firstPage.summary.pages) || 1;
    for (let batch = 2; batch <= totalPages; batch += 10) {
      const promises = [];
      for (let p = batch; p <= Math.min(batch + 9, totalPages); p++) {
        promises.push(fetchDataset('control_flotas_posiciones', 500, p));
      }
      (await Promise.all(promises)).forEach(json => collectPositions(json.resources));
    }

    const lineNames = await getBusLines();
    const trailsByLine = new Map();
    byLineVehicle.forEach(({ lineId, pts }) => {
      pts.sort((a, b) => a.ts.localeCompare(b.ts));
      const simplified = [pts[0]];
      for (let i = 1; i < pts.length; i++) {
        const last = simplified[simplified.length - 1];
        if (haversineMeters(last.lat, last.lng, pts[i].lat, pts[i].lng) >= 10) simplified.push(pts[i]);
      }
      if (simplified.length < 3) return;
      if (!trailsByLine.has(lineId)) trailsByLine.set(lineId, []);
      trailsByLine.get(lineId).push(simplified);
    });

    function distToSegment(p, a, b) {
      const dx = b.lng - a.lng, dy = b.lat - a.lat, lenSq = dx * dx + dy * dy;
      if (lenSq === 0) return haversineMeters(p.lat, p.lng, a.lat, a.lng);
      let t = Math.max(0, Math.min(1, ((p.lng - a.lng) * dx + (p.lat - a.lat) * dy) / lenSq));
      return haversineMeters(p.lat, p.lng, a.lat + dy * t, a.lng + dx * t);
    }

    const routes = [];
    let osrmCacheUpdated = false;

    for (const [lineId, trails] of trailsByLine) {
      trails.sort((a, b) => b.length - a.length);
      const primary = trails[0];
      for (let t = 1; t < trails.length; t++) {
        for (const p of trails[t]) {
          let bestSegIdx = -1, bestDist = Infinity;
          for (let i = 0; i < primary.length - 1; i++) {
            const d = distToSegment(p, primary[i], primary[i + 1]);
            if (d < bestDist) { bestDist = d; bestSegIdx = i; }
          }
          if (bestDist < 80 && bestSegIdx >= 0) {
            const dA = haversineMeters(p.lat, p.lng, primary[bestSegIdx].lat, primary[bestSegIdx].lng);
            const dB = haversineMeters(p.lat, p.lng, primary[bestSegIdx + 1].lat, primary[bestSegIdx + 1].lng);
            if (dA > 10 && dB > 10) primary.splice(bestSegIdx + 1, 0, p);
          }
        }
      }

      let segmentCoords;
      const cached = osrmCache[lineId];
      if (cached && Date.now() - cached.ts < OSRM_CACHE_TTL) {
        segmentCoords = cached.coords;
      } else {
        if (routes.length > 0) await delay(300);
        const matched = await matchTrailToRoads(primary);
        if (matched) {
          segmentCoords = matched;
          osrmCache[lineId] = { coords: matched, ts: Date.now() };
          osrmCacheUpdated = true;
        } else {
          segmentCoords = primary.map(p => [p.lat, p.lng]);
        }
      }

      const lineInfo = lineNames[lineId] || {};
      routes.push({ lineId, lineNum: lineInfo.num || lineId, routeName: lineInfo.name || '', segments: [segmentCoords] });
    }

    if (osrmCacheUpdated) saveOsrmCache();
    routes.sort((a, b) => a.lineNum.localeCompare(b.lineNum));
    routesCache = { data: { routes }, ts: Date.now() };
    console.log(`[cache] bus-routes: ${routes.length} lines`);
  } catch (e) {
    console.error('[cache] bus-routes failed:', e.message);
  }
}

app.get('/api/bus-routes', (req, res) => {
  if (!routesCache.data) return res.status(503).json({ error: 'Bus routes loading, try again shortly' });
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'public, max-age=120');
  res.json(routesCache.data);
});

// ============================================================
// CACHE STATUS ENDPOINT
// ============================================================

app.get('/api/cache-status', (req, res) => {
  const status = {};
  for (const key of Object.keys(DATASETS)) {
    const entry = cache[key];
    status[key] = entry ? { age: Math.round((Date.now() - entry.ts) / 1000), size: entry.data.length } : { status: 'loading' };
  }
  for (const key of Object.keys(EXTERNALS)) {
    const entry = cache[key];
    status[key] = entry ? { age: Math.round((Date.now() - entry.ts) / 1000), size: entry.data.length } : { status: 'loading' };
  }
  status.buses = busCache.data ? { age: Math.round((Date.now() - busCache.ts) / 1000), vehicles: busCache.data.resources.length } : { status: 'loading' };
  status['bus-routes'] = routesCache.data ? { age: Math.round((Date.now() - routesCache.ts) / 1000), routes: routesCache.data.routes.length } : { status: 'loading' };
  res.json(status);
});

// ============================================================
// STARTUP: pre-cache everything, then start refresh loops
// ============================================================

async function warmCache() {
  console.log('[cache] Pre-caching all datasets...');
  const start = Date.now();

  // Fetch contenedores first (largest dataset, 7 pages) to avoid API rate-limits
  const dc = DATASETS.contenedores;
  await cacheDataset('contenedores', dc.dataset, { singlePage: dc.singlePage, items: dc.items });

  // Fetch remaining datos.santander.es datasets in parallel (batches of 8)
  const datasetKeys = Object.keys(DATASETS).filter(k => k !== 'contenedores');
  for (let i = 0; i < datasetKeys.length; i += 8) {
    const batch = datasetKeys.slice(i, i + 8);
    await Promise.all(batch.map(key => {
      const d = DATASETS[key];
      return cacheDataset(key, d.dataset, { singlePage: d.singlePage, items: d.items });
    }));
  }

  // Fetch all external APIs in parallel
  await Promise.all(Object.entries(EXTERNALS).map(([key, cfg]) =>
    cacheExternal(key, cfg.url, cfg.contentType)
  ));

  // Fetch buses and routes
  await Promise.all([refreshBuses(), refreshBusRoutes()]);

  console.log(`[cache] All data pre-cached in ${((Date.now() - start) / 1000).toFixed(1)}s`);

  // Set up refresh intervals
  for (const [key, cfg] of Object.entries(DATASETS)) {
    setInterval(() => cacheDataset(key, cfg.dataset, { singlePage: cfg.singlePage, items: cfg.items }), cfg.refresh);
  }
  for (const [key, cfg] of Object.entries(EXTERNALS)) {
    setInterval(() => cacheExternal(key, cfg.url, cfg.contentType), cfg.refresh);
  }
  setInterval(refreshBuses, 30000);       // Buses every 30s
  setInterval(refreshBusRoutes, 600000);  // Routes every 10min
}

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Santander Pulso running on port ${PORT}`);
  warmCache();
});
