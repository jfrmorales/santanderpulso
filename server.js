const express = require('express');
const http = require('http');
const https = require('https');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3000;

app.use(express.static(path.join(__dirname, 'public')));

// Generic proxy helpers
function proxyDataset(dataset, req, res) {
  const page = req.query.page || 1;
  const items = req.query.items || 200;
  const url = `http://datos.santander.es/api/rest/datasets/${dataset}.json?items=${items}&page=${page}`;

  http.get(url, (apiRes) => {
    let data = '';
    apiRes.on('data', chunk => data += chunk);
    apiRes.on('end', () => {
      res.setHeader('Content-Type', 'application/json');
      res.setHeader('Cache-Control', 'public, max-age=60');
      res.send(data);
    });
  }).on('error', () => {
    res.status(502).json({ error: `Error connecting to Santander API (${dataset})` });
  });
}

function proxyHttps(url, cacheSeconds, req, res, contentType = 'application/json') {
  https.get(url, (apiRes) => {
    let data = '';
    apiRes.on('data', chunk => data += chunk);
    apiRes.on('end', () => {
      res.setHeader('Content-Type', contentType);
      res.setHeader('Cache-Control', `public, max-age=${cacheSeconds}`);
      res.send(data);
    });
  }).on('error', () => {
    res.status(502).json({ error: 'Error fetching external data' });
  });
}

function proxyHttp(url, cacheSeconds, req, res, contentType = 'application/xml') {
  http.get(url, (apiRes) => {
    let data = '';
    apiRes.on('data', chunk => data += chunk);
    apiRes.on('end', () => {
      res.setHeader('Content-Type', contentType);
      res.setHeader('Cache-Control', `public, max-age=${cacheSeconds}`);
      res.send(data);
    });
  }).on('error', () => {
    res.status(502).json({ error: 'Error fetching external data' });
  });
}

// === WASTE ===
app.get('/api/contenedores', (req, res) => proxyDataset('residuos_contenedores', req, res));
app.get('/api/papeleras', (req, res) => proxyDataset('residuos_papeleras', req, res));
app.get('/api/vehiculos', (req, res) => proxyDataset('residuos_vehiculos', req, res));

// === TRANSPORT ===
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

// Bus lines cache (refreshes every 24h)
let linesCache = { data: null, ts: 0 };
async function getBusLines() {
  if (linesCache.data && Date.now() - linesCache.ts < 86400000) return linesCache.data;
  const json = await fetchDataset('lineas_bus', 500, 1);
  const map = {};
  if (json.resources) {
    json.resources.forEach(l => {
      const id = l['dc:identifier'];
      const num = l['ayto:numero'] || '';
      const name = l['dc:name'] || '';
      if (id) map[id] = { num, name };
    });
  }
  linesCache = { data: map, ts: Date.now() };
  return map;
}

// Santander bounding box for coordinate validation
const SANT_BOUNDS = { latMin: 43.4, latMax: 43.52, lngMin: -3.92, lngMax: -3.74 };
function validCoords(lat, lng) {
  return lat >= SANT_BOUNDS.latMin && lat <= SANT_BOUNDS.latMax &&
         lng >= SANT_BOUNDS.lngMin && lng <= SANT_BOUNDS.lngMax;
}

let busCache = { data: null, ts: 0 };
app.get('/api/buses', async (req, res) => {
  if (busCache.data && Date.now() - busCache.ts < 30000) {
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'public, max-age=15');
    return res.json(busCache.data);
  }
  const startTime = Date.now();
  // Keep 2 most recent positions per vehicle (latest + previous for heading)
  const latest = new Map();   // vid -> { current, previous }

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

  // Phase 1: fetch page 1 to get total page count
  const firstPage = await fetchDataset('control_flotas_posiciones', 500, 1);
  processResources(firstPage.resources);
  const totalPages = (firstPage.summary && firstPage.summary.pages) || 1;

  // Phase 2: fetch remaining pages in parallel (batches of 10)
  for (let batch = 2; batch <= totalPages; batch += 10) {
    const promises = [];
    for (let p = batch; p <= Math.min(batch + 9, totalPages); p++) {
      promises.push(fetchDataset('control_flotas_posiciones', 500, p));
    }
    const results = await Promise.all(promises);
    results.forEach(json => processResources(json.resources));
  }

  // Filter: only vehicles active within 5 minutes of most recent record
  let maxInstante = '';
  latest.forEach(({ current }) => { if (current['ayto:instante'] > maxInstante) maxInstante = current['ayto:instante']; });
  const cutoff = new Date(new Date(maxInstante).getTime() - 5 * 60000).toISOString();

  // Build response with previous position for heading calculation
  const lineNames = await getBusLines();
  const activeLines = new Set();
  const active = [];
  latest.forEach(({ current, previous }) => {
    if (current['ayto:instante'] < cutoff) return;
    const enriched = { ...current };
    if (previous) {
      enriched._prevLat = previous['wgs84_pos:lat'];
      enriched._prevLng = previous['wgs84_pos:long'];
    }
    const line = current['ayto:linea'] || '';
    const lineInfo = lineNames[line] || {};
    enriched._lineNum = lineInfo.num || line;
    enriched._routeName = lineInfo.name || '';
    activeLines.add(lineInfo.num || line);
    active.push(enriched);
  });

  const result = {
    resources: active,
    meta: {
      activeCount: active.length,
      totalVehicles: latest.size,
      lines: [...activeLines].sort(),
      dataTimestamp: maxInstante,
      fetchDuration: Date.now() - startTime,
      totalPages
    }
  };
  busCache = { data: result, ts: Date.now() };
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'public, max-age=15');
  res.json(result);
});
// Bus routes reconstructed from GPS trails
function haversineMeters(lat1, lng1, lat2, lng2) {
  const R = 6371000, toRad = d => d * Math.PI / 180;
  const dLat = toRad(lat2 - lat1), dLng = toRad(lng2 - lng1);
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// Santander bay polygon (water area to exclude GPS points)
const BAY_POLYGON = [
  [43.4730, -3.7920], // Punta Magdalena
  [43.4650, -3.7700], // Este bahía
  [43.4500, -3.7650], // El Puntal
  [43.4380, -3.7800], // Sur (Pedreña)
  [43.4380, -3.8050], // Suroeste
  [43.4500, -3.8200], // Puerto zona sur
  [43.4580, -3.8150], // Puerto
  [43.4650, -3.8050], // Centro-oeste
  [43.4730, -3.7920], // Cierre
];
function pointInBay(lat, lng) {
  let inside = false;
  const poly = BAY_POLYGON;
  for (let i = 0, j = poly.length - 1; i < poly.length; j = i++) {
    const [yi, xi] = poly[i], [yj, xj] = poly[j];
    if (((yi > lat) !== (yj > lat)) && (lng < (xj - xi) * (lat - yi) / (yj - yi) + xi)) inside = !inside;
  }
  return inside;
}

// === OSRM Map Matching ===
// Decode Google encoded polyline to [[lat, lng], ...]
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

// OSRM persistent cache (disk-backed, 24h TTL)
const OSRM_CACHE_FILE = path.join(__dirname, 'data', 'osrm-cache.json');
const OSRM_CACHE_TTL = 24 * 60 * 60 * 1000; // 24 hours
let osrmCache = {};
try {
  if (fs.existsSync(OSRM_CACHE_FILE)) {
    osrmCache = JSON.parse(fs.readFileSync(OSRM_CACHE_FILE, 'utf8'));
  }
} catch (e) {
  console.warn('OSRM cache load failed, starting fresh:', e.message);
  osrmCache = {};
}

function saveOsrmCache() {
  try {
    fs.writeFileSync(OSRM_CACHE_FILE, JSON.stringify(osrmCache), 'utf8');
  } catch (e) {
    console.warn('OSRM cache save failed:', e.message);
  }
}

// Fetch OSRM route for a chunk of waypoints (snaps to roads)
function osrmRouteChunk(coords) {
  return new Promise((resolve, reject) => {
    // OSRM expects lng,lat order; round to 6 decimals to keep URL short
    const coordStr = coords.map(p => `${Number(p.lng).toFixed(6)},${Number(p.lat).toFixed(6)}`).join(';');
    const url = `https://router.project-osrm.org/route/v1/driving/${coordStr}?overview=full&geometries=polyline`;
    if (url.length > 8000) {
      console.warn(`OSRM URL too long (${url.length} chars, ${coords.length} pts), skipping chunk`);
      return resolve(null);
    }
    const req = https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(data);
          if (json.code === 'Ok' && json.routes && json.routes.length > 0) {
            resolve(decodePolyline(json.routes[0].geometry));
          } else {
            console.warn(`OSRM chunk (${coords.length} pts): ${json.code} ${json.message || ''}`);
            resolve(null);
          }
        } catch (e) {
          resolve(null);
        }
      });
    });
    req.on('error', () => resolve(null));
    req.setTimeout(10000, () => { req.destroy(); resolve(null); });
  });
}

const delay = ms => new Promise(r => setTimeout(r, ms));

// Distance from point P [lat,lng] to line segment [A,B] in meters
function distPointToLine(p, a, b) {
  const dx = b[1] - a[1], dy = b[0] - a[0];
  const lenSq = dx * dx + dy * dy;
  if (lenSq === 0) return haversineMeters(p[0], p[1], a[0], a[1]);
  let t = ((p[1] - a[1]) * dx + (p[0] - a[0]) * dy) / lenSq;
  t = Math.max(0, Math.min(1, t));
  return haversineMeters(p[0], p[1], a[0] + dy * t, a[1] + dx * t);
}

// Match a full trail to roads, chunking if needed
async function matchTrailToRoads(trail) {
  const CHUNK_SIZE = 100;
  const OVERLAP = 5;

  if (trail.length < 2) return null;

  // If small enough, single request
  if (trail.length <= CHUNK_SIZE) {
    return await osrmRouteChunk(trail);
  }

  // Chunk with overlap
  const allCoords = [];
  for (let start = 0; start < trail.length; start += CHUNK_SIZE - OVERLAP) {
    const chunk = trail.slice(start, start + CHUNK_SIZE);
    if (chunk.length < 2) break;

    if (start > 0) await delay(200);
    const matched = await osrmRouteChunk(chunk);
    if (!matched) return null; // Fail entire line on any chunk failure

    if (allCoords.length === 0) {
      allCoords.push(...matched);
    } else {
      // Skip overlapping points at the start of this chunk's result
      let skipIdx = 0;
      const lastPt = allCoords[allCoords.length - 1];
      for (let i = 0; i < Math.min(matched.length, 20); i++) {
        if (haversineMeters(lastPt[0], lastPt[1], matched[i][0], matched[i][1]) < 30) {
          skipIdx = i + 1;
        }
      }
      allCoords.push(...matched.slice(skipIdx));
    }

    if (start + CHUNK_SIZE >= trail.length) break;
  }

  // Simplify: remove points within 5m of the line between their neighbors
  if (allCoords.length > 3) {
    const simplified = [allCoords[0]];
    for (let i = 1; i < allCoords.length - 1; i++) {
      const prev = simplified[simplified.length - 1];
      const next = allCoords[i + 1];
      const d = distPointToLine(allCoords[i], prev, next);
      if (d > 5) simplified.push(allCoords[i]);
    }
    simplified.push(allCoords[allCoords.length - 1]);
    return simplified;
  }
  return allCoords.length > 0 ? allCoords : null;
}

let routesCache = { data: null, ts: 0 };
app.get('/api/bus-routes', async (req, res) => {
  if (routesCache.data && Date.now() - routesCache.ts < 600000) {
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Cache-Control', 'public, max-age=120');
    return res.json(routesCache.data);
  }
  // Group positions by lineId + vehicleId
  const byLineVehicle = new Map(); // "lineId:vid" -> { lineId, pts: [] }

  function collectPositions(resources) {
    if (!resources) return;
    resources.forEach(b => {
      const lineId = b['ayto:linea'], vid = b['ayto:vehiculo'];
      if (!lineId || !vid) return;
      const lat = parseFloat(b['wgs84_pos:lat']), lng = parseFloat(b['wgs84_pos:long']);
      if (isNaN(lat) || isNaN(lng) || !validCoords(lat, lng)) return;
      if (pointInBay(lat, lng)) return;
      const key = `${lineId}:${vid}`;
      if (!byLineVehicle.has(key)) byLineVehicle.set(key, { lineId, pts: [] });
      byLineVehicle.get(key).pts.push({ lat, lng, ts: b['ayto:instante'] || '' });
    });
  }

  // Fetch all pages in parallel
  const firstPage = await fetchDataset('control_flotas_posiciones', 500, 1);
  collectPositions(firstPage.resources);
  const totalPages = (firstPage.summary && firstPage.summary.pages) || 1;

  for (let batch = 2; batch <= totalPages; batch += 10) {
    const promises = [];
    for (let p = batch; p <= Math.min(batch + 9, totalPages); p++) {
      promises.push(fetchDataset('control_flotas_posiciones', 500, p));
    }
    const results = await Promise.all(promises);
    results.forEach(json => collectPositions(json.resources));
  }

  // Build simplified trails per vehicle, grouped by line
  const lineNames = await getBusLines();
  const trailsByLine = new Map(); // lineId -> [trail, trail, ...]

  byLineVehicle.forEach(({ lineId, pts }) => {
    pts.sort((a, b) => a.ts.localeCompare(b.ts));
    // Sequential dedup: ≥10m from previous point
    const simplified = [pts[0]];
    for (let i = 1; i < pts.length; i++) {
      const last = simplified[simplified.length - 1];
      if (haversineMeters(last.lat, last.lng, pts[i].lat, pts[i].lng) >= 10) {
        simplified.push(pts[i]);
      }
    }
    if (simplified.length < 3) return;
    if (!trailsByLine.has(lineId)) trailsByLine.set(lineId, []);
    trailsByLine.get(lineId).push(simplified);
  });

  // Distance from point P to segment [A, B]
  function distToSegment(p, a, b) {
    const dx = b.lng - a.lng, dy = b.lat - a.lat;
    const lenSq = dx * dx + dy * dy;
    if (lenSq === 0) return haversineMeters(p.lat, p.lng, a.lat, a.lng);
    let t = ((p.lng - a.lng) * dx + (p.lat - a.lat) * dy) / lenSq;
    t = Math.max(0, Math.min(1, t));
    return haversineMeters(p.lat, p.lng, a.lat + dy * t, a.lng + dx * t);
  }

  const routes = [];
  let osrmCacheUpdated = false;

  for (const [lineId, trails] of trailsByLine) {
    // Sort trails by length, longest first (best coverage)
    trails.sort((a, b) => b.length - a.length);
    // Start with the longest trail as the primary
    const primary = trails[0];

    // Enrich with points from other vehicles
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
          if (dA > 10 && dB > 10) {
            primary.splice(bestSegIdx + 1, 0, p);
          }
        }
      }
    }

    // Try OSRM map matching: check disk cache first, then call API
    let segmentCoords;
    const cached = osrmCache[lineId];
    if (cached && Date.now() - cached.ts < OSRM_CACHE_TTL) {
      segmentCoords = cached.coords;
    } else {
      if (routes.length > 0) await delay(300); // Rate limit between lines
      const matched = await matchTrailToRoads(primary);
      if (matched) {
        segmentCoords = matched;
        osrmCache[lineId] = { coords: matched, ts: Date.now() };
        osrmCacheUpdated = true;
        console.log(`OSRM matched line ${lineId}: ${primary.length} GPS pts → ${matched.length} road pts`);
      } else {
        segmentCoords = primary.map(p => [p.lat, p.lng]);
        console.warn(`OSRM match failed for line ${lineId}, using raw GPS (${primary.length} pts)`);
      }
    }

    const lineInfo = lineNames[lineId] || {};
    routes.push({
      lineId,
      lineNum: lineInfo.num || lineId,
      routeName: lineInfo.name || '',
      segments: [segmentCoords]
    });
  }

  if (osrmCacheUpdated) saveOsrmCache();

  routes.sort((a, b) => a.lineNum.localeCompare(b.lineNum));
  const result = { routes };
  routesCache = { data: result, ts: Date.now() };
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Cache-Control', 'public, max-age=120');
  res.json(result);
});

app.get('/api/buses-eta', (req, res) => proxyDataset('control_flotas_estimaciones', req, res));
app.get('/api/paradas', (req, res) => proxyDataset('paradas_bus', req, res));
app.get('/api/lineas', (req, res) => proxyDataset('lineas_bus', req, res));

// === BICYCLES ===
app.get('/api/tusbic', (req, res) => {
  proxyHttps('https://api.nextbike.net/maps/nextbike-live.json?city=914', 30, req, res);
});

// === PARKING & MOBILITY ===
app.get('/api/zonas-ola', (req, res) => proxyDataset('zonas_ola', req, res));
app.get('/api/plazas-motos', (req, res) => proxyDataset('plazas_motos', req, res));
app.get('/api/plazas-pmr', (req, res) => proxyDataset('plazas_pmr', req, res));
app.get('/api/zonas-carga', (req, res) => proxyDataset('zonas_carga', req, res));
app.get('/api/carril-bici', (req, res) => proxyDataset('carril_bici', req, res));
app.get('/api/zonas-30', (req, res) => proxyDataset('zonas_30', req, res));

// === DISTRICTS & POPULATION ===
app.get('/api/distritos', (req, res) => proxyDataset('distritos', req, res));
app.get('/api/secciones', (req, res) => proxyDataset('secciones', req, res));

// === ENVIRONMENT ===
app.get('/api/sensores', (req, res) => proxyDataset('sensores_smart_env_monitoring', req, res));
app.get('/api/agua-calidad', (req, res) => proxyDataset('agua_calidad', req, res));
app.get('/api/saneamiento', (req, res) => proxyDataset('agua_estado_red_saneamiento', req, res));

// === COMMERCE & MARKETS ===
app.get('/api/comercios', (req, res) => proxyDataset('comercios_comercios', req, res));
app.get('/api/mercados', (req, res) => proxyDataset('mercados_mercados', req, res));
app.get('/api/hosteleria', (req, res) => proxyDataset('establecimientos_hosteleros', req, res));

// === PARKS ===
app.get('/api/parques', (req, res) => proxyDataset('parques', req, res));
app.get('/api/jardines', (req, res) => proxyDataset('jardines', req, res));

// === TOURISM ===
app.get('/api/playas', (req, res) => proxyDataset('puntos_interes_playa', req, res));
app.get('/api/museos', (req, res) => proxyDataset('puntos_interes_museos', req, res));
app.get('/api/monumentos', (req, res) => proxyDataset('puntos_interes_monumento', req, res));
app.get('/api/edificios-interes', (req, res) => proxyDataset('puntos_interes_edificio_interes', req, res));
app.get('/api/deporte', (req, res) => proxyDataset('puntos_interes_deporte', req, res));
app.get('/api/bibliotecas', (req, res) => proxyDataset('puntos_interes_bibliotecas', req, res));

// === WEATHER ===
app.get('/api/aemet', (req, res) => {
  proxyHttps('https://www.aemet.es/xml/municipios/localidad_39075.xml', 1800, req, res, 'application/xml');
});

// === POLLEN ===
app.get('/api/polen', (req, res) => {
  proxyHttps(
    'https://air-quality-api.open-meteo.com/v1/air-quality?latitude=43.46&longitude=-3.81&current=alder_pollen,birch_pollen,grass_pollen,mugwort_pollen,olive_pollen,ragweed_pollen&timezone=Europe/Madrid',
    3600, req, res
  );
});

// === SEISMIC ===
app.get('/api/sismos', (req, res) => {
  proxyHttp('http://www.ign.es/ign/RssTools/sismologia.xml', 300, req, res);
});

// === NEWS ===
app.get('/api/noticias', (req, res) => {
  proxyHttps('https://santander.es/rss/noticia/rss.xml', 1800, req, res, 'application/xml');
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Santander Pulso running on port ${PORT}`);
});
