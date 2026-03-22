const express = require('express');
const http = require('http');
const https = require('https');
const path = require('path');

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
app.get('/api/buses', (req, res) => proxyDataset('control_flotas_posiciones', req, res));
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
