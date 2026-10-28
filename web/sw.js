// Service Worker — 오프라인 지원
// 전략:
//   - app shell (HTML/CSS/JS/manifest/icon, Leaflet CDN): cache-first
//   - 데이터 (heatmap.json, boundaries.geojson, detail/*.json): network-first
//     → 최신 데이터 우선, 오프라인 시 캐시 폴백
//   - OSM 타일: 네트워크 우선, 캐시 안 함 (저장소 폭발 방지)

const VERSION = 'v30-2026-10-29-progress';
const SHELL_CACHE = `shell-${VERSION}`;
const DATA_CACHE = `data-${VERSION}`;

const SHELL_ASSETS = [
  './',
  './index.html',
  './visit-evidence.html',
  './visit-evidence.js?v=3',
  './field-followup.js?v=1',
  './progress.html',
  './manifest.json',
  './icon.svg',
  './research.css?v=17',
  './research.js?v=17',
  './explorer.js?v=2',
  './revenue-map.js?v=9',
  './data/research.json',
  './detail/index.html',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(SHELL_CACHE)
      .then(c => c.addAll(SHELL_ASSETS).catch(() => {}))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys => Promise.all(
      keys.filter(k => k !== SHELL_CACHE && k !== DATA_CACHE).map(k => caches.delete(k))
    )).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const req = e.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // OSM 타일 — 네트워크만, 캐시 X
  if (url.host.endsWith('tile.openstreetmap.org')) return;

  // 데이터 파일 — network-first
  const isData =
    url.pathname.includes('/web/v3/') ||
    url.pathname.endsWith('/research.json') ||
    url.pathname.endsWith('/revenue.json') ||
    url.pathname.endsWith('/visit-evidence.json') ||
    url.pathname.endsWith('/field-followup.json') ||
    url.pathname.endsWith('/gyeonggi-revenue-scope.json') ||
    url.pathname.endsWith('/research_providers.json') ||
    url.pathname.endsWith('/heatmap.json') ||
    url.pathname.endsWith('/boundaries.geojson') ||
    url.pathname.endsWith('/narrow_lists.json') ||
    url.pathname.endsWith('/all_clinics.json') ||
    url.pathname.includes('/data/detail/');

  if (isData) {
    e.respondWith(
      fetch(req).then(resp => {
        if (resp.ok) {
          const copy = resp.clone();
          caches.open(DATA_CACHE).then(c => c.put(req, copy));
        }
        return resp;
      }).catch(() => caches.match(req))
    );
    return;
  }

  // 그 외 (app shell + Leaflet CDN) — cache-first
  e.respondWith(
    caches.match(req).then(cached =>
      cached || fetch(req).then(resp => {
        if (resp.ok && (url.origin === self.location.origin || url.host === 'unpkg.com')) {
          const copy = resp.clone();
          caches.open(SHELL_CACHE).then(c => c.put(req, copy));
        }
        return resp;
      })
    )
  );
});
