'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const num = value => value == null ? '미확인' : Number(value).toLocaleString('ko-KR',{maximumFractionDigits:0});
  const pct = value => value == null ? '미확인' : Math.round(value*100)+'%';
  let data, map, layer, selected, markers = new Map();
  const isPending = r => Boolean(r.research_pending_reason);
  const status = r => isPending(r) ? '확인 대기' : r.research_order != null ? `조사 순서 ${r.research_order}` : '비교 가능 · 현장 미확인';
  function tab(name) {
    document.querySelectorAll('.panel').forEach(p => p.hidden = p.id !== name);
    document.querySelectorAll('[data-tab]').forEach(b => {b.classList.toggle('active',b.dataset.tab===name); b.setAttribute('aria-pressed',String(b.dataset.tab===name));});
    if (name==='explore' && map) setTimeout(()=>map.invalidateSize(),0);
  }
  document.querySelectorAll('[data-tab]').forEach(b=>b.addEventListener('click',()=>tab(b.dataset.tab)));
  const link = s => `<a target="_blank" rel="noopener noreferrer" href="${esc(s.url)}">${esc(s.title)}</a>`;
  function sourceLinks(ids) {return ids.map(id=>data.evidence.sources.find(s=>s.id===id)).filter(Boolean).map(link).join(' · ');}
  function badge(r) {return `<span class="badge ${isPending(r)?'pending':''}">${esc(status(r))}</span>`;}
  function table(headers,rows) {return `<div class="scroll"><table><thead><tr>${headers.map(h=>`<th>${esc(h)}</th>`).join('')}</tr></thead><tbody>${rows.map(row=>'<tr>'+row.map(v=>`<td>${v}</td>`).join('')+'</tr>').join('')}</tbody></table></div>`;}
  function renderDetail(r, pan=true) {
    selected=r.adm_cd;
    document.querySelectorAll('.candidate').forEach(b=>{const active=b.dataset.id===selected;b.classList.toggle('active',active);b.setAttribute('aria-pressed',String(active));});
    const review=r.review;
    const nearby=data.providers[r.adm_cd] || [];
    const matches=data.provider_matches.filter(p=>nearby.some(n=>n.provider_id===p.provider_id));
    const range=(age,radius)=>`${num(r[`uniform_pop_${age}_${radius}m`])} / ${num(r[`reweighted_pop_${age}_${radius}m`])}`;
    $('detail').innerHTML=`<div class="detail-head"><div>${badge(r)}<h2>${esc(r.adm_nm)}</h2><p class="muted">${r.population_name && r.population_name!==r.adm_nm?'현재 인구 연결 명칭: '+esc(r.population_name)+' · ':''}기존 점수 순위 ${r.rank == null?'없음':num(r.rank)+'위'} · 실제 매물 아님</p></div><a target="_blank" rel="noopener noreferrer" href="https://www.google.com/maps/search/?api=1&query=${Number(r.lat_shops)},${Number(r.lon_shops)}">중심점 지도 ↗</a></div>
      ${isPending(r)?`<p class="callout"><b>확인 대기:</b> ${esc(r.research_pending_reason)}. 부적합 판정이 아닙니다.</p>`:''}
      <h3>수요와 경쟁</h3>${table(['지표','현재 조사 값'],[
        ['1km 20세 이상 · 균등 / 과거분포',range('20plus',1000)+'명'],['1.5km 40세 이상 · 균등 / 과거분포',range('40plus',1500)+'명'],
        ['내과 전문의 의원 · 500m / 1km',`${num(r.internal_specialist_clinics_500m)} / ${num(r.internal_specialist_clinics_1000m)}곳`],
        ['내과 또는 가정의학과 전문의 의원 · 1km',num(r.primary_specialist_clinics_1000m)+'곳'],
        ['위내시경 장비 양성 · 의원 / 병원 1.5km',`${num(r.egd_clinics_1500m)} / ${num(r.egd_hospitals_1500m)}곳`],
        ['상위 50 포함률 · 균등 / 과거분포',pct(r.uniform_top50_fraction)+' / '+pct(r.reweighted_top50_fraction)]])}
      <p class="source-note muted">인구는 두 배분 가정이며 실측·신뢰구간이 아닙니다. 포함률은 각 배분 방식의 72가지 가정에서 상위 50에 든 비율이며 성공확률이 아닙니다. HIRA 기관 2026-09 / 장비 2025-12 표기.</p>
      <h3>통근 · 기존 중심점 기준</h3>${table(['수단','출근','퇴근'],[['자차',r.t_car_am==null?'미확인':num(r.t_car_am)+'분',r.t_car_pm==null?'미확인':num(r.t_car_pm)+'분'],['대중교통',r.t_transit_am==null?'미확인':num(r.t_transit_am)+'분',r.t_transit_pm==null?'미확인':num(r.t_transit_pm)+'분']])}<p class="muted">실제 건물 선택 후 동일 요일·시간대 재확인. 미확인을 불가로 처리하지 않습니다.</p>
      ${review?`<h3>사전 검증 · ${esc(review.group)}</h3><p class="callout">${esc(review.challenge)}</p><h3>주거 변화</h3><p>${esc(review.housing)}</p><p>${sourceLinks(review.housing_sources)}</p><h3>임장에서 확인할 동선</h3><p>${esc(review.walk)}</p>`:'<p class="callout">이 중심점의 기관별 공개 서비스·주거 변화 사전 검증은 아직 작성되지 않았습니다.</p>'}
      <h3>주소를 대조한 서비스 안내</h3>${matches.length?table(['기관 / 거리','근거 수준','HIRA 내과 인원 / 웹 안내','확인 내용'],matches.map(p=>{const n=nearby.find(n=>n.provider_id===p.provider_id);return [esc(p.official_name)+`<small> 약 ${n.distance_m}m</small>`,esc(p.service_state),`${num(p.internal_specialists_hira)} / ${p.staff_web==null?'미수집':num(p.staff_web)+'명 안내'}`,esc(p.hours)+'<br>'+esc(p.interpretation)+'<br>'+sourceLinks(p.sources)];})):'<p class="muted">주소까지 연결한 공개 서비스 근거가 없습니다. 서비스 없음이나 경쟁 부재라는 뜻은 아닙니다.</p>'}
      <p class="muted">웹 의료진은 해당 페이지의 안내이며 실제 상주·전체 재직 인원을 확정하지 않습니다. 인원 차이는 덮어쓰지 않고 재확인합니다.</p>
      <details><summary>주변 공식 원문 기관 ${nearby.length}곳 ${nearby.length?'· 2km 직선거리순':''}</summary>${nearby.length?table(['기관','거리','내과 전문의','위 장비 근거','주소'],nearby.map(p=>[esc(p.name),num(p.distance_m)+'m',num(p.internal_specialists),p.egd_equipment_2025_positive?'양성 기록':'미확인',esc(p.address)])):'<p>이 중심점은 상세 기관 목록을 아직 내보내지 않았습니다. 기관 수 0이라는 뜻은 아닙니다.</p>'}</details>`;
    if(map && pan) {map.setView([r.lat_shops,r.lon_shops],14);markers.get(r.adm_cd)?.openPopup();}
  }
  function filtered() {
    const q=$('search').value.trim(), scope=$('scope').value, region=$('region').value;
    return data.anchors.filter(r=>(!q || (r.adm_nm+' '+(r.population_name||'')).includes(q)) && (!region||r.region===region) &&
      (scope==='all'||scope==='research'&&r.research_order!=null||scope==='review'&&r.review||scope==='pending'&&isPending(r)))
      .sort((a,b)=>(a.research_order??999)-(b.research_order??999)||a.adm_nm.localeCompare(b.adm_nm,'ko'));
  }
  function draw() {
    const rows=filtered();
    $('count').textContent=rows.length+'개 표시';
    $('list').innerHTML=rows.length?rows.map(r=>`<button class="candidate" data-id="${esc(r.adm_cd)}" aria-pressed="false">${badge(r)}<span class="name">${esc(r.adm_nm)}</span><small>${isPending(r)?esc(r.research_pending_reason):'상위50 포함률 '+pct(r.uniform_top50_fraction)+' / '+pct(r.reweighted_top50_fraction)}</small>${r.review?`<small>${esc(r.review.group)}</small>`:''}</button>`).join(''):'<p class="empty">조건에 맞는 중심점이 없습니다. 검색어나 지역을 변경해 주세요.</p>';
    $('list').querySelectorAll('.candidate').forEach(b=>b.addEventListener('click',()=>renderDetail(rows.find(r=>r.adm_cd===b.dataset.id))));
    if(map){layer.clearLayers();markers=new Map(); rows.forEach(r=>{const color=isPending(r)?'#ad7a28':r.research_order!=null?'#175c4b':'#7a8981'; const m=L.circleMarker([r.lat_shops,r.lon_shops],{radius:r.research_order!=null?7:5,color,fillColor:color,fillOpacity:.8,weight:1}).addTo(layer).bindPopup(`<b>${esc(r.adm_nm)}</b><br>${esc(status(r))}`);m.on('click',()=>renderDetail(r,false));markers.set(r.adm_cd,m);});if(rows.length)map.fitBounds(rows.map(r=>[r.lat_shops,r.lon_shops]),{padding:[25,25],maxZoom:13});}
    if(rows.length)renderDetail(rows.find(r=>r.adm_cd===selected)||rows[0],false);else{$('detail').innerHTML='<p class="empty">현재 필터에 표시되는 후보가 없습니다.</p>';selected=null;}
  }
  function jump(id){tab('explore');$('scope').value='all';$('search').value='';$('region').value='';selected=id;draw();renderDetail(data.anchors.find(r=>r.adm_cd===id));$('detail').scrollIntoView({behavior:'smooth',block:'start'});}
  function renderPanels(){
    const reviewed=data.anchors.filter(r=>r.review).sort((a,b)=>(a.research_order??999)-(b.research_order??999));
    $('review').innerHTML=`<div class="card"><p class="eyebrow">FIELDWORK PREPARATION</p><h2>매물은 임장에서, 지금은 생활권 검증</h2><p>상도1·중계2·3·개봉2·남가좌2·쌍문4·부천 상1은 잠정 답사 가설입니다. 중동·심곡2는 비교 예비로 유지합니다.</p><p class="callout">6곳을 확정하지 않았습니다. 최신 소지역 인구, 실제 경쟁 진료, 인천 경계 보완을 먼저 진행한 뒤 임장할 3~5개 생활권과 방문 동선을 정합니다. 온라인 매물 유무로 후보를 탈락시키지 않습니다.</p><ol><li>최신 소지역 인구와 재개발 이주·입주 확인</li><li>주요 경쟁기관의 실제 진료 구성과 의료진 역할 확인</li><li>인천 경계 보완 후 비교에서 빠진 후보 재검토</li><li>임장 생활권·방문 순서 확정 후 현장에서 중개업소와 건물 확인</li></ol></div><div class="grid2">${reviewed.map(r=>`<article><p class="badge">${esc(r.review.group)}</p><h2>${esc(r.adm_nm)}</h2><p>${esc(r.review.challenge)}</p><p class="muted">주거 변화: ${esc(r.review.housing)}</p><p><b>현장 확인:</b> ${esc(r.review.walk)}</p><button data-jump="${esc(r.adm_cd)}">수요·경쟁 근거 보기</button></article>`).join('')}</div>`;
    $('review').querySelectorAll('[data-jump]').forEach(b=>b.addEventListener('click',()=>jump(b.dataset.jump)));
    const coverage=['서울','부천','인천','경기 기타'].map(region=>{const rows=data.anchors.filter(r=>r.region===region);return [region,num(rows.length),num(rows.filter(r=>!isPending(r)).length),num(rows.filter(r=>r.research_order!=null).length)];});
    $('audit').innerHTML=`<div class="card"><h2>인구 결측보다, 배분 방식과 경쟁 정의가 문제였습니다</h2><ul><li>기존 745곳의 동별 총인구·50~69세 인구가 2026년 9월 공식 자료와 모두 일치했습니다.</li><li>총인구·50~69세·연령비율·최종 P 값 결측은 없었습니다. 3년 추세 4건은 중앙값 대체 대상이며 기존 Top30에 없었습니다.</li><li>기존 경쟁 밀도는 동 단위 의사 수와 반경 인구의 공간 범위가 달랐습니다. 이번 조사는 같은 반경의 인구와 기관을 비교합니다.</li><li>기관명 대신 전문의·장비 양성 기록을 연결했습니다. 수도권 내과 전문의 의원 3,352곳 중 이름에 ‘내과’가 없는 곳은 363곳입니다.</li></ul></div>
    <div class="card"><h2>인구 계산만 바꿔 본 기존 Top30</h2><p>기존 경쟁·추세 점수는 고정하고 인구 규모 항만 교체한 진단입니다. 경쟁 오류까지 전부 수정한 새 추천 순위가 아닙니다.</p>${table(['진단','비교 표본','부천 포함','기존 Top30 유지'],data.diagnostics.map(r=>[esc(r.scenario),num(r.comparable_n),r.bucheon+' / 30',r.retained+' / 30']))}</div>
    <div class="card"><h2>서울 집중도 그대로 정답이 될 수 없습니다</h2>${table(['지역','전체','비교 가능','조사 후보 15곳 중'],coverage)}<p class="callout">인천은 147곳 중 40곳만 비교 가능했습니다. 107곳은 확인 대기이고, 그중 59곳은 행정개편 경계 확인만 남았습니다. 부천 37곳은 모두 비교했으며 별도 감점·지역 할당은 없습니다.</p><p>부천의 비교 후보 상1·중동·심곡2를 보존합니다. 자료가 부족한 지역을 낮은 점수로 바꾸지 않았습니다.</p></div>
    <div class="card"><h2>조사 순서를 만든 144가지 가정</h2><p>인구 배분 2가지 × 외래 반경 2가지 × 검진 반경 3가지 × 외래 경쟁 범위 2가지 × 병원 영향 3가지 × 검진 연령 범위 2가지.</p><p>일반 외래 수요, 검진 수요, 각 수요 대비 확인된 기관을 비교합니다. 네 지표 중 가장 낮은 백분위를 사용하고, 두 인구 배분 가정에서 유지되는 정도로 조사 순서를 정했습니다. 중심점은 최소 2km 간격으로 추렸습니다.</p><p class="muted">검증된 경영 예측식이 아닌 조사 운영 규칙입니다. 병원 영향 1·2·3은 가정이며 실제 처리 능력이 아닙니다. 높은 포함률이 수익성·성공확률을 보장하지 않습니다.</p></div>`;
    $('sources').innerHTML=`<div class="card"><h2>현재 확보한 자료와 남은 확인</h2><p>분석 기준일 ${esc(data.as_of)}. 이번 화면의 조사 데이터는 고정 스냅샷입니다. 이전 자동 점수의 갱신 날짜와 섞지 않습니다.</p>${table(['자료','시점·한계'],[
      ['주민등록 인구','2026-09 · 외국인 제외 · 동 합계 대조 완료'],['WorldPop 분포','2020 분포를 2026-09 동 인구에 맞춰 배분 · 최신 관측 격자가 아님'],['HIRA 병·의원 / 상세 장비','2026-09 / 2025-12 표기 · 현재 가동·검사량 미확인'],['행정동 경계','ver20260201 · 민간 가공 · 인천 개편 연결 미확인'],['SGIS 최신 소지역 인구','원문 미확보 · 기존 인구 배분 가정은 아직 대체하지 않음'],['의료진·진료 안내','기관 자체 안내·검색 색인·제3자 근거를 구분 · 실제 운영 재확인'],['건물·매물','아직 검증하지 않음 · 임장에서 확인 · 예산은 평가 제외']])}<p class="callout">장비 원문은 711,808행, 포털 표기는 692,635행으로 다릅니다. 과거 내시경 장비기관 61곳은 현재 기관번호에 연결되지 않았습니다. 미연결·장비 미확인을 폐업·미보유로 단정하지 않습니다.</p><p>연세은혜내과는 HIRA 내과 전문의 1명과 웹 2인 안내, 서울오케이내과는 HIRA 5명과 확인 페이지 3명 안내가 다릅니다. 시점·별관·역할·페이지 누락을 확인하기 전 덮어쓰지 않았습니다.</p></div><div class="card"><h2>자료 출처</h2><ul class="source-list"><li><a href="https://jumin.mois.go.kr/ageStatMonth.do" target="_blank" rel="noopener">행정안전부 주민등록 연령별 인구</a></li><li><a href="https://www.data.go.kr/data/15051059/fileData.do" target="_blank" rel="noopener">HIRA 병·의원 현황</a> · 공공누리 제1유형</li><li><a href="https://www.data.go.kr/data/15051055/fileData.do" target="_blank" rel="noopener">HIRA 상세 의료장비</a> · 공공누리 제1유형</li><li><a href="https://github.com/vuski/admdongkor/tree/master/ver20260201" target="_blank" rel="noopener">행정동 경계 ver20260201</a></li><li><a href="https://worldpop.arcgis.com/arcgis/rest/services/WorldPop_Total_Population_100m/ImageServer" target="_blank" rel="noopener">WorldPop 공식 클라우드 2020 격자</a> · DOI 10.5258/SOTON/WP00645 · CC BY 4.0 · 최근접 보간 출력, 국가 TIFF와 정렬이 다를 수 있음</li>${data.evidence.sources.map(s=>`<li>${link(s)}<br><small>${esc(s.kind)} · ${esc(s.claim)}</small></li>`).join('')}</ul></div>`;
  }
  async function init(){
    try {
      const response=await fetch('data/research.json',{cache:'no-cache'});
      if(!response.ok)throw new Error('HTTP '+response.status);
      data=await response.json();
      if(data.status!=='research_not_recommendation'||!Array.isArray(data.anchors)||data.anchors.length!==827)throw new Error('검증 자료 형식 불일치');
      $('snapshot').textContent=data.as_of+' 검증';
      $('metrics').innerHTML=[['15곳','검증 우선 조사 후보'],['8곳','기관·주거 변화 사전 조사'],[data.method.pending+'곳','확인 대기 · 부적합 아님'],['144가지','가정 비교 · 성공확률 아님']].map(([n,label])=>`<div class="metric"><b>${esc(n)}</b><span>${esc(label)}</span></div>`).join('');
      if(window.L){try{map=L.map('map').setView([37.52,126.95],10);layer=L.layerGroup().addTo(map);const tiles=L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'}).addTo(map);tiles.on('tileerror',()=>{$('map-caption').textContent='배경지도를 불러오지 못했습니다. 중심점과 목록은 사용할 수 있습니다.';});}catch(error){map=null;$('map').textContent='지도를 불러오지 못했습니다. 목록에서 후보를 선택해 주세요.';}}
      else $('map').innerHTML='<p class="empty">지도 연결이 불가능합니다. 모든 후보와 검증 내용은 목록에서 확인할 수 있습니다.</p>';
      renderPanels();draw();
      ['search','scope','region'].forEach(id=>$(id).addEventListener(id==='search'?'input':'change',draw));
      $('reset').addEventListener('click',()=>{$('search').value='';$('scope').value='research';$('region').value='';draw();});
    } catch(error){$('error').hidden=false;$('error').textContent='조사 자료를 불러오지 못했습니다. 연결을 확인하고 새로고침해 주세요. 이전 점수를 새 조사 결과로 대신 표시하지 않습니다.';$('list').innerHTML='';console.error(error);}
  }
  init();
  if('serviceWorker' in navigator)navigator.serviceWorker.register('sw.js').catch(()=>{});
})();
