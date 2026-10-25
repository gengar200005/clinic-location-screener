'use strict';
class ClinicExplorer {
  constructor(map) {
    this.map=map; this.layers=L.layerGroup().addTo(map); this.context=L.layerGroup().addTo(map);
    this.radius=1000; this.filter='all'; this.sort='distance'; this.providers=null; this.serial=0;
    document.getElementById('explorer-controls').innerHTML=`<div class="map-tools"><b>주변 병·의원 상세 탐색</b><label>표시 반경 <select id="clinic-radius"><option value="500">500m</option><option value="1000" selected>1km</option><option value="1500">1.5km</option><option value="2000">2km</option></select></label><label>기관 필터 <select id="clinic-filter"><option value="all">전체 병·의원</option><option value="internal">내과 전문의</option><option value="primary">내과·가정의학과 전문의</option><option value="endo">위내시경 장비 기록</option></select></label><label>정렬 <select id="clinic-sort"><option value="distance">가까운 순</option><option value="internal">내과 우선</option></select></label><button id="anchor-reset">후보 중심점 복귀</button><button id="radius-fit">반경 전체 보기</button><label hidden><input type="checkbox" id="legacy-context"> 이전 경계·역·상가 참고</label><button id="map-expand" aria-pressed="false">지도 크게 보기</button></div><p id="anchor-status" class="muted" aria-live="polite"></p><p class="muted">지도를 클릭하거나 빨간 기준점을 끌어 주변 병·의원을 다시 탐색할 수 있습니다. 거리는 직선거리입니다.</p>`;
    for(const [id,key] of [['clinic-radius','radius'],['clinic-filter','filter'],['clinic-sort','sort']]) document.getElementById(id).addEventListener('change',e=>{this[key]=key==='radius'?Number(e.target.value):e.target.value;this.render();if(key==='radius')this.fit();});
    document.getElementById('anchor-reset').onclick=()=>{if(this.candidate){this.anchor=[this.candidate.lat_shops,this.candidate.lon_shops];this.custom=false;this.render();this.fit();}};
    document.getElementById('radius-fit').onclick=()=>this.fit();
    document.getElementById('legacy-context').onchange=()=>this.loadContext();
    document.getElementById('map-expand').onclick=e=>{const expanded=document.getElementById('map').classList.toggle('expanded');e.target.textContent=expanded?'지도 크기 복귀':'지도 크게 보기';e.target.setAttribute('aria-pressed',String(expanded));map.invalidateSize();this.fit();};
    map.on('click',e=>{if(this.candidate)this.move(e.latlng.lat,e.latlng.lng);});
    this.load();
  }
  escape(v){return String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
  value(v){return v==null?'미확인':Number(v).toLocaleString('ko-KR');}
  async load(){
    this.error=null;
    try {const r=await fetch('priority_assets/providers.json',{cache:'no-cache'});if(!r.ok)throw Error(r.status);const d=await r.json();if(!Array.isArray(d.rows)||!d.columns.includes('lat'))throw Error('format');this.providers=d.rows.map(row=>Object.fromEntries(d.columns.map((c,i)=>[c,row[i]])));}
    catch(e){this.error='병·의원 자료를 불러오지 못했습니다. 기관이 없다는 뜻은 아닙니다.';}
    this.render();
  }
  select(r){this.candidate=r;this.anchor=[r.lat_shops,r.lon_shops];this.custom=false;this.render();this.loadContext();}
  clear(){this.serial++;this.candidate=null;this.anchor=null;this.layers.clearLayers();this.context.clearLayers();document.getElementById('provider-explorer').innerHTML='후보를 선택해 주세요.';document.getElementById('anchor-status').textContent='';}
  move(lat,lon){this.anchor=[lat,lon];this.custom=true;this.render();}
  fit(){if(this.anchor)this.map.fitBounds(L.latLng(this.anchor).toBounds(this.radius*2),{padding:[20,20],animate:false});}
  distance(p){const rad=Math.PI/180;const a=this.anchor;const x=Math.sin((p.lat-a[0])*rad/2)**2+Math.cos(a[0]*rad)*Math.cos(p.lat*rad)*Math.sin((p.lon-a[1])*rad/2)**2;return 6371000*2*Math.atan2(Math.sqrt(x),Math.sqrt(Math.max(0,1-x)));}
  links(p){const q=encodeURIComponent(p.name+' '+p.address);let s=`<a target="_blank" rel="noopener" href="https://map.naver.com/p/search/${q}">네이버 지도</a> · <a target="_blank" rel="noopener" href="https://map.kakao.com/link/search/${q}">카카오 지도</a> · <a target="_blank" rel="noopener" href="https://search.naver.com/search.naver?query=${q}">검색</a>`;if(p.website&&/^https?:\/\//i.test(p.website))s+=` · <a target="_blank" rel="noopener" href="${this.escape(p.website)}">홈페이지</a>`;if(p.phone&&/^[0-9+()\s-]+$/.test(p.phone))s+=` · <a href="tel:${this.escape(p.phone)}">전화</a>`;return s;}
  info(p){return `<b>${this.escape(p.name)}</b> · ${this.escape(p.type_name)}<br>전체 의사 ${this.value(p.total_doctors)}명 · 내과 전문의 ${this.value(p.internal_specialists)}명 · 가정의학과 ${this.value(p.family_specialists)}명<br>위내시경 장비: ${p.egd_equipment_2025_positive?'양성 기록':'미확인'} · 대장내시경: ${p.colo_equipment_2025_positive?'양성 기록':'미확인'}<br>${this.escape(p.address)}<br>${this.escape(p.phone||'연락처 미확인')} · 개설일 ${this.escape(p['개설일자']||'미확인')}<br>${this.links(p)}`;}
  render(){
    if(!this.anchor)return;
    const box=document.getElementById('provider-explorer');this.layers.clearLayers();
    const label=this.custom?'직접 선택한 기준점':this.candidate.adm_nm+' · 후보 중심점';
    document.getElementById('anchor-status').textContent=label+' ('+this.anchor.map(v=>Number(v).toFixed(5)).join(', ')+')';
    [500,1000,1500,2000].filter(r=>r<=this.radius).forEach(r=>L.circle(this.anchor,{radius:r,color:r===this.radius?'#175c4b':'#738d82',weight:r===this.radius?2:1,dashArray:r===this.radius?null:'4,4',fillOpacity:.02,interactive:false}).bindTooltip(r+'m').addTo(this.layers));
    const pin=L.marker(this.anchor,{draggable:true,icon:L.divIcon({className:'anchor-pin',html:'<span>＋</span>',iconSize:[30,30],iconAnchor:[15,15]})}).addTo(this.layers).bindPopup(this.escape(label));
    pin.on('dragend',()=>{const p=pin.getLatLng();this.move(p.lat,p.lng);});
    if(!this.providers){box.innerHTML=this.error?`<p>${this.error}</p><button id="provider-retry">다시 불러오기</button>`:'병·의원 자료를 불러오는 중…';if(this.error)document.getElementById('provider-retry').onclick=()=>this.load();return;}
    const base=this.providers.map(p=>({p,d:this.distance(p)})).filter(x=>x.d<=this.radius);
    const internal=base.filter(x=>x.p.internal_specialists>0).length;
    const rows=base.filter(({p})=>this.filter==='all'||this.filter==='internal'&&p.internal_specialists>0||this.filter==='primary'&&(p.internal_specialists>0||p.family_specialists>0)||this.filter==='endo'&&p.egd_equipment_2025_positive).sort((a,b)=>this.sort==='internal'?(Number(b.p.internal_specialists>0)-Number(a.p.internal_specialists>0)||a.d-b.d):a.d-b.d);
    box.innerHTML=`<h3>반경 ${this.radius.toLocaleString()}m · 병·의원 ${base.length}곳 / 내과 전문의 기관 ${internal}곳</h3><p class="muted">필터 결과 ${rows.length}곳 · HIRA 2026-09 / 장비 2025-12. 위·대장 장비 기록은 현재 운영·검진 지정 여부와 다릅니다. 전문의 결측은 0명으로 처리하지 않습니다. 치과·한방·약국은 이 목록에 포함하지 않습니다.</p>${this.custom?'<p class="callout">기준점을 옮겨 기관 거리와 수만 다시 계산했습니다. 아래 수요·통근·조사 순서는 원래 후보 중심점 기준입니다.</p>':''}<div class="provider-list">${rows.length?rows.map(({p,d},i)=>`<div class="provider-row"><button data-provider="${i}">${this.escape(p.name)} · ${Math.round(d)}m ${p.internal_specialists>0?'· 내과':''} ${p.egd_equipment_2025_positive?'· 위내시경 장비':''}</button><details><summary>의료진·주소·연락처 상세보기</summary>${this.info(p)}</details></div>`).join(''):'<p>현재 반경·필터에 일치하는 기록이 없습니다. 실제 기관 부재를 확정하지 않습니다.</p>'}</div><p class="muted">지도 핀: 빨강 내과 전문의 · 보라 위내시경 장비 기록 · 회색 그 외. 목록의 기관명을 누르면 해당 위치를 표시합니다.</p><div id="context-note" class="muted"></div>`;
    const markers=rows.map(({p,d})=>{const color=p.internal_specialists>0?'#c44236':p.egd_equipment_2025_positive?'#7953a5':'#6c7b85';return L.circleMarker([p.lat,p.lon],{radius:p.internal_specialists>0?7:5,color,weight:2,fillOpacity:.8,bubblingMouseEvents:false}).addTo(this.layers).bindPopup(`<div class="provider-popup">${this.info(p)}<br>현재 기준점에서 ${Math.round(d)}m</div>`,{maxWidth:340});});
    box.querySelectorAll('[data-provider]').forEach(b=>b.onclick=()=>{const i=Number(b.dataset.provider);this.map.setView([rows[i].p.lat,rows[i].p.lon],17);markers[i].openPopup();document.getElementById('map').scrollIntoView({block:'center',behavior:'smooth'});});
    this.contextNote();
  }
  contextNote(){const el=document.getElementById('context-note');if(el)el.innerHTML=this.contextMessage||'';}
  async loadContext(){
    const serial=++this.serial;this.context.clearLayers();this.contextMessage='';this.contextNote();
    if(!this.candidate||!document.getElementById('legacy-context').checked)return;
    const id=this.candidate.adm_cd;
    try{const response=await fetch(`data/detail/${encodeURIComponent(id)}.json`);if(!response.ok)throw Error('missing');const d=await response.json();if(serial!==this.serial)return;
      if(d.boundary)L.geoJSON(d.boundary,{style:{color:'#929987',weight:1,dashArray:'5,5',fillOpacity:.025},interactive:false}).addTo(this.context);
      if(d.commercial_hull)L.geoJSON(Array.isArray(d.commercial_hull)?{type:"Polygon",coordinates:[d.commercial_hull]}:d.commercial_hull,{style:{color:'#d09b2e',weight:2,fillOpacity:.08},interactive:false}).addTo(this.context);
      if(d.station){L.circleMarker([d.station.lat,d.station.lon],{radius:10,color:'#2479aa',bubblingMouseEvents:false}).bindPopup('이전 자료의 역: '+this.escape(d.station.name)).addTo(this.context);L.circle([d.station.lat,d.station.lon],{radius:500,color:'#2479aa',fillOpacity:.02,interactive:false}).addTo(this.context);}
      if(d.commercial_anchor)L.circleMarker([d.commercial_anchor.lat,d.commercial_anchor.lon],{radius:9,color:'#c69020',bubblingMouseEvents:false}).bindPopup('이전 자료의 상가 추정 중심').addTo(this.context);
      this.contextMessage=`경계·역·상가 영역은 이전 버전 자료이며 최신 검증값이 아닙니다. <a href="detail/#${encodeURIComponent(id)}" target="_blank" rel="noopener">이전 상세 전체 보기 ↗</a>`;
    }catch(e){if(serial!==this.serial)return;this.contextMessage='이 후보는 이전 상세 자료가 없습니다. 최신 병·의원 탐색은 계속 사용할 수 있습니다.';}
    this.contextNote();
  }
}
