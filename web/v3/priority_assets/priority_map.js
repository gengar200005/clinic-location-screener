/* Restore the existing clinic explorer against the new candidate coordinates. */
(()=>{
 const selector=document.getElementById('map-candidate');
 const candidates=D.nodes.slice().sort((a,b)=>(a.priority_rank??9999)-(b.priority_rank??9999)||a.name.localeCompare(b.name,'ko'));
 selector.innerHTML=candidates.map(n=>`<option value="${esc(n.anchor_id)}">${n.priority_rank==null?'통근 경계':`공동 ${n.priority_rank}위`} · ${esc(n.name)}</option>`).join('');
 const message=document.getElementById('map-message');
 if(!window.L){message.textContent='지도 라이브러리를 불러오지 못했습니다. 아래 후보 카드의 반경별 기관 상세는 계속 사용할 수 있습니다.';return;}
 const map=L.map('map').setView([37.50,126.95],12);
 L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>'}).addTo(map).on('tileerror',()=>{message.textContent='배경지도 연결 실패: 기준점·반경·기관 목록은 사용할 수 있습니다.';});
 const explorer=new ClinicExplorer(map);
 const candidateLayer=L.layerGroup().addTo(map);
 function select(id,scroll=false){
  const n=candidates.find(n=>n.anchor_id===id);if(!n)return;
  selector.value=id;
  explorer.select({...n,lat_shops:n.lat,lon_shops:n.lon,adm_nm:n.name});
  explorer.fit();
  document.getElementById('map-selected-evidence').textContent=`${n.name} · 기본 조건 ${n.priority_rank==null?'경계 검토':`공동 ${n.priority_rank}위`} · 중심점의 이동 검증 ${n.resident}/32. 지도 기준점을 옮겨도 인구·통근·순위는 자동 재산정되지 않습니다.`;
  if(scroll)document.getElementById('interactive-explorer').scrollIntoView({behavior:'smooth',block:'start'});
 }
 candidates.forEach(n=>L.circleMarker([n.lat,n.lon],{radius:5,color:'#1675ac',fillOpacity:.7,bubblingMouseEvents:false}).addTo(candidateLayer).bindTooltip(n.name).on('click',()=>select(n.anchor_id)));
 selector.addEventListener('change',()=>select(selector.value));
 document.getElementById('show-all-candidates').onclick=()=>map.fitBounds(candidates.map(n=>[n.lat,n.lon]),{padding:[25,25]});
 document.addEventListener('click',event=>{const button=event.target.closest('[data-map-anchor]');if(button)select(button.dataset.mapAnchor,true);});
 const requested=new URLSearchParams(location.search).get('anchor');
 const initial=candidates.find(n=>n.anchor_id===requested)||candidates.find(n=>n.name.includes('사당로 2'))||candidates[0];select(initial.anchor_id);
})();
