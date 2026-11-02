'use strict';
(()=>{
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const num=x=>x==null||x===''?'미확인':Number.isFinite(Number(x))?Number(x).toLocaleString('ko-KR',{maximumFractionDigits:1}):'미확인';
 const pct=x=>x==null?'비교 불가':`${x>0?'+':''}${num(x)}%`;
 const allowed=p=>!p.exclusion;
 const ready=data=>data.candidates.filter(p=>allowed(p)&&p.visit_state==='방문 준비 완료');
 const shortlist=(data)=>data.candidates.filter(p=>allowed(p)&&p.research_order&&p.research_order<=(p.region==='서울'?data.rules.seoul_shortlist:data.rules.gyeonggi_shortlist)).sort(order);
 const order=(a,b)=>(a.region==='서울'?0:1)-(b.region==='서울'?0:1)||(a.research_order??9999)-(b.research_order??9999)||a.id.localeCompare(b.id);
 const commuteLabel=x=>({'car_likely_within_60_under_historical_error_band':'과거 경로 추정상 60분 안쪽','car_60min_boundary_keep_for_review':'60분 경계·현재 경로 확인 필요','insufficient_nearby_car_evidence':'근접 경로 부족·통근 미확인','car_likely_over_60_transit_unverified':'자차60분 초과 가능·대중교통 미확인'}[x]||x);
 const priority=p=>p.research_order?`${p.region} 조사 ${p.research_order}`:p.exclusion?'현재 제외':'현재 비대표·참고';
 const adapt=p=>({adm_cd:p.id,adm_nm:p.area,population_name:p.area,lat_shops:p.lat,lon_shops:p.lon,region:p.region,current_record:p,market_code:p.market_code,market_link_verified:!!p.market_code});
 let promise;
 function load(){return promise??=fetch('data/current-recommendations.json',{cache:'no-cache'}).then(r=>{if(!r.ok)throw Error('현재 추천 자료를 불러오지 못했습니다.');return r.json()}).then(d=>{if(!d.version||new Set(d.candidates.map(p=>p.id)).size!==d.candidates.length)throw Error('현재 추천 고유키 오류');return d})}
 const link=p=>`market-demand.html?candidate=${encodeURIComponent(p.id)}`;
 function coverage(d){return `<p class="muted">추천 산출 ${esc(d.as_of)} · 공통 버전 <code>${esc(d.version)}</code> · 기존 점수와 별도인 조사 순서</p><div class="scroll"><table><thead><tr><th>지역</th><th>확보 좌표</th><th>범위/러프통근 대기</th><th>공통 비교 / 지표미연결</th><th>의료매출 연결/미연결</th></tr></thead><tbody>${d.coverage.map(c=>`<tr><td>${c.region}</td><td>${num(c.registry_coordinates)}</td><td>${num(c.personal_or_scope_excluded)}</td><td>${num(c.comparable_common)} / ${num(c.common_unlinked_eligible)}</td><td>${num(c.market_linked)} / ${num(c.unlinked_market)}</td></tr>`).join('')}</tbody></table></div><p><a href="data/current-inventory.csv" download>원본18,675좌표+수동방문1건 결정·미연결 CSV</a> · <a href="data/current-recommendations.json">현재 추천 데이터</a></p><p>서울·경기 순서는 별도 비교군입니다. 행정동별 대표 조사점이며 실제 생활권/임대건물이 아닙니다. 같은 동 안에서도 철길·하천·대로에 따라 진료권이 갈릴 수 있습니다.</p>`}
 function detail(p,d){
  const m=d.markets[p.market_code],t=m?.trend,e=p.evidence?.['1000'],r=p.evidence?.['1500'];
  return `<div data-current-version="${esc(d.version)}"><p class="badge">${esc(priority(p))} · ${esc(p.visit_state)}</p><h2>${esc(p.name)}</h2><p>${esc(p.area)} · ${esc(p.zone)}<br>조사좌표 ${p.lat}, ${p.lon} · 공실 아님</p><p><b>선정 이유:</b> ${p.reasons.map(esc).join(' · ')}</p>${e?`<div class="scroll"><table><tr><th>직선반경</th><th>50~69세 하한 추정</th><th>내과 전문의 의원</th><th>내시경 의원/병원</th><th>위암 검진 지정</th></tr>${[[1000,e],[1500,r]].filter(x=>x[1]).map(([radius,x])=>`<tr><td>${radius/1000}km</td><td>${num(x.demand)}명</td><td>${num(x.internal)}</td><td>${num(x.endo)} / ${num(x.hospital)}</td><td>${num(x.gastric)}</td></tr>`).join('')}</table></div>`:'<p>이 좌표의 공통 수요·공급 산출물 미연결. 다른 좌표 값을 승계하지 않습니다.</p>'}
  <p>${t?`최근 ${t.compared_quarters}개 대응분기 · 금액 ${pct(t.amount_yoy)} / 거래 ${pct(t.transactions_yoy)} / 건당 ${pct(t.unit_yoy)}. ${esc(t.explanation)}`:'의료소비 연결/정의 확인 대기. 저수요 판정 아님'}</p>
  <p><b>반대 근거:</b> ${p.counter_evidence.map(esc).join(' · ')}</p><p><b>민감도:</b> 대안 규칙에서 같은 행정동 대표 조사순서 ${num(p.sensitivity?.[0])}~${num(p.sensitivity?.[1])}. 동급 조사군 안의 실행 순서이며 성공·수익성 순위가 아닙니다.</p>
  <p><b>러프 통근:</b> 출근 ${num(p.commute.am)}분 / 퇴근 ${num(p.commute.pm)}분 · ${esc(commuteLabel(p.commute.state))} · 현재 건물 실측 아님</p>
  ${p.representative_id&&p.representative_id!==p.id?`<p>같은 행정동 조사 묶음의 <a href="market-demand.html?candidate=${encodeURIComponent(p.representative_id)}">현재 대표는 별도 좌표</a>입니다. 이 화면의 수요·공급은 지금 선택한 좌표 값이고, 민감도는 대표 교체를 허용한 행정동 조사순서입니다. 같은 생활권이라는 뜻은 아닙니다.</p>`:''}<p><b>과거 비교:</b> ${esc(p.change)}${p.old_visit_order?' · 과거10권역 방문순서 '+p.old_visit_order:''}. 다른 모집단/단위의 순위 차감을 하지 않습니다.</p>
  <p><b>다음 행동:</b> ${esc(p.next_check)}</p>${p.manual_opinion?`<p class="muted">수동 의견: ${esc(p.manual_opinion.source)} · 작성 ${esc(p.manual_opinion.recorded_on)} · 현재 자료로 재확인 필요. 자동 선정 근거와 별도입니다.</p>`:''}
  ${p.buildings?.length?`<details><summary>기존 건물·광고 기록 · 현재 가용 공간과 구별</summary>${p.buildings.map(b=>`<p>${esc(b.zone)} · ${esc(b.address)}<br>${esc(b.record_type)} · ${esc(b.availability)} · 전용㎡ ${num(b.exclusive_area_sqm)} · 기록 ${esc(b.checked_on)}<br><a href="${esc(b.source)}" target="_blank" rel="noopener">기존 출처</a></p>`).join('')}<p>한 건물 부적합은 생활권 전체 탈락 아님. 다른 주소에 조사점 수요를 승계하지 않음.</p></details>`:''}<p class="muted">인구 ${esc(d.periods.residents)} · 공급 ${esc(d.periods.providers)} · 의료 ${esc(d.periods.medical)} · 경계 ${esc(d.periods.boundary)}</p><a href="${link(p)}">시장 상세·전체 비교 →</a> · <a href="index.html?candidate=${encodeURIComponent(p.id)}">지도에서 같은 조사좌표 →</a><p class="muted">현재 추천 버전 ${esc(d.version)}</p></div>`;
 }
 function cards(d,rows=shortlist(d)){return `<div data-current-version="${d.version}"><p>먼저 공간 문의: ${rows.filter(p=>p.region==='서울').slice(0,3).map(p=>esc(p.name)).join(' → ')}. 조사 후보 ${rows.length}곳 · 전체 후보 중 방문 준비 완료 ${ready(d).length}곳. ${ready(d).filter(p=>!rows.some(r=>r.id===p.id)).map(p=>`<a href="${link(p)}">${esc(p.name)}: 방문 준비 완료(조사 우선군과 별도)</a>`).join(' · ')} 공간·층·현 공실·전용95~100평 근거 확보 후 방문합니다.</p>${rows.map(p=>`<article class="card"><p class="badge">${priority(p)} · ${esc(p.visit_state)}</p><h3><a href="${link(p)}">${esc(p.name)}</a></h3><p>${esc(p.area)}</p><p>${p.reasons.map(esc).join(' · ')}</p><p>반대 근거: ${p.counter_evidence.map(esc).join(' · ')}</p><p>다음 확인: ${esc(p.next_check)}</p></article>`).join('')}</div>`}
 const api={esc,num,pct,allowed,ready,shortlist,order,priority,adapt,load,coverage,detail,cards,link};
 if(typeof module!=='undefined')module.exports=api;
 if(typeof window==='undefined')return;window.CurrentView=api;
 const mount=document.getElementById('current-recommendations-mount');
 if(mount)load().then(d=>{mount.innerHTML=`<h1>현재 조사·방문 준비</h1>${coverage(d)}${cards(d)}`}).catch(e=>mount.textContent=e.message);
})();
