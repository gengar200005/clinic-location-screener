'use strict';
(()=>{
 const esc=x=>String(x??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const value=c=>!c?'미제공':c.state==='suppressed'?'비공개(*)':c.state==='invalid'?'형식 오류·계산 보류':c.state==='missing'?'결측':String(c.value);
 const num=x=>x==null||String(x).trim()===''?'미확인':Number.isFinite(Number(x))?Number(x).toLocaleString('ko-KR',{maximumFractionDigits:2}):'미확인';
 const pct=x=>x==null||String(x).trim()===''?'비교 불가':`${Number(x)>0?'+':''}${num(x)}%`;
 const q=x=>x?`${x.slice(0,4)}년 ${x.slice(-1)}분기`:'기간 미확인';
 const metric=(title,main,note='')=>`<div class="md-metric"><span>${esc(title)}</span><b>${esc(main)}</b><span>${esc(note)}</span></div>`;
 const marginal=items=>Object.keys(items||{}).length?`<dl>${Object.entries(items).map(([k,v])=>`<dt>${esc(k)}</dt><dd>${esc(value(v))}</dd>`).join('')}</dl>`:'미제공';
 function matchingPilot(pilots,anchor){return anchor?pilots.find(p=>p.administrative_fullname===anchor.adm_nm):undefined}
 function chart(history){
  const vals=history.map(h=>h.amount.state==='observed'?Number(h.amount.value):null),valid=vals.filter(x=>x!=null&&Number.isFinite(x));if(!valid.length)return '<p>그래프 자료 없음</p>';
  const lo=Math.min(...valid),hi=Math.max(...valid),span=hi-lo||1;let line='',points='';
  vals.forEach((v,i)=>{if(v==null){line+=' ';return}const x=12+i*576/Math.max(vals.length-1,1),y=114-(v-lo)/span*96;const last=i>0&&vals[i-1]!=null;line+=`${last?'L':'M'}${x.toFixed(1)},${y.toFixed(1)} `;points+=`<circle cx="${x}" cy="${y}" r="3"><title>${esc(q(history[i].quarter))}: ${esc(history[i].amount.value)}원</title></circle>`});
  return `<svg class="md-chart" viewBox="0 0 600 135" role="img" aria-label="행정동 일반의원 매출 추이"><path d="${line}" fill="none" stroke="currentColor" stroke-width="2"/><g fill="currentColor">${points}</g></svg><p class="md-muted">${q(history[0].quarter)}~${q(history.at(-1).quarter)} · 세로축 자동범위 ${num(lo)}~${num(hi)}원 · 결측 구간은 연결하지 않음</p>`;
 }

 let data,lastAnchor;
 function selectedRecord(){return lastAnchor?.current_record||data?.candidates.find(p=>!p.exclusion&&p.research_order&&p.area===lastAnchor?.adm_nm)}
 function inline(){const el=document.getElementById('market-demand-inline');if(!el||!data)return;const p=selectedRecord();el.innerHTML=p?`<div class="md-card" data-current-version="${data.version}"><h3>현재 조사·시장 근거</h3><p>${CurrentView.priority(p)} · ${esc(p.visit_state)} · ${esc(p.area)}</p><p>${p.reasons.map(esc).join(' · ')}</p><a href="${CurrentView.link(p)}">같은 후보의 통합 상세 →</a><p>버전 ${data.version}</p></div>`:'<div class="md-card"><h3>현재 조사·시장 근거</h3><p>이 행정동/좌표는 현재 대표 미연결입니다. 0점·부적합이 아닙니다.</p><a href="current-fieldwork.html">현재 조사 목록 →</a></div>';}
 const api={select(anchor){lastAnchor=anchor;inline()},matchingPilot,esc,value,pct};
 if(typeof module!=='undefined'&&module.exports)module.exports=api;
 if(typeof window==='undefined'||typeof document==='undefined')return;
 window.marketDemand=api;
 function render(){
  const p=data.candidates.find(p=>p.id===document.getElementById('market-pilot').value);if(!p||p.exclusion)return;
  const m=data.markets[p.market_code],latest=m?.latest,living=data.limitations;
  document.getElementById('market-detail').innerHTML=CurrentView.detail(p,data)+(m?`<section class="md-card"><h2>행정동 일반의원 의료소비</h2><p>${esc(m.scope)} · ${esc(latest.quarter)} 표시 원자료 · 후보 건물이나 반경 매출 아님</p><div class="md-grid">${metric('금액 원자료',value(latest.amount)+'원')}${metric('거래',value(latest.transactions)+'건','환자 수 아님')}${metric('건당 금액',(latest.per_transaction??'미확인')+'원','진료 가격 아님')}${metric('점포',value(latest.shops)+'곳','전년동기 변화 '+(latest.shop_change??'미확인'))}</div>${chart(m.history)}<p>${esc(m.trend.explanation)} · 4분기 금액 ${pct(m.trend.amount_yoy)} / 거래 ${pct(m.trend.transactions_yoy)} / 건당 ${pct(m.trend.unit_yoy)}</p><details><summary>실제 제공 연령별 의료매출</summary>${marginal(latest.age_amounts)}<p>60세 이상 구간 유지. 연령×시간 값 생성 안 함.</p></details><details><summary>22분기 금액·거래·건당 금액·점포</summary><div class="md-scroll"><table>${m.history.map(h=>`<tr><td>${esc(h.quarter)}</td><td>${esc(value(h.amount))}원</td><td>${esc(value(h.transactions))}건</td><td>${esc(h.per_transaction??'미확인')}원</td><td>${esc(value(h.shops))}곳</td></tr>`).join('')}</table></div></details></section><section class="md-card"><h2>상권 활동·직장·길단위인구 · 참고, 선정 미반영</h2><p>소비 단위·거주지/결제지 기준·연1회 갱신 설명과 원문 분기 값의 충돌은 미해결입니다. 소득으로 해석하지 않습니다. 직장·주거·생활인구를 더하지 않습니다.</p>${Object.entries(m.context).map(([key,c])=>`<details><summary>${{consumption:'상권 소비활동',workers:'직장인구',street:'길단위인구'}[key]} · 표시 ${c.period??'미확인'} · 값 ${c.value??'미확인'}</summary><dl>${Object.entries(c.fields||{}).map(([k,v])=>`<dt>${esc(k)}</dt><dd>${esc(v===''?'결측':v)}</dd>`).join('')}</dl></details>`).join('')}<p>연령/시간/요일 주변합은 교차자료 아님. 길단위인구는 실제 보행량·고유 방문자 수가 아닙니다. 시간 집계 정의 미검증으로 시간평균 환산 없음.</p></section>`:'<section class="md-card"><p>이 좌표의 의료소비 연결/정의 검증 대기. 다른 행정동 자료나 경기 과거의료기관 원문을 대입하지 않습니다.</p></section>')+`<section class="md-card"><h2>250m 생활인구 · 반경 집계/4주 분석 미완료</h2><p>기존 2026-10-29 하루 샘플에서 동일 격자/시각의 다중 행정동 행 의미를 검증하지 못했습니다. 이번 전체 후보에 확장하거나 임의 합산하지 않았습니다. 비공개(*)를0으로 처리하지 않고, 기간이 다른 매출과 전환율을 계산하지 않습니다.</p><p>현재 공통 버전 ${data.version}</p></section>`;
  const u=new URL(location.href);u.searchParams.set('candidate',p.id);u.searchParams.delete('pilot');history.replaceState(null,'',u);
 }
 CurrentView.load().then(d=>{
  data=d;inline();if(!document.getElementById('market-page'))return;
  const candidates=d.candidates.filter(CurrentView.allowed).sort(CurrentView.order),select=document.getElementById('market-pilot');
  select.innerHTML=candidates.map(p=>`<option value="${esc(p.id)}">${CurrentView.priority(p)} · ${esc(p.name)}</option>`).join('');
  const query=new URLSearchParams(location.search),requested=query.get('candidate')||query.get('pilot');if(candidates.some(p=>p.id===requested))select.value=requested;
  document.getElementById('market-status').innerHTML=CurrentView.coverage(d);
  document.getElementById('market-comparison').innerHTML=CurrentView.shortlist(d).map(p=>{const t=d.markets[p.market_code]?.trend;return `<tr><td><a href="${CurrentView.link(p)}">${esc(p.name)}</a><br>${CurrentView.priority(p)}</td><td>${p.old_area_visit_order?'과거10권역 순서 '+p.old_area_visit_order:'종전10권역 밖'}<br>${esc(p.history_change||p.change)}</td><td>${t?pct(t.amount_yoy)+' / '+pct(t.transactions_yoy)+' / '+pct(t.unit_yoy):'의료소비 비교보류'}</td><td>${p.reasons.map(esc).join(' · ')}</td><td>${esc(p.visit_state)}<br>대안순서 ${p.sensitivity.map(CurrentView.num).join('~')}</td></tr>`}).join('');
  document.getElementById('market-sources').innerHTML=`<p>같은 추천 버전 ${d.version} · ${esc(d.periods.medical)}</p><p><a href="https://data.seoul.go.kr/dataList/OA-22175/S/1/datasetView.do">서울 일반의원 의료매출</a> · <a href="https://data.seoul.go.kr/dataList/OA-22172/S/1/datasetView.do">점포</a> · <a href="https://data.seoul.go.kr/dataList/OA-22166/S/1/datasetView.do">소비</a> · <a href="https://data.seoul.go.kr/dataList/OA-22184/S/1/datasetView.do">직장</a> · <a href="https://data.seoul.go.kr/dataList/OA-22178/S/1/datasetView.do">길단위</a> · <a href="https://data.seoul.go.kr/dataList/OA-22784/S/1/datasetView.do">250m</a></p><ul>${d.limitations.map(x=>`<li>${esc(x)}</li>`).join('')}</ul>`;
  select.addEventListener('change',render);render();
 }).catch(e=>{const el=document.getElementById('market-error')||document.getElementById('market-demand-inline');if(el)el.textContent=e.message});
})();
