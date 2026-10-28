'use strict';
let data;
const $=id=>document.getElementById(id);
const qLabel=q=>`${q.slice(0,4)}년 ${q.slice(4)}분기`;
const money=x=>x==null?'자료 없음':Math.round(x).toLocaleString('ko-KR')+'원';
const total=x=>x==null?'자료 없음':(x/100000000).toFixed(2)+'억원';
const pct=x=>x===null?'자료 없음':`${x>0?'+':''}${x.toFixed(1)}%`;
function td(tr,text,cls=''){const e=document.createElement('td');e.textContent=text;e.className=cls;tr.append(e);return e;}
function history(code){
 const rows=data.records.filter(r=>r.code===code).sort((a,b)=>a.quarter.localeCompare(b.quarter));
 const box=$('history');box.replaceChildren();box.hidden=false;
 const h=document.createElement('h2');h.textContent=`${rows[0].name} · 분기별 변화`;box.append(h);
 const p=document.createElement('p');p.textContent='총 추정매출은 원자료 금액(원)입니다. 1인당 값은 같은 분기 행정동 상주인구로 나눈 참고값이며 주민의 실제 소비액이나 환자당 매출이 아닙니다.';box.append(p);
 const table=document.createElement('table');const head=document.createElement('tr');
 for(const text of ['분기','총 추정매출(원자료)','주민 1인당 환산액','상주인구','전년 동기 대비','점포 수'])td(head,text);table.append(head);
 for(const r of rows){const tr=document.createElement('tr');td(tr,qLabel(r.quarter));td(tr,money(r.amount));td(tr,money(r.per_resident));td(tr,r.population?.toLocaleString('ko-KR')??'미연결');td(tr,pct(r.yoy));td(tr,r.shops??'자료 없음');table.append(tr);}const scroll=document.createElement('div');scroll.className='scroll';scroll.append(table);box.append(scroll);box.scrollIntoView({behavior:'auto'});
}
function render(){
 const query=$('search').value.trim();const candidates=new Set(data.matches.filter(m=>m.research_order!=null&&m.code).map(m=>m.code));
 const rows=data.records.filter(r=>r.quarter===$('quarter').value&&r.name.includes(query)&&(!$('candidate').checked||candidates.has(r.code))).sort((a,b)=>a.name.localeCompare(b.name,'ko'));
 $('rows').replaceChildren();$('count').textContent=`${rows.length}개 동 · 매출 자료 없음 ${rows.filter(r=>r.amount===null).length}개 · 동 이름을 누르면 과거 추세 표시`;
 for(const r of rows){const tr=document.createElement('tr');const cell=td(tr,'');const b=document.createElement('button');b.textContent=r.name;b.onclick=()=>history(r.code);cell.append(b);td(tr,total(r.amount)).title=money(r.amount);td(tr,money(r.per_resident));td(tr,r.population?.toLocaleString('ko-KR')??'미연결');td(tr,pct(r.yoy),r.yoy>0?'up':r.yoy<0?'down':'');td(tr,pct(r.qoq),r.qoq>0?'up':r.qoq<0?'down':'');td(tr,r.shops??'자료 없음');td(tr,r.shop_change===null?'자료 없음':`${r.shop_change>0?'+':''}${r.shop_change}`);td(tr,r.age50plus_pct===null?'자료 없음':`${r.age50plus_pct}%`);$('rows').append(tr);}
}
fetch('data/revenue.json?v=20261028-fullpopulation').then(r=>{if(!r.ok)throw Error('자료 응답 오류');return r.json();}).then(x=>{data=x;$('meta').textContent=`자료 확장 ${x.extended_at??x.retrieved_at} · 최신 ${qLabel(x.latest)} · 최신 매출 ${x.coverage.latest_sales_areas}개 동 / 점포 자료 ${x.coverage.latest_shop_areas}개 동`;
 for(const q of [...x.quarters].reverse()){const o=document.createElement('option');o.value=q;o.textContent=qLabel(q);$('quarter').append(o);}
 for(const id of ['quarter','search','candidate'])$(id).addEventListener('input',render);render();
}).catch(e=>{$('meta').textContent=`자료를 불러오지 못했습니다: ${e.message}`;});

fetch('data/gyeonggi-revenue-scope.json?v=20261028-recheck').then(r=>{if(!r.ok)throw Error('자료 응답 오류');return r.json();}).then(x=>{
 $('gg-meta').textContent=`비교 순위 기준 ${x.ranking_as_of} · 확인 ${new Date(x.checked_at).toLocaleString("ko-KR",{timeZone:"Asia/Seoul"})} (한국시간) · ${x.reason}`;
 const statusLabel={verified:'확인',blocked:'확보 실패',pending:'검증 대기'};
 $('gg-checks').replaceChildren();
 for(const check of x.checks){const tr=document.createElement('tr');td(tr,check.label);td(tr,statusLabel[check.status]??'미확인');td(tr,check.detail);$('gg-checks').append(tr);}
 $('gg-coverage').textContent=`검증된 매출 추세 ${x.coverage.verified_trends} / ${x.coverage.candidates}개 · 검증된 행정동 코드 연결 ${x.coverage.verified_code_matches}개`;
 function renderGyeonggi(){
  const rows=x.candidates.filter(r=>r.name.includes($('gg-search').value.trim()));
  $('gg-count').textContent=`${rows.length} / ${x.candidates.length}개 비교 후보 · 매출 미확보`;
  $('gg-rows').replaceChildren();
  for(const r of rows){const tr=document.createElement('tr');td(tr,r.name);td(tr,r.rank);td(tr,'미확보');td(tr,'원자료 코드표 대기');$('gg-rows').append(tr);}
 }
 $('gg-search').addEventListener('input',renderGyeonggi);renderGyeonggi();
}).catch(e=>{$('gg-meta').textContent=`경기 후보 목록을 불러오지 못했습니다: ${e.message}`;});
