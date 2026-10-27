'use strict';
let data;
const $=id=>document.getElementById(id);
const qLabel=q=>`${q.slice(0,4)}년 ${q.slice(4)}분기`;
const pct=x=>x===null?'자료 없음':`${x>0?'+':''}${x.toFixed(1)}%`;
function td(tr,text,cls=''){const e=document.createElement('td');e.textContent=text;e.className=cls;tr.append(e);return e;}
function history(code){
 const rows=data.records.filter(r=>r.code===code).sort((a,b)=>a.quarter.localeCompare(b.quarter));
 const box=$('history');box.replaceChildren();box.hidden=false;
 const h=document.createElement('h2');h.textContent=`${rows[0].name} · 분기별 변화`;box.append(h);
 const p=document.createElement('p');p.textContent='지수: 이 동의 첫 유효 분기=100. 누락 분기는 연결하거나 보간하지 않습니다.';box.append(p);
 const base=rows.find(r=>r.amount>0)?.amount;
 const table=document.createElement('table');const head=document.createElement('tr');
 for(const text of ['분기','매출 지수','전년 동기 대비','점포 수'])td(head,text);table.append(head);
 for(const r of rows){const tr=document.createElement('tr');td(tr,qLabel(r.quarter));td(tr,r.amount===null||!base?'자료 없음':(r.amount/base*100).toFixed(1));td(tr,pct(r.yoy));td(tr,r.shops??'자료 없음');table.append(tr);}box.append(table);box.scrollIntoView({behavior:'smooth'});
}
function render(){
 const query=$('search').value.trim();const candidates=new Set(data.matches.filter(m=>m.research_order!=null&&m.code).map(m=>m.code));
 const rows=data.records.filter(r=>r.quarter===$('quarter').value&&r.name.includes(query)&&(!$('candidate').checked||candidates.has(r.code))).sort((a,b)=>a.name.localeCompare(b.name,'ko'));
 $('rows').replaceChildren();$('count').textContent=`${rows.length}개 동 · 매출 자료 없음 ${rows.filter(r=>r.amount===null).length}개 · 동 이름을 누르면 과거 추세 표시`;
 for(const r of rows){const tr=document.createElement('tr');const cell=td(tr,'');const b=document.createElement('button');b.textContent=r.name;b.onclick=()=>history(r.code);cell.append(b);td(tr,pct(r.yoy),r.yoy>0?'up':r.yoy<0?'down':'');td(tr,pct(r.qoq),r.qoq>0?'up':r.qoq<0?'down':'');td(tr,r.shops??'자료 없음');td(tr,r.shop_change===null?'자료 없음':`${r.shop_change>0?'+':''}${r.shop_change}`);td(tr,r.age50plus_pct===null?'자료 없음':`${r.age50plus_pct}%`);$('rows').append(tr);}
}
fetch('data/revenue.json').then(r=>{if(!r.ok)throw Error('자료 응답 오류');return r.json();}).then(x=>{data=x;$('meta').textContent=`수집 ${x.retrieved_at} · 최신 ${qLabel(x.latest)} · 최신 매출 ${x.coverage.latest_sales_areas}개 동 / 점포 자료 ${x.coverage.latest_shop_areas}개 동`;
 for(const q of [...x.quarters].reverse()){const o=document.createElement('option');o.value=q;o.textContent=qLabel(q);$('quarter').append(o);}
 for(const id of ['quarter','search','candidate'])$(id).addEventListener('input',render);render();
}).catch(e=>{$('meta').textContent=`자료를 불러오지 못했습니다: ${e.message}`;});

fetch('data/gyeonggi-revenue-scope.json').then(r=>{if(!r.ok)throw Error('자료 응답 오류');return r.json();}).then(x=>{
 $('gg-meta').textContent=`비교 순위 기준 ${x.ranking_as_of} · 확인 ${x.checked_at} · ${x.reason}`;
 function renderGyeonggi(){
  const rows=x.candidates.filter(r=>r.name.includes($('gg-search').value.trim()));
  $('gg-count').textContent=`${rows.length} / ${x.candidates.length}개 비교 후보 · 매출 미확보`;
  $('gg-rows').replaceChildren();
  for(const r of rows){const tr=document.createElement('tr');td(tr,r.name);td(tr,r.rank);td(tr,'미확보');$('gg-rows').append(tr);}
 }
 $('gg-search').addEventListener('input',renderGyeonggi);renderGyeonggi();
}).catch(e=>{$('gg-meta').textContent=`경기 후보 목록을 불러오지 못했습니다: ${e.message}`;});
