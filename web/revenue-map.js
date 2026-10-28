'use strict';
// SGIS candidate ids are connected only through the existing audited match list.
const RevenueView = {
  areaAnchors: (visible, all) => all ?? visible,
  money: v => v==null?'자료 없음':Math.round(v).toLocaleString('ko-KR')+'원',
  percent: v => Number.isFinite(v) ? `${v>0?'+':''}${v.toFixed(1)}%` : '자료 없음',
  color: v => !Number.isFinite(v) ? '#969fa5' : v>=10 ? '#176b63' : v>0 ? '#75bcb1' : v===0 ? '#e5dcb6' : v>-10 ? '#dca46d' : '#af5932',
  record(data, anchor, quarter) {
    const match=data.matches.find(m=>m.anchor_id===anchor.adm_cd && m.name===anchor.adm_nm && m.method==='unique_exact_name');
    return match?.code ? data.records.find(r=>r.code===match.code && r.quarter===quarter) ?? null : null;
  }
};
if(typeof module!=='undefined')module.exports=RevenueView;

class RevenueMap {
  constructor(map, onSelect) {
    this.map=map;this.onSelect=onSelect;this.rows=[];this.selected=null;
    this.box=document.getElementById('map-revenue');
    this.controls=document.getElementById('revenue-map-controls');
    this.controls.innerHTML='<div class="map-tools"><b>행정동별 매출 지도</b><label><input id="revenue-overlay" type="checkbox" checked> 행정동 매출 탐색</label><label>기준 분기 <select id="revenue-quarter" disabled></select></label><button id="revenue-all">전체 행정동 탐색</button><a href="revenue.html">전체 매출표 ↗</a><p id="revenue-legend" class="muted">자료 불러오는 중…</p></div>';
    this.box.innerHTML='<p>매출 자료를 불러오는 중…</p>';
    if(map){map.createPane('revenueAreas');map.getPane('revenueAreas').style.zIndex=390;this.layer=L.layerGroup().addTo(map);}
    document.getElementById('revenue-overlay').onchange=()=>this.draw();
    document.getElementById('revenue-quarter').onchange=()=>{this.draw();this.select(this.selected);};
    document.getElementById('revenue-all').onclick=()=>{
      document.getElementById('scope').value='all';document.getElementById('search').value='';document.getElementById('region').value='';
      document.getElementById('scope').dispatchEvent(new Event('change'));
    };
    this.load();
  }
  async load(){
    try{
      const get=async url=>{const r=await fetch(url);if(!r.ok)throw Error('HTTP '+r.status);return r.json();};
      this.data=await get('data/revenue.json?v=20261028-fullpopulation');
      const q=document.getElementById('revenue-quarter');
      for(const quarter of [...this.data.quarters].reverse()){const o=document.createElement('option');o.value=quarter;o.textContent=this.quarterLabel(quarter);q.append(o);}q.disabled=false;
      this.select(this.selected);
      try{this.boundaries=await get('data/boundaries.geojson');}catch{this.boundaryError=true;}
      this.draw();
    }catch{
      this.box.textContent='매출 자료를 불러오지 못했습니다. 후보와 주변 의원 탐색은 계속 사용할 수 있습니다.';
      document.getElementById('revenue-legend').textContent='매출 자료 연결 오류 · 새로고침 후 다시 확인해 주세요.';
    }
  }
  quarterLabel(q){return `${q.slice(0,4)}년 ${q.slice(4)}분기`;}
  get quarter(){return document.getElementById('revenue-quarter').value;}
  update(rows,allRows){this.rows=rows;this.allRows=RevenueView.areaAnchors(rows,allRows);this.draw();}
  draw(){
    if(!this.data)return;
    this.layer?.clearLayers();
    const legend=document.getElementById('revenue-legend');
    if(!document.getElementById('revenue-overlay').checked){legend.textContent='매출 색상 꺼짐 · 후보를 선택하면 아래 요약을 확인할 수 있습니다.';return;}
    if(!this.map||this.boundaryError){legend.textContent='행정동 경계를 불러올 수 없습니다. 후보 목록의 매출 요약은 사용할 수 있습니다.';return;}
    if(!this.boundaries)return;
    const byId=new Map((this.allRows ?? this.rows).map(r=>[r.adm_cd,r]));let count=0;
    const features=this.boundaries.features.filter(f=>{const a=byId.get(f.properties.adm_cd);return a&&a.adm_nm===f.properties.adm_nm;});
    L.geoJSON(features,{pane:'revenueAreas',bubblingMouseEvents:false,
      style:f=>{const a=byId.get(f.properties.adm_cd);const r=RevenueView.record(this.data,a,this.quarter);if(Number.isFinite(r?.yoy))count++;
        return {color:'#80948b',weight:.6,opacity:.45,fillColor:RevenueView.color(r?.yoy),fillOpacity:0};},
      onEachFeature:(f,polygon)=>{const a=byId.get(f.properties.adm_cd);const r=RevenueView.record(this.data,a,this.quarter);
        const tip=document.createElement('span');tip.textContent=`${a.adm_nm} · 총 추정매출 ${RevenueView.money(r?.amount)} · 주민 1인당 ${RevenueView.money(r?.per_resident)} · 전년 대비 ${RevenueView.percent(r?.yoy)}`;polygon.bindTooltip(tip);
        const choose=()=>{this.onSelect(a);const popup=document.createElement('div');popup.textContent=`${a.adm_nm} · ${this.quarterLabel(this.quarter)} · 총 추정매출 ${RevenueView.money(r?.amount)} · 주민 1인당 ${RevenueView.money(r?.per_resident)} · 전년 대비 ${RevenueView.percent(r?.yoy)}`;if(r)popup.append(this.trend(r.code));polygon.bindPopup(popup,{maxWidth:380,minWidth:280}).openPopup();};
        const highlight=()=>polygon.setStyle({fillOpacity:.30,weight:1.5,opacity:.9});
        const unhighlight=()=>polygon.setStyle({fillOpacity:0,weight:.6,opacity:.45});
        polygon.on('mouseover',highlight);polygon.on('mouseout',unhighlight);
        polygon.on('click',choose);
        polygon.on('add',()=>{const el=polygon.getElement();if(!el)return;el.addEventListener('focus',highlight);el.addEventListener('blur',unhighlight);el.setAttribute('role','button');el.setAttribute('tabindex','0');el.setAttribute('aria-label',`${a.adm_nm} 매출 보기`);el.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();e.stopPropagation();choose();}});});}
    }).addTo(this.layer);
    legend.innerHTML='';
    for(const [v,label] of [[15,'+10% 이상'],[5,'0% 초과'],[0,'0%'],[-5,'−10% 초과'],[-15,'−10% 이하'],[null,'자료 없음']]){
      const item=document.createElement('span');item.className='revenue-key';const swatch=document.createElement('i');swatch.style.background=RevenueView.color(v);item.append(swatch,document.createTextNode(label));legend.append(item);
    }
    legend.append(document.createTextNode(` · 커서를 올린 동만 색상 표시 · 전년 동기 대비 · 후보 목록 필터와 별개로 전체 경계 ${features.length}개, 증감 자료 ${count}개. 색은 입지 등급이 아닙니다.`));
  }
  select(anchor){
    this.selected=anchor;
    if(!this.data)return;
    this.box.replaceChildren();
    if(!anchor){this.box.textContent='지도나 목록에서 행정동을 선택해 주세요.';return;}
    const title=document.createElement('h2');title.textContent=`${anchor.adm_nm} · 매출 요약`;this.box.append(title);
    const r=RevenueView.record(this.data,anchor,this.quarter);
    const note=document.createElement('p');note.className='muted';
    if(!r){note.textContent=anchor.adm_nm.startsWith('경기도 ')?'경기 매출 원자료 미확보 · 의원 업종·기간·행정동 코드 연결 검증 대기. 0원이나 경쟁 부재라는 뜻이 아닙니다.':'이 후보와 연결할 매출 자료가 없습니다. 코드·동명 연결 미확인 또는 제공 범위 밖입니다.';this.box.append(note);return;}
    note.textContent=`${this.quarterLabel(this.quarter)} · 서울 일반의원 업종 추정매출 · 내과 전용·개별 의원 실매출 아님`;
    this.box.append(note);
    const stats=document.createElement('div');stats.className='revenue-stats';
    for(const [label,value] of [['총 추정매출(원자료)',r.amount==null?'자료 없음':(r.amount/100000000).toFixed(2)+'억원'],['주민 1인당 환산액',RevenueView.money(r.per_resident)],['상주인구',r.population==null?'미연결':r.population.toLocaleString('ko-KR')+'명'],['전년 동기 대비',RevenueView.percent(r.yoy)],['직전 분기 대비',RevenueView.percent(r.qoq)],['점포 수',r.shops==null?'자료 없음':`${r.shops}곳`],['50세 이상 매출 비중',r.age50plus_pct==null?'자료 없음':`${r.age50plus_pct}%`]]){
      const card=document.createElement('div');const small=document.createElement('span');small.textContent=label;const strong=document.createElement('strong');strong.textContent=value;card.append(small,strong);stats.append(card);
    }this.box.append(stats);
    this.box.append(this.trend(r.code));
    const details=document.createElement('details');const summary=document.createElement('summary');summary.textContent='분기별 금액 표 보기';details.append(summary);
    const table=document.createElement('table');const head=table.createTHead().insertRow();for(const label of ['분기','총 추정매출(원자료)','주민 1인당 환산액','전년 대비','점포 수']){const th=document.createElement('th');th.textContent=label;head.append(th);}
    const history=this.data.records.filter(x=>x.code===r.code).sort((a,b)=>a.quarter.localeCompare(b.quarter));
    const body=table.createTBody();for(const row of history){const tr=body.insertRow();for(const value of [this.quarterLabel(row.quarter),RevenueView.money(row.amount),RevenueView.money(row.per_resident),RevenueView.percent(row.yoy),row.shops??'자료 없음'])tr.insertCell().textContent=value;}
    const scroll=document.createElement('div');scroll.className='scroll';scroll.append(table);details.append(scroll);this.box.append(details);
    const caveat=document.createElement('p');caveat.className='muted';caveat.textContent='매출액은 원자료 금액이며 월매출·분기총액으로 환산하지 않습니다. 1인당 값은 같은 분기·코드·동명이 일치하는 상주인구로 나눕니다. 주민 실제 소비액·환자당 매출이 아니며 외부 유입 환자를 포함합니다. 유일한 정확한 서울 동명으로 연결한 참고값이며 공식 코드 대응·시점별 경계 일치는 미검증입니다. 색상은 기존 경계에 참고 표시합니다. 반경·기준점을 옮겨도 이 행정동 매출은 재계산되지 않습니다. 출처: 서울특별시·서울신용보증재단 / 공공누리 1유형.';this.box.append(caveat);
  }
  trend(code){
    const wrap=document.createElement('div');
    const heading=document.createElement('p');heading.textContent='매출 추이 · 일반의원 원자료';heading.style.fontWeight='700';wrap.append(heading);
    const rows=this.data.quarters.map(q=>this.data.records.find(r=>r.code===code&&r.quarter===q));
    const values=rows.map(r=>r?.amount).filter(Number.isFinite);
    if(!values.length){wrap.append(document.createTextNode('연결된 추이 자료가 없습니다.'));return wrap;}
    const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 320 120');svg.setAttribute('role','img');svg.setAttribute('aria-label','분기별 일반의원 추정매출 추이. 결측 분기는 선을 연결하지 않습니다.');svg.style.cssText='width:100%;min-width:220px;display:block';
    const low=Math.min(...values),high=Math.max(...values),range=high-low||1;
    let points=[];
    const flush=()=>{if(points.length){const line=document.createElementNS(svg.namespaceURI,'polyline');line.setAttribute('points',points.join(' '));line.setAttribute('fill','none');line.setAttribute('stroke','#237563');line.setAttribute('stroke-width','2');svg.append(line);points=[];}};
    rows.forEach((r,i)=>{if(!Number.isFinite(r?.amount)){flush();return;}const x=12+i*296/Math.max(rows.length-1,1),y=98-(r.amount-low)*70/range;points.push(`${x},${y}`);const dot=document.createElementNS(svg.namespaceURI,'circle');dot.setAttribute('cx',x);dot.setAttribute('cy',y);dot.setAttribute('r','2.5');dot.setAttribute('fill','#237563');const title=document.createElementNS(svg.namespaceURI,'title');title.textContent=`${this.quarterLabel(this.data.quarters[i])}: ${RevenueView.money(r.amount)}`;dot.append(title);svg.append(dot);});flush();wrap.append(svg);
    const caption=document.createElement('p');caption.className='muted';caption.style.fontSize='12px';caption.textContent=`${this.quarterLabel(this.data.quarters[0])} ~ ${this.quarterLabel(this.data.quarters.at(-1))} · 범위 ${(low/1e8).toFixed(2)}~${(high/1e8).toFixed(2)}억원 · 세로축 자동 범위 · 결측은 연결 안 함`;wrap.append(caption);return wrap;
  }
}
