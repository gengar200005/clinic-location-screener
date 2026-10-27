'use strict';
// SGIS candidate ids are connected only through the existing audited match list.
const RevenueView = {
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
    this.controls.innerHTML='<div class="map-tools"><b>행정동별 매출 지도</b><label><input id="revenue-overlay" type="checkbox" checked> 행정동 색상 표시</label><label>기준 분기 <select id="revenue-quarter" disabled></select></label><button id="revenue-all">전체 행정동 탐색</button><a href="revenue.html">전체 매출표 ↗</a><p id="revenue-legend" class="muted">자료 불러오는 중…</p></div>';
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
      this.data=await get('data/revenue.json');
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
  update(rows){this.rows=rows;this.draw();}
  draw(){
    if(!this.data)return;
    this.layer?.clearLayers();
    const legend=document.getElementById('revenue-legend');
    if(!document.getElementById('revenue-overlay').checked){legend.textContent='매출 색상 꺼짐 · 후보를 선택하면 아래 요약을 확인할 수 있습니다.';return;}
    if(!this.map||this.boundaryError){legend.textContent='행정동 경계를 불러올 수 없습니다. 후보 목록의 매출 요약은 사용할 수 있습니다.';return;}
    if(!this.boundaries)return;
    const byId=new Map(this.rows.map(r=>[r.adm_cd,r]));let count=0;
    const features=this.boundaries.features.filter(f=>{const a=byId.get(f.properties.adm_cd);return a&&a.adm_nm===f.properties.adm_nm;});
    L.geoJSON(features,{pane:'revenueAreas',bubblingMouseEvents:false,
      style:f=>{const a=byId.get(f.properties.adm_cd);const r=RevenueView.record(this.data,a,this.quarter);if(Number.isFinite(r?.yoy))count++;
        return {color:'#566c68',weight:1,fillColor:RevenueView.color(r?.yoy),fillOpacity:.43};},
      onEachFeature:(f,polygon)=>{const a=byId.get(f.properties.adm_cd);const r=RevenueView.record(this.data,a,this.quarter);
        const tip=document.createElement('span');tip.textContent=`${a.adm_nm} · 전년 대비 ${RevenueView.percent(r?.yoy)}`;polygon.bindTooltip(tip);
        const choose=()=>{this.onSelect(a);const popup=document.createElement('div');popup.textContent=`${a.adm_nm} · ${this.quarterLabel(this.quarter)} · 전년 대비 ${RevenueView.percent(r?.yoy)} · 지도 아래 요약 확인`;polygon.bindPopup(popup).openPopup();};
        polygon.on('click',choose);
        polygon.on('add',()=>{const el=polygon.getElement();if(!el)return;el.setAttribute('role','button');el.setAttribute('tabindex','0');el.setAttribute('aria-label',`${a.adm_nm} 매출 보기`);el.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();e.stopPropagation();choose();}});});}
    }).addTo(this.layer);
    legend.innerHTML='';
    for(const [v,label] of [[15,'+10% 이상'],[5,'0% 초과'],[0,'0%'],[-5,'−10% 초과'],[-15,'−10% 이하'],[null,'자료 없음']]){
      const item=document.createElement('span');item.className='revenue-key';const swatch=document.createElement('i');swatch.style.background=RevenueView.color(v);item.append(swatch,document.createTextNode(label));legend.append(item);
    }
    legend.append(document.createTextNode(` · 전년 동기 대비 · 현재 목록 중 경계 ${features.length}/${this.rows.length}개, 증감 자료 ${count}개. 색은 입지 등급이 아닙니다.`));
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
    for(const [label,value] of [['전년 동기 대비',RevenueView.percent(r.yoy)],['직전 분기 대비',RevenueView.percent(r.qoq)],['점포 수',r.shops==null?'자료 없음':`${r.shops}곳`],['50세 이상 매출 비중',r.age50plus_pct==null?'자료 없음':`${r.age50plus_pct}%`]]){
      const card=document.createElement('div');const small=document.createElement('span');small.textContent=label;const strong=document.createElement('strong');strong.textContent=value;card.append(small,strong);stats.append(card);
    }this.box.append(stats);
    const details=document.createElement('details');const summary=document.createElement('summary');summary.textContent='분기별 추세 펼치기';details.append(summary);
    const table=document.createElement('table');const head=table.createTHead().insertRow();for(const label of ['분기','매출 지수','전년 대비','점포 수']){const th=document.createElement('th');th.textContent=label;head.append(th);}
    const history=this.data.records.filter(x=>x.code===r.code).sort((a,b)=>a.quarter.localeCompare(b.quarter));const base=history.find(x=>x.amount>0)?.amount;
    const body=table.createTBody();for(const row of history){const tr=body.insertRow();for(const value of [this.quarterLabel(row.quarter),row.amount==null||!base?'자료 없음':(row.amount/base*100).toFixed(1),RevenueView.percent(row.yoy),row.shops??'자료 없음'])tr.insertCell().textContent=value;}
    const scroll=document.createElement('div');scroll.className='scroll';scroll.append(table);details.append(scroll);this.box.append(details);
    const caveat=document.createElement('p');caveat.className='muted';caveat.textContent='지수는 첫 유효 분기=100. 유일한 정확한 서울 동명으로 연결한 참고값이며 공식 코드 대응·시점별 경계 일치는 미검증입니다. 색상은 기존 경계에 참고 표시합니다. 반경·기준점을 옮겨도 이 행정동 매출은 재계산되지 않습니다. 출처: 서울특별시·서울신용보증재단 / 공공누리 1유형.';this.box.append(caveat);
  }
}
