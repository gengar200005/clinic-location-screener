const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path');
const view=require('../web/current-view.js'),data=require('../web/data/current-recommendations.json'),revenueView=require('../web/revenue-map.js'),revenue=require('../web/data/revenue.json');
test('all current views share one source; outside-six candidates can enter',()=>{
 const rows=view.shortlist(data);assert.equal(rows.length,45);assert.equal(rows.filter(p=>p.region==='서울').length,30);
 assert.equal(rows.filter(p=>p.region==='경기').length,15);assert.ok(rows.some(p=>!p.old_area_visit_order));
 assert.ok(rows.every(p=>!p.area.includes('정릉')&&!p.exclusion));assert.equal(new Set(rows.map(p=>p.id)).size,45);
 const root=path.join(__dirname,'../web');for(const f of ['index.html','market-demand.html','current-fieldwork.html'])assert.match(fs.readFileSync(path.join(root,f),'utf8'),/current-view\.js/);
 assert.match(fs.readFileSync(path.join(root,'current-view.js'),'utf8'),/data\/current-recommendations\.json/);
 assert.match(fs.readFileSync(path.join(root,'market-demand.js'),'utf8'),/CurrentView\.load/);
 assert.match(fs.readFileSync(path.join(root,'research.js'),'utf8'),/currentData=await CurrentView\.load/);
});
test('current coordinate uses its own supply/location and audited administrative medical context',()=>{
 const p=view.shortlist(data).find(p=>p.market_code),a=view.adapt(p);
 assert.equal(a.lat_shops,p.lat);assert.equal(a.adm_cd,p.id);
 assert.ok(revenueView.record(revenue,a,'20262'));
 assert.equal(revenueView.record(revenue,{...a,adm_nm:'다른 동명'},'20262'),null);
 assert.equal(revenueView.record(revenue,{...a,market_link_verified:false},'20262'),null);
 const g=view.shortlist(data).find(p=>p.region==='경기');assert.equal(revenueView.record(revenue,view.adapt(g),'20262'),null);
});
test('current detail and field cards display same version and visit state, historical ranking is not subtracted',()=>{
 const p=view.shortlist(data)[0],detail=view.detail(p,data),cards=view.cards(data,[p]);
 for(const s of [detail,cards]){assert.ok(s.includes(data.version));assert.ok(s.includes(p.visit_state));}
 assert.ok(!detail.includes('239위에서'));assert.ok(detail.includes('순위 차감'));
});
test('historical field entry points explicitly link to current state without rewriting snapshots',()=>{
 for(const f of ['v3/field_visit_plan.html','v3/priority_review.html','visit-evidence.html','site-validation.html']){
  const s=fs.readFileSync(path.join(__dirname,'../web',f),'utf8');assert.match(s,/과거 고정 분석 기록/);assert.match(s,/current-fieldwork\.html/);
 }
});
test('a verified visit-ready building outside the market shortlist is still shown as ready everywhere',()=>{
 const building={...data.candidates.find(p=>p.id==='deuxville191'),visit_state:'방문 준비 완료'};
 const changed={...data,candidates:data.candidates.map(p=>p.id===building.id?building:p)};
 assert.equal(view.ready(changed).length,1);
 assert.equal(view.shortlist(changed).length,45);
 assert.match(view.cards(changed),/방문 준비 완료 1곳/);
 assert.match(view.cards(changed),/조사 우선군과 별도/);
});
