const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const view=require('../web/market-demand.js');
const data=require('../web/data/market-demand.json');
test('exact administrative identity, no fuzzy name or Gyeonggi join',()=>{
 const p=data.pilots[0];
 assert.equal(view.matchingPilot(data.pilots,{adm_nm:p.administrative_fullname}),p);
 for(const name of ['상도1동','경기도 상도1동','서울특별시 동작구 상도동'])assert.equal(view.matchingPilot(data.pilots,{adm_nm:name}),undefined);
 assert.equal(view.matchingPilot(data.pilots,null),undefined);
});
test('zero, suppression and absence remain distinct in the UI',()=>{
 assert.equal(view.value({state:'observed',value:'0'}),'0');
 assert.equal(view.value({state:'suppressed',value:null}),'비공개(*)');
 assert.equal(view.value({state:'missing',value:null}),'결측');
 assert.notEqual(view.pct(null),view.pct(0));
 assert.equal(view.esc('<script>"'), '&lt;script&gt;&quot;');
});
test('pilots keep radius-independent finance, blocked grid aggregation, no new success score',()=>{
 assert.equal(data.pilots.length,6);assert.deepEqual(data.coverage,{consumption:6,workers:6,street:6});
 for(const p of data.pilots){
  assert.equal(new Set(p.medical.map(r=>r.quarter)).size,p.medical.length);
  assert.equal(p.medical.length,22);assert.equal(p.medical.at(-1).quarter,'20262');
  assert.equal(p.street.age_time_cross,null);assert.equal(p.living_population.totals,null);
  assert.equal(p.workers.actual_observation_date,null);assert.equal(p.consumption.quarterly_growth,null);
  assert.equal('success_score' in p,false);assert.equal('expected_patients' in p,false);
 }
});
test('integration preserves original map/revenue hooks and isolates the new module',()=>{
 const read=f=>fs.readFileSync(path.join(__dirname,'../web',f),'utf8');
 assert.match(read('index.html'),/id="market-demand-inline"/);
 assert.match(read('research.js'),/window\.revenueMap\?\.select\(r\)/);
 assert.match(read('research.js'),/window\.marketDemand\?\.select\(null\)/);
 assert.match(read('sw.js'),/market-demand\.json/);
 assert.match(read('market-demand.css'),/@media\(max-width:600px\)/);
});
