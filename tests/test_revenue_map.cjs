const test=require('node:test');
const assert=require('node:assert/strict');
const view=require('../web/revenue-map.js');
const data=require('../web/data/revenue.json');
const anchors=require('../web/data/research.json').anchors;
const sangam=anchors.find(a=>a.adm_nm==='서울특별시 마포구 상암동');

test('candidate list filtering does not remove revenue areas from the map',()=>{
  const visible=[sangam];
  assert.equal(view.areaAnchors(visible,anchors).length,827);
  assert.equal(view.areaAnchors(visible,anchors).includes(anchors.find(a=>a.adm_nm==='서울특별시 동작구 상도1동')),true);
  assert.deepEqual(view.areaAnchors(visible),visible);
});
test('missing is distinct from zero growth',()=>{
  assert.equal(view.percent(null),'자료 없음');
  assert.equal(view.percent(0),'0.0%');
  assert.notEqual(view.color(null),view.color(0));
});
test('reuse exact audited identity; never join same-looking codes or renamed areas',()=>{
  assert.equal(view.record(data,sangam,'20262').shops,35);
  assert.equal(view.record(data,{...sangam,adm_nm:'변경된 동명'},'20262'),null);
  assert.equal(view.record(data,sangam,'20263'),null);
  for(const anchor of anchors.filter(a=>a.adm_nm.startsWith('경기도 ')))
    assert.equal(view.record(data,anchor,'20262'),null);
});
