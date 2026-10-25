/* User-entered observations, local to this browser; never overwrite source facts. */
(()=>{
 const key='clinic-v3-provider-scale-v1';let saved={};
 try{saved=JSON.parse(localStorage.getItem(key)||'{}');if(!saved||Array.isArray(saved)||typeof saved!=='object')saved={};}catch{saved={};}
 const escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const original=ClinicExplorer.prototype.info;
 const kinds={exclusive:'전용면적',reported:'인허가 신고 면적',leased:'계약·공급면적',building:'건물 전체 면적',unknown:'면적 기준 미확인'};
 ClinicExplorer.prototype.info=function(p){
  const r=saved[p.provider_id]||{},a=Number(r.area_m2),v=Number(r.revenue_manwon);
  const area=r.area_m2>0?`${a.toLocaleString('ko-KR',{maximumFractionDigits:1})}㎡ / ${(a/3.305785124).toFixed(1)}평 · ${kinds[r.area_kind]||kinds.unknown}`:'미확인';
  const revenue=r.revenue_manwon>0?`${v.toLocaleString('ko-KR')}만원 · ${r.revenue_period} · ${r.revenue_kind==='reported'?'실매출로 전달받은 값(독립 검증 전)':'추정 매출'}`:'미확인';
  const opts=(values,current)=>Object.entries(values).map(([k,v])=>`<option value="${k}" ${k===current?'selected':''}>${v}</option>`).join('');
  return original.call(this,p)+`<div class="scale-box"><b>면적 기준 규모</b><br>${escape(area)}<br><b>매출</b><br>${escape(revenue)}<p class="muted">${r.updated_at?'사용자 입력 · '+escape(r.updated_at.slice(0,10)):'면적·매출 공개자료 미연결'}${r.source?'<br>면적 출처: '+escape(r.source):''}${r.revenue_source?'<br>매출 출처: '+escape(r.revenue_source):''}</p><details><summary>면적·매출 기록 / 수정</summary><form data-scale-id="${escape(p.provider_id)}"><p>이 브라우저에만 저장됩니다. 공식 확인값이나 순위 점수에 자동 반영되지 않습니다.</p><label>면적 <input name="area" type="number" min="0.01" step="any" value="${r.area_m2>0?escape(r.area_m2):''}" placeholder="미확인 시 비워두기"></label><label>입력 단위 <select name="unit"><option value="m2">㎡</option><option value="pyeong">평</option></select></label><label>면적 기준 <select name="area_kind">${opts(kinds,r.area_kind||'unknown')}</select></label><label>면적 출처·기준일 <input name="source" value="${escape(r.source||'')}" placeholder="예: 도면 / 2026-10-25"></label><label>매출 (만원) <input name="revenue" type="number" min="0.01" step="any" value="${r.revenue_manwon>0?escape(r.revenue_manwon):''}" placeholder="미확인 시 비워두기"></label><label>매출 구분 <select name="revenue_kind">${opts({estimated:'추정값',reported:'실매출로 전달받은 값'},r.revenue_kind||'estimated')}</select></label><label>매출 기간 <input name="revenue_period" value="${escape(r.revenue_period||'')}" placeholder="예: 2025년 연간 / 2026년 9월"></label><label>매출 출처·범위 <input name="revenue_source" value="${escape(r.revenue_source||'')}" placeholder="출처, 보험·비급여 포함 여부 등"></label><button type="submit">이 브라우저에 기록</button><p role="status" class="scale-status"></p></form></details><p class="muted">전용 95~100평과 비교하려면 전용면적이 필요합니다. 신고·계약·건물 면적은 별도 기준입니다. 매출은 이익이 아니며 카드 결제액·보험 청구액만으로 총매출을 확정할 수 없습니다.</p></div>`;
 };
 document.addEventListener('submit',event=>{
  const form=event.target.closest('form[data-scale-id]');if(!form)return;event.preventDefault();
  const d=new FormData(form),status=form.querySelector('.scale-status');
  const a=String(d.get('area')).trim(),r=String(d.get('revenue')).trim();
  if((a&&(!Number.isFinite(+a)||+a<=0))||(r&&(!Number.isFinite(+r)||+r<=0))){status.textContent='양수를 입력하거나 미확인은 비워주세요.';return;}
  if(a&&!String(d.get('source')).trim()){status.textContent='면적의 출처와 기준일을 입력해 주세요.';return;}
  if(r&&(!String(d.get('revenue_period')).trim()||!String(d.get('revenue_source')).trim())){status.textContent='매출의 기간과 출처·범위를 입력해 주세요.';return;}
  const record={area_m2:a?+a*(d.get('unit')==='pyeong'?3.305785124:1):null,area_kind:d.get('area_kind'),source:String(d.get('source')).trim(),
   revenue_manwon:r?+r:null,revenue_kind:d.get('revenue_kind'),revenue_period:String(d.get('revenue_period')).trim(),revenue_source:String(d.get('revenue_source')).trim(),updated_at:new Date().toISOString(),verification:'user_entered_not_independently_verified'};
  try{const next={...saved,[form.dataset.scaleId]:record};localStorage.setItem(key,JSON.stringify(next));saved=next;status.textContent='기록했습니다. 의원 상세를 다시 열거나 반경을 변경하면 표시가 갱신됩니다.';}catch{status.textContent='브라우저 저장에 실패했습니다. 입력 내용을 별도로 보관해 주세요.';}
 });
})();
