// Execute actual rendering and refresh helpers against a small isolated DOM.
const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),assert=require('node:assert/strict');
const html=fs.readFileSync(path.join(__dirname,'..','index.html'),'utf8');
assert.doesNotMatch(html,/d-guidance-description|overview-guidance/);
function source(name){const m=new RegExp(`  (?:async )?function ${name}\\(`).exec(html);assert.ok(m,name);return html.slice(m.index,html.indexOf('\n  }',m.index)+4);}
const context=vm.createContext({assert,console,URLSearchParams});
vm.runInContext(`
const API='', elements=new Map(), requests=[], notices=[];
const document={getElementById:id=>{if(!elements.has(id))elements.set(id,{innerHTML:'',textContent:'',dataset:{},setAttribute(){},removeAttribute(){}});return elements.get(id)}};
const esc=s=>String(s??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
const escAttr=esc, showToast=text=>notices.push(text);
const getCustomerInsight=c=>c.insight||{}, getCustomerListGuidance=c=>c.list_guidance||c.insight?.list_guidance||{};
const getCustomerPlanningSeed=()=>({}), renderContactList=()=>{};
const customerCanStartContact=c=>Boolean(c.insight?.list_guidance?.can_start_unscheduled_contact);
let currentCustomer=null,detailStatsRequestSerial=0;
const fetch=url=>new Promise(resolve=>requests.push({url,resolve:stats=>resolve({ok:true,json:async()=>stats})}));
${['latestOrderNumber','getCardDateHtml','renderLatestOrder','customerStatsUrl','refreshStats','applyStats',
   'renderDetailTags','getGuidanceStatusClass','getGuidanceStatusLabel','renderCustomerDecisionDetail'].map(source).join('\n')}
`,context);
vm.runInContext(String.raw`(async()=>{
const order={order_date:'2026-10-19',delivery_date:'2026-11-04',currency:'SEK',total_dfp:18,order_value:6480,
 lines:[{article:'<img src=x onerror=alert(1)>',dfp:12},{article:'Blåbär',dfp:6}]};
renderLatestOrder(order);
assert.match(elements.get('d-latest-order-body').innerHTML,/&lt;img/);
assert.doesNotMatch(elements.get('d-latest-order-body').innerHTML,/<img/);
assert.match(elements.get('d-latest-order-body').innerHTML,/6\u00a0480 SEK/);
renderLatestOrder({...order,total_dfp:null,order_value:null,lines:[{article:'Unknown',dfp:null}]});
assert.match(elements.get('d-latest-order-body').innerHTML,/Ordervärde: <strong>— SEK/);
assert.doesNotMatch(elements.get('d-latest-order-body').innerHTML,/>0</);
renderLatestOrder(null);assert.equal(elements.get('d-latest-order-body').textContent,'Ingen order registrerad');
const a={customer_id:'a',customer:'Same name',customer_segment:'A',sales_person:'Sofia',phone:'0701',
 insight:{list_guidance:{focus_label:'Återköp',status_key:'overdue_followup',status_label:'Att hantera',can_start_unscheduled_contact:false}}};
const b={...a,customer_id:'b'};
currentCustomer=a;renderCustomerDecisionDetail(a);
assert.match(elements.get('d-tags').innerHTML,/A<\/span>[\s\S]*Ansvarig: Sofia[\s\S]*Återköp[\s\S]*Missad aktivitet/);
assert.equal((elements.get('d-tags').innerHTML.match(/<span /g)||[]).length,4);
assert.doesNotMatch(elements.get('d-tags').innerHTML,/Att hantera|Prioritet|Affärsvärde/);
assert.equal(elements.get('d-call-btn').disabled,true);
assert.equal(elements.get('update-contact-btn').disabled,false);
assert.equal(elements.get('d-plan-btn').disabled,false);
const stats=(ref)=>({order_count:1,total_sales:10,currency:'SEK',latest_order:{...order,lines:[{article:ref,dfp:1}]},contacts:[],timeline:[]});
const old=refreshStats(a);currentCustomer=b;const recent=refreshStats(b);
assert.match(requests[0].url,/customer_id=a/);assert.match(requests[1].url,/customer_id=b/);
requests[1].resolve(stats('B only'));await recent;
requests[0].resolve(stats('A stale'));await old;
assert.match(elements.get('d-latest-order-body').innerHTML,/B only/);
assert.doesNotMatch(elements.get('d-latest-order-body').innerHTML,/A stale/);
assert.deepEqual(notices,[]);
console.log('Customer detail: safe order rendering, nulls, overview, contact gates and late stats identity PASS');
})()`,context).catch(e=>{console.error(e);process.exitCode=1});
